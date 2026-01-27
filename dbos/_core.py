import asyncio
import copy
import functools
import inspect
import json
import sys
import threading
import time
import uuid
from concurrent.futures import Future
from dataclasses import dataclass
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Generic,
    List,
    Optional,
    ParamSpec,
    TypeVar,
    Union,
    cast,
)

from dbos._outcome import NoResult, Outcome, Pending
from dbos._utils import GlobalParams, retriable_postgres_exception

from ._app_db import ApplicationDatabase, TransactionResultInternal
from ._context import (
    DBOSAssumeRole,
    DBOSContext,
    DBOSContextEnsure,
    EnterDBOSStepCtx,
    EnterDBOSTransaction,
    EnterDBOSWorkflow,
    OperationType,
    SetWorkflowID,
    TracedAttributes,
    assert_current_dbos_context,
    get_local_dbos_context,
)
from ._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSException,
    DBOSMaxStepRetriesExceeded,
    DBOSNonExistentWorkflowError,
    DBOSQueueDeduplicatedError,
    DBOSRecoveryError,
    DBOSUnexpectedStepError,
    DBOSWorkflowCancelledError,
    DBOSWorkflowConflictIDError,
    DBOSWorkflowFunctionNotFoundError,
)
from ._logger import dbos_logger
from ._registrations import (
    DEFAULT_MAX_RECOVERY_ATTEMPTS,
    get_config_name,
    get_dbos_class_name,
    get_dbos_func_name,
    get_func_info,
    get_or_create_func_info,
    set_dbos_func_name,
    set_func_info,
    set_temp_workflow_type,
)
from ._roles import check_required_roles
from ._serialization import WorkflowInputs
from ._sys_db import (
    EnqueueOptionsInternal,
    GetEventWorkflowContext,
    OperationResultInternal,
    WorkflowStatus,
    WorkflowStatusInternal,
    WorkflowStatusString,
)

if TYPE_CHECKING:
    from ._dbos import (
        DBOS,
        WorkflowHandle,
        WorkflowHandleAsync,
        DBOSRegistry,
        IsolationLevel,
    )

from sqlalchemy.exc import DBAPIError, InvalidRequestError

P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values
F = TypeVar("F", bound=Callable[..., Any])

TEMP_SEND_WF_NAME = "<temp>.temp_send_workflow"
DEBOUNCER_WORKFLOW_NAME = "_dbos_debouncer_workflow"
DEFAULT_POLLING_INTERVAL = 1.0


class WorkflowHandleFuture(Generic[R]):

    def __init__(self, workflow_id: str, future: Future[R], dbos: "DBOS"):
        self.workflow_id = workflow_id
        self.future = future
        self.dbos = dbos

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(
        self, *, polling_interval_sec: float = DEFAULT_POLLING_INTERVAL
    ) -> R:
        try:
            r = self.future.result()
        except Exception as e:
            serialized_e = self.dbos._serializer.serialize(e)
            self.dbos._sys_db.record_get_result(self.workflow_id, None, serialized_e)
            raise
        serialized_r = self.dbos._serializer.serialize(r)
        self.dbos._sys_db.record_get_result(self.workflow_id, serialized_r, None)
        return r

    def get_status(self) -> WorkflowStatus:
        stat = self.dbos.get_workflow_status(self.workflow_id)
        if stat is None:
            raise DBOSNonExistentWorkflowError("target", self.workflow_id)
        return stat


class WorkflowHandlePolling(Generic[R]):

    def __init__(self, workflow_id: str, dbos: "DBOS"):
        self.workflow_id = workflow_id
        self.dbos = dbos

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(
        self, *, polling_interval_sec: float = DEFAULT_POLLING_INTERVAL
    ) -> R:
        try:
            r: R = self.dbos._sys_db.await_workflow_result(
                self.workflow_id, polling_interval_sec
            )
        except Exception as e:
            serialized_e = self.dbos._serializer.serialize(e)
            self.dbos._sys_db.record_get_result(self.workflow_id, None, serialized_e)
            raise
        serialized_r = self.dbos._serializer.serialize(r)
        self.dbos._sys_db.record_get_result(self.workflow_id, serialized_r, None)
        return r

    def get_status(self) -> WorkflowStatus:
        stat = self.dbos.get_workflow_status(self.workflow_id)
        if stat is None:
            raise DBOSNonExistentWorkflowError("target", self.workflow_id)
        return stat


class WorkflowHandleAsyncTask(Generic[R]):

    def __init__(self, workflow_id: str, task: asyncio.Future[R], dbos: "DBOS"):
        self.workflow_id = workflow_id
        self.task = task
        self.dbos = dbos

    def get_workflow_id(self) -> str:
        return self.workflow_id

    async def get_result(
        self, *, polling_interval_sec: float = DEFAULT_POLLING_INTERVAL
    ) -> R:
        try:
            r = await self.task
        except Exception as e:
            serialized_e = self.dbos._serializer.serialize(e)
            await asyncio.to_thread(
                self.dbos._sys_db.record_get_result,
                self.workflow_id,
                None,
                serialized_e,
            )
            raise
        serialized_r = self.dbos._serializer.serialize(r)
        await asyncio.to_thread(
            self.dbos._sys_db.record_get_result, self.workflow_id, serialized_r, None
        )
        return r

    async def get_status(self) -> WorkflowStatus:
        stat = await asyncio.to_thread(self.dbos.get_workflow_status, self.workflow_id)
        if stat is None:
            raise DBOSNonExistentWorkflowError("target", self.workflow_id)
        return stat


class WorkflowHandleAsyncPolling(Generic[R]):

    def __init__(self, workflow_id: str, dbos: "DBOS"):
        self.workflow_id = workflow_id
        self.dbos = dbos

    def get_workflow_id(self) -> str:
        return self.workflow_id

    async def get_result(
        self, *, polling_interval_sec: float = DEFAULT_POLLING_INTERVAL
    ) -> R:
        try:
            r: R = await asyncio.to_thread(
                self.dbos._sys_db.await_workflow_result,
                self.workflow_id,
                polling_interval_sec,
            )
        except Exception as e:
            serialized_e = self.dbos._serializer.serialize(e)
            await asyncio.to_thread(
                self.dbos._sys_db.record_get_result,
                self.workflow_id,
                None,
                serialized_e,
            )
            raise
        serialized_r = self.dbos._serializer.serialize(r)
        await asyncio.to_thread(
            self.dbos._sys_db.record_get_result, self.workflow_id, serialized_r, None
        )
        return r

    async def get_status(self) -> WorkflowStatus:
        stat = await asyncio.to_thread(self.dbos.get_workflow_status, self.workflow_id)
        if stat is None:
            raise DBOSNonExistentWorkflowError("target", self.workflow_id)
        return stat


from typing import Optional, TypedDict


class StepOptions(TypedDict, total=False):
    """
    Configuration options for steps.

    Attributes:
        name:
            Optional name for the step.
            If not provided, the function's name will be used.

        retries_allowed:
            Whether the step should be retried on failure.

        interval_seconds:
            Initial delay (in seconds) between retry attempts.

        max_attempts:
            Maximum number of attempts before the step is
            considered failed.

        backoff_rate:
            Multiplier applied to `interval_seconds` after
            each failed attempt (e.g. 2.0 = exponential backoff).
    """

    name: Optional[str]
    retries_allowed: bool
    interval_seconds: float
    max_attempts: int
    backoff_rate: float


DEFAULT_STEP_OPTIONS: StepOptions = {
    "name": None,
    "retries_allowed": False,
    "interval_seconds": 1.0,
    "max_attempts": 3,
    "backoff_rate": 2.0,
}


def normalize_step_options(opts: Optional[StepOptions]) -> StepOptions:
    return {**DEFAULT_STEP_OPTIONS, **(opts or {})}


def _init_workflow(
    dbos: "DBOS",
    ctx: DBOSContext,
    *,
    inputs: WorkflowInputs,
    wf_name: str,
    class_name: Optional[str],
    config_name: Optional[str],
    queue: Optional[str],
    workflow_timeout_ms: Optional[int],
    workflow_deadline_epoch_ms: Optional[int],
    max_recovery_attempts: Optional[int],
    enqueue_options: Optional[EnqueueOptionsInternal],
    is_recovery_request: Optional[bool],
    is_dequeued_request: Optional[bool],
) -> tuple[WorkflowStatusInternal, bool]:
    wfid = (
        ctx.workflow_id
        if len(ctx.workflow_id) > 0
        else ctx.id_assigned_for_next_workflow
    )

    # If we have a class name, the first arg is the instance and do not serialize
    if class_name is not None:
        inputs = {"args": inputs["args"][1:], "kwargs": inputs["kwargs"]}

    # Initialize a workflow status object from the context
    status: WorkflowStatusInternal = {
        "workflow_uuid": wfid,
        "status": (
            WorkflowStatusString.PENDING.value
            if queue is None
            else WorkflowStatusString.ENQUEUED.value
        ),
        "name": wf_name,
        "class_name": class_name,
        "config_name": config_name,
        "output": None,
        "error": None,
        "app_id": ctx.app_id,
        "app_version": (
            enqueue_options["app_version"]
            if enqueue_options is not None
            and enqueue_options["app_version"] is not None
            else GlobalParams.app_version
        ),
        "executor_id": ctx.executor_id,
        "recovery_attempts": None,
        "authenticated_user": ctx.authenticated_user,
        "authenticated_roles": (
            json.dumps(ctx.authenticated_roles) if ctx.authenticated_roles else None
        ),
        "assumed_role": ctx.assumed_role,
        "queue_name": queue,
        "created_at": None,
        "updated_at": None,
        "workflow_timeout_ms": workflow_timeout_ms,
        "workflow_deadline_epoch_ms": workflow_deadline_epoch_ms,
        "deduplication_id": (
            enqueue_options["deduplication_id"] if enqueue_options is not None else None
        ),
        "priority": (
            (
                enqueue_options["priority"]
                if enqueue_options["priority"] is not None
                else 0
            )
            if enqueue_options is not None
            else 0
        ),
        "inputs": dbos._serializer.serialize(inputs),
        "queue_partition_key": (
            enqueue_options["queue_partition_key"]
            if enqueue_options is not None
            else None
        ),
        "forked_from": None,
        "parent_workflow_id": (
            ctx.parent_workflow_id if len(ctx.parent_workflow_id) > 0 else None
        ),
        "started_at_epoch_ms": None,
        "owner_xid": None,
    }

    # Synchronously record the status and inputs for workflows
    try:
        wf_status, workflow_deadline_epoch_ms, should_execute = (
            dbos._sys_db.init_workflow(
                status,
                max_recovery_attempts=max_recovery_attempts,
                is_dequeued_request=is_dequeued_request,
                is_recovery_request=is_recovery_request,
                owner_xid=str(uuid.uuid4()),
            )
        )
    except DBOSQueueDeduplicatedError as e:
        if ctx.has_parent():
            result: OperationResultInternal = {
                "workflow_uuid": ctx.parent_workflow_id,
                "function_id": ctx.parent_workflow_fid,
                "function_name": wf_name,
                "output": None,
                "error": dbos._serializer.serialize(e),
                "started_at_epoch_ms": int(time.time() * 1000),
            }
            dbos._sys_db.record_operation_result(result)
        raise

    if should_execute and workflow_deadline_epoch_ms is not None:
        evt = threading.Event()
        dbos.background_thread_stop_events.append(evt)

        def timeout_func() -> None:
            try:
                assert workflow_deadline_epoch_ms is not None
                time_to_wait_sec = (
                    workflow_deadline_epoch_ms - (time.time() * 1000)
                ) / 1000
                if time_to_wait_sec > 0:
                    was_stopped = evt.wait(time_to_wait_sec)
                    if was_stopped:
                        return
                dbos._sys_db.cancel_workflow(wfid)
            except Exception as e:
                dbos.logger.warning(
                    f"Exception in timeout thread for workflow {wfid}: {e}"
                )

        timeout_thread = threading.Thread(target=timeout_func, daemon=True)
        timeout_thread.start()
        dbos._background_threads.append(timeout_thread)

    ctx.workflow_deadline_epoch_ms = workflow_deadline_epoch_ms
    status["workflow_deadline_epoch_ms"] = workflow_deadline_epoch_ms
    status["status"] = wf_status
    return status, should_execute


def _get_wf_invoke_func(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
) -> Callable[[Callable[[], R]], R]:
    def persist(func: Callable[[], R]) -> R:
        if (
            status["status"] == WorkflowStatusString.ERROR.value
            or status["status"] == WorkflowStatusString.SUCCESS.value
        ):
            dbos.logger.debug(
                f"Workflow {status['workflow_uuid']} is already completed with status {status['status']}"
            )
            # Directly return the result if the workflow is already completed
            recorded_result: R = dbos._sys_db.await_workflow_result(
                status["workflow_uuid"], polling_interval=DEFAULT_POLLING_INTERVAL
            )
            return recorded_result
        try:
            owned = dbos._active_workflows_set.acquire(status["workflow_uuid"])
            if owned:
                if inspect.iscoroutinefunction(func):
                    output = dbos._background_event_loop.submit_coroutine(
                        cast(Coroutine[Any, Any, R], func())
                    )
                else:
                    output = func()
            else:
                # Await the workflow result
                output = dbos._sys_db.await_workflow_result(
                    status["workflow_uuid"], polling_interval=DEFAULT_POLLING_INTERVAL
                )
                return output

            dbos._sys_db.update_workflow_outcome(
                status["workflow_uuid"],
                "SUCCESS",
                output=dbos._serializer.serialize(output),
            )
            return output
        except DBOSWorkflowConflictIDError:
            # Await the workflow result
            r: R = dbos._sys_db.await_workflow_result(
                status["workflow_uuid"], polling_interval=DEFAULT_POLLING_INTERVAL
            )
            return r
        except DBOSWorkflowCancelledError as error:
            raise DBOSAwaitedWorkflowCancelledError(status["workflow_uuid"])
        except Exception as error:
            dbos._sys_db.update_workflow_outcome(
                status["workflow_uuid"],
                "ERROR",
                error=dbos._serializer.serialize(error),
            )
            raise
        finally:
            if owned:
                dbos._active_workflows_set.release(status["workflow_uuid"])

    return persist


class ActiveWorkflowById:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._m: dict[str, bool] = {}

    def acquire(self, key: str) -> bool:
        """
        Returns is_owner
        """
        with self._lock:
            entry = self._m.get(key)
            if entry is None:
                self._m[key] = True
                return True
            return False

    def release(
        self,
        key: str,
    ) -> None:
        """
        Removes the key when work done
        """
        with self._lock:
            del self._m[key]

    def activeList(self) -> List[str]:
        return list(self._m.keys())


def _execute_workflow_wthread(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
    func: "Callable[P, R]",
    ctx: DBOSContext,
    args: tuple[Any],
    kwargs: dict[str, Any],
) -> R:
    attributes: TracedAttributes = {
        "name": get_dbos_func_name(func),
        "operationType": OperationType.WORKFLOW.value,
    }
    with EnterDBOSWorkflow(attributes, ctx):
        try:
            return _get_wf_invoke_func(dbos, status)(
                functools.partial(func, *args, **kwargs)
            )
        except Exception as e:
            dbos.logger.error(
                f"Exception encountered in asynchronous workflow:", exc_info=e
            )
            raise


async def _execute_workflow_async(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
    func: "Callable[P, Coroutine[Any, Any, R]]",
    ctx: DBOSContext,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> R:
    attributes: TracedAttributes = {
        "name": get_dbos_func_name(func),
        "operationType": OperationType.WORKFLOW.value,
    }
    with EnterDBOSWorkflow(attributes, ctx):
        try:
            result = Pending[R](functools.partial(func, *args, **kwargs)).then(
                _get_wf_invoke_func(dbos, status)
            )
            return await result()
        except Exception as e:
            dbos.logger.error(
                f"Exception encountered in asynchronous workflow:", exc_info=e
            )
            raise


def execute_workflow_by_id(
    dbos: "DBOS", workflow_id: str, is_recovery: bool, is_dequeue: bool
) -> "WorkflowHandle[Any]":
    status = dbos._sys_db.get_workflow_status(workflow_id)
    if not status:
        raise DBOSRecoveryError(workflow_id, "Workflow status not found")
    inputs: WorkflowInputs = dbos._serializer.deserialize(status["inputs"])
    wf_func = dbos._registry.workflow_info_map.get(status["name"], None)
    if not wf_func:
        raise DBOSWorkflowFunctionNotFoundError(
            workflow_id,
            f"{status['name']} is not a registered workflow function",
        )
    with DBOSContextEnsure():
        # If this function belongs to a configured class, add that class instance as its first argument
        if status["config_name"] is not None:
            config_name = status["config_name"]
            class_name = status["class_name"]
            iname = f"{class_name}/{config_name}"
            if iname not in dbos._registry.instance_info_map:
                raise DBOSWorkflowFunctionNotFoundError(
                    workflow_id,
                    f"configured class instance '{iname}' is not registered",
                )
            class_instance = dbos._registry.instance_info_map[iname]
            inputs["args"] = (class_instance,) + inputs["args"]
        # If this function is a class method, add that class object as its first argument
        elif status["class_name"] is not None:
            class_name = status["class_name"]
            if class_name not in dbos._registry.class_info_map:
                raise DBOSWorkflowFunctionNotFoundError(
                    workflow_id,
                    f"class '{class_name}' is not registered",
                )
            class_object = dbos._registry.class_info_map[class_name]
            inputs["args"] = (class_object,) + inputs["args"]

        with SetWorkflowID(workflow_id):
            return start_workflow(
                dbos,
                wf_func,
                inputs["args"],
                inputs["kwargs"],
                queue_name=status["queue_name"],
                execute_workflow=True,
                is_recovery=is_recovery,
                is_dequeued=is_dequeue,
            )


def start_workflow(
    dbos: "DBOS",
    func: "Callable[P, Union[R, Coroutine[Any, Any, R]]]",
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    queue_name: Optional[str] = None,
    execute_workflow: bool = True,
    is_recovery: bool = False,
    is_dequeued: bool = False,
) -> "WorkflowHandle[R]":

    # If the function has a class, add the class object as its first argument
    fself: Optional[object] = None
    if hasattr(func, "__self__"):
        fself = func.__self__
    if fself is not None:
        args = (fself,) + args

    fi = get_func_info(func)
    if fi is None:
        raise DBOSWorkflowFunctionNotFoundError(
            "<NONE>",
            f"{func.__name__} is not a registered workflow function",
        )

    func = cast("Workflow[P, R]", func.__orig_func)  # type: ignore

    inputs: WorkflowInputs = {
        "args": args,
        "kwargs": kwargs,
    }

    local_ctx = get_local_dbos_context()
    workflow_timeout_ms, workflow_deadline_epoch_ms = _get_timeout_deadline(
        local_ctx, queue_name
    )
    workflow_timeout_ms = (
        local_ctx.workflow_timeout_ms if local_ctx is not None else None
    )
    enqueue_options = EnqueueOptionsInternal(
        deduplication_id=local_ctx.deduplication_id if local_ctx is not None else None,
        priority=local_ctx.priority if local_ctx is not None else None,
        app_version=local_ctx.app_version if local_ctx is not None else None,
        queue_partition_key=(
            local_ctx.queue_partition_key if local_ctx is not None else None
        ),
    )
    new_wf_ctx = DBOSContext.create_start_workflow_child(local_ctx)
    new_child_workflow_id = new_wf_ctx.id_assigned_for_next_workflow

    if new_wf_ctx.has_parent():
        recorded_result = dbos._sys_db.check_operation_execution(
            new_wf_ctx.parent_workflow_id,
            new_wf_ctx.parent_workflow_fid,
            get_dbos_func_name(func),
        )
        if recorded_result and recorded_result["error"]:
            e: Exception = dbos._sys_db.serializer.deserialize(recorded_result["error"])
            raise e
        elif recorded_result and recorded_result["child_workflow_id"]:
            return WorkflowHandlePolling(recorded_result["child_workflow_id"], dbos)

    status, should_execute = _init_workflow(
        dbos,
        new_wf_ctx,
        inputs=inputs,
        wf_name=get_dbos_func_name(func),
        class_name=get_dbos_class_name(fi, func, args),
        config_name=get_config_name(fi, func, args),
        queue=queue_name,
        workflow_timeout_ms=workflow_timeout_ms,
        workflow_deadline_epoch_ms=workflow_deadline_epoch_ms,
        max_recovery_attempts=fi.max_recovery_attempts,
        enqueue_options=enqueue_options,
        is_recovery_request=is_recovery,
        is_dequeued_request=is_dequeued,
    )

    wf_status = status["status"]
    if new_wf_ctx.has_parent():
        dbos._sys_db.record_child_workflow(
            new_wf_ctx.parent_workflow_id,
            new_child_workflow_id,
            new_wf_ctx.parent_workflow_fid,
            get_dbos_func_name(func),
        )

    if (
        not execute_workflow
        or not should_execute
        or wf_status == WorkflowStatusString.ERROR.value
        or wf_status == WorkflowStatusString.SUCCESS.value
    ):
        return WorkflowHandlePolling(new_child_workflow_id, dbos)

    future = dbos._executor.submit(
        cast(Callable[..., R], _execute_workflow_wthread),
        dbos,
        status,
        func,
        new_wf_ctx,
        args,
        kwargs,
    )
    return WorkflowHandleFuture(new_child_workflow_id, future, dbos)


async def start_workflow_async(
    dbos: "DBOS",
    local_ctx: Optional[DBOSContext],
    new_wf_ctx: DBOSContext,
    func: "Callable[P, Coroutine[Any, Any, R]]",
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    queue_name: Optional[str] = None,
    execute_workflow: bool = True,
    is_recovery_request: bool = False,
    is_dequeued_request: bool = False,
) -> "WorkflowHandleAsync[R]":
    # If the function has a class, add the class object as its first argument
    fself: Optional[object] = None
    if hasattr(func, "__self__"):
        fself = func.__self__
    if fself is not None:
        args = (fself,) + args

    fi = get_func_info(func)
    if fi is None:
        raise DBOSWorkflowFunctionNotFoundError(
            "<NONE>",
            f"{func.__name__} is not a registered workflow function",
        )

    func = cast("Workflow[P, R]", func.__orig_func)  # type: ignore

    inputs: WorkflowInputs = {
        "args": args,
        "kwargs": kwargs,
    }

    workflow_timeout_ms, workflow_deadline_epoch_ms = _get_timeout_deadline(
        local_ctx, queue_name
    )
    enqueue_options = EnqueueOptionsInternal(
        deduplication_id=local_ctx.deduplication_id if local_ctx is not None else None,
        priority=local_ctx.priority if local_ctx is not None else None,
        app_version=local_ctx.app_version if local_ctx is not None else None,
        queue_partition_key=(
            local_ctx.queue_partition_key if local_ctx is not None else None
        ),
    )
    new_child_workflow_id = new_wf_ctx.id_assigned_for_next_workflow

    if new_wf_ctx.has_parent():
        recorded_result = await asyncio.to_thread(
            dbos._sys_db.check_operation_execution,
            new_wf_ctx.parent_workflow_id,
            new_wf_ctx.parent_workflow_fid,
            get_dbos_func_name(func),
        )
        if recorded_result and recorded_result["error"]:
            e: Exception = dbos._sys_db.serializer.deserialize(recorded_result["error"])
            raise e
        elif recorded_result and recorded_result["child_workflow_id"]:
            return WorkflowHandleAsyncPolling(
                recorded_result["child_workflow_id"], dbos
            )

    status, should_execute = await asyncio.to_thread(
        _init_workflow,
        dbos,
        new_wf_ctx,
        inputs=inputs,
        wf_name=get_dbos_func_name(func),
        class_name=get_dbos_class_name(fi, func, args),
        config_name=get_config_name(fi, func, args),
        queue=queue_name,
        workflow_timeout_ms=workflow_timeout_ms,
        workflow_deadline_epoch_ms=workflow_deadline_epoch_ms,
        max_recovery_attempts=fi.max_recovery_attempts,
        enqueue_options=enqueue_options,
        is_recovery_request=is_recovery_request,
        is_dequeued_request=is_dequeued_request,
    )

    if new_wf_ctx.has_parent():
        await asyncio.to_thread(
            dbos._sys_db.record_child_workflow,
            new_wf_ctx.parent_workflow_id,
            new_child_workflow_id,
            new_wf_ctx.parent_workflow_fid,
            get_dbos_func_name(func),
        )

    wf_status = status["status"]

    if (
        not execute_workflow
        or not should_execute
        or wf_status == WorkflowStatusString.ERROR.value
        or wf_status == WorkflowStatusString.SUCCESS.value
    ):
        return WorkflowHandleAsyncPolling(new_child_workflow_id, dbos)

    coro = _execute_workflow_async(dbos, status, func, new_wf_ctx, args, kwargs)
    # Shield the workflow task from cancellation
    task = asyncio.shield(asyncio.create_task(coro))
    return WorkflowHandleAsyncTask(new_child_workflow_id, task, dbos)


if sys.version_info < (3, 12):

    def _mark_coroutine(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> R:
            return await func(*args, **kwargs)  # type: ignore

        return async_wrapper  # type: ignore

else:

    def _mark_coroutine(func: Callable[P, R]) -> Callable[P, R]:
        inspect.markcoroutinefunction(func)

        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> R:
            return await func(*args, **kwargs)  # type: ignore

        return async_wrapper  # type: ignore


def workflow_wrapper(
    dbosreg: "DBOSRegistry",
    func: Callable[P, R],
    max_recovery_attempts: Optional[int] = DEFAULT_MAX_RECOVERY_ATTEMPTS,
) -> Callable[P, R]:
    func.__orig_func = func  # type: ignore

    fi = get_or_create_func_info(func)
    fi.max_recovery_attempts = max_recovery_attempts

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> R:
        fi = get_func_info(func)
        assert fi is not None
        if dbosreg.dbos is None:
            raise DBOSException(
                f"Function {get_dbos_func_name(func)} invoked before DBOS initialized"
            )
        dbos = dbosreg.dbos

        rr: Optional[str] = check_required_roles(func, fi)
        attributes: TracedAttributes = {
            "name": get_dbos_func_name(func),
            "operationType": OperationType.WORKFLOW.value,
        }
        inputs: WorkflowInputs = {
            "args": args,
            "kwargs": kwargs,
        }
        cctx = get_local_dbos_context()
        newwfctx = DBOSContext.create_start_workflow_child(cctx)
        resctx: Optional[DBOSContext] = None
        if cctx is not None and cctx.is_workflow():
            resctx = cctx.snapshot_step_ctx(reserve_sleep_id=False)
        workflow_timeout_ms, workflow_deadline_epoch_ms = _get_timeout_deadline(
            cctx, queue=None
        )

        wfOutcome = Outcome[R].make(functools.partial(func, *args, **kwargs))

        workflow_id = None

        def init_wf() -> Callable[[Callable[[], R]], R]:

            def recorded_result(
                c_wfid: str, dbos: "DBOS"
            ) -> Callable[[Callable[[], R]], R]:
                def recorded_result_inner(func: Callable[[], R]) -> R:
                    r: R = dbos._sys_db.await_workflow_result(
                        c_wfid, polling_interval=DEFAULT_POLLING_INTERVAL
                    )
                    return r

                return recorded_result_inner

            nonlocal workflow_id
            workflow_id = newwfctx.workflow_id

            if newwfctx.has_parent():
                r = dbos._sys_db.check_operation_execution(
                    newwfctx.parent_workflow_id,
                    newwfctx.parent_workflow_fid,
                    get_dbos_func_name(func),
                )
                if r and r["error"]:
                    e: Exception = dbos._sys_db.serializer.deserialize(r["error"])
                    raise e
                elif r and r["child_workflow_id"]:
                    return recorded_result(r["child_workflow_id"], dbos)

            status, should_execute = _init_workflow(
                dbos,
                newwfctx,
                inputs=inputs,
                wf_name=get_dbos_func_name(func),
                class_name=get_dbos_class_name(fi, func, args),
                config_name=get_config_name(fi, func, args),
                queue=None,
                workflow_timeout_ms=workflow_timeout_ms,
                workflow_deadline_epoch_ms=workflow_deadline_epoch_ms,
                max_recovery_attempts=max_recovery_attempts,
                enqueue_options=None,
                is_recovery_request=False,
                is_dequeued_request=False,
            )

            def get_recorded_result(_func: Callable[[], R]) -> R:
                return cast(
                    R,
                    dbos._sys_db.await_workflow_result(
                        status["workflow_uuid"],
                        polling_interval=DEFAULT_POLLING_INTERVAL,
                    ),
                )

            # TODO: maybe modify the parameters if they've been changed by `_init_workflow`
            dbos.logger.debug(
                f"Running workflow, id: {newwfctx.workflow_id}, name: {get_dbos_func_name(func)}"
            )

            if newwfctx.has_parent():
                dbos._sys_db.record_child_workflow(
                    newwfctx.parent_workflow_id,
                    newwfctx.workflow_id,
                    newwfctx.parent_workflow_fid,
                    get_dbos_func_name(func),
                )

            if should_execute:
                return _get_wf_invoke_func(dbos, status)
            else:
                dbos.logger.debug(
                    f"Workflow {status['workflow_uuid']} already run with status {status['status']}"
                )
                return get_recorded_result

        def record_get_result(func: Callable[[], R]) -> R:
            """
            If a child workflow is invoked synchronously, this records the implicit "getResult" where the
            parent retrieves the child's output. It executes in the CALLER'S context, not the workflow's.
            """
            try:
                r = func()
            except Exception as e:
                serialized_e = dbos._serializer.serialize(e)
                assert workflow_id is not None
                dbos._sys_db.record_get_result(workflow_id, None, serialized_e, resctx)
                raise
            serialized_r = dbos._serializer.serialize(r)
            assert workflow_id is not None
            dbos._sys_db.record_get_result(workflow_id, serialized_r, None, resctx)
            return r

        outcome = (
            wfOutcome.wrap(init_wf, dbos=dbos)
            .also(DBOSAssumeRole(rr))
            .also(EnterDBOSWorkflow(attributes, newwfctx))
            .then(record_get_result, dbos=dbos)
        )
        return outcome()  # type: ignore

    return _mark_coroutine(wrapper) if inspect.iscoroutinefunction(func) else wrapper


def decorate_workflow(
    reg: "DBOSRegistry", name: Optional[str], max_recovery_attempts: Optional[int]
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def _workflow_decorator(func: Callable[P, R]) -> Callable[P, R]:
        wrapped_func = workflow_wrapper(reg, func, max_recovery_attempts)
        func_name = name if name is not None else func.__qualname__
        set_dbos_func_name(func, func_name)
        set_dbos_func_name(wrapped_func, func_name)
        reg.register_wf_function(func_name, wrapped_func, "workflow")
        return wrapped_func

    return _workflow_decorator


def decorate_transaction(
    dbosreg: "DBOSRegistry", name: Optional[str], isolation_level: "IsolationLevel"
) -> Callable[[F], F]:
    def decorator(func: F) -> F:

        transaction_name = name if name is not None else func.__qualname__

        def invoke_tx(*args: Any, **kwargs: Any) -> Any:
            if dbosreg.dbos is None:
                raise DBOSException(
                    f"Function {transaction_name} invoked before DBOS initialized"
                )

            dbos = dbosreg.dbos
            assert (
                dbos._app_db
            ), "Transactions can only be used if DBOS is configured with an application_database_url"
            with dbos._app_db.sessionmaker() as session:
                attributes: TracedAttributes = {
                    "name": transaction_name,
                    "operationType": OperationType.TRANSACTION.value,
                }
                with EnterDBOSTransaction(session, attributes=attributes):
                    ctx = assert_current_dbos_context()
                    # Check if the step record for this transaction exists
                    recorded_step_output = dbos._sys_db.check_operation_execution(
                        ctx.workflow_id, ctx.function_id, transaction_name
                    )
                    if recorded_step_output:
                        dbos.logger.debug(
                            f"Replaying transaction, id: {ctx.function_id}, name: {attributes['name']}"
                        )
                        if recorded_step_output["error"]:
                            step_error: Exception = dbos._serializer.deserialize(
                                recorded_step_output["error"]
                            )
                            raise step_error
                        elif recorded_step_output["output"]:
                            return dbos._serializer.deserialize(
                                recorded_step_output["output"]
                            )
                        else:
                            raise Exception("Output and error are both None")

                    txn_output: TransactionResultInternal = {
                        "workflow_uuid": ctx.workflow_id,
                        "function_id": ctx.function_id,
                        "output": None,
                        "error": None,
                        "txn_snapshot": "",  # TODO: add actual snapshot
                        "executor_id": None,
                        "txn_id": None,
                        "function_name": transaction_name,
                    }
                    step_output: OperationResultInternal = {
                        "workflow_uuid": ctx.workflow_id,
                        "function_id": ctx.function_id,
                        "function_name": transaction_name,
                        "output": None,
                        "error": None,
                        "started_at_epoch_ms": int(time.time() * 1000),
                    }
                    retry_wait_seconds = 0.001
                    backoff_factor = 1.5
                    max_retry_wait_seconds = 2.0
                    while True:
                        has_recorded_error = False
                        txn_error: Optional[Exception] = None
                        try:
                            with session.begin():
                                # This must be the first statement in the transaction!
                                session.connection(
                                    execution_options={
                                        "isolation_level": isolation_level
                                    }
                                )
                                # Check recorded output for OAOO
                                recorded_output = (
                                    ApplicationDatabase.check_transaction_execution(
                                        session,
                                        ctx.workflow_id,
                                        ctx.function_id,
                                        transaction_name,
                                    )
                                )
                                if recorded_output:
                                    dbos.logger.debug(
                                        f"Replaying transaction, id: {ctx.function_id}, name: {attributes['name']}"
                                    )
                                    if recorded_output["error"]:
                                        deserialized_error: Exception = (
                                            dbos._serializer.deserialize(
                                                recorded_output["error"]
                                            )
                                        )
                                        has_recorded_error = True
                                        step_output["error"] = recorded_output["error"]
                                        dbos._sys_db.record_operation_result(
                                            step_output
                                        )
                                        raise deserialized_error
                                    elif recorded_output["output"]:
                                        step_output["output"] = recorded_output[
                                            "output"
                                        ]
                                        dbos._sys_db.record_operation_result(
                                            step_output
                                        )
                                        return dbos._serializer.deserialize(
                                            recorded_output["output"]
                                        )
                                    else:
                                        raise Exception(
                                            "Output and error are both None"
                                        )
                                else:
                                    dbos.logger.debug(
                                        f"Running transaction, id: {ctx.function_id}, name: {attributes['name']}"
                                    )

                                output = func(*args, **kwargs)
                                txn_output["output"] = dbos._serializer.serialize(
                                    output
                                )
                                assert (
                                    ctx.sql_session is not None
                                ), "Cannot find a database connection"
                                dbos._app_db.record_transaction_output(
                                    ctx.sql_session, txn_output
                                )
                                break
                        except DBAPIError as dbapi_error:
                            if retriable_postgres_exception(
                                dbapi_error
                            ) or dbos._app_db._is_serialization_error(dbapi_error):
                                # Retry on serialization failure
                                span = ctx.get_current_dbos_span()
                                if span:
                                    span.add_event(
                                        "Transaction Failure",
                                        {"retry_wait_seconds": retry_wait_seconds},
                                    )
                                time.sleep(retry_wait_seconds)
                                retry_wait_seconds = min(
                                    retry_wait_seconds * backoff_factor,
                                    max_retry_wait_seconds,
                                )
                                continue
                            txn_error = dbapi_error
                            raise
                        except InvalidRequestError as invalid_request_error:
                            dbos.logger.error(
                                f"InvalidRequestError in transaction {transaction_name} \033[1m Hint: Do not call commit() or rollback() within a DBOS transaction.\033[0m"
                            )
                            txn_error = invalid_request_error
                            raise
                        except DBOSUnexpectedStepError:
                            raise
                        except Exception as error:
                            txn_error = error
                            raise
                        finally:
                            # Don't record the error if it was already recorded
                            if txn_error and not has_recorded_error:
                                step_output["error"] = txn_output["error"] = (
                                    dbos._serializer.serialize(txn_error)
                                )
                                dbos._app_db.record_transaction_error(txn_output)
                                dbos._sys_db.record_operation_result(step_output)
            step_output["output"] = dbos._serializer.serialize(output)
            dbos._sys_db.record_operation_result(step_output)
            return output

        if inspect.iscoroutinefunction(func):
            raise DBOSException(
                f"Function {transaction_name} is a coroutine function, but DBOS.transaction does not support coroutine functions"
            )

        fi = get_or_create_func_info(func)

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            rr: Optional[str] = check_required_roles(func, fi)
            # Entering transaction is allowed:
            #  In a workflow (that is not in a step already)
            #  Not in a workflow (we will start the single op workflow)
            ctx = get_local_dbos_context()
            if ctx and ctx.is_within_workflow():
                assert (
                    ctx.is_workflow()
                ), "Transactions must be called from within workflows"
                with DBOSAssumeRole(rr):
                    return invoke_tx(*args, **kwargs)
            else:
                tempwf = dbosreg.workflow_info_map.get("<temp>." + transaction_name)
                assert tempwf
                return tempwf(*args, **kwargs)

        set_dbos_func_name(func, transaction_name)
        set_dbos_func_name(wrapper, transaction_name)

        def temp_wf(*args: Any, **kwargs: Any) -> Any:
            return wrapper(*args, **kwargs)

        wrapped_wf = workflow_wrapper(dbosreg, temp_wf)
        set_dbos_func_name(temp_wf, "<temp>." + transaction_name)
        set_dbos_func_name(wrapped_wf, "<temp>." + transaction_name)
        set_temp_workflow_type(temp_wf, "transaction")
        dbosreg.register_wf_function(
            get_dbos_func_name(temp_wf), wrapped_wf, "transaction"
        )
        wrapper.__orig_func = temp_wf  # type: ignore
        set_func_info(wrapped_wf, get_or_create_func_info(func))
        set_func_info(temp_wf, get_or_create_func_info(func))

        return cast(F, wrapper)

    return decorator


def invoke_step(
    dbos: "DBOS",
    step_ctx: DBOSContext,
    func: Callable[P, Coroutine[Any, Any, R]] | Callable[P, R],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    step_name: str,
    retries_allowed: bool,
    interval_seconds: float,
    max_attempts: int,
    backoff_rate: float,
) -> R | Coroutine[Any, Any, R]:
    attributes: TracedAttributes = {
        "name": step_name,
        "operationType": OperationType.STEP.value,
    }

    step_start_time = int(time.time() * 1000)
    attempts = max_attempts if retries_allowed else 1
    max_retry_interval_seconds: float = 3600  # 1 Hour

    def on_exception(attempt: int, error: BaseException) -> float:
        dbos.logger.warning(
            f"Step being automatically retried (attempt {attempt + 1} of {attempts})",
            exc_info=error,
        )
        ctx = assert_current_dbos_context()
        span = ctx.get_current_dbos_span()
        if span:
            span.add_event(
                f"Step attempt {attempt} failed",
                {
                    "error": str(error),
                    "retryIntervalSeconds": interval_seconds,
                },
            )
        return min(
            interval_seconds * (backoff_rate**attempt),
            max_retry_interval_seconds,
        )

    def record_step_result(func: Callable[[], R]) -> R:
        ctx = assert_current_dbos_context()
        step_output: OperationResultInternal = {
            "workflow_uuid": ctx.workflow_id,
            "function_id": ctx.function_id,
            "function_name": step_name,
            "output": None,
            "error": None,
            "started_at_epoch_ms": step_start_time,
        }

        try:
            output = func()
        except Exception as error:
            step_output["error"] = dbos._serializer.serialize(error)
            dbos._sys_db.record_operation_result(step_output)
            raise
        step_output["output"] = dbos._serializer.serialize(output)
        dbos._sys_db.record_operation_result(step_output)
        return output

    def check_existing_result() -> Union[NoResult, R]:
        ctx = assert_current_dbos_context()
        recorded_output = dbos._sys_db.check_operation_execution(
            ctx.workflow_id, ctx.function_id, step_name
        )
        if recorded_output:
            dbos.logger.debug(
                f"Replaying step, id: {ctx.function_id}, name: {attributes['name']}"
            )
            if recorded_output["error"] is not None:
                deserialized_error: Exception = dbos._serializer.deserialize(
                    recorded_output["error"]
                )
                raise deserialized_error
            elif recorded_output["output"] is not None:
                return cast(R, dbos._serializer.deserialize(recorded_output["output"]))
            else:
                raise Exception("Output and error are both None")
        else:
            dbos.logger.debug(
                f"Running step, id: {ctx.function_id}, name: {attributes['name']}"
            )
            return NoResult()

    stepOutcome = Outcome[R].make(functools.partial(func, *args, **kwargs))
    if retries_allowed:
        stepOutcome = stepOutcome.retry(
            max_attempts,
            on_exception,
            lambda i, e: DBOSMaxStepRetriesExceeded(step_name, i, e),
        )

    outcome = (
        stepOutcome.then(record_step_result)
        .intercept(check_existing_result, dbos=dbos)
        .also(EnterDBOSStepCtx(attributes, step_ctx))
    )
    return outcome()


def run_step(
    dbos: "DBOS",
    func: Callable[P, Coroutine[Any, Any, R]] | Callable[P, R],
    options: StepOptions,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> R:
    options = normalize_step_options(options)
    # If the step is called from a workflow, run it as a step.
    # Otherwise, run it as a normal function.
    ctx = get_local_dbos_context()
    if ctx and ctx.is_workflow():
        outcome = invoke_step(
            dbos,
            ctx.snapshot_step_ctx(),
            func,
            args,
            kwargs,
            step_name=options["name"] if options["name"] else func.__qualname__,
            retries_allowed=options["retries_allowed"],
            interval_seconds=options["interval_seconds"],
            max_attempts=options["max_attempts"],
            backoff_rate=options["backoff_rate"],
        )
        if inspect.iscoroutinefunction(func):
            return dbos._background_event_loop.submit_coroutine(
                cast(Coroutine[Any, Any, R], outcome)
            )
        else:
            return cast(R, outcome)
    else:
        if inspect.iscoroutinefunction(func):

            async def runfunc() -> R:
                return await cast(Callable[P, Coroutine[Any, Any, R]], func)(
                    *args, **kwargs
                )

            return dbos._background_event_loop.submit_coroutine(runfunc())
        else:
            return cast(Callable[P, R], func)(*args, **kwargs)


async def run_step_async(
    dbos: "DBOS",
    step_ctx: Optional[DBOSContext],
    func: Callable[P, Coroutine[Any, Any, R]] | Callable[P, R],
    options: StepOptions,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> R:
    # If the step is called from a workflow, run it as a step.
    # Otherwise, run it as a normal function.
    options = normalize_step_options(options)
    if step_ctx and step_ctx.is_workflow():
        outcome = invoke_step(
            dbos,
            step_ctx,
            func,
            args,
            kwargs,
            step_name=options["name"] if options["name"] else func.__qualname__,
            retries_allowed=options["retries_allowed"],
            interval_seconds=options["interval_seconds"],
            max_attempts=options["max_attempts"],
            backoff_rate=options["backoff_rate"],
        )
        if inspect.iscoroutinefunction(func):
            return await cast(Coroutine[Any, Any, R], outcome)
        else:
            return cast(R, outcome)
    else:
        if inspect.iscoroutinefunction(func):
            return await cast(Callable[P, Coroutine[Any, Any, R]], func)(
                *args, **kwargs
            )
        else:
            return await asyncio.to_thread(
                lambda: cast(Callable[P, R], func)(*args, **kwargs)
            )


def decorate_step(
    dbosreg: "DBOSRegistry",
    *,
    name: Optional[str],
    retries_allowed: bool,
    interval_seconds: float,
    max_attempts: int,
    backoff_rate: float,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(func: Callable[P, R]) -> Callable[P, R]:

        step_name = name if name is not None else func.__qualname__

        fi = get_or_create_func_info(func)

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # If the step is called from a workflow, run it as a step.
            # Otherwise, run it as a normal function.
            ctx = get_local_dbos_context()
            if ctx and ctx.is_workflow():
                if dbosreg.dbos is None:
                    raise DBOSException(
                        f"Function {step_name} invoked before DBOS initialized"
                    )
                rr: Optional[str] = check_required_roles(func, fi)
                with DBOSAssumeRole(rr):
                    return invoke_step(
                        dbosreg.dbos,
                        ctx.snapshot_step_ctx(),
                        func,
                        args,
                        kwargs,
                        step_name=step_name,
                        retries_allowed=retries_allowed,
                        interval_seconds=interval_seconds,
                        max_attempts=max_attempts,
                        backoff_rate=backoff_rate,
                    )
            else:
                return func(*args, **kwargs)

        wrapper = (
            _mark_coroutine(wrapper) if inspect.iscoroutinefunction(func) else wrapper  # type: ignore
        )

        set_dbos_func_name(func, step_name)
        set_dbos_func_name(wrapper, step_name)

        def temp_wf_sync(*args: Any, **kwargs: Any) -> Any:
            return wrapper(*args, **kwargs)

        async def temp_wf_async(*args: Any, **kwargs: Any) -> Any:
            return await wrapper(*args, **kwargs)

        temp_wf = temp_wf_async if inspect.iscoroutinefunction(func) else temp_wf_sync
        wrapped_wf = workflow_wrapper(dbosreg, temp_wf)
        set_dbos_func_name(temp_wf, "<temp>." + step_name)
        set_dbos_func_name(wrapped_wf, "<temp>." + step_name)
        set_temp_workflow_type(temp_wf, "step")
        dbosreg.register_wf_function(get_dbos_func_name(temp_wf), wrapped_wf, "step")
        wrapper.__orig_func = temp_wf  # type: ignore
        set_func_info(wrapped_wf, get_or_create_func_info(func))
        set_func_info(temp_wf, get_or_create_func_info(func))

        return cast(Callable[P, R], wrapper)

    return decorator


def send(
    dbos: "DBOS",
    cur_ctx: Optional["DBOSContext"],
    destination_id: str,
    message: Any,
    topic: Optional[str] = None,
) -> None:
    def do_send(destination_id: str, message: Any, topic: Optional[str]) -> None:
        assert cur_ctx is not None
        attributes: TracedAttributes = {
            "name": "send",
        }
        with EnterDBOSStepCtx(attributes, cur_ctx) as ctx:
            dbos._sys_db.send(
                ctx.workflow_id,
                ctx.curr_step_function_id,
                destination_id,
                message,
                topic,
            )

    if cur_ctx and cur_ctx.is_within_workflow():
        assert cur_ctx.is_workflow(), "send() must be called from within a workflow"
        return do_send(destination_id, message, topic)
    else:
        wffn = dbos._registry.workflow_info_map.get(TEMP_SEND_WF_NAME)
        assert wffn
        wffn(destination_id, message, topic)


def recv(
    dbos: "DBOS",
    cur_ctx: Optional["DBOSContext"],
    topic: Optional[str] = None,
    timeout_seconds: float = 60,
) -> Any:
    if cur_ctx is not None:
        # Must call it within a workflow
        assert cur_ctx.is_workflow(), "recv() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "recv",
        }
        with EnterDBOSStepCtx(attributes, cur_ctx) as ctx:
            timeout_function_id = ctx.curr_step_function_id + 1
            return dbos._sys_db.recv(
                ctx.workflow_id,
                ctx.curr_step_function_id,
                timeout_function_id,
                topic,
                timeout_seconds,
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("recv() must be called from within a workflow")


def set_event(
    dbos: "DBOS", cur_ctx: Optional["DBOSContext"], key: str, value: Any
) -> None:
    if cur_ctx is not None:
        if cur_ctx.is_workflow():
            # If called from a workflow function, run as a step
            attributes: TracedAttributes = {
                "name": "set_event",
            }
            with EnterDBOSStepCtx(attributes, cur_ctx) as ctx:
                dbos._sys_db.set_event_from_workflow(
                    ctx.workflow_id, ctx.curr_step_function_id, key, value
                )
        elif cur_ctx.is_step():
            dbos._sys_db.set_event_from_step(
                cur_ctx.workflow_id, cur_ctx.curr_step_function_id, key, value
            )
        else:
            raise DBOSException(
                "set_event() must be called from within a workflow or step"
            )
    else:
        raise DBOSException("set_event() must be called from within a workflow or step")


def get_event(
    dbos: "DBOS",
    cur_ctx: Optional[DBOSContext],
    workflow_id: str,
    key: str,
    timeout_seconds: float = 60,
) -> Any:
    if cur_ctx is not None and cur_ctx.is_within_workflow():
        # Call it within a workflow
        assert (
            cur_ctx.is_workflow()
        ), "get_event() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "get_event",
        }
        with EnterDBOSStepCtx(attributes, cur_ctx) as ctx:
            timeout_function_id = ctx.curr_step_function_id + 1
            caller_ctx: GetEventWorkflowContext = {
                "workflow_uuid": ctx.workflow_id,
                "function_id": ctx.curr_step_function_id,
                "timeout_function_id": timeout_function_id,
            }
            return dbos._sys_db.get_event(workflow_id, key, timeout_seconds, caller_ctx)
    else:
        # Directly call it outside of a workflow
        return dbos._sys_db.get_event(workflow_id, key, timeout_seconds)


def durable_sleep(
    dbos: "DBOS", cur_ctx: Optional["DBOSContext"], seconds: float
) -> None:
    if cur_ctx is not None:
        # Must call it within a workflow
        assert cur_ctx.is_workflow(), "sleep() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "sleep",
        }
        with EnterDBOSStepCtx(attributes, cur_ctx) as ctx:
            dbos._sys_db.sleep(ctx.workflow_id, ctx.curr_step_function_id, seconds)
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("sleep() must be called from within a workflow")


def write_stream(
    dbos: "DBOS", step_ctx: Optional["DBOSContext"], key: str, value: Any
) -> None:
    if step_ctx is not None:
        # Must call it within a workflow
        if step_ctx.is_workflow():
            attributes: TracedAttributes = {
                "name": "write_stream",
            }
            with EnterDBOSStepCtx(attributes, step_ctx) as ctx:
                dbos._sys_db.write_stream_from_workflow(
                    ctx.workflow_id, ctx.function_id, key, value
                )
        elif step_ctx.is_step():
            dbos._sys_db.write_stream_from_step(
                step_ctx.workflow_id, step_ctx.function_id, key, value
            )
        else:
            raise DBOSException(
                "write_stream() must be called from within a workflow or step"
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException(
            "write_stream() must be called from within a workflow or step"
        )


def close_stream(dbos: "DBOS", step_ctx: Optional["DBOSContext"], key: str) -> None:
    if step_ctx is not None:
        # Must call it within a workflow
        if step_ctx.is_workflow():
            attributes: TracedAttributes = {
                "name": "close_stream",
            }
            with EnterDBOSStepCtx(attributes, step_ctx) as ctx:
                dbos._sys_db.close_stream(ctx.workflow_id, ctx.function_id, key)
        else:
            raise DBOSException("close_stream() must be called from within a workflow")
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("close_stream() must be called from within a workflow")


def _get_timeout_deadline(
    ctx: Optional[DBOSContext], queue: Optional[str]
) -> tuple[Optional[int], Optional[int]]:
    if ctx is None:
        return None, None
    # If a timeout is explicitly specified, use it over any propagated deadline
    if ctx.workflow_timeout_ms:
        if queue:
            # Queued workflows are assigned a deadline on dequeue
            return ctx.workflow_timeout_ms, None
        else:
            # Otherwise, compute the deadline immediately
            return (
                ctx.workflow_timeout_ms,
                int(time.time() * 1000) + ctx.workflow_timeout_ms,
            )
    # Otherwise, return the propagated deadline, if any
    else:
        return None, ctx.workflow_deadline_epoch_ms
