import asyncio
import functools
import inspect
import json
import sys
import threading
import time
from concurrent.futures import Future
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Generic,
    Optional,
    TypeVar,
    Union,
    cast,
)

import psycopg

from dbos._outcome import Immediate, NoResult, Outcome, Pending
from dbos._utils import GlobalParams, retriable_postgres_exception

from ._app_db import ApplicationDatabase, TransactionResultInternal

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from . import _serialization
from ._context import (
    DBOSAssumeRole,
    DBOSContext,
    DBOSContextEnsure,
    DBOSContextSwap,
    EnterDBOSChildWorkflow,
    EnterDBOSStep,
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
    DBOSRecoveryError,
    DBOSUnexpectedStepError,
    DBOSWorkflowCancelledError,
    DBOSWorkflowConflictIDError,
    DBOSWorkflowFunctionNotFoundError,
)
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


class WorkflowHandleFuture(Generic[R]):

    def __init__(self, workflow_id: str, future: Future[R], dbos: "DBOS"):
        self.workflow_id = workflow_id
        self.future = future
        self.dbos = dbos

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(self) -> R:
        try:
            r = self.future.result()
        except Exception as e:
            serialized_e = _serialization.serialize_exception(e)
            self.dbos._sys_db.record_get_result(self.workflow_id, None, serialized_e)
            raise
        serialized_r = _serialization.serialize(r)
        self.dbos._sys_db.record_get_result(self.workflow_id, serialized_r, None)
        return r

    def get_status(self) -> WorkflowStatus:
        stat = self.dbos.get_workflow_status(self.workflow_id)
        if stat is None:
            raise DBOSNonExistentWorkflowError(self.workflow_id)
        return stat


class WorkflowHandlePolling(Generic[R]):

    def __init__(self, workflow_id: str, dbos: "DBOS"):
        self.workflow_id = workflow_id
        self.dbos = dbos

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(self) -> R:
        try:
            r: R = self.dbos._sys_db.await_workflow_result(self.workflow_id)
        except Exception as e:
            serialized_e = _serialization.serialize_exception(e)
            self.dbos._sys_db.record_get_result(self.workflow_id, None, serialized_e)
            raise
        serialized_r = _serialization.serialize(r)
        self.dbos._sys_db.record_get_result(self.workflow_id, serialized_r, None)
        return r

    def get_status(self) -> WorkflowStatus:
        stat = self.dbos.get_workflow_status(self.workflow_id)
        if stat is None:
            raise DBOSNonExistentWorkflowError(self.workflow_id)
        return stat


class WorkflowHandleAsyncTask(Generic[R]):

    def __init__(self, workflow_id: str, task: asyncio.Future[R], dbos: "DBOS"):
        self.workflow_id = workflow_id
        self.task = task
        self.dbos = dbos

    def get_workflow_id(self) -> str:
        return self.workflow_id

    async def get_result(self) -> R:
        try:
            r = await self.task
        except Exception as e:
            serialized_e = _serialization.serialize_exception(e)
            await asyncio.to_thread(
                self.dbos._sys_db.record_get_result,
                self.workflow_id,
                None,
                serialized_e,
            )
            raise
        serialized_r = _serialization.serialize(r)
        await asyncio.to_thread(
            self.dbos._sys_db.record_get_result, self.workflow_id, serialized_r, None
        )
        return r

    async def get_status(self) -> WorkflowStatus:
        stat = await asyncio.to_thread(self.dbos.get_workflow_status, self.workflow_id)
        if stat is None:
            raise DBOSNonExistentWorkflowError(self.workflow_id)
        return stat


class WorkflowHandleAsyncPolling(Generic[R]):

    def __init__(self, workflow_id: str, dbos: "DBOS"):
        self.workflow_id = workflow_id
        self.dbos = dbos

    def get_workflow_id(self) -> str:
        return self.workflow_id

    async def get_result(self) -> R:
        try:
            r: R = await asyncio.to_thread(
                self.dbos._sys_db.await_workflow_result, self.workflow_id
            )
        except Exception as e:
            serialized_e = _serialization.serialize_exception(e)
            await asyncio.to_thread(
                self.dbos._sys_db.record_get_result,
                self.workflow_id,
                None,
                serialized_e,
            )
            raise
        serialized_r = _serialization.serialize(r)
        await asyncio.to_thread(
            self.dbos._sys_db.record_get_result, self.workflow_id, serialized_r, None
        )
        return r

    async def get_status(self) -> WorkflowStatus:
        stat = await asyncio.to_thread(self.dbos.get_workflow_status, self.workflow_id)
        if stat is None:
            raise DBOSNonExistentWorkflowError(self.workflow_id)
        return stat


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
) -> WorkflowStatusInternal:
    wfid = (
        ctx.workflow_id
        if len(ctx.workflow_id) > 0
        else ctx.id_assigned_for_next_workflow
    )

    # In debug mode, just return the existing status
    if dbos.debug_mode:
        get_status_result = dbos._sys_db.get_workflow_status(wfid)
        if get_status_result is None:
            raise DBOSNonExistentWorkflowError(wfid)
        return get_status_result

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
        "inputs": _serialization.serialize_args(inputs),
    }

    # Synchronously record the status and inputs for workflows
    wf_status, workflow_deadline_epoch_ms = dbos._sys_db.init_workflow(
        status,
        max_recovery_attempts=max_recovery_attempts,
    )

    if workflow_deadline_epoch_ms is not None:
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
    return status


def _get_wf_invoke_func(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
) -> Callable[[Callable[[], R]], R]:
    def persist(func: Callable[[], R]) -> R:
        if not dbos.debug_mode and (
            status["status"] == WorkflowStatusString.ERROR.value
            or status["status"] == WorkflowStatusString.SUCCESS.value
        ):
            dbos.logger.debug(
                f"Workflow {status['workflow_uuid']} is already completed with status {status['status']}"
            )
            # Directly return the result if the workflow is already completed
            recorded_result: R = dbos._sys_db.await_workflow_result(
                status["workflow_uuid"]
            )
            return recorded_result
        try:
            dbos._active_workflows_set.add(status["workflow_uuid"])
            output = func()
            if not dbos.debug_mode:
                dbos._sys_db.update_workflow_outcome(
                    status["workflow_uuid"],
                    "SUCCESS",
                    output=_serialization.serialize(output),
                )
            return output
        except DBOSWorkflowConflictIDError:
            # Await the workflow result
            r: R = dbos._sys_db.await_workflow_result(status["workflow_uuid"])
            return r
        except DBOSWorkflowCancelledError as error:
            raise DBOSAwaitedWorkflowCancelledError(status["workflow_uuid"])
        except Exception as error:
            if not dbos.debug_mode:
                dbos._sys_db.update_workflow_outcome(
                    status["workflow_uuid"],
                    "ERROR",
                    error=_serialization.serialize_exception(error),
                )
            raise
        finally:
            dbos._active_workflows_set.discard(status["workflow_uuid"])

    return persist


def _execute_workflow_wthread(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
    func: "Callable[P, R]",
    ctx: DBOSContext,
    *args: Any,
    **kwargs: Any,
) -> R:
    attributes: TracedAttributes = {
        "name": get_dbos_func_name(func),
        "operationType": OperationType.WORKFLOW.value,
    }
    with DBOSContextSwap(ctx):
        with EnterDBOSWorkflow(attributes):
            try:
                result = (
                    Outcome[R]
                    .make(functools.partial(func, *args, **kwargs))
                    .then(_get_wf_invoke_func(dbos, status))
                )
                if isinstance(result, Immediate):
                    return cast(Immediate[R], result)()
                else:
                    return dbos._background_event_loop.submit_coroutine(
                        cast(Pending[R], result)()
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
    *args: Any,
    **kwargs: Any,
) -> R:
    attributes: TracedAttributes = {
        "name": get_dbos_func_name(func),
        "operationType": OperationType.WORKFLOW.value,
    }
    with DBOSContextSwap(ctx):
        with EnterDBOSWorkflow(attributes):
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


def execute_workflow_by_id(dbos: "DBOS", workflow_id: str) -> "WorkflowHandle[Any]":
    status = dbos._sys_db.get_workflow_status(workflow_id)
    if not status:
        raise DBOSRecoveryError(workflow_id, "Workflow status not found")
    inputs = _serialization.deserialize_args(status["inputs"])
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
                status["queue_name"],
                True,
                *inputs["args"],
                **inputs["kwargs"],
            )


def _get_new_wf() -> tuple[str, DBOSContext]:
    # Sequence of events for starting a workflow:
    #   First - is there a WF already running?
    #      (and not in step as that is an error)
    #   Assign an ID to the workflow, if it doesn't have an app-assigned one
    #      If this is a root workflow, assign a new ID
    #      If this is a child workflow, assign parent wf id with call# suffix
    #   Make a (system) DB record for the workflow
    #   Pass the new context to a worker thread that will run the wf function
    cur_ctx = get_local_dbos_context()
    if cur_ctx is not None and cur_ctx.is_within_workflow():
        assert cur_ctx.is_workflow()  # Not in a step
        cur_ctx.function_id += 1
        if len(cur_ctx.id_assigned_for_next_workflow) == 0:
            cur_ctx.id_assigned_for_next_workflow = (
                cur_ctx.workflow_id + "-" + str(cur_ctx.function_id)
            )

    new_wf_ctx = DBOSContext() if cur_ctx is None else cur_ctx.create_child()
    new_wf_ctx.id_assigned_for_next_workflow = new_wf_ctx.assign_workflow_id()
    new_wf_id = new_wf_ctx.id_assigned_for_next_workflow

    return (new_wf_id, new_wf_ctx)


def start_workflow(
    dbos: "DBOS",
    func: "Callable[P, Union[R, Coroutine[Any, Any, R]]]",
    queue_name: Optional[str],
    execute_workflow: bool,
    *args: P.args,
    **kwargs: P.kwargs,
) -> "WorkflowHandle[R]":

    # If the function has a class, add the class object as its first argument
    fself: Optional[object] = None
    if hasattr(func, "__self__"):
        fself = func.__self__
    if fself is not None:
        args = (fself,) + args  # type: ignore

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
    )
    new_wf_id, new_wf_ctx = _get_new_wf()

    ctx = new_wf_ctx
    new_child_workflow_id = ctx.id_assigned_for_next_workflow
    if ctx.has_parent():
        child_workflow_id = dbos._sys_db.check_child_workflow(
            ctx.parent_workflow_id, ctx.parent_workflow_fid
        )
        if child_workflow_id is not None:
            return WorkflowHandlePolling(child_workflow_id, dbos)

    status = _init_workflow(
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
    )

    wf_status = status["status"]
    if ctx.has_parent():
        dbos._sys_db.record_child_workflow(
            ctx.parent_workflow_id,
            new_child_workflow_id,
            ctx.parent_workflow_fid,
            get_dbos_func_name(func),
        )

    if not execute_workflow or (
        not dbos.debug_mode
        and (
            wf_status == WorkflowStatusString.ERROR.value
            or wf_status == WorkflowStatusString.SUCCESS.value
        )
    ):
        return WorkflowHandlePolling(new_wf_id, dbos)

    future = dbos._executor.submit(
        cast(Callable[..., R], _execute_workflow_wthread),
        dbos,
        status,
        func,
        new_wf_ctx,
        *args,
        **kwargs,
    )
    return WorkflowHandleFuture(new_wf_id, future, dbos)


async def start_workflow_async(
    dbos: "DBOS",
    func: "Callable[P, Coroutine[Any, Any, R]]",
    queue_name: Optional[str],
    execute_workflow: bool,
    *args: P.args,
    **kwargs: P.kwargs,
) -> "WorkflowHandleAsync[R]":
    # If the function has a class, add the class object as its first argument
    fself: Optional[object] = None
    if hasattr(func, "__self__"):
        fself = func.__self__
    if fself is not None:
        args = (fself,) + args  # type: ignore

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
    enqueue_options = EnqueueOptionsInternal(
        deduplication_id=local_ctx.deduplication_id if local_ctx is not None else None,
        priority=local_ctx.priority if local_ctx is not None else None,
        app_version=local_ctx.app_version if local_ctx is not None else None,
    )
    new_wf_id, new_wf_ctx = _get_new_wf()

    ctx = new_wf_ctx
    new_child_workflow_id = ctx.id_assigned_for_next_workflow
    if ctx.has_parent():
        child_workflow_id = await asyncio.to_thread(
            dbos._sys_db.check_child_workflow,
            ctx.parent_workflow_id,
            ctx.parent_workflow_fid,
        )
        if child_workflow_id is not None:
            return WorkflowHandleAsyncPolling(child_workflow_id, dbos)

    status = await asyncio.to_thread(
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
    )

    if ctx.has_parent():
        await asyncio.to_thread(
            dbos._sys_db.record_child_workflow,
            ctx.parent_workflow_id,
            new_child_workflow_id,
            ctx.parent_workflow_fid,
            get_dbos_func_name(func),
        )

    wf_status = status["status"]

    if not execute_workflow or (
        not dbos.debug_mode
        and (
            wf_status == WorkflowStatusString.ERROR.value
            or wf_status == WorkflowStatusString.SUCCESS.value
        )
    ):
        return WorkflowHandleAsyncPolling(new_wf_id, dbos)

    coro = _execute_workflow_async(dbos, status, func, new_wf_ctx, *args, **kwargs)
    # Shield the workflow task from cancellation
    task = asyncio.shield(asyncio.create_task(coro))
    return WorkflowHandleAsyncTask(new_wf_id, task, dbos)


if sys.version_info < (3, 12):

    def _mark_coroutine(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> R:
            return await func(*args, **kwargs)  # type: ignore

        return async_wrapper  # type: ignore

else:

    def _mark_coroutine(func: Callable[P, R]) -> Callable[P, R]:
        inspect.markcoroutinefunction(func)
        return func


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
        ctx = get_local_dbos_context()
        workflow_timeout_ms, workflow_deadline_epoch_ms = _get_timeout_deadline(
            ctx, queue=None
        )

        enterWorkflowCtxMgr = (
            EnterDBOSChildWorkflow if ctx and ctx.is_workflow() else EnterDBOSWorkflow
        )

        wfOutcome = Outcome[R].make(functools.partial(func, *args, **kwargs))

        workflow_id = None

        def init_wf() -> Callable[[Callable[[], R]], R]:

            def recorded_result(
                c_wfid: str, dbos: "DBOS"
            ) -> Callable[[Callable[[], R]], R]:
                def recorded_result_inner(func: Callable[[], R]) -> R:
                    r: R = dbos._sys_db.await_workflow_result(c_wfid)
                    return r

                return recorded_result_inner

            ctx = assert_current_dbos_context()  # Now the child ctx
            nonlocal workflow_id
            workflow_id = ctx.workflow_id

            if ctx.has_parent():
                child_workflow_id = dbos._sys_db.check_child_workflow(
                    ctx.parent_workflow_id, ctx.parent_workflow_fid
                )
                if child_workflow_id is not None:
                    return recorded_result(child_workflow_id, dbos)

            status = _init_workflow(
                dbos,
                ctx,
                inputs=inputs,
                wf_name=get_dbos_func_name(func),
                class_name=get_dbos_class_name(fi, func, args),
                config_name=get_config_name(fi, func, args),
                queue=None,
                workflow_timeout_ms=workflow_timeout_ms,
                workflow_deadline_epoch_ms=workflow_deadline_epoch_ms,
                max_recovery_attempts=max_recovery_attempts,
                enqueue_options=None,
            )

            # TODO: maybe modify the parameters if they've been changed by `_init_workflow`
            dbos.logger.debug(
                f"Running workflow, id: {ctx.workflow_id}, name: {get_dbos_func_name(func)}"
            )

            if ctx.has_parent():
                dbos._sys_db.record_child_workflow(
                    ctx.parent_workflow_id,
                    ctx.workflow_id,
                    ctx.parent_workflow_fid,
                    get_dbos_func_name(func),
                )

            return _get_wf_invoke_func(dbos, status)

        def record_get_result(func: Callable[[], R]) -> R:
            """
            If a child workflow is invoked synchronously, this records the implicit "getResult" where the
            parent retrieves the child's output. It executes in the CALLER'S context, not the workflow's.
            """
            try:
                r = func()
            except Exception as e:
                serialized_e = _serialization.serialize_exception(e)
                assert workflow_id is not None
                dbos._sys_db.record_get_result(workflow_id, None, serialized_e)
                raise
            serialized_r = _serialization.serialize(r)
            assert workflow_id is not None
            dbos._sys_db.record_get_result(workflow_id, serialized_r, None)
            return r

        outcome = (
            wfOutcome.wrap(init_wf)
            .also(DBOSAssumeRole(rr))
            .also(enterWorkflowCtxMgr(attributes))
            .then(record_get_result)
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
            ctx = assert_current_dbos_context()
            status = dbos._sys_db.get_workflow_status(ctx.workflow_id)
            if status and status["status"] == WorkflowStatusString.CANCELLED.value:
                raise DBOSWorkflowCancelledError(
                    f"Workflow {ctx.workflow_id} is cancelled. Aborting transaction {transaction_name}."
                )

            with dbos._app_db.sessionmaker() as session:
                attributes: TracedAttributes = {
                    "name": transaction_name,
                    "operationType": OperationType.TRANSACTION.value,
                }
                with EnterDBOSTransaction(session, attributes=attributes):
                    ctx = assert_current_dbos_context()
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
                                if dbos.debug_mode and recorded_output is None:
                                    raise DBOSException(
                                        "Transaction output not found in debug mode"
                                    )
                                if recorded_output:
                                    dbos.logger.debug(
                                        f"Replaying transaction, id: {ctx.function_id}, name: {attributes['name']}"
                                    )
                                    if recorded_output["error"]:
                                        deserialized_error = (
                                            _serialization.deserialize_exception(
                                                recorded_output["error"]
                                            )
                                        )
                                        has_recorded_error = True
                                        raise deserialized_error
                                    elif recorded_output["output"]:
                                        return _serialization.deserialize(
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
                                txn_output["output"] = _serialization.serialize(output)
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
                                span = ctx.get_current_span()
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
                                txn_output["error"] = (
                                    _serialization.serialize_exception(txn_error)
                                )
                                dbos._app_db.record_transaction_error(txn_output)
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

        def invoke_step(*args: Any, **kwargs: Any) -> Any:
            if dbosreg.dbos is None:
                raise DBOSException(
                    f"Function {step_name} invoked before DBOS initialized"
                )
            dbos = dbosreg.dbos

            attributes: TracedAttributes = {
                "name": step_name,
                "operationType": OperationType.STEP.value,
            }

            attempts = max_attempts if retries_allowed else 1
            max_retry_interval_seconds: float = 3600  # 1 Hour

            def on_exception(attempt: int, error: BaseException) -> float:
                dbos.logger.warning(
                    f"Step being automatically retried (attempt {attempt + 1} of {attempts})",
                    exc_info=error,
                )
                ctx = assert_current_dbos_context()
                span = ctx.get_current_span()
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
                }

                try:
                    output = func()
                except Exception as error:
                    step_output["error"] = _serialization.serialize_exception(error)
                    dbos._sys_db.record_operation_result(step_output)
                    raise
                step_output["output"] = _serialization.serialize(output)
                dbos._sys_db.record_operation_result(step_output)
                return output

            def check_existing_result() -> Union[NoResult, R]:
                ctx = assert_current_dbos_context()
                recorded_output = dbos._sys_db.check_operation_execution(
                    ctx.workflow_id, ctx.function_id, step_name
                )
                if dbos.debug_mode and recorded_output is None:
                    raise DBOSException("Step output not found in debug mode")
                if recorded_output:
                    dbos.logger.debug(
                        f"Replaying step, id: {ctx.function_id}, name: {attributes['name']}"
                    )
                    if recorded_output["error"] is not None:
                        deserialized_error = _serialization.deserialize_exception(
                            recorded_output["error"]
                        )
                        raise deserialized_error
                    elif recorded_output["output"] is not None:
                        return cast(
                            R, _serialization.deserialize(recorded_output["output"])
                        )
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
                .intercept(check_existing_result)
                .also(EnterDBOSStep(attributes))
            )
            return outcome()

        fi = get_or_create_func_info(func)

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # If the step is called from a workflow, run it as a step.
            # Otherwise, run it as a normal function.
            ctx = get_local_dbos_context()
            if ctx and ctx.is_workflow():
                rr: Optional[str] = check_required_roles(func, fi)
                with DBOSAssumeRole(rr):
                    return invoke_step(*args, **kwargs)
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
    dbos: "DBOS", destination_id: str, message: Any, topic: Optional[str] = None
) -> None:
    def do_send(destination_id: str, message: Any, topic: Optional[str]) -> None:
        attributes: TracedAttributes = {
            "name": "send",
        }
        with EnterDBOSStep(attributes):
            ctx = assert_current_dbos_context()
            dbos._sys_db.send(
                ctx.workflow_id,
                ctx.curr_step_function_id,
                destination_id,
                message,
                topic,
            )

    ctx = get_local_dbos_context()
    if ctx and ctx.is_within_workflow():
        assert ctx.is_workflow(), "send() must be called from within a workflow"
        return do_send(destination_id, message, topic)
    else:
        wffn = dbos._registry.workflow_info_map.get(TEMP_SEND_WF_NAME)
        assert wffn
        wffn(destination_id, message, topic)


def recv(dbos: "DBOS", topic: Optional[str] = None, timeout_seconds: float = 60) -> Any:
    cur_ctx = get_local_dbos_context()
    if cur_ctx is not None:
        # Must call it within a workflow
        assert cur_ctx.is_workflow(), "recv() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "recv",
        }
        with EnterDBOSStep(attributes):
            ctx = assert_current_dbos_context()
            ctx.function_id += 1  # Reserve for the sleep
            timeout_function_id = ctx.function_id
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


def set_event(dbos: "DBOS", key: str, value: Any) -> None:
    cur_ctx = get_local_dbos_context()
    if cur_ctx is not None:
        # Must call it within a workflow
        assert (
            cur_ctx.is_workflow()
        ), "set_event() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "set_event",
        }
        with EnterDBOSStep(attributes):
            ctx = assert_current_dbos_context()
            dbos._sys_db.set_event(
                ctx.workflow_id, ctx.curr_step_function_id, key, value
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("set_event() must be called from within a workflow")


def get_event(
    dbos: "DBOS", workflow_id: str, key: str, timeout_seconds: float = 60
) -> Any:
    cur_ctx = get_local_dbos_context()
    if cur_ctx is not None and cur_ctx.is_within_workflow():
        # Call it within a workflow
        assert (
            cur_ctx.is_workflow()
        ), "get_event() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "get_event",
        }
        with EnterDBOSStep(attributes):
            ctx = assert_current_dbos_context()
            ctx.function_id += 1
            timeout_function_id = ctx.function_id
            caller_ctx: GetEventWorkflowContext = {
                "workflow_uuid": ctx.workflow_id,
                "function_id": ctx.curr_step_function_id,
                "timeout_function_id": timeout_function_id,
            }
            return dbos._sys_db.get_event(workflow_id, key, timeout_seconds, caller_ctx)
    else:
        # Directly call it outside of a workflow
        return dbos._sys_db.get_event(workflow_id, key, timeout_seconds)


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
