import asyncio
import copy
import functools
import inspect
import json
import math
import sys
import threading
import time
import uuid
from concurrent.futures import Future
from contextlib import AbstractContextManager
from functools import wraps
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    Generic,
    List,
    Literal,
    Optional,
    ParamSpec,
    Tuple,
    Type,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

from dbos._outcome import DeferredResult, NoResult, Outcome, Pending, resolve_deferred
from dbos._utils import GlobalParams, retriable_postgres_exception

from ._app_db import ApplicationDatabase, TransactionResultInternal
from ._context import (
    OTEL_CARRIER_ATTRIBUTE,
    DBOSAssumeRole,
    DBOSContext,
    DBOSContextSetAuth,
    DuplicationPolicy,
    EnterDBOSStepCtx,
    EnterDBOSTransaction,
    EnterDBOSWorkflow,
    OperationType,
    SetEnqueueOptions,
    SetWorkflowID,
    TracedAttributes,
    assert_current_dbos_context,
    extract_trace_context,
    get_local_dbos_context,
    otel_carrier_from_attributes,
    restore_otel_carrier,
)
from ._enqueue_options import (
    EnqueueOptions,
    build_enqueue_status,
    validate_duplication_policy,
)
from ._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSException,
    DBOSMaxStepRetriesExceeded,
    DBOSNonExistentWorkflowError,
    DBOSNotAuthorizedError,
    DBOSQueueDeduplicatedError,
    DBOSRecoveryError,
    DBOSStepTimeoutError,
    DBOSStreamNondeterminismError,
    DBOSStreamTimeoutError,
    DBOSUnexpectedStepError,
    DBOSWorkflowCancelledError,
    DBOSWorkflowConflictIDError,
    DBOSWorkflowFunctionNotFoundError,
    MaxRecoveryAttemptsExceededError,
)
from ._event_loop import retrieve_future_exception
from ._logger import dbos_logger
from ._registrations import (
    DEFAULT_MAX_RECOVERY_ATTEMPTS,
    DBOSFuncInfo,
    DBOSFuncType,
    ValidateArgsCallable,
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
from ._serialization import (
    DBOSPortableJSON,
    Serializer,
    WorkflowInputs,
    WorkflowSerializationFormat,
    _safe_str,
    coerce_portable_args_to_hints,
    deserialize_args,
    deserialize_exception,
    deserialize_value,
    serialize_args,
    serialize_exception,
    serialize_value,
    serialize_value_as,
)
from ._sys_db import (
    EnqueueOptionsInternal,
    OperationResultInternal,
    SendMessage,
    SystemDatabase,
    WorkflowStatus,
    WorkflowStatusInternal,
    WorkflowStatusString,
    _dbos_stream_closed_sentinel,
    _no_stream_value,
    is_stream_closed_sentinel,
    workflow_is_active,
)
from ._tracer import dbos_tracer

if TYPE_CHECKING:
    from contextvars import Token

    from opentelemetry.context import Context as OtelContext

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
DEFAULT_POLLING_INTERVAL = 1.0


def _deferred_workflow_result(
    dbos: "DBOS", workflow_id: str, *, fail_if_missing: bool = False
) -> DeferredResult[Any]:
    """Wait for a workflow's result, deferred so the Outcome layer picks the mode:
    a sync (Immediate) caller blocks in-thread, but an async (Pending) workflow
    awaits on the event loop instead of pinning a thread-pool worker in a blocking
    poll. Pinning could otherwise starve the shared executor and deadlock recovery
    when many async runs wait on other executions: directly-invoked children, or the
    runs that own their outcomes.

    fail_if_missing fails fast for callers that know the row exists, so one that was
    deleted is not polled for forever."""
    return DeferredResult(
        lambda: dbos._sys_db.await_workflow_result(
            workflow_id,
            polling_interval=DEFAULT_POLLING_INTERVAL,
            fail_if_missing=fail_if_missing,
        ),
        lambda: dbos._sys_db.await_workflow_result_async(
            workflow_id,
            polling_interval=DEFAULT_POLLING_INTERVAL,
            fail_if_missing=fail_if_missing,
        ),
    )


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
        start_time = int(time.time() * 1000)
        try:
            try:
                r = self.future.result()
            # If the handle was cancelled, check the database
            except (DBOSWorkflowCancelledError, DBOSAwaitedWorkflowCancelledError):
                r = self.dbos._sys_db.await_workflow_result(
                    self.workflow_id, polling_interval_sec
                )
        except Exception as e:
            serialized_e, serialization = serialize_exception(
                e, None, self.dbos._serializer
            )
            self.dbos._sys_db.record_get_result(
                self.workflow_id,
                None,
                serialized_e,
                serialization,
                started_at_epoch_ms=start_time,
            )
            raise
        serialized_r, serialization = serialize_value(r, None, self.dbos._serializer)
        self.dbos._sys_db.record_get_result(
            self.workflow_id,
            serialized_r,
            None,
            serialization,
            started_at_epoch_ms=start_time,
        )
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
        start_time = int(time.time() * 1000)
        try:
            r: R = self.dbos._sys_db.await_workflow_result(
                self.workflow_id, polling_interval_sec
            )
        except Exception as e:
            serialized_e, serialization = serialize_exception(
                e, None, self.dbos._serializer
            )
            self.dbos._sys_db.record_get_result(
                self.workflow_id,
                None,
                serialized_e,
                serialization,
                started_at_epoch_ms=start_time,
            )
            raise
        serialized_r, serialization = serialize_value(r, None, self.dbos._serializer)
        self.dbos._sys_db.record_get_result(
            self.workflow_id,
            serialized_r,
            None,
            serialization,
            started_at_epoch_ms=start_time,
        )
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
        start_time = int(time.time() * 1000)
        try:
            try:
                r = await self.task
            # If the handle was cancelled, check the database
            except (DBOSWorkflowCancelledError, DBOSAwaitedWorkflowCancelledError):
                r = await self.dbos._sys_db.await_workflow_result_async(
                    self.workflow_id, polling_interval_sec
                )
        except Exception as e:
            serialized_e, serialization = serialize_exception(
                e, None, self.dbos._serializer
            )
            await asyncio.to_thread(
                self.dbos._sys_db.record_get_result,
                self.workflow_id,
                None,
                serialized_e,
                serialization,
                started_at_epoch_ms=start_time,
            )
            raise
        serialized_r, serialization = serialize_value(r, None, self.dbos._serializer)
        await asyncio.to_thread(
            self.dbos._sys_db.record_get_result,
            self.workflow_id,
            serialized_r,
            None,
            serialization,
            started_at_epoch_ms=start_time,
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
        start_time = int(time.time() * 1000)
        try:
            r: R = await self.dbos._sys_db.await_workflow_result_async(
                self.workflow_id,
                polling_interval_sec,
            )
        except Exception as e:
            serialized_e, serialization = serialize_exception(
                e, None, self.dbos._serializer
            )
            await asyncio.to_thread(
                self.dbos._sys_db.record_get_result,
                self.workflow_id,
                None,
                serialized_e,
                serialization,
                started_at_epoch_ms=start_time,
            )
            raise
        serialized_r, serialization = serialize_value(r, None, self.dbos._serializer)
        await asyncio.to_thread(
            self.dbos._sys_db.record_get_result,
            self.workflow_id,
            serialized_r,
            None,
            serialization,
            started_at_epoch_ms=start_time,
        )
        return r

    async def get_status(self) -> WorkflowStatus:
        stat = await asyncio.to_thread(self.dbos.get_workflow_status, self.workflow_id)
        if stat is None:
            raise DBOSNonExistentWorkflowError("target", self.workflow_id)
        return stat


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

        should_retry:
            Optional predicate called with a raised exception to decide
            whether the step should be retried. If it returns False (or
            an awaitable resolving to False), the exception is re-raised
            immediately without further retries. Async validators are
            only supported for async steps.

        preemptible:
            If True, cancel the (async) step if its workflow is cancelled.
            Only supported for async steps.

        timeout_seconds:
            If set, cancel the (async) step and raise DBOSStepTimeoutError if it
            runs for longer than this many seconds. Only supported for async
            steps. Each retry attempt gets a fresh timeout. Inert outside a
            workflow, where the step runs as a normal function call.
    """

    name: Optional[str]
    retries_allowed: bool
    interval_seconds: float
    max_attempts: int
    backoff_rate: float
    should_retry: Optional[Callable[[BaseException], Union[bool, Awaitable[bool]]]]
    preemptible: bool
    timeout_seconds: Optional[float]


DEFAULT_STEP_OPTIONS: StepOptions = {
    "name": None,
    "retries_allowed": False,
    "interval_seconds": 1.0,
    "max_attempts": 3,
    "backoff_rate": 2.0,
    "should_retry": None,
    "preemptible": False,
    "timeout_seconds": None,
}


def normalize_step_options(opts: Optional[StepOptions]) -> StepOptions:
    return {**DEFAULT_STEP_OPTIONS, **(opts or {})}


def _attributes_with_otel_carrier(ctx: DBOSContext) -> Optional[dict[str, Any]]:
    """Fold a PropagateOtelContext carrier into the workflow's persisted attributes.

    Held outside ctx.workflow_attributes so PropagateOtelContext and
    SetWorkflowAttributes nest in either order without clobbering each other.
    """
    if ctx.otel_carrier is None:
        return ctx.workflow_attributes
    attributes = dict(ctx.workflow_attributes) if ctx.workflow_attributes else {}
    attributes[OTEL_CARRIER_ATTRIBUTE] = ctx.otel_carrier
    return attributes


def _assemble_workflow_status(
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
    enqueue_options: Optional[EnqueueOptionsInternal],
    serialization_type: Optional[WorkflowSerializationFormat],
    child_workflow_id: Optional[str] = None,
) -> WorkflowStatusInternal:
    """Build (without persisting) the status row for a new workflow."""
    # If launching child, capture ID before to_thread dispatch, so a concurrent end_workflow() on shutdown can't blank the id read here.
    wfid = (
        child_workflow_id
        if child_workflow_id
        else (
            ctx.workflow_id
            if len(ctx.workflow_id) > 0
            else ctx.id_assigned_for_next_workflow
        )
    )
    if not wfid:
        # An empty id is never valid; fail loudly instead of persisting a row that wedges recovery.
        raise DBOSException(
            "Attempted to initialize a workflow with an empty workflow ID. "
            "The workflow context was likely cleared concurrently (e.g. by "
            "shutdown) while the workflow was being recorded."
        )

    # If we have a class name, the first arg is the instance and do not serialize
    if class_name is not None and class_name != "":
        inputs = {"args": inputs["args"][1:], "kwargs": inputs["kwargs"]}

    sertype: WorkflowSerializationFormat | None = serialization_type
    if sertype is None or sertype == WorkflowSerializationFormat.DEFAULT:
        sertype = ctx.serialization_type
    serargs, serialization = serialize_args(
        inputs["args"], inputs["kwargs"], sertype, dbos._serializer
    )

    # This application owns what it starts; only a debounce acting for another names one here.
    owner_app = (
        enqueue_options["application_name"]
        if enqueue_options is not None
        and enqueue_options["application_name"] is not None
        else GlobalParams.app_name
    )

    # Initialize a workflow status object from the context
    status: WorkflowStatusInternal = {
        "workflow_uuid": wfid,
        "status": (
            WorkflowStatusString.PENDING.value
            if queue is None
            else (
                WorkflowStatusString.DELAYED.value
                if enqueue_options is not None
                and enqueue_options["delay_until_epoch_ms"] is not None
                else WorkflowStatusString.ENQUEUED.value
            )
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
            # Left unset for another application, whose own latest version must run it.
            else (
                GlobalParams.app_version if owner_app == GlobalParams.app_name else None
            )
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
        "inputs": serargs,
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
        "serialization": serialization,
        "delay_until_epoch_ms": (
            enqueue_options["delay_until_epoch_ms"]
            if enqueue_options is not None
            else None
        ),
        "debounce_deadline_epoch_ms": (
            enqueue_options["debounce_deadline_epoch_ms"]
            if enqueue_options is not None
            else None
        ),
        "is_debounced": (
            enqueue_options["is_debounced"] if enqueue_options is not None else False
        ),
        "attributes": _attributes_with_otel_carrier(ctx),
        # schedule_name is only set by the persistent scheduler, which builds
        # the workflow status directly rather than going through this path.
        "schedule_name": None,
        "application_name": owner_app,
    }
    # Consume the attributes from the workflow's context so that workflows
    # started inside this workflow do not inherit them.
    ctx.workflow_attributes = None
    ctx.otel_carrier = None
    return status


def _schedule_workflow_timeout(
    dbos: "DBOS", wfid: str, workflow_deadline_epoch_ms: Optional[int]
) -> None:
    """Cancel wfid once its deadline passes. A None deadline is a no-op."""
    if workflow_deadline_epoch_ms is None:
        return
    deadline_ms = workflow_deadline_epoch_ms

    async def timeout_func() -> None:
        try:
            time_to_wait_sec = (deadline_ms - (time.time() * 1000)) / 1000
            if time_to_wait_sec > 0:
                await asyncio.sleep(time_to_wait_sec)

            await asyncio.to_thread(dbos._sys_db.cancel_workflows, [wfid])
        except Exception as e:
            dbos.logger.warning(f"Exception in timeout task for workflow {wfid}: {e}")

    dbos._background_event_loop.submit_coroutine_nowait(
        timeout_func(), task_set=dbos._timeout_tasks
    )


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
    enqueue_options: Optional[EnqueueOptionsInternal],
    serialization_type: Optional[WorkflowSerializationFormat],
    child_workflow_id: Optional[str] = None,
    child_start_time_ms: Optional[int] = None,
    duplication_policy: Optional[DuplicationPolicy] = None,
) -> tuple[WorkflowStatusInternal, bool, Optional[str]]:
    """Persist this workflow's initial status row.

    Returns the status, whether this caller should execute the workflow, and, under
    duplication_policy 'return-existing', the ID of the workflow that already holds
    this deduplication ID (None if this call created the workflow itself).
    """
    # Assembled once, outside the retry loop below: assembling consumes the
    # context's workflow attributes and trace carrier.
    status = _assemble_workflow_status(
        dbos,
        ctx,
        inputs=inputs,
        wf_name=wf_name,
        class_name=class_name,
        config_name=config_name,
        queue=queue,
        workflow_timeout_ms=workflow_timeout_ms,
        workflow_deadline_epoch_ms=workflow_deadline_epoch_ms,
        enqueue_options=enqueue_options,
        serialization_type=serialization_type,
        child_workflow_id=child_workflow_id,
    )
    wfid = status["workflow_uuid"]

    # Synchronously record the status and inputs for workflows
    while True:
        try:
            wf_status, workflow_deadline_epoch_ms, should_execute = (
                dbos._sys_db.init_workflow(
                    status,
                    owner_xid=str(uuid.uuid4()),
                )
            )
            break
        except DBOSQueueDeduplicatedError as e:
            if duplication_policy == "return-existing":
                existing_id = _deduplicated_workflow_id(dbos, status)
                if existing_id is not None:
                    # The caller records the parent->child mapping, so the error is
                    # deliberately not checkpointed at the parent's function ID here.
                    return status, False, existing_id
                continue
            sererr, serialization = serialize_exception(
                e,
                status["serialization"],
                dbos._serializer,
            )
            if ctx.has_parent():
                result: OperationResultInternal = {
                    "workflow_uuid": ctx.parent_workflow_id,
                    "function_id": ctx.parent_workflow_fid,
                    "function_name": wf_name,
                    "output": None,
                    "error": sererr,
                    "serialization": serialization,
                    "started_at_epoch_ms": (
                        child_start_time_ms
                        if child_start_time_ms is not None
                        else int(time.time() * 1000)
                    ),
                }
                dbos._sys_db.record_operation_result(result)
            raise

    if should_execute:
        _schedule_workflow_timeout(dbos, wfid, workflow_deadline_epoch_ms)

    ctx.workflow_deadline_epoch_ms = workflow_deadline_epoch_ms
    status["workflow_deadline_epoch_ms"] = workflow_deadline_epoch_ms
    status["status"] = wf_status
    return status, should_execute, None


def prepare_enqueued_workflow(
    dbos: "DBOS",
    func: "Callable[..., Any]",
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    queue_name: str,
    workflow_id: str,
    queue_partition_key: Optional[str] = None,
) -> WorkflowStatusInternal:
    """Build (without persisting) an ENQUEUED status row for func on queue_name.

    For batch enqueuers (e.g. the Kafka consumer) that persist many rows in one
    transaction via SystemDatabase.init_workflows. Ignores any ambient DBOS
    context: the workflow ID and enqueue options are passed explicitly.
    """
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
    serialization_type = fi.serialization_type
    if serialization_type is None:
        serialization_type = WorkflowSerializationFormat.DEFAULT

    func = cast("Workflow[P, R]", func.__orig_func)  # type: ignore

    inputs: WorkflowInputs = {"args": args, "kwargs": kwargs}
    enqueue_options = EnqueueOptionsInternal(
        deduplication_id=None,
        priority=None,
        app_version=None,
        queue_partition_key=queue_partition_key,
        delay_until_epoch_ms=None,
        debounce_deadline_epoch_ms=None,
        is_debounced=False,
        application_name=None,
    )
    return _assemble_workflow_status(
        dbos,
        DBOSContext(),  # fresh context: no parent, auth, or attributes
        inputs=inputs,
        wf_name=get_dbos_func_name(func),
        class_name=get_dbos_class_name(fi, func, args),
        config_name=get_config_name(fi, func, args),
        queue=queue_name,
        workflow_timeout_ms=None,
        workflow_deadline_epoch_ms=None,
        enqueue_options=enqueue_options,
        serialization_type=serialization_type,
        child_workflow_id=workflow_id,
    )


def _serialize_exception_for_persistence(
    error: Exception,
    serialization: Optional[str],
    serializer: Serializer,
) -> str:
    """Serialize an exception for persisting a workflow's ERROR outcome.

    Serializing an arbitrary user exception can itself fail (e.g. an unpicklable
    attribute, or a broken ``__str__``/``__repr__``). If it does, fall back to a
    plain, always-serializable Exception so the workflow is still terminalized as
    ERROR instead of being left PENDING and re-executed on every recovery. The
    fallback message is built with ``_safe_str`` so a broken ``__str__`` cannot
    itself raise here and defeat the fallback.
    """
    try:
        return serialize_exception(error, serialization, serializer)[0]
    except Exception:
        fallback = Exception(f"{type(error).__name__}: {_safe_str(error)}")
        return serialize_exception(fallback, serialization, serializer)[0]


def _get_wf_invoke_func(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
    release_active: Callable[[], None] = lambda: None,
) -> Callable[[Callable[[], R]], R]:
    def persist(func: Callable[[], R]) -> R:
        def adopt_recorded_outcome(warning: str) -> R:
            # Deferred, not waited for here: persist runs on a to_thread worker for
            # an async workflow, and the owning execution may take arbitrarily long
            # to record its outcome. fail_if_missing: this run inserted or read the
            # row, so a missing one was deleted -- fail fast with
            # DBOSNonExistentWorkflowError instead of polling forever.
            dbos.logger.warning(warning)
            return cast(
                R,
                _deferred_workflow_result(
                    dbos, status["workflow_uuid"], fail_if_missing=True
                ),
            )

        def not_recorded_warning() -> str:
            return f"Workflow {status['workflow_uuid']} outcome was not recorded: the workflow is no longer owned by this execution. Waiting for the recorded outcome"

        if (
            status["status"] == WorkflowStatusString.ERROR.value
            or status["status"] == WorkflowStatusString.SUCCESS.value
        ):
            dbos.logger.debug(
                f"Workflow {status['workflow_uuid']} is already completed with status {status['status']}"
            )
            # Directly return the result if the workflow is already completed: the
            # row holds it, so a missing one throws rather than polling.
            return cast(
                R,
                _deferred_workflow_result(
                    dbos, status["workflow_uuid"], fail_if_missing=True
                ),
            )
        try:
            if inspect.iscoroutinefunction(func):
                output = dbos._background_event_loop.submit_coroutine(
                    cast(Coroutine[Any, Any, R], func())
                )
            else:
                output = func()

            serval, _serialization = serialize_value_as(
                output, status["serialization"], dbos._serializer
            )
            # Release the active-workflow-ID entry before the outcome becomes
            # durable: once it is visible, a resume can re-dispatch this
            # workflow to this executor, and a stale entry would send that
            # dispatch down the non-owner path to wait forever.
            release_active()
        except DBOSWorkflowConflictIDError:
            # Another execution owns this workflow's step checkpoints.
            release_active()
            return adopt_recorded_outcome(
                f"Aborting duplicate execution of workflow {status['workflow_uuid']}."
            )
        except DBOSWorkflowCancelledError:
            # The run observed its own cancellation. Park the execution.
            release_active()
            return adopt_recorded_outcome(
                f"Workflow {status['workflow_uuid']} was cancelled during execution. Waiting for the recorded outcome"
            )
        except Exception as error:
            error_str = _serialize_exception_for_persistence(
                error, status["serialization"], dbos._serializer
            )
            release_active()
            if not dbos._sys_db.update_workflow_outcome(
                status["workflow_uuid"],
                WorkflowStatusString.ERROR.value,
                error=error_str,
            ):
                # We couldn't update the workflow status: park the execution.
                return adopt_recorded_outcome(not_recorded_warning())
            raise
        if not dbos._sys_db.update_workflow_outcome(
            status["workflow_uuid"],
            WorkflowStatusString.SUCCESS.value,
            output=serval,
        ):
            # We couldn't update the workflow status: park the execution.
            return adopt_recorded_outcome(not_recorded_warning())
        return output

    return persist


class ActiveWorkflowById:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Value is the queue bucket (queue_name, queue_partition_key)
        self._m: dict[str, Optional[Tuple[str, Optional[str]]]] = {}

    def acquire(
        self,
        key: str,
        queue_name: Optional[str] = None,
        queue_partition_key: Optional[str] = None,
    ) -> bool:
        """
        Returns is_owner
        """
        with self._lock:
            if key in self._m:
                return False
            self._m[key] = (
                (queue_name, queue_partition_key) if queue_name is not None else None
            )
            return True

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
        with self._lock:
            return list(self._m.keys())

    def count_for_queue(self, queue_name: str) -> int:
        """
        Count the number of active workflows associated with a given queue,
        across every partition of it.
        """
        with self._lock:
            return sum(
                1
                for bucket in self._m.values()
                if bucket is not None and bucket[0] == queue_name
            )

    def count_for_partition(
        self, queue_name: str, queue_partition_key: Optional[str]
    ) -> int:
        """
        Count the number of active workflows associated with one partition of a
        given queue.
        """
        target = (queue_name, queue_partition_key)
        with self._lock:
            return sum(1 for bucket in self._m.values() if bucket == target)


def _check_required_roles_or_finalize_error(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
    func: "Callable[..., Any]",
    fi: Optional[DBOSFuncInfo],
) -> Optional[str]:
    """Run the required-role check for a workflow about to execute.

    A required-role denial is terminal: the persisted auth context will never
    satisfy the check on a subsequent attempt. Finalize the row as ERROR instead
    of letting the exception escape and leave the workflow stuck PENDING (which,
    on the queue/recovery paths, would be redequeued forever). Only
    DBOSNotAuthorizedError is treated this way; any other (non-deterministic)
    error propagates without finalizing, so it remains retryable.
    """
    try:
        return check_required_roles(func, fi)
    except DBOSNotAuthorizedError as role_error:
        dbos._sys_db.update_workflow_outcome(
            status["workflow_uuid"],
            WorkflowStatusString.ERROR.value,
            error=_serialize_exception_for_persistence(
                role_error, status["serialization"], dbos._serializer
            ),
        )
        raise


def _capture_otel_context() -> "Optional[OtelContext]":
    """Capture the caller's OpenTelemetry context to re-attach on the executor thread.

    Returns None when tracing is off, so opentelemetry -- an optional dependency -- is
    never imported on the workflow execution path unless the user enabled OTLP.
    """
    if dbos_tracer.disable_otlp:
        return None
    from opentelemetry import context as otel_context

    return otel_context.get_current()


def _carrier_otel_context(
    carrier: Optional[dict[str, Any]], submitted_ctx: "Optional[OtelContext]"
) -> "Optional[OtelContext]":
    """Resolve the OpenTelemetry context a workflow's span should parent to.

    A carrier wins over the context captured at submit time, so the workflow lands on the
    same trace whether it runs inline, after a queue handoff, or on recovery. A carrier
    that cannot be read falls back to the submitted context rather than rooting a detached
    trace, and never fails the workflow. None leaves the ambient context in place.
    """
    if dbos_tracer.disable_otlp:
        return None
    if carrier is None:
        return submitted_ctx
    extracted = extract_trace_context(carrier)
    return extracted if extracted is not None else submitted_ctx


def _workflow_otel_context(
    status: WorkflowStatusInternal, submitted_ctx: "Optional[OtelContext]"
) -> "Optional[OtelContext]":
    """Resolve the parent context for a workflow whose status row is already built."""
    return _carrier_otel_context(
        otel_carrier_from_attributes(status.get("attributes")), submitted_ctx
    )


class _UseOtelContext(AbstractContextManager[None, Literal[False]]):
    """Make an OpenTelemetry context ambient for the duration of a workflow's execution.

    ThreadPoolExecutor.submit does not carry contextvars across threads, so a workflow
    started by DBOS.start_workflow would otherwise root a new trace instead of parenting
    to the span active at the call site. asyncio.create_task copies the context already.
    A None context is a no-op, leaving whatever is ambient in place.

    A class rather than a @contextmanager generator so it also satisfies Outcome.also.
    """

    def __init__(self, otel_ctx: "Optional[OtelContext]") -> None:
        self.otel_ctx = otel_ctx
        self.token: "Optional[Token[OtelContext]]" = None

    def __enter__(self) -> None:
        if self.otel_ctx is None:
            return
        from opentelemetry import context as otel_context

        self.token = otel_context.attach(self.otel_ctx)

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        if self.token is not None:
            from opentelemetry import context as otel_context

            otel_context.detach(self.token)
            self.token = None
        return False


def _execute_workflow_wthread(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
    func: "Callable[P, R]",
    ctx: DBOSContext,
    args: tuple[Any],
    kwargs: dict[str, Any],
    otel_ctx: "Optional[OtelContext]" = None,
) -> R:
    attributes: TracedAttributes = {
        "name": get_dbos_func_name(func),
        "operationType": OperationType.WORKFLOW.value,
        "queueName": status.get("queue_name"),
    }
    fi = get_func_info(func)
    with (
        _UseOtelContext(_workflow_otel_context(status, otel_ctx)),
        EnterDBOSWorkflow(attributes, ctx),
    ):
        rr: Optional[str] = _check_required_roles_or_finalize_error(
            dbos, status, func, fi
        )
        with DBOSAssumeRole(rr):
            owned = dbos._active_workflows_set.acquire(
                status["workflow_uuid"],
                status.get("queue_name"),
                status.get("queue_partition_key"),
            )
            # release_active is called both by persist (before the outcome
            # write) and by the finally below. The guard makes the second call
            # a no-op: between the two, a resumed execution of this workflow
            # may have re-acquired the ID, and its entry must not be removed.
            released = False

            def release_active() -> None:
                nonlocal released
                if not released:
                    released = True
                    dbos._active_workflows_set.release(status["workflow_uuid"])

            try:
                if owned:
                    # Resolved here: this path invokes persist outside the Outcome
                    # layer, and a park blocks the thread it already owns.
                    return resolve_deferred(
                        _get_wf_invoke_func(dbos, status, release_active)(
                            functools.partial(func, *args, **kwargs)
                        )
                    )
                else:
                    # Parked on the concurrent execution that owns the active
                    # entry. The row is known to exist (this dispatch inserted
                    # or read it), so a missing row means it was deleted: fail
                    # fast rather than polling forever.
                    output: R = dbos._sys_db.await_workflow_result(
                        status["workflow_uuid"],
                        polling_interval=DEFAULT_POLLING_INTERVAL,
                        fail_if_missing=True,
                    )
                    return output
            except Exception as e:
                # This path runs on the executor thread pool, not the event loop.
                dbos.logger.error(
                    f"Exception encountered in background workflow:", exc_info=e
                )
                raise
            finally:
                if owned:
                    release_active()


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
        "queueName": status.get("queue_name"),
    }
    fi = get_func_info(func)
    # No submitted context: asyncio.create_task already copied the caller's.
    with (
        _UseOtelContext(_workflow_otel_context(status, None)),
        EnterDBOSWorkflow(attributes, ctx),
    ):
        rr: Optional[str] = _check_required_roles_or_finalize_error(
            dbos, status, func, fi
        )
        with DBOSAssumeRole(rr):
            owned = dbos._active_workflows_set.acquire(
                status["workflow_uuid"],
                status.get("queue_name"),
                status.get("queue_partition_key"),
            )
            # release_active is called both by persist (before the outcome
            # write) and by the finally below. The guard makes the second call
            # a no-op: between the two, a resumed execution of this workflow
            # may have re-acquired the ID, and its entry must not be removed.
            released = False

            def release_active() -> None:
                nonlocal released
                if not released:
                    released = True
                    dbos._active_workflows_set.release(status["workflow_uuid"])

            try:
                if owned:
                    result = Pending[R](functools.partial(func, *args, **kwargs)).then(
                        _get_wf_invoke_func(dbos, status, release_active)
                    )
                    return await result()
                else:
                    # Wait on the event loop rather than pinning a to_thread worker in a blocking poll.
                    # Parked on the concurrent execution that owns the active
                    # entry. The row is known to exist (this dispatch inserted
                    # or read it), so a missing row means it was deleted: fail
                    # fast rather than polling forever.
                    return cast(
                        R,
                        await dbos._sys_db.await_workflow_result_async(
                            status["workflow_uuid"],
                            polling_interval=DEFAULT_POLLING_INTERVAL,
                            fail_if_missing=True,
                        ),
                    )
            except Exception as e:
                dbos.logger.error(
                    f"Exception encountered in asynchronous workflow:", exc_info=e
                )
                raise
            finally:
                if owned:
                    release_active()


def execute_dequeued_workflow(
    dbos: "DBOS", status: WorkflowStatusInternal
) -> "WorkflowHandle[Any]":
    """Run a workflow the queue has just claimed, from its persisted status.

    Deliberately skips _init_workflow: the claim already wrote everything it would
    (PENDING, executor, deadline, recovery_attempts) and this status was read back
    from that row, so re-upserting it only rewrites the columns it just read.
    """
    workflow_id = status["workflow_uuid"]
    if not workflow_id.strip():
        # Empty or whitespace workflow IDs are not allowed
        recovery_error = DBOSRecoveryError(
            workflow_id, "Cannot recover a workflow with an empty or whitespace-only ID"
        )
        error_str = _serialize_exception_for_persistence(
            recovery_error, status["serialization"], dbos._serializer
        )
        dbos._sys_db.update_workflow_outcome(
            workflow_id,
            WorkflowStatusString.ERROR.value,
            error=error_str,
        )
        raise recovery_error
    try:
        inputs: WorkflowInputs = deserialize_args(
            status["inputs"], status["serialization"], dbos._serializer
        )
    except Exception as deser_error:
        # Mark workflow as ERROR immediately instead of leaving it PENDING for infinite retry
        error_str = _serialize_exception_for_persistence(
            deser_error, status["serialization"], dbos._serializer
        )
        dbos._sys_db.update_workflow_outcome(
            workflow_id,
            WorkflowStatusString.ERROR.value,
            error=error_str,
        )
        raise
    wf_func = dbos._registry.workflow_info_map.get(status["name"], None)
    if not wf_func:
        raise DBOSWorkflowFunctionNotFoundError(
            workflow_id,
            f"{status['name']} is not a registered workflow function",
        )
    fi = get_func_info(wf_func)
    if fi is None:
        raise DBOSWorkflowFunctionNotFoundError(
            "<NONE>",
            f"{wf_func.__name__} is not a registered workflow function",
        )
    # The claim counted this dispatch; dead-letter the workflow if that exhausted its attempts.
    recovery_attempts = status["recovery_attempts"]
    if (
        status["status"]
        not in (WorkflowStatusString.SUCCESS.value, WorkflowStatusString.ERROR.value)
        and fi.max_recovery_attempts is not None
        and recovery_attempts is not None
        and recovery_attempts > fi.max_recovery_attempts + 1
    ):
        dbos._sys_db.dead_letter_workflows(
            [workflow_id], min_recovery_attempts=recovery_attempts
        )
        raise MaxRecoveryAttemptsExceededError(workflow_id, fi.max_recovery_attempts)
    # Type-coerce arguments whose type is lost to portable JSON serialization.
    using_portable_serialization = status[
        "serialization"
    ] == DBOSPortableJSON.name() or (
        status["serialization"] is None
        and dbos._serializer.name() == DBOSPortableJSON.name()
    )
    if using_portable_serialization and inputs is not None and fi.validate_args is None:
        inputs = coerce_portable_args_to_hints(wf_func, inputs)
    # Run argument validation if configured on the workflow
    if fi.validate_args is not None and inputs is not None:
        try:
            validated_args, validated_kwargs = fi.validate_args(
                inputs["args"], inputs["kwargs"]
            )
            inputs = {"args": validated_args, "kwargs": validated_kwargs}
        except Exception as val_error:
            error_str = _serialize_exception_for_persistence(
                val_error, status["serialization"], dbos._serializer
            )
            dbos._sys_db.update_workflow_outcome(
                workflow_id,
                WorkflowStatusString.ERROR.value,
                error=error_str,
            )
            raise
    # Restore authentication context from the saved workflow status
    recovered_user = status.get("authenticated_user")
    recovered_roles_str = status.get("authenticated_roles")
    recovered_roles = json.loads(recovered_roles_str) if recovered_roles_str else None

    with DBOSContextSetAuth(recovered_user, recovered_roles):
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
            if fi.func_type != DBOSFuncType.Static:
                inputs["args"] = (class_object,) + inputs["args"]

        # Restore the claimed row's ID and trace carrier onto the worker's ambient context.
        with (
            SetWorkflowID(workflow_id),
            SetEnqueueOptions(queue_partition_key=status.get("queue_partition_key")),
            restore_otel_carrier(
                otel_carrier_from_attributes(status.get("attributes"))
            ),
        ):
            # Only a PENDING row can own its outcome, so a row moved on since the claim would run for nothing.
            if status["status"] != WorkflowStatusString.PENDING.value:
                return WorkflowHandlePolling(workflow_id, dbos)

            # Same context start_workflow builds: create_start_workflow_child consumes the
            # ambient SetWorkflowID, so the run adopts the claimed row's ID.
            ctx = DBOSContext.create_start_workflow_child(get_local_dbos_context())
            # Consume the restored carrier so workflows started inside this one do not inherit it.
            ctx.workflow_attributes = None
            ctx.otel_carrier = None
            # The row is authoritative: a workflow enqueued under another serializer must replay under it.
            serialization_type = (
                fi.serialization_type or WorkflowSerializationFormat.DEFAULT
            )
            if status["serialization"] == DBOSPortableJSON.name():
                serialization_type = WorkflowSerializationFormat.PORTABLE
            ctx.serialization_type = serialization_type
            ctx.workflow_deadline_epoch_ms = status["workflow_deadline_epoch_ms"]
            _schedule_workflow_timeout(
                dbos, workflow_id, status["workflow_deadline_epoch_ms"]
            )

            func = cast("Workflow[..., Any]", wf_func.__orig_func)  # type: ignore
            if inspect.iscoroutinefunction(func):

                async def start_workflow_task() -> None:
                    task = asyncio.create_task(
                        _execute_workflow_async(
                            dbos, status, func, ctx, inputs["args"], inputs["kwargs"]
                        )
                    )
                    # The loop keeps only weak references to tasks, and the dequeue path keeps no handle (#710).
                    dbos._workflow_tasks.add(task)
                    task.add_done_callback(dbos._workflow_tasks.discard)
                    # Nothing awaits this task, so mark its exception retrieved (#796).
                    task.add_done_callback(retrieve_future_exception)

                # Onto the event loop. Blocks only until the task is created, so a stopped
                # loop surfaces here; the local concurrency count lags until it acquires.
                dbos._background_event_loop.submit_coroutine(start_workflow_task())
                return WorkflowHandlePolling(workflow_id, dbos)
            else:
                # Onto the thread pool
                future = dbos._executor.submit(
                    cast(Callable[..., Any], _execute_workflow_wthread),
                    dbos,
                    status,
                    func,
                    ctx,
                    inputs["args"],
                    inputs["kwargs"],
                    _capture_otel_context(),
                )
                return WorkflowHandleFuture(workflow_id, future, dbos)


def start_workflow(
    dbos: "DBOS",
    func: "Callable[P, Union[R, Coroutine[Any, Any, R]]]",
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    queue_name: Optional[str] = None,
    execute_workflow: bool = True,
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
    serialization_type = fi.serialization_type
    if serialization_type is None:
        serialization_type = WorkflowSerializationFormat.DEFAULT

    func = cast("Workflow[P, R]", func.__orig_func)  # type: ignore

    inputs: WorkflowInputs = {
        "args": args,
        "kwargs": kwargs,
    }

    local_ctx = get_local_dbos_context()
    _validate_enqueue_only_options(local_ctx, queue_name)
    duplication_policy = _resolve_duplication_policy(local_ctx, queue_name)
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
        delay_until_epoch_ms=(
            local_ctx.delay_until_epoch_ms if local_ctx is not None else None
        ),
        debounce_deadline_epoch_ms=(
            local_ctx.debounce_deadline_epoch_ms if local_ctx is not None else None
        ),
        is_debounced=(local_ctx.is_debounced if local_ctx is not None else False),
        application_name=(
            local_ctx.debounce_application_name if local_ctx is not None else None
        ),
    )
    new_wf_ctx = DBOSContext.create_start_workflow_child(local_ctx)
    new_child_workflow_id = new_wf_ctx.id_assigned_for_next_workflow

    child_start_time = int(time.time() * 1000)
    if new_wf_ctx.has_parent():
        recorded_result = dbos._sys_db.check_operation_execution(
            new_wf_ctx.parent_workflow_id,
            new_wf_ctx.parent_workflow_fid,
            get_dbos_func_name(func),
        )
        if recorded_result and recorded_result["error"]:
            e: Exception = deserialize_exception(
                recorded_result["error"],
                recorded_result["serialization"],
                dbos._sys_db.serializer,
            )
            raise e
        elif recorded_result and recorded_result["child_workflow_id"]:
            return WorkflowHandlePolling(recorded_result["child_workflow_id"], dbos)

    status, should_execute, attached_workflow_id = _init_workflow(
        dbos,
        new_wf_ctx,
        inputs=inputs,
        wf_name=get_dbos_func_name(func),
        class_name=get_dbos_class_name(fi, func, args),
        config_name=get_config_name(fi, func, args),
        queue=queue_name,
        workflow_timeout_ms=workflow_timeout_ms,
        workflow_deadline_epoch_ms=workflow_deadline_epoch_ms,
        enqueue_options=enqueue_options,
        serialization_type=serialization_type,
        child_workflow_id=new_child_workflow_id,
        child_start_time_ms=child_start_time,
        duplication_policy=duplication_policy,
    )
    if attached_workflow_id is not None:
        # Attached to the workflow already holding this deduplication ID. It is
        # recorded as this caller's child below, then polled like any other handle.
        new_child_workflow_id = attached_workflow_id

    if status["serialization"] == DBOSPortableJSON.name():
        serialization_type = WorkflowSerializationFormat.PORTABLE
    new_wf_ctx.serialization_type = serialization_type

    wf_status = status["status"]
    if new_wf_ctx.has_parent():
        dbos._sys_db.record_child_workflow(
            new_wf_ctx.parent_workflow_id,
            new_child_workflow_id,
            new_wf_ctx.parent_workflow_fid,
            get_dbos_func_name(func),
            started_at_epoch_ms=child_start_time,
        )

    if (
        not execute_workflow
        or not should_execute
        or wf_status == WorkflowStatusString.ERROR.value
        or wf_status == WorkflowStatusString.SUCCESS.value
    ):
        return WorkflowHandlePolling(new_child_workflow_id, dbos)

    # Captured on the caller's thread, re-attached inside the executor thread.
    future = dbos._executor.submit(
        cast(Callable[..., R], _execute_workflow_wthread),
        dbos,
        status,
        func,
        new_wf_ctx,
        args,
        kwargs,
        _capture_otel_context(),
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
    serialization_type = fi.serialization_type
    if serialization_type is None:
        serialization_type = WorkflowSerializationFormat.DEFAULT

    func = cast("Workflow[P, R]", func.__orig_func)  # type: ignore

    inputs: WorkflowInputs = {
        "args": args,
        "kwargs": kwargs,
    }

    _validate_enqueue_only_options(local_ctx, queue_name)
    duplication_policy = _resolve_duplication_policy(local_ctx, queue_name)
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
        delay_until_epoch_ms=(
            local_ctx.delay_until_epoch_ms if local_ctx is not None else None
        ),
        debounce_deadline_epoch_ms=(
            local_ctx.debounce_deadline_epoch_ms if local_ctx is not None else None
        ),
        is_debounced=(local_ctx.is_debounced if local_ctx is not None else False),
        application_name=(
            local_ctx.debounce_application_name if local_ctx is not None else None
        ),
    )
    new_child_workflow_id = new_wf_ctx.id_assigned_for_next_workflow

    child_start_time = int(time.time() * 1000)
    if new_wf_ctx.has_parent():
        recorded_result = await asyncio.to_thread(
            dbos._sys_db.check_operation_execution,
            new_wf_ctx.parent_workflow_id,
            new_wf_ctx.parent_workflow_fid,
            get_dbos_func_name(func),
        )
        if recorded_result and recorded_result["error"]:
            e: Exception = deserialize_exception(
                recorded_result["error"],
                recorded_result["serialization"],
                dbos._sys_db.serializer,
            )
            raise e
        elif recorded_result and recorded_result["child_workflow_id"]:
            return WorkflowHandleAsyncPolling(
                recorded_result["child_workflow_id"], dbos
            )

    status, should_execute, attached_workflow_id = await asyncio.to_thread(
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
        enqueue_options=enqueue_options,
        serialization_type=serialization_type,
        child_workflow_id=new_child_workflow_id,
        child_start_time_ms=child_start_time,
        duplication_policy=duplication_policy,
    )
    if attached_workflow_id is not None:
        # Attached to the workflow already holding this deduplication ID. It is
        # recorded as this caller's child below, then polled like any other handle.
        new_child_workflow_id = attached_workflow_id

    if status["serialization"] == DBOSPortableJSON.name():
        serialization_type = WorkflowSerializationFormat.PORTABLE
    new_wf_ctx.serialization_type = serialization_type

    if new_wf_ctx.has_parent():
        await asyncio.to_thread(
            dbos._sys_db.record_child_workflow,
            new_wf_ctx.parent_workflow_id,
            new_child_workflow_id,
            new_wf_ctx.parent_workflow_fid,
            get_dbos_func_name(func),
            started_at_epoch_ms=child_start_time,
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
    inner_task = asyncio.create_task(coro)
    # Hold a strong reference to the workflow task until it completes: the
    # event loop only keeps weak references to tasks, and callers (notably
    # execute_dequeued_workflow on the dequeue path) may discard the returned
    # handle. Without this, a cyclic GC pass can destroy the pending task
    # mid-execution, killing the workflow with GeneratorExit (#710).
    dbos._workflow_tasks.add(inner_task)
    inner_task.add_done_callback(dbos._workflow_tasks.discard)
    # Shield the workflow task from cancellation
    task = asyncio.shield(inner_task)
    # Nothing awaits this future when dequeue/recovery callers discard the handle (#796)
    task.add_done_callback(retrieve_future_exception)
    return WorkflowHandleAsyncTask(new_child_workflow_id, task, dbos)


def _build_enqueue_with_options(
    dbos: "DBOS",
    local_ctx: Optional[DBOSContext],
    new_wf_ctx: DBOSContext,
    options: "EnqueueOptions",
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> tuple[WorkflowStatusInternal, Optional[DuplicationPolicy]]:
    """Build (without persisting) the ENQUEUED row described by these options.

    The options are authoritative; anything they leave unset falls back to the
    ambient DBOS context (SetWorkflowID, SetEnqueueOptions, SetWorkflowTimeout,
    SetWorkflowAttributes, PropagateOtelContext), so this behaves like every
    other enqueue from inside an application. The one deliberate exception is
    app_version, which stays unset unless asked for: the target workflow may
    belong to another executor, so stamping the caller's version would strand
    the row. Unset routes it to the latest registered version instead, which
    callers targeting another binary must account for.
    """
    # An option set to None counts as unset, matching validate_enqueue_options and
    # build_enqueue_status, so a dict built from config cannot defeat the fallbacks.
    resolved = copy.copy(options)
    if resolved.get("workflow_id") is None and new_wf_ctx.id_assigned_for_next_workflow:
        resolved["workflow_id"] = new_wf_ctx.id_assigned_for_next_workflow
    if local_ctx is not None:
        if (
            resolved.get("deduplication_id") is None
            and local_ctx.deduplication_id is not None
        ):
            resolved["deduplication_id"] = local_ctx.deduplication_id
        if (
            resolved.get("duplication_policy") is None
            and local_ctx.duplication_policy is not None
        ):
            resolved["duplication_policy"] = local_ctx.duplication_policy
        if resolved.get("priority") is None and local_ctx.priority is not None:
            resolved["priority"] = local_ctx.priority
        if resolved.get("app_version") is None and local_ctx.app_version is not None:
            resolved["app_version"] = local_ctx.app_version
        if (
            resolved.get("queue_partition_key") is None
            and local_ctx.queue_partition_key is not None
        ):
            resolved["queue_partition_key"] = local_ctx.queue_partition_key
        if (
            resolved.get("authenticated_user") is None
            and local_ctx.authenticated_user is not None
        ):
            resolved["authenticated_user"] = local_ctx.authenticated_user
        if (
            resolved.get("authenticated_roles") is None
            and local_ctx.authenticated_roles is not None
        ):
            resolved["authenticated_roles"] = local_ctx.authenticated_roles
    if (
        resolved.get("queue_partition_key") is not None
        and resolved.get("deduplication_id") is not None
    ):
        raise DBOSException("Deduplication is not supported for partitioned queues")

    _, status = build_enqueue_status(resolved, dbos._serializer, args, kwargs)

    if status["application_name"] is None:
        # No explicit target, so this application owns it.
        status["application_name"] = GlobalParams.app_name
    status["app_id"] = new_wf_ctx.app_id
    status["parent_workflow_id"] = (
        new_wf_ctx.parent_workflow_id if new_wf_ctx.has_parent() else None
    )
    if resolved.get("workflow_timeout") is None:
        # Inherit an explicit SetWorkflowTimeout, else the parent's propagated deadline.
        status["workflow_timeout_ms"], status["workflow_deadline_epoch_ms"] = (
            _get_timeout_deadline(local_ctx, resolved["queue_name"])
        )
    if (
        resolved.get("delay_seconds") is None
        and local_ctx is not None
        and local_ctx.delay_until_epoch_ms is not None
    ):
        status["delay_until_epoch_ms"] = local_ctx.delay_until_epoch_ms
        status["status"] = WorkflowStatusString.DELAYED.value
    ambient_attributes = _attributes_with_otel_carrier(new_wf_ctx)
    if ambient_attributes is not None:
        # Merge rather than replace: ambient attributes survive, but options win per
        # key, including the otel carrier an ambient PropagateOtelContext set.
        status["attributes"] = {**ambient_attributes, **(status["attributes"] or {})}
    return status, resolved.get("duplication_policy")


def _check_recorded_enqueue(
    dbos: "DBOS", new_wf_ctx: DBOSContext, wf_name: str
) -> Optional[str]:
    """Return the child ID this enqueue already recorded, or None if it is new.

    Raises the recorded error if the original call failed. Runs before the row is
    built so a replay re-validates and re-serializes nothing it already checkpointed.
    """
    if not new_wf_ctx.has_parent():
        return None
    recorded_result = dbos._sys_db.check_operation_execution(
        new_wf_ctx.parent_workflow_id,
        new_wf_ctx.parent_workflow_fid,
        wf_name,
    )
    if recorded_result and recorded_result["error"]:
        e: Exception = deserialize_exception(
            recorded_result["error"],
            recorded_result["serialization"],
            dbos._sys_db.serializer,
        )
        raise e
    elif recorded_result and recorded_result["child_workflow_id"]:
        return recorded_result["child_workflow_id"]
    return None


def _deduplicated_workflow_id(
    dbos: "DBOS", status: WorkflowStatusInternal
) -> Optional[str]:
    """The workflow already holding this row's deduplication ID, or None if it is unheld.

    None means the holder released the ID between the insert that collided and this
    lookup (it completed or was cancelled), so the caller may retry its insert.
    """
    queue_name, dedup_id = status["queue_name"], status["deduplication_id"]
    assert queue_name is not None and dedup_id is not None
    return dbos._sys_db.get_deduplicated_workflow(queue_name, dedup_id)


def _persist_enqueue_with_options(
    dbos: "DBOS",
    new_wf_ctx: DBOSContext,
    status: WorkflowStatusInternal,
    duplication_policy: Optional[DuplicationPolicy] = None,
) -> str:
    """Persist the enqueue, recording it as a child of the calling workflow.

    Under duplication_policy 'return-existing', returns the ID of the workflow that
    already holds this deduplication ID instead of enqueueing a new one.
    """
    wf_name = status["name"]
    workflow_id = status["workflow_uuid"]
    child_start_time = int(time.time() * 1000)
    while True:
        try:
            dbos._sys_db.init_workflow(
                status,
                owner_xid=None,
            )
            break
        except DBOSQueueDeduplicatedError as e:
            if duplication_policy == "return-existing":
                existing_id = _deduplicated_workflow_id(dbos, status)
                if existing_id is not None:
                    # Recorded as this caller's child below, so the error is
                    # deliberately not checkpointed at the parent's function ID.
                    workflow_id = existing_id
                    break
                continue
            sererr, serialization = serialize_exception(
                e,
                status["serialization"],
                dbos._serializer,
            )
            if new_wf_ctx.has_parent():
                result: OperationResultInternal = {
                    "workflow_uuid": new_wf_ctx.parent_workflow_id,
                    "function_id": new_wf_ctx.parent_workflow_fid,
                    "function_name": wf_name,
                    "output": None,
                    "error": sererr,
                    "serialization": serialization,
                    "started_at_epoch_ms": child_start_time,
                }
                dbos._sys_db.record_operation_result(result)
            raise

    if new_wf_ctx.has_parent():
        dbos._sys_db.record_child_workflow(
            new_wf_ctx.parent_workflow_id,
            workflow_id,
            new_wf_ctx.parent_workflow_fid,
            wf_name,
            started_at_epoch_ms=child_start_time,
        )
    return workflow_id


def enqueue_workflow_with_options(
    dbos: "DBOS",
    options: "EnqueueOptions",
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> "WorkflowHandle[Any]":
    local_ctx = get_local_dbos_context()
    new_wf_ctx = DBOSContext.create_start_workflow_child(local_ctx)
    recorded_child_id = _check_recorded_enqueue(
        dbos, new_wf_ctx, options["workflow_name"]
    )
    if recorded_child_id is not None:
        return WorkflowHandlePolling(recorded_child_id, dbos)
    status, duplication_policy = _build_enqueue_with_options(
        dbos, local_ctx, new_wf_ctx, options, args, kwargs
    )
    workflow_id = _persist_enqueue_with_options(
        dbos, new_wf_ctx, status, duplication_policy
    )
    return WorkflowHandlePolling(workflow_id, dbos)


async def enqueue_workflow_with_options_async(
    dbos: "DBOS",
    local_ctx: Optional[DBOSContext],
    new_wf_ctx: DBOSContext,
    options: "EnqueueOptions",
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> "WorkflowHandleAsync[Any]":
    recorded_child_id = await asyncio.to_thread(
        _check_recorded_enqueue, dbos, new_wf_ctx, options["workflow_name"]
    )
    if recorded_child_id is not None:
        return WorkflowHandleAsyncPolling(recorded_child_id, dbos)
    status, duplication_policy = _build_enqueue_with_options(
        dbos, local_ctx, new_wf_ctx, options, args, kwargs
    )
    workflow_id = await asyncio.to_thread(
        _persist_enqueue_with_options, dbos, new_wf_ctx, status, duplication_policy
    )
    return WorkflowHandleAsyncPolling(workflow_id, dbos)


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
    *,
    serialization_type: WorkflowSerializationFormat = WorkflowSerializationFormat.DEFAULT,
    validate_args: Optional["ValidateArgsCallable"] = None,
) -> Callable[P, R]:
    func.__orig_func = func  # type: ignore

    fi = get_or_create_func_info(func)
    fi.max_recovery_attempts = max_recovery_attempts
    fi.serialization_type = serialization_type
    fi.validate_args = validate_args

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
        # Freeze the child id before to_thread dispatch: a concurrent end_workflow() on shutdown could blank newwfctx.workflow_id mid-registration.
        child_wfid = newwfctx.id_assigned_for_next_workflow
        parent_wfid = newwfctx.parent_workflow_id
        parent_fid = newwfctx.parent_workflow_fid
        resctx: Optional[DBOSContext] = None
        if cctx is not None and cctx.is_workflow():
            resctx = cctx.snapshot_step_ctx(reserve_sleep_id=False)
        workflow_timeout_ms, workflow_deadline_epoch_ms = _get_timeout_deadline(
            cctx, queue=None
        )

        wfOutcome = Outcome[R].make(functools.partial(func, *args, **kwargs))

        workflow_id = None
        # Holds the initialized status so the invoke step can be built once the workflow is cleared to execute.
        init_status: dict[str, WorkflowStatusInternal] = {}
        # Hoisted out of record_get_result: Pending.then invokes it only after awaiting the body.
        get_result_start_time = int(time.time() * 1000)

        def check_and_init() -> Union[NoResult, "DeferredResult[R]", R]:
            """Initialize the workflow row, returning a deferred wait for an existing workflow's result to skip re-running its body, or NoResult to run it."""
            nonlocal workflow_id
            workflow_id = child_wfid

            child_start_time = int(time.time() * 1000)
            if parent_wfid:
                r = dbos._sys_db.check_operation_execution(
                    parent_wfid,
                    parent_fid,
                    get_dbos_func_name(func),
                )
                if r and r["error"]:
                    raise deserialize_exception(
                        r["error"], r["serialization"], dbos._sys_db.serializer
                    )
                elif r and r["child_workflow_id"]:
                    return _deferred_workflow_result(dbos, r["child_workflow_id"])

            status, should_execute, _ = _init_workflow(
                dbos,
                newwfctx,
                inputs=inputs,
                wf_name=get_dbos_func_name(func),
                class_name=get_dbos_class_name(fi, func, args),
                config_name=get_config_name(fi, func, args),
                queue=None,
                workflow_timeout_ms=workflow_timeout_ms,
                workflow_deadline_epoch_ms=workflow_deadline_epoch_ms,
                enqueue_options=None,
                serialization_type=fi.serialization_type,
                child_workflow_id=child_wfid,
                child_start_time_ms=child_start_time,
            )

            # The body writes events, streams, and messages in this format; without it a directly
            # invoked workflow would write them under the default while its own row says otherwise.
            serialization_type = fi.serialization_type
            if serialization_type is None:
                serialization_type = WorkflowSerializationFormat.DEFAULT
            # The row is authoritative: a workflow enqueued under another serializer must replay under it.
            if status["serialization"] == DBOSPortableJSON.name():
                serialization_type = WorkflowSerializationFormat.PORTABLE
            newwfctx.serialization_type = serialization_type

            # TODO: maybe modify the parameters if they've been changed by `_init_workflow`
            dbos.logger.debug(
                f"Running workflow, id: {child_wfid}, name: {get_dbos_func_name(func)}"
            )

            if parent_wfid:
                dbos._sys_db.record_child_workflow(
                    parent_wfid,
                    child_wfid,
                    parent_fid,
                    get_dbos_func_name(func),
                    started_at_epoch_ms=child_start_time,
                )

            if should_execute:
                init_status["status"] = status
                return NoResult()
            # Already completed or running elsewhere: wait for its result instead of re-running the body.
            dbos.logger.debug(
                f"Workflow {status['workflow_uuid']} already run with status {status['status']}"
            )
            return _deferred_workflow_result(dbos, status["workflow_uuid"])

        def get_wf_invoke() -> Callable[[Callable[[], R]], R]:
            return _get_wf_invoke_func(dbos, init_status["status"])

        def record_get_result(func: Callable[[], R]) -> R:
            """
            If a child workflow is invoked synchronously, this records the implicit "getResult" where the
            parent retrieves the child's output. It executes in the CALLER'S context, not the workflow's.
            """
            try:
                r = func()
            except Exception as e:
                serialized_e, serialization = serialize_exception(
                    e, None, dbos._serializer
                )
                assert workflow_id is not None
                dbos._sys_db.record_get_result(
                    workflow_id,
                    None,
                    serialized_e,
                    serialization,
                    resctx,
                    started_at_epoch_ms=get_result_start_time,
                )
                raise
            serialized_r, serialization = serialize_value(r, None, dbos._serializer)
            assert workflow_id is not None
            dbos._sys_db.record_get_result(
                workflow_id,
                serialized_r,
                None,
                serialization,
                resctx,
                started_at_epoch_ms=get_result_start_time,
            )
            return r

        outcome = (
            wfOutcome.wrap(get_wf_invoke, dbos=dbos)
            .intercept(check_and_init, dbos=dbos)
            .also(DBOSAssumeRole(rr))
            .also(EnterDBOSWorkflow(attributes, newwfctx))
            # Outside EnterDBOSWorkflow so the carrier is active when the span is created,
            # and read now: check_and_init consumes the carrier off newwfctx. Inside
            # record_get_result, which belongs to the caller's trace, not the workflow's.
            .also(_UseOtelContext(_carrier_otel_context(newwfctx.otel_carrier, None)))
            .then(record_get_result, dbos=dbos)
        )
        return outcome()  # type: ignore

    return _mark_coroutine(wrapper) if inspect.iscoroutinefunction(func) else wrapper


def decorate_workflow(
    reg: "DBOSRegistry",
    name: Optional[str],
    max_recovery_attempts: Optional[int],
    *,
    serialization_type: Optional[WorkflowSerializationFormat] = None,
    validate_args: Optional["ValidateArgsCallable"] = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    if serialization_type is None:
        serialization_type = WorkflowSerializationFormat.DEFAULT

    def _workflow_decorator(func: Callable[P, R]) -> Callable[P, R]:
        resolved_validate_args = validate_args
        # If pydantic_args_validator sentinel is passed, build a real validator
        # from the function's type hints at decoration time
        if resolved_validate_args is not None:
            from ._validation import pydantic_args_validator

            if resolved_validate_args is pydantic_args_validator:
                from ._validation import make_pydantic_args_validator

                resolved_validate_args = make_pydantic_args_validator(func)
        wrapped_func = workflow_wrapper(
            reg,
            func,
            max_recovery_attempts,
            serialization_type=serialization_type,
            validate_args=resolved_validate_args,
        )
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

            try:
                asyncio.get_running_loop()
            except RuntimeError:
                pass
            else:
                dbosreg.dbos.logger.warning(
                    f"Transaction {transaction_name} was called while an event loop is running. Invoke transactions from an async context using asyncio.to_thread to avoid blocking the event loop."
                )

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
                    step_start_time = int(time.time() * 1000)
                    # Check if the step record for this transaction exists
                    recorded_step_output = dbos._sys_db.check_operation_execution(
                        ctx.workflow_id, ctx.function_id, transaction_name
                    )
                    if recorded_step_output:
                        dbos.logger.debug(
                            f"Replaying transaction, id: {ctx.function_id}, name: {attributes['name']}"
                        )
                        if recorded_step_output["error"]:
                            step_error: Exception = deserialize_exception(
                                recorded_step_output["error"],
                                recorded_step_output["serialization"],
                                dbos._serializer,
                            )
                            raise step_error
                        elif recorded_step_output["output"]:
                            return deserialize_value(
                                recorded_step_output["output"],
                                recorded_step_output["child_workflow_id"],
                                dbos._serializer,
                            )
                        else:
                            raise Exception("Output and error are both None")

                    txn_output: TransactionResultInternal = {
                        "workflow_uuid": ctx.workflow_id,
                        "function_id": ctx.function_id,
                        "output": None,
                        "error": None,
                        "serialization": None,
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
                        "serialization": None,
                        "started_at_epoch_ms": step_start_time,
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
                                            deserialize_exception(
                                                recorded_output["error"],
                                                recorded_output["serialization"],
                                                dbos._serializer,
                                            )
                                        )
                                        has_recorded_error = True
                                        step_output["error"] = recorded_output["error"]
                                        step_output["serialization"] = recorded_output[
                                            "serialization"
                                        ]
                                        dbos._sys_db.record_operation_result(
                                            step_output
                                        )
                                        raise deserialized_error
                                    elif recorded_output["output"]:
                                        step_output["output"] = recorded_output[
                                            "output"
                                        ]
                                        step_output["serialization"] = recorded_output[
                                            "serialization"
                                        ]
                                        dbos._sys_db.record_operation_result(
                                            step_output
                                        )
                                        return deserialize_value(
                                            recorded_output["output"],
                                            recorded_output["serialization"],
                                            dbos._serializer,
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
                                serialized_r, serialization = serialize_value(
                                    output, None, dbos._serializer
                                )
                                txn_output["output"] = serialized_r
                                txn_output["serialization"] = serialization
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
                                serialized_e, serialization = serialize_exception(
                                    txn_error, None, dbos._serializer
                                )
                                step_output["error"] = txn_output["error"] = (
                                    serialized_e
                                )
                                step_output["serialization"] = txn_output[
                                    "serialization"
                                ] = serialization
                                dbos._app_db.record_transaction_error(txn_output)
                                dbos._sys_db.record_operation_result(step_output)
            serialized_r, serialization = serialize_value(
                output, None, dbos._serializer
            )
            step_output["output"] = serialized_r
            step_output["serialization"] = serialization
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


_PREEMPTIBLE_POLL_INTERVAL_SEC = 1.0


async def _supervise_step(
    dbos: "DBOS",
    workflow_id: str,
    func: Callable[..., Coroutine[Any, Any, R]],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    step_name: str,
    preemptible: bool,
    timeout_seconds: Optional[float],
) -> R:
    """Run an async step under the watchdogs its options ask for, deciding in
    one place whether it completed, was preempted, or blew its deadline."""

    async def poll_for_cancellation() -> bool:
        """Return True once the workflow is observed CANCELLED. Never cancels
        anything itself: the supervisor owns that decision."""
        while True:
            await asyncio.sleep(_PREEMPTIBLE_POLL_INTERVAL_SEC)
            try:
                status = await asyncio.to_thread(
                    dbos._sys_db.get_workflow_status, workflow_id
                )
            except Exception as e:
                dbos.logger.warning(
                    f"Error polling status for preemptible step in workflow {workflow_id}: {e}"
                )
                continue
            if (
                status is not None
                and status["status"] == WorkflowStatusString.CANCELLED.value
            ):
                return True

    loop = asyncio.get_running_loop()
    step_task: asyncio.Task[R] = asyncio.create_task(func(*args, **kwargs))
    poller_task: Optional[asyncio.Task[bool]] = (
        asyncio.create_task(poll_for_cancellation()) if preemptible else None
    )
    deadline = None if timeout_seconds is None else loop.time() + timeout_seconds
    abort: Optional[BaseException] = None
    try:
        watched: set[asyncio.Task[Any]] = {step_task}
        if poller_task is not None:
            watched.add(poller_task)
        while abort is None:
            remaining = None if deadline is None else max(0.0, deadline - loop.time())
            done, _ = await asyncio.wait(
                watched, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
            # A step that finished keeps its outcome, whatever else happened.
            if step_task in done:
                return step_task.result()
            if poller_task is not None and poller_task in done:
                watched.discard(poller_task)
                observed = (
                    not poller_task.cancelled() and poller_task.exception() is None
                )
                # A cancellation outranks a deadline reached at the same moment:
                # not checkpointing a cancelled workflow's step is the safer error.
                if observed and poller_task.result():
                    abort = DBOSWorkflowCancelledError(
                        f"Workflow {workflow_id} is cancelled. Aborting preemptible step."
                    )
                continue
            assert timeout_seconds is not None
            abort = DBOSStepTimeoutError(step_name, timeout_seconds)
        # Stop the step and let its cleanup finish before surfacing the reason.
        # wait_for would hand back a suppressed-cancel body's value (3.10-3.13).
        step_task.cancel()
        await asyncio.wait({step_task})
        outcome = None if step_task.cancelled() else step_task.exception()
        if outcome is not None and not isinstance(outcome, Exception):
            raise outcome
        raise abort from outcome
    finally:
        # Don't leak either task if the outer coroutine was itself cancelled,
        # and retrieve any outcome so asyncio doesn't log it as unhandled.
        for task in (step_task, poller_task):
            if task is None:
                continue
            if not task.done():
                task.cancel()
                try:
                    await task
                except BaseException:
                    pass
            elif not task.cancelled():
                task.exception()


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
    should_retry: Optional[
        Callable[[BaseException], Union[bool, Awaitable[bool]]]
    ] = None,
    preemptible: bool = False,
    timeout_seconds: Optional[float] = None,
) -> R | Coroutine[Any, Any, R]:
    if (
        should_retry is not None
        and inspect.iscoroutinefunction(should_retry)
        and not inspect.iscoroutinefunction(func)
    ):
        raise DBOSException(
            f"Step {step_name} is sync but should_retry is async. "
            f"Use an async step to pair with an async validator."
        )
    if preemptible and not inspect.iscoroutinefunction(func):
        raise DBOSException(
            f"Step {step_name} is sync but preemptible=True. "
            f"Preemption is only supported for async steps."
        )
    if timeout_seconds is not None:
        if not inspect.iscoroutinefunction(func):
            raise DBOSException(
                f"Step {step_name} is sync but timeout_seconds is set. "
                f"Step timeouts are only supported for async steps."
            )
        if not (timeout_seconds > 0 and math.isfinite(timeout_seconds)):
            # `<= 0` would admit NaN, which silently disables the timeout.
            raise DBOSException(
                f"Step {step_name} has timeout_seconds={timeout_seconds}. "
                f"Step timeouts must be positive and finite."
            )

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
            "serialization": None,
            "started_at_epoch_ms": step_start_time,
        }

        try:
            output = func()
        except DBOSWorkflowCancelledError:
            # The step was preempted by workflow cancellation. Don't record
            # an outcome — let the step be re-run on resume.
            raise
        except Exception as error:
            serialized_e, serialization = serialize_exception(
                error, None, dbos._serializer
            )
            step_output["error"] = serialized_e
            step_output["serialization"] = serialization
            dbos._sys_db.record_operation_result(step_output)
            raise
        serialized_r, serialization = serialize_value(output, None, dbos._serializer)
        step_output["output"] = serialized_r
        step_output["serialization"] = serialization
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
                deserialized_error: Exception = deserialize_exception(
                    recorded_output["error"],
                    recorded_output["serialization"],
                    dbos._serializer,
                )
                raise deserialized_error
            elif recorded_output["output"] is not None:
                return cast(
                    R,
                    deserialize_value(
                        recorded_output["output"],
                        recorded_output["serialization"],
                        dbos._serializer,
                    ),
                )
            else:
                raise Exception("Output and error are both None")
        else:
            dbos.logger.debug(
                f"Running step, id: {ctx.function_id}, name: {attributes['name']}"
            )
            return NoResult()

    if preemptible or timeout_seconds is not None:
        async_func = cast(Callable[..., Coroutine[Any, Any, R]], func)
        step_partial: Callable[[], Union[R, Coroutine[Any, Any, R]]] = (
            functools.partial(
                _supervise_step,
                dbos,
                step_ctx.workflow_id,
                async_func,
                args,
                kwargs,
                step_name=step_name,
                preemptible=preemptible,
                timeout_seconds=timeout_seconds,
            )
        )
    else:
        step_partial = functools.partial(func, *args, **kwargs)
    # The supervisor sits inside the retry layer, so each attempt gets a fresh
    # deadline and retry sleeps are not charged against it.
    stepOutcome = Outcome[R].make(step_partial)
    if retries_allowed:
        stepOutcome = stepOutcome.retry(
            max_attempts,
            on_exception,
            lambda i, e: DBOSMaxStepRetriesExceeded(step_name, i, e),
            should_retry,
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
            should_retry=options["should_retry"],
            preemptible=options["preemptible"],
            timeout_seconds=options["timeout_seconds"],
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

        def invoke() -> Union[R, Coroutine[Any, Any, R]]:
            return invoke_step(
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
                should_retry=options["should_retry"],
                preemptible=options["preemptible"],
                timeout_seconds=options["timeout_seconds"],
            )

        if inspect.iscoroutinefunction(func):
            # Async step: build the Pending outcome on the loop and await it.
            return await cast(Coroutine[Any, Any, R], invoke())
        else:
            # Sync step: run it off-loop so its DB checkpoints, body, and retry sleep don't block the loop.
            return await asyncio.to_thread(lambda: cast(R, invoke()))
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
    should_retry: Optional[
        Callable[[BaseException], Union[bool, Awaitable[bool]]]
    ] = None,
    preemptible: bool = False,
    timeout_seconds: Optional[float] = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        if preemptible and not inspect.iscoroutinefunction(func):
            raise DBOSException(
                f"Step {name or func.__qualname__} is sync but preemptible=True. "
                f"Preemption is only supported for async steps."
            )
        if timeout_seconds is not None:
            if not inspect.iscoroutinefunction(func):
                raise DBOSException(
                    f"Step {name or func.__qualname__} is sync but timeout_seconds is set. "
                    f"Step timeouts are only supported for async steps."
                )
            if not (timeout_seconds > 0 and math.isfinite(timeout_seconds)):
                raise DBOSException(
                    f"Step {name or func.__qualname__} has timeout_seconds={timeout_seconds}. "
                    f"Step timeouts must be positive and finite."
                )

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
                        should_retry=should_retry,
                        preemptible=preemptible,
                        timeout_seconds=timeout_seconds,
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


def send_bulk(
    dbos: "DBOS",
    cur_ctx: Optional["DBOSContext"],
    messages: List[SendMessage],
    *,
    serialization_type: Optional[WorkflowSerializationFormat],
    function_name: str,
    span_name: str,
    send_to_forks: bool,
) -> None:
    """Send one or more messages, optionally as a step within a workflow.

    Underlies both `DBOS.send` (a single message) and `DBOS.send_bulk` (many),
    which differ only in the `function_name`/`span_name` they record. When
    `send_to_forks` is set, each message also reaches every workflow recursively
    forked from its destination.
    """
    if (
        serialization_type is None
        or serialization_type == WorkflowSerializationFormat.DEFAULT
    ):
        serialization_type = (
            cur_ctx.serialization_type
            if cur_ctx is not None
            else WorkflowSerializationFormat.DEFAULT
        )

    if cur_ctx and cur_ctx.is_workflow():
        # Inside a workflow, the entire send is recorded as a single step.
        attributes: TracedAttributes = {
            "name": span_name,
        }
        with EnterDBOSStepCtx(attributes, cur_ctx) as ctx:
            dbos._sys_db.send_bulk(
                messages,
                serialization_type=serialization_type,
                workflow_id=ctx.workflow_id,
                function_id=ctx.curr_step_function_id,
                function_name=function_name,
                send_to_forks=send_to_forks,
            )
    else:
        dbos._sys_db.send_bulk(
            messages,
            serialization_type=serialization_type,
            workflow_id=None,
            function_id=None,
            function_name=function_name,
            send_to_forks=send_to_forks,
        )


def set_event(
    dbos: "DBOS",
    cur_ctx: Optional["DBOSContext"],
    key: str,
    value: Any,
    *,
    serialization_type: WorkflowSerializationFormat,
) -> None:
    if (
        serialization_type is None
        or serialization_type == WorkflowSerializationFormat.DEFAULT
    ):
        serialization_type = (
            cur_ctx.serialization_type
            if cur_ctx is not None
            else WorkflowSerializationFormat.DEFAULT
        )

    if cur_ctx is not None:
        if cur_ctx.is_workflow():
            # If called from a workflow function, run as a step
            attributes: TracedAttributes = {
                "name": "set_event",
            }
            with EnterDBOSStepCtx(attributes, cur_ctx) as ctx:
                dbos._sys_db.set_event_from_workflow(
                    ctx.workflow_id,
                    ctx.curr_step_function_id,
                    key,
                    value,
                    serialization_type=serialization_type,
                )
        elif cur_ctx.is_step():
            dbos._sys_db.set_event_from_step(
                cur_ctx.workflow_id,
                cur_ctx.curr_step_function_id,
                key,
                value,
                serialization_type=serialization_type,
            )
        else:
            raise DBOSException(
                "set_event() must be called from within a workflow or step"
            )
    else:
        raise DBOSException("set_event() must be called from within a workflow or step")


def record_sleep(
    dbos: "DBOS", cur_ctx: Optional["DBOSContext"], seconds: float
) -> float:
    if cur_ctx is not None:
        # Must call it within a workflow
        assert cur_ctx.is_workflow(), "sleep() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "sleep",
        }
        with EnterDBOSStepCtx(attributes, cur_ctx) as ctx:
            return dbos._sys_db.record_sleep(
                ctx.workflow_id,
                ctx.curr_step_function_id,
                seconds,
                project_completion_time=True,
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("sleep() must be called from within a workflow")


def write_stream(
    dbos: "DBOS",
    step_ctx: Optional["DBOSContext"],
    key: str,
    value: Any,
    *,
    serialization_type: WorkflowSerializationFormat,
) -> None:
    if (
        serialization_type is None
        or serialization_type == WorkflowSerializationFormat.DEFAULT
    ):
        serialization_type = (
            step_ctx.serialization_type
            if step_ctx is not None
            else WorkflowSerializationFormat.DEFAULT
        )

    if step_ctx is not None:
        # Must call it within a workflow
        if step_ctx.is_workflow():
            attributes: TracedAttributes = {
                "name": "write_stream",
            }
            with EnterDBOSStepCtx(attributes, step_ctx) as ctx:
                dbos._sys_db.write_stream_from_workflow(
                    ctx.workflow_id,
                    ctx.function_id,
                    key,
                    value,
                    serialization_type=serialization_type,
                )
        elif step_ctx.is_step():
            dbos._sys_db.write_stream_from_step(
                step_ctx.workflow_id,
                step_ctx.function_id,
                key,
                value,
                serialization_type=serialization_type,
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
        # Must call it within a workflow or step
        if step_ctx.is_workflow():
            attributes: TracedAttributes = {
                "name": "close_stream",
            }
            with EnterDBOSStepCtx(attributes, step_ctx) as ctx:
                dbos._sys_db.close_stream_from_workflow(
                    ctx.workflow_id, ctx.function_id, key
                )
        elif step_ctx.is_step():
            dbos._sys_db.close_stream_from_step(
                step_ctx.workflow_id, step_ctx.function_id, key
            )
        else:
            raise DBOSException(
                "close_stream() must be called from within a workflow or step"
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException(
            "close_stream() must be called from within a workflow or step"
        )


_READ_STREAM_FUNCTION_NAME = "DBOS.readStream"
# How often a reader re-checks its own workflow for cancellation while it reads.
_STREAM_CANCEL_CHECK_INTERVAL_SEC = 1.0
_READ_STREAM_OFFSET_FUNCTION_NAME = "DBOS.readStreamOffset"

# Returned by _StreamReadCheckpoint.replay when a step has no recorded result and must be read live.
_no_recorded_value = object()


class _StreamReadCheckpoint:
    """Checkpoints the values a workflow reads from a stream, one step per value.

    A replayed reader re-yields its recorded values instead of re-reading a stream
    that may have advanced since, so it observes the sequence it originally did.
    Reads from a step, from a client, or from outside a workflow are not recorded.
    """

    def __init__(
        self,
        sys_db: "SystemDatabase",
        key: str,
        function_name: str,
        enabled: bool,
    ) -> None:
        self._sys_db = sys_db
        self._key = key
        self._function_name = function_name
        # A client reads its own system database, so it must never bind to the ambient workflow.
        self._enabled = enabled
        self._ctx: Optional[DBOSContext] = None
        self._bound = False
        self._last_cancel_check = time.time()
        # Function ids are allocated in order, so the first miss proves every later one misses too.
        self._replaying = True
        self._started_at_epoch_ms = 0
        # Whether this reader currently holds a reserved step that is not yet recorded.
        self._in_flight = False

    def begin(self) -> Optional[DBOSContext]:
        """Reserve the step that records the next value, or None if this read is not checkpointed."""
        if not self._enabled:
            return None
        if not self._bound:
            self._bound = True
            ctx = get_local_dbos_context()
            # Bind once and hold the context: a nested step call swaps the contextvar out from under us.
            if ctx is not None and ctx.is_workflow():
                self._ctx = ctx
        if self._ctx is None:
            return None
        # Sequential reads record before they yield and so never overlap; an overlap means the reads are concurrent and their step ids depend on scheduling. Every read shares one function name, so the step-name guard would not catch the resulting swap on replay.
        if self._ctx.active_stream_reads > 0:
            raise DBOSStreamNondeterminismError(self._ctx.workflow_id, self._key)
        self._ctx.active_stream_reads += 1
        self._in_flight = True
        # Started when the read began, so the recorded step spans the wait for the value.
        self._started_at_epoch_ms = int(time.time() * 1000)
        return self._ctx.snapshot_step_ctx()

    def end(self) -> None:
        """Release the reserved step once its outcome is settled. Idempotent."""
        if not self._in_flight:
            return
        self._in_flight = False
        if self._ctx is not None:
            self._ctx.active_stream_reads -= 1

    def replay(self, step_ctx: Optional[DBOSContext]) -> Any:
        """Return the value recorded for this step, or _no_recorded_value to read it live."""
        if step_ctx is None or not self._replaying:
            return _no_recorded_value
        recorded = self._sys_db.check_operation_execution(
            step_ctx.workflow_id, step_ctx.function_id, self._function_name
        )
        if recorded is None:
            self._replaying = False
            return _no_recorded_value
        dbos_logger.debug(
            f"Replaying readStream, id: {step_ctx.function_id}, key: {self._key}"
        )
        if recorded["error"] is not None:
            # A timeout is the only failure recorded here; a replay re-raises it without waiting.
            raise deserialize_exception(
                recorded["error"], recorded["serialization"], self._sys_db.serializer
            )
        return deserialize_value(
            recorded["output"], recorded["serialization"], self._sys_db.serializer
        )

    def check_cancelled(self) -> None:
        """Raise if the reading workflow has been cancelled, probing at most once an interval.

        The replay probe stops querying once past the frontier, so nothing else would notice.
        """
        if self._ctx is None:
            return
        now = time.time()
        if now - self._last_cancel_check < _STREAM_CANCEL_CHECK_INTERVAL_SEC:
            return
        self._last_cancel_check = now
        status = self._sys_db.get_workflow_status(self._ctx.workflow_id)
        if (
            status is not None
            and status["status"] == WorkflowStatusString.CANCELLED.value
        ):
            raise DBOSWorkflowCancelledError(
                f"Workflow {self._ctx.workflow_id} is cancelled. Aborting read_stream."
            )

    def record(self, step_ctx: Optional[DBOSContext], value: Any) -> None:
        """Record a value delivered to the workflow, in the app's serializer as step outputs are.

        Checkpoints are read back only by this runtime replaying this workflow, never across
        languages, so they do not follow the workflow's declared interop format.
        """
        if step_ctx is None:
            return
        output, serialization = serialize_value(value, None, self._sys_db.serializer)
        result: OperationResultInternal = {
            "workflow_uuid": step_ctx.workflow_id,
            "function_id": step_ctx.function_id,
            "function_name": self._function_name,
            "started_at_epoch_ms": self._started_at_epoch_ms,
            "output": output,
            "serialization": serialization,
            "error": None,
        }
        self._sys_db.record_operation_result(result)
        self.end()

    def record_timeout(self, step_ctx: Optional[DBOSContext], error: Exception) -> None:
        """Record that the wait for this value timed out, so a replay raises it rather than waiting.

        A timeout is an outcome rather than a failure of the read, the same way get_event records
        the None it returns, and it is the failure a workflow is most likely to catch and act on.
        """
        if step_ctx is None:
            return
        serialized, serialization = serialize_exception(
            error, None, self._sys_db.serializer
        )
        result: OperationResultInternal = {
            "workflow_uuid": step_ctx.workflow_id,
            "function_id": step_ctx.function_id,
            "function_name": self._function_name,
            "started_at_epoch_ms": self._started_at_epoch_ms,
            "output": None,
            "serialization": serialization,
            "error": serialized,
        }
        self._sys_db.record_operation_result(result)
        self.end()


def read_stream_offset(
    sys_db: "SystemDatabase",
    workflow_id: str,
    key: str,
    offset: int,
    *,
    polling_interval: Optional[float] = None,
    timeout_seconds: Optional[float] = None,
    checkpoint: bool = True,
) -> Any:
    """Return the single value at one offset of a stream, waiting for it to be written."""
    values = read_stream(
        sys_db,
        workflow_id,
        key,
        offset=offset,
        polling_interval=polling_interval,
        timeout_seconds=timeout_seconds,
        function_name=_READ_STREAM_OFFSET_FUNCTION_NAME,
        checkpoint=checkpoint,
    )
    try:
        for value in values:
            return value
        # The stream ended before reaching this offset, so no value will ever arrive.
        raise DBOSStreamTimeoutError(workflow_id, key)
    finally:
        # Close rather than abandon, so the listener is unregistered here and not at collection.
        values.close()


async def read_stream_offset_async(
    sys_db: "SystemDatabase",
    workflow_id: str,
    key: str,
    offset: int,
    *,
    polling_interval: Optional[float] = None,
    timeout_seconds: Optional[float] = None,
    checkpoint: bool = True,
) -> Any:
    """Return the single value at one offset of a stream, waiting for it to be written."""
    values = read_stream_async(
        sys_db,
        workflow_id,
        key,
        offset=offset,
        polling_interval=polling_interval,
        timeout_seconds=timeout_seconds,
        function_name=_READ_STREAM_OFFSET_FUNCTION_NAME,
        checkpoint=checkpoint,
    )
    try:
        async for value in values:
            return value
        # The stream ended before reaching this offset, so no value will ever arrive.
        raise DBOSStreamTimeoutError(workflow_id, key)
    finally:
        # Close rather than abandon, so the listener is unregistered here and not at collection.
        await values.aclose()


def read_stream(
    sys_db: "SystemDatabase",
    workflow_id: str,
    key: str,
    *,
    offset: int,
    polling_interval: Optional[float] = None,
    timeout_seconds: Optional[float] = None,
    function_name: str = _READ_STREAM_FUNCTION_NAME,
    checkpoint: bool = True,
) -> Generator[Any, Any, None]:
    """Yield a stream's values in order, checkpointing each one when read from a workflow."""
    if timeout_seconds is not None and timeout_seconds < 0:
        raise ValueError(f"timeout_seconds must not be negative, got {timeout_seconds}")
    # A zero or negative interval never waits, so the reader would spin on the database.
    if polling_interval is not None and (
        not math.isfinite(polling_interval) or polling_interval < 0.001
    ):
        raise ValueError(
            f"polling_interval_sec must be a finite number at least 0.001 seconds, got {polling_interval}"
        )
    if polling_interval is None:
        polling_interval = sys_db._notification_listener_polling_interval_sec
    recorder = _StreamReadCheckpoint(sys_db, key, function_name, checkpoint)
    event, payload = sys_db.register_stream_listener(workflow_id, key)
    final_read = False
    try:
        while True:
            # One step per delivered value, reserved before the read so it spans the wait.
            step_ctx = recorder.begin()
            recorder.check_cancelled()
            # The timeout is per value: the clock restarts every time one is delivered.
            deadline = (
                time.time() + timeout_seconds if timeout_seconds is not None else None
            )
            recorded = recorder.replay(step_ctx)
            if recorded is not _no_recorded_value:
                # Settled from history, so the step is no longer in flight.
                recorder.end()
                if is_stream_closed_sentinel(recorded):
                    return
                yield recorded
                offset += 1
                continue
            value: Any = _no_stream_value
            while True:
                # Clear before reading so a notification arriving after the read
                # leaves the event set and the wait below returns immediately.
                event.clear()
                # One round trip for both the value and the workflow's status.
                status, value = sys_db.read_stream_value(workflow_id, key, offset)
                if status is None:
                    raise DBOSNonExistentWorkflowError("target", workflow_id)
                if value is not _no_stream_value or final_read:
                    break
                # No value yet: stop if the workflow is done, else wait for a
                # notification. Workflow completion fires none, so the wait
                # is bounded by the polling interval to notice termination.
                if not workflow_is_active(status):
                    # Cancel and timeout set a terminal status out-of-band while the workflow is still writing, so drain to the first empty offset before stopping.
                    final_read = True
                    continue
                wait_for = polling_interval
                if deadline is not None:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        error = DBOSStreamTimeoutError(
                            workflow_id, key, timeout_seconds
                        )
                        recorder.record_timeout(step_ctx, error)
                        raise error
                    wait_for = min(remaining, polling_interval)
                recorder.check_cancelled()
                event.wait(timeout=wait_for)
            if value is _no_stream_value or is_stream_closed_sentinel(value):
                # The end is recorded too, so a replay stops exactly where this read did.
                recorder.record(step_ctx, _dbos_stream_closed_sentinel)
                return
            # Recorded before the yield, so a crash after the workflow acts on the value still replays it.
            recorder.record(step_ctx, value)
            yield value
            offset += 1
    finally:
        # Release the reserved step if the read was abandoned or raised mid-flight.
        recorder.end()
        sys_db.unregister_stream_listener(payload)


async def read_stream_async(
    sys_db: "SystemDatabase",
    workflow_id: str,
    key: str,
    *,
    offset: int,
    polling_interval: Optional[float] = None,
    timeout_seconds: Optional[float] = None,
    function_name: str = _READ_STREAM_FUNCTION_NAME,
    checkpoint: bool = True,
) -> AsyncGenerator[Any, None]:
    """Yield a stream's values in order, checkpointing each one when read from a workflow."""
    if timeout_seconds is not None and timeout_seconds < 0:
        raise ValueError(f"timeout_seconds must not be negative, got {timeout_seconds}")
    # A zero or negative interval never waits, so the reader would spin on the database.
    if polling_interval is not None and (
        not math.isfinite(polling_interval) or polling_interval < 0.001
    ):
        raise ValueError(
            f"polling_interval_sec must be a finite number at least 0.001 seconds, got {polling_interval}"
        )
    if polling_interval is None:
        polling_interval = sys_db._notification_listener_polling_interval_sec
    recorder = _StreamReadCheckpoint(sys_db, key, function_name, checkpoint)
    event, payload = sys_db.register_stream_listener(workflow_id, key)
    final_read = False
    try:
        while True:
            # One step per delivered value, reserved before the read so it spans the wait.
            step_ctx = recorder.begin()
            await asyncio.to_thread(recorder.check_cancelled)
            # The timeout is per value: the clock restarts every time one is delivered.
            deadline = (
                time.time() + timeout_seconds if timeout_seconds is not None else None
            )
            recorded = await asyncio.to_thread(recorder.replay, step_ctx)
            if recorded is not _no_recorded_value:
                # Settled from history, so the step is no longer in flight.
                recorder.end()
                if is_stream_closed_sentinel(recorded):
                    return
                yield recorded
                offset += 1
                continue
            value: Any = _no_stream_value
            while True:
                # Clear before reading so a notification arriving after the read
                # leaves the event set and the wait below returns immediately.
                event.clear()
                # One round trip for both the value and the workflow's status.
                status, value = await asyncio.to_thread(
                    sys_db.read_stream_value, workflow_id, key, offset
                )
                if status is None:
                    raise DBOSNonExistentWorkflowError("target", workflow_id)
                if value is not _no_stream_value or final_read:
                    break
                # No value yet: stop if the workflow is done, else await a
                # notification, re-reading at the fallback interval in case one
                # was dropped.
                if not workflow_is_active(status):
                    # Cancel and timeout set a terminal status out-of-band while the workflow is still writing, so drain to the first empty offset before stopping.
                    final_read = True
                    continue
                wait_for = polling_interval
                if deadline is not None:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        error = DBOSStreamTimeoutError(
                            workflow_id, key, timeout_seconds
                        )
                        await asyncio.to_thread(
                            recorder.record_timeout, step_ctx, error
                        )
                        raise error
                    wait_for = min(remaining, polling_interval)
                await asyncio.to_thread(recorder.check_cancelled)
                await event.wait_async(timeout=wait_for)
            if value is _no_stream_value or is_stream_closed_sentinel(value):
                # The end is recorded too, so a replay stops exactly where this read did.
                await asyncio.to_thread(
                    recorder.record, step_ctx, _dbos_stream_closed_sentinel
                )
                return
            # Recorded before the yield, so a crash after the workflow acts on the value still replays it.
            await asyncio.to_thread(recorder.record, step_ctx, value)
            yield value
            offset += 1
    finally:
        # Release the reserved step if the read was abandoned or raised mid-flight.
        recorder.end()
        sys_db.unregister_stream_listener(payload)


def _validate_enqueue_only_options(
    ctx: Optional[DBOSContext], queue: Optional[str]
) -> None:
    """Reject enqueue options on a workflow that is not being enqueued."""
    if queue is not None or ctx is None:
        return
    set_options = [
        name
        for name, value in (
            ("deduplication_id", ctx.deduplication_id),
            ("priority", ctx.priority),
            ("queue_partition_key", ctx.queue_partition_key),
            ("delay_seconds", ctx.delay_until_epoch_ms),
        )
        if value is not None
    ]
    if set_options:
        raise DBOSException(
            f"Enqueue option(s) {', '.join(set_options)} set on a workflow that is not being enqueued. "
            "These options are only supported when enqueueing a workflow onto a queue."
        )


def _resolve_duplication_policy(
    ctx: Optional[DBOSContext], queue: Optional[str]
) -> Optional[DuplicationPolicy]:
    """Validate the ambient duplication policy for this enqueue and return it."""
    if ctx is None:
        return None
    validate_duplication_policy(ctx.duplication_policy, queue, ctx.deduplication_id)
    return ctx.duplication_policy


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
