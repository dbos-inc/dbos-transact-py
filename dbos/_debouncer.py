import asyncio
import math
import time
import types
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Dict,
    Generic,
    Literal,
    Optional,
    ParamSpec,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

from dbos._client import (
    DBOSClient,
    EnqueueOptions,
    WorkflowHandleClientAsyncPolling,
    WorkflowHandleClientPolling,
)
from dbos._context import (
    SetEnqueueOptions,
    SetWorkflowAttributes,
    SetWorkflowDebounce,
    SetWorkflowID,
    SetWorkflowTimeout,
    snapshot_step_context,
)
from dbos._core import (
    DEBOUNCER_WORKFLOW_NAME,
    WorkflowHandleAsyncPolling,
    WorkflowHandlePolling,
)
from dbos._error import DBOSQueueDeduplicatedError
from dbos._queue import Queue
from dbos._registrations import get_dbos_func_name, get_func_info
from dbos._serialization import WorkflowInputs, serialize_args
from dbos._sys_db import DebounceResult

if TYPE_CHECKING:
    from dbos._dbos import WorkflowHandle, WorkflowHandleAsync

P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values


_DEBOUNCER_TOPIC = "DEBOUNCER_TOPIC"

# How long to wait before retrying when a legacy (pre-delay-based) debouncer
# workflow still holds a debounce key during a version upgrade.
_LEGACY_DRAIN_RETRY_SEC = 1.0


def _resolve_queue_name(queue: Optional[Union[Queue, str]]) -> Optional[str]:
    if queue is None:
        return None
    if isinstance(queue, Queue):
        return queue.name
    return queue


# The action to take after attempting to bounce an existing debounced workflow.
_BounceAction = Literal["return", "retry", "wait", "raise"]


def _classify_bounce(result: DebounceResult) -> Tuple[_BounceAction, Optional[str]]:
    """Decide what a debounce caller should do after a bounce attempt.

    - "return": an existing debounced workflow was extended; return a handle to it.
    - "retry": the key is unheld (the previous debounced workflow already started)
      or a rare race occurred; try enqueueing a fresh workflow again.
    - "wait": a legacy debouncer workflow holds the key during a version upgrade;
      wait for it to drain, then retry.
    - "raise": the key is held by a non-debounced workflow (a caller's own
      deduplicated workflow); surface the deduplication conflict.
    """
    if result["bounced_workflow_id"] is not None:
        return "return", result["bounced_workflow_id"]
    if result["holder_workflow_id"] is None:
        return "retry", None
    if result["holder_name"] == DEBOUNCER_WORKFLOW_NAME:
        return "wait", None
    if not result["holder_is_debounced"]:
        return "raise", None
    return "retry", None


# Options saved from the local context to pass through to the debounced function
class ContextOptions(TypedDict):
    workflow_id: str
    deduplication_id: Optional[str]
    priority: Optional[int]
    app_version: Optional[str]
    workflow_timeout_sec: Optional[float]
    attributes: Optional[Dict[str, Any]]


# Parameters for the debouncer workflow
class DebouncerOptions(TypedDict):
    workflow_name: str
    debounce_timeout_sec: Optional[float]
    queue_name: Optional[str]


# The message sent from a debounce to the debouncer workflow
class DebouncerMessage(TypedDict):
    inputs: WorkflowInputs
    message_id: str
    debounce_period_sec: float


# DEPRECATED: retained only so debouncer workflows enqueued before the switch to
# delay-based debouncing can drain during a version upgrade. New debounces no
# longer enqueue this workflow. Remove in a future release.
async def debouncer_workflow(
    initial_debounce_period_sec: float,
    ctx: ContextOptions,
    options: DebouncerOptions,
    *args: Tuple[Any, ...],
    **kwargs: Dict[str, Any],
) -> None:
    from dbos._dbos import DBOS, _get_dbos_instance

    dbos = _get_dbos_instance()

    workflow_inputs: WorkflowInputs = {"args": args, "kwargs": kwargs}

    # Every time the debounced workflow is called, a message is sent to this workflow.
    # It waits until debounce_period_sec have passed since the last message or until
    # debounce_timeout_sec has elapsed.
    async def get_debounce_deadline_epoch_sec() -> float:
        return (
            time.time() + options["debounce_timeout_sec"]
            if options["debounce_timeout_sec"]
            else math.inf
        )

    async def get_time() -> float:
        return time.time()

    debounce_deadline_epoch_sec = await DBOS.run_step_async(
        {"name": "get_debounce_deadline_epoch_sec"}, get_debounce_deadline_epoch_sec
    )
    debounce_period_sec = initial_debounce_period_sec
    while True:
        now = await DBOS.run_step_async({"name": "get_time"}, get_time)
        if now >= debounce_deadline_epoch_sec:
            break
        timeout = min(debounce_period_sec, max(debounce_deadline_epoch_sec - now, 0))
        message: Optional[DebouncerMessage] = await DBOS.recv_async(
            _DEBOUNCER_TOPIC, timeout_seconds=timeout
        )
        if message is None:
            break
        workflow_inputs = message["inputs"]
        debounce_period_sec = message["debounce_period_sec"]
        # Acknowledge receipt of the message
        await DBOS.set_event_async(message["message_id"], message["message_id"])

    # After the timeout or period has elapsed, enqueue the user workflow with the requested
    # context parameters.
    func = dbos._registry.workflow_info_map.get(options["workflow_name"], None)
    if not func:
        raise Exception(
            f"Invalid workflow name provided to debouncer: {options['workflow_name']}"
        )
    if options["queue_name"]:
        queue = dbos._registry.queue_info_map.get(options["queue_name"], None)
        if not queue:
            queue = await DBOS.retrieve_queue_async(options["queue_name"])
        if not queue:
            raise Exception(
                f"Invalid queue name provided to debouncer: {options['queue_name']}"
            )
    else:
        queue = dbos._registry.get_internal_queue()
    with SetWorkflowID(ctx["workflow_id"]):
        # Use .get() because inputs recorded before attributes existed lack the key
        with (
            SetWorkflowTimeout(ctx["workflow_timeout_sec"]),
            SetWorkflowAttributes(ctx.get("attributes")),
        ):
            if options["queue_name"]:
                # Apply the caller's enqueue options only on a user-specified queue,
                # matching the original behavior (the direct-start path applied none).
                with SetEnqueueOptions(
                    deduplication_id=ctx["deduplication_id"],
                    priority=ctx["priority"],
                    app_version=ctx["app_version"],
                ):
                    await queue.enqueue_async(
                        func, *workflow_inputs["args"], **workflow_inputs["kwargs"]
                    )
            else:
                await queue.enqueue_async(
                    func, *workflow_inputs["args"], **workflow_inputs["kwargs"]
                )


class Debouncer(Generic[P, R]):

    def __init__(
        self,
        workflow_name: str,
        *,
        debounce_timeout_sec: Optional[float] = None,
        queue: Optional[Union[Queue, str]] = None,
    ):
        self.func_name = workflow_name
        self.options: DebouncerOptions = {
            "debounce_timeout_sec": debounce_timeout_sec,
            "queue_name": _resolve_queue_name(queue),
            "workflow_name": workflow_name,
        }

    @staticmethod
    def create(
        workflow: Callable[P, R],
        *,
        debounce_timeout_sec: Optional[float] = None,
        queue: Optional[Union[Queue, str]] = None,
    ) -> "Debouncer[P, R]":

        if isinstance(workflow, (types.MethodType)):
            raise TypeError("Only workflow functions may be debounced, not methods")
        return Debouncer[P, R](
            get_dbos_func_name(workflow),
            debounce_timeout_sec=debounce_timeout_sec,
            queue=queue,
        )

    @staticmethod
    def create_async(
        workflow: Callable[P, Coroutine[Any, Any, R]],
        *,
        debounce_timeout_sec: Optional[float] = None,
        queue: Optional[Union[Queue, str]] = None,
    ) -> "Debouncer[P, R]":

        if isinstance(workflow, (types.MethodType)):
            raise TypeError("Only workflow functions may be debounced, not methods")
        return Debouncer[P, R](
            get_dbos_func_name(workflow),
            debounce_timeout_sec=debounce_timeout_sec,
            queue=queue,
        )

    def _bounce(
        self,
        dbos: Any,
        func: Callable[..., Any],
        queue_name: str,
        deduplication_id: str,
        debounce_period_sec: float,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
    ) -> DebounceResult:
        # Serialize the new inputs the same way the initial enqueue did, so a
        # bounce that replaces them stays consistent with the workflow's format.
        fi = get_func_info(func)
        serialization_type = fi.serialization_type if fi is not None else None
        inputs, serialization = serialize_args(
            args, kwargs, serialization_type, dbos._serializer
        )
        delay_until_epoch_ms = int(time.time() * 1000 + debounce_period_sec * 1000)
        result: DebounceResult = dbos._sys_db.debounce_delayed_workflow(
            queue_name=queue_name,
            deduplication_id=deduplication_id,
            delay_until_epoch_ms=delay_until_epoch_ms,
            inputs=inputs,
            serialization=serialization,
        )
        return result

    def debounce(
        self,
        debounce_key: str,
        debounce_period_sec: float,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> "WorkflowHandle[R]":
        from dbos._dbos import DBOS, _get_dbos_instance

        dbos = _get_dbos_instance()

        # Resolve the queue the debounced workflow will run on.
        queue_name = self.options["queue_name"]
        if queue_name:
            queue: Optional[Queue] = dbos._registry.queue_info_map.get(queue_name)
            if queue is None:
                queue = DBOS.retrieve_queue(queue_name)
            if queue is None:
                raise Exception(
                    f"Invalid queue name provided to debouncer: {queue_name}"
                )
        else:
            queue = dbos._registry.get_internal_queue()
        assert queue is not None

        # Resolve the workflow function so bounced inputs serialize identically.
        func = dbos._registry.workflow_info_map.get(self.options["workflow_name"])
        if func is None:
            raise Exception(
                f"Invalid workflow name provided to debouncer: {self.options['workflow_name']}"
            )

        deduplication_id = f"{self.options['workflow_name']}-{debounce_key}"
        timeout_sec = self.options["debounce_timeout_sec"]

        while True:
            # These are only observed the first time a fresh workflow is created;
            # on replay the enqueue short-circuits through its checkpoint.
            now_ms = int(time.time() * 1000)
            deadline_ms = int(now_ms + timeout_sec * 1000) if timeout_sec else None
            delay_until_ms = now_ms + int(debounce_period_sec * 1000)
            if deadline_ms is not None:
                delay_until_ms = min(delay_until_ms, deadline_ms)
            try:
                # Enqueue a fresh debounced workflow. The debounce key occupies the
                # deduplication slot, so a concurrent debounce for the same key
                # collides here and falls through to the bounce path below.
                with SetWorkflowDebounce(
                    deduplication_id=deduplication_id,
                    delay_until_epoch_ms=delay_until_ms,
                    debounce_deadline_epoch_ms=deadline_ms,
                ):
                    handle = queue.enqueue(func, *args, **kwargs)
                return handle
            except DBOSQueueDeduplicatedError:
                # A debounced workflow already exists for this key. Extend its
                # delay and replace its inputs, as a checkpointed step so a
                # debounce called from within a workflow replays deterministically.
                result = dbos._sys_db.call_function_as_step(
                    lambda: self._bounce(
                        dbos,
                        func,
                        queue.name,
                        deduplication_id,
                        debounce_period_sec,
                        args,
                        kwargs,
                    ),
                    "DBOS.debounce_delayed_workflow",
                    snapshot_step_context(reserve_sleep_id=False),
                )
                action, bounced_wfid = _classify_bounce(result)
                if action == "return":
                    assert bounced_wfid is not None
                    return WorkflowHandlePolling(bounced_wfid, dbos)
                if action == "raise":
                    raise
                if action == "wait":
                    time.sleep(_LEGACY_DRAIN_RETRY_SEC)
                # "retry"/"wait": loop and attempt a fresh enqueue again.

    async def debounce_async(
        self,
        debounce_key: str,
        debounce_period_sec: float,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> "WorkflowHandleAsync[R]":
        from dbos._dbos import _get_dbos_instance

        dbos = _get_dbos_instance()
        handle = await asyncio.to_thread(
            self.debounce, debounce_key, debounce_period_sec, *args, **kwargs
        )
        return WorkflowHandleAsyncPolling(handle.workflow_id, dbos)


class DebouncerClient:

    def __init__(
        self,
        client: DBOSClient,
        workflow_options: EnqueueOptions,
        *,
        debounce_timeout_sec: Optional[float] = None,
        queue: Optional[Union[Queue, str]] = None,
    ):
        self.workflow_options = workflow_options
        self.debouncer_options: DebouncerOptions = {
            "debounce_timeout_sec": debounce_timeout_sec,
            "queue_name": _resolve_queue_name(queue),
            "workflow_name": workflow_options["workflow_name"],
        }
        self.client = client

    def debounce(
        self, debounce_key: str, debounce_period_sec: float, *args: Any, **kwargs: Any
    ) -> "WorkflowHandle[R]":
        # Run the debounced workflow on the debouncer's queue if one was given,
        # otherwise on the queue named in the workflow options.
        queue_name = (
            self.debouncer_options["queue_name"] or self.workflow_options["queue_name"]
        )
        deduplication_id = f"{self.debouncer_options['workflow_name']}-{debounce_key}"
        timeout_sec = self.debouncer_options["debounce_timeout_sec"]
        serialization_type = self.workflow_options.get("serialization_type")

        while True:
            now_ms = int(time.time() * 1000)
            deadline_ms = int(now_ms + timeout_sec * 1000) if timeout_sec else None
            delay_sec = debounce_period_sec
            if deadline_ms is not None:
                delay_sec = min(delay_sec, max((deadline_ms - now_ms) / 1000.0, 0.0))
            options = cast(
                EnqueueOptions,
                {
                    **self.workflow_options,
                    "queue_name": queue_name,
                    "deduplication_id": deduplication_id,
                    "delay_seconds": delay_sec,
                    "debounce_deadline_epoch_ms": deadline_ms,
                    "is_debounced": True,
                },
            )
            try:
                # Enqueue a fresh debounced workflow. A concurrent debounce for the
                # same key collides on the deduplication slot and bounces below.
                return self.client.enqueue(options, *args, **kwargs)
            except DBOSQueueDeduplicatedError:
                # A debounced workflow already exists for this key; extend its
                # delay and replace its inputs.
                inputs, serialization = serialize_args(
                    args, kwargs, serialization_type, self.client._serializer
                )
                bounce_delay_ms = int(time.time() * 1000 + debounce_period_sec * 1000)
                result = self.client._sys_db.debounce_delayed_workflow(
                    queue_name=queue_name,
                    deduplication_id=deduplication_id,
                    delay_until_epoch_ms=bounce_delay_ms,
                    inputs=inputs,
                    serialization=serialization,
                )
                action, bounced_wfid = _classify_bounce(result)
                if action == "return":
                    assert bounced_wfid is not None
                    return WorkflowHandleClientPolling[R](
                        bounced_wfid, self.client._sys_db
                    )
                if action == "raise":
                    raise
                if action == "wait":
                    time.sleep(_LEGACY_DRAIN_RETRY_SEC)
                # "retry"/"wait": loop and attempt a fresh enqueue again.

    async def debounce_async(
        self, deboucne_key: str, debounce_period_sec: float, *args: Any, **kwargs: Any
    ) -> "WorkflowHandleAsync[R]":
        handle: "WorkflowHandle[R]" = await asyncio.to_thread(
            self.debounce, deboucne_key, debounce_period_sec, *args, **kwargs
        )
        return WorkflowHandleClientAsyncPolling[R](
            handle.workflow_id, self.client._sys_db
        )
