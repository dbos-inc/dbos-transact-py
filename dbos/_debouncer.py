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
    Optional,
    ParamSpec,
    Tuple,
    TypedDict,
    TypeVar,
)

from dbos._client import (
    DBOSClient,
    EnqueueOptions,
    WorkflowHandleClientAsyncPolling,
    WorkflowHandleClientPolling,
)
from dbos._context import (
    DBOSContextEnsure,
    SetEnqueueOptions,
    SetWorkflowID,
    SetWorkflowTimeout,
    assert_current_dbos_context,
    snapshot_step_context,
)
from dbos._core import (
    DEBOUNCER_WORKFLOW_NAME,
    WorkflowHandleAsyncPolling,
    WorkflowHandlePolling,
)
from dbos._error import DBOSQueueDeduplicatedError
from dbos._queue import Queue
from dbos._registrations import get_dbos_func_name
from dbos._serialization import WorkflowInputs
from dbos._utils import INTERNAL_QUEUE_NAME, generate_uuid

if TYPE_CHECKING:
    from dbos._dbos import WorkflowHandle, WorkflowHandleAsync

P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values


_DEBOUNCER_TOPIC = "DEBOUNCER_TOPIC"


# Options saved from the local context to pass through to the debounced function
class ContextOptions(TypedDict):
    workflow_id: str
    deduplication_id: Optional[str]
    priority: Optional[int]
    app_version: Optional[str]
    workflow_timeout_sec: Optional[float]


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


def debouncer_workflow(
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
    def get_debounce_deadline_epoch_sec() -> float:
        return (
            time.time() + options["debounce_timeout_sec"]
            if options["debounce_timeout_sec"]
            else math.inf
        )

    debounce_deadline_epoch_sec = dbos._sys_db.call_function_as_step(
        get_debounce_deadline_epoch_sec,
        "get_debounce_deadline_epoch_sec",
        snapshot_step_context(reserve_sleep_id=False),
    )
    debounce_period_sec = initial_debounce_period_sec
    while (
        DBOS.run_step({"name": "get_time"}, lambda: time.time())
        < debounce_deadline_epoch_sec
    ):
        time_until_deadline = max(
            debounce_deadline_epoch_sec
            - DBOS.run_step({"name": "get_time"}, lambda: time.time()),
            0,
        )
        timeout = min(debounce_period_sec, time_until_deadline)
        message: DebouncerMessage = DBOS.recv(_DEBOUNCER_TOPIC, timeout_seconds=timeout)
        if message is None:
            break
        else:
            workflow_inputs = message["inputs"]
            debounce_period_sec = message["debounce_period_sec"]
            # Acknowledge receipt of the message
            DBOS.set_event(message["message_id"], message["message_id"])
    # After the timeout or period has elapsed, start the user workflow with the requested context parameters,
    # either directly or on a queue.
    with SetWorkflowID(ctx["workflow_id"]):
        with SetWorkflowTimeout(ctx["workflow_timeout_sec"]):
            func = dbos._registry.workflow_info_map.get(options["workflow_name"], None)
            if not func:
                raise Exception(
                    f"Invalid workflow name provided to debouncer: {options['workflow_name']}"
                )
            if options["queue_name"]:
                queue = dbos._registry.queue_info_map.get(options["queue_name"], None)
                if not queue:
                    raise Exception(
                        f"Invalid queue name provided to debouncer: {options['queue_name']}"
                    )
                with SetEnqueueOptions(
                    deduplication_id=ctx["deduplication_id"],
                    priority=ctx["priority"],
                    app_version=ctx["app_version"],
                ):
                    queue.enqueue(
                        func, *workflow_inputs["args"], **workflow_inputs["kwargs"]
                    )
            else:
                DBOS.start_workflow(
                    func, *workflow_inputs["args"], **workflow_inputs["kwargs"]
                )


class Debouncer(Generic[P, R]):

    def __init__(
        self,
        workflow_name: str,
        *,
        debounce_timeout_sec: Optional[float] = None,
        queue: Optional[Queue] = None,
    ):
        self.func_name = workflow_name
        self.options: DebouncerOptions = {
            "debounce_timeout_sec": debounce_timeout_sec,
            "queue_name": queue.name if queue else None,
            "workflow_name": workflow_name,
        }

    @staticmethod
    def create(
        workflow: Callable[P, R],
        *,
        debounce_timeout_sec: Optional[float] = None,
        queue: Optional[Queue] = None,
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
        queue: Optional[Queue] = None,
    ) -> "Debouncer[P, R]":

        if isinstance(workflow, (types.MethodType)):
            raise TypeError("Only workflow functions may be debounced, not methods")
        return Debouncer[P, R](
            get_dbos_func_name(workflow),
            debounce_timeout_sec=debounce_timeout_sec,
            queue=queue,
        )

    def debounce(
        self,
        debounce_key: str,
        debounce_period_sec: float,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> "WorkflowHandle[R]":
        from dbos._dbos import DBOS, _get_dbos_instance

        dbos = _get_dbos_instance()
        internal_queue = dbos._registry.get_internal_queue()

        # Read all workflow settings from context, pass them through ContextOptions
        # into the debouncer to apply to the user workflow, then reset the context
        # so workflow settings aren't applied to the debouncer.
        with DBOSContextEnsure():
            ctx = assert_current_dbos_context()

            # Deterministically generate the user workflow ID and message ID
            def assign_debounce_ids() -> tuple[str, str]:
                return generate_uuid(), ctx.assign_workflow_id()

            message_id, user_workflow_id = dbos._sys_db.call_function_as_step(
                assign_debounce_ids,
                "DBOS.assign_debounce_ids",
                snapshot_step_context(reserve_sleep_id=False),
            )
            ctx.id_assigned_for_next_workflow = ""
            ctx.is_within_set_workflow_id_block = False
            ctxOptions: ContextOptions = {
                "workflow_id": user_workflow_id,
                "app_version": ctx.app_version,
                "deduplication_id": ctx.deduplication_id,
                "priority": ctx.priority,
                "workflow_timeout_sec": (
                    ctx.workflow_timeout_ms / 1000.0
                    if ctx.workflow_timeout_ms
                    else None
                ),
            }
        while True:
            try:
                # Attempt to enqueue a debouncer for this workflow.
                deduplication_id = f"{self.options['workflow_name']}-{debounce_key}"
                with SetEnqueueOptions(deduplication_id=deduplication_id):
                    with SetWorkflowTimeout(None):
                        internal_queue.enqueue(
                            debouncer_workflow,
                            debounce_period_sec,
                            ctxOptions,
                            self.options,
                            *args,
                            **kwargs,
                        )
                return WorkflowHandlePolling(user_workflow_id, dbos)
            except DBOSQueueDeduplicatedError:
                # If there is already a debouncer, send a message to it.
                # Deterministically retrieve the ID of the debouncer
                def get_deduplicated_workflow() -> Optional[str]:
                    return dbos._sys_db.get_deduplicated_workflow(
                        queue_name=internal_queue.name,
                        deduplication_id=deduplication_id,
                    )

                dedup_wfid = dbos._sys_db.call_function_as_step(
                    get_deduplicated_workflow,
                    "DBOS.get_deduplicated_workflow",
                    snapshot_step_context(reserve_sleep_id=False),
                )
                if dedup_wfid is None:
                    continue
                else:
                    workflow_inputs: WorkflowInputs = {"args": args, "kwargs": kwargs}
                    message: DebouncerMessage = {
                        "message_id": message_id,
                        "inputs": workflow_inputs,
                        "debounce_period_sec": debounce_period_sec,
                    }
                    DBOS.send(dedup_wfid, message, _DEBOUNCER_TOPIC)
                    # Wait for the debouncer to acknowledge receipt of the message.
                    # If the message is not acknowledged, this likely means the debouncer started its workflow
                    # and exited without processing this message, so try again.
                    if not DBOS.get_event(dedup_wfid, message_id, timeout_seconds=1):
                        continue
                    # Retrieve the user workflow ID from the input to the debouncer
                    # and return a handle to it
                    dedup_workflow_input = (
                        DBOS.retrieve_workflow(dedup_wfid).get_status().input
                    )
                    assert dedup_workflow_input is not None
                    user_workflow_id = dedup_workflow_input["args"][1]["workflow_id"]
                    return WorkflowHandlePolling(user_workflow_id, dbos)

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
        queue: Optional[Queue] = None,
    ):
        self.workflow_options = workflow_options
        self.debouncer_options: DebouncerOptions = {
            "debounce_timeout_sec": debounce_timeout_sec,
            "queue_name": queue.name if queue else None,
            "workflow_name": workflow_options["workflow_name"],
        }
        self.client = client

    def debounce(
        self, debounce_key: str, debounce_period_sec: float, *args: Any, **kwargs: Any
    ) -> "WorkflowHandle[R]":

        ctxOptions: ContextOptions = {
            "workflow_id": (
                self.workflow_options["workflow_id"]
                if self.workflow_options.get("workflow_id")
                else generate_uuid()
            ),
            "app_version": self.workflow_options.get("app_version"),
            "deduplication_id": self.workflow_options.get("deduplication_id"),
            "priority": self.workflow_options.get("priority"),
            "workflow_timeout_sec": self.workflow_options.get("workflow_timeout"),
        }
        message_id = generate_uuid()
        while True:
            try:
                # Attempt to enqueue a debouncer for this workflow.
                deduplication_id = (
                    f"{self.debouncer_options['workflow_name']}-{debounce_key}"
                )
                debouncer_options: EnqueueOptions = {
                    "workflow_name": DEBOUNCER_WORKFLOW_NAME,
                    "queue_name": INTERNAL_QUEUE_NAME,
                    "deduplication_id": deduplication_id,
                }
                self.client.enqueue(
                    debouncer_options,
                    debounce_period_sec,
                    ctxOptions,
                    self.debouncer_options,
                    *args,
                    **kwargs,
                )
                return WorkflowHandleClientPolling[R](
                    ctxOptions["workflow_id"], self.client._sys_db
                )
            except DBOSQueueDeduplicatedError:
                # If there is already a debouncer, send a message to it.
                dedup_wfid = self.client._sys_db.get_deduplicated_workflow(
                    queue_name=INTERNAL_QUEUE_NAME,
                    deduplication_id=deduplication_id,
                )
                if dedup_wfid is None:
                    continue
                else:
                    workflow_inputs: WorkflowInputs = {"args": args, "kwargs": kwargs}
                    message: DebouncerMessage = {
                        "message_id": message_id,
                        "inputs": workflow_inputs,
                        "debounce_period_sec": debounce_period_sec,
                    }
                    self.client.send(dedup_wfid, message, _DEBOUNCER_TOPIC)
                    # Wait for the debouncer to acknowledge receipt of the message.
                    # If the message is not acknowledged, this likely means the debouncer started its workflow
                    # and exited without processing this message, so try again.
                    if not self.client.get_event(
                        dedup_wfid, message_id, timeout_seconds=1
                    ):
                        continue
                    # Retrieve the user workflow ID from the input to the debouncer
                    # and return a handle to it
                    dedup_workflow_input = (
                        self.client.retrieve_workflow(dedup_wfid).get_status().input
                    )
                    assert dedup_workflow_input is not None
                    user_workflow_id = dedup_workflow_input["args"][1]["workflow_id"]
                    return WorkflowHandleClientPolling[R](
                        user_workflow_id, self.client._sys_db
                    )

    async def debounce_async(
        self, deboucne_key: str, debounce_period_sec: float, *args: Any, **kwargs: Any
    ) -> "WorkflowHandleAsync[R]":
        handle: "WorkflowHandle[R]" = await asyncio.to_thread(
            self.debounce, deboucne_key, debounce_period_sec, *args, **kwargs
        )
        return WorkflowHandleClientAsyncPolling[R](
            handle.workflow_id, self.client._sys_db
        )
