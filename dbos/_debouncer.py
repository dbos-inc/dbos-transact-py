import asyncio
import math
import time
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Dict,
    Optional,
    Tuple,
    TypedDict,
)

from dbos._context import (
    DBOSContextEnsure,
    SetEnqueueOptions,
    SetWorkflowID,
    SetWorkflowTimeout,
    assert_current_dbos_context,
)
from dbos._core import WorkflowHandleAsyncPolling, WorkflowHandlePolling
from dbos._error import DBOSQueueDeduplicatedError
from dbos._queue import Queue
from dbos._serialization import WorkflowInputs

if TYPE_CHECKING:
    from dbos._dbos import P, R, WorkflowHandle, WorkflowHandleAsync


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
    debounce_period_sec: float
    debounce_timeout_sec: Optional[float]
    queue_name: Optional[str]


# The message sent from a debounce to the debouncer workflow
class DebouncerMessage(TypedDict):
    inputs: WorkflowInputs
    message_id: str


def debouncer_workflow(
    func: Callable[..., Any],
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
    debounce_deadline_epoch_sec = (
        time.time() + options["debounce_timeout_sec"]
        if options["debounce_timeout_sec"]
        else math.inf
    )
    while time.time() < debounce_deadline_epoch_sec:
        time_until_deadline = max(debounce_deadline_epoch_sec - time.time(), 0)
        timeout = min(options["debounce_period_sec"], time_until_deadline)
        message: DebouncerMessage = DBOS.recv(_DEBOUNCER_TOPIC, timeout_seconds=timeout)
        if message is None:
            break
        else:
            workflow_inputs = message["inputs"]
            # Acknowledge receipt of the message
            DBOS.set_event(message["message_id"], message["message_id"])
    with SetWorkflowID(ctx["workflow_id"]):
        with SetWorkflowTimeout(ctx["workflow_timeout_sec"]):
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


class Debouncer:

    def __init__(
        self,
        *,
        debounce_key: str,
        debounce_period_sec: float,
        debounce_timeout_sec: Optional[float] = None,
        queue: Optional[Queue] = None,
    ):
        self.options: DebouncerOptions = {
            "debounce_period_sec": debounce_period_sec,
            "debounce_timeout_sec": debounce_timeout_sec,
            "queue_name": queue.name if queue else None,
        }
        self.debounce_key = debounce_key

    def debounce(
        self, func: "Callable[P, R]", *args: "P.args", **kwargs: "P.kwargs"
    ) -> "WorkflowHandle[R]":
        from dbos._dbos import DBOS, _get_dbos_instance

        dbos = _get_dbos_instance()
        internal_queue = dbos._registry.get_internal_queue()

        # If a workflow ID is requested with SetWorkflowID,
        # use that ID for the user workflow and not the debouncer.
        with DBOSContextEnsure():
            ctx = assert_current_dbos_context()
            user_workflow_id = ctx.assign_workflow_id()
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
        message_id = str(uuid.uuid4())
        while True:
            try:
                # Attempt to enqueue a debouncer for this workflow.
                with SetEnqueueOptions(deduplication_id=self.debounce_key):
                    with SetWorkflowTimeout(None):
                        internal_queue.enqueue(
                            debouncer_workflow,
                            func,
                            ctxOptions,
                            self.options,
                            *args,
                            **kwargs,
                        )
                return WorkflowHandlePolling(user_workflow_id, dbos)
            except DBOSQueueDeduplicatedError:
                # If there is already a debouncer, send a message to it.
                dedup_wfid = dbos._sys_db.get_deduplicated_workflow(
                    queue_name=internal_queue.name, deduplication_id=self.debounce_key
                )
                if dedup_wfid is None:
                    continue
                else:
                    workflow_inputs: WorkflowInputs = {"args": args, "kwargs": kwargs}
                    message: DebouncerMessage = {
                        "message_id": message_id,
                        "inputs": workflow_inputs,
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
        func: "Callable[P, Coroutine[Any, Any, R]]",
        *args: "P.args",
        **kwargs: "P.kwargs",
    ) -> "WorkflowHandleAsync[R]":
        from dbos._dbos import _get_dbos_instance

        dbos = _get_dbos_instance()
        handle = await asyncio.to_thread(self.debounce, func, *args, **kwargs)  # type: ignore
        return WorkflowHandleAsyncPolling(handle.workflow_id, dbos)
