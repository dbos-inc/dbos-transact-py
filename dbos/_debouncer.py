import asyncio
import uuid
from typing import Any, Callable, Coroutine, Dict, Tuple

from dbos._context import SetEnqueueOptions, SetWorkflowID
from dbos._core import WorkflowHandleAsyncPolling, WorkflowHandlePolling
from dbos._dbos import (
    DBOS,
    P,
    R,
    WorkflowHandle,
    WorkflowHandleAsync,
    _get_dbos_instance,
)
from dbos._error import DBOSQueueDeduplicatedError
from dbos._serialization import WorkflowInputs

_DEBOUNCER_TOPIC = "DEBOUNCER_TOPIC"


def debouncer_workflow(
    func: Callable[..., Any],
    workflow_id: str,
    debounce_period_sec: float,
    *args: Tuple[Any, ...],
    **kwargs: Dict[str, Any],
) -> None:

    workflow_inputs: WorkflowInputs = {"args": args, "kwargs": kwargs}
    message = None
    while True:
        message = DBOS.recv(_DEBOUNCER_TOPIC, timeout_seconds=debounce_period_sec)
        if message is None:
            break
        else:
            workflow_inputs = message
    with SetWorkflowID(workflow_id):
        DBOS.start_workflow(func, *workflow_inputs["args"], **workflow_inputs["kwargs"])


class Debouncer:

    def __init__(self, debounce_key: str, debounce_period_sec: float):
        self.debounce_key = debounce_key
        self.debounce_period_sec = debounce_period_sec

    def debounce(
        self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> WorkflowHandle[R]:
        dbos = _get_dbos_instance()
        internal_queue = dbos._registry.get_internal_queue()
        while True:
            try:
                user_workflow_id = str(uuid.uuid4())
                with SetEnqueueOptions(deduplication_id=self.debounce_key):
                    internal_queue.enqueue(
                        debouncer_workflow,
                        func,
                        user_workflow_id,
                        self.debounce_period_sec,
                        *args,
                        **kwargs,
                    )
                return WorkflowHandlePolling(user_workflow_id, dbos)
            except DBOSQueueDeduplicatedError:
                dedup_wfid = dbos._sys_db.get_deduplicated_workflow(
                    queue_name=internal_queue.name, deduplication_id=self.debounce_key
                )
                if dedup_wfid is None:
                    continue
                else:
                    dedup_workflow_input = (
                        DBOS.retrieve_workflow(dedup_wfid).get_status().input
                    )
                    assert dedup_workflow_input is not None
                    user_workflow_id = dedup_workflow_input["args"][1]
                    workflow_inputs: WorkflowInputs = {"args": args, "kwargs": kwargs}
                    DBOS.send(dedup_wfid, workflow_inputs, _DEBOUNCER_TOPIC)
                    return WorkflowHandlePolling(user_workflow_id, dbos)

    async def debounce_async(
        self,
        func: Callable[P, Coroutine[Any, Any, R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> WorkflowHandleAsync[R]:
        handle = await asyncio.to_thread(self.debounce, func, *args, **kwargs)  # type: ignore
        dbos = _get_dbos_instance()
        return WorkflowHandleAsyncPolling(handle.workflow_id, dbos)
