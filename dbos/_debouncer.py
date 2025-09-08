import uuid
from typing import Any, Callable, Dict, Tuple

from dbos._context import SetEnqueueOptions, SetWorkflowID
from dbos._core import WorkflowHandlePolling
from dbos._dbos import DBOS, P, R, WorkflowHandle, _get_dbos_instance
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
                generated_wfid = str(uuid.uuid4())
                with SetEnqueueOptions(deduplication_id=self.debounce_key):
                    internal_queue.enqueue(
                        debouncer_workflow,
                        func,
                        generated_wfid,
                        self.debounce_period_sec,
                        *args,
                        **kwargs,
                    )
                return WorkflowHandlePolling(generated_wfid, dbos)
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
                    user_workflow_id: str = dedup_workflow_input["args"][1]
                    workflow_inputs: WorkflowInputs = {"args": args, "kwargs": kwargs}
                    DBOS.send(dedup_wfid, workflow_inputs, _DEBOUNCER_TOPIC)
                    return WorkflowHandlePolling(user_workflow_id, dbos)
