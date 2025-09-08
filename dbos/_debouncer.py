from typing import Callable

from dbos._context import SetWorkflowID
from dbos._dbos import DBOS, P, R
from dbos._serialization import WorkflowInputs

_DEBOUNCER_TOPIC = "DEBOUNCER_TOPIC"


@DBOS.workflow()
def _debouncer_workflow(
    func: Callable[P, R],
    workflow_id: str,
    timeout: float,
    *args: P.args,
    **kwargs: P.kwargs,
) -> None:
    workflow_inputs: WorkflowInputs = {"args": args, "kwargs": kwargs}
    message = None
    while True:
        message = DBOS.recv(_DEBOUNCER_TOPIC, timeout_seconds=timeout)
        if message is None:
            break
        else:
            workflow_inputs = message
    with SetWorkflowID(workflow_id):
        DBOS.start_workflow(func, *workflow_inputs["args"], **workflow_inputs["kwargs"])
