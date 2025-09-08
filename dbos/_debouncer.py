import sys
from typing import Callable, TypeVar

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from dbos._context import SetWorkflowID
from dbos._serialization import WorkflowInputs

P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values

_DEBOUNCER_TOPIC = "DEBOUNCER_TOPIC"


def debouncer_workflow(
    func: Callable[P, R],
    workflow_id: str,
    timeout: float,
    *args: P.args,
    **kwargs: P.kwargs,
) -> None:
    from dbos._dbos import DBOS

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
