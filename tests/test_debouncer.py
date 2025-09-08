import uuid

from dbos import DBOS, WorkflowHandle
from dbos._core import DEBOUNCER_WORKFLOW_NAME


def test_debouncer(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    assert workflow(5) == 5

    wfid = str(uuid.uuid4())
    debouncer_workflow = dbos._registry.workflow_info_map[DEBOUNCER_WORKFLOW_NAME]
    debouncer_workflow(workflow, wfid, 0, 5)
    handle: WorkflowHandle[int] = DBOS.retrieve_workflow(wfid, existing_workflow=False)
    assert handle.get_result() == 5
