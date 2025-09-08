import uuid

from dbos import DBOS, Debouncer, WorkflowHandle
from dbos._core import DEBOUNCER_WORKFLOW_NAME


def workflow(x: int) -> int:
    return x


def test_debouncer_workflow(dbos: DBOS) -> None:

    DBOS.workflow()(workflow)
    value = 5

    wfid = str(uuid.uuid4())
    debouncer_workflow = dbos._registry.workflow_info_map[DEBOUNCER_WORKFLOW_NAME]
    debouncer_workflow(workflow, wfid, 0, value)
    handle: WorkflowHandle[int] = DBOS.retrieve_workflow(wfid, existing_workflow=False)
    assert handle.get_result() == value


def test_debouncer(dbos: DBOS) -> None:

    DBOS.workflow()(workflow)
    first_value = 4
    second_value = 5

    debouncer = Debouncer("key", 2)
    first_handle = debouncer.debounce(workflow, first_value)
    second_handle = debouncer.debounce(workflow, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value
