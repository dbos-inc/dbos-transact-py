import uuid

from dbos import DBOS, WorkflowHandle


def test_debouncer(dbos: DBOS) -> None:
    from dbos._debouncer import _debouncer_workflow

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    assert workflow(5) == 5

    wfid = str(uuid.uuid4())
    _debouncer_workflow(workflow, wfid, 0, 5)
    handle: WorkflowHandle[int] = DBOS.retrieve_workflow(wfid, existing_workflow=False)
    assert handle.get_result() == 5
