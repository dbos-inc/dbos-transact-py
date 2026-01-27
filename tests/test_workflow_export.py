import pytest
from sqlalchemy.exc import IntegrityError

from dbos import DBOS, DBOSConfig


def test_workflow_export(dbos: DBOS, config: DBOSConfig) -> None:

    key = "key"
    value = "value"

    @DBOS.step()
    def step() -> None:
        return

    @DBOS.workflow()
    def child_workflow() -> str:
        wfid = DBOS.workflow_id
        assert wfid is not None
        grandchild_workflow()
        return wfid

    @DBOS.workflow()
    def grandchild_workflow() -> str:
        wfid = DBOS.workflow_id
        assert wfid is not None
        return wfid

    @DBOS.workflow()
    def workflow() -> tuple[str, str]:
        assert DBOS.workflow_id
        child_id = child_workflow()
        for _ in range(10):
            step()
        DBOS.set_event(key, value)
        DBOS.write_stream(key, value)
        return DBOS.workflow_id, child_id

    workflow_id, child_id = workflow()
    # Child workflow ID follows pattern: parent_id-function_id
    grandchild_id = f"{child_id}-1"

    exported_workflow = dbos._sys_db.export_workflow(workflow_id, export_children=True)
    original_steps = DBOS.list_workflow_steps(workflow_id)
    # Importing the workflow into an existing database fails with a
    # primary key conflict
    with pytest.raises(IntegrityError):
        dbos._sys_db.import_workflow(exported_workflow)

    # Reset the system database
    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.reset_system_database()
    DBOS.launch()

    # Importing the workflow succeeds
    dbos._sys_db.import_workflow(exported_workflow)

    # All workflow information is present
    assert DBOS.get_event(workflow_id, key) == value
    read_values = []
    for v in DBOS.read_stream(workflow_id, key):
        read_values.append(v)
    assert read_values == [value]
    imported_steps = DBOS.list_workflow_steps(workflow_id)
    assert len(imported_steps) == len(original_steps)
    for imported_step, original_step in zip(imported_steps, original_steps):
        assert imported_step == original_step

    # The child workflows are also copied over
    assert len(DBOS.list_workflows()) == 3

    # Verify parent relationships are preserved after import
    imported_parent_status = DBOS.get_workflow_status(workflow_id)
    assert imported_parent_status is not None
    assert imported_parent_status.parent_workflow_id is None

    imported_child_status = DBOS.get_workflow_status(child_id)
    assert imported_child_status is not None
    assert imported_child_status.parent_workflow_id == workflow_id

    imported_grandchild_status = DBOS.get_workflow_status(grandchild_id)
    assert imported_grandchild_status is not None
    assert imported_grandchild_status.parent_workflow_id == child_id

    # The imported workflow can be forked
    forked_workflow = DBOS.fork_workflow(workflow_id, len(imported_steps))
    assert forked_workflow.get_result()[0] == forked_workflow.workflow_id
    assert DBOS.get_event(forked_workflow.workflow_id, key) == value

    DBOS.destroy(destroy_registry=True)
