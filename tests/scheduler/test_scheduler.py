import time
from datetime import datetime

from dbos_transact import DBOS


def test_scheduled_workflow(dbos: DBOS) -> None:
    wf_counter: int = 0

    @dbos.scheduled("* * * * * *")
    @dbos.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    time.sleep(2)
    assert wf_counter >= 1 and wf_counter <= 3


def test_scheduler_oaoo(dbos: DBOS) -> None:
    wf_counter: int = 0
    txn_counter: int = 0
    workflow_id: str = ""

    @dbos.scheduled("* * * * * *")
    @dbos.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        test_transaction()
        nonlocal wf_counter
        wf_counter += 1
        nonlocal workflow_id
        workflow_id = DBOS.workflow_id

    @dbos.transaction()
    def test_transaction() -> None:
        nonlocal txn_counter
        txn_counter += 1

    time.sleep(2)
    assert wf_counter >= 1 and wf_counter <= 3
    assert txn_counter == wf_counter

    dbos.sys_db.update_workflow_status(
        {
            "workflow_uuid": workflow_id,
            "status": "PENDING",
            "name": test_workflow.__qualname__,
            "output": None,
            "error": None,
            "executor_id": None,
            "app_id": None,
            "app_version": None,
            "request": None,
            "recovery_attempts": None,
        }
    )

    workflow_handles = dbos.recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == None

    assert txn_counter + 1 == wf_counter
