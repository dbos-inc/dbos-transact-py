import time
from datetime import datetime

# Public API
from dbos import DBOS


def test_scheduled_workflow(dbos: DBOS) -> None:
    wf_counter: int = 0

    @dbos.scheduled("* * * * * *")
    @dbos.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    time.sleep(2)
    assert wf_counter >= 1 and wf_counter <= 3


def test_scheduled_workflow_exception(dbos: DBOS) -> None:
    wf_counter: int = 0

    @dbos.scheduled("* * * * * *")
    @dbos.workflow()
    def test_failing_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1
        raise Exception("error")

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
    max_tries = 10
    for i in range(max_tries):
        try:
            assert txn_counter == wf_counter
            break
        except Exception as e:
            if i == max_tries - 1:
                raise e
            else:
                time.sleep(1)

    dbos.sys_db.update_workflow_status(
        {
            "workflow_uuid": workflow_id,
            "status": "PENDING",
            "name": test_workflow.__qualname__,
            "class_name": None,
            "config_name": None,
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
    max_tries = 10
    for i in range(max_tries):
        try:
            assert txn_counter + 1 == wf_counter
            break
        except Exception as e:
            if i == max_tries - 1:
                raise e
            else:
                time.sleep(1)


def test_long_workflow(dbos: DBOS) -> None:
    """
    This runs every hour and does nothing. Goal is to verify that it shuts down properly.
    """

    @dbos.scheduled("0 * * * *")
    @dbos.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        pass
