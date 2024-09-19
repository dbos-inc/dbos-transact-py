import time
from datetime import datetime

# Private test API
# Public API
from dbos import DBOS


def test_scheduled_workflow(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    time.sleep(4)
    assert wf_counter > 2 and wf_counter <= 4


def test_scheduled_transaction(dbos: DBOS) -> None:
    txn_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.transaction()
    def test_transaction(scheduled: datetime, actual: datetime) -> None:
        nonlocal txn_counter
        txn_counter += 1

    time.sleep(4)
    assert txn_counter > 2 and txn_counter <= 4


def test_scheduled_workflow_exception(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def test_failing_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1
        raise Exception("error")

    time.sleep(3)
    assert wf_counter >= 1 and wf_counter <= 3


def test_scheduler_oaoo(dbos: DBOS) -> None:
    wf_counter: int = 0
    txn_counter: int = 0
    workflow_id: str = ""

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        test_transaction()
        nonlocal wf_counter
        wf_counter += 1
        nonlocal workflow_id
        workflow_id = DBOS.workflow_id

    @DBOS.transaction()
    def test_transaction() -> None:
        nonlocal txn_counter
        txn_counter += 1

    time.sleep(3)
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

    # Stop all scheduled workflows
    for event in dbos.stop_events:
        event.set()

    dbos._sys_db.wait_for_buffer_flush()
    dbos._sys_db.update_workflow_status(
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
            "authenticated_user": None,
            "authenticated_roles": None,
            "assumed_role": None,
            "queue_name": None,
        }
    )

    workflow_handles = DBOS.recover_pending_workflows()
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

    @DBOS.scheduled("0 * * * *")
    @DBOS.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        pass
