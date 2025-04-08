import threading
import time
import uuid

import pytest

# Public API
from dbos import DBOS, ConfigFile, SetWorkflowID


def test_two_steps_cancel(dbos: DBOS, config: ConfigFile) -> None:
    steps_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.step()
    def step_one() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.step()
    def step_two() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.workflow()
    def simple_workflow() -> None:
        step_one()
        main_thread_event.set()
        workflow_event.wait()
        step_two()

    # Start the workflow and cancel it.
    # Verify it stops after step one but before step two
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(simple_workflow)
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(Exception):
        handle.get_result()
    assert steps_completed == 1

    # Resume the workflow. Verify it completes successfully.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == None
    assert steps_completed == 2

    # Resume the workflow again. Verify it does not run again.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == None
    assert steps_completed == 2


def test_two_transactions_cancel(dbos: DBOS, config: ConfigFile) -> None:

    tr_completed = 0

    @DBOS.transaction()
    def transaction_one() -> None:
        nonlocal tr_completed
        tr_completed += 1
        print("Transaction one completed!")

    @DBOS.transaction()
    def transaction_two() -> None:
        nonlocal tr_completed
        tr_completed += 1
        print("Step two completed!")

    @DBOS.workflow()
    def simple_workflow() -> None:
        transaction_one()
        dbos.sleep(2)
        transaction_two()
        print("Executed Simple workflow")
        return

    # run the workflow
    wfuuid = str(uuid.uuid4())
    try:
        with SetWorkflowID(wfuuid):
            simple_workflow()

        dbos.cancel_workflow(wfuuid)
    except Exception as e:
        # time.sleep(1)  # wait for the workflow to complete
        assert (
            tr_completed == 1
        ), f"Expected tr_completed to be 1, but got {tr_completed}"

    dbos.resume_workflow(wfuuid)
    time.sleep(1)

    assert (
        tr_completed == 2
    ), f"Expected steps_completed to be 2, but got {tr_completed}"

    # resume it a 2nd time

    dbos.resume_workflow(wfuuid)
    time.sleep(1)

    assert (
        tr_completed == 2
    ), f"Expected steps_completed to be 2, but got {tr_completed}"
