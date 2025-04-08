import threading
import uuid

import pytest

# Public API
from dbos import DBOS, Queue, SetWorkflowID
from dbos._error import DBOSWorkflowCancelledError
from tests.conftest import queue_entries_are_cleaned_up


def test_cancel_resume(dbos: DBOS) -> None:
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
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(DBOSWorkflowCancelledError):
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


def test_cancel_resume_txn(dbos: DBOS) -> None:
    txn_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.transaction()
    def txn_one() -> None:
        nonlocal txn_completed
        txn_completed += 1

    @DBOS.transaction()
    def txn_two() -> None:
        nonlocal txn_completed
        txn_completed += 1

    @DBOS.workflow()
    def simple_workflow() -> None:
        txn_one()
        main_thread_event.set()
        workflow_event.wait()
        txn_two()

    # Start the workflow and cancel it.
    # Verify it stops after step one but before step two
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(simple_workflow)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(DBOSWorkflowCancelledError):
        handle.get_result()
    assert txn_completed == 1

    # Resume the workflow. Verify it completes successfully.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == None
    assert txn_completed == 2

    # Resume the workflow again. Verify it does not run again.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == None
    assert txn_completed == 2


def test_cancel_resume_queue(dbos: DBOS) -> None:
    steps_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    queue = Queue("test_queue")

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
        handle = queue.enqueue(simple_workflow)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    assert steps_completed == 1

    # Resume the workflow. Verify it completes successfully.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == None
    assert steps_completed == 2

    # Resume the workflow again. Verify it does not run again.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == None
    assert steps_completed == 2

    # Verify nothing is left on any queue
    assert queue_entries_are_cleaned_up(dbos)
