import threading
import uuid
from typing import Callable

import pytest

# Public API
from dbos import DBOS, Queue, SetWorkflowID
from dbos._dbos import DBOSConfiguredInstance
from dbos._error import DBOSWorkflowCancelledError
from dbos._utils import INTERNAL_QUEUE_NAME
from tests.conftest import queue_entries_are_cleaned_up


def test_cancel_resume(dbos: DBOS) -> None:
    steps_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()
    input = 5

    @DBOS.step()
    def step_one() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.step()
    def step_two() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        step_one()
        main_thread_event.set()
        workflow_event.wait()
        step_two()
        return x

    # Start the workflow and cancel it.
    # Verify it stops after step one but before step two
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(simple_workflow, input)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(DBOSWorkflowCancelledError):
        handle.get_result()
    assert steps_completed == 1

    # Resume the workflow. Verify it completes successfully.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == input
    assert steps_completed == 2

    # Resume the workflow again. Verify it does not run again.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == input
    assert steps_completed == 2


def test_cancel_resume_txn(dbos: DBOS) -> None:
    txn_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()
    input = 5

    @DBOS.transaction()
    def txn_one() -> None:
        nonlocal txn_completed
        txn_completed += 1

    @DBOS.transaction()
    def txn_two() -> None:
        nonlocal txn_completed
        txn_completed += 1

    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        txn_one()
        main_thread_event.set()
        workflow_event.wait()
        txn_two()
        return x

    # Start the workflow and cancel it.
    # Verify it stops after step one but before step two
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(simple_workflow, input)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(DBOSWorkflowCancelledError):
        handle.get_result()
    assert txn_completed == 1

    # Resume the workflow. Verify it completes successfully.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == input
    assert txn_completed == 2

    # Resume the workflow again. Verify it does not run again.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == input
    assert txn_completed == 2


def test_cancel_resume_queue(dbos: DBOS) -> None:
    steps_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()
    input = 5

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
    def simple_workflow(x: int) -> int:
        step_one()
        main_thread_event.set()
        workflow_event.wait()
        step_two()
        return x

    # Start the workflow and cancel it.
    # Verify it stops after step one but before step two
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = queue.enqueue(simple_workflow, input)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(Exception):
        handle.get_result()
    assert steps_completed == 1

    # Resume the workflow. Verify it completes successfully.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == input
    assert steps_completed == 2

    # Resume the workflow again. Verify it does not run again.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == input
    assert steps_completed == 2

    # Verify nothing is left on any queue
    assert queue_entries_are_cleaned_up(dbos)


def test_restart(dbos: DBOS) -> None:
    input = 2
    multiplier = 5

    @DBOS.dbos_class()
    class TestClass(DBOSConfiguredInstance):

        def __init__(self, multiplier: int) -> None:
            self.multiply: Callable[[int], int] = lambda x: x * multiplier
            super().__init__("test_class")

        @DBOS.workflow()
        def workflow(self, x: int) -> int:
            return self.multiply(x)

        @DBOS.step()
        def step(self, x: int) -> int:
            return self.multiply(x)

    inst = TestClass(multiplier)

    # Start the workflow, let it finish, restart it.
    # Verify it returns the same result with a different workflow ID.
    handle = DBOS.start_workflow(inst.workflow, input)
    assert handle.get_result() == input * multiplier
    forked_handle = DBOS.restart_workflow(handle.workflow_id)
    assert forked_handle.workflow_id != handle.workflow_id
    assert forked_handle.get_result() == input * multiplier

    # Enqueue the workflow, let it finish, restart it.
    # Verify it returns the same result with a different workflow ID and queue.
    queue = Queue("test_queue")
    handle = queue.enqueue(inst.workflow, input)
    assert handle.get_result() == input * multiplier
    forked_handle = DBOS.restart_workflow(handle.workflow_id)
    assert forked_handle.workflow_id != handle.workflow_id
    assert forked_handle.get_status().queue_name == INTERNAL_QUEUE_NAME
    assert forked_handle.get_result() == input * multiplier

    # Enqueue the step, let it finish, restart it.
    # Verify it returns the same result with a different workflow ID and queue.
    handle = queue.enqueue(inst.step, input)
    assert handle.get_result() == input * multiplier
    forked_handle = DBOS.restart_workflow(handle.workflow_id)
    assert forked_handle.workflow_id != handle.workflow_id
    assert forked_handle.get_status().queue_name != handle.get_status().queue_name
    assert forked_handle.get_result() == input * multiplier

    # Verify restarting a nonexistent workflow throws an exception
    with pytest.raises(Exception):
        DBOS.restart_workflow("fake_id")

    # Verify nothing is left on any queue
    assert queue_entries_are_cleaned_up(dbos)
