import threading
import uuid
from typing import Callable

import pytest

# Public API
from dbos import DBOS, Queue, SetWorkflowID
from dbos._dbos import DBOSConfiguredInstance
from dbos._error import DBOSException, DBOSWorkflowCancelledError
from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams
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
        # A handler like this should not catch DBOSWorkflowCancelledError
        try:
            step_two()
        except Exception:
            raise
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
    assert handle.get_status().app_version == GlobalParams.app_version
    assert handle.get_status().queue_name == INTERNAL_QUEUE_NAME
    assert handle.get_result() == input
    assert steps_completed == 2

    # Resume the workflow again. Verify it does not run again.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == input
    assert steps_completed == 2

    assert queue_entries_are_cleaned_up(dbos)


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


def test_fork_steps(
    dbos: DBOS,
) -> None:

    stepOneCount = 0
    stepTwoCount = 0
    stepThreeCount = 0
    stepFourCount = 0
    stepFiveCount = 0

    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return stepOne(x) + stepTwo(x) + stepThree(x) + stepFour(x) + stepFive(x)

    @DBOS.step()
    def stepOne(x: int) -> int:
        nonlocal stepOneCount
        stepOneCount += 1
        return x + 1

    @DBOS.step()
    def stepTwo(x: int) -> int:
        nonlocal stepTwoCount
        stepTwoCount += 1
        return x + 2

    @DBOS.step()
    def stepThree(x: int) -> int:
        nonlocal stepThreeCount
        stepThreeCount += 1
        return x + 3

    @DBOS.step()
    def stepFour(x: int) -> int:
        nonlocal stepFourCount
        stepFourCount += 1
        return x + 4

    @DBOS.step()
    def stepFive(x: int) -> int:
        nonlocal stepFiveCount
        stepFiveCount += 1
        return x + 5

    input = 1
    output = 5 * input + 15

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert simple_workflow(input) == output

    assert stepOneCount == 1
    assert stepTwoCount == 1
    assert stepThreeCount == 1
    assert stepFourCount == 1
    assert stepFiveCount == 1

    fork_id = str(uuid.uuid4())
    with SetWorkflowID(fork_id):
        forked_handle = DBOS.fork_workflow(wfid, 3)
    assert forked_handle.workflow_id == fork_id
    assert forked_handle.get_status().app_version == GlobalParams.app_version
    assert forked_handle.get_result() == output

    assert stepOneCount == 1
    assert stepTwoCount == 1
    assert stepThreeCount == 2
    assert stepFourCount == 2
    assert stepFiveCount == 2

    forked_handle = DBOS.fork_workflow(wfid, 5)
    assert forked_handle.workflow_id != wfid
    assert forked_handle.get_result() == output

    assert stepOneCount == 1
    assert stepTwoCount == 1
    assert stepThreeCount == 2
    assert stepFourCount == 2
    assert stepFiveCount == 3

    forked_handle = DBOS.fork_workflow(wfid, 1)
    assert forked_handle.workflow_id != wfid
    assert forked_handle.get_result() == output

    assert stepOneCount == 2
    assert stepTwoCount == 2
    assert stepThreeCount == 3
    assert stepFourCount == 3
    assert stepFiveCount == 4


def test_restart_fromsteps_transactionsonly(
    dbos: DBOS,
) -> None:

    trOneCount = 0
    trTwoCount = 0
    trThreeCount = 0
    trFourCount = 0
    trFiveCount = 0

    @DBOS.workflow()
    def simple_workflow() -> None:
        trOne()
        trTwo()
        trThree()
        trFour()
        trFive()
        return

    @DBOS.transaction()
    def trOne() -> None:
        nonlocal trOneCount
        trOneCount += 1
        return

    @DBOS.transaction()
    def trTwo() -> None:
        nonlocal trTwoCount
        trTwoCount += 1
        return

    @DBOS.transaction()
    def trThree() -> None:
        nonlocal trThreeCount
        trThreeCount += 1
        return

    @DBOS.transaction()
    def trFour() -> None:
        nonlocal trFourCount
        trFourCount += 1
        return

    @DBOS.transaction()
    def trFive() -> None:
        nonlocal trFiveCount
        trFiveCount += 1
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        simple_workflow()

    assert trOneCount == 1
    assert trTwoCount == 1
    assert trThreeCount == 1
    assert trFourCount == 1
    assert trFiveCount == 1

    forked_handle = DBOS.fork_workflow(wfid, 2)
    assert forked_handle.workflow_id != wfid
    forked_handle.get_result()

    assert trOneCount == 1
    assert trTwoCount == 2
    assert trThreeCount == 2
    assert trFourCount == 2
    assert trFiveCount == 2

    forked_handle = DBOS.fork_workflow(wfid, 4)
    assert forked_handle.workflow_id != wfid
    forked_handle.get_result()

    assert trOneCount == 1
    assert trTwoCount == 2
    assert trThreeCount == 2
    assert trFourCount == 3
    assert trFiveCount == 3

    forked_handle = DBOS.fork_workflow(wfid, 1)
    assert forked_handle.workflow_id != wfid
    forked_handle.get_result()

    assert trOneCount == 2
    assert trTwoCount == 3
    assert trThreeCount == 3
    assert trFourCount == 4
    assert trFiveCount == 4


def test_restart_fromsteps_steps_tr(
    dbos: DBOS,
) -> None:

    trOneCount = 0
    stepTwoCount = 0
    trThreeCount = 0
    stepFourCount = 0
    trFiveCount = 0

    @DBOS.workflow()
    def simple_workflow() -> None:
        trOne()
        stepTwo()
        trThree()
        stepFour()
        trFive()
        return

    @DBOS.transaction()
    def trOne() -> None:
        nonlocal trOneCount
        trOneCount += 1
        return

    @DBOS.step()
    def stepTwo() -> None:
        nonlocal stepTwoCount
        stepTwoCount += 1
        return

    @DBOS.transaction()
    def trThree() -> None:
        nonlocal trThreeCount
        trThreeCount += 1
        return

    @DBOS.step()
    def stepFour() -> None:
        nonlocal stepFourCount
        stepFourCount += 1
        return

    @DBOS.transaction()
    def trFive() -> None:
        nonlocal trFiveCount
        trFiveCount += 1
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        simple_workflow()

    assert trOneCount == 1
    assert stepTwoCount == 1
    assert trThreeCount == 1
    assert stepFourCount == 1
    assert trFiveCount == 1

    forked_handle = DBOS.fork_workflow(wfid, 3)
    assert forked_handle.workflow_id != wfid
    forked_handle.get_result()

    assert trOneCount == 1
    assert stepTwoCount == 1
    assert trThreeCount == 2
    assert stepFourCount == 2
    assert trFiveCount == 2

    forked_handle = DBOS.fork_workflow(wfid, 5)
    assert forked_handle.workflow_id != wfid
    forked_handle.get_result()

    assert trOneCount == 1
    assert stepTwoCount == 1
    assert trThreeCount == 2
    assert stepFourCount == 2
    assert trFiveCount == 3

    # invalid < 1 will default to 1
    forked_handle = DBOS.fork_workflow(wfid, -1)
    assert forked_handle.workflow_id != wfid
    forked_handle.get_result()

    assert trOneCount == 2
    assert stepTwoCount == 2
    assert trThreeCount == 3
    assert stepFourCount == 3
    assert trFiveCount == 4


def test_restart_fromsteps_childwf(
    dbos: DBOS,
) -> None:

    stepOneCount = 0
    childwfCount = 0
    stepThreeCount = 0

    @DBOS.workflow()
    def simple_workflow() -> None:
        stepOne()
        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid):
            handle = dbos.start_workflow(
                child_workflow,
                wfid,
            )
        handle.get_result()
        stepThree()
        return

    @DBOS.step()
    def stepOne() -> None:
        nonlocal stepOneCount
        stepOneCount += 1
        return

    @DBOS.workflow()
    def child_workflow(id: str) -> str:
        nonlocal childwfCount
        childwfCount += 1
        return id

    @DBOS.step()
    def stepThree() -> None:
        nonlocal stepThreeCount
        stepThreeCount += 1
        return

    @DBOS.workflow()
    def fork(workflow_id: str, step: int) -> str:
        handle = DBOS.fork_workflow(workflow_id, step)
        handle.get_result()
        return handle.get_workflow_id()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        h = DBOS.start_workflow(simple_workflow)
    h.get_result()

    assert stepOneCount == 1
    assert childwfCount == 1
    assert stepThreeCount == 1

    forked_handle = DBOS.fork_workflow(wfid, 2)
    forked_handle.get_result()
    assert forked_handle.workflow_id != wfid
    assert stepOneCount == 1
    assert childwfCount == 2
    assert stepThreeCount == 2

    forked_handle = DBOS.fork_workflow(wfid, 3)
    forked_handle.get_result()
    assert forked_handle.workflow_id != wfid
    assert stepOneCount == 1
    assert childwfCount == 2
    assert stepThreeCount == 3

    # call fork from within a workflow
    forkwfid = str(uuid.uuid4())
    with SetWorkflowID(forkwfid):
        fh = DBOS.start_workflow(fork, wfid, 1)
    firstforkedwfid = fh.get_result()
    assert firstforkedwfid != wfid
    assert stepOneCount == 2
    assert childwfCount == 3
    assert stepThreeCount == 4

    # call the workflow again with the same id
    # testing that fork is not called again
    with SetWorkflowID(forkwfid):
        fh2 = DBOS.start_workflow(fork, wfid, 1)

    secondforkedwfid = fh2.get_result()
    assert secondforkedwfid == firstforkedwfid

    assert stepOneCount == 2
    assert childwfCount == 3
    assert stepThreeCount == 4


def test_fork_version(
    dbos: DBOS,
) -> None:

    stepOneCount = 0
    stepTwoCount = 0

    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return stepOne(x) + stepTwo(x)

    @DBOS.step()
    def stepOne(x: int) -> int:
        nonlocal stepOneCount
        stepOneCount += 1
        return x + 1

    @DBOS.step()
    def stepTwo(x: int) -> int:
        nonlocal stepTwoCount
        stepTwoCount += 1
        return x + 2

    input = 1
    output = 2 * input + 3

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        assert simple_workflow(input) == output

    # Fork the workflow with a different version. Verify it is set to that version.
    new_version = "my_new_version"
    handle = DBOS.fork_workflow(workflow_id, 2, application_version=new_version)
    assert handle.get_status().app_version == new_version
    assert handle.get_status().queue_name == INTERNAL_QUEUE_NAME
    # Set the global version to this new version, verify the workflow completes
    GlobalParams.app_version = new_version
    assert handle.get_result() == output
    assert queue_entries_are_cleaned_up(dbos)
