import threading
import time
import uuid

import pytest
import sqlalchemy as sa

from dbos import (
    DBOS,
    DBOSClient,
    Queue,
    SetEnqueueOptions,
    SetWorkflowID,
    WorkflowHandle,
)
from dbos._error import DBOSAwaitedWorkflowCancelledError
from dbos._schemas.application_database import ApplicationSchema
from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams
from dbos._workflow_commands import garbage_collect, global_timeout
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
        cancelled_handle = DBOS.start_workflow(simple_workflow, input)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        cancelled_handle.get_result()
    assert steps_completed == 1

    # Resume the workflow. Verify it completes successfully.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_status().app_version == DBOS.application_version
    assert handle.get_status().queue_name == INTERNAL_QUEUE_NAME
    assert handle.get_result() == input
    assert steps_completed == 2

    # The original handle should also retrieve the correct result
    assert cancelled_handle.get_result() == input

    # Resume the workflow again. Verify it does not run again.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == input
    assert steps_completed == 2

    assert queue_entries_are_cleaned_up(dbos)


def test_delete_workflow(dbos: DBOS) -> None:
    @DBOS.transaction()
    def txn(x: int) -> int:
        DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return x

    @DBOS.workflow()
    def child_workflow(x: int) -> int:
        txn(x)
        return x * 2

    @DBOS.workflow()
    def parent_workflow(x: int) -> int:
        child_handle = DBOS.start_workflow(child_workflow, x)
        return child_handle.get_result()

    # Run the parent workflow which starts a child workflow
    parent_wfid = str(uuid.uuid4())
    with SetWorkflowID(parent_wfid):
        result = parent_workflow(5)
    assert result == 10

    # Get the child workflow ID
    children = dbos._sys_db.get_workflow_children(parent_wfid)
    assert len(children) == 1
    child_wfid = children[0]

    # Verify both workflows exist
    assert DBOS.get_workflow_status(parent_wfid) is not None
    assert DBOS.get_workflow_status(child_wfid) is not None

    # Verify transaction outputs exist for the child workflow
    assert dbos._app_db
    with dbos._app_db.engine.begin() as c:
        rows = c.execute(
            sa.select(ApplicationSchema.transaction_outputs.c.workflow_uuid).where(
                ApplicationSchema.transaction_outputs.c.workflow_uuid == child_wfid
            )
        ).all()
        assert len(rows) == 1

    # Delete without delete_children - only parent should be deleted
    DBOS.delete_workflow(parent_wfid, delete_children=False)
    assert DBOS.get_workflow_status(parent_wfid) is None
    assert DBOS.get_workflow_status(child_wfid) is not None

    # Run again to test delete_children=True
    parent_wfid2 = str(uuid.uuid4())
    with SetWorkflowID(parent_wfid2):
        result = parent_workflow(7)
    assert result == 14

    children2 = dbos._sys_db.get_workflow_children(parent_wfid2)
    assert len(children2) == 1
    child_wfid2 = children2[0]

    # Verify both workflows exist
    assert DBOS.get_workflow_status(parent_wfid2) is not None
    assert DBOS.get_workflow_status(child_wfid2) is not None

    # Delete with delete_children=True - both should be deleted
    DBOS.delete_workflow(parent_wfid2, delete_children=True)
    assert DBOS.get_workflow_status(parent_wfid2) is None
    assert DBOS.get_workflow_status(child_wfid2) is None

    # Verify transaction outputs are deleted for child
    with dbos._app_db.engine.begin() as c:
        rows = c.execute(
            sa.select(ApplicationSchema.transaction_outputs.c.workflow_uuid).where(
                ApplicationSchema.transaction_outputs.c.workflow_uuid == child_wfid2
            )
        ).all()
        assert len(rows) == 0

    # Verify deleting a non-existent workflow doesn't error
    DBOS.delete_workflow(parent_wfid2, delete_children=False)


def test_bulk_cancel(dbos: DBOS) -> None:
    steps_completed = 0
    workflow_events: dict[str, threading.Event] = {}
    main_events: dict[str, threading.Event] = {}

    @DBOS.step()
    def step_one() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.step()
    def step_two() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.workflow()
    def blocking_workflow() -> str:
        wfid = DBOS.workflow_id
        assert wfid is not None
        step_one()
        main_events[wfid].set()
        workflow_events[wfid].wait()
        step_two()
        return wfid

    # Start three workflows, wait for each to reach its blocking point
    wfids: list[str] = []
    handles = []
    for _ in range(3):
        wfid = str(uuid.uuid4())
        wfids.append(wfid)
        workflow_events[wfid] = threading.Event()
        main_events[wfid] = threading.Event()
        with SetWorkflowID(wfid):
            handles.append(DBOS.start_workflow(blocking_workflow))
        main_events[wfid].wait()

    assert steps_completed == 3

    # Bulk cancel all three workflows at once
    DBOS.cancel_workflows(wfids)

    # Release all workflows so they can observe cancellation
    for evt in workflow_events.values():
        evt.set()

    for handle in handles:
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            handle.get_result()

    # step_two should not have run for any workflow
    assert steps_completed == 3

    assert queue_entries_are_cleaned_up(dbos)


def test_bulk_resume(dbos: DBOS) -> None:
    steps_completed = 0
    workflow_events: dict[str, threading.Event] = {}
    main_events: dict[str, threading.Event] = {}

    @DBOS.step()
    def step_one() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.step()
    def step_two() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.workflow()
    def blocking_workflow(x: int) -> int:
        wfid = DBOS.workflow_id
        assert wfid is not None
        step_one()
        main_events[wfid].set()
        workflow_events[wfid].wait()
        step_two()
        return x

    # Start three workflows and cancel them
    wfids: list[str] = []
    handles = []
    for i in range(3):
        wfid = str(uuid.uuid4())
        wfids.append(wfid)
        workflow_events[wfid] = threading.Event()
        main_events[wfid] = threading.Event()
        with SetWorkflowID(wfid):
            handles.append(DBOS.start_workflow(blocking_workflow, i))
        main_events[wfid].wait()

    assert steps_completed == 3

    DBOS.cancel_workflows(wfids)
    for evt in workflow_events.values():
        evt.set()
    for handle in handles:
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            handle.get_result()
    assert steps_completed == 3

    # Bulk resume all three workflows
    resumed_handles = DBOS.resume_workflows(wfids)
    assert len(resumed_handles) == 3
    for i, handle in enumerate(resumed_handles):
        assert handle.get_result() == i
    assert steps_completed == 6

    assert queue_entries_are_cleaned_up(dbos)


def test_bulk_delete(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return x

    # Run three workflows
    wfids: list[str] = []
    for i in range(3):
        wfid = str(uuid.uuid4())
        wfids.append(wfid)
        with SetWorkflowID(wfid):
            assert simple_workflow(i) == i

    # Verify all exist
    for wfid in wfids:
        assert DBOS.get_workflow_status(wfid) is not None

    # Bulk delete all three
    DBOS.delete_workflows(wfids)

    # Verify all are gone
    for wfid in wfids:
        assert DBOS.get_workflow_status(wfid) is None


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
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
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
    app_version = forked_handle.get_status().app_version
    assert app_version is None or app_version == DBOS.application_version
    assert forked_handle.get_status().forked_from == wfid
    assert forked_handle.get_result() == output

    assert stepOneCount == 1
    assert stepTwoCount == 1
    assert stepThreeCount == 2
    assert stepFourCount == 2
    assert stepFiveCount == 2

    forked_handle = DBOS.fork_workflow(wfid, 5)
    fork_id_2 = forked_handle.workflow_id
    assert forked_handle.workflow_id != wfid
    assert forked_handle.get_status().forked_from == wfid
    assert forked_handle.get_result() == output

    assert stepOneCount == 1
    assert stepTwoCount == 1
    assert stepThreeCount == 2
    assert stepFourCount == 2
    assert stepFiveCount == 3

    forked_handle = DBOS.fork_workflow(wfid, 1)
    fork_id_3 = forked_handle.workflow_id
    assert forked_handle.workflow_id != wfid
    assert forked_handle.get_status().forked_from == wfid
    assert forked_handle.get_result() == output

    assert stepOneCount == 2
    assert stepTwoCount == 2
    assert stepThreeCount == 3
    assert stepFourCount == 3
    assert stepFiveCount == 4

    forks = DBOS.list_workflows(forked_from=wfid)
    assert len(forks) == 3
    assert [f.workflow_id for f in forks] == [fork_id, fork_id_2, fork_id_3]

    # The original workflow should be marked as having been forked from.
    original_status = DBOS.get_workflow_status(wfid)
    assert original_status is not None
    assert original_status.was_forked_from is True
    # Forked workflows are not themselves forked from.
    for fork in forks:
        assert fork.was_forked_from is False


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
    fork_id_one = forked_handle.workflow_id
    forked_handle.get_result()

    assert trOneCount == 1
    assert trTwoCount == 2
    assert trThreeCount == 2
    assert trFourCount == 2
    assert trFiveCount == 2

    forked_handle = DBOS.fork_workflow(wfid, 4)
    assert forked_handle.workflow_id != wfid
    fork_id_two = forked_handle.workflow_id
    forked_handle.get_result()

    assert trOneCount == 1
    assert trTwoCount == 2
    assert trThreeCount == 2
    assert trFourCount == 3
    assert trFiveCount == 3

    forked_handle = DBOS.fork_workflow(wfid, 1)
    assert forked_handle.workflow_id != wfid
    fork_id_three = forked_handle.workflow_id
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


def test_resume_and_fork_to_queue(dbos: DBOS) -> None:
    step_one_count = 0
    step_two_count = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.step()
    def step_one(x: int) -> int:
        nonlocal step_one_count
        step_one_count += 1
        return x + 1

    @DBOS.step()
    def step_two(x: int) -> int:
        nonlocal step_two_count
        step_two_count += 1
        return x + 2

    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        a = step_one(x)
        main_thread_event.set()
        workflow_event.wait()
        b = step_two(x)
        return a + b

    queue = Queue("test_resume_fork_queue")
    input = 5
    output = (input + 1) + (input + 2)

    # Enqueue workflow, let step_one run, then cancel before step_two
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = queue.enqueue(simple_workflow, input)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(Exception):
        handle.get_result()
    assert step_one_count == 1
    assert step_two_count == 0

    # Resume the workflow onto the queue and verify queue_name
    resumed_handle = DBOS.resume_workflow(wfid, queue_name="test_resume_fork_queue")
    assert resumed_handle.get_status().queue_name == "test_resume_fork_queue"
    assert resumed_handle.get_result() == output
    assert step_one_count == 1  # Step 1 replayed from checkpoint
    assert step_two_count == 1

    # Fork the workflow onto the queue from step 2 and verify queue_name
    forked_handle = DBOS.fork_workflow(
        wfid, 2, queue_name="test_resume_fork_queue", queue_partition_key="my_partition"
    )
    assert forked_handle.get_status().queue_name == "test_resume_fork_queue"
    assert forked_handle.get_status().forked_from == wfid
    assert forked_handle.get_result() == output
    assert step_one_count == 1  # Step 1 replayed from checkpoint
    assert step_two_count == 2  # Step 2 was re-executed

    assert queue_entries_are_cleaned_up(dbos)


def test_garbage_collection(dbos: DBOS, skip_with_sqlite_imprecise_time: None) -> None:
    event = threading.Event()

    @DBOS.step()
    def step(x: int) -> int:
        return x

    @DBOS.transaction()
    def txn(x: int) -> int:
        DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return x

    @DBOS.workflow()
    def workflow(x: int) -> int:
        step(x)
        txn(x)
        return x

    @DBOS.workflow()
    def blocked_workflow() -> str:
        txn(0)
        event.wait()
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    num_workflows = 10

    handle = DBOS.start_workflow(blocked_workflow)
    for i in range(num_workflows):
        assert workflow(i) == i

    # Garbage collect all but one workflow
    garbage_collect(dbos, cutoff_epoch_timestamp_ms=None, rows_threshold=1)
    # Verify two workflows remain: the newest and the blocked workflow
    workflows = DBOS.list_workflows()
    assert len(workflows) == 2
    assert workflows[0].workflow_id == handle.workflow_id
    # Verify txn outputs are preserved only for the remaining workflows
    assert dbos._app_db
    with dbos._app_db.engine.begin() as c:
        rows = c.execute(
            sa.select(
                ApplicationSchema.transaction_outputs.c.workflow_uuid,
            )
        ).all()
        assert len(rows) == 2

    # Garbage collect all previous workflows
    garbage_collect(
        dbos, cutoff_epoch_timestamp_ms=int(time.time() * 1000), rows_threshold=None
    )
    # Verify only the blocked workflow remains
    workflows = DBOS.list_workflows()
    assert len(workflows) == 1
    assert workflows[0].workflow_id == handle.workflow_id
    # Verify txn outputs are preserved only for the remaining workflow
    with dbos._app_db.engine.begin() as c:
        rows = c.execute(
            sa.select(
                ApplicationSchema.transaction_outputs.c.workflow_uuid,
            )
        ).all()
        assert len(rows) == 1

    # Finish the blocked workflow, garbage collect everything
    event.set()
    assert handle.get_result() is not None
    garbage_collect(
        dbos, cutoff_epoch_timestamp_ms=int(time.time() * 1000), rows_threshold=None
    )
    # Verify only the blocked workflow remains
    workflows = DBOS.list_workflows()
    assert len(workflows) == 0

    # ENQUEUED and DELAYED workflows must not be garbage collected
    queue = Queue("gc_test_queue")

    @DBOS.workflow()
    def gc_test_workflow() -> None:
        pass

    enqueued_handle = queue.enqueue(gc_test_workflow)
    with SetEnqueueOptions(delay_seconds=60.0):
        delayed_handle = queue.enqueue(gc_test_workflow)

    garbage_collect(
        dbos, cutoff_epoch_timestamp_ms=int(time.time() * 1000), rows_threshold=None
    )
    workflows = DBOS.list_workflows()
    wf_ids = {w.workflow_id for w in workflows}
    assert enqueued_handle.workflow_id in wf_ids
    assert delayed_handle.workflow_id in wf_ids

    # Clean up so they don't interfere with the rest of the test
    DBOS.cancel_workflow(enqueued_handle.workflow_id)
    DBOS.cancel_workflow(delayed_handle.workflow_id)

    # Verify GC runs without error on a blank table
    garbage_collect(dbos, cutoff_epoch_timestamp_ms=None, rows_threshold=1)

    # Run workflows, wait, run them again
    for i in range(num_workflows):
        assert workflow(i) == i
    time.sleep(1)
    for i in range(num_workflows):
        assert workflow(i) == i

    # GC the first half, verify only half were GC'ed
    garbage_collect(
        dbos,
        cutoff_epoch_timestamp_ms=int(time.time() * 1000) - 1000,
        rows_threshold=None,
    )
    workflows = DBOS.list_workflows()
    assert len(workflows) == num_workflows


def test_global_timeout(dbos: DBOS) -> None:
    event = threading.Event()

    @DBOS.workflow()
    def blocked_workflow() -> str:
        while not event.wait(0):
            DBOS.sleep(0.1)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    num_workflows = 10
    handles = [DBOS.start_workflow(blocked_workflow) for _ in range(num_workflows)]

    # Wait two seconds, start one final workflow, then timeout all workflows started more than one second ago
    time.sleep(2)
    final_handle = DBOS.start_workflow(blocked_workflow)
    cutoff_epoch_timestamp_ms = int(time.time() * 1000) - 1000
    global_timeout(dbos, cutoff_epoch_timestamp_ms)

    # Verify all workflows started before the global timeout are cancelled
    for handle in handles:
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            handle.get_result()
    event.set()
    assert final_handle.get_result() is not None


def test_fork_events(dbos: DBOS) -> None:
    key = "key"
    event = threading.Event()

    @DBOS.step()
    def step(val: int) -> None:
        DBOS.set_event(key, val)

    @DBOS.workflow()
    def workflow() -> str:
        event.wait()
        DBOS.set_event(key, 0)
        DBOS.set_event(key, 1)
        step(2)
        assert DBOS.workflow_id
        return DBOS.workflow_id

    # Verify the workflow runs and the event's final value is correct
    event.set()
    handle = DBOS.start_workflow(workflow)
    assert handle.get_result() == handle.workflow_id
    assert DBOS.get_event(handle.workflow_id, key) == 2

    # Block the workflow so forked workflows cannot advance
    event.clear()

    # Fork the workflow from each step, verify the event is set to the appropriate value
    fork_one = DBOS.fork_workflow(handle.workflow_id, 1)
    assert DBOS.get_event(fork_one.workflow_id, key, timeout_seconds=0.0) is None
    fork_two = DBOS.fork_workflow(handle.workflow_id, 2)
    assert DBOS.get_event(fork_two.workflow_id, key) == 0
    fork_three = DBOS.fork_workflow(handle.workflow_id, 3)
    assert DBOS.get_event(fork_three.workflow_id, key) == 1
    fork_four = DBOS.fork_workflow(handle.workflow_id, 4)
    assert DBOS.get_event(fork_four.workflow_id, key) == 2
    # Fork from a fork
    fork_five = DBOS.fork_workflow(fork_four.workflow_id, 4)
    assert DBOS.get_event(fork_four.workflow_id, key) == 2

    # Unblock the forked workflows, verify they successfully complete
    event.set()
    for handle in [fork_one, fork_two, fork_three, fork_four, fork_five]:
        assert handle.get_result()
        assert DBOS.get_event(handle.workflow_id, key) == 2


def test_fork_streams(dbos: DBOS) -> None:
    key = "key"
    event = threading.Event()

    def read_stream(id: str, x: int) -> list[int]:
        gen = DBOS.read_stream(id, key)
        return [next(gen) for _ in range(x)]

    @DBOS.step()
    def step(val: int) -> None:
        DBOS.write_stream(key, val)

    @DBOS.workflow()
    def workflow() -> str:
        event.wait()
        DBOS.write_stream(key, 0)
        DBOS.write_stream(key, 1)
        step(2)
        DBOS.close_stream(key)
        assert DBOS.workflow_id
        return DBOS.workflow_id

    # Verify the workflow runs and streams the appropriate values
    event.set()
    handle = DBOS.start_workflow(workflow)
    assert handle.get_result() == handle.workflow_id
    assert list(DBOS.read_stream(handle.workflow_id, key)) == [0, 1, 2]

    # Block the workflow so forked workflows cannot advance
    event.clear()

    # Fork the workflow from each step, verify the stream contains the appropriate values
    fork_one = DBOS.fork_workflow(handle.workflow_id, 1)
    assert read_stream(fork_one.workflow_id, 0) == []
    fork_two = DBOS.fork_workflow(handle.workflow_id, 2)
    assert read_stream(fork_two.workflow_id, 1) == [0]
    fork_three = DBOS.fork_workflow(handle.workflow_id, 3)
    assert read_stream(fork_three.workflow_id, 2) == [0, 1]
    fork_four = DBOS.fork_workflow(handle.workflow_id, 4)
    assert read_stream(fork_four.workflow_id, 3) == [0, 1, 2]
    fork_five = DBOS.fork_workflow(handle.workflow_id, 5)
    assert list(DBOS.read_stream(fork_five.workflow_id, key)) == [0, 1, 2]

    # Unblock the forked workflows, verify they successfully complete
    event.set()
    for handle in [fork_one, fork_two, fork_three, fork_four, fork_five]:
        assert handle.get_result()
        assert list(DBOS.read_stream(handle.workflow_id, key)) == [0, 1, 2]


def test_bulk_fork_from_last_failed_step(dbos: DBOS) -> None:
    step_one_count = 0
    step_two_count = 0
    step_three_count = 0

    @DBOS.step()
    def step_one() -> int:
        nonlocal step_one_count
        step_one_count += 1
        return 1

    @DBOS.step()
    def step_two() -> int:
        nonlocal step_two_count
        step_two_count += 1
        if step_two_count == 1:  # fail on first call only (wf1)
            raise Exception("step two failed")
        return 2

    @DBOS.step()
    def step_three() -> int:
        nonlocal step_three_count
        step_three_count += 1
        if step_three_count == 1:  # fail on first call only (wf2)
            raise Exception("step three failed")
        return 3

    @DBOS.workflow()
    def three_step_workflow() -> int:
        return step_one() + step_two() + step_three()

    # wf1: step 2 fails → last failed step is 2
    wf1_id = str(uuid.uuid4())
    with SetWorkflowID(wf1_id):
        with pytest.raises(Exception, match="step two failed"):
            three_step_workflow()
    assert step_one_count == 1
    assert step_two_count == 1
    assert step_three_count == 0

    # wf2: step 2 succeeds, step 3 fails → last failed step is 3
    wf2_id = str(uuid.uuid4())
    with SetWorkflowID(wf2_id):
        with pytest.raises(Exception, match="step three failed"):
            three_step_workflow()
    assert step_one_count == 2
    assert step_two_count == 2
    assert step_three_count == 1

    # wf3: all steps succeed → no failed step, fall back to last step (3)
    wf3_id = str(uuid.uuid4())
    with SetWorkflowID(wf3_id):
        assert three_step_workflow() == 6
    assert step_one_count == 3
    assert step_two_count == 3
    assert step_three_count == 2

    # Bulk fork all three
    forked_ids = dbos._sys_db.bulk_fork_from_last_failed_step(
        [wf1_id, wf2_id, wf3_id],
        application_version=None,
    )
    assert len(forked_ids) == 3

    fork1: WorkflowHandle[int] = DBOS.retrieve_workflow(forked_ids[0])
    fork2: WorkflowHandle[int] = DBOS.retrieve_workflow(forked_ids[1])
    fork3: WorkflowHandle[int] = DBOS.retrieve_workflow(forked_ids[2])
    assert fork1.get_result() == 6
    assert fork2.get_result() == 6
    assert fork3.get_result() == 6

    # fork1 re-ran from step 2: step_one replayed, step_two and step_three re-executed
    # fork2 re-ran from step 3: step_one and step_two replayed, step_three re-executed
    # fork3 re-ran from step 3: step_one and step_two replayed, step_three re-executed
    assert step_one_count == 3  # replayed for all three forks
    assert step_two_count == 4  # re-run for fork1 only
    assert step_three_count == 5  # re-run for all three forks

    # All three originals should be marked as having been forked from.
    for wid in [wf1_id, wf2_id, wf3_id]:
        wid_status = DBOS.get_workflow_status(wid)
        assert wid_status is not None
        assert wid_status.was_forked_from is True
    # The forked workflows themselves should not be marked.
    for fid in forked_ids:
        fid_status = DBOS.get_workflow_status(fid)
        assert fid_status is not None
        assert fid_status.was_forked_from is False


def test_get_all_events(dbos: DBOS) -> None:
    @DBOS.workflow()
    def event_workflow() -> str:
        DBOS.set_event("key1", "value1")
        DBOS.set_event("key2", 42)
        DBOS.set_event("key1", "updated")
        return DBOS.workflow_id  # type: ignore

    handle = DBOS.start_workflow(event_workflow)
    wfid = handle.get_result()

    events = dbos._sys_db.get_all_events(wfid)
    assert events == {"key1": "updated", "key2": 42}

    # Empty workflow has no events
    empty_events = dbos._sys_db.get_all_events("nonexistent")
    assert empty_events == {}


def test_get_all_notifications(dbos: DBOS) -> None:
    recv_event = threading.Event()

    @DBOS.workflow()
    def receiver_workflow() -> str:
        DBOS.recv(topic="topic_a")
        DBOS.recv(topic="topic_b")
        recv_event.set()
        return DBOS.workflow_id  # type: ignore

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(receiver_workflow)

    # Send messages to the receiver workflow (two consumed, one unconsumed)
    DBOS.send(wfid, "hello", topic="topic_a")
    DBOS.send(wfid, {"data": 123}, topic="topic_b")
    recv_event.wait()
    handle.get_result()

    # Send an extra message that the workflow never consumes
    DBOS.send(wfid, "unconsumed", topic="topic_c")

    notifications = dbos._sys_db.get_all_notifications(wfid)
    assert len(notifications) == 3
    assert notifications[0]["topic"] == "topic_a"
    assert notifications[0]["message"] == "hello"
    assert notifications[0]["consumed"] is True
    assert notifications[1]["topic"] == "topic_b"
    assert notifications[1]["message"] == {"data": 123}
    assert notifications[1]["consumed"] is True
    assert notifications[2]["topic"] == "topic_c"
    assert notifications[2]["message"] == "unconsumed"
    assert notifications[2]["consumed"] is False

    # Nonexistent workflow has no notifications
    assert dbos._sys_db.get_all_notifications("nonexistent") == []


def test_get_all_notifications_null_topic(dbos: DBOS) -> None:
    recv_event = threading.Event()

    @DBOS.workflow()
    def receiver_workflow() -> str:
        DBOS.recv()
        recv_event.set()
        return DBOS.workflow_id  # type: ignore

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(receiver_workflow)

    DBOS.send(wfid, "no_topic_msg")
    recv_event.wait()
    handle.get_result()

    notifications = dbos._sys_db.get_all_notifications(wfid)
    assert len(notifications) == 1
    assert notifications[0]["topic"] is None
    assert notifications[0]["message"] == "no_topic_msg"


def test_get_all_stream_entries(dbos: DBOS) -> None:
    @DBOS.workflow()
    def stream_workflow() -> str:
        DBOS.write_stream("stream_a", 10)
        DBOS.write_stream("stream_a", 20)
        DBOS.write_stream("stream_b", "hello")
        DBOS.close_stream("stream_a")
        DBOS.close_stream("stream_b")
        return DBOS.workflow_id  # type: ignore

    handle = DBOS.start_workflow(stream_workflow)
    wfid = handle.get_result()

    streams = dbos._sys_db.get_all_stream_entries(wfid)
    assert streams == {"stream_a": [10, 20], "stream_b": ["hello"]}

    # Nonexistent workflow has no streams
    assert dbos._sys_db.get_all_stream_entries("nonexistent") == {}


def test_client_delete_workflow(client: DBOSClient, dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return x

    # Test single delete
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert simple_workflow(1) == 1
    assert len(client.list_workflows(workflow_ids=[wfid])) == 1
    client.delete_workflow(wfid)
    assert len(client.list_workflows(workflow_ids=[wfid])) == 0

    # Test bulk delete
    wfids: list[str] = []
    for i in range(3):
        wfid = str(uuid.uuid4())
        wfids.append(wfid)
        with SetWorkflowID(wfid):
            assert simple_workflow(i) == i
    assert len(client.list_workflows(workflow_ids=wfids)) == 3
    client.delete_workflows(wfids)
    assert len(client.list_workflows(workflow_ids=wfids)) == 0
