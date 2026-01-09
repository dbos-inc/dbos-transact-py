import threading
import time
import uuid

import pytest
import sqlalchemy as sa

from dbos import DBOS, Queue, SetWorkflowID, WorkflowHandle
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
        handle = DBOS.start_workflow(simple_workflow, input)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()
    assert steps_completed == 1

    # Resume the workflow. Verify it completes successfully.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_status().app_version == DBOS.application_version
    assert handle.get_status().queue_name == INTERNAL_QUEUE_NAME
    assert handle.get_result() == input
    assert steps_completed == 2

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
