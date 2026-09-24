import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait
from time import sleep
from typing import Any, List

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

from dbos import DBOS, SendMessage, SetWorkflowID
from dbos._core import ActiveWorkflowById
from dbos._debug_trigger import DebugAction, DebugTriggers
from dbos._error import DBOSAwaitedWorkflowCancelledError
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import ThreadSafeEventDict
from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams, LoopAwareEvent
from tests.conftest import (
    reexecute_workflow_by_id,
    retry_until_success,
    set_workflow_status,
)


def test_simple_workflow(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class TryConcExec:
        conc_exec = 0
        max_conc = 0

        conc_wf = 0
        max_wf = 0
        step_runs = 0

        @DBOS.step()
        @staticmethod
        def testConcStep() -> None:
            TryConcExec.step_runs += 1
            TryConcExec.conc_exec += 1
            TryConcExec.max_conc = max(TryConcExec.conc_exec, TryConcExec.max_conc)
            sleep(1)
            TryConcExec.conc_exec -= 1

        @DBOS.workflow()
        @staticmethod
        def testConcWorkflow() -> None:
            TryConcExec.conc_wf += 1
            TryConcExec.max_wf = max(TryConcExec.conc_wf, TryConcExec.max_wf)
            sleep(0.5)
            TryConcExec.testConcStep()
            sleep(0.5)
            TryConcExec.conc_wf -= 1

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        wfh1 = DBOS.start_workflow(TryConcExec.testConcWorkflow)
    with SetWorkflowID(wfid):
        wfh2 = DBOS.start_workflow(TryConcExec.testConcWorkflow)

    wfh1.get_result()
    wfh2.get_result()
    assert TryConcExec.max_conc == 1
    assert TryConcExec.max_wf == 1

    # Recovery part
    set_workflow_status(dbos._sys_db, wfid, "PENDING")
    for handle in DBOS._recover_pending_workflows():
        handle.get_result()

    assert TryConcExec.max_conc == 1
    assert TryConcExec.max_wf == 1

    # Direct exec part
    def run(wfid: str) -> None:
        with SetWorkflowID(wfid):
            TryConcExec.testConcWorkflow()

    wfid2 = str(uuid.uuid4())
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(run, wfid2),
            executor.submit(run, wfid2),
        ]
        wait(futures)

    assert TryConcExec.max_conc == 1
    assert TryConcExec.max_wf == 1

    # Two dequeue dispatches of one ID: each takes ownership in turn, so the first
    # stops at its checkpoint and adopts the second's outcome. Their bodies may overlap.
    step_runs_before = TryConcExec.step_runs
    wfh1r = reexecute_workflow_by_id(dbos, wfid)
    wfh2r = reexecute_workflow_by_id(dbos, wfid)
    assert wfh1r.get_result() is None
    assert wfh2r.get_result() is None
    # The step was already checkpointed, so neither dispatch runs its body again.
    assert TryConcExec.step_runs == step_runs_before
    status = DBOS.get_workflow_status(wfid)
    assert status is not None and status.status == "SUCCESS"


def test_step_undoredo(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class CatchPlainException1:
        execNum = 0
        started = False
        completed = False
        aborted = False
        trouble = False

        @DBOS.step()
        @staticmethod
        def testStartAction() -> None:
            sleep(1)
            CatchPlainException1.started = True

        @DBOS.step()
        @staticmethod
        def testCompleteAction() -> None:
            assert CatchPlainException1.started
            sleep(1)
            CatchPlainException1.completed = True

        @DBOS.step()
        @staticmethod
        def testCancelAction() -> None:
            CatchPlainException1.aborted = True
            CatchPlainException1.started = False

        @staticmethod
        def reportTrouble() -> None:
            CatchPlainException1.trouble = True
            assert str("Trouble?") == "None!"

        @DBOS.workflow()
        @staticmethod
        def testConcWorkflow() -> None:
            try:
                # Step 1, tell external system to start processing
                CatchPlainException1.testStartAction()
            except Exception:
                # If we fail for any reason, try to abort
                try:
                    CatchPlainException1.testCancelAction()
                except Exception:
                    # Take some other notification action (sysadmin!)
                    CatchPlainException1.reportTrouble()

            # Step 2, finish the process
            CatchPlainException1.testCompleteAction()

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        wfh1 = DBOS.start_workflow(CatchPlainException1.testConcWorkflow)
    with SetWorkflowID(wfid):
        wfh2 = DBOS.start_workflow(CatchPlainException1.testConcWorkflow)

    wfh1.get_result()
    wfh2.get_result()

    print(
        f"Started: {CatchPlainException1.started}; "
        f"Completed: {CatchPlainException1.completed}; "
        f"Aborted: {CatchPlainException1.aborted}; "
        f"Trouble: {CatchPlainException1.trouble}"
    )
    assert CatchPlainException1.started
    assert CatchPlainException1.completed
    assert not CatchPlainException1.trouble


def test_step_undoredo2(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class UsingFinallyClause:
        execNum = 0
        started = False
        completed = False
        aborted = False
        trouble = False

        @DBOS.step()
        @staticmethod
        def testStartAction() -> None:
            sleep(1)
            UsingFinallyClause.started = True

        @DBOS.step()
        @staticmethod
        def testCompleteAction() -> None:
            assert UsingFinallyClause.started
            sleep(1)
            UsingFinallyClause.completed = True

        @DBOS.step()
        @staticmethod
        def testCancelAction() -> None:
            UsingFinallyClause.aborted = True
            UsingFinallyClause.started = False

        @staticmethod
        def reportTrouble() -> None:
            UsingFinallyClause.trouble = True
            assert str("Trouble?") == "None!"

        @DBOS.workflow()
        @staticmethod
        def testConcWorkflow() -> None:
            finished = False
            try:
                # Step 1, tell external system to start processing
                UsingFinallyClause.testStartAction()

                # Step 2, finish the process
                UsingFinallyClause.testCompleteAction()

                finished = True
            finally:
                if not finished:
                    # If we fail for any reason, try to abort
                    try:
                        UsingFinallyClause.testCancelAction()
                    except Exception:
                        UsingFinallyClause.reportTrouble()

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        wfh1 = DBOS.start_workflow(UsingFinallyClause.testConcWorkflow)
    with SetWorkflowID(wfid):
        wfh2 = DBOS.start_workflow(UsingFinallyClause.testConcWorkflow)

    wfh1.get_result()
    wfh2.get_result()

    print(
        f"Started: {UsingFinallyClause.started}; "
        f"Completed: {UsingFinallyClause.completed}; "
        f"Aborted: {UsingFinallyClause.aborted}; "
        f"Trouble: {UsingFinallyClause.trouble}"
    )
    assert UsingFinallyClause.started
    assert UsingFinallyClause.completed
    assert not UsingFinallyClause.trouble


def test_step_sequence(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class TryConcExec2:
        curExec = 0
        curStep = 0

        @DBOS.step()
        @staticmethod
        def step1() -> None:
            # This makes the step take a while ... sometimes.
            if TryConcExec2.curExec % 2 == 0:
                TryConcExec2.curExec += 1
                sleep(1)
            TryConcExec2.curStep = 1

        @DBOS.step()
        @staticmethod
        def step2() -> None:
            TryConcExec2.curStep = 2

        @DBOS.workflow()
        @staticmethod
        def testConcWorkflow() -> None:
            TryConcExec2.step1()
            TryConcExec2.step2()

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        wfh1 = DBOS.start_workflow(TryConcExec2.testConcWorkflow)
    with SetWorkflowID(wfid):
        wfh2 = DBOS.start_workflow(TryConcExec2.testConcWorkflow)

    wfh1.get_result()
    wfh2.get_result()
    assert TryConcExec2.curStep == 2


def test_commit_hiccup(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class TryDbGlitch:
        @DBOS.step()
        @staticmethod
        def step1() -> str:
            sleep(1)
            return "Yay!"

        @DBOS.workflow()
        @staticmethod
        def testWorkflow() -> str:
            return TryDbGlitch.step1()

    assert TryDbGlitch.testWorkflow() == "Yay!"
    DebugTriggers.set_debug_trigger(
        DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT,
        DebugAction().set_exception_to_throw(
            OperationalError(
                statement=None,
                params=None,
                orig=BaseException("Connection lost"),
                connection_invalidated=True,
            )
        ),
    )

    assert TryDbGlitch.testWorkflow() == "Yay!"
    DebugTriggers.set_debug_trigger(
        DebugTriggers.DEBUG_TRIGGER_INITWF_COMMIT,
        DebugAction().set_exception_to_throw(
            OperationalError(
                statement=None,
                params=None,
                orig=BaseException("Connection lost"),
                connection_invalidated=True,
            )
        ),
    )
    assert TryDbGlitch.testWorkflow() == "Yay!"


def test_status_wf(dbos: DBOS) -> None:
    """Test use of name `status`."""

    @DBOS.step()
    def stepf(s: str) -> None:
        print(s)

    @DBOS.workflow()
    def status_workflow(status: str = "None") -> None:
        stepf(status)

    status_workflow()
    status_workflow(status="Starting")
    DBOS.start_workflow(status_workflow).get_result()
    DBOS.start_workflow(status_workflow, status="Ending").get_result()


def _step_names(wfid: str) -> List[str]:
    return [step["function_name"] for step in DBOS.list_workflow_steps(wfid)]


@pytest.mark.parametrize("handoff", ["resume", "recovery"])
def test_handoff_parks_live_execution(dbos: DBOS, handoff: str) -> None:
    """A running execution whose workflow is handed to another stops at its next
    checkpoint, and the new owner runs alongside it in the same process and finishes."""
    release = threading.Event()
    calls = {"blocked": 0, "after": 0}

    @DBOS.step()
    def blocked_step() -> str:
        calls["blocked"] += 1
        assert release.wait(30)
        return "blocked"

    @DBOS.step()
    def after_step() -> str:
        calls["after"] += 1
        return "after"

    @DBOS.workflow()
    def handed_off_workflow() -> str:
        return blocked_step() + after_step()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(handed_off_workflow)

    try:

        def blocked() -> None:
            assert calls["blocked"] == 1

        retry_until_success(blocked, interval=0.1, max_attempts=100)
        first_owner = dbos._sys_db.get_workflow_owner(wfid)
        assert first_owner is not None

        if handoff == "resume":
            DBOS.resume_workflow(wfid)
        else:
            assert dbos._sys_db.reenqueue_for_recovery(
                wfid, [GlobalParams.executor_id], INTERNAL_QUEUE_NAME
            )

        def reclaimed() -> None:
            owner = dbos._sys_db.get_workflow_owner(wfid)
            assert owner is not None and owner != first_owner

        retry_until_success(reclaimed, interval=0.1, max_attempts=100)

        def redispatched() -> None:
            # The new owner started without waiting for the stale execution to let go.
            assert calls["blocked"] == 2

        retry_until_success(redispatched, interval=0.1, max_attempts=100)
        release.set()

        assert handle.get_result() == "blockedafter"
        assert DBOS.retrieve_workflow(wfid).get_result() == "blockedafter"
        # The stale execution's step result was refused, and it never ran on past it.
        assert calls == {"blocked": 2, "after": 1}
        assert len(_step_names(wfid)) == 2
    finally:
        # A failed assertion must not leave the step blocked, which wedges shutdown.
        release.set()


def test_waiters_keep_their_own_events() -> None:
    """Each waiter on a key keeps its own event: one waiter's wake or clear never affects another."""
    registry = ThreadSafeEventDict()
    first_event = LoopAwareEvent()
    registry.add("wf::topic", first_event, ("wf", "topic"))
    first_event.set()

    # A waiter joining after an earlier one was woken starts unset.
    second_event = LoopAwareEvent()
    registry.add("wf::topic", second_event, ("wf", "topic"))
    assert not second_event.is_set()

    # A signal wakes every waiter, and clearing one leaves the others set.
    first_event.clear()
    signal = registry.get("wf::topic")
    assert signal is not None
    signal.set()
    assert first_event.is_set() and second_event.is_set()
    first_event.clear()
    assert second_event.is_set()

    # The key stays registered until its last waiter leaves.
    registry.pop("wf::topic", first_event)
    assert registry.get("wf::topic") is signal
    registry.pop("wf::topic", second_event)
    assert registry.get("wf::topic") is None


def test_recv_ignores_a_stale_waiters_wake(dbos: DBOS) -> None:
    """A recv that joins a leftover, already-woken waiter still waits for its message."""

    @DBOS.workflow()
    def recv_workflow() -> str:
        return str(DBOS.recv("topic", timeout_seconds=30))

    wfid = str(uuid.uuid4())
    payload = f"{wfid}::topic"
    # What a stale execution leaves behind between its wake and its cleanup.
    stale_event = LoopAwareEvent()
    stale_event.set()
    dbos._sys_db.notifications_map.add(payload, stale_event, (wfid, "topic"))
    try:
        with SetWorkflowID(wfid):
            handle = DBOS.start_workflow(recv_workflow)

        def joined() -> None:
            signal = dbos._sys_db.notifications_map.get(payload)
            assert signal is not None and signal.waiter_count() == 2

        retry_until_success(joined, interval=0.1, max_attempts=100)
        DBOS.send(wfid, "hello", "topic")
        assert handle.get_result() == "hello"
    finally:
        dbos._sys_db.notifications_map.pop(payload, stale_event)


def test_active_entries_release_their_own_bucket() -> None:
    """A resumed workflow's executions can sit in different queues; each release removes its own."""
    active = ActiveWorkflowById()
    first = active.add("wf", "A", None)
    second = active.add("wf", "B", None)
    # The older entry first: removing the most recent bucket instead would drop B.
    first.release()
    first.release()  # idempotent
    assert active.count_for_queue("A") == 0
    assert active.count_for_queue("B") == 1
    assert active.activeList() == ["wf"]
    second.release()
    assert active.activeList() == []


def _steal_ownership(dbos: DBOS, wfid: str) -> None:
    """Hand the workflow to another execution without changing its status, as a resume's claim does."""
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
            .values(execution_xid="another-execution")
        )


def test_lost_ownership_parks_at_a_sleep(dbos: DBOS) -> None:
    """A stale execution reaching DBOS.sleep parks there instead of sleeping the full duration."""
    release = threading.Event()
    started = threading.Event()

    @DBOS.workflow()
    def sleeping_workflow() -> str:
        started.set()
        assert release.wait(30)
        DBOS.sleep(20)
        return "slept"

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(sleeping_workflow)
    try:
        assert started.wait(10)
        _steal_ownership(dbos, wfid)
        release.set()

        def parked() -> None:
            # Released before parking: the stale execution gave up at the sleep's refused checkpoint.
            assert wfid not in dbos._active_workflows_set.activeList()

        # Well inside the 20s it would otherwise sleep.
        retry_until_success(parked, interval=0.1, max_attempts=100)
        assert _step_names(wfid) == []
    finally:
        release.set()
        # Nobody else will write an outcome: cancel so the parked execution returns.
        DBOS.cancel_workflow(wfid)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()


@pytest.mark.parametrize("write", ["set_event", "write_stream", "close_stream"])
def test_stale_step_cannot_write_events_or_streams(dbos: DBOS, write: str) -> None:
    """A step of an execution that lost ownership cannot set events or write streams."""
    release = threading.Event()
    started = threading.Event()

    @DBOS.step()
    def writing_step() -> None:
        started.set()
        assert release.wait(30)
        if write == "set_event":
            DBOS.set_event("key", "stale")
        elif write == "write_stream":
            DBOS.write_stream("key", "stale")
        else:
            DBOS.close_stream("key")

    @DBOS.workflow()
    def writing_workflow() -> str:
        writing_step()
        return "done"

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(writing_workflow)
    try:
        assert started.wait(10)
        _steal_ownership(dbos, wfid)
        release.set()

        def parked() -> None:
            assert wfid not in dbos._active_workflows_set.activeList()

        retry_until_success(parked, interval=0.1, max_attempts=100)
        assert dbos._sys_db.get_all_events(wfid) == {}
        with dbos._sys_db.engine.begin() as c:
            streamed = c.execute(
                sa.select(sa.func.count())
                .select_from(SystemSchema.streams)
                .where(SystemSchema.streams.c.workflow_uuid == wfid)
            ).scalar()
        assert streamed == 0
        assert _step_names(wfid) == []
    finally:
        release.set()
        # Nobody else will write an outcome: cancel so the parked execution returns.
        DBOS.cancel_workflow(wfid)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()


def test_stale_parent_cannot_start_an_inline_child(dbos: DBOS) -> None:
    """A parent that lost ownership cannot insert or run a directly invoked child."""
    release = threading.Event()
    started = threading.Event()
    child_calls = {"n": 0}
    child_id = str(uuid.uuid4())

    @DBOS.workflow()
    def child_workflow() -> str:
        child_calls["n"] += 1
        return "child"

    @DBOS.workflow()
    def parent_workflow() -> str:
        started.set()
        assert release.wait(30)
        with SetWorkflowID(child_id):
            return child_workflow()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(parent_workflow)
    try:
        assert started.wait(10)
        _steal_ownership(dbos, wfid)
        release.set()

        def parked() -> None:
            assert wfid not in dbos._active_workflows_set.activeList()

        retry_until_success(parked, interval=0.1, max_attempts=100)
        # Refused at the child's insert: no child row, no child run, no parent step.
        assert child_calls["n"] == 0
        assert DBOS.get_workflow_status(child_id) is None
        assert _step_names(wfid) == []
    finally:
        release.set()
        DBOS.cancel_workflow(wfid)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()


def test_handoff_while_waiting_in_recv(dbos: DBOS) -> None:
    """A stale execution blocked in recv shares the topic's waiter with its
    replacement; the message goes to the owner, and the stale execution parks."""
    calls = {"recv": 0}

    @DBOS.workflow()
    def recv_workflow() -> str:
        calls["recv"] += 1
        message = DBOS.recv("topic", timeout_seconds=30)
        return str(message)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(recv_workflow)

    def waiting() -> None:
        assert calls["recv"] == 1
        assert dbos._sys_db.notifications_map.get(f"{wfid}::topic") is not None

    retry_until_success(waiting, interval=0.1, max_attempts=100)
    first_owner = dbos._sys_db.get_workflow_owner(wfid)
    DBOS.resume_workflow(wfid)

    def redispatched() -> None:
        assert dbos._sys_db.get_workflow_owner(wfid) not in (None, first_owner)
        assert calls["recv"] == 2

    retry_until_success(redispatched, interval=0.1, max_attempts=100)
    # Two messages in one transaction, so whichever execution consumes second finds one.
    DBOS.send_bulk(
        [
            SendMessage(destination_id=wfid, message="hello", topic="topic"),
            SendMessage(destination_id=wfid, message="second", topic="topic"),
        ]
    )

    assert handle.get_result() == "hello"
    assert DBOS.retrieve_workflow(wfid).get_result() == "hello"
    # The stale execution's consume rolled back with its refused checkpoint, so the
    # second message is still waiting; a leaked consume would have taken it.
    with dbos._sys_db.engine.connect() as c:
        unconsumed = c.execute(
            sa.select(SystemSchema.notifications.c.message).where(
                SystemSchema.notifications.c.destination_uuid == wfid,
                SystemSchema.notifications.c.consumed == False,
            )
        ).all()
    assert len(unconsumed) == 1


def test_cancel_refuses_running_step_result(dbos: DBOS) -> None:
    """A step that finishes after its workflow is cancelled records nothing, so a
    resume re-runs it."""
    release = threading.Event()
    calls = {"blocked": 0}

    @DBOS.step()
    def blocked_step() -> str:
        calls["blocked"] += 1
        assert release.wait(30)
        return "blocked"

    @DBOS.workflow()
    def cancelled_workflow() -> str:
        return blocked_step()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(cancelled_workflow)

    try:

        def blocked() -> None:
            assert calls["blocked"] == 1

        retry_until_success(blocked, interval=0.1, max_attempts=100)
        DBOS.cancel_workflow(wfid)
        assert dbos._sys_db.get_workflow_owner(wfid) is None
        release.set()
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            handle.get_result()
        assert _step_names(wfid) == []

        DBOS.resume_workflow(wfid)
        assert DBOS.retrieve_workflow(wfid).get_result() == "blocked"
        assert calls["blocked"] == 2
    finally:
        release.set()


def test_owner_check_blocks_hand_off_until_commit(dbos: DBOS) -> None:
    """A hand-off waits for an open ownership check's transaction, so it cannot land before the write."""

    @DBOS.workflow()
    def owned_workflow() -> str:
        return "done"

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        owned_workflow()
    # PENDING again, keeping the finished execution's token.
    set_workflow_status(dbos._sys_db, wfid, "PENDING")
    token = dbos._sys_db.get_workflow_owner(wfid)
    assert token is not None

    handed_off = threading.Event()

    def hand_off() -> None:
        dbos._sys_db.cancel_workflows([wfid])
        handed_off.set()

    with dbos._sys_db.engine.begin() as c:
        dbos._sys_db._check_owner_txn(c, wfid, token)
        thread = threading.Thread(target=hand_off)
        thread.start()
        # Negative check: the hand-off must still be waiting on the open check.
        assert not handed_off.wait(0.5)
    thread.join(timeout=30)
    assert handed_off.is_set()
    assert dbos._sys_db.get_workflow_owner(wfid) is None


def test_stale_owner_cannot_write_outcome(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An execution that lost ownership after its last step cannot record the outcome."""
    release = threading.Event()
    started = threading.Event()

    @DBOS.workflow()
    def outcome_workflow() -> str:
        started.set()
        assert release.wait(30)
        return "done"

    writes: List[bool] = []
    real_update = dbos._sys_db.update_workflow_outcome

    def tracking_update(*args: Any, **kwargs: Any) -> bool:
        landed = real_update(*args, **kwargs)
        writes.append(landed)
        return landed

    monkeypatch.setattr(dbos._sys_db, "update_workflow_outcome", tracking_update)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(outcome_workflow)

    try:
        assert started.wait(10)
        with dbos._sys_db.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
                .values(execution_xid="another-execution")
            )
        release.set()

        def refused() -> None:
            assert writes == [False]

        retry_until_success(refused, interval=0.1, max_attempts=100)
        status = DBOS.get_workflow_status(wfid)
        assert status is not None and status.status == "PENDING"

        # Release the parked execution, which waits on an outcome nobody else will write.
        DBOS.cancel_workflow(wfid)
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            handle.get_result()
    finally:
        release.set()
        # The parked execution waits on an outcome nobody else writes; cancelling is a no-op once terminal.
        DBOS.cancel_workflow(wfid)
