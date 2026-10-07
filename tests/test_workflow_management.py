import threading
import time
import uuid
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

from dbos import DBOS, DBOSClient, SetWorkflowID, WorkflowHandle
from dbos._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded,
    DBOSNonExistentWorkflowError,
    DBOSWorkflowCancelledError,
)
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import (
    deserialize_value,
    serialize_exception,
    serialize_value_as,
)
from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams
from tests.conftest import queue_entries_are_cleaned_up, reexecute_workflow_by_id


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


def test_active_id_released_before_outcome_write(dbos: DBOS) -> None:
    # A resume that lands while run 1's stale outcome write is still in flight:
    # this same executor dequeues the resumed workflow and must run it to
    # completion alongside the stale run, which parks once its write is refused.
    runs = 0
    entered = threading.Event()
    release_workflow = threading.Event()
    parked = threading.Event()
    release_stale_write = threading.Event()
    second_run_done = threading.Event()

    @DBOS.workflow()
    def blocking_workflow() -> str:
        nonlocal runs
        runs += 1
        if runs == 1:
            entered.set()
            assert release_workflow.wait(timeout=30)
            return ""
        second_run_done.set()
        return "completed"

    wfid = str(uuid.uuid4())

    # Park run 1's terminal outcome write, holding open the window between the
    # function returning and its outcome becoming durable.
    original_update_outcome = dbos._sys_db.update_workflow_outcome
    parked_once = threading.Event()

    def parking_update_outcome(
        workflow_id: str,
        status: Any,
        *,
        output: Any = None,
        error: Any = None,
        owner_xid: Any = None,
    ) -> bool:
        if workflow_id == wfid and not parked_once.is_set():
            parked_once.set()
            parked.set()
            assert release_stale_write.wait(timeout=30)
        return original_update_outcome(
            workflow_id, status, output=output, error=error, owner_xid=owner_xid
        )

    dbos._sys_db.update_workflow_outcome = parking_update_outcome  # type: ignore[method-assign]

    try:
        with SetWorkflowID(wfid):
            DBOS.start_workflow(blocking_workflow)
        assert entered.wait(timeout=15)

        DBOS.cancel_workflow(wfid)

        # Run 1 returns; its stale outcome write is held in flight here.
        release_workflow.set()
        assert parked.wait(timeout=15)
        assert DBOS.get_workflow_status(wfid).status == "CANCELLED"  # type: ignore[union-attr]

        resumed_handle = DBOS.resume_workflow(wfid)

        # While the stale write is still parked, the resumed workflow must be
        # dequeued and executed by this same executor.
        assert second_run_done.wait(
            timeout=15
        ), "resumed dispatch was blocked by a stale active workflow ID"

        assert resumed_handle.get_result() == "completed"
        assert runs == 2
    finally:
        release_stale_write.set()
        dbos._sys_db.update_workflow_outcome = original_update_outcome  # type: ignore[method-assign]

    assert queue_entries_are_cleaned_up(dbos)


def test_cancel_after_final_step(dbos: DBOS) -> None:
    # A workflow cancelled after its final step completes (but before it
    # finishes) must not be able to complete successfully. CANCELLED is terminal.
    steps_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()
    input = 5

    @DBOS.step()
    def step_one() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        # The only step runs and records its output...
        step_one()
        # ...then the workflow is cancelled before it returns.
        main_thread_event.set()
        workflow_event.wait()
        return x

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        cancelled_handle = DBOS.start_workflow(simple_workflow, input)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()

    # The workflow must not complete successfully.
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        cancelled_handle.get_result()
    assert steps_completed == 1
    assert DBOS.get_workflow_status(wfid).status == "CANCELLED"  # type: ignore[union-attr]

    # Resuming it should let it complete successfully.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == input
    assert DBOS.get_workflow_status(wfid).status == "SUCCESS"  # type: ignore[union-attr]
    assert steps_completed == 1  # step_one was already recorded, not re-run

    assert queue_entries_are_cleaned_up(dbos)


def test_workflow_outcome_is_owned_by_the_pending_row(dbos: DBOS) -> None:
    # A run may record its outcome only while its workflow_status row is still
    # PENDING: that row is what says "this run is what the workflow is doing".
    # Every other status means the run lost ownership (a concurrent resume
    # re-enqueued it, a recovery raced it, it was cancelled or dead-lettered)
    # and the recorded outcome, not the one the run computed, is the workflow's
    # outcome.

    # Each run blocks until the test has rewritten its row, then returns a
    # result the test can tell apart from anything recorded out-of-band.
    controls: dict[str, tuple[threading.Event, threading.Event]] = {}

    @DBOS.workflow()
    def blocked_workflow(wfid: str) -> str:
        started, release = controls[wfid]
        started.set()
        assert release.wait(timeout=30)
        return "own-result"

    # Stands in for a run that observes its own cancellation mid-flight: the
    # cancellation is raised only after the test has rewritten the row.
    @DBOS.workflow()
    def self_cancelling_workflow(wfid: str) -> str:
        started, release = controls[wfid]
        started.set()
        assert release.wait(timeout=30)
        raise DBOSWorkflowCancelledError(f"workflow {wfid} observed cancellation")

    def start_blocked_run(
        workflow: Any = blocked_workflow,
    ) -> tuple[WorkflowHandle[str], threading.Event]:
        """Start a run and return once it is blocked inside the workflow
        function, with its row PENDING."""
        wfid = f"outcome-ownership-{uuid.uuid4()}"
        started, release = threading.Event(), threading.Event()
        controls[wfid] = (started, release)
        with SetWorkflowID(wfid):
            handle = DBOS.start_workflow(workflow, wfid)
        assert started.wait(timeout=15), "the workflow never started"
        return handle, release

    def encode_output(value: str) -> str:
        serval, _ = serialize_value_as(value, None, dbos._serializer)
        assert serval is not None
        return serval

    def rewrite_row(
        wfid: str,
        status: str,
        output: Any = None,
        error: Any = None,
    ) -> None:
        """Take the row away from the blocked run, standing in for the
        concurrent resume/recovery/cancel that would do it in production."""
        with dbos._sys_db.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .values(status=status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
            )
            if output is None and error is None:
                return
            # Record the payload where a real recorder puts it, not in the
            # legacy columns: the run must lose to the payload table itself.
            c.execute(
                dbos._sys_db.dialect.insert(SystemSchema.workflow_output)
                .values(
                    workflow_uuid=wfid,
                    output=output,
                    error=error,
                )
                .on_conflict_do_update(
                    index_elements=["workflow_uuid"],
                    set_={"output": output, "error": error},
                )
            )

    def read_row(wfid: str) -> Any:
        with dbos._sys_db.engine.begin() as c:
            return c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.status,
                    SystemSchema.workflow_output.c.output,
                    SystemSchema.workflow_status.c.serialization,
                )
                .select_from(
                    SystemSchema.workflow_status.outerjoin(
                        SystemSchema.workflow_output,
                        SystemSchema.workflow_status.c.workflow_uuid
                        == SystemSchema.workflow_output.c.workflow_uuid,
                    )
                )
                .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
            ).fetchone()

    # 1. A recorded success supersedes the run's own result.
    handle, release = start_blocked_run()
    rewrite_row(
        handle.workflow_id, "SUCCESS", output=encode_output("recorded-elsewhere")
    )
    release.set()
    assert (
        handle.get_result() == "recorded-elsewhere"
    ), "the run must report the recorded output, not its own"
    row = read_row(handle.workflow_id)
    assert row[0] == "SUCCESS"
    assert (
        deserialize_value(row[1], row[2], dbos._serializer) == "recorded-elsewhere"
    ), "the recorded output must not be overwritten"

    # 2. A recorded error supersedes the run's own result.
    handle, release = start_blocked_run()
    recorded_error, _ = serialize_exception(
        Exception("recorded failure"), None, dbos._serializer
    )
    rewrite_row(handle.workflow_id, "ERROR", error=recorded_error)
    release.set()
    with pytest.raises(Exception, match="recorded failure"):
        handle.get_result()
    assert read_row(handle.workflow_id)[0] == "ERROR"

    # 3. A non-terminal row parks the run until an outcome is recorded.
    # ENQUEUED with no queue name: nothing dequeues it, so the run stays parked
    # until this test records the outcome itself.
    handle, release = start_blocked_run()
    parked_wfid = handle.workflow_id
    rewrite_row(parked_wfid, "ENQUEUED")
    release.set()

    parked_outcome: dict[str, Any] = {}
    done = threading.Event()

    def get_parked_result() -> None:
        try:
            parked_outcome["result"] = handle.get_result()
        except Exception as e:
            parked_outcome["error"] = e
        done.set()

    threading.Thread(target=get_parked_result, daemon=True).start()

    # The run releases its active-workflow-ID entry immediately before it tries
    # to record its outcome. Waiting for that makes the check below assert that
    # the run parked, rather than merely that it had not gotten around to the
    # write yet.
    deadline = time.time() + 30
    while parked_wfid in dbos._active_workflows_set.activeList():
        assert time.time() < deadline, "the run never reached its outcome write"
        time.sleep(0.01)
    assert not done.wait(
        timeout=2
    ), f"the run must wait for the owning execution, got {parked_outcome}"

    rewrite_row(parked_wfid, "SUCCESS", output=encode_output("recorded-by-owner"))
    assert done.wait(timeout=30), "the parked run did not pick up the recorded outcome"
    assert (
        parked_outcome.get("result") == "recorded-by-owner"
    ), f"the run must report the recorded output, not its own: {parked_outcome}"

    # 4. A dead-lettered row fails the run with the DLQ error.
    handle, release = start_blocked_run()
    rewrite_row(handle.workflow_id, "MAX_RECOVERY_ATTEMPTS_EXCEEDED")
    release.set()
    with pytest.raises(DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded):
        handle.get_result()
    row = read_row(handle.workflow_id)
    assert row[0] == "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
    assert row[1] is None, "the refused outcome must not record an output"

    # 5. A deleted row fails the run with the non-existent-workflow error.
    handle, release = start_blocked_run()
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.delete(SystemSchema.workflow_status).where(
                SystemSchema.workflow_status.c.workflow_uuid == handle.workflow_id
            )
        )
    # The guarded UPDATE is a single statement: on a missing row it reports
    # not-landed rather than raising. It is the parked await, told the row must
    # already exist, that detects the deletion and raises.
    assert not dbos._sys_db.update_workflow_outcome(
        handle.workflow_id, "SUCCESS", output=encode_output("never-lands")
    )
    with pytest.raises(DBOSNonExistentWorkflowError):
        dbos._sys_db.await_workflow_result(
            handle.workflow_id, polling_interval=0.01, fail_if_missing=True
        )
    release.set()
    with pytest.raises(DBOSNonExistentWorkflowError):
        handle.get_result()

    # 6. A run that observes its own cancellation adopts the recorded outcome
    # rather than trusting its local view: here a concurrent "resume" already
    # rewrote the row to SUCCESS, so the handle reports that outcome instead of
    # a cancellation that is no longer the workflow's state.
    handle, release = start_blocked_run(self_cancelling_workflow)
    rewrite_row(
        handle.workflow_id, "SUCCESS", output=encode_output("recorded-after-cancel")
    )
    release.set()
    assert (
        handle.get_result() == "recorded-after-cancel"
    ), "the run must adopt the recorded outcome, not report its cancellation"

    # ... and when the row is genuinely CANCELLED, the handle still reports the
    # awaited-cancelled error, as before.
    handle, release = start_blocked_run(self_cancelling_workflow)
    rewrite_row(handle.workflow_id, "CANCELLED")
    release.set()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()
    assert read_row(handle.workflow_id)[0] == "CANCELLED"


def test_delete_workflow(dbos: DBOS) -> None:
    @DBOS.step()
    def step(x: int) -> int:
        return x

    @DBOS.workflow()
    def child_workflow(x: int) -> int:
        step(x)
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


def test_cancel_workflow_children(dbos: DBOS) -> None:
    # Build a three-level tree: parent -> child -> grandchild, each blocking.
    parent_id = str(uuid.uuid4())
    child_id = str(uuid.uuid4())
    grandchild_id = str(uuid.uuid4())
    ids = [parent_id, child_id, grandchild_id]

    workflow_events: dict[str, threading.Event] = {i: threading.Event() for i in ids}
    main_events: dict[str, threading.Event] = {i: threading.Event() for i in ids}

    @DBOS.step()
    def noop() -> None:
        pass

    @DBOS.workflow()
    def grandchild_workflow() -> str:
        wfid = DBOS.workflow_id
        assert wfid is not None
        main_events[wfid].set()
        workflow_events[wfid].wait()
        # A step after the wait so the workflow observes its cancellation.
        noop()
        return wfid

    @DBOS.workflow()
    def child_workflow() -> str:
        wfid = DBOS.workflow_id
        assert wfid is not None
        with SetWorkflowID(grandchild_id):
            DBOS.start_workflow(grandchild_workflow)
        main_events[wfid].set()
        workflow_events[wfid].wait()
        noop()
        return wfid

    @DBOS.workflow()
    def parent_workflow() -> str:
        wfid = DBOS.workflow_id
        assert wfid is not None
        with SetWorkflowID(child_id):
            DBOS.start_workflow(child_workflow)
        main_events[wfid].set()
        workflow_events[wfid].wait()
        noop()
        return wfid

    with SetWorkflowID(parent_id):
        parent_handle = DBOS.start_workflow(parent_workflow)

    # Wait until the whole tree is running and blocked
    for i in ids:
        main_events[i].wait()

    # The cascade should discover the full descendant tree
    assert set(dbos._sys_db.get_workflow_children(parent_id)) == {
        child_id,
        grandchild_id,
    }

    # Cancelling without cancel_children only affects the parent
    DBOS.cancel_workflow(parent_id, cancel_children=False)
    assert DBOS.get_workflow_status(parent_id).status == "CANCELLED"  # type: ignore[union-attr]
    assert DBOS.get_workflow_status(child_id).status != "CANCELLED"  # type: ignore[union-attr]
    assert DBOS.get_workflow_status(grandchild_id).status != "CANCELLED"  # type: ignore[union-attr]

    # Cancelling with cancel_children cancels the entire subtree
    DBOS.cancel_workflow(parent_id, cancel_children=True)
    for i in ids:
        status = DBOS.get_workflow_status(i)
        assert status is not None
        assert status.status == "CANCELLED"

    # Release the workflows so they observe the cancellation and threads exit
    for evt in workflow_events.values():
        evt.set()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        parent_handle.get_result()

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


def test_resume_nonexistent_workflow(dbos: DBOS, client: DBOSClient) -> None:
    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return x

    missing_id = str(uuid.uuid4())

    # Resuming a missing ID must fail, not return a handle whose get_result() polls forever
    with pytest.raises(DBOSNonExistentWorkflowError):
        DBOS.resume_workflow(missing_id)
    with pytest.raises(DBOSNonExistentWorkflowError):
        client.resume_workflow(missing_id)

    # Resuming a workflow that exists but already completed is still a legal no-op
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert simple_workflow(5) == 5
    assert DBOS.resume_workflow(wfid).get_result() == 5

    # A bulk resume containing a missing ID is all-or-nothing: nothing is re-enqueued
    with pytest.raises(DBOSNonExistentWorkflowError):
        DBOS.resume_workflows([wfid, missing_id])
    status = DBOS.get_workflow_status(wfid)
    assert status is not None and status.status == "SUCCESS"

    assert queue_entries_are_cleaned_up(dbos)


@pytest.mark.asyncio
async def test_resume_nonexistent_workflow_async(dbos: DBOS) -> None:
    missing_id = str(uuid.uuid4())

    with pytest.raises(DBOSNonExistentWorkflowError):
        await DBOS.resume_workflow_async(missing_id)
    with pytest.raises(DBOSNonExistentWorkflowError):
        await DBOS.resume_workflows_async([missing_id])


def test_fork_nonexistent_workflow(dbos: DBOS, client: DBOSClient) -> None:
    missing_id = str(uuid.uuid4())

    with pytest.raises(DBOSNonExistentWorkflowError):
        DBOS.fork_workflow(missing_id, 1)
    with pytest.raises(DBOSNonExistentWorkflowError):
        client.fork_workflow(missing_id, 1)


def test_fork_nonexistent_workflow_replays_recorded_error(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return x

    missing_id = str(uuid.uuid4())

    @DBOS.workflow()
    def forker() -> str:
        try:
            DBOS.fork_workflow(missing_id, 1)
        except DBOSNonExistentWorkflowError:
            return "missing"
        return "forked"

    handle = DBOS.start_workflow(forker)
    assert handle.get_result() == "missing"
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert isinstance(steps[0]["error"], DBOSNonExistentWorkflowError)

    # Once the target exists, a replay still takes the recorded branch and forks nothing.
    with SetWorkflowID(missing_id):
        assert simple_workflow(1) == 1
    assert reexecute_workflow_by_id(dbos, handle.workflow_id).get_result() == "missing"
    assert DBOS.list_workflows(forked_from=missing_id) == []


def test_workflow_command_transient_error_is_retried_not_recorded(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    target_id = str(uuid.uuid4())
    original_children = dbos._sys_db._get_direct_children
    calls = {"count": 0}

    def flaky_children(ids: list[str]) -> list[str]:
        if ids == [target_id]:
            calls["count"] += 1
            if calls["count"] == 1:
                raise OperationalError(
                    "SELECT",
                    {},
                    Exception("connection lost"),
                    connection_invalidated=True,
                )
        return original_children(ids)

    monkeypatch.setattr(dbos._sys_db, "_get_direct_children", flaky_children)

    @DBOS.workflow()
    def canceller() -> None:
        DBOS.cancel_workflow(target_id, cancel_children=True)

    handle = DBOS.start_workflow(canceller)
    handle.get_result()
    assert calls["count"] == 2
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert [s["function_name"] for s in steps] == ["DBOS.cancelWorkflow"]
    assert steps[0]["error"] is None


def test_fork_retry_after_lost_commit_ack(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return x

    source_id = str(uuid.uuid4())
    with SetWorkflowID(source_id):
        assert simple_workflow(1) == 1

    original_attempt = dbos._sys_db._fork_workflow_attempt
    calls = {"count": 0}

    def commit_then_drop(*args: Any, **kwargs: Any) -> list[str]:
        calls["count"] += 1
        result = original_attempt(*args, **kwargs)
        if calls["count"] == 1:
            # The fork committed, but the client never hears back.
            raise OperationalError(
                "COMMIT", {}, Exception("connection lost"), connection_invalidated=True
            )
        return result

    monkeypatch.setattr(dbos._sys_db, "_fork_workflow_attempt", commit_then_drop)

    @DBOS.workflow()
    def forker() -> str:
        return DBOS.fork_workflow(source_id, 1).workflow_id

    handle = DBOS.start_workflow(forker)
    fork_id = handle.get_result()
    assert calls["count"] == 2
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert steps[0]["function_name"] == "DBOS.forkWorkflow"
    assert steps[0]["error"] is None
    assert [w.workflow_id for w in DBOS.list_workflows(forked_from=source_id)] == [
        fork_id
    ]


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


def test_cancel_resume_queue(dbos: DBOS) -> None:
    steps_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()
    input = 5

    DBOS.register_queue("test_queue")

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
        handle = DBOS.enqueue_workflow("test_queue", simple_workflow, input)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()
    assert steps_completed == 1
    assert DBOS.get_workflow_status(wfid).status == "CANCELLED"  # type: ignore[union-attr]

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

    # Filter by was_forked_from=True returns only the original; False returns only the forks.
    forked_from_workflows = DBOS.list_workflows(was_forked_from=True)
    assert len(forked_from_workflows) == 1
    assert forked_from_workflows[0].workflow_id == wfid
    not_forked_from_workflows = DBOS.list_workflows(was_forked_from=False)
    assert {w.workflow_id for w in not_forked_from_workflows} == {
        fork_id,
        fork_id_2,
        fork_id_3,
    }

    # is_fork is the other end of the relationship: it matches the forks themselves.
    is_fork_workflows = DBOS.list_workflows(is_fork=True)
    assert {w.workflow_id for w in is_fork_workflows} == {
        fork_id,
        fork_id_2,
        fork_id_3,
    }
    not_fork_workflows = DBOS.list_workflows(is_fork=False)
    assert [w.workflow_id for w in not_fork_workflows] == [wfid]


def test_restart_fromsteps_stepsonly(
    dbos: DBOS,
) -> None:

    stepOneCount = 0
    stepTwoCount = 0
    stepThreeCount = 0
    stepFourCount = 0
    stepFiveCount = 0

    @DBOS.workflow()
    def simple_workflow() -> None:
        stepOne()
        stepTwo()
        stepThree()
        stepFour()
        stepFive()
        return

    @DBOS.step()
    def stepOne() -> None:
        nonlocal stepOneCount
        stepOneCount += 1
        return

    @DBOS.step()
    def stepTwo() -> None:
        nonlocal stepTwoCount
        stepTwoCount += 1
        return

    @DBOS.step()
    def stepThree() -> None:
        nonlocal stepThreeCount
        stepThreeCount += 1
        return

    @DBOS.step()
    def stepFour() -> None:
        nonlocal stepFourCount
        stepFourCount += 1
        return

    @DBOS.step()
    def stepFive() -> None:
        nonlocal stepFiveCount
        stepFiveCount += 1
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        simple_workflow()

    assert stepOneCount == 1
    assert stepTwoCount == 1
    assert stepThreeCount == 1
    assert stepFourCount == 1
    assert stepFiveCount == 1

    forked_handle = DBOS.fork_workflow(wfid, 2)
    assert forked_handle.workflow_id != wfid
    fork_id_one = forked_handle.workflow_id
    forked_handle.get_result()

    assert stepOneCount == 1
    assert stepTwoCount == 2
    assert stepThreeCount == 2
    assert stepFourCount == 2
    assert stepFiveCount == 2

    forked_handle = DBOS.fork_workflow(wfid, 4)
    assert forked_handle.workflow_id != wfid
    fork_id_two = forked_handle.workflow_id
    forked_handle.get_result()

    assert stepOneCount == 1
    assert stepTwoCount == 2
    assert stepThreeCount == 2
    assert stepFourCount == 3
    assert stepFiveCount == 3

    forked_handle = DBOS.fork_workflow(wfid, 1)
    assert forked_handle.workflow_id != wfid
    fork_id_three = forked_handle.workflow_id
    forked_handle.get_result()

    assert stepOneCount == 2
    assert stepTwoCount == 3
    assert stepThreeCount == 3
    assert stepFourCount == 4
    assert stepFiveCount == 4


def test_restart_fromsteps_invalid_start(
    dbos: DBOS,
) -> None:

    stepOneCount = 0
    stepTwoCount = 0
    stepThreeCount = 0
    stepFourCount = 0
    stepFiveCount = 0

    @DBOS.workflow()
    def simple_workflow() -> None:
        stepOne()
        stepTwo()
        stepThree()
        stepFour()
        stepFive()
        return

    @DBOS.step()
    def stepOne() -> None:
        nonlocal stepOneCount
        stepOneCount += 1
        return

    @DBOS.step()
    def stepTwo() -> None:
        nonlocal stepTwoCount
        stepTwoCount += 1
        return

    @DBOS.step()
    def stepThree() -> None:
        nonlocal stepThreeCount
        stepThreeCount += 1
        return

    @DBOS.step()
    def stepFour() -> None:
        nonlocal stepFourCount
        stepFourCount += 1
        return

    @DBOS.step()
    def stepFive() -> None:
        nonlocal stepFiveCount
        stepFiveCount += 1
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        simple_workflow()

    assert stepOneCount == 1
    assert stepTwoCount == 1
    assert stepThreeCount == 1
    assert stepFourCount == 1
    assert stepFiveCount == 1

    forked_handle = DBOS.fork_workflow(wfid, 3)
    assert forked_handle.workflow_id != wfid
    forked_handle.get_result()

    assert stepOneCount == 1
    assert stepTwoCount == 1
    assert stepThreeCount == 2
    assert stepFourCount == 2
    assert stepFiveCount == 2

    forked_handle = DBOS.fork_workflow(wfid, 5)
    assert forked_handle.workflow_id != wfid
    forked_handle.get_result()

    assert stepOneCount == 1
    assert stepTwoCount == 1
    assert stepThreeCount == 2
    assert stepFourCount == 2
    assert stepFiveCount == 3

    # invalid < 1 will default to 1
    forked_handle = DBOS.fork_workflow(wfid, -1)
    assert forked_handle.workflow_id != wfid
    forked_handle.get_result()

    assert stepOneCount == 2
    assert stepTwoCount == 2
    assert stepThreeCount == 3
    assert stepFourCount == 3
    assert stepFiveCount == 4


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


def test_fork_timeout(dbos: DBOS) -> None:
    @DBOS.workflow()
    def blocking_workflow() -> None:
        while True:
            DBOS.sleep(0.1)

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(blocking_workflow)
    DBOS.cancel_workflow(workflow_id)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()

    # Forking with a short timeout should cancel the forked workflow too.
    forked_handle = DBOS.fork_workflow(workflow_id, 1, timeout_seconds=0.1)
    assert forked_handle.get_status().workflow_timeout_ms == 100
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        forked_handle.get_result()
    assert queue_entries_are_cleaned_up(dbos)

    # Forking without a timeout should leave it unset.
    no_timeout_handle = DBOS.fork_workflow(workflow_id, 1)
    assert no_timeout_handle.get_status().workflow_timeout_ms is None
    DBOS.cancel_workflow(no_timeout_handle.workflow_id)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        no_timeout_handle.get_result()


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

    DBOS.register_queue("test_resume_fork_queue")
    input = 5
    output = (input + 1) + (input + 2)

    # Enqueue workflow, let step_one run, then cancel before step_two
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.enqueue_workflow("test_resume_fork_queue", simple_workflow, input)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()
    assert DBOS.get_workflow_status(wfid).status == "CANCELLED"  # type: ignore[union-attr]
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


def test_fork_replacement_children(dbos: DBOS) -> None:
    multiplier = 2

    @DBOS.step()
    def child_step(x: int) -> int:
        return x * multiplier

    @DBOS.workflow()
    def child_wf(x: int) -> int:
        return child_step(x)

    @DBOS.step()
    def combine(results: list[int]) -> int:
        return sum(results)

    child_ids: list[str] = []

    @DBOS.workflow()
    def parent_wf() -> int:
        h1 = DBOS.start_workflow(child_wf, 10)
        h2 = DBOS.start_workflow(child_wf, 20)
        h3 = DBOS.start_workflow(child_wf, 30)
        h4 = DBOS.start_workflow(child_wf, 40)
        h5 = DBOS.start_workflow(child_wf, 50)
        child_ids.clear()
        child_ids.extend([h.workflow_id for h in [h1, h2, h3, h4, h5]])
        results = [h.get_result() for h in [h1, h2, h3, h4, h5]]
        return combine(results)

    # Run the parent. Children return x*2.
    parent_handle = DBOS.start_workflow(parent_wf)
    original_result = parent_handle.get_result()
    # [10*2, 20*2, 30*2, 40*2, 50*2] = [20, 40, 60, 80, 100] → sum = 300
    assert original_result == 300
    assert len(child_ids) == 5
    orig_ids = list(child_ids)

    # Change the multiplier so forked children produce different results.
    multiplier = 10

    # Fork children 0, 2, and 4 from step 1 (re-run child_step with new multiplier).
    forked_child_0 = DBOS.fork_workflow(orig_ids[0], 1)
    forked_child_2 = DBOS.fork_workflow(orig_ids[2], 1)
    forked_child_4 = DBOS.fork_workflow(orig_ids[4], 1)
    assert forked_child_0.get_result() == 100  # 10 * 10
    assert forked_child_2.get_result() == 300  # 30 * 10
    assert forked_child_4.get_result() == 500  # 50 * 10

    # Fork the parent from step 6 (combine step, after the 5 start_workflow steps).
    # Steps 1-5 are replayed with replaced child_workflow_ids.
    # The workflow then re-reads results from the new children before re-executing combine.
    forked_parent = DBOS.fork_workflow(
        parent_handle.workflow_id,
        6,
        replacement_children={
            orig_ids[0]: forked_child_0.workflow_id,
            orig_ids[2]: forked_child_2.workflow_id,
            orig_ids[4]: forked_child_4.workflow_id,
        },
    )
    forked_result = forked_parent.get_result()
    # [10*10, 20*2, 30*10, 40*2, 50*10] = [100, 40, 300, 80, 500] → sum = 1020
    assert forked_result == 1020


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


def test_legacy_payload_rows_still_read(dbos: DBOS) -> None:
    """Payloads live only in the payload tables, but a row written before the split
    keeps them on workflow_status and every read path must still resolve it."""

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    assert workflow(11) == 11
    workflow_id = DBOS.list_workflows()[0].workflow_id
    ws, wi, wo = (
        SystemSchema.workflow_status,
        SystemSchema.workflow_input,
        SystemSchema.workflow_output,
    )

    with dbos._sys_db.engine.begin() as c:
        legacy = c.execute(
            sa.select(ws.c.inputs, ws.c.output).where(ws.c.workflow_uuid == workflow_id)
        ).one()
        inputs = c.execute(
            sa.select(wi.c.inputs).where(wi.c.workflow_uuid == workflow_id)
        ).scalar_one()
        output = c.execute(
            sa.select(wo.c.output).where(wo.c.workflow_uuid == workflow_id)
        ).scalar_one()
    # No dual write: the legacy columns stay empty.
    assert tuple(legacy) == (None, None)
    assert inputs is not None and output is not None

    # Move the payloads onto the legacy columns: a pre-split row looks exactly like this.
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(ws)
            .where(ws.c.workflow_uuid == workflow_id)
            .values(inputs=inputs, output=output)
        )
        c.execute(sa.delete(wi).where(wi.c.workflow_uuid == workflow_id))
        c.execute(sa.delete(wo).where(wo.c.workflow_uuid == workflow_id))

    listed = DBOS.list_workflows(workflow_ids=[workflow_id])[0]
    assert listed.input is not None
    assert listed.output == 11
    handle: WorkflowHandle[int] = DBOS.retrieve_workflow(workflow_id)
    assert handle.get_result() == 11
    forked: WorkflowHandle[int] = DBOS.fork_workflow(workflow_id, 1)
    assert forked.get_result() == 11
