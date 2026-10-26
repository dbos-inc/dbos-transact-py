import asyncio
import gc
import threading
import uuid
import weakref
from contextlib import nullcontext
from typing import Any, Dict, List, Optional, Set

import pytest
from sqlalchemy import event

from dbos import DBOS, SetShardKey, SetWorkflowID
from dbos import _core as dbos_core
from dbos._context import get_local_dbos_context
from dbos._error import DBOSAwaitedWorkflowCancelledError, DBOSException
from dbos._shard_keys import bound_shard_key
from dbos._sys_db import StepInfo, WorkflowStatus
from dbos._utils import INTERNAL_QUEUE_NAME
from tests.conftest import (
    queue_entries_are_cleaned_up,
    reexecute_workflow_by_id,
    retry_until_success_async,
    using_sharding,
)


@pytest.mark.asyncio
async def test_cancel_workflow_async(dbos: DBOS) -> None:
    """Test async cancel_workflow method."""
    steps_completed = 0
    workflow_event = asyncio.Event()
    main_event = asyncio.Event()
    input_val = 5

    @DBOS.step()
    async def step_one() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.step()
    async def step_two() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.workflow()
    async def simple_workflow(x: int) -> int:
        await step_one()
        main_event.set()
        await workflow_event.wait()
        await step_two()
        return x

    # Start the workflow and cancel it async
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.start_workflow_async(simple_workflow, input_val)
    await main_event.wait()
    await DBOS.cancel_workflow_async(wfid)
    workflow_event.set()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await handle.get_result()
    assert steps_completed == 1


@pytest.mark.asyncio
async def test_resume_workflow_async(dbos: DBOS) -> None:
    """Test async resume_workflow method."""
    steps_completed = 0
    workflow_event = asyncio.Event()
    main_event = asyncio.Event()
    input_val = 5

    @DBOS.step()
    async def step_one() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.step()
    async def step_two() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.workflow()
    async def simple_workflow(x: int) -> int:
        await step_one()
        main_event.set()
        await workflow_event.wait()
        await step_two()
        return x

    # Start the workflow and cancel it
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        cancelled_handle = await DBOS.start_workflow_async(simple_workflow, input_val)
    await main_event.wait()
    await DBOS.cancel_workflow_async(wfid)
    workflow_event.set()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await cancelled_handle.get_result()
    assert steps_completed == 1

    # Resume the workflow async
    async_handle = await DBOS.resume_workflow_async(wfid)
    assert (await async_handle.get_result()) == input_val
    assert steps_completed == 2

    # The cancelled handle should still work after resumption
    assert (await cancelled_handle.get_result()) == input_val


@pytest.mark.asyncio
async def test_fork_workflow_async(dbos: DBOS) -> None:
    """Test async fork_workflow method."""
    step_one_count = 0
    step_two_count = 0
    step_three_count = 0

    @DBOS.step()
    async def step_one(x: int) -> int:
        nonlocal step_one_count
        step_one_count += 1
        return x + 1

    @DBOS.step()
    async def step_two(x: int) -> int:
        nonlocal step_two_count
        step_two_count += 1
        return x + 2

    @DBOS.step()
    async def step_three(x: int) -> int:
        nonlocal step_three_count
        step_three_count += 1
        return x + 3

    @DBOS.workflow()
    async def simple_workflow(x: int) -> int:
        return await step_one(x) + await step_two(x) + await step_three(x)

    input_val = 1
    output = 3 * input_val + 6

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.start_workflow_async(simple_workflow, input_val)
        assert await handle.get_result() == output

    assert step_one_count == 1
    assert step_two_count == 1
    assert step_three_count == 1

    # Fork from step 2 async
    forked_handle = await DBOS.fork_workflow_async(wfid, 2)
    assert forked_handle.workflow_id != wfid
    assert (await forked_handle.get_result()) == output

    # Verify step 1 didn't run again, but steps 2 and 3 did
    assert step_one_count == 1
    assert step_two_count == 2
    assert step_three_count == 2


@pytest.mark.asyncio
async def test_list_workflows_async(dbos: DBOS) -> None:
    """Test async list_workflows method."""
    workflow_ids = []

    @DBOS.workflow()
    async def simple_workflow(x: int) -> int:
        return x

    # Create a few workflows
    for i in range(3):
        wfid = str(uuid.uuid4())
        workflow_ids.append(wfid)
        with SetWorkflowID(wfid):
            handle = await DBOS.start_workflow_async(simple_workflow, i)
            await handle.get_result()

    # List workflows async
    workflows: List[WorkflowStatus] = await DBOS.list_workflows_async()

    # Verify we have at least our workflows
    assert len(workflows) >= 3

    # Verify our workflow IDs are present
    found_ids = {wf.workflow_id for wf in workflows}
    for wfid in workflow_ids:
        assert wfid in found_ids

    # Test filtering by workflow_ids
    filtered_workflows = await DBOS.list_workflows_async(workflow_ids=workflow_ids[:2])
    assert len(filtered_workflows) == 2
    assert all(wf.workflow_id in workflow_ids[:2] for wf in filtered_workflows)


@pytest.mark.asyncio
async def test_list_queued_workflows_async(dbos: DBOS) -> None:
    """Test async list_queued_workflows method."""
    await DBOS.register_queue_async("test_queue_async")
    workflow_event = asyncio.Event()

    @DBOS.workflow()
    async def blocking_workflow(x: int) -> int:
        await workflow_event.wait()
        return x

    # Enqueue a workflow but don't let it complete yet
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.enqueue_workflow_async(
            "test_queue_async", blocking_workflow, 42
        )

    # List queued workflows async while workflow is still running
    queued_workflows = await DBOS.list_queued_workflows_async(
        queue_name="test_queue_async"
    )

    # Verify our workflow is in the list
    assert len(queued_workflows) >= 1
    assert any(wf.workflow_id == wfid for wf in queued_workflows)

    # Let the workflow complete
    workflow_event.set()
    await handle.get_result()

    assert queue_entries_are_cleaned_up(dbos)


@pytest.mark.asyncio
async def test_list_workflow_steps_async(dbos: DBOS) -> None:
    """Test async list_workflow_steps method."""

    @DBOS.step()
    async def step_one(x: int) -> int:
        return x + 1

    @DBOS.step()
    async def step_two(x: int) -> int:
        return x + 2

    @DBOS.workflow()
    async def simple_workflow(x: int) -> int:
        await step_one(x)
        await step_two(x)
        return x

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.start_workflow_async(simple_workflow, 1)
        await handle.get_result()

    # List workflow steps async
    steps: List[StepInfo] = await DBOS.list_workflow_steps_async(wfid)

    # Verify we have the expected steps (steps are returned as dictionaries)
    assert len(steps) >= 2
    step_names = {step["function_name"] for step in steps}
    assert any("step_one" in name for name in step_names)
    assert any("step_two" in name for name in step_names)


@pytest.mark.asyncio
async def test_bulk_async_workflow_management(dbos: DBOS) -> None:
    """Test that all bulk async workflow management methods work and are checkpointed."""

    @DBOS.workflow()
    async def target_workflow(x: int) -> int:
        return x

    # Create target workflows
    cancel_ids: list[str] = []
    resume_ids: list[str] = []
    delete_ids: list[str] = []
    for i in range(2):
        for ids in [cancel_ids, resume_ids, delete_ids]:
            wfid = str(uuid.uuid4())
            ids.append(wfid)
            with SetWorkflowID(wfid):
                h = await DBOS.start_workflow_async(target_workflow, i)
                await h.get_result()

    # Pick one workflow to fork
    fork_target_id = resume_ids[0]

    @DBOS.workflow()
    async def management_workflow() -> None:
        await DBOS.cancel_workflows_async(cancel_ids)
        handles = await DBOS.resume_workflows_async(resume_ids)
        assert len(handles) == 2
        for i, h in enumerate(handles):
            assert (await h.get_result()) == i
        await DBOS.delete_workflows_async(delete_ids)
        workflows = await DBOS.list_workflows_async(workflow_ids=cancel_ids)
        assert len(workflows) == 2
        for wf in workflows:
            assert wf.workflow_id in cancel_ids
        steps = await DBOS.list_workflow_steps_async(fork_target_id)
        assert isinstance(steps, list)
        forked = await DBOS.fork_workflow_async(fork_target_id, 1)
        assert (await forked.get_result()) == 0

    mgmt_wfid = str(uuid.uuid4())
    with SetWorkflowID(mgmt_wfid):
        handle = await DBOS.start_workflow_async(management_workflow)
    await handle.get_result()

    # Verify the management operations are checkpointed as steps
    steps = await DBOS.list_workflow_steps_async(mgmt_wfid)
    step_names = [step["function_name"] for step in steps]
    assert "DBOS.cancelWorkflow" in step_names
    assert "DBOS.resumeWorkflow" in step_names
    assert "DBOS.deleteWorkflow" in step_names
    assert "DBOS.listWorkflows" in step_names
    assert "DBOS.listWorkflowSteps" in step_names
    assert "DBOS.forkWorkflow" in step_names

    # Verify delete actually took effect
    for did in delete_ids:
        assert await DBOS.get_workflow_status_async(did) is None

    assert queue_entries_are_cleaned_up(dbos)


@pytest.mark.asyncio
async def test_preemptible_step_cancellation_and_resume(dbos: DBOS) -> None:
    """End-to-end preemption: a preemptible step is cancelled mid-flight when
    the workflow is cancelled (CancelledError observed inside the step, no
    retry storm even with retries_allowed=True), and on resume the step is
    re-run from scratch and completes successfully."""
    invocation_count = 0
    step_started = asyncio.Event()
    first_invocation_cancelled = False

    @DBOS.step(
        preemptible=True,
        retries_allowed=True,
        max_attempts=5,
        interval_seconds=0.01,
    )
    async def long_running_step() -> str:
        nonlocal invocation_count, first_invocation_cancelled
        invocation_count += 1
        if invocation_count == 1:
            step_started.set()
            try:
                await asyncio.sleep(60)
                return "should-not-reach"
            except asyncio.CancelledError:
                first_invocation_cancelled = True
                raise
        return "done"

    @DBOS.workflow()
    async def wf() -> str:
        return await long_running_step()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.start_workflow_async(wf)

    await step_started.wait()
    await DBOS.cancel_workflow_async(wfid)

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await handle.get_result()
    assert first_invocation_cancelled
    # No retry storm: cancellation must bypass the retry layer.
    assert invocation_count == 1

    resumed = await DBOS.resume_workflow_async(wfid)
    assert (await resumed.get_result()) == "done"
    assert invocation_count == 2

    # After resume, the step should be recorded exactly once with the
    # successful output — no leftover error checkpoint from the preempted
    # first invocation.
    steps = await DBOS.list_workflow_steps_async(wfid)
    step_entries = [s for s in steps if "long_running_step" in s["function_name"]]
    assert len(step_entries) == 1
    assert step_entries[0]["output"] == "done"
    assert step_entries[0]["error"] is None


@pytest.mark.asyncio
async def test_preemptible_step_poll_is_batched(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """All in-flight preemptible steps on a loop share one status-only query per
    interval, run outside any step's DBOS context but with each workflow's shard key."""
    monkeypatch.setattr(dbos_core, "_PREEMPTIBLE_POLL_INTERVAL_SEC", 0.05)
    started = 0
    step_keys: Dict[str, Optional[str]] = {}

    @DBOS.step(preemptible=True)
    async def long_running_step(payload: str) -> str:
        nonlocal started
        started += 1
        ctx = get_local_dbos_context()
        assert ctx is not None and ctx.workflow_id is not None
        step_keys[ctx.workflow_id] = ctx.shard_key
        await asyncio.sleep(60)
        return payload

    @DBOS.workflow()
    async def wf(payload: str) -> List[str]:
        return list(
            await asyncio.gather(long_running_step(payload), long_running_step(payload))
        )

    calls: List[List[str]] = []
    contexts: List[Any] = []
    bindings: List[Dict[str, Optional[str]]] = []
    read = dbos._sys_db.get_cancelled_workflow_ids

    def recording_read(workflow_ids: List[str]) -> Set[str]:
        calls.append(list(workflow_ids))
        contexts.append(get_local_dbos_context())
        bindings.append({wfid: bound_shard_key(wfid) for wfid in workflow_ids})
        return read(workflow_ids)

    statements: List[str] = []

    def record(_conn: Any, _cursor: Any, statement: str, *_: Any) -> None:
        statements.append(" ".join(statement.split()))

    wfids = [str(uuid.uuid4()), str(uuid.uuid4())]
    # Sharded, each workflow gets its own key, so each read must bind the right one.
    keys = [f"preempt-{wfid}" if using_sharding() else None for wfid in wfids]
    handles = []
    for wfid, key in zip(wfids, keys):
        with (
            SetWorkflowID(wfid),
            SetShardKey(key) if key is not None else nullcontext(),
        ):
            handles.append(await DBOS.start_workflow_async(wf, "x" * 100_000))

    def all_started() -> None:
        assert started == 4

    await retry_until_success_async(all_started, interval=0.05, max_attempts=200)
    monkeypatch.setattr(dbos._sys_db, "get_cancelled_workflow_ids", recording_read)
    event.listen(dbos._sys_db.engine, "before_cursor_execute", record)
    try:

        def polled() -> None:
            assert len(calls) >= 3

        await retry_until_success_async(polled, interval=0.05, max_attempts=200)
    finally:
        event.remove(dbos._sys_db.engine, "before_cursor_execute", record)
    # Each read lists each workflow once, bound to its own step's key.
    for c, b in zip(calls, bindings):
        assert len(c) == len(set(c))
        assert b == {wfid: step_keys[wfid] for wfid in c}
    assert {wfid for c in calls for wfid in c} == set(wfids)
    # Steps ran under the keys the test set, so matching bindings aren't None == None.
    assert step_keys == dict(zip(wfids, keys))
    if not using_sharding():
        # Unsharded, one query per interval covers both workflows.
        assert all(sorted(c) == sorted(wfids) for c in calls)
    assert all(c is None for c in contexts)
    polls = [s for s in statements if "workflow_status.workflow_uuid IN" in s]
    assert polls
    assert not [s for s in polls if "inputs" in s]

    for wfid, key in zip(wfids, keys):
        await DBOS.cancel_workflow_async(wfid, shard_key=key)
    for handle in handles:
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            await handle.get_result()


@pytest.mark.asyncio
async def test_preemptible_poll_ignores_steps_registered_mid_read(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A CANCELLED read only preempts steps registered before it began, so a stale
    answer can't preempt the step of a workflow resumed during the read."""
    monkeypatch.setattr(dbos_core, "_PREEMPTIBLE_POLL_INTERVAL_SEC", 0.05)
    wfid = "test-stale-read-" + str(uuid.uuid4())
    read_started = threading.Event()
    release = threading.Event()
    reads = 0

    def blocking_read(workflow_ids: List[str]) -> Set[str]:
        nonlocal reads
        reads += 1
        if reads > 1:
            return set()
        read_started.set()
        assert release.wait(10)
        return {wfid}

    monkeypatch.setattr(dbos._sys_db, "get_cancelled_workflow_ids", blocking_read)
    watcher = dbos_core._cancellation_watcher(dbos)
    before = watcher.register(wfid, None)
    try:
        assert await asyncio.to_thread(read_started.wait, 10)
        after = watcher.register(wfid, None)
        try:
            release.set()
            await asyncio.wait_for(before, timeout=10)

            def read_again() -> None:
                assert reads >= 2

            await retry_until_success_async(read_again, interval=0.05, max_attempts=200)
            assert not after.done()
        finally:
            watcher.unregister(wfid, None, after)
    finally:
        release.set()
        watcher.unregister(wfid, None, before)


@pytest.mark.parametrize("poller_runs", [True, False])
def test_cancellation_watcher_releases_closed_loops(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch, poller_runs: bool
) -> None:
    """A loop's watcher is dropped once the loop is gone, so apps that run a loop per
    call don't accumulate them, including when shutdown cancels a poller that never ran.
    """
    monkeypatch.setattr(dbos_core, "_PREEMPTIBLE_POLL_INTERVAL_SEC", 0.01)
    wfid = "test-closed-loop-" + str(uuid.uuid4())

    async def register_and_leave() -> "weakref.ref[asyncio.AbstractEventLoop]":
        watcher = dbos_core._cancellation_watcher(dbos)
        waiter = watcher.register(wfid, None)
        poller = watcher._poller
        assert poller is not None
        watcher.unregister(wfid, None, waiter)
        if poller_runs:
            await poller
        return weakref.ref(asyncio.get_running_loop())

    loop_ref = asyncio.run(register_and_leave())
    gc.collect()
    assert loop_ref() is None
    assert len(dbos._cancellation_watchers) == 0


@pytest.mark.asyncio
async def test_preemptible_poll_keeps_shard_keys_apart(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A workflow ID is unique only within its shard key: the same ID under two keys is
    read per key, and one key's failing read doesn't block the other's."""
    monkeypatch.setattr(dbos_core, "_PREEMPTIBLE_POLL_INTERVAL_SEC", 0.05)
    wfid = "test-twins-" + str(uuid.uuid4())

    def read(workflow_ids: List[str]) -> Set[str]:
        assert workflow_ids == [wfid]
        key = bound_shard_key(wfid)
        if key == "key-a":
            raise RuntimeError("shard unavailable")
        return {wfid} if key == "key-b" else set()

    monkeypatch.setattr(dbos._sys_db, "get_cancelled_workflow_ids", read)
    watcher = dbos_core._cancellation_watcher(dbos)
    a = watcher.register(wfid, "key-a")
    b = watcher.register(wfid, "key-b")
    try:
        await asyncio.wait_for(b, timeout=10)
        assert not a.done()
    finally:
        watcher.unregister(wfid, "key-a", a)
        watcher.unregister(wfid, "key-b", b)


@pytest.mark.asyncio
async def test_preemptible_step_normal_paths(dbos: DBOS) -> None:
    """preemptible=True doesn't break normal step semantics (exceptions
    propagate), and DBOS.run_step_async with preemptible=True in
    StepOptions actually triggers preemption when the workflow is
    cancelled."""

    # Normal exception still propagates.
    @DBOS.step(preemptible=True)
    async def boom() -> None:
        raise ValueError("boom")

    @DBOS.workflow()
    async def err_wf() -> None:
        await boom()

    with pytest.raises(ValueError, match="boom"):
        await err_wf()

    # Preemptible passed via StepOptions to run_step_async actually preempts.
    step_started = asyncio.Event()
    step_cancelled = False

    async def long_step() -> None:
        nonlocal step_cancelled
        step_started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            step_cancelled = True
            raise

    @DBOS.workflow()
    async def cancel_wf() -> None:
        await DBOS.run_step_async({"preemptible": True}, long_step)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.start_workflow_async(cancel_wf)
    await step_started.wait()
    await DBOS.cancel_workflow_async(wfid)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await handle.get_result()
    assert step_cancelled


def test_preemptible_rejected_for_sync_step(dbos: DBOS) -> None:
    """preemptible=True on a sync step is rejected at decoration time."""
    with pytest.raises(DBOSException):

        @DBOS.step(preemptible=True)
        def sync_step() -> None:
            pass


@pytest.mark.asyncio
async def test_preemptible_step_no_leak_on_outer_cancel(dbos: DBOS) -> None:
    """If the outer coroutine running the step supervisor is cancelled, the
    inner step task is also cancelled (no leak)."""
    from dbos._core import _supervise_step

    step_started = asyncio.Event()
    step_cancelled = asyncio.Event()

    async def long_step() -> None:
        step_started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            step_cancelled.set()
            raise

    # A workflow id that doesn't exist is never seen CANCELLED, so cancellation
    # in this test comes only from the outer task being cancelled.
    fake_wfid = "test-no-leak-" + str(uuid.uuid4())
    outer = asyncio.create_task(
        _supervise_step(
            dbos,
            fake_wfid,
            long_step,
            (),
            {},
            step_name="long_step",
            preemptible=True,
            timeout_seconds=None,
            shard_key="test-no-leak" if using_sharding() else None,
        )
    )
    await step_started.wait()
    outer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await outer

    # If the step task leaked, this would time out.
    await asyncio.wait_for(step_cancelled.wait(), timeout=5.0)


@pytest.mark.asyncio
async def test_get_result_async_replays_recorded_error(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def failing_child() -> None:
        raise ValueError("child failed")

    child_id = str(uuid.uuid4())

    @DBOS.workflow()
    async def parent() -> str:
        try:
            await DBOS.get_result_async(child_id)
        except ValueError as e:
            return str(e)
        return "succeeded"

    with SetWorkflowID(child_id):
        child_handle = await DBOS.start_workflow_async(failing_child)
    with pytest.raises(ValueError):
        await child_handle.get_result()

    handle = await DBOS.start_workflow_async(parent)
    assert await handle.get_result() == "child failed"
    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    assert isinstance(steps[0]["error"], ValueError)

    # The replay re-raises the recorded error rather than awaiting the now-deleted child.
    await DBOS.delete_workflow_async(child_id)
    replayed = await asyncio.to_thread(
        reexecute_workflow_by_id, dbos, handle.workflow_id
    )
    assert await asyncio.to_thread(replayed.get_result) == "child failed"
