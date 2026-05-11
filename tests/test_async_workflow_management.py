import asyncio
import uuid
from typing import List

import pytest

from dbos import DBOS, Queue, SetWorkflowID
from dbos._error import DBOSAwaitedWorkflowCancelledError, DBOSException
from dbos._sys_db import StepInfo, WorkflowStatus
from dbos._utils import INTERNAL_QUEUE_NAME
from tests.conftest import queue_entries_are_cleaned_up


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


@pytest.mark.asyncio
async def test_preemptible_step_normal_paths(dbos: DBOS) -> None:
    """preemptible=True doesn't break normal step semantics: normal
    exceptions still propagate, success returns the value."""

    @DBOS.step(preemptible=True)
    async def add_one(x: int) -> int:
        return x + 1

    @DBOS.step(preemptible=True)
    async def boom() -> None:
        raise ValueError("boom")

    @DBOS.workflow()
    async def ok_wf(x: int) -> int:
        return await add_one(x)

    @DBOS.workflow()
    async def err_wf() -> None:
        await boom()

    assert (await ok_wf(41)) == 42
    with pytest.raises(ValueError, match="boom"):
        await err_wf()


def test_preemptible_rejected_for_sync_step(dbos: DBOS) -> None:
    """preemptible=True on a sync step is rejected at decoration time."""
    with pytest.raises(DBOSException):

        @DBOS.step(preemptible=True)
        def sync_step() -> None:
            pass


@pytest.mark.asyncio
async def test_preemptible_step_no_leak_on_outer_cancel(dbos: DBOS) -> None:
    """If the outer coroutine running _run_preemptible_step is cancelled,
    the inner step task is also cancelled (no leak)."""
    from dbos._core import _run_preemptible_step

    step_started = asyncio.Event()
    step_cancelled = asyncio.Event()

    async def long_step() -> None:
        step_started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            step_cancelled.set()
            raise

    # A workflow id that doesn't exist — the poller's status query returns
    # None each time, so cancellation in this test comes only from the outer
    # task being cancelled.
    fake_wfid = "test-no-leak-" + str(uuid.uuid4())
    outer = asyncio.create_task(
        _run_preemptible_step(dbos, fake_wfid, long_step, (), {})
    )
    await step_started.wait()
    outer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await outer

    # If the step task leaked, this would time out.
    await asyncio.wait_for(step_cancelled.wait(), timeout=5.0)
