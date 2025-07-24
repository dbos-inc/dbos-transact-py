import asyncio
import threading
import time
import uuid
from typing import List

import pytest

from dbos import DBOS, Queue, SetWorkflowID
from dbos._error import DBOSAwaitedWorkflowCancelledError
from dbos._sys_db import StepInfo, WorkflowStatus
from tests.conftest import queue_entries_are_cleaned_up


@pytest.mark.asyncio
async def test_cancel_workflow_async(dbos: DBOS) -> None:
    """Test async cancel_workflow method."""
    steps_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()
    input_val = 5

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

    # Start the workflow and cancel it async
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(simple_workflow, input_val)
    main_thread_event.wait()
    await DBOS.cancel_workflow_async(wfid)
    workflow_event.set()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()
    assert steps_completed == 1


@pytest.mark.asyncio
async def test_resume_workflow_async(dbos: DBOS) -> None:
    """Test async resume_workflow method."""
    steps_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()
    input_val = 5

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

    # Start the workflow and cancel it
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(simple_workflow, input_val)
    main_thread_event.wait()
    DBOS.cancel_workflow(wfid)
    workflow_event.set()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()
    assert steps_completed == 1

    # Resume the workflow async
    async_handle = await DBOS.resume_workflow_async(wfid)
    assert (await async_handle.get_result()) == input_val
    assert steps_completed == 2


@pytest.mark.asyncio
async def test_restart_workflow_async(dbos: DBOS) -> None:
    """Test async restart_workflow method."""
    input_val = 2
    multiplier = 5

    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return x * multiplier

    # Start the workflow, let it finish, restart it async
    handle = DBOS.start_workflow(simple_workflow, input_val)
    assert handle.get_result() == input_val * multiplier

    forked_handle = await DBOS.restart_workflow_async(handle.workflow_id)
    assert forked_handle.workflow_id != handle.workflow_id
    assert (await forked_handle.get_result()) == input_val * multiplier


@pytest.mark.asyncio
async def test_fork_workflow_async(dbos: DBOS) -> None:
    """Test async fork_workflow method."""
    step_one_count = 0
    step_two_count = 0
    step_three_count = 0

    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return step_one(x) + step_two(x) + step_three(x)

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

    @DBOS.step()
    def step_three(x: int) -> int:
        nonlocal step_three_count
        step_three_count += 1
        return x + 3

    input_val = 1
    output = 3 * input_val + 6

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert simple_workflow(input_val) == output

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
    def simple_workflow(x: int) -> int:
        return x

    # Create a few workflows
    for i in range(3):
        wfid = str(uuid.uuid4())
        workflow_ids.append(wfid)
        with SetWorkflowID(wfid):
            handle = DBOS.start_workflow(simple_workflow, i)
            handle.get_result()

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
    queue = Queue("test_queue_async")
    workflow_event = threading.Event()

    @DBOS.workflow()
    def blocking_workflow(x: int) -> int:
        workflow_event.wait()
        return x

    # Enqueue a workflow but don't let it complete yet
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = queue.enqueue(blocking_workflow, 42)

    # List queued workflows async while workflow is still running
    queued_workflows = await DBOS.list_queued_workflows_async(
        queue_name="test_queue_async"
    )

    # Verify our workflow is in the list
    assert len(queued_workflows) >= 1
    assert any(wf.workflow_id == wfid for wf in queued_workflows)

    # Let the workflow complete
    workflow_event.set()
    handle.get_result()

    assert queue_entries_are_cleaned_up(dbos)


@pytest.mark.asyncio
async def test_list_workflow_steps_async(dbos: DBOS) -> None:
    """Test async list_workflow_steps method."""

    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        step_one(x)
        step_two(x)
        return x

    @DBOS.step()
    def step_one(x: int) -> int:
        return x + 1

    @DBOS.step()
    def step_two(x: int) -> int:
        return x + 2

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(simple_workflow, 1)
        handle.get_result()

    # List workflow steps async
    steps: List[StepInfo] = await DBOS.list_workflow_steps_async(wfid)

    # Verify we have the expected steps (steps are returned as dictionaries)
    assert len(steps) >= 2
    step_names = {step["function_name"] for step in steps}
    assert any("step_one" in name for name in step_names)
    assert any("step_two" in name for name in step_names)
