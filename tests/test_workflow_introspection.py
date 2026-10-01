import asyncio
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, List, Tuple

import pytest
import sqlalchemy as sa
from sqlalchemy import event

# Public API
from dbos import DBOS, DBOSClient, DBOSConfig, SetWorkflowID, WorkflowStatusString
from dbos._error import DBOSQueryTimeoutError, DBOSStepNondeterminismError
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import DefaultSerializer
from dbos._sys_db import OperationResultInternal, SystemDatabase
from dbos._utils import GlobalParams

from .conftest import postgres_urls, retry_until_success, set_workflow_status


def test_list_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return x + 1

    # Run a simple workflow
    wfid = str(uuid.uuid4)
    with SetWorkflowID(wfid):
        assert simple_workflow(1) == 2

    # List the workflow, then test every output
    outputs = DBOS.list_workflows()
    assert len(outputs) == 1
    output = outputs[0]
    assert output.workflow_id == wfid
    assert output.status == "SUCCESS"
    assert output.name == simple_workflow.__qualname__
    assert output.class_name == None
    assert output.config_name == None
    assert output.authenticated_user == None
    assert output.assumed_role == None
    assert output.authenticated_roles == None
    assert output.created_at is not None and output.created_at > 0
    assert output.updated_at is not None and output.updated_at > 0
    assert output.queue_name == None
    assert output.executor_id == GlobalParams.executor_id
    assert output.app_version == DBOS.application_version
    assert output.app_id == ""
    assert output.recovery_attempts == 1
    assert output.workflow_timeout_ms is None
    assert output.workflow_deadline_epoch_ms is None
    assert output.input is not None
    assert output.output is not None

    # Test ignoring input and output
    outputs = DBOS.list_workflows(load_input=False, load_output=False)
    assert len(outputs) == 1
    output = outputs[0]
    assert output.input is None
    assert output.output is None

    # Test searching by status
    outputs = DBOS.list_workflows(status="PENDING")
    assert len(outputs) == 0
    outputs = DBOS.list_workflows(status="SUCCESS")
    assert len(outputs) == 1
    outputs = DBOS.list_workflows(status=["SUCCESS", "PENDING"])
    assert len(outputs) == 1

    # Test searching by workflow name
    outputs = DBOS.list_workflows(name="no")
    assert len(outputs) == 0
    outputs = DBOS.list_workflows(name=simple_workflow.__qualname__)
    assert len(outputs) == 1
    outputs = DBOS.list_workflows(name=[simple_workflow.__qualname__, "no"])
    assert len(outputs) == 1
    outputs = DBOS.list_workflows(name=["no", "also_no"])
    assert len(outputs) == 0

    # Test searching by workflow ID
    outputs = DBOS.list_workflows(workflow_ids=["no"])
    assert len(outputs) == 0
    outputs = DBOS.list_workflows(workflow_ids=[wfid, "no"])
    assert len(outputs) == 1

    # Test searching by application version
    outputs = DBOS.list_workflows(app_version="no")
    assert len(outputs) == 0
    outputs = DBOS.list_workflows(app_version=DBOS.application_version)
    assert len(outputs) == 1
    outputs = DBOS.list_workflows(app_version=[DBOS.application_version, "no"])
    assert len(outputs) == 1
    outputs = DBOS.list_workflows(app_version=["no", "also_no"])
    assert len(outputs) == 0

    # Test searching by executor ID
    outputs = DBOS.list_workflows(executor_id="nonexistent_executor")
    assert len(outputs) == 0
    outputs = DBOS.list_workflows(executor_id=GlobalParams.executor_id)
    assert len(outputs) == 1
    outputs = DBOS.list_workflows(executor_id=[GlobalParams.executor_id, "no"])
    assert len(outputs) == 1
    outputs = DBOS.list_workflows(executor_id=["no", "also_no"])
    assert len(outputs) == 0


def test_list_workflow_error(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        raise Exception(f"Test error: {x}")

    # Run a simple workflow
    wfid = str(uuid.uuid4)
    with SetWorkflowID(wfid):
        with pytest.raises(Exception) as exc_info:
            simple_workflow(1)
        assert str(exc_info.value) == "Test error: 1"

    # List the workflow, then test every output
    outputs = DBOS.list_workflows()
    assert len(outputs) == 1
    output = outputs[0]
    assert output.workflow_id == wfid
    assert output.status == "ERROR"
    assert output.name == simple_workflow.__qualname__
    assert output.class_name == None
    assert output.config_name == None
    assert output.authenticated_user == None
    assert output.assumed_role == None
    assert output.authenticated_roles == None
    assert output.created_at is not None and output.created_at > 0
    assert output.updated_at is not None and output.updated_at > 0
    assert output.queue_name == None
    assert output.executor_id == GlobalParams.executor_id
    assert output.app_version == DBOS.application_version
    assert output.app_id == ""
    assert output.recovery_attempts == 1
    assert output.workflow_timeout_ms is None
    assert output.workflow_deadline_epoch_ms is None
    assert output.input is not None
    assert output.output is None
    assert output.error is not None
    assert isinstance(output.error, Exception)

    # Test ignoring input and output
    outputs = DBOS.list_workflows(load_input=False, load_output=False)
    assert len(outputs) == 1
    output = outputs[0]
    assert output.input is None
    assert output.output is None
    assert output.error is None

    # Test searching by status
    outputs = DBOS.list_workflows(status="PENDING")
    assert len(outputs) == 0
    outputs = DBOS.list_workflows(status="ERROR")
    assert len(outputs) == 1
    outputs = DBOS.list_workflows(status=["ERROR", "PENDING"])
    assert len(outputs) == 1


def test_list_workflow_limit(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        return

    num_workflows = 5
    for i in range(num_workflows):
        with SetWorkflowID(str(i)):
            simple_workflow()

    # Test all workflows appear
    outputs = DBOS.list_workflows()
    assert len(outputs) == num_workflows
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(i)

    # Test sort_desc inverts the order:
    outputs = DBOS.list_workflows(sort_desc=True)
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(num_workflows - i - 1)

    # Test LIMIT 2 returns the first two
    outputs = DBOS.list_workflows(limit=2)
    assert len(outputs) == 2
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(i)

    # Test LIMIT 2 OFFSET 2 returns the third and fourth
    outputs = DBOS.list_workflows(limit=2, offset=2)
    assert len(outputs) == 2
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(i + 2)

    # Test OFFSET 4 returns only the fifth entry
    outputs = DBOS.list_workflows(offset=num_workflows - 1)
    assert len(outputs) == 1
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(i + 4)


def test_list_workflow_start_end_times(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    now = datetime.now()
    starttime = (now - timedelta(seconds=20)).isoformat()
    simple_workflow()
    endtime = datetime.now().isoformat()

    output = DBOS.list_workflows(start_time=starttime, end_time=endtime)
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    newstarttime = (now - timedelta(seconds=30)).isoformat()
    newendtime = starttime

    output = DBOS.list_workflows(
        start_time=newstarttime,
        end_time=newendtime,
    )
    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"


def test_list_workflow_prefix(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    with SetWorkflowID("test1"):
        simple_workflow()
    with SetWorkflowID("test_"):
        simple_workflow()

    output = DBOS.list_workflows(workflow_id_prefix="invalid")
    assert len(output) == 0
    output = DBOS.list_workflows(workflow_id_prefix="test")
    assert len(output) == 2
    output = DBOS.list_workflows(workflow_id_prefix="test_")
    assert len(output) == 1
    output = DBOS.list_workflows(workflow_id_prefix="test1")
    assert len(output) == 1
    output = DBOS.list_workflows(workflow_id_prefix=["test1", "test_"])
    assert len(output) == 2
    output = DBOS.list_workflows(workflow_id_prefix=["test1", "invalid"])
    assert len(output) == 1
    output = DBOS.list_workflows(workflow_id_prefix=["invalid", "also_invalid"])
    assert len(output) == 0


def test_list_workflow_completed_at(
    dbos: DBOS, skip_with_sqlite_imprecise_time: None
) -> None:
    release = threading.Event()

    @DBOS.workflow()
    def simple_workflow() -> None:
        return

    @DBOS.workflow()
    def failing_workflow() -> None:
        raise RuntimeError("boom")

    @DBOS.workflow()
    def pending_workflow() -> None:
        release.wait()

    # Successful workflow gets completed_at set.
    before_success = datetime.now().isoformat()
    simple_workflow()
    after_success = datetime.now().isoformat()

    [success_status] = DBOS.list_workflows(name=simple_workflow.__qualname__)
    assert success_status.status == "SUCCESS"
    assert success_status.completed_at is not None
    assert success_status.completed_at >= success_status.created_at  # type: ignore[operator]

    # Errored workflow gets completed_at set.
    with pytest.raises(RuntimeError):
        failing_workflow()
    [error_status] = DBOS.list_workflows(name=failing_workflow.__qualname__)
    assert error_status.status == "ERROR"
    assert error_status.completed_at is not None

    # Cancelled workflow gets completed_at set; resumed workflow clears it.
    cancel_id = str(uuid.uuid4())
    with SetWorkflowID(cancel_id):
        DBOS.start_workflow(pending_workflow)
    DBOS.cancel_workflow(cancel_id)
    cancelled = DBOS.get_workflow_status(cancel_id)
    assert cancelled is not None
    assert cancelled.status == "CANCELLED"
    assert cancelled.completed_at is not None

    resumed_handle = DBOS.resume_workflow(cancel_id)
    resumed = DBOS.get_workflow_status(cancel_id)
    assert resumed is not None
    assert resumed.completed_at is None

    # completed_before/completed_after only match terminal workflows in range.
    in_range = DBOS.list_workflows(
        completed_after=before_success, completed_before=after_success
    )
    ids_in_range = {w.workflow_id for w in in_range}
    assert success_status.workflow_id in ids_in_range
    # The error and the resumed-pending workflows complete outside this window.
    assert error_status.workflow_id not in ids_in_range
    assert cancel_id not in ids_in_range

    # completed_after alone excludes never-completed workflows.
    only_completed = DBOS.list_workflows(completed_after=before_success)
    completed_ids = {w.workflow_id for w in only_completed}
    assert success_status.workflow_id in completed_ids
    assert error_status.workflow_id in completed_ids
    assert cancel_id not in completed_ids

    # A window before any work happened matches nothing.
    far_past = (datetime.now() - timedelta(days=1)).isoformat()
    none_yet = DBOS.list_workflows(completed_before=far_past)
    assert len(none_yet) == 0

    # Release the resumed workflow and wait for it to finish.
    release.set()
    resumed_handle.get_result()


def test_list_workflow_end_times_positive(
    dbos: DBOS, skip_with_sqlite_imprecise_time: None
) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    now = datetime.now()

    time_0 = (now - timedelta(seconds=40)).isoformat()
    time_1 = (now - timedelta(seconds=20)).isoformat()
    simple_workflow()
    time_2 = datetime.now().isoformat()
    # created_at is ms-granular, so keep the second workflow out of time_2's millisecond.
    time.sleep(0.01)
    simple_workflow()
    time_3 = datetime.now().isoformat()

    output = DBOS.list_workflows(start_time=time_0, end_time=time_1)
    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"

    output = DBOS.list_workflows(start_time=time_1, end_time=time_2)
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    output = DBOS.list_workflows(
        start_time=time_1,
        end_time=time_3,
    )
    assert len(output) == 2, f"Expected list length to be 2, but got {len(output)}"


def test_get_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    simple_workflow()
    output = DBOS.list_workflows()
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    assert output[0] != None, "Expected output to be not None"

    wfUuid = output[0].workflow_id

    info = DBOS.get_workflow_status(wfUuid)
    assert info is not None, "Expected output to be not None"

    if info is not None:
        assert info.workflow_id == wfUuid, f"Expected workflow_uuid to be {wfUuid}"


def test_queued_workflows(dbos: DBOS, skip_with_sqlite_imprecise_time: None) -> None:
    queued_steps = 5
    step_events = [threading.Event() for _ in range(queued_steps)]
    event = threading.Event()
    DBOS.register_queue("test_queue")

    @DBOS.workflow()
    def test_workflow() -> list[int]:
        handles = []
        for i in range(queued_steps):
            h = DBOS.enqueue_workflow("test_queue", blocking_step, i)
            handles.append(h)
        return [h.get_result() for h in handles]

    @DBOS.step()
    def blocking_step(i: int) -> int:
        step_events[i].set()
        event.wait()
        return i

    # The workflow enqueues blocking steps, wait for all to start
    handle = DBOS.start_workflow(test_workflow)
    for e in step_events:
        e.wait()

    # Verify all blocking steps are enqueued and have the right data
    workflows = DBOS.list_queued_workflows()
    assert len(workflows) == queued_steps
    for i, workflow in enumerate(workflows):
        assert workflow.status == WorkflowStatusString.PENDING.value
        assert workflow.queue_name == "test_queue"
        assert workflow.input is not None
        # Verify oldest queue entries appear first
        assert workflow.input["args"][0] == i
        assert workflow.output is None
        assert workflow.error is None
        assert "blocking_step" in workflow.name
        assert workflow.executor_id == GlobalParams.executor_id
        assert workflow.app_version == DBOS.application_version
        assert workflow.created_at is not None and workflow.created_at > 0
        assert workflow.updated_at is not None and workflow.updated_at > 0
        assert workflow.recovery_attempts == 1
        assert workflow.workflow_timeout_ms is None
        assert workflow.workflow_deadline_epoch_ms is None

    # Test ignoring input
    workflows = DBOS.list_queued_workflows(load_input=False)
    assert len(workflows) == queued_steps
    for workflow in workflows:
        assert workflow.input is None

    # Test sort_desc inverts the order
    workflows = DBOS.list_queued_workflows(sort_desc=True)
    assert len(workflows) == queued_steps
    for i, workflow in enumerate(workflows):
        # Verify newest queue entries appear first
        assert workflow.input is not None
        assert workflow.input["args"][0] == queued_steps - i - 1

    # Verify list_workflows also properly lists the blocking steps
    workflows = DBOS.list_workflows()
    assert len(workflows) == queued_steps + 1
    for i, workflow in enumerate(workflows[1:]):
        assert workflow.status == WorkflowStatusString.PENDING.value
        assert workflow.queue_name == "test_queue"
        assert workflow.input is not None
        # Verify oldest queue entries appear first
        assert workflow.input["args"][0] == i
        assert workflow.output is None
        assert workflow.error is None
        assert "blocking_step" in workflow.name
        assert workflow.executor_id == GlobalParams.executor_id
        assert workflow.app_version == DBOS.application_version
        assert workflow.created_at is not None and workflow.created_at > 0
        assert workflow.updated_at is not None and workflow.updated_at > 0
        assert workflow.recovery_attempts == 1

    # Test every filter
    workflows = DBOS.list_queued_workflows(status=WorkflowStatusString.PENDING.value)
    assert len(workflows) == queued_steps
    workflows = DBOS.list_queued_workflows(status=WorkflowStatusString.ENQUEUED.value)
    assert len(workflows) == 0
    workflows = DBOS.list_queued_workflows(status=["ENQUEUED", "PENDING"])
    assert len(workflows) == queued_steps
    workflows = DBOS.list_workflows(queue_name="test_queue")
    assert len(workflows) == queued_steps
    workflows = DBOS.list_queued_workflows(queue_name="test_queue")
    assert len(workflows) == queued_steps
    workflows = DBOS.list_queued_workflows(queue_name="no")
    assert len(workflows) == 0
    workflows = DBOS.list_queued_workflows(queue_name=["test_queue", "no"])
    assert len(workflows) == queued_steps
    workflows = DBOS.list_queued_workflows(queue_name=["no", "also_no"])
    assert len(workflows) == 0
    workflows = DBOS.list_queued_workflows(name=f"<temp>.{blocking_step.__qualname__}")
    assert len(workflows) == queued_steps
    workflows = DBOS.list_queued_workflows(name="no")
    assert len(workflows) == 0
    workflows = DBOS.list_queued_workflows(
        name=[f"<temp>.{blocking_step.__qualname__}", "no"]
    )
    assert len(workflows) == queued_steps
    workflows = DBOS.list_queued_workflows(name=["no", "also_no"])
    assert len(workflows) == 0
    now = datetime.now(timezone.utc)
    start_time = (now - timedelta(seconds=10)).isoformat()
    end_time = (now + timedelta(seconds=10)).isoformat()
    workflows = DBOS.list_queued_workflows(start_time=start_time, end_time=end_time)
    assert len(workflows) == queued_steps
    workflows = DBOS.list_queued_workflows(
        start_time=now.isoformat(), end_time=end_time
    )
    assert len(workflows) == 0
    workflows = DBOS.list_queued_workflows(limit=2)
    assert len(workflows) == 2
    workflows = DBOS.list_queued_workflows(limit=2, offset=2)
    assert len(workflows) == 2
    workflows = DBOS.list_queued_workflows(offset=queued_steps - 1)
    assert len(workflows) == 1
    workflows = DBOS.list_queued_workflows(executor_id="nonexistent_executor")
    assert len(workflows) == 0
    workflows = DBOS.list_queued_workflows(executor_id=GlobalParams.executor_id)
    assert len(workflows) == queued_steps
    workflows = DBOS.list_queued_workflows(executor_id=[GlobalParams.executor_id, "no"])
    assert len(workflows) == queued_steps
    workflows = DBOS.list_queued_workflows(executor_id=["no", "also_no"])
    assert len(workflows) == 0

    # Confirm the workflow finishes and nothing is enqueued afterwards
    event.set()
    assert handle.get_result() == [0, 1, 2, 3, 4]
    workflows = DBOS.list_queued_workflows()
    assert len(workflows) == 0
    workflows = DBOS.list_queued_workflows(status="SUCCESS")
    assert len(workflows) == queued_steps

    # Test the steps are listed properly
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(steps) == queued_steps * 2
    for i in range(queued_steps):
        # Check the enqueues
        assert steps[i]["function_id"] == i + 1
        assert steps[i]["function_name"] == f"<temp>.{blocking_step.__qualname__}"
        assert steps[i]["child_workflow_id"] is not None
        assert steps[i]["output"] is None
        assert steps[i]["error"] is None
        # Check the get_results
        assert steps[i + queued_steps]["function_id"] == queued_steps + i + 1
        assert steps[i + queued_steps]["function_name"] == "DBOS.getResult"
        assert steps[i + queued_steps]["child_workflow_id"] is not None
        assert steps[i + queued_steps]["output"] == i
        assert steps[i + queued_steps]["error"] is None

    child_workflows = DBOS.list_workflows(name=f"<temp>.{blocking_step.__qualname__}")
    assert (len(child_workflows)) == queued_steps
    for i, c in enumerate(child_workflows):
        steps = DBOS.list_workflow_steps(c.workflow_id)
        assert len(steps) == 1
        assert steps[0]["function_id"] == 1
        assert steps[0]["function_name"] == blocking_step.__qualname__
        assert steps[0]["child_workflow_id"] is None
        assert steps[0]["output"] == i
        assert steps[0]["error"] is None


def test_list_2steps_sleep(dbos: DBOS) -> None:

    @DBOS.workflow()
    def simple_workflow() -> None:
        stepOne()
        stepTwo()
        DBOS.sleep(1)
        return

    @DBOS.step()
    def stepOne() -> None:
        return

    @DBOS.step()
    def stepTwo() -> None:
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        simple_workflow()

    wfsteps = DBOS.list_workflow_steps(wfid)
    assert len(wfsteps) == 3
    assert wfsteps[0]["function_name"] == stepOne.__qualname__
    assert wfsteps[1]["function_name"] == stepTwo.__qualname__
    assert wfsteps[2]["function_name"] == "DBOS.sleep"


def test_list_workflow_steps_limit(dbos: DBOS) -> None:

    @DBOS.step()
    def step_a() -> str:
        return "a"

    @DBOS.step()
    def step_b() -> str:
        return "b"

    @DBOS.step()
    def step_c() -> str:
        return "c"

    @DBOS.workflow()
    def multi_step_workflow() -> None:
        step_a()
        step_b()
        step_c()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        multi_step_workflow()

    # All steps returned without pagination
    all_steps = DBOS.list_workflow_steps(wfid)
    assert len(all_steps) == 3

    # Test LIMIT 2 returns the first two steps
    steps = DBOS.list_workflow_steps(wfid, limit=2)
    assert len(steps) == 2
    assert steps[0]["function_name"] == step_a.__qualname__
    assert steps[1]["function_name"] == step_b.__qualname__

    # Test LIMIT 2 OFFSET 1 returns the second and third steps
    steps = DBOS.list_workflow_steps(wfid, limit=2, offset=1)
    assert len(steps) == 2
    assert steps[0]["function_name"] == step_b.__qualname__
    assert steps[1]["function_name"] == step_c.__qualname__

    # Test OFFSET 2 returns only the last step
    steps = DBOS.list_workflow_steps(wfid, offset=2)
    assert len(steps) == 1
    assert steps[0]["function_name"] == step_c.__qualname__


def test_list_workflow_steps_load_output(dbos: DBOS) -> None:

    @DBOS.step()
    def step_a() -> str:
        return "a"

    @DBOS.workflow()
    def output_workflow() -> None:
        step_a()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        output_workflow()

    # By default outputs are loaded
    steps = DBOS.list_workflow_steps(wfid)
    assert len(steps) == 1
    assert steps[0]["output"] == "a"
    assert steps[0]["error"] is None

    # load_output=False skips deserializing outputs
    steps = DBOS.list_workflow_steps(wfid, load_output=False)
    assert len(steps) == 1
    assert steps[0]["function_name"] == step_a.__qualname__
    assert steps[0]["output"] is None
    assert steps[0]["error"] is None


@pytest.mark.asyncio
async def test_list_workflow_steps_load_output_async(dbos: DBOS) -> None:

    @DBOS.step()
    async def step_a() -> str:
        return "a"

    @DBOS.workflow()
    async def output_workflow() -> None:
        await step_a()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await output_workflow()

    # By default outputs are loaded
    steps = await DBOS.list_workflow_steps_async(wfid)
    assert len(steps) == 1
    assert steps[0]["output"] == "a"
    assert steps[0]["error"] is None

    # load_output=False skips deserializing outputs
    steps = await DBOS.list_workflow_steps_async(wfid, load_output=False)
    assert len(steps) == 1
    assert steps[0]["function_name"] == step_a.__qualname__
    assert steps[0]["output"] is None
    assert steps[0]["error"] is None


def test_send_recv(dbos: DBOS) -> None:

    @DBOS.workflow()
    def send_workflow(target: str) -> None:
        DBOS.send(target, "Hello, World!")
        return

    @DBOS.workflow()
    def recv_workflow() -> str:
        return str(DBOS.recv(timeout_seconds=1))

    wfid_r = str(uuid.uuid4())
    with SetWorkflowID(wfid_r):
        recv_workflow()

    wfid_s = str(uuid.uuid4())
    with SetWorkflowID(wfid_s):
        send_workflow(wfid_r)

    wfsteps_send = DBOS.list_workflow_steps(wfid_s)
    assert len(wfsteps_send) == 1
    assert wfsteps_send[0]["function_name"] == "DBOS.send"

    wfsteps_recv = DBOS.list_workflow_steps(wfid_r)
    assert len(wfsteps_recv) == 2
    assert wfsteps_recv[1]["function_name"] == "DBOS.sleep"
    assert wfsteps_recv[0]["function_name"] == "DBOS.recv"


def test_set_get_event(dbos: DBOS) -> None:
    value = "Hello, World!"

    @DBOS.workflow()
    def set_get_workflow() -> Any:
        DBOS.set_event("key", value)
        stepOne()
        DBOS.get_event("fake_id", "fake_value", 0)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return DBOS.get_event(workflow_id, "key", 1)

    @DBOS.step()
    def stepOne() -> None:
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert set_get_workflow() == value

    wfsteps = DBOS.list_workflow_steps(wfid)
    assert len(wfsteps) == 6
    assert wfsteps[0]["function_name"] == "DBOS.setEvent"
    assert wfsteps[1]["function_name"] == stepOne.__qualname__
    assert wfsteps[2]["function_name"] == "DBOS.getEvent"
    assert wfsteps[2]["child_workflow_id"] == None
    assert wfsteps[2]["output"] == None
    assert wfsteps[2]["error"] == None
    assert wfsteps[3]["function_name"] == "DBOS.sleep"
    assert wfsteps[4]["function_name"] == "DBOS.getEvent"
    assert wfsteps[4]["child_workflow_id"] == None
    assert wfsteps[4]["output"] == value
    assert wfsteps[4]["error"] == None
    assert wfsteps[5]["function_name"] == "DBOS.sleep"


def test_callchild_first_sync(dbos: DBOS) -> None:

    @DBOS.workflow()
    def parentWorkflow() -> str:
        child_id = child_workflow()
        stepOne()
        stepTwo()
        return child_id

    @DBOS.step()
    def stepOne() -> None:
        return

    @DBOS.step()
    def stepTwo() -> None:
        return

    @DBOS.workflow()
    def child_workflow() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        child_id = parentWorkflow()

    wfsteps = DBOS.list_workflow_steps(wfid)
    assert len(wfsteps) == 4
    assert wfsteps[0]["function_name"] == child_workflow.__qualname__
    assert wfsteps[0]["child_workflow_id"] == child_id
    assert wfsteps[0]["output"] == None
    assert wfsteps[0]["error"] == None
    assert wfsteps[1]["function_name"] == "DBOS.getResult"
    assert wfsteps[1]["child_workflow_id"] == child_id
    assert wfsteps[1]["output"] == child_id
    assert wfsteps[1]["error"] == None
    assert wfsteps[2]["function_name"] == stepOne.__qualname__
    assert wfsteps[3]["function_name"] == stepTwo.__qualname__


@pytest.mark.asyncio
async def test_callchild_direct_asyncio(dbos: DBOS) -> None:

    @DBOS.workflow()
    async def parentWorkflow() -> str:
        child_id = await child_workflow()
        await stepOne()
        await stepTwo()
        return child_id

    @DBOS.step()
    async def stepOne() -> None:
        return

    @DBOS.step()
    async def stepTwo() -> None:
        return

    @DBOS.workflow()
    async def child_workflow() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        child_id = await parentWorkflow()

    wfsteps = await DBOS.list_workflow_steps_async(wfid)
    assert len(wfsteps) == 4
    assert wfsteps[0]["function_name"] == child_workflow.__qualname__
    assert wfsteps[0]["child_workflow_id"] == child_id
    assert wfsteps[0]["output"] == None
    assert wfsteps[0]["error"] == None
    assert wfsteps[1]["function_name"] == "DBOS.getResult"
    assert wfsteps[1]["child_workflow_id"] == child_id
    assert wfsteps[1]["output"] == child_id
    assert wfsteps[1]["error"] == None
    assert wfsteps[2]["function_name"] == stepOne.__qualname__
    assert wfsteps[3]["function_name"] == stepTwo.__qualname__


def test_callchild_last_sync(dbos: DBOS) -> None:

    @DBOS.workflow()
    def parentWorkflow() -> None:
        stepOne()
        stepTwo()
        child_workflow()
        return

    @DBOS.step()
    def stepOne() -> None:
        return

    @DBOS.step()
    def stepTwo() -> None:
        return

    @DBOS.workflow()
    def child_workflow() -> None:
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        parentWorkflow()

    wfsteps = DBOS.list_workflow_steps(wfid)
    assert len(wfsteps) == 4
    assert wfsteps[0]["function_name"] == stepOne.__qualname__
    assert wfsteps[1]["function_name"] == stepTwo.__qualname__
    assert wfsteps[2]["function_name"] == child_workflow.__qualname__
    assert wfsteps[3]["function_name"] == "DBOS.getResult"


def test_callchild_first_async_thread(dbos: DBOS) -> None:

    @DBOS.workflow()
    def parentWorkflow() -> None:
        handle = dbos.start_workflow(child_workflow)
        handle.get_status()
        stepOne()
        stepTwo()
        return

    @DBOS.step()
    def stepOne() -> None:
        return

    @DBOS.step()
    def stepTwo() -> None:
        return

    @DBOS.workflow()
    def child_workflow() -> None:
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        parentWorkflow()

    wfsteps = DBOS.list_workflow_steps(wfid)
    assert len(wfsteps) == 4
    assert wfsteps[0]["function_name"] == child_workflow.__qualname__
    assert wfsteps[1]["function_name"] == "DBOS.getStatus"
    assert wfsteps[2]["function_name"] == stepOne.__qualname__
    assert wfsteps[3]["function_name"] == stepTwo.__qualname__


def test_list_steps_errors(dbos: DBOS) -> None:
    DBOS.register_queue("test-queue")

    @DBOS.step()
    def failing_step() -> None:
        raise Exception("fail")

    @DBOS.workflow()
    def call_step() -> None:
        return failing_step()

    @DBOS.workflow()
    def start_step() -> None:
        handle = DBOS.start_workflow(failing_step)
        return handle.get_result()

    @DBOS.workflow()
    def enqueue_step() -> None:
        handle = DBOS.enqueue_workflow("test-queue", failing_step)
        return handle.get_result()

    # Test calling a failing step directly
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            call_step()
    wfsteps = DBOS.list_workflow_steps(wfid)
    assert len(wfsteps) == 1
    assert wfsteps[0]["function_name"] == failing_step.__qualname__
    assert wfsteps[0]["child_workflow_id"] == None
    assert wfsteps[0]["output"] == None
    assert isinstance(wfsteps[0]["error"], Exception)

    # Test start_workflow on a failing step
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            start_step()
    wfsteps = DBOS.list_workflow_steps(wfid)
    assert len(wfsteps) == 2
    assert wfsteps[0]["function_name"] == f"<temp>.{failing_step.__qualname__}"
    assert wfsteps[0]["child_workflow_id"] == f"{wfid}-1"
    assert wfsteps[0]["output"] == None
    assert wfsteps[0]["error"] == None
    assert wfsteps[1]["function_name"] == f"DBOS.getResult"
    assert wfsteps[1]["child_workflow_id"] == f"{wfid}-1"
    assert wfsteps[1]["output"] == None
    assert isinstance(wfsteps[1]["error"], Exception)

    # Test enqueueing a failing step
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            enqueue_step()
    wfsteps = DBOS.list_workflow_steps(wfid)
    assert len(wfsteps) == 2
    assert wfsteps[0]["function_name"] == f"<temp>.{failing_step.__qualname__}"
    assert wfsteps[0]["child_workflow_id"] == f"{wfid}-1"
    assert wfsteps[0]["output"] == None
    assert wfsteps[0]["error"] == None
    assert wfsteps[1]["function_name"] == f"DBOS.getResult"
    assert wfsteps[1]["child_workflow_id"] == f"{wfid}-1"
    assert wfsteps[1]["output"] == None
    assert isinstance(wfsteps[1]["error"], Exception)


@pytest.mark.asyncio
async def test_list_steps_errors_async(dbos: DBOS) -> None:
    await DBOS.register_queue_async("test-queue")

    @DBOS.step()
    async def failing_step() -> None:
        raise Exception("fail")

    @DBOS.workflow()
    async def call_step() -> None:
        return await failing_step()

    @DBOS.workflow()
    async def start_step() -> None:
        handle = await DBOS.start_workflow_async(failing_step)
        return await handle.get_result()

    @DBOS.workflow()
    async def enqueue_step() -> None:
        handle = await DBOS.enqueue_workflow_async("test-queue", failing_step)
        return await handle.get_result()

    # Test calling a failing step directly
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            await call_step()
    wfsteps = await DBOS.list_workflow_steps_async(wfid)
    assert len(wfsteps) == 1
    assert wfsteps[0]["function_name"] == failing_step.__qualname__
    assert wfsteps[0]["child_workflow_id"] == None
    assert wfsteps[0]["output"] == None
    assert isinstance(wfsteps[0]["error"], Exception)

    # Test start_workflow on a failing step
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            await start_step()
    wfsteps = await DBOS.list_workflow_steps_async(wfid)
    assert len(wfsteps) == 2
    assert wfsteps[0]["function_name"] == f"<temp>.{failing_step.__qualname__}"
    assert wfsteps[0]["child_workflow_id"] == f"{wfid}-1"
    assert wfsteps[0]["output"] == None
    assert wfsteps[0]["error"] == None
    assert wfsteps[1]["function_name"] == f"DBOS.getResult"
    assert wfsteps[1]["child_workflow_id"] == f"{wfid}-1"
    assert wfsteps[1]["output"] == None
    assert isinstance(wfsteps[1]["error"], Exception)

    # Test enqueueing a failing step
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            await enqueue_step()
    wfsteps = await DBOS.list_workflow_steps_async(wfid)
    assert len(wfsteps) == 2
    assert wfsteps[0]["function_name"] == f"<temp>.{failing_step.__qualname__}"
    assert wfsteps[0]["child_workflow_id"] == f"{wfid}-1"
    assert wfsteps[0]["output"] == None
    assert wfsteps[0]["error"] == None
    assert wfsteps[1]["function_name"] == f"DBOS.getResult"
    assert wfsteps[1]["child_workflow_id"] == f"{wfid}-1"
    assert wfsteps[1]["output"] == None
    assert isinstance(wfsteps[1]["error"], Exception)


def test_callchild_middle_async_thread(dbos: DBOS) -> None:

    @DBOS.workflow()
    def parentWorkflow() -> str:
        stepOne()
        handle = dbos.start_workflow(child_workflow)
        handle.get_status()
        stepTwo()
        handle.get_result()
        return handle.workflow_id

    @DBOS.step()
    def stepOne() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    @DBOS.step()
    def stepTwo() -> None:
        return

    @DBOS.workflow()
    def child_workflow() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        child_id = parentWorkflow()

    wfsteps = DBOS.list_workflow_steps(wfid)
    assert len(wfsteps) == 5
    assert wfsteps[0]["function_name"] == stepOne.__qualname__
    assert wfsteps[0]["child_workflow_id"] == None
    assert wfsteps[0]["output"] == wfid
    assert wfsteps[0]["error"] == None
    assert wfsteps[1]["function_name"] == child_workflow.__qualname__
    assert wfsteps[1]["child_workflow_id"] == child_id
    assert wfsteps[1]["output"] == None
    assert wfsteps[1]["error"] == None
    assert wfsteps[2]["function_name"] == "DBOS.getStatus"
    assert wfsteps[3]["function_name"] == stepTwo.__qualname__
    assert wfsteps[3]["child_workflow_id"] == None
    assert wfsteps[3]["output"] == None
    assert wfsteps[3]["error"] == None
    assert wfsteps[4]["function_name"] == "DBOS.getResult"
    assert wfsteps[4]["child_workflow_id"] == child_id
    assert wfsteps[4]["output"] == child_id
    assert wfsteps[4]["error"] == None


@pytest.mark.asyncio
async def test_callchild_first_asyncio(dbos: DBOS) -> None:

    @DBOS.workflow()
    async def parentWorkflow() -> str:
        handle = await dbos.start_workflow_async(child_workflow)
        child_id = await handle.get_result()
        await stepOne()
        await stepTwo()
        return child_id

    @DBOS.step()
    async def stepOne() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    @DBOS.step()
    async def stepTwo() -> None:
        return

    @DBOS.workflow()
    async def child_workflow() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await dbos.start_workflow_async(parentWorkflow)
        child_id = await handle.get_result()

    wfsteps = await DBOS.list_workflow_steps_async(wfid)
    assert len(wfsteps) == 4
    assert wfsteps[0]["function_name"] == child_workflow.__qualname__
    assert wfsteps[0]["child_workflow_id"] == child_id
    assert wfsteps[0]["output"] == None
    assert wfsteps[0]["error"] == None
    assert wfsteps[1]["function_name"] == "DBOS.getResult"
    assert wfsteps[1]["child_workflow_id"] == child_id
    assert wfsteps[1]["output"] == child_id
    assert wfsteps[1]["error"] == None
    assert wfsteps[2]["function_name"] == stepOne.__qualname__
    assert wfsteps[2]["child_workflow_id"] == None
    assert wfsteps[2]["output"] == wfid
    assert wfsteps[2]["error"] == None
    assert wfsteps[3]["function_name"] == stepTwo.__qualname__
    assert wfsteps[3]["child_workflow_id"] == None
    assert wfsteps[3]["output"] == None
    assert wfsteps[3]["error"] == None


def test_callchild_rerun_async_thread(dbos: DBOS) -> None:

    @DBOS.workflow()
    def parentWorkflow() -> str:
        childwfid = str(uuid.uuid4())
        with SetWorkflowID(childwfid):
            handle = dbos.start_workflow(child_workflow, childwfid)
            return handle.get_result()

    @DBOS.workflow()
    def child_workflow(id: str) -> str:
        return id

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = dbos.start_workflow(parentWorkflow)
        res1 = handle.get_result()

    with SetWorkflowID(wfid):
        handle = dbos.start_workflow(parentWorkflow)
        res2 = handle.get_result()

    assert res1 == res2


def test_callchild_rerun_sync(dbos: DBOS) -> None:

    @DBOS.workflow()
    def parentWorkflow() -> str:
        childwfid = str(uuid.uuid4())
        with SetWorkflowID(childwfid):
            return child_workflow(childwfid)

    @DBOS.workflow()
    def child_workflow(id: str) -> str:
        return id

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        res1 = parentWorkflow()

    with SetWorkflowID(wfid):
        res2 = parentWorkflow()

    assert res1 == res2


@pytest.mark.asyncio
async def test_callchild_rerun_asyncio(dbos: DBOS) -> None:

    @DBOS.workflow()
    async def parentWorkflow() -> str:
        childwfid = str(uuid.uuid4())
        with SetWorkflowID(childwfid):
            handle = await dbos.start_workflow_async(child_workflow, childwfid)
            return await handle.get_result()

    @DBOS.workflow()
    async def child_workflow(id: str) -> str:
        return id

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await dbos.start_workflow_async(parentWorkflow)
        res1 = await handle.get_result()

    with SetWorkflowID(wfid):
        handle = await dbos.start_workflow_async(parentWorkflow)
        res2 = await handle.get_result()

    assert res1 == res2


def test_list_workflows_as_step(dbos: DBOS) -> None:
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.workflow()
    def listing_workflow() -> int:
        length = len(DBOS.list_workflows())
        main_thread_event.set()
        workflow_event.wait()
        return length

    @DBOS.workflow()
    def simple_workflow() -> None:
        return

    # Start the workflow. It should find one workflow.
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(listing_workflow)
    main_thread_event.wait()

    # Run another workflow
    simple_workflow()

    # Run the listing workflow again with the same ID.
    with SetWorkflowID(wfid):
        handle_two = DBOS.start_workflow(listing_workflow)

    # Complete both executions. They should each find one workflow.
    workflow_event.set()
    assert handle.get_result() == 1
    assert handle_two.get_result() == 1


def test_call_as_step_within_step(dbos: DBOS) -> None:
    # If we call any util functions within a step, it should be called directly without checkpointing

    @DBOS.step()
    def getStatus(workflow_id: str) -> str:
        status = DBOS.get_workflow_status(workflow_id)
        assert status is not None
        return status.status

    @DBOS.workflow()
    def getStatusWorkflow() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return getStatus(workflow_id)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        status = getStatusWorkflow()
        assert status == WorkflowStatusString.PENDING.value

    steps = DBOS.list_workflow_steps(wfid)

    assert len(steps) == 1
    assert steps[0]["function_name"] == getStatus.__qualname__


def test_step_timing(dbos: DBOS) -> None:
    num_steps = 5
    start_time = int(time.time() * 1000)

    @DBOS.step()
    def step() -> None:
        time.sleep(0.1)

    @DBOS.workflow()
    def workflow() -> None:
        for _ in range(num_steps):
            step()
        DBOS.set_event("key", "value")
        DBOS.list_workflows()
        DBOS.recv(timeout_seconds=0)

    handle = DBOS.start_workflow(workflow)
    handle.get_result()

    steps = DBOS.list_workflow_steps(handle.workflow_id)
    for s in steps:
        assert s["started_at_epoch_ms"] and s["completed_at_epoch_ms"]
        assert s["started_at_epoch_ms"] >= start_time
        assert s["completed_at_epoch_ms"] >= s["started_at_epoch_ms"]
        if s["function_id"] < num_steps:
            assert s["completed_at_epoch_ms"] - s["started_at_epoch_ms"] >= 100


@pytest.mark.asyncio
async def test_async_step_timing(dbos: DBOS) -> None:
    num_steps = 5
    start_time = int(time.time() * 1000)

    @DBOS.step()
    async def step() -> None:
        await asyncio.sleep(0.1)

    @DBOS.workflow()
    async def workflow() -> None:
        for _ in range(num_steps):
            await step()
        await DBOS.set_event_async("key", "value")
        await DBOS.list_workflows_async()
        await DBOS.recv_async(timeout_seconds=0)

    handle = await DBOS.start_workflow_async(workflow)
    await handle.get_result()

    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    for s in steps:
        assert s["started_at_epoch_ms"] and s["completed_at_epoch_ms"]
        assert s["started_at_epoch_ms"] >= start_time
        assert s["completed_at_epoch_ms"] >= s["started_at_epoch_ms"]
        if s["function_id"] < num_steps:
            assert s["completed_at_epoch_ms"] - s["started_at_epoch_ms"] >= 100


def _get_result_step(steps: list[Any]) -> Any:
    matches = [s for s in steps if s["function_name"] == "DBOS.getResult"]
    assert len(matches) == 1, f"expected one getResult step, got {len(matches)}"
    return matches[0]


def _only_step(steps: list[Any], function_name: str) -> Any:
    matches = [s for s in steps if s["function_name"] == function_name]
    assert len(matches) == 1, f"expected one {function_name} step, got {len(matches)}"
    return matches[0]


def test_sleep_timing(dbos: DBOS) -> None:
    @DBOS.workflow()
    def workflow() -> None:
        DBOS.sleep(1.0)

    handle = DBOS.start_workflow(workflow)
    handle.get_result()

    step = _only_step(DBOS.list_workflow_steps(handle.workflow_id), "DBOS.sleep")
    assert step["started_at_epoch_ms"] and step["completed_at_epoch_ms"]
    assert step["completed_at_epoch_ms"] - step["started_at_epoch_ms"] >= 1000


@pytest.mark.asyncio
async def test_sleep_timing_async(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def workflow() -> None:
        await DBOS.sleep_async(1.0)

    handle = await DBOS.start_workflow_async(workflow)
    await handle.get_result()

    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    step = _only_step(steps, "DBOS.sleep")
    assert step["started_at_epoch_ms"] and step["completed_at_epoch_ms"]
    assert step["completed_at_epoch_ms"] - step["started_at_epoch_ms"] >= 1000


def test_recv_timeout_registration_is_instantaneous(dbos: DBOS) -> None:
    @DBOS.workflow()
    def receiver() -> Any:
        return DBOS.recv(timeout_seconds=60)

    handle = DBOS.start_workflow(receiver)

    # Poll rather than sleep, so the floor is our wait and not workflow startup.
    def registered() -> None:
        assert any(
            s["function_name"] == "DBOS.sleep"
            for s in DBOS.list_workflow_steps(handle.workflow_id)
        )

    retry_until_success(registered)
    waiting_since = time.time()
    time.sleep(1.0)
    DBOS.send(handle.workflow_id, "msg")
    assert handle.get_result() == "msg"
    waited_ms = int((time.time() - waiting_since) * 1000)

    steps = DBOS.list_workflow_steps(handle.workflow_id)
    # The registration is abandoned on delivery, so it cannot span the wait.
    sleep_step = _only_step(steps, "DBOS.sleep")
    sleep_ms = sleep_step["completed_at_epoch_ms"] - sleep_step["started_at_epoch_ms"]
    assert sleep_ms < waited_ms

    recv_step = _only_step(steps, "DBOS.recv")
    assert recv_step["completed_at_epoch_ms"] - recv_step["started_at_epoch_ms"] >= 900


def test_sleep_timing_unchanged_on_replay(dbos: DBOS) -> None:
    runs: list[int] = []

    @DBOS.workflow()
    def workflow() -> None:
        runs.append(1)
        DBOS.sleep(1.0)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        workflow()
    before = _only_step(DBOS.list_workflow_steps(wfid), "DBOS.sleep")

    # Without the PENDING reset the body is short-circuited and never re-runs.
    set_workflow_status(dbos._sys_db, wfid, "PENDING")
    DBOS._recover_pending_workflows()
    DBOS.retrieve_workflow(wfid).get_result()
    assert len(runs) == 2

    after = _only_step(DBOS.list_workflow_steps(wfid), "DBOS.sleep")
    assert after["started_at_epoch_ms"] == before["started_at_epoch_ms"]
    assert after["completed_at_epoch_ms"] == before["completed_at_epoch_ms"]


def test_get_result_timing_from_handle(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child() -> str:
        time.sleep(0.5)
        return "c"

    @DBOS.workflow()
    def parent() -> None:
        handle = DBOS.start_workflow(child)
        handle.get_result()

    handle = DBOS.start_workflow(parent)
    handle.get_result()

    step = _get_result_step(DBOS.list_workflow_steps(handle.workflow_id))
    assert step["started_at_epoch_ms"] and step["completed_at_epoch_ms"]
    assert step["completed_at_epoch_ms"] - step["started_at_epoch_ms"] >= 400


def test_child_workflow_row_timing(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child() -> str:
        time.sleep(0.5)
        return "c"

    @DBOS.workflow()
    def parent() -> None:
        DBOS.start_workflow(child)

    handle = DBOS.start_workflow(parent)
    handle.get_result()

    steps = DBOS.list_workflow_steps(handle.workflow_id)
    rows = [s for s in steps if s["function_name"] == child.__qualname__]
    assert len(rows) == 1
    step = rows[0]
    assert step["started_at_epoch_ms"] and step["completed_at_epoch_ms"]
    assert step["completed_at_epoch_ms"] >= step["started_at_epoch_ms"]
    # Bookkeeping: must not absorb the child's 500ms runtime.
    assert step["completed_at_epoch_ms"] - step["started_at_epoch_ms"] < 400


@pytest.mark.asyncio
async def test_child_workflow_row_timing_async(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def child() -> str:
        await asyncio.sleep(0.5)
        return "c"

    @DBOS.workflow()
    async def parent() -> None:
        await DBOS.start_workflow_async(child)

    handle = await DBOS.start_workflow_async(parent)
    await handle.get_result()

    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    rows = [s for s in steps if s["function_name"] == child.__qualname__]
    assert len(rows) == 1
    step = rows[0]
    assert step["started_at_epoch_ms"] and step["completed_at_epoch_ms"]
    assert step["completed_at_epoch_ms"] >= step["started_at_epoch_ms"]
    assert step["completed_at_epoch_ms"] - step["started_at_epoch_ms"] < 400


def test_get_result_timing_sync_child(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child() -> str:
        time.sleep(0.5)
        return "c"

    @DBOS.workflow()
    def parent() -> None:
        child()

    handle = DBOS.start_workflow(parent)
    handle.get_result()

    step = _get_result_step(DBOS.list_workflow_steps(handle.workflow_id))
    assert step["started_at_epoch_ms"] and step["completed_at_epoch_ms"]
    assert step["completed_at_epoch_ms"] - step["started_at_epoch_ms"] >= 400


@pytest.mark.asyncio
async def test_get_result_timing_async_handle(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def child() -> str:
        await asyncio.sleep(0.5)
        return "c"

    @DBOS.workflow()
    async def parent() -> None:
        handle = await DBOS.start_workflow_async(child)
        await handle.get_result()

    handle = await DBOS.start_workflow_async(parent)
    await handle.get_result()

    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    step = _get_result_step(steps)
    assert step["started_at_epoch_ms"] and step["completed_at_epoch_ms"]
    assert step["completed_at_epoch_ms"] - step["started_at_epoch_ms"] >= 400


@pytest.mark.asyncio
async def test_asyncio_wait_timing(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def child() -> None:
        await asyncio.sleep(0.5)

    @DBOS.workflow()
    async def parent() -> None:
        handle = await DBOS.start_workflow_async(child)
        await DBOS.asyncio_wait([asyncio.ensure_future(handle.get_result())])

    handle = await DBOS.start_workflow_async(parent)
    await handle.get_result()

    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    waits = [s for s in steps if s["function_name"] == "DBOS.asyncio_wait"]
    assert len(waits) == 1
    step = waits[0]
    assert step["started_at_epoch_ms"] and step["completed_at_epoch_ms"]
    assert step["completed_at_epoch_ms"] - step["started_at_epoch_ms"] >= 400


@pytest.mark.asyncio
async def test_get_result_timing_async_direct_child(dbos: DBOS) -> None:
    """Pending.then invokes its callback only after awaiting the body."""

    @DBOS.workflow()
    async def child() -> str:
        await asyncio.sleep(1.0)
        return "c"

    @DBOS.workflow()
    async def parent() -> None:
        await child()

    handle = await DBOS.start_workflow_async(parent)
    await handle.get_result()

    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    step = _get_result_step(steps)
    assert step["started_at_epoch_ms"] and step["completed_at_epoch_ms"]
    assert step["completed_at_epoch_ms"] - step["started_at_epoch_ms"] >= 900


def test_get_result_timing_enqueued_child(dbos: DBOS) -> None:
    """Covers the polling handle, which only an enqueued child produces."""
    queue = DBOS.register_queue("timing-queue")

    @DBOS.workflow()
    def child() -> str:
        time.sleep(1.0)
        return "c"

    @DBOS.workflow()
    def parent() -> None:
        queue.enqueue(child).get_result()

    handle = DBOS.start_workflow(parent)
    handle.get_result()

    steps = DBOS.list_workflow_steps(handle.workflow_id)
    get_result = _get_result_step(steps)
    assert (
        get_result["completed_at_epoch_ms"] - get_result["started_at_epoch_ms"] >= 900
    )

    marker = _only_step(steps, child.__qualname__)
    assert marker["started_at_epoch_ms"] and marker["completed_at_epoch_ms"]
    assert marker["completed_at_epoch_ms"] - marker["started_at_epoch_ms"] < 900


def test_get_result_timing_on_error(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child() -> None:
        time.sleep(1.0)
        raise Exception("boom")

    @DBOS.workflow()
    def parent() -> None:
        DBOS.start_workflow(child).get_result()

    handle = DBOS.start_workflow(parent)
    with pytest.raises(Exception):
        handle.get_result()

    step = _get_result_step(DBOS.list_workflow_steps(handle.workflow_id))
    assert step["started_at_epoch_ms"] and step["completed_at_epoch_ms"]
    assert step["completed_at_epoch_ms"] - step["started_at_epoch_ms"] >= 900


def test_child_marker_timing_direct_call(dbos: DBOS) -> None:
    """The direct-call path builds its own child_start_time."""

    @DBOS.workflow()
    def child() -> str:
        time.sleep(1.0)
        return "c"

    @DBOS.workflow()
    def parent() -> None:
        child()

    handle = DBOS.start_workflow(parent)
    handle.get_result()

    marker = _only_step(
        DBOS.list_workflow_steps(handle.workflow_id), child.__qualname__
    )
    assert marker["started_at_epoch_ms"] and marker["completed_at_epoch_ms"]
    assert marker["completed_at_epoch_ms"] - marker["started_at_epoch_ms"] < 900


def test_get_event_timeout_registration_is_instantaneous(dbos: DBOS) -> None:
    """get_event's internal sleep is the second deliberately-unprojected call site."""

    @DBOS.workflow()
    def setter() -> None:
        time.sleep(1.0)
        DBOS.set_event("k", "v")

    getter_started = threading.Event()

    @DBOS.workflow()
    def getter(target: str) -> Any:
        getter_started.set()
        return DBOS.get_event(target, "k", timeout_seconds=60)

    target_id = str(uuid.uuid4())
    handle = DBOS.start_workflow(getter, target_id)
    assert getter_started.wait(timeout=10)
    with SetWorkflowID(target_id):
        DBOS.start_workflow(setter)
    assert handle.get_result() == "v"

    steps = DBOS.list_workflow_steps(handle.workflow_id)
    sleep_step = _only_step(steps, "DBOS.sleep")
    assert sleep_step["completed_at_epoch_ms"] - sleep_step["started_at_epoch_ms"] < 900

    get_event_step = _only_step(steps, "DBOS.getEvent")
    span = (
        get_event_step["completed_at_epoch_ms"] - get_event_step["started_at_epoch_ms"]
    )
    assert span >= 900


def _completed_workflow_id(dbos: DBOS) -> str:
    @DBOS.workflow()
    def workflow() -> None:
        return

    handle = DBOS.start_workflow(workflow)
    handle.get_result()
    return handle.workflow_id


def test_step_conflict_over_child_workflow_row(dbos: DBOS) -> None:
    workflow_id = _completed_workflow_id(dbos)

    dbos._sys_db.record_operation_result(
        {
            "workflow_uuid": workflow_id,
            "function_id": 10,
            "function_name": "child.wf",
            "output": None,
            "error": None,
            "serialization": None,
            "started_at_epoch_ms": int(time.time() * 1000),
            "child_workflow_id": str(uuid.uuid4()),
        }
    )

    result: OperationResultInternal = {
        "workflow_uuid": workflow_id,
        "function_id": 10,
        "function_name": "a.step",
        "output": "1",
        "error": None,
        "serialization": None,
        "started_at_epoch_ms": int(time.time() * 1000),
        "child_workflow_id": None,
    }
    with pytest.raises(DBOSStepNondeterminismError):
        # Explicit far-future completion time so it can't equal the child row's same-millisecond completed_at.
        dbos._sys_db.record_operation_result(
            result, completed_at_epoch_ms=int(time.time() * 1000) + 3_600_000
        )


def test_step_conflict_over_row_without_completion(dbos: DBOS) -> None:
    """No current write path leaves one NULL, so the row is built by hand."""
    workflow_id = _completed_workflow_id(dbos)

    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.operation_outputs).values(
                workflow_uuid=workflow_id,
                function_id=11,
                function_name="legacy.step",
                child_workflow_id=str(uuid.uuid4()),
            )
        )

    result: OperationResultInternal = {
        "workflow_uuid": workflow_id,
        "function_id": 11,
        "function_name": "a.step",
        "output": "1",
        "error": None,
        "serialization": None,
        "started_at_epoch_ms": int(time.time() * 1000),
        "child_workflow_id": None,
    }
    with pytest.raises(DBOSStepNondeterminismError):
        dbos._sys_db.record_operation_result(result)


def test_list_workflows_by_parent(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child_workflow(name: str) -> str:
        return f"child_{name}"

    @DBOS.workflow()
    def parent_workflow() -> tuple[str, str, str]:
        parent_id = DBOS.workflow_id
        assert parent_id is not None
        # Start multiple child workflows
        child_workflow("sync")
        handle1 = dbos.start_workflow(child_workflow, "async1")
        handle2 = dbos.start_workflow(child_workflow, "async2")
        handle1.get_result()
        handle2.get_result()
        return parent_id, handle1.workflow_id, handle2.workflow_id

    # Run the parent workflow
    parent_id = str(uuid.uuid4())
    with SetWorkflowID(parent_id):
        _, async_child1_id, async_child2_id = parent_workflow()

    # The sync child workflow ID follows the pattern: parent_id-function_id
    sync_child_id = f"{parent_id}-1"

    # Verify each child handle has correct parent_workflow_id
    sync_child_status = DBOS.get_workflow_status(sync_child_id)
    assert sync_child_status is not None
    assert sync_child_status.parent_workflow_id == parent_id

    async_child1_status = DBOS.get_workflow_status(async_child1_id)
    assert async_child1_status is not None
    assert async_child1_status.parent_workflow_id == parent_id

    async_child2_status = DBOS.get_workflow_status(async_child2_id)
    assert async_child2_status is not None
    assert async_child2_status.parent_workflow_id == parent_id

    # List all workflows - should have parent + 3 children
    all_workflows = DBOS.list_workflows()
    assert len(all_workflows) == 4

    # List workflows by parent_workflow_id - should only return the 3 children
    children = DBOS.list_workflows(parent_workflow_id=parent_id)
    assert len(children) == 3
    for child in children:
        assert child.parent_workflow_id == parent_id
        assert child.name == child_workflow.__qualname__

    # Verify parent workflow has no parent
    parent_status = DBOS.get_workflow_status(parent_id)
    assert parent_status is not None
    assert parent_status.parent_workflow_id is None

    # Filter with non-existent parent ID returns nothing
    no_children = DBOS.list_workflows(parent_workflow_id="nonexistent")
    assert len(no_children) == 0

    # Test searching by parent_workflow_id as a list
    children = DBOS.list_workflows(parent_workflow_id=[parent_id, "nonexistent"])
    assert len(children) == 3
    no_children = DBOS.list_workflows(
        parent_workflow_id=["nonexistent", "also_nonexistent"]
    )
    assert len(no_children) == 0


def test_list_workflows_has_parent(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child_workflow() -> None:
        return

    @DBOS.workflow()
    def parent_workflow() -> None:
        child_workflow()

    # Run a parent workflow (which starts a child) and a standalone workflow
    parent_id = str(uuid.uuid4())
    with SetWorkflowID(parent_id):
        parent_workflow()

    standalone_id = str(uuid.uuid4())
    with SetWorkflowID(standalone_id):
        child_workflow()

    # All workflows: parent + child + standalone = 3
    all_wfs = DBOS.list_workflows()
    assert len(all_wfs) == 3

    # has_parent=True returns only the child (which has a parent)
    with_parent = DBOS.list_workflows(has_parent=True)
    assert len(with_parent) == 1
    assert with_parent[0].parent_workflow_id == parent_id

    # has_parent=False returns workflows without a parent
    without_parent = DBOS.list_workflows(has_parent=False)
    assert len(without_parent) == 2
    ids = {wf.workflow_id for wf in without_parent}
    assert parent_id in ids
    assert standalone_id in ids


def _bound_values(parameters: Any) -> List[Any]:
    """Flatten a driver parameter set: psycopg passes a dict, sqlite3 a tuple."""
    if isinstance(parameters, dict):
        return list(parameters.values())
    if isinstance(parameters, (list, tuple)):
        values: List[Any] = []
        for parameter in parameters:
            values.extend(_bound_values(parameter))
        return values
    return [parameters]


def test_time_filters_bind_as_integers(dbos: DBOS) -> None:
    """Every epoch-ms time filter must bind as an int, not a float: a float bind has
    Postgres cast the BIGINT column to double precision, costing the query its index."""

    ts = "2026-01-01T00:00:00+00:00"
    epoch_ms = int(datetime.fromisoformat(ts).timestamp() * 1000)
    sys_db = dbos._sys_db

    def list_workflows() -> Any:
        return sys_db.list_workflows(
            start_time=ts,
            end_time=ts,
            completed_after=ts,
            completed_before=ts,
            dequeued_after=ts,
            dequeued_before=ts,
        )

    # Each filter, and the number of epoch-ms bounds it emits.
    cases: List[Tuple[str, Callable[[], Any], int]] = [
        ("list_workflows", list_workflows, 6),
    ]

    captured: List[Any] = []
    test_thread = threading.get_ident()

    def before_cursor_execute(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        # Queue pollers share this engine; only this thread's statements are ours.
        if threading.get_ident() == test_thread:
            captured.append(parameters)

    for name, call, expected_bounds in cases:
        captured.clear()
        event.listen(sys_db.engine, "before_cursor_execute", before_cursor_execute)
        try:
            call()
        finally:
            event.remove(sys_db.engine, "before_cursor_execute", before_cursor_execute)
        values = [v for parameters in captured for v in _bound_values(parameters)]
        assert not [v for v in values if isinstance(v, float)], name
        assert values.count(epoch_ms) == expected_bounds, name


def _make_sysdb(pool_size: int = 2, **kwargs: Any) -> SystemDatabase:
    """A second handle on the test system database, so a test can pick its timeout."""
    return SystemDatabase.create(
        system_database_url=postgres_urls()[1],
        engine_kwargs={"pool_size": pool_size},
        engine=None,
        schema="dbos",
        serializer=DefaultSerializer(),
        executor_id=None,
        **kwargs,
    )


def test_observability_queries_carry_a_statement_timeout(
    skip_with_sqlite: None, dbos: DBOS
) -> None:
    with dbos._sys_db._observability_query() as c:
        assert c.execute(sa.text("SHOW statement_timeout")).scalar() == "30s"


def test_observability_statement_timeout_does_not_leak(
    skip_with_sqlite: None, dbos: DBOS
) -> None:
    """SET LOCAL, so the cap dies with its transaction rather than riding the
    pooled connection into unrelated queries."""
    # A one-connection pool, so the next transaction is guaranteed the same session:
    # against a larger pool this draws a different connection and proves nothing.
    sys_db = _make_sysdb(pool_size=1)
    try:
        with sys_db._observability_query() as c:
            assert c.execute(sa.text("SHOW statement_timeout")).scalar() == "30s"
            session = id(c.connection.dbapi_connection)

        with sys_db.engine.begin() as c:
            assert id(c.connection.dbapi_connection) == session
            assert c.execute(sa.text("SHOW statement_timeout")).scalar() == "0"
    finally:
        sys_db.destroy()


def test_every_observability_query_sets_the_timeout(
    skip_with_sqlite: None, dbos: DBOS
) -> None:
    """Each capped method must issue the cap on its own transaction. Reverting any
    one of them to a plain engine.begin() has to fail here."""
    sys_db = dbos._sys_db

    @DBOS.workflow()
    def simple_workflow() -> int:
        return step()

    @DBOS.step()
    def step() -> int:
        return 1

    assert simple_workflow() == 1
    wfid = DBOS.list_workflows()[0].workflow_id
    cases: List[Tuple[str, Callable[[], Any]]] = [
        ("list_workflows", lambda: sys_db.list_workflows()),
        ("list_workflow_steps", lambda: sys_db.list_workflow_steps(wfid)),
        ("get_all_events", lambda: sys_db.get_all_events(wfid)),
        ("list_application_versions", lambda: sys_db.list_application_versions()),
    ]

    captured: List[str] = []
    test_thread = threading.get_ident()

    def before_cursor_execute(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        # Queue pollers share this engine; only this thread's statements are ours.
        if threading.get_ident() == test_thread:
            captured.append(statement)

    for name, call in cases:
        captured.clear()
        event.listen(sys_db.engine, "before_cursor_execute", before_cursor_execute)
        try:
            call()
        finally:
            event.remove(sys_db.engine, "before_cursor_execute", before_cursor_execute)
        assert [s for s in captured if "statement_timeout" in s] == [
            "SET LOCAL statement_timeout = 30000"
        ], name


def test_id_keyed_workflow_lookup_is_not_capped(
    skip_with_sqlite: None, dbos: DBOS
) -> None:
    """A workflow_ids filter is a primary-key identity read, bounded by the ID list,
    so it takes no cap: a status lookup must not fail the workflow that made it."""
    sys_db = dbos._sys_db

    @DBOS.workflow()
    def simple_workflow() -> int:
        return 1

    assert simple_workflow() == 1
    wfid = DBOS.list_workflows()[0].workflow_id

    captured: List[str] = []
    test_thread = threading.get_ident()

    def before_cursor_execute(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        if threading.get_ident() == test_thread:
            captured.append(statement)

    cases: List[Tuple[str, Callable[[], Any]]] = [
        ("list_workflows", lambda: sys_db.list_workflows(workflow_ids=[wfid])),
        ("get_workflow_status", lambda: DBOS.get_workflow_status(wfid)),
    ]

    for name, call in cases:
        captured.clear()
        event.listen(sys_db.engine, "before_cursor_execute", before_cursor_execute)
        try:
            assert call() is not None, name
        finally:
            event.remove(sys_db.engine, "before_cursor_execute", before_cursor_execute)
        assert [s for s in captured if "statement_timeout" in s] == [], name


def test_observability_query_timeout_cancels_a_slow_query(
    skip_with_sqlite: None, dbos: DBOS
) -> None:
    sys_db = _make_sysdb(observability_query_timeout_sec=0.3)
    try:
        with pytest.raises(DBOSQueryTimeoutError):
            with sys_db._observability_query() as c:
                c.execute(sa.text("SELECT pg_sleep(30)"))

        # The cancelled query leaves its connection usable.
        with sys_db.engine.begin() as c:
            assert c.execute(sa.text("SELECT 1")).scalar() == 1
    finally:
        sys_db.destroy()


def test_observability_query_timeout_can_be_disabled(
    skip_with_sqlite: None, dbos: DBOS
) -> None:
    sys_db = _make_sysdb(observability_query_timeout_sec=0)
    try:
        assert sys_db._observability_query_timeout_ms is None
        with sys_db._observability_query() as c:
            assert c.execute(sa.text("SHOW statement_timeout")).scalar() == "0"
    finally:
        sys_db.destroy()


def test_sub_millisecond_observability_query_timeout_still_caps(
    skip_with_sqlite: None, dbos: DBOS
) -> None:
    """A sub-millisecond request must floor to 1ms, not truncate to 0, which PostgreSQL
    reads as no timeout at all."""
    sys_db = _make_sysdb(observability_query_timeout_sec=0.0005)
    try:
        assert sys_db._observability_query_timeout_ms == 1
        with sys_db._observability_query() as c:
            assert c.execute(sa.text("SHOW statement_timeout")).scalar() == "1ms"
    finally:
        sys_db.destroy()


def test_configured_observability_query_timeout(
    config: DBOSConfig, cleanup_test_databases: None, skip_with_sqlite: None
) -> None:
    config["observability_query_timeout_sec"] = 5
    try:
        dbos = DBOS(config=config)
        DBOS.launch()
        assert dbos._sys_db._observability_query_timeout_ms == 5000
        with dbos._sys_db._observability_query() as c:
            assert c.execute(sa.text("SHOW statement_timeout")).scalar() == "5s"
    finally:
        DBOS.destroy(destroy_registry=True)


def test_client_observability_query_timeout(
    skip_with_sqlite: None, config: DBOSConfig, dbos: DBOS
) -> None:
    system_database_url = config["system_database_url"]
    assert system_database_url is not None
    client = DBOSClient(
        system_database_url=system_database_url,
        observability_query_timeout_sec=5,
    )
    try:
        assert client._sys_db._observability_query_timeout_ms == 5000
    finally:
        client.destroy()


def test_observability_queries_still_return_rows(dbos: DBOS) -> None:
    """The timeout must not disturb a query that finishes inside it."""

    @DBOS.workflow()
    def simple_workflow() -> int:
        return 1

    assert simple_workflow() == 1
    workflows = DBOS.list_workflows()
    assert len(workflows) == 1
    assert DBOS.list_workflow_steps(workflows[0].workflow_id) is not None
