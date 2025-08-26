import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

# Public API
from dbos import DBOS, Queue, SetWorkflowID, WorkflowStatusString, _workflow_commands
from dbos._app_db import ApplicationDatabase
from dbos._sys_db import SystemDatabase
from dbos._utils import GlobalParams


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
    assert output.app_version == GlobalParams.app_version
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

    # Test searching by workflow ID
    outputs = DBOS.list_workflows(workflow_ids=["no"])
    assert len(outputs) == 0
    outputs = DBOS.list_workflows(workflow_ids=[wfid, "no"])
    assert len(outputs) == 1

    # Test searching by application version
    outputs = DBOS.list_workflows(app_version="no")
    assert len(outputs) == 0
    outputs = DBOS.list_workflows(app_version=GlobalParams.app_version)
    assert len(outputs) == 1


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
    assert output.app_version == GlobalParams.app_version
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

    info = _workflow_commands.get_workflow(dbos._sys_db, wfUuid)
    assert info is not None, "Expected output to be not None"

    if info is not None:
        assert info.workflow_id == wfUuid, f"Expected workflow_uuid to be {wfUuid}"


def test_queued_workflows(dbos: DBOS, skip_with_sqlite_imprecise_time: None) -> None:
    queued_steps = 5
    step_events = [threading.Event() for _ in range(queued_steps)]
    event = threading.Event()
    queue = Queue("test_queue")

    @DBOS.workflow()
    def test_workflow() -> list[int]:
        handles = []
        for i in range(queued_steps):
            h = queue.enqueue(blocking_step, i)
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
        assert workflow.queue_name == queue.name
        assert workflow.input is not None
        # Verify oldest queue entries appear first
        assert workflow.input["args"][0] == i
        assert workflow.output is None
        assert workflow.error is None
        assert "blocking_step" in workflow.name
        assert workflow.executor_id == GlobalParams.executor_id
        assert workflow.app_version == GlobalParams.app_version
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
        assert workflow.queue_name == queue.name
        assert workflow.input is not None
        # Verify oldest queue entries appear first
        assert workflow.input["args"][0] == i
        assert workflow.output is None
        assert workflow.error is None
        assert "blocking_step" in workflow.name
        assert workflow.executor_id == GlobalParams.executor_id
        assert workflow.app_version == GlobalParams.app_version
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
    workflows = DBOS.list_queued_workflows(queue_name=queue.name)
    assert len(workflows) == queued_steps
    workflows = DBOS.list_queued_workflows(queue_name="no")
    assert len(workflows) == 0
    workflows = DBOS.list_queued_workflows(name=f"<temp>.{blocking_step.__qualname__}")
    assert len(workflows) == queued_steps
    workflows = DBOS.list_queued_workflows(name="no")
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

    # Confirm the workflow finishes and nothing is enqueued afterwards
    event.set()
    assert handle.get_result() == [0, 1, 2, 3, 4]
    workflows = DBOS.list_queued_workflows()
    assert len(workflows) == 0

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
    assert len(wfsteps) == 5
    assert wfsteps[0]["function_name"] == "DBOS.setEvent"
    assert wfsteps[1]["function_name"] == stepOne.__qualname__
    assert wfsteps[3]["function_name"] == "DBOS.sleep"
    assert wfsteps[2]["function_name"] == "DBOS.getEvent"
    assert wfsteps[2]["child_workflow_id"] == None
    assert wfsteps[2]["output"] == None
    assert wfsteps[2]["error"] == None
    assert wfsteps[4]["function_name"] == "DBOS.getEvent"
    assert wfsteps[4]["child_workflow_id"] == None
    assert wfsteps[4]["output"] == value
    assert wfsteps[4]["error"] == None


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
    queue = Queue("test-queue")

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
        handle = queue.enqueue(failing_step)
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
    queue = Queue("test-queue")

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
        handle = await queue.enqueue_async(failing_step)
        return await handle.get_result()

    # Test calling a failing step directly
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            await call_step()
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
            await start_step()
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
            await enqueue_step()
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
        stepOne()
        stepTwo()
        return child_id

    @DBOS.step()
    def stepOne() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    @DBOS.step()
    def stepTwo() -> None:
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


def test_list_transaction(dbos: DBOS) -> None:

    @DBOS.workflow()
    def simple_workflow() -> None:
        transactionOne()
        stepTwo()
        DBOS.sleep(1)
        return

    @DBOS.transaction()
    def transactionOne() -> str:
        return "a test transaction"

    @DBOS.step()
    def stepTwo() -> None:
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        simple_workflow()

    wfsteps = DBOS.list_workflow_steps(wfid)
    assert len(wfsteps) == 3
    assert wfsteps[0]["function_name"] == transactionOne.__qualname__
    assert wfsteps[0]["output"] == "a test transaction"
    assert wfsteps[0]["error"] == None
    assert wfsteps[1]["function_name"] == stepTwo.__qualname__
    assert wfsteps[2]["function_name"] == "DBOS.sleep"


def test_list_transaction_error(dbos: DBOS) -> None:

    @DBOS.workflow()
    def simple_workflow() -> None:
        transactionOne()
        stepTwo()
        try:
            transactionErr()
        except Exception as e:
            print(f"Error: {e}")
        DBOS.sleep(1)
        return

    @DBOS.transaction()
    def transactionOne() -> str:
        return "a test transaction"

    @DBOS.transaction()
    def transactionErr() -> None:
        raise Exception("a test transaction error")

    @DBOS.step()
    def stepTwo() -> None:
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        simple_workflow()

    wfsteps = DBOS.list_workflow_steps(wfid)
    assert len(wfsteps) == 4
    assert wfsteps[0]["function_name"] == transactionOne.__qualname__
    assert wfsteps[0]["output"] == "a test transaction"
    assert wfsteps[0]["error"] == None
    assert wfsteps[1]["function_name"] == stepTwo.__qualname__
    assert wfsteps[2]["function_name"] == transactionErr.__qualname__
    assert wfsteps[2]["output"] == None
    assert isinstance(wfsteps[2]["error"], Exception)
    assert wfsteps[3]["function_name"] == "DBOS.sleep"


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

    @DBOS.transaction()
    def transactionStatus() -> None:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        DBOS.get_workflow_status(workflow_id)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        status = getStatusWorkflow()
        assert status == WorkflowStatusString.PENDING.value

    steps = DBOS.list_workflow_steps(wfid)

    assert len(steps) == 1
    assert steps[0]["function_name"] == getStatus.__qualname__

    with pytest.raises(Exception) as exc_info:
        transactionStatus()
    assert "Invalid call to `DBOS.getStatus` inside a transaction" in str(
        exc_info.value
    )
