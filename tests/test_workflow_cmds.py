import threading
import uuid
from datetime import datetime, timedelta, timezone

import pytest

# Public API
from dbos import (
    DBOS,
    ConfigFile,
    Queue,
    SetWorkflowID,
    WorkflowStatusString,
    _workflow_commands,
)
from dbos._sys_db import SystemDatabase
from dbos._utils import GlobalParams


def test_list_workflow(dbos: DBOS, sys_db: SystemDatabase) -> None:
    @DBOS.workflow()
    def simple_workflow(x: int) -> int:
        return x + 1

    # Run a simple workflow
    wfid = str(uuid.uuid4)
    with SetWorkflowID(wfid):
        assert simple_workflow(1) == 2
    dbos._sys_db._flush_workflow_status_buffer()

    # List the workflow, then test every output
    outputs = _workflow_commands.list_workflows(sys_db)
    assert len(outputs) == 1
    output = outputs[0]
    assert output.workflow_id == wfid
    assert output.status == "SUCCESS"
    assert output.workflow_name == simple_workflow.__qualname__
    assert output.workflow_class_name == None
    assert output.workflow_config_name == None
    assert output.authenticated_user == None
    assert output.assumed_role == None
    assert output.authenticated_roles == None
    assert output.request == None
    assert output.created_at is not None and output.created_at > 0
    assert output.updated_at is not None and output.updated_at > 0
    assert output.queue_name == None
    assert output.executor_id == GlobalParams.executor_id
    assert output.app_version == GlobalParams.app_version
    assert output.app_id == ""
    assert output.recovery_attempts == 1

    # Test searching by status
    outputs = _workflow_commands.list_workflows(sys_db, status="PENDING")
    assert len(outputs) == 0
    outputs = _workflow_commands.list_workflows(sys_db, status="SUCCESS")
    assert len(outputs) == 1

    # Test searching by workflow name
    outputs = _workflow_commands.list_workflows(sys_db, name="no")
    assert len(outputs) == 0
    outputs = _workflow_commands.list_workflows(
        sys_db, name=simple_workflow.__qualname__
    )
    assert len(outputs) == 1

    # Test searching by workflow ID
    outputs = _workflow_commands.list_workflows(sys_db, workflow_ids=["no"])
    assert len(outputs) == 0
    outputs = _workflow_commands.list_workflows(sys_db, workflow_ids=[wfid, "no"])
    assert len(outputs) == 1

    # Test searching by application version
    outputs = _workflow_commands.list_workflows(sys_db, app_version="no")
    assert len(outputs) == 0
    outputs = _workflow_commands.list_workflows(
        sys_db, app_version=GlobalParams.app_version
    )
    assert len(outputs) == 1


def test_list_workflow_limit(dbos: DBOS, sys_db: SystemDatabase) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        return

    num_workflows = 5
    for i in range(num_workflows):
        with SetWorkflowID(str(i)):
            simple_workflow()

    # Test all workflows appear
    outputs = _workflow_commands.list_workflows(sys_db)
    assert len(outputs) == num_workflows
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(i)

    # Test sort_desc inverts the order:
    outputs = _workflow_commands.list_workflows(sys_db, sort_desc=True)
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(num_workflows - i - 1)

    # Test LIMIT 2 returns the first two
    outputs = _workflow_commands.list_workflows(sys_db, limit=2)
    assert len(outputs) == 2
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(i)

    # Test LIMIT 2 OFFSET 2 returns the third and fourth
    outputs = _workflow_commands.list_workflows(sys_db, limit=2, offset=2)
    assert len(outputs) == 2
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(i + 2)

    # Test OFFSET 4 returns only the fifth entry
    outputs = _workflow_commands.list_workflows(sys_db, offset=num_workflows - 1)
    assert len(outputs) == 1
    for i, output in enumerate(outputs):
        assert output.workflow_id == str(i + 4)


def test_list_workflow_start_end_times(dbos: DBOS, sys_db: SystemDatabase) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    now = datetime.now()
    starttime = (now - timedelta(seconds=20)).isoformat()
    simple_workflow()
    endtime = datetime.now().isoformat()

    output = _workflow_commands.list_workflows(
        sys_db, start_time=starttime, end_time=endtime
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    newstarttime = (now - timedelta(seconds=30)).isoformat()
    newendtime = starttime

    output = _workflow_commands.list_workflows(
        sys_db,
        start_time=newstarttime,
        end_time=newendtime,
    )
    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"


def test_list_workflow_end_times_positive(dbos: DBOS, sys_db: SystemDatabase) -> None:
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

    output = _workflow_commands.list_workflows(
        sys_db, start_time=time_0, end_time=time_1
    )
    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"

    output = _workflow_commands.list_workflows(
        sys_db, start_time=time_1, end_time=time_2
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    output = _workflow_commands.list_workflows(
        sys_db,
        start_time=time_1,
        end_time=time_3,
    )
    assert len(output) == 2, f"Expected list length to be 2, but got {len(output)}"


def test_get_workflow(dbos: DBOS, config: ConfigFile, sys_db: SystemDatabase) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    simple_workflow()
    output = _workflow_commands.list_workflows(
        sys_db,
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    assert output[0] != None, "Expected output to be not None"

    wfUuid = output[0].workflow_id

    info = _workflow_commands.get_workflow(sys_db, wfUuid, True)
    assert info is not None, "Expected output to be not None"

    if info is not None:
        assert info.workflow_id == wfUuid, f"Expected workflow_uuid to be {wfUuid}"


def test_queued_workflows(dbos: DBOS, sys_db: SystemDatabase) -> None:
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
    workflows = _workflow_commands.list_queued_workflows(sys_db)
    assert len(workflows) == queued_steps
    for i, workflow in enumerate(workflows):
        assert workflow.status == WorkflowStatusString.PENDING.value
        assert workflow.queue_name == queue.name
        assert workflow.input is not None
        # Verify oldest queue entries appear first
        assert workflow.input["args"][0] == i
        assert workflow.output is None
        assert workflow.error is None
        assert "blocking_step" in workflow.workflow_name
        assert workflow.executor_id == GlobalParams.executor_id
        assert workflow.app_version == GlobalParams.app_version
        assert workflow.created_at is not None and workflow.created_at > 0
        assert workflow.updated_at is not None and workflow.updated_at > 0
        assert workflow.recovery_attempts == 1

    # Test sort_desc inverts the order
    workflows = _workflow_commands.list_queued_workflows(sys_db, sort_desc=True)
    assert len(workflows) == queued_steps
    for i, workflow in enumerate(workflows):
        # Verify newest queue entries appear first
        assert workflow.input is not None
        assert workflow.input["args"][0] == queued_steps - i - 1

    # Verify list_workflows also properly lists the blocking steps
    workflows = _workflow_commands.list_workflows(sys_db)
    assert len(workflows) == queued_steps + 1
    for i, workflow in enumerate(workflows[1:]):
        assert workflow.status == WorkflowStatusString.PENDING.value
        assert workflow.queue_name == queue.name
        assert workflow.input is not None
        # Verify oldest queue entries appear first
        assert workflow.input["args"][0] == i
        assert workflow.output is None
        assert workflow.error is None
        assert "blocking_step" in workflow.workflow_name
        assert workflow.executor_id == GlobalParams.executor_id
        assert workflow.app_version == GlobalParams.app_version
        assert workflow.created_at is not None and workflow.created_at > 0
        assert workflow.updated_at is not None and workflow.updated_at > 0
        assert workflow.recovery_attempts == 1

    # Test every filter
    workflows = _workflow_commands.list_queued_workflows(
        sys_db, status=WorkflowStatusString.PENDING.value
    )
    assert len(workflows) == queued_steps
    workflows = _workflow_commands.list_queued_workflows(
        sys_db, status=WorkflowStatusString.ENQUEUED.value
    )
    assert len(workflows) == 0
    workflows = _workflow_commands.list_queued_workflows(sys_db, queue_name=queue.name)
    assert len(workflows) == queued_steps
    workflows = _workflow_commands.list_queued_workflows(sys_db, queue_name="no")
    assert len(workflows) == 0
    workflows = _workflow_commands.list_queued_workflows(
        sys_db, name=f"<temp>.{blocking_step.__qualname__}"
    )
    assert len(workflows) == queued_steps
    workflows = _workflow_commands.list_queued_workflows(sys_db, name="no")
    assert len(workflows) == 0
    now = datetime.now(timezone.utc)
    start_time = (now - timedelta(seconds=10)).isoformat()
    end_time = (now + timedelta(seconds=10)).isoformat()
    workflows = _workflow_commands.list_queued_workflows(
        sys_db, start_time=start_time, end_time=end_time
    )
    assert len(workflows) == queued_steps
    workflows = _workflow_commands.list_queued_workflows(
        sys_db, start_time=now.isoformat(), end_time=end_time
    )
    assert len(workflows) == 0
    workflows = _workflow_commands.list_queued_workflows(sys_db, limit=2)
    assert len(workflows) == 2
    workflows = _workflow_commands.list_queued_workflows(sys_db, limit=2, offset=2)
    assert len(workflows) == 2
    workflows = _workflow_commands.list_queued_workflows(
        sys_db, offset=queued_steps - 1
    )
    assert len(workflows) == 1

    # Confirm the workflow finishes and nothing is enqueued afterwards
    event.set()
    assert handle.get_result() == [0, 1, 2, 3, 4]
    workflows = _workflow_commands.list_queued_workflows(sys_db)
    assert len(workflows) == 0


def test_list_2steps_sleep(dbos: DBOS, sys_db: SystemDatabase) -> None:

    @DBOS.workflow()
    def simple_workflow() -> None:
        stepOne()
        stepTwo()
        DBOS.sleep(1)
        print("Executed Simple workflow")
        return

    @DBOS.step()
    def stepOne() -> None:
        print("Executed stepOne")
        return

    @DBOS.step()
    def stepTwo() -> None:
        print("Executed stepTwo")
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        simple_workflow()

    wfsteps = _workflow_commands.list_workflow_steps(sys_db, wfid)
    assert len(wfsteps.steps) == 3
    assert wfsteps.workflow_uuid == wfid
    assert wfsteps.steps[0].function_name == "stepOne"
    assert wfsteps.steps[1].function_name == "stepTwo"
    assert wfsteps.steps[2].function_name == "DBOS.sleep"


def test_send_recv(dbos: DBOS, sys_db: SystemDatabase) -> None:

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

    wfsteps_send = _workflow_commands.list_workflow_steps(sys_db, wfid_s)
    assert wfsteps_send.workflow_uuid == wfid_s
    assert len(wfsteps_send.steps) == 1
    assert wfsteps_send.steps[0].function_name == "DBOS.send"

    wfsteps_recv = _workflow_commands.list_workflow_steps(sys_db, wfid_r)
    assert wfsteps_recv.workflow_uuid == wfid_r
    assert len(wfsteps_recv.steps) == 2
    assert wfsteps_recv.steps[0].function_name == "DBOS.sleep"
    assert wfsteps_recv.steps[1].function_name == "DBOS.recv"


def test_set_get_event(dbos: DBOS, sys_db: SystemDatabase) -> None:

    @DBOS.workflow()
    def set_get_workflow() -> None:
        DBOS.set_event("key", "Hello, World!")
        stepOne()
        DBOS.get_event("wfid", "key", 1)
        return

    @DBOS.step()
    def stepOne() -> None:
        print("Executed stepOne")
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        set_get_workflow()

    wfsteps = _workflow_commands.list_workflow_steps(sys_db, wfid)
    assert wfsteps.workflow_uuid == wfid
    assert len(wfsteps.steps) == 4
    assert wfsteps.steps[0].function_name == "DBOS.setEvent"
    assert wfsteps.steps[1].function_name == "stepOne"
    assert wfsteps.steps[2].function_name == "DBOS.sleep"
    assert wfsteps.steps[3].function_name == "DBOS.getEvent"


def test_callchild_first_sync(dbos: DBOS, sys_db: SystemDatabase) -> None:

    @DBOS.workflow()
    def parentWorkflow() -> None:
        child_workflow()
        stepOne()
        stepTwo()
        return

    @DBOS.step()
    def stepOne() -> None:
        print("Executed stepOne")
        return

    @DBOS.step()
    def stepTwo() -> None:
        print("Executed stepOne")
        return

    @DBOS.workflow()
    def child_workflow() -> None:
        print("Executed child workflow")
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        parentWorkflow()

    wfsteps = _workflow_commands.list_workflow_steps(sys_db, wfid)
    assert wfsteps.workflow_uuid == wfid
    assert len(wfsteps.steps) == 3
    assert wfsteps.steps[0].function_name == "child_workflow"
    assert wfsteps.steps[1].function_name == "stepOne"
    assert wfsteps.steps[2].function_name == "stepTwo"


def test_callchild_first_async_thread(dbos: DBOS, sys_db: SystemDatabase) -> None:

    @DBOS.workflow()
    def parentWorkflow() -> None:
        handle = dbos.start_workflow(child_workflow)
        handle.get_status()
        stepOne()
        stepTwo()
        return

    @DBOS.step()
    def stepOne() -> None:
        print("Executed stepOne")
        return

    @DBOS.step()
    def stepTwo() -> None:
        print("Executed stepOne")
        return

    @DBOS.workflow()
    def child_workflow() -> None:
        print("Executed child workflow")
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        parentWorkflow()

    wfsteps = _workflow_commands.list_workflow_steps(sys_db, wfid)
    assert wfsteps.workflow_uuid == wfid
    assert len(wfsteps.steps) == 4
    assert wfsteps.steps[0].function_name == "child_workflow"
    assert wfsteps.steps[1].function_name == "getStatus"
    assert wfsteps.steps[2].function_name == "stepOne"
    assert wfsteps.steps[3].function_name == "stepTwo"


@pytest.mark.asyncio
async def test_callchild_first_asyncio(dbos: DBOS, sys_db: SystemDatabase) -> None:

    @DBOS.workflow()
    async def parentWorkflow() -> str:
        handle = await dbos.start_workflow_async(child_workflow)
        print(await handle.get_result())
        stepOne()
        stepTwo()
        return "done"

    @DBOS.step()
    def stepOne() -> None:
        print("Executed stepOne")
        return

    @DBOS.step()
    def stepTwo() -> None:
        print("Executed stepOne")
        return

    @DBOS.workflow()
    async def child_workflow() -> str:
        print("Executed child workflow")
        return "done"

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await dbos.start_workflow_async(parentWorkflow)
        print(await handle.get_result())

    dbos._sys_db._flush_workflow_status_buffer()

    wfsteps = _workflow_commands.list_workflow_steps(sys_db, wfid)
    assert wfsteps.workflow_uuid == wfid
    assert len(wfsteps.steps) == 3
    assert wfsteps.steps[0].function_name == "child_workflow"
    assert wfsteps.steps[1].function_name == "stepOne"
    assert wfsteps.steps[2].function_name == "stepTwo"


def test_callchild_rerun_async_thread(dbos: DBOS, sys_db: SystemDatabase) -> None:

    @DBOS.workflow()
    def parentWorkflow() -> str:
        childwfid = str(uuid.uuid4())
        with SetWorkflowID(childwfid):
            handle = dbos.start_workflow(child_workflow, childwfid)
            return handle.get_result()

    @DBOS.workflow()
    def child_workflow(id: str) -> str:
        print("Executed child workflow")
        return id

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = dbos.start_workflow(parentWorkflow)
        res1 = handle.get_result()

    with SetWorkflowID(wfid):
        handle = dbos.start_workflow(parentWorkflow)
        res2 = handle.get_result()

    assert res1 == res2


def test_callchild_rerun_sync(dbos: DBOS, sys_db: SystemDatabase) -> None:

    @DBOS.workflow()
    def parentWorkflow() -> str:
        childwfid = str(uuid.uuid4())
        print("generated childwfid: ", childwfid)
        with SetWorkflowID(childwfid):
            return child_workflow(childwfid)

    @DBOS.workflow()
    def child_workflow(id: str) -> str:
        print("Executed child workflow")
        return id

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        res1 = parentWorkflow()

    with SetWorkflowID(wfid):
        res2 = parentWorkflow()

    assert res1 == res2


@pytest.mark.asyncio
async def test_callchild_rerun_asyncio(dbos: DBOS, sys_db: SystemDatabase) -> None:

    @DBOS.workflow()
    async def parentWorkflow() -> str:
        childwfid = str(uuid.uuid4())
        with SetWorkflowID(childwfid):
            handle = await dbos.start_workflow_async(child_workflow, childwfid)
            return await handle.get_result()

    @DBOS.workflow()
    async def child_workflow(id: str) -> str:
        print("Executed child workflow")
        return id

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await dbos.start_workflow_async(parentWorkflow)
        res1 = await handle.get_result()

    with SetWorkflowID(wfid):
        handle = await dbos.start_workflow_async(parentWorkflow)
        res2 = await handle.get_result()

    assert res1 == res2
