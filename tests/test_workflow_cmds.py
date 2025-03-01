import threading
import uuid
from datetime import datetime, timedelta, timezone

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
    def simple_workflow(x: int) -> None:
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
    assert output.workflowUUID == wfid
    assert output.status == "SUCCESS"
    assert output.workflowName == simple_workflow.__qualname__
    assert output.workflowClassName == None
    assert output.workflowConfigName == None
    assert output.authenticated_user == None
    assert output.assumed_role == None
    assert output.authenticated_roles == None
    assert output.request == None
    assert output.created_at > 0
    assert output.updated_at > 0
    assert output.queue_name == None
    assert output.executor_id == GlobalParams.executor_id
    assert output.app_version == GlobalParams.app_version
    assert output.app_id == ""
    assert output.recovery_attempts == 1


def test_list_workflow_limit(dbos: DBOS, sys_db: SystemDatabase) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    simple_workflow()
    simple_workflow()
    # get the workflow list
    output = _workflow_commands.list_workflows(sys_db, limit=2)
    assert len(output) == 2, f"Expected list length to be 1, but got {len(output)}"


def test_list_workflow_status_name(dbos: DBOS, sys_db: SystemDatabase) -> None:
    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    simple_workflow()
    dbos._sys_db._flush_workflow_status_buffer()
    output = _workflow_commands.list_workflows(sys_db, status="PENDING")
    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"

    output = _workflow_commands.list_workflows(sys_db, status="SUCCESS")
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    output = _workflow_commands.list_workflows(sys_db, name="no")
    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"

    output = _workflow_commands.list_workflows(
        sys_db, name=simple_workflow.__qualname__
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"


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
        sys_db, 10, start_time=time_0, end_time=time_1
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

    wfUuid = output[0].workflowUUID

    info = _workflow_commands.get_workflow(config, wfUuid, True)
    assert info is not None, "Expected output to be not None"

    if info is not None:
        assert info.workflowUUID == wfUuid, f"Expected workflow_uuid to be {wfUuid}"


def test_queued_workflows(dbos: DBOS, config: ConfigFile) -> None:
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
    workflows = _workflow_commands.list_queued_workflows(config)
    assert len(workflows) == queued_steps
    for i, workflow in enumerate(workflows):
        assert workflow.status == WorkflowStatusString.PENDING.value
        assert workflow.queue_name == queue.name
        assert workflow.input is not None
        # Verify oldest queue entries appear first
        assert workflow.input["args"][0] == i
        assert workflow.output is None
        assert workflow.error is None
        assert "blocking_step" in workflow.workflowName

    # Test every filter
    workflows = _workflow_commands.list_queued_workflows(
        config, status=WorkflowStatusString.PENDING.value
    )
    assert len(workflows) == queued_steps
    workflows = _workflow_commands.list_queued_workflows(
        config, status=WorkflowStatusString.ENQUEUED.value
    )
    assert len(workflows) == 0
    workflows = _workflow_commands.list_queued_workflows(config, queue_name=queue.name)
    assert len(workflows) == queued_steps
    workflows = _workflow_commands.list_queued_workflows(config, queue_name="no")
    assert len(workflows) == 0
    workflows = _workflow_commands.list_queued_workflows(
        config, name=f"<temp>.{blocking_step.__qualname__}"
    )
    assert len(workflows) == queued_steps
    workflows = _workflow_commands.list_queued_workflows(config, name="no")
    assert len(workflows) == 0
    now = datetime.now(timezone.utc)
    start_time = (now - timedelta(seconds=10)).isoformat()
    end_time = (now + timedelta(seconds=10)).isoformat()
    workflows = _workflow_commands.list_queued_workflows(
        config, start_time=start_time, end_time=end_time
    )
    assert len(workflows) == queued_steps
    workflows = _workflow_commands.list_queued_workflows(
        config, start_time=now.isoformat(), end_time=end_time
    )
    assert len(workflows) == 0

    # Confirm the workflow finishes and nothing is enqueued afterwards
    event.set()
    assert handle.get_result() == [0, 1, 2, 3, 4]
    workflows = _workflow_commands.list_queued_workflows(config)
    assert len(workflows) == 0
