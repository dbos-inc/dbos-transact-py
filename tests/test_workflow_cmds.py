import threading
import time
from datetime import datetime, timedelta, timezone

# Public API
from dbos import DBOS, ConfigFile, Queue, WorkflowStatusString, _workflow_commands


def test_list_workflow(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing list_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    output = _workflow_commands.list_workflows(
        config, 10, None, None, None, None, False, None
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"
    assert output[0] != None, "Expected output to be not None"
    if output[0] != None:
        assert output[0].workflowUUID.strip(), "field_name is an empty string"


def test_list_workflow_limit(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing list_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    simple_workflow()
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    output = _workflow_commands.list_workflows(
        config, 2, None, None, None, None, False, None
    )
    assert len(output) == 2, f"Expected list length to be 1, but got {len(output)}"


def test_list_workflow_status(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing list_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    output = _workflow_commands.list_workflows(
        config, 10, None, None, None, "PENDING", False, None
    )
    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"

    output = _workflow_commands.list_workflows(
        config, 10, None, None, None, "SUCCESS", False, None
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"


def test_list_workflow_start_end_times(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing list_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    now = datetime.now()
    starttime = (now - timedelta(seconds=20)).isoformat()
    print(starttime)

    # run the workflow
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    endtime = datetime.now().isoformat()
    print(endtime)

    output = _workflow_commands.list_workflows(
        config, 10, None, starttime, endtime, None, False, None
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    newstarttime = (now - timedelta(seconds=30)).isoformat()
    newendtime = starttime

    output = _workflow_commands.list_workflows(
        config, 10, None, newstarttime, newendtime, None, False, None
    )
    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"


def test_list_workflow_end_times_positive(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing list_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    now = datetime.now()

    time_0 = (now - timedelta(seconds=40)).isoformat()

    time_1 = (now - timedelta(seconds=20)).isoformat()

    # run the workflow
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    time_2 = datetime.now().isoformat()

    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    time_3 = datetime.now().isoformat()

    output = _workflow_commands.list_workflows(
        config, 10, None, time_0, time_1, None, False, None
    )

    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"

    output = _workflow_commands.list_workflows(
        config, 10, None, time_1, time_2, None, False, None
    )

    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    output = _workflow_commands.list_workflows(
        config, 10, None, time_1, time_3, None, False, None
    )
    assert len(output) == 2, f"Expected list length to be 2, but got {len(output)}"


def test_get_workflow(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing get_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    output = _workflow_commands.list_workflows(
        config, 10, None, None, None, None, False, None
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    assert output[0] != None, "Expected output to be not None"

    wfUuid = output[0].workflowUUID

    info = _workflow_commands.get_workflow(config, wfUuid, True)
    assert info is not None, "Expected output to be not None"

    if info is not None:
        assert info.workflowUUID == wfUuid, f"Expected workflow_uuid to be {wfUuid}"


def test_cancel_workflow(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing get_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        time.sleep(3)
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    # get the workflow list
    output = _workflow_commands.list_workflows(
        config, 10, None, None, None, None, False, None
    )
    # assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    print(output[0])
    assert output[0] != None, "Expected output to be not None"
    wfUuid = output[0].workflowUUID

    _workflow_commands.cancel_workflow(config, wfUuid)

    info = _workflow_commands.get_workflow(config, wfUuid, True)
    assert info is not None, "Expected info to be not None"
    if info is not None:
        assert info.status == "CANCELLED", f"Expected status to be CANCELLED"


def test_queued_workflows(dbos: DBOS, config: ConfigFile) -> None:
    queued_steps = 5
    step_events = [threading.Event() for _ in range(queued_steps)]
    event = threading.Event()
    queue = Queue("test_queue")

    @DBOS.workflow()
    def test_workflow() -> list[int]:
        handles = []
        for i in range(queued_steps):
            h = queue.enqueue(test_step, i)
            handles.append(h)
        return [h.get_result() for h in handles]

    @DBOS.step()
    def test_step(i: int) -> int:
        step_events[i].set()
        event.wait()
        return i

    handle = DBOS.start_workflow(test_workflow)
    for e in step_events:
        e.wait()

    workflows = _workflow_commands.list_queued_workflows(config)
    assert len(workflows) == queued_steps
    for workflow in workflows:
        assert workflow.status == WorkflowStatusString.PENDING.value
        assert workflow.queue_name == queue.name
        assert 0 <= workflow.input["args"][0] < 10
        assert workflow.output is None
        assert workflow.error is None
        assert "test_step" in workflow.workflowName

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
    workflows = _workflow_commands.list_queued_workflows(
        config, queue_name="not the name"
    )
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

    event.set()
    assert handle.get_result() == [0, 1, 2, 3, 4]
