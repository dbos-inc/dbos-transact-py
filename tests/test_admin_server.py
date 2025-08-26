import os
import socket
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pytest
import requests
import sqlalchemy as sa
from requests.exceptions import ConnectionError

# Public API
from dbos import (
    DBOS,
    DBOSConfig,
    Queue,
    SetWorkflowID,
    WorkflowHandle,
    _workflow_commands,
)
from dbos._error import DBOSAwaitedWorkflowCancelledError
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import WorkflowStatusString
from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams


def test_admin_endpoints(dbos: DBOS) -> None:

    # Test GET /dbos-healthz
    response = requests.get("http://localhost:3001/dbos-healthz", timeout=5)
    assert response.status_code == 200
    assert response.text == "healthy"

    # Test POST /dbos-workflow-recovery
    data = ["executor1", "executor2"]
    response = requests.post(
        "http://localhost:3001/dbos-workflow-recovery", json=data, timeout=5
    )
    assert response.status_code == 200
    assert response.json() == []

    # Test GET /dbos-workflow-queues-metadata
    Queue("q1")
    Queue("q2", concurrency=1)
    Queue("q3", concurrency=1, worker_concurrency=1)
    Queue("q4", concurrency=1, worker_concurrency=1, limiter={"limit": 0, "period": 0})
    response = requests.get(
        "http://localhost:3001/dbos-workflow-queues-metadata", timeout=5
    )
    assert response.status_code == 200
    assert response.json() == [
        {"name": INTERNAL_QUEUE_NAME},
        {"name": "q1"},
        {"name": "q2", "concurrency": 1},
        {"name": "q3", "concurrency": 1, "workerConcurrency": 1},
        {
            "name": "q4",
            "concurrency": 1,
            "workerConcurrency": 1,
            "rateLimit": {"limit": 0, "period": 0},
        },
    ]

    # Test GET not found
    response = requests.get("http://localhost:3001/stuff", timeout=5)
    assert response.status_code == 404

    # Test POST not found
    response = requests.post("http://localhost:3001/stuff", timeout=5)
    assert response.status_code == 404

    response = requests.get("http://localhost:3001/deactivate", timeout=5)
    assert response.status_code == 200

    for event in dbos.poller_stop_events:
        assert event.is_set()


def test_deactivate(dbos: DBOS, config: DBOSConfig) -> None:
    wf_counter: int = 0

    queue = Queue("example-queue")

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    @DBOS.workflow()
    def regular_workflow() -> int:
        return 5

    # Let the scheduled workflow run
    time.sleep(5)
    val = wf_counter
    assert val > 0
    # Deactivate--scheduled workflow should stop
    response = requests.get("http://localhost:3001/deactivate", timeout=5)
    assert response.status_code == 200
    for event in dbos.poller_stop_events:
        assert event.is_set()
    # Verify the scheduled workflow does not run anymore
    time.sleep(5)
    assert wf_counter <= val + 2
    # Enqueue a workflow, verify it still runs
    assert queue.enqueue(regular_workflow).get_result() == 5

    # Test deferred event receivers
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def deferred_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    DBOS.launch()
    assert len(dbos.poller_stop_events) > 0
    for event in dbos.poller_stop_events:
        assert not event.is_set()
    # Deactivate--scheduled workflow should stop
    response = requests.get("http://localhost:3001/deactivate", timeout=5)
    assert response.status_code == 200
    for event in dbos.poller_stop_events:
        assert event.is_set()


def test_admin_recovery(config: DBOSConfig) -> None:
    os.environ["DBOS__VMID"] = "testexecutor"
    os.environ["DBOS__APPVERSION"] = "testversion"
    os.environ["DBOS__APPID"] = "testappid"
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()

    step_counter: int = 0
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_step(var2)
        return res + var

    @DBOS.step()
    def test_step(var2: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return var2 + "1"

    wfuuid = str(uuid.uuid4())
    DBOS.logger.info("Initiating: " + wfuuid)
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") == "bob1bob"

    # Manually update the database to pretend the workflow comes from another executor and is pending
    with dbos._sys_db.engine.begin() as c:
        query = (
            sa.update(SystemSchema.workflow_status)
            .values(
                status=WorkflowStatusString.PENDING.value, executor_id="other-executor"
            )
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        )
        c.execute(query)

    status = dbos.get_workflow_status(wfuuid)
    assert (
        status is not None and status.status == "PENDING"
    ), "Workflow status not updated"

    # Test POST /dbos-workflow-recovery
    data = ["other-executor"]
    response = requests.post(
        "http://localhost:3001/dbos-workflow-recovery", json=data, timeout=5
    )
    assert response.status_code == 200
    assert response.json() == [wfuuid]

    # Wait until the workflow is recovered
    max_retries = 10
    succeeded = False
    for attempt in range(max_retries):
        status = dbos.get_workflow_status(wfuuid)
        if status is not None and status.status == "SUCCESS":
            assert status.status == "SUCCESS"
            assert status.executor_id == "testexecutor"
            succeeded = True
            break
        else:
            time.sleep(1)
            print(f"Attempt {attempt + 1} failed. Retrying in 1 second...")
    assert succeeded, "Workflow did not recover"


def test_admin_diff_port(config: DBOSConfig, cleanup_test_databases: None) -> None:
    # Initialize singleton
    DBOS.destroy()  # In case of other tests leaving it

    try:
        # Initialize DBOS
        config["admin_port"] = 8001
        DBOS(config=config)
        DBOS.launch()

        # Test GET /dbos-healthz
        response = requests.get("http://localhost:8001/dbos-healthz", timeout=5)
        assert response.status_code == 200
        assert response.text == "healthy"
    finally:
        # Clean up after the test
        DBOS.destroy()


def test_disable_admin_server(config: DBOSConfig, cleanup_test_databases: None) -> None:
    # Initialize singleton
    DBOS.destroy()  # In case of other tests leaving it

    config["run_admin_server"] = False
    try:
        DBOS(config=config)
        DBOS.launch()

        with pytest.raises(ConnectionError):
            requests.get("http://localhost:3001/dbos-healthz", timeout=1)
    finally:
        # Clean up after the test
        DBOS.destroy()


def test_busy_admin_server_port_does_not_throw(config: DBOSConfig) -> None:
    # Initialize singleton
    DBOS.destroy()  # In case of other tests leaving it

    server_thread = None
    stop_event = threading.Event()
    try:

        def start_dummy_server(port: int, stop_event: threading.Event) -> None:
            """Starts a simple TCP server on the given port."""
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(("0.0.0.0", port))
            server_socket.listen(1)
            # We need to call accept for the port to be considered busy by the OS...
            while not stop_event.is_set():
                try:
                    server_socket.settimeout(
                        1
                    )  # Timeout for accept in case the thread is stopped.
                    client_socket, _ = server_socket.accept()
                    client_socket.close()
                except socket.timeout:
                    pass
            server_socket.close()

        port_to_block = 3001
        server_thread = threading.Thread(
            target=start_dummy_server, args=(port_to_block, stop_event)
        )
        server_thread.daemon = (
            True  # Allows the thread to be terminated when the main thread exits
        )
        server_thread.start()

        DBOS(config=config)
        DBOS.launch()
    finally:
        # Clean up after the test
        DBOS.destroy()
        if server_thread and server_thread.is_alive():
            stop_event.set()
            server_thread.join(2)
            if server_thread.is_alive():
                print("Warning: Server thread did not terminate gracefully.")


def test_admin_workflow_resume(dbos: DBOS) -> None:
    event = threading.Event()

    @DBOS.workflow()
    def blocking_workflow() -> None:
        event.wait()
        DBOS.sleep(0.1)

    # Run the workflow
    handle = DBOS.start_workflow(blocking_workflow)

    # Verify the workflow is pending
    output = DBOS.list_workflows()
    assert len(output) == 1
    assert output[0] != None
    wfid = output[0].workflow_id
    info = _workflow_commands.get_workflow(dbos._sys_db, wfid)
    assert info is not None
    assert info.status == "PENDING"

    # Cancel the workflow. Verify it was cancelled
    response = requests.post(
        f"http://localhost:3001/workflows/{wfid}/cancel", json=[], timeout=5
    )
    assert response.status_code == 204
    event.set()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()
    info = _workflow_commands.get_workflow(dbos._sys_db, wfid)
    assert info is not None
    assert info.status == "CANCELLED"

    # Manually update the database to pretend the workflow comes from another executor and is pending
    with dbos._sys_db.engine.begin() as c:
        query = (
            sa.update(SystemSchema.workflow_status)
            .values(
                status=WorkflowStatusString.PENDING.value, executor_id="other-executor"
            )
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
        )
        c.execute(query)

    # Resume the workflow. Verify that it succeeds again.
    response = requests.post(
        f"http://localhost:3001/workflows/{wfid}/resume", json=[], timeout=5
    )
    assert response.status_code == 204
    # Wait for the workflow to finish
    DBOS.retrieve_workflow(wfid).get_result()
    info = _workflow_commands.get_workflow(dbos._sys_db, wfid)
    assert info is not None
    assert info.status == "SUCCESS"
    assert info.executor_id == GlobalParams.executor_id


def test_admin_workflow_restart(dbos: DBOS) -> None:

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    time.sleep(1)

    # get the workflow list
    output = DBOS.list_workflows()
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    assert output[0] != None, "Expected output to be not None"

    wfUuid = output[0].workflow_id

    info = _workflow_commands.get_workflow(dbos._sys_db, wfUuid)
    assert info is not None, "Expected output to be not None"

    assert info.status == "SUCCESS", f"Expected status to be SUCCESS"

    response = requests.post(
        f"http://localhost:3001/workflows/{wfUuid}/restart", json=[], timeout=5
    )
    assert response.status_code == 200

    new_workflow_id = response.json().get("workflow_id")
    print(f"New workflow ID: {new_workflow_id}")
    assert new_workflow_id is not None, "Expected new workflow ID to be not None"

    worked = False
    count = 0
    while count < 5:
        # Check if the workflow is in the database
        info = _workflow_commands.get_workflow(dbos._sys_db, new_workflow_id)
        assert info is not None, "Expected output to be not None"
        if info.status == "SUCCESS":
            worked = True
            break
        time.sleep(1)
        count += 1

    assert worked, "Workflow did not finish successfully"


def test_admin_workflow_fork(dbos: DBOS) -> None:

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()

    # get the workflow list
    output = DBOS.list_workflows()
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    assert output[0] != None, "Expected output to be not None"

    wfUuid = output[0].workflow_id

    info = _workflow_commands.get_workflow(dbos._sys_db, wfUuid)
    assert info is not None, "Expected output to be not None"

    assert info.status == "SUCCESS", f"Expected status to be SUCCESS"

    response = requests.post(
        f"http://localhost:3001/workflows/{wfUuid}/fork", json={}, timeout=5
    )
    assert response.status_code == 200

    new_workflow_id = response.json().get("workflow_id")
    print(f"New workflow ID: {new_workflow_id}")
    assert new_workflow_id is not None, "Expected new workflow ID to be not None"
    assert new_workflow_id != wfUuid, "Expected new workflow ID to be different"

    output = DBOS.list_workflows()
    assert len(output) == 2, f"Expected list length to be 2, but got {len(output)}"

    worked = False
    count = 0
    while count < 5:
        # Check if the workflow is in the database
        info = _workflow_commands.get_workflow(dbos._sys_db, new_workflow_id)
        assert info is not None, "Expected output to be not None"
        if info.status == "SUCCESS":
            worked = True
            break
        time.sleep(1)
        count += 1

    # test for new_workflow_id and app version

    new_version = "my_new_version"
    GlobalParams.app_version = new_version

    response = requests.post(
        f"http://localhost:3001/workflows/{wfUuid}/fork",
        json={"new_workflow_id": "123456", "application_version": new_version},
        timeout=5,
    )
    assert response.status_code == 200

    new_workflow_id = response.json().get("workflow_id")
    assert new_workflow_id == "123456", "Expected new workflow ID is not 123456"

    handle: WorkflowHandle[None] = dbos.retrieve_workflow(new_workflow_id)
    assert (
        handle.get_status().app_version == new_version
    ), f"Expected application version to be {new_version}, but got {handle.get_status().app_version}"

    assert worked, "Workflow did not finish successfully"


def test_list_workflows(dbos: DBOS, skip_with_sqlite_imprecise_time: None) -> None:
    # Create workflows for testing
    @DBOS.workflow()
    def test_workflow_1() -> None:
        pass

    @DBOS.workflow()
    def test_workflow_2(my_time: datetime) -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id + " completed at " + my_time.isoformat()

    # Start workflows
    handle_1 = DBOS.start_workflow(test_workflow_1)
    time.sleep(2)  # Sleep for 2 seconds between workflows
    handle_2 = DBOS.start_workflow(test_workflow_2, datetime.now())

    # Wait for workflows to complete
    handle_1.get_result()
    handle_2.get_result()

    # List workflows and dynamically set the filter "name"
    workflows_list = DBOS.list_workflows()
    assert (
        len(workflows_list) >= 2
    ), f"Expected at least 2 workflows, but got {len(workflows_list)}"

    workflow_ids = [w.workflow_id for w in workflows_list]

    # Convert created_at to ISO 8601 format
    created_at_second_workflow = workflows_list[1].created_at
    assert (
        created_at_second_workflow is not None
    ), "created_at for the second workflow is None"
    start_time_filter = datetime.fromtimestamp(
        created_at_second_workflow / 1000, tz=timezone.utc
    ).isoformat()

    # Test POST /workflows with filters
    filters: Dict[str, Any] = {
        "workflow_uuids": workflow_ids,
        "start_time": start_time_filter,
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200

    workflows = response.json()
    assert len(workflows) == 1, f"Expected 1 workflows, but got {len(workflows)}"

    # Make sure it contains all the expected fields
    assert workflows[0]["WorkflowUUID"] == handle_2.workflow_id, "Workflow ID mismatch"
    assert workflows[0]["WorkflowName"] == test_workflow_2.__qualname__
    assert workflows[0]["Status"] == "SUCCESS"
    assert workflows[0]["WorkflowClassName"] is None
    assert workflows[0]["WorkflowConfigName"] is None
    assert workflows[0]["AuthenticatedUser"] is None
    assert workflows[0]["AssumedRole"] is None
    assert workflows[0]["AuthenticatedRoles"] is None
    # By default, input and output are not loaded
    assert workflows[0]["Input"] is None
    assert workflows[0]["Output"] is None
    assert workflows[0]["Error"] is None
    assert workflows[0]["CreatedAt"] is not None and len(workflows[0]["CreatedAt"]) > 0
    assert workflows[0]["UpdatedAt"] is not None and len(workflows[0]["UpdatedAt"]) > 0
    assert workflows[0]["QueueName"] is None
    assert workflows[0]["ApplicationVersion"] == GlobalParams.app_version
    assert workflows[0]["ExecutorID"] == GlobalParams.executor_id

    # Only load input and output as requested
    filters = {
        "workflow_uuids": workflow_ids,
        "start_time": start_time_filter,
        "load_input": True,
        "load_output": True,
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200

    workflows = response.json()
    assert len(workflows) == 1
    assert workflows[0]["Output"] is not None and len(workflows[0]["Output"]) > 0
    assert workflows[0]["Input"] is not None and len(workflows[0]["Input"]) > 0

    # Test POST /workflows without filters
    response = requests.post("http://localhost:3001/workflows", json={}, timeout=5)
    assert response.status_code == 200

    workflows = response.json()
    assert len(workflows) == len(
        workflows_list
    ), f"Expected {len(workflows_list)} workflows, but got {len(workflows)}"
    for workflow in workflows:
        assert workflow["WorkflowUUID"] in workflow_ids, "Workflow ID mismatch"

    # Verify sort_desc inverts the order
    filters = {
        "sort_desc": True,
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200
    workflows = response.json()
    assert len(workflows) == len(workflows_list)
    assert (
        workflows[0]["WorkflowUUID"] == handle_2.workflow_id
    ), "First workflow should be the last one started"

    # Test all filters
    filters = {
        "workflow_uuids": ["not-a-valid-uuid"],
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200
    workflows = response.json()
    assert len(workflows) == 0, "Expected no workflows for invalid UUID"

    filters = {
        "workflow_uuids": [handle_1.workflow_id, handle_2.workflow_id],
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200
    workflows = response.json()
    assert len(workflows) == 2

    filters = {
        "authenticated_user": "no-user",
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200
    workflows = response.json()
    assert len(workflows) == 0

    filters = {
        "workflow_name": test_workflow_1.__qualname__,
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200
    workflows = response.json()
    assert len(workflows) == 1
    assert workflows[0]["WorkflowUUID"] == handle_1.workflow_id

    filters = {
        "end_time": (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200
    workflows = response.json()
    assert len(workflows) == 0

    filters = {
        "start_time": (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(),
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200
    workflows = response.json()
    assert len(workflows) == 0

    filters = {
        "status": ["SUCCESS", "CANCELLED"],
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200
    workflows = response.json()
    assert len(workflows) == 2

    filters = {
        "application_version": GlobalParams.app_version,
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200
    workflows = response.json()
    assert len(workflows) == 2

    filters = {
        "limit": 1,
        "offset": 1,
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200
    workflows = response.json()
    assert len(workflows) == 1
    assert workflows[0]["WorkflowUUID"] == handle_2.workflow_id

    filters = {
        "workflow_id_prefix": handle_1.workflow_id[
            :10
        ],  # First 10 characters of the workflow name
    }
    response = requests.post("http://localhost:3001/workflows", json=filters, timeout=5)
    assert response.status_code == 200
    workflows = response.json()
    assert len(workflows) == 1
    assert workflows[0]["WorkflowUUID"] == handle_1.workflow_id


def test_get_workflow_by_id(dbos: DBOS) -> None:
    # Create workflows for testing
    @DBOS.workflow()
    def test_workflow_1() -> None:
        pass

    @DBOS.workflow()
    def test_workflow_2(my_time: datetime) -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id + " completed at " + my_time.isoformat()

    # Start workflows
    handle_1 = DBOS.start_workflow(test_workflow_1)
    handle_2 = DBOS.start_workflow(test_workflow_2, datetime.now())

    # Wait for workflows to complete
    handle_1.get_result()
    assert handle_2.get_result() is not None

    # Get the workflow ID of the second workflow
    workflow_id = handle_2.workflow_id

    # Test GET /workflows/:workflow_id for an existing workflow
    response = requests.get(f"http://localhost:3001/workflows/{workflow_id}", timeout=5)
    assert (
        response.status_code == 200
    ), f"Expected status code 200, but got {response.status_code}"

    workflow_data = response.json()
    assert workflow_data["WorkflowUUID"] == workflow_id, "Workflow ID mismatch"
    assert (
        workflow_data["Status"] == "SUCCESS"
    ), "Expected workflow status to be SUCCESS"
    assert workflow_data["WorkflowName"] == test_workflow_2.__qualname__
    assert workflow_data["WorkflowClassName"] is None
    assert workflow_data["WorkflowConfigName"] is None
    assert workflow_data["AuthenticatedUser"] is None
    assert workflow_data["AssumedRole"] is None
    assert workflow_data["AuthenticatedRoles"] is None
    assert workflow_data["Input"] is not None and len(workflow_data["Input"]) > 0
    assert workflow_data["Output"] is not None and len(workflow_data["Output"]) > 0
    assert workflow_data["Error"] is None
    assert (
        workflow_data["CreatedAt"] is not None and len(workflow_data["CreatedAt"]) > 0
    )
    assert (
        workflow_data["UpdatedAt"] is not None and len(workflow_data["UpdatedAt"]) > 0
    )
    assert workflow_data["QueueName"] is None
    assert workflow_data["ApplicationVersion"] == GlobalParams.app_version
    assert workflow_data["ExecutorID"] == GlobalParams.executor_id

    # Test GET /workflows/:workflow_id for a non-existing workflow
    non_existing_workflow_id = "non-existing-id"
    response = requests.get(
        f"http://localhost:3001/workflows/{non_existing_workflow_id}", timeout=5
    )
    assert (
        response.status_code == 404
    ), f"Expected status code 404, but got {response.status_code}"


def test_admin_garbage_collect(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    workflow()

    assert len(DBOS.list_workflows()) == 1

    response = requests.post(
        f"http://localhost:3001/dbos-garbage-collect",
        json={"cutoff_epoch_timestamp_ms": int(time.time() * 1000)},
        timeout=5,
    )
    response.raise_for_status()

    assert len(DBOS.list_workflows()) == 0


def test_admin_global_timeout(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow() -> None:
        while True:
            DBOS.sleep(0.1)

    handle = DBOS.start_workflow(workflow)
    time.sleep(1)
    cutoff_epoch_timestamp_ms = int(time.time() * 1000) - 1000
    response = requests.post(
        f"http://localhost:3001/dbos-global-timeout",
        json={"cutoff_epoch_timestamp_ms": cutoff_epoch_timestamp_ms},
        timeout=5,
    )
    response.raise_for_status()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()


def test_queued_workflows_endpoint(
    dbos: DBOS, skip_with_sqlite_imprecise_time: None
) -> None:
    """Test the /queues endpoint with various filters and scenarios."""

    # Set up a queue for testing
    test_queue1 = Queue("test-queue-1", concurrency=1)
    test_queue2 = Queue("test-queue-2", concurrency=1)

    @DBOS.workflow()
    def blocking_workflow(i: int) -> str:
        while True:
            time.sleep(0.1)

    # Enqueue some workflows to create queued entries
    handles: list[WorkflowHandle[str]] = []
    handles.append(test_queue1.enqueue(blocking_workflow, 1))
    handles.append(test_queue1.enqueue(blocking_workflow, 2))
    handles.append(test_queue2.enqueue(blocking_workflow, 3))

    # Test basic queued workflows endpoint
    response = requests.post("http://localhost:3001/queues", json={}, timeout=5)
    assert (
        response.status_code == 200
    ), f"Expected status 200, got {response.status_code}"

    queued_workflows = response.json()
    assert isinstance(queued_workflows, list), "Response should be a list"
    assert (
        len(queued_workflows) == 3
    ), f"Expected 3 queued workflows, got {len(queued_workflows)}"

    # Make sure it contains all the expected fields
    assert queued_workflows[0]["WorkflowName"] == blocking_workflow.__qualname__
    assert (
        queued_workflows[0]["WorkflowUUID"] == handles[0].workflow_id
    ), "Workflow ID mismatch"
    assert (
        queued_workflows[0]["Status"] == "ENQUEUED"
        or queued_workflows[0]["Status"] == "PENDING"
    )
    assert queued_workflows[0]["WorkflowClassName"] is None
    assert queued_workflows[0]["WorkflowConfigName"] is None
    assert queued_workflows[0]["AuthenticatedUser"] is None
    assert queued_workflows[0]["AssumedRole"] is None
    assert queued_workflows[0]["AuthenticatedRoles"] is None
    assert queued_workflows[0]["Input"] is None
    assert queued_workflows[0]["Output"] is None
    assert queued_workflows[0]["Error"] is None
    assert (
        queued_workflows[0]["CreatedAt"] is not None
        and len(queued_workflows[0]["CreatedAt"]) > 0
    )
    assert (
        queued_workflows[0]["UpdatedAt"] is not None
        and len(queued_workflows[0]["UpdatedAt"]) > 0
    )
    assert queued_workflows[0]["QueueName"] == test_queue1.name
    assert queued_workflows[0]["ApplicationVersion"] == GlobalParams.app_version
    assert queued_workflows[0]["ExecutorID"] == GlobalParams.executor_id

    # Verify sort_desc inverts the order
    filters: Dict[str, Any] = {
        "sort_desc": True,
    }
    response = requests.post("http://localhost:3001/queues", json=filters, timeout=5)
    assert response.status_code == 200
    filtered_workflows = response.json()
    assert len(filtered_workflows) == len(handles)
    assert (
        filtered_workflows[0]["WorkflowUUID"] == handles[2].workflow_id
    ), "First workflow should be the last one enqueued"

    # Only load input as requested
    filters = {
        "load_input": True,
    }
    response = requests.post("http://localhost:3001/queues", json=filters, timeout=5)
    assert response.status_code == 200
    filtered_workflows = response.json()
    assert len(filtered_workflows) == len(handles)
    assert filtered_workflows[0]["Input"] is not None
    assert "1" in filtered_workflows[0]["Input"]

    # Test all filters
    filters = {
        "workflow_name": blocking_workflow.__qualname__,
    }
    response = requests.post("http://localhost:3001/queues", json=filters, timeout=5)
    assert response.status_code == 200
    filtered_workflows = response.json()
    assert len(filtered_workflows) == len(handles)

    filters = {
        "end_time": (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
    }
    response = requests.post("http://localhost:3001/queues", json=filters, timeout=5)
    assert response.status_code == 200
    filtered_workflows = response.json()
    assert len(filtered_workflows) == 0

    filters = {
        "start_time": (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(),
    }
    response = requests.post("http://localhost:3001/queues", json=filters, timeout=5)
    assert response.status_code == 200
    filtered_workflows = response.json()
    assert len(filtered_workflows) == 0

    filters = {
        "status": ["PENDING", "ENQUEUED"],
    }
    response = requests.post("http://localhost:3001/queues", json=filters, timeout=5)
    assert response.status_code == 200
    filtered_workflows = response.json()
    assert len(filtered_workflows) == len(handles)

    filters = {
        "queue_name": test_queue1.name,
    }
    response = requests.post("http://localhost:3001/queues", json=filters, timeout=5)
    assert response.status_code == 200
    filtered_workflows = response.json()
    assert len(filtered_workflows) == 2

    filters = {"queue_name": test_queue1.name, "limit": 1, "offset": 1}
    response = requests.post("http://localhost:3001/queues", json=filters, timeout=5)
    assert response.status_code == 200
    filtered_workflows = response.json()
    assert isinstance(filtered_workflows, list), "Response should be a list"
    assert (
        len(filtered_workflows) == 1
    ), f"Expected 1 workflow, got {len(filtered_workflows)}"
    assert filtered_workflows[0]["WorkflowUUID"] == handles[1].workflow_id

    # Test with non-existent queue name
    filters = {"queue_name": "non-existent-queue"}
    response = requests.post("http://localhost:3001/queues", json=filters, timeout=5)
    assert response.status_code == 200

    empty_result = response.json()
    assert isinstance(
        empty_result, list
    ), "Response should be a list even for non-existent queue"
    assert len(empty_result) == 0, "Expected no workflows for non-existent queue"
