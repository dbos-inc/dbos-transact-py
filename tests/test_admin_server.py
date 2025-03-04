import os
import threading
import time
import uuid

import requests
import sqlalchemy as sa

# Public API
from dbos import DBOS, ConfigFile, Queue, SetWorkflowID, _workflow_commands
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import SystemDatabase, WorkflowStatusString
from dbos._utils import GlobalParams


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

    for event in dbos.stop_events:
        assert event.is_set(), "Event is not set!"


def test_admin_recovery(config: ConfigFile) -> None:
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
        DBOS.logger.info("WFID: " + DBOS.workflow_id)
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

    dbos._sys_db.wait_for_buffer_flush()

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


def test_admin_diff_port(cleanup_test_databases: None) -> None:
    # Initialize singleton
    DBOS.destroy()  # In case of other tests leaving it

    config_string = """name: test-app
language: python
database:
  hostname: localhost
  port: 5432
  username: postgres
  password: ${PGPASSWORD}
  app_db_name: dbostestpy
runtimeConfig:
  start:
    - python3 main.py
  admin_port: 8001
"""
    # Write the config to a text file for the moment
    with open("dbos-config.yaml", "w") as file:
        file.write(config_string)

    try:
        # Initialize DBOS
        DBOS()
        DBOS.launch()

        # Test GET /dbos-healthz
        response = requests.get("http://localhost:8001/dbos-healthz", timeout=5)
        assert response.status_code == 200
        assert response.text == "healthy"
    finally:
        # Clean up after the test
        DBOS.destroy()
        os.remove("dbos-config.yaml")


def test_admin_workflow_resume(dbos: DBOS, sys_db: SystemDatabase) -> None:
    counter: int = 0
    event = threading.Event()

    @DBOS.workflow()
    def simple_workflow() -> None:
        event.set()
        nonlocal counter
        counter += 1

    # Run the workflow and flush its results
    simple_workflow()
    assert counter == 1
    dbos._sys_db.wait_for_buffer_flush()

    # Verify the workflow has succeeded
    output = _workflow_commands.list_workflows(sys_db)
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"
    assert output[0] != None, "Expected output to be not None"
    wfUuid = output[0].workflow_id
    info = _workflow_commands.get_workflow(sys_db, wfUuid, True)
    assert info is not None, "Expected output to be not None"
    assert info.status == "SUCCESS", f"Expected status to be SUCCESS"

    # Cancel the workflow. Verify it was cancelled
    response = requests.post(
        f"http://localhost:3001/workflows/{wfUuid}/cancel", json=[], timeout=5
    )
    assert response.status_code == 204
    info = _workflow_commands.get_workflow(sys_db, wfUuid, True)
    assert info is not None
    assert info.status == "CANCELLED", f"Expected status to be CANCELLED"

    # Manually update the database to pretend the workflow comes from another executor and is pending
    with dbos._sys_db.engine.begin() as c:
        query = (
            sa.update(SystemSchema.workflow_status)
            .values(
                status=WorkflowStatusString.PENDING.value, executor_id="other-executor"
            )
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfUuid)
        )
        c.execute(query)

    # Resume the workflow. Verify that it succeeds again.
    event.clear()
    response = requests.post(
        f"http://localhost:3001/workflows/{wfUuid}/resume", json=[], timeout=5
    )
    assert response.status_code == 204
    assert event.wait(timeout=5)
    dbos._sys_db.wait_for_buffer_flush()
    assert counter == 2
    info = _workflow_commands.get_workflow(sys_db, wfUuid, True)
    assert info is not None
    assert info.status == "SUCCESS", f"Expected status to be SUCCESS"
    assert info.executor_id == GlobalParams.executor_id

    # Resume the workflow. Verify it does not run and status remains SUCCESS
    response = requests.post(
        f"http://localhost:3001/workflows/{wfUuid}/resume", json=[], timeout=5
    )
    assert response.status_code == 204
    dbos._sys_db.wait_for_buffer_flush()
    info = _workflow_commands.get_workflow(sys_db, wfUuid, True)
    assert info is not None
    assert info.status == "SUCCESS", f"Expected status to be SUCCESS"
    assert counter == 2


def test_admin_workflow_restart(dbos: DBOS, sys_db: SystemDatabase) -> None:

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    time.sleep(1)

    # get the workflow list
    output = _workflow_commands.list_workflows(sys_db)
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    assert output[0] != None, "Expected output to be not None"

    wfUuid = output[0].workflow_id

    info = _workflow_commands.get_workflow(sys_db, wfUuid, True)
    assert info is not None, "Expected output to be not None"

    assert info.status == "SUCCESS", f"Expected status to be SUCCESS"

    response = requests.post(
        f"http://localhost:3001/workflows/{wfUuid}/cancel", json=[], timeout=5
    )
    assert response.status_code == 204

    info = _workflow_commands.get_workflow(sys_db, wfUuid, True)
    if info is not None:
        assert info.status == "CANCELLED", f"Expected status to be CANCELLED"
    else:
        assert False, "Expected info to be not None"

    response = requests.post(
        f"http://localhost:3001/workflows/{wfUuid}/restart", json=[], timeout=5
    )
    assert response.status_code == 204

    time.sleep(1)

    info = _workflow_commands.get_workflow(sys_db, wfUuid, True)
    if info is not None:
        assert info.status == "CANCELLED", f"Expected status to be CANCELLED"
    else:
        assert False, "Expected info to be not None"

    output = _workflow_commands.list_workflows(sys_db)
    assert len(output) == 2, f"Expected list length to be 2, but got {len(output)}"

    if output[0].workflow_id == wfUuid:
        new_wfUuid = output[1].workflow_id
    else:
        new_wfUuid = output[0].workflow_id

    info = _workflow_commands.get_workflow(sys_db, new_wfUuid, True)
    if info is not None:
        assert info.status == "SUCCESS", f"Expected status to be SUCCESS"
    else:
        assert False, "Expected info to be not None"
