import os
import time
import uuid

import requests

# Public API
from dbos import DBOS, SetWorkflowID


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

    # Test GET /dbos-perf
    response = requests.get("http://localhost:3001/dbos-perf", timeout=5)
    assert response.status_code == 200
    perf_util = response.json()
    assert perf_util is not None
    assert "idle" in perf_util
    assert "active" in perf_util
    assert "utilization" in perf_util

    # Test GET /dbos-perf again, should be a valid utilization between 0~1
    time.sleep(0.2)
    response = requests.get("http://localhost:3001/dbos-perf", timeout=5)
    assert response.status_code == 200
    perf_util = response.json()
    assert perf_util["utilization"] >= 0.0
    assert perf_util["utilization"] <= 1.0

    # Test GET not found
    response = requests.get("http://localhost:3001/stuff", timeout=5)
    assert response.status_code == 404

    # Test POST not found
    response = requests.post("http://localhost:3001/stuff", timeout=5)
    assert response.status_code == 404


def test_admin_recovery(dbos: DBOS) -> None:
    os.environ["DBOS__VMID"] = "testexecutor"
    os.environ["DBOS__APPVERSION"] = "testversion"
    os.environ["DBOS__APPID"] = "testappid"

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
    # Change the workflow status to pending
    dbos._sys_db.update_workflow_status(
        {
            "workflow_uuid": wfuuid,
            "status": "PENDING",
            "name": test_workflow.__qualname__,
            "class_name": None,
            "config_name": None,
            "output": None,
            "error": None,
            "executor_id": None,
            "app_id": None,
            "app_version": None,
            "request": None,
            "recovery_attempts": None,
            "authenticated_user": None,
            "authenticated_roles": None,
            "assumed_role": None,
            "queue_name": None,
        }
    )
    status = dbos.get_workflow_status(wfuuid)
    assert (
        status is not None and status.status == "PENDING"
    ), "Workflow status not updated"

    # Test POST /dbos-workflow-recovery
    data = ["testexecutor"]
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
