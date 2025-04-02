import json
import math
import os
import runpy
import subprocess
import sys
import time
import uuid
from typing import Any, Optional, TypedDict, cast

import sqlalchemy as sa

from dbos import DBOS, ConfigFile, DBOSClient, EnqueueOptions, Queue, SetWorkflowID
from dbos._dbos import WorkflowHandle
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import SystemDatabase
from dbos._utils import GlobalParams
from tests.client_collateral import retrieve_test, send_test


class Person(TypedDict):
    first: str
    last: str
    age: int


def run_client_collateral() -> None:
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, "client_collateral.py")
    runpy.run_path(filename)


def test_client_enqueue_appver_not_set(dbos: DBOS, client: DBOSClient) -> None:
    run_client_collateral()

    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "enqueue_test",
        "workflow_id": wfid,
    }

    client.enqueue(options, 42, "test", johnDoe)  # type: ignore

    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'

    wf_status = dbos.get_workflow_status(wfid)
    assert wf_status is not None
    assert wf_status.status == "SUCCESS"
    assert wf_status.name == "enqueue_test"
    assert wf_status.app_version == GlobalParams.app_version


def test_client_enqueue_appver_set(dbos: DBOS, client: DBOSClient) -> None:
    run_client_collateral()

    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "enqueue_test",
        "workflow_id": wfid,
        "app_version": GlobalParams.app_version,
    }

    client.enqueue(options, 42, "test", johnDoe)  # type: ignore

    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'

    wf_status = dbos.get_workflow_status(wfid)
    assert wf_status is not None
    assert wf_status.status == "SUCCESS"
    assert wf_status.name == "enqueue_test"
    assert wf_status.app_version == GlobalParams.app_version


def test_client_enqueue_wrong_appver(dbos: DBOS, client: DBOSClient) -> None:
    run_client_collateral()

    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "enqueue_test",
        "workflow_id": wfid,
        "app_version": "0123456789abcdef",
    }

    client.enqueue(options, 42, "test", johnDoe)  # type: ignore

    time.sleep(5)

    wf_status = dbos.get_workflow_status(wfid)
    assert wf_status is not None
    assert wf_status.status == "ENQUEUED"
    assert wf_status.name == "enqueue_test"
    assert wf_status.app_version == options["app_version"]


def test_client_enqueue_idempotent(
    config: ConfigFile, client: DBOSClient, sys_db: SystemDatabase
) -> None:
    DBOS.destroy(destroy_registry=True)

    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "enqueue_test",
        "workflow_id": wfid,
    }

    client.enqueue(options, 42, "test", johnDoe)  # type: ignore
    client.enqueue(options, 42, "test", johnDoe)  # type: ignore

    wf_status = sys_db.get_workflow_status(wfid)
    assert wf_status is not None
    assert wf_status["status"] == "ENQUEUED"
    assert wf_status["name"] == "enqueue_test"
    assert wf_status["app_version"] == None

    dbos = DBOS(config=config)
    DBOS.launch()
    run_client_collateral()

    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'

    DBOS.destroy(destroy_registry=True)


def test_client_send_with_topic(client: DBOSClient, dbos: DBOS) -> None:

    from tests.client_collateral import send_test

    run_client_collateral()

    now = time.time_ns()
    wfid = str(uuid.uuid4())
    topic = f"test-topic-{now}"
    message = f"Hello, DBOS! {now}"

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(send_test, topic)

    client.send(handle.get_workflow_id(), message, topic)

    result = handle.get_result()
    assert result == message


def test_client_send_no_topic(client: DBOSClient, dbos: DBOS) -> None:

    run_client_collateral()

    now = time.time_ns()
    wfid = str(uuid.uuid4())
    message = f"Hello, DBOS! {now}"

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(send_test)

    client.send(handle.get_workflow_id(), message)

    result = handle.get_result()
    assert result == message


def run_send_worker(wfid: str, topic: Optional[str], app_ver: str) -> None:
    script_path = os.path.join(os.path.dirname(__file__), "client_worker.py")
    args = [sys.executable, script_path, wfid]
    if topic is not None:
        args.append(topic)

    env = os.environ.copy()
    env["DBOS__APPVERSION"] = app_ver
    result = subprocess.run(args, env=env, capture_output=True, text=True)
    assert result.returncode == 0, f"Worker failed with error: {result.stderr}"
    DBOS.logger.info(result.stdout)


def test_client_send_idempotent(dbos: DBOS, client: DBOSClient) -> None:
    run_client_collateral()

    now = math.floor(time.time())
    wfid = f"test-send-{now}"
    topic = f"test-topic-{now}"
    message = f"Hello, DBOS! {now}"
    idempotency_key = f"test-idempotency-{now}"
    sendWFID = f"{wfid}-{idempotency_key}"

    run_send_worker(wfid, topic, GlobalParams.app_version)

    client.send(wfid, message, topic, idempotency_key)
    client.send(wfid, message, topic, idempotency_key)

    with dbos._sys_db.engine.connect() as conn:
        result = conn.execute(
            sa.text(
                "SELECT * FROM dbos.notifications WHERE destination_uuid = :wfid"
            ).bindparams(wfid=wfid)
        ).fetchall()
        assert len(result) == 1
        result = conn.execute(
            sa.text(
                "SELECT * FROM dbos.operation_outputs WHERE workflow_uuid = :wfid"
            ).bindparams(wfid=sendWFID)
        ).fetchall()
        assert len(result) == 1
        result = conn.execute(
            sa.text(
                "SELECT * FROM dbos.workflow_status WHERE workflow_uuid = :wfid"
            ).bindparams(wfid=sendWFID)
        ).fetchall()
        assert len(result) == 1

    DBOS.recover_pending_workflows()
    handle = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == message


def test_client_send_failure(dbos: DBOS, client: DBOSClient) -> None:
    run_client_collateral()

    now = math.floor(time.time())
    wfid = f"test-send-fail-{now}"
    topic = f"test-topic-{now}"
    message = f"Hello, DBOS! {now}"
    idempotency_key = f"test-idempotency-{now}"
    sendWFID = f"{wfid}-{idempotency_key}"

    run_send_worker(wfid, topic, GlobalParams.app_version)

    client.send(wfid, message, topic, idempotency_key)

    # simulate a send failure by deleting notification but leaving the WF status table result
    with dbos._sys_db.engine.connect() as conn:
        result = conn.execute(
            sa.text(
                "DELETE FROM dbos.operation_outputs WHERE workflow_uuid = :wfid"
            ).bindparams(wfid=sendWFID)
        )
        assert result.rowcount == 1
        result = conn.execute(
            sa.text(
                "DELETE FROM dbos.notifications WHERE destination_uuid = :wfid"
            ).bindparams(wfid=wfid)
        )
        assert result.rowcount == 1
        result = conn.execute(
            sa.text(
                "SELECT recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid = :wfid"
            ).bindparams(wfid=sendWFID)
        ).fetchall()
        assert len(result) == 1
        assert result[0][0] == 1

    client.send(wfid, message, topic, idempotency_key)

    with dbos._sys_db.engine.connect() as conn:
        result = conn.execute(
            sa.text(
                "SELECT recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid = :wfid"
            ).bindparams(wfid=sendWFID)
        ).fetchall()
        assert len(result) == 1
        assert result[0][0] == 2

    DBOS.recover_pending_workflows()
    handle = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == message


def test_client_retrieve_wf(client: DBOSClient, dbos: DBOS) -> None:
    run_client_collateral()

    message = f"Hello, DBOS! {time.time_ns()}"
    handle1 = DBOS.start_workflow(retrieve_test, message)

    handle2: WorkflowHandle[str] = client.retrieve_workflow(handle1.get_workflow_id())
    assert handle1.get_workflow_id() == handle2.get_workflow_id()
    status = handle2.get_status()
    assert status.status == "PENDING"

    result = handle2.get_result()
    assert result == message


def test_client_retrieve_wf_done(client: DBOSClient, dbos: DBOS) -> None:
    run_client_collateral()

    message = f"Hello, DBOS! {time.time_ns()}"
    handle1 = DBOS.start_workflow(retrieve_test, message)
    result1 = handle1.get_result()
    assert result1 == message

    handle2: WorkflowHandle[str] = client.retrieve_workflow(handle1.get_workflow_id())
    assert handle1.get_workflow_id() == handle2.get_workflow_id()
    result2 = handle2.get_result()
    assert result2 == message
