import importlib
import math
import os
import runpy
import subprocess
import sys
import time
import uuid
from typing import Optional, TypedDict

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

from dbos import DBOS, DBOSClient, DBOSConfig, EnqueueOptions, SetWorkflowID
from dbos._dbos import WorkflowHandle, WorkflowHandleAsync
from dbos._sys_db import SystemDatabase
from dbos._utils import GlobalParams
from tests import client_collateral
from tests.client_collateral import event_test, retrieve_test, send_test


class Person(TypedDict):
    first: str
    last: str
    age: int


def run_client_collateral() -> None:
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, "client_collateral.py")
    runpy.run_path(filename)


def test_client_no_migrate(
    dbos: DBOS, config: DBOSConfig, skip_with_sqlite: None
) -> None:
    # Drop the system database
    DBOS.destroy()
    DBOS(config=config)
    DBOS.reset_system_database()

    # The client should not be able to connect to the system database
    with pytest.raises(Exception) as exc_info:
        assert config["application_database_url"]
        client = DBOSClient(config["application_database_url"])
        client.list_workflows()
    assert f'database "dbostestpy_dbos_sys" does not exist' in str(exc_info.value)


def test_client_enqueue_and_get_result(dbos: DBOS, client: DBOSClient) -> None:
    run_client_collateral()

    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "enqueue_test",
        "workflow_id": wfid,
    }

    handle: WorkflowHandle[str] = client.enqueue(options, 42, "test", johnDoe)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'

    list_results = client.list_workflows()
    assert len(list_results) == 1
    assert list_results[0].workflow_id == wfid
    assert list_results[0].status == "SUCCESS"
    assert list_results[0].output == result
    assert list_results[0].input is not None

    # Skip loading input and output
    list_results = client.list_workflows(load_input=False, load_output=False)
    assert len(list_results) == 1
    assert list_results[0].workflow_id == wfid
    assert list_results[0].status == "SUCCESS"
    assert list_results[0].output is None
    assert list_results[0].input is None


def test_enqueue_with_timeout(dbos: DBOS, client: DBOSClient) -> None:
    run_client_collateral()

    wfid = str(uuid.uuid4())
    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "blocked_workflow",
        "workflow_timeout": 1,
        "workflow_id": wfid,
    }

    handle: WorkflowHandle[str] = client.enqueue(options)

    list_results = client.list_queued_workflows()
    assert len(list_results) == 1
    assert list_results[0].workflow_id == wfid
    assert list_results[0].status in ["PENDING", "ENQUEUED"]

    with pytest.raises(Exception) as exc_info:
        handle.get_result()
    assert "was cancelled" in str(exc_info.value)


def test_client_enqueue_appver_not_set(dbos: DBOS, client: DBOSClient) -> None:
    run_client_collateral()

    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "enqueue_test",
        "workflow_id": wfid,
    }

    client.enqueue(options, 42, "test", johnDoe)

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

    client.enqueue(options, 42, "test", johnDoe)

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

    client.enqueue(options, 42, "test", johnDoe)

    time.sleep(5)

    wf_status = dbos.get_workflow_status(wfid)
    assert wf_status is not None
    assert wf_status.status == "ENQUEUED"
    assert wf_status.name == "enqueue_test"
    assert wf_status.app_version == options["app_version"]


def test_client_enqueue_idempotent(config: DBOSConfig, client: DBOSClient) -> None:
    DBOS.destroy(destroy_registry=True)

    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "enqueue_test",
        "workflow_id": wfid,
    }

    client.enqueue(options, 42, "test", johnDoe)
    client.enqueue(options, 42, "test", johnDoe)

    DBOS(config=config)
    DBOS.launch()
    run_client_collateral()

    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'

    wf_status = DBOS.retrieve_workflow(handle.get_workflow_id()).get_status()
    assert wf_status is not None
    assert wf_status.status == "SUCCESS"
    assert wf_status.name == "enqueue_test"
    assert wf_status.app_version == GlobalParams.app_version

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


def test_client_send_idempotent(
    dbos: DBOS, client: DBOSClient, skip_with_sqlite: None
) -> None:
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
        nresult = conn.execute(
            sa.text(
                "SELECT * FROM dbos.notifications WHERE destination_uuid = :wfid"
            ).bindparams(wfid=wfid)
        ).fetchall()
        assert len(nresult) == 1
        oresult = conn.execute(
            sa.text(
                "SELECT * FROM dbos.operation_outputs WHERE workflow_uuid = :wfid"
            ).bindparams(wfid=sendWFID)
        ).fetchall()
        assert len(oresult) == 1
        sresult = conn.execute(
            sa.text(
                "SELECT * FROM dbos.workflow_status WHERE workflow_uuid = :wfid"
            ).bindparams(wfid=sendWFID)
        ).fetchall()
        assert len(sresult) == 1

    DBOS._recover_pending_workflows()
    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result2 = handle.get_result()
    assert result2 == message


def test_client_send_failure(
    dbos: DBOS, client: DBOSClient, skip_with_sqlite: None
) -> None:
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
        oresult = conn.execute(
            sa.text(
                "DELETE FROM dbos.operation_outputs WHERE workflow_uuid = :wfid"
            ).bindparams(wfid=sendWFID)
        )
        assert oresult.rowcount == 1
        nresult = conn.execute(
            sa.text(
                "DELETE FROM dbos.notifications WHERE destination_uuid = :wfid"
            ).bindparams(wfid=wfid)
        )
        assert nresult.rowcount == 1
        sresult = conn.execute(
            sa.text(
                "SELECT recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid = :wfid"
            ).bindparams(wfid=sendWFID)
        ).fetchall()
        assert len(sresult) == 1
        assert sresult[0][0] == 1

    client.send(wfid, message, topic, idempotency_key)

    with dbos._sys_db.engine.connect() as conn:
        s2result = conn.execute(
            sa.text(
                "SELECT recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid = :wfid"
            ).bindparams(wfid=sendWFID)
        ).fetchall()
        assert len(s2result) == 1
        assert s2result[0][0] == 2

    DBOS._recover_pending_workflows()
    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == message


def test_client_get_event(client: DBOSClient, dbos: DBOS) -> None:
    run_client_collateral()

    now = math.floor(time.time())
    wfid = f"test-client-event-{now}"
    key = f"key-{now}"
    value = f"value-{now}"

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(event_test, key, value, None)

    client_value = client.get_event(wfid, key, 10)
    assert client_value == value
    result = handle.get_result()
    assert result == f"{key}-{value}"


def test_client_get_event_finished(client: DBOSClient, dbos: DBOS) -> None:
    run_client_collateral()

    now = math.floor(time.time())
    wfid = f"test-client-event-{now}"
    key = f"key-{now}"
    value = f"value-{now}"

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(event_test, key, value, None)
        result = handle.get_result()
        assert result == f"{key}-{value}"

    client_value = client.get_event(wfid, key, 10)
    assert client_value == value


def test_client_get_event_update(client: DBOSClient, dbos: DBOS) -> None:
    run_client_collateral()

    now = math.floor(time.time())
    wfid = f"test-client-event-{now}"
    key = f"key-{now}"
    value = f"value-{now}"

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(event_test, key, value, 10)

    client_value = client.get_event(wfid, key, 10)
    assert client_value == value
    result = handle.get_result()
    assert result == f"{key}-{value}"
    client_value = client.get_event(wfid, key, 10)
    assert client_value == f"updated-{value}"


def test_client_get_event_update_finished(client: DBOSClient, dbos: DBOS) -> None:
    run_client_collateral()

    now = math.floor(time.time())
    wfid = f"test-client-event-{now}"
    key = f"key-{now}"
    value = f"value-{now}"

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(event_test, key, value, 10)
        result = handle.get_result()
        assert result == f"{key}-{value}"

    client_value = client.get_event(wfid, key, 10)
    assert client_value == f"updated-{value}"


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


def test_client_fork(dbos: DBOS, client: DBOSClient) -> None:
    run_client_collateral()

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "fork_test",
    }

    input = 5
    handle: WorkflowHandle[int] = client.enqueue(options, input)
    assert handle.get_result() == input * 2
    assert len(client.list_workflow_steps(handle.workflow_id)) == 2

    fork_id = str(uuid.uuid4())
    with SetWorkflowID(fork_id):
        forked_handle: WorkflowHandle[int] = client.fork_workflow(handle.workflow_id, 1)
    assert forked_handle.workflow_id == fork_id
    assert forked_handle.get_result() == input * 2

    forked_handle = client.fork_workflow(handle.workflow_id, 2)
    assert (
        forked_handle.workflow_id != handle.workflow_id
        and forked_handle.workflow_id != fork_id
    )
    assert forked_handle.get_result() == input * 2

    assert len(client.list_workflows()) == 3


@pytest.mark.asyncio
async def test_client_fork_async(dbos: DBOS, client: DBOSClient) -> None:
    run_client_collateral()

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "fork_test",
    }

    input = 5
    handle: WorkflowHandleAsync[int] = await client.enqueue_async(options, input)
    assert await handle.get_result() == input * 2
    assert len(await client.list_workflow_steps_async(handle.workflow_id)) == 2

    forked_handle: WorkflowHandleAsync[int] = await client.fork_workflow_async(
        handle.workflow_id, 1
    )
    assert forked_handle.workflow_id != handle.workflow_id
    assert await forked_handle.get_result() == input * 2

    forked_handle = await client.fork_workflow_async(handle.workflow_id, 2)
    assert forked_handle.workflow_id != handle.workflow_id
    assert await forked_handle.get_result() == input * 2

    assert len(await client.list_workflows_async()) == 3


def test_enqueue_with_deduplication(dbos: DBOS, client: DBOSClient) -> None:
    run_client_collateral()

    wfid = str(uuid.uuid4())
    dedup_id = f"dedup-{wfid}"
    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "retrieve_test",
        "workflow_id": wfid,
        "deduplication_id": dedup_id,
    }

    handle: WorkflowHandle[str] = client.enqueue(options, "abc")
    # Enqueue itself again should not raise an error
    handle2: WorkflowHandle[str] = client.enqueue(options, "def")

    # Enqueue with the same deduplication ID but different workflow ID should raise an error
    wfid2 = str(uuid.uuid4())
    options["workflow_id"] = wfid2
    with pytest.raises(Exception) as exc_info:
        client.enqueue(options, "def")
    assert (
        f"Workflow {wfid2} was deduplicated due to an existing workflow in queue test_queue with deduplication ID {dedup_id}."
        in str(exc_info.value)
    )

    list_results = client.list_queued_workflows()
    assert len(list_results) == 1
    assert list_results[0].workflow_id == wfid
    assert list_results[0].status in ["PENDING", "ENQUEUED"]
    assert list_results[0].input is not None
    assert list_results[0].output is None

    # Skip loading input
    list_results = client.list_queued_workflows(load_input=False)
    assert len(list_results) == 1
    assert list_results[0].workflow_id == wfid
    assert list_results[0].status in ["PENDING", "ENQUEUED"]
    assert list_results[0].input is None
    assert list_results[0].output is None

    assert handle.get_result() == "abc"
    assert handle2.get_result() == "abc"


def test_enqueue_with_priority(dbos: DBOS, client: DBOSClient) -> None:
    importlib.reload(client_collateral)
    from tests.client_collateral import inorder_results

    options: EnqueueOptions = {
        "queue_name": "inorder_queue",
        "workflow_name": "retrieve_test",
        "priority": -1,
    }

    with pytest.raises(Exception) as exc_info:
        client.enqueue(options, "abc")
    assert "Invalid priority" in str(exc_info.value)

    # Without priority should work
    del options["priority"]
    handle: WorkflowHandle[str] = client.enqueue(options, "abc")

    # Enqueue with a lower priority
    options["priority"] = 5
    handle2: WorkflowHandle[str] = client.enqueue(options, "def")

    # Enqueue with a higher priority
    options["priority"] = 1
    handle3: WorkflowHandle[str] = client.enqueue(options, "ghi")

    assert handle.get_result() == "abc"
    assert handle3.get_result() == "ghi"
    assert handle2.get_result() == "def"

    # Should be in the order of priority
    assert inorder_results == ["abc", "ghi", "def"]


def test_client_bad_url() -> None:
    with pytest.raises(DBAPIError) as exc_info:
        DBOSClient("postgresql://postgres:fakepassword@localhost:5433/fake_database")
