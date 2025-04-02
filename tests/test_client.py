import json
import os
import subprocess
import sys
import time
import uuid
from typing import Optional, TypedDict, cast

import sqlalchemy as sa

from dbos import DBOS, ConfigFile, DBOSClient, EnqueueOptions, Queue, SetWorkflowID
from dbos._dbos import WorkflowHandle
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import SystemDatabase
from dbos._utils import GlobalParams


class Person(TypedDict):
    first: str
    last: str
    age: int


def get_wf_status(
    sys_db: SystemDatabase, wfid: str
) -> Optional[sa.Row[tuple[str, str]]]:
    with sys_db.engine.connect() as c:
        stmt = sa.select(
            SystemSchema.workflow_status.c.status,
            SystemSchema.workflow_status.c.application_version,
        ).where(SystemSchema.workflow_status.c.workflow_uuid == wfid)

        return c.execute(stmt).fetchone()


def test_client_enqueue_appver_not_set(dbos: DBOS, client: DBOSClient) -> None:

    @DBOS.workflow()
    def enqueue_test(numVal: int, strVal: str, person: Person) -> str:
        return f"{numVal}-{strVal}-{json.dumps(person)}"

    queue = Queue("test_queue")

    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "test_client_enqueue_appver_not_set.<locals>.enqueue_test",
        "workflow_id": wfid,
    }

    client.enqueue(options, 42, "test", johnDoe)  # type: ignore

    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'

    wf_status = get_wf_status(dbos._sys_db, wfid)
    assert wf_status is not None
    status, app_version = wf_status
    assert status == "SUCCESS"
    assert app_version == GlobalParams.app_version


def test_client_enqueue_appver_set(dbos: DBOS, client: DBOSClient) -> None:
    @DBOS.workflow()
    def enqueue_test(numVal: int, strVal: str, person: Person) -> str:
        return f"{numVal}-{strVal}-{json.dumps(person)}"

    queue = Queue("test_queue")

    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "test_client_enqueue_appver_set.<locals>.enqueue_test",
        "workflow_id": wfid,
        "app_version": GlobalParams.app_version,
    }

    client.enqueue(options, 42, "test", johnDoe)  # type: ignore

    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'

    wf_status = get_wf_status(dbos._sys_db, wfid)
    assert wf_status is not None
    status, app_version = wf_status
    assert status == "SUCCESS"
    assert app_version == GlobalParams.app_version


def test_client_enqueue_wrong_appver(dbos: DBOS, client: DBOSClient) -> None:

    @DBOS.workflow()
    def enqueue_test(numVal: int, strVal: str, person: Person) -> str:
        return f"{numVal}-{strVal}-{json.dumps(person)}"

    queue = Queue("test_queue")

    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "test_client_enqueue_wrong_appver.<locals>.enqueue_test",
        "workflow_id": wfid,
        "app_version": "abcdef",
    }

    client.enqueue(options, 42, "test", johnDoe)  # type: ignore

    time.sleep(5)

    wf_status = get_wf_status(dbos._sys_db, wfid)
    assert wf_status is not None
    status, app_version = wf_status
    assert status == "ENQUEUED"
    assert app_version == "abcdef"


def test_client_enqueue_idempotent(dbos: DBOS, client: DBOSClient) -> None:

    @DBOS.workflow()
    def enqueue_test(numVal: int, strVal: str, person: Person) -> str:
        return f"{numVal}-{strVal}-{json.dumps(person)}"

    queue = Queue("test_queue")

    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    options: EnqueueOptions = {
        "queue_name": "test_queue",
        "workflow_name": "test_client_enqueue_idempotent.<locals>.enqueue_test",
        "workflow_id": wfid,
    }

    client.enqueue(options, 42, "test", johnDoe)  # type: ignore
    client.enqueue(options, 42, "test", johnDoe)  # type: ignore

    wf_status = get_wf_status(dbos._sys_db, wfid)
    assert wf_status is not None
    status, app_version = wf_status
    assert status == "ENQUEUED"
    assert app_version == None

    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'


def test_client_send_with_topic(client: DBOSClient, dbos: DBOS) -> None:

    @DBOS.workflow()
    def sendTest(topic: Optional[str] = None) -> str:
        return cast(str, DBOS.recv(topic, 60))

    now = time.time_ns()
    wfid = f"{now}-test_client_send_with_topic"
    topic = f"test-topic-{now}"
    message = f"Hello, DBOS! {now}"

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(sendTest, topic)

    client.send(handle.get_workflow_id(), message, topic)

    result = handle.get_result()
    assert result == message


def test_client_send_no_topic(client: DBOSClient, dbos: DBOS) -> None:

    @DBOS.workflow()
    def sendTest(topic: Optional[str] = None) -> str:
        return cast(str, DBOS.recv(topic, 60))

    now = time.time_ns()
    wfid = f"{now}-test_client_send_with_topic"
    message = f"Hello, DBOS! {now}"

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(sendTest)

    client.send(handle.get_workflow_id(), message)

    result = handle.get_result()
    assert result == message


def test_client_send_idempotent(client: DBOSClient, dbos: DBOS) -> None:

    @DBOS.workflow()
    def sendTest(topic: Optional[str] = None) -> str:
        return cast(str, DBOS.recv(topic, 60))

    now = time.time_ns()
    wfid = f"{now}-test_client_send_with_topic"
    message = f"Hello, DBOS! {now}"

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(sendTest)

    client.send(handle.get_workflow_id(), message)

    result = handle.get_result()
    assert result == message


def test_client_retrieve_wf(client: DBOSClient, dbos: DBOS) -> None:

    @DBOS.workflow()
    def retrieve_test(value: str) -> str:
        DBOS.sleep(5)
        return value

    handle1 = DBOS.start_workflow(retrieve_test, "Hello, DBOS!")

    handle2: WorkflowHandle[str] = client.retrieve_workflow(handle1.get_workflow_id())
    assert handle1.get_workflow_id() == handle2.get_workflow_id()
    status = handle2.get_status()
    assert status.status == "PENDING"

    result = handle2.get_result()
    assert result == "Hello, DBOS!"


def test_client_retrieve_wf_done(client: DBOSClient, dbos: DBOS) -> None:

    @DBOS.workflow()
    def retrieve_test(value: str) -> str:
        return value

    handle1 = DBOS.start_workflow(retrieve_test, "Hello, DBOS!")
    result1 = handle1.get_result()
    assert result1 == "Hello, DBOS!"

    handle2: WorkflowHandle[str] = client.retrieve_workflow(handle1.get_workflow_id())
    assert handle1.get_workflow_id() == handle2.get_workflow_id()
    result2 = handle2.get_result()
    assert result2 == "Hello, DBOS!"
