import json
import time
import uuid
from typing import TypedDict

import sqlalchemy as sa

from dbos import DBOS, DBOSClient, EnqueueOptions, Queue
from dbos._dbos import WorkflowHandle
from dbos._schemas.system_database import SystemSchema
from dbos._utils import GlobalParams


class Person(TypedDict):
    first: str
    last: str
    age: int


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

    client.enqueue(options, 42, "test", johnDoe)
    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'


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

    client.enqueue(options, 42, "test", johnDoe)
    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'


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

    client.enqueue(options, 42, "test", johnDoe)
    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)

    with dbos._sys_db.engine.connect() as c:
        stmt = sa.select(
            SystemSchema.workflow_status.c.status,
            SystemSchema.workflow_status.c.application_version,
        ).where(SystemSchema.workflow_status.c.workflow_uuid == wfid)

        row = c.execute(stmt).fetchone()
        assert row is not None
        # assert row["status"] == "ENQUEUED"
        # assert row["app_version"] == "abcdef"
