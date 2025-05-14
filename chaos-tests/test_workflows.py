import uuid
from typing import Any

import sqlalchemy as sa

from dbos import DBOS, Queue, SetWorkflowID


def test_workflow(dbos: DBOS) -> None:

    @DBOS.step()
    def step_one(x: int) -> int:
        return x + 1

    @DBOS.step()
    def step_two(x: int) -> int:
        return x + 2

    @DBOS.transaction()
    def txn_one(x: int) -> int:
        DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return x + 3

    @DBOS.workflow()
    def workflow(x: int) -> int:
        x = step_one(x)
        x = step_two(x)
        x = txn_one(x)
        return x

    num_workflows = 5000

    for i in range(num_workflows):
        assert workflow(i) == i + 6


def test_recv(dbos: DBOS) -> None:

    topic = "test_topic"

    @DBOS.workflow()
    def recv_workflow() -> Any:
        return DBOS.recv(topic, timeout_seconds=10)

    num_workflows = 1000

    for i in range(num_workflows):
        handle = DBOS.start_workflow(recv_workflow)

        value = str(uuid.uuid4())
        DBOS.send(handle.workflow_id, value, topic)

        assert handle.get_result() == value


def test_events(dbos: DBOS) -> None:

    key = "test_key"

    @DBOS.workflow()
    def event_workflow() -> str:
        value = str(uuid.uuid4())
        DBOS.set_event(key, value)
        return value

    num_workflows = 5000

    for i in range(num_workflows):
        id = str(uuid.uuid4())
        with SetWorkflowID(id):
            value = event_workflow()
        assert DBOS.get_event(id, key, timeout_seconds=0) == value


def test_queues(dbos: DBOS) -> None:

    queue = Queue("test_queue")

    @DBOS.step()
    def step_one(x: int) -> int:
        return x + 1

    @DBOS.step()
    def step_two(x: int) -> int:
        return x + 2

    @DBOS.transaction()
    def txn_one(x: int) -> int:
        DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return x + 3

    @DBOS.workflow()
    def workflow(x: int) -> int:
        x = queue.enqueue(step_one, x).get_result()
        x = queue.enqueue(step_two, x).get_result()
        x = queue.enqueue(txn_one, x).get_result()
        return x

    num_workflows = 30

    for i in range(num_workflows):
        assert queue.enqueue(workflow, i).get_result() == i + 6
