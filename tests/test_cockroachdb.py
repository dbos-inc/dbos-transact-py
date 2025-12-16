import os

import pytest
from sqlalchemy import create_engine

from dbos import DBOS, DBOSConfig, Queue


def test_cockroachdb() -> None:
    database_url = os.environ.get("DBOS_COCKROACHDB_URL")
    if database_url is None:
        pytest.skip("No CockroachDB database URL provided")

    key = "key"
    value = "value"

    @DBOS.workflow()
    def workflow() -> str:
        DBOS.set_event(key, value)
        message: str = DBOS.recv()
        return message

    queue = Queue("queue")

    try:
        engine = create_engine(database_url)
        config: DBOSConfig = {
            "name": "cockroachdb-test",
            "system_database_url": database_url,
            "use_listen_notify": False,
            "system_database_engine": engine,
        }
        DBOS(config=config)
        DBOS.launch()
        handle = queue.enqueue(workflow)
        assert DBOS.get_event(handle.workflow_id, key) == value
        DBOS.send(handle.workflow_id, value)
        assert handle.get_result() == value
    finally:
        DBOS.destroy(destroy_registry=True)
