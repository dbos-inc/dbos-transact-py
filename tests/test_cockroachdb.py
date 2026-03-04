import os
from urllib.parse import urlparse, urlunparse

import pytest
from sqlalchemy import create_engine, text

from dbos import DBOS, DBOSConfig, Queue


def test_cockroachdb() -> None:
    database_url = os.environ.get("DBOS_COCKROACHDB_URL")
    if database_url is None:
        pytest.skip("No CockroachDB database URL provided")

    # Drop and recreate the dbos_test database using the provided URL
    default_engine = create_engine(database_url, isolation_level="AUTOCOMMIT")
    with default_engine.connect() as conn:
        conn.execute(text("DROP DATABASE IF EXISTS dbos_test CASCADE"))
        conn.execute(text("CREATE DATABASE dbos_test"))
    default_engine.dispose()

    # Use the dbos_test database for the test
    parsed = urlparse(database_url)
    test_url = urlunparse(parsed._replace(path="/dbos_test"))

    key = "key"
    value = "value"

    @DBOS.workflow()
    def workflow() -> str:
        DBOS.set_event(key, value)
        message: str = DBOS.recv()
        return message

    queue = Queue("queue")

    try:
        engine = create_engine(test_url)
        config: DBOSConfig = {
            "name": "cockroachdb-test",
            "system_database_url": test_url,
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
