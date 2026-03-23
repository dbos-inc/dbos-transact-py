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


def test_cockroachdb_fork() -> None:
    database_url = os.environ.get("DBOS_COCKROACHDB_URL")
    if database_url is None:
        pytest.skip("No CockroachDB database URL provided")

    default_engine = create_engine(database_url, isolation_level="AUTOCOMMIT")
    with default_engine.connect() as conn:
        conn.execute(text("DROP DATABASE IF EXISTS dbos_test_fork CASCADE"))
        conn.execute(text("CREATE DATABASE dbos_test_fork"))
    default_engine.dispose()

    parsed = urlparse(database_url)
    test_url = urlunparse(parsed._replace(path="/dbos_test_fork"))

    step_one_count = 0
    step_two_count = 0
    step_three_count = 0

    @DBOS.step()
    def step_one(x: int) -> int:
        nonlocal step_one_count
        step_one_count += 1
        DBOS.set_event("after_step_one", x + 1)
        return x + 1

    @DBOS.step()
    def step_two(x: int) -> int:
        nonlocal step_two_count
        step_two_count += 1
        DBOS.set_event("after_step_two", x + 2)
        return x + 2

    @DBOS.step()
    def step_three(x: int) -> int:
        nonlocal step_three_count
        step_three_count += 1
        DBOS.set_event("after_step_three", x + 3)
        return x + 3

    @DBOS.workflow()
    def three_step_workflow(x: int) -> int:
        a = step_one(x)
        b = step_two(x)
        c = step_three(x)
        return a + b + c

    try:
        engine = create_engine(test_url)
        config: DBOSConfig = {
            "name": "cockroachdb-fork-test",
            "system_database_url": test_url,
            "use_listen_notify": False,
            "system_database_engine": engine,
        }
        DBOS(config=config)
        DBOS.launch()

        # Run the original workflow
        handle = DBOS.start_workflow(three_step_workflow, 5)
        assert handle.get_result() == 21  # (5+1) + (5+2) + (5+3)
        assert step_one_count == 1
        assert step_two_count == 1
        assert step_three_count == 1

        wfid = handle.workflow_id

        # Verify events on the original workflow
        assert DBOS.get_event(wfid, "after_step_one") == 6
        assert DBOS.get_event(wfid, "after_step_two") == 7
        assert DBOS.get_event(wfid, "after_step_three") == 8

        # Fork from step 2: step_one replayed, step_two and step_three re-executed
        forked = DBOS.fork_workflow(wfid, 2)
        assert forked.get_result() == 21
        assert step_one_count == 1  # replayed
        assert step_two_count == 2  # re-executed
        assert step_three_count == 2  # re-executed

        # Events set before the fork point should be accessible on the fork
        assert DBOS.get_event(forked.workflow_id, "after_step_one") == 6
        # Events set at or after the fork point should also be set (by re-execution)
        assert DBOS.get_event(forked.workflow_id, "after_step_two") == 7
        assert DBOS.get_event(forked.workflow_id, "after_step_three") == 8

        # Verify was_forked_from and forked_from fields
        assert forked.get_status().forked_from == wfid
        original_status = DBOS.get_workflow_status(wfid)
        assert original_status is not None
        assert original_status.was_forked_from is True
        assert forked.get_status().was_forked_from is False

        # Verify list_workflows filter
        forked_from_list = DBOS.list_workflows(was_forked_from=True)
        assert any(w.workflow_id == wfid for w in forked_from_list)
        not_forked_from_list = DBOS.list_workflows(was_forked_from=False)
        assert any(w.workflow_id == forked.workflow_id for w in not_forked_from_list)
    finally:
        DBOS.destroy(destroy_registry=True)
