"""Tests for SQLite threading bugs.

Bug 1: SQLite `_create_engine` ignores `engine_kwargs`, so custom pool classes
       or other engine configuration cannot be used.

Bug 2: `DBOS._destroy` closes the database BEFORE joining background threads,
       creating a race where threads may access the closed database.
"""

import threading
import time

import pytest

from dbos import DBOS, DBOSConfig, Queue
from dbos._sys_db_sqlite import SQLiteSystemDatabase
from tests.conftest import default_config, using_sqlite


@pytest.fixture()
def sqlite_config() -> DBOSConfig:
    if not using_sqlite():
        pytest.skip("This test only runs with SQLite")
    return default_config()


def test_sqlite_engine_kwargs_passed() -> None:
    """Test that SQLite _create_engine passes engine_kwargs to create_engine."""
    engine_kwargs = {"echo": True}

    sys_db = SQLiteSystemDatabase.__new__(SQLiteSystemDatabase)
    engine = sys_db._create_engine("sqlite:///:memory:", engine_kwargs)

    assert engine.echo is True, "engine_kwargs should be passed to create_engine"

    engine.dispose()


def test_sqlite_filters_postgres_connect_args() -> None:
    """Test that SQLite filters out PostgreSQL-specific connect_args.

    SQLite doesn't support `application_name` or `connect_timeout` which are
    PostgreSQL-specific. These must be filtered out or SQLite will error.
    """
    engine_kwargs = {
        "echo": True,
        "connect_args": {
            "application_name": "dbos_transact",
            "connect_timeout": 10,
            "check_same_thread": False,
        },
    }

    sys_db = SQLiteSystemDatabase.__new__(SQLiteSystemDatabase)
    engine = sys_db._create_engine("sqlite:///:memory:", engine_kwargs)

    # Engine should be created successfully (would fail if invalid args passed)
    assert engine.echo is True

    # Original kwargs should not be mutated
    assert "application_name" in engine_kwargs["connect_args"]

    engine.dispose()


def test_destroy_closes_db_before_joining_threads(
    sqlite_config: DBOSConfig, cleanup_test_databases: None
) -> None:
    """Test that DBOS.destroy joins threads before closing the database.

    This test reproduces the race condition where `_destroy` closes the database
    BEFORE joining background threads. Background threads (like the queue thread)
    may still be running and try to access the database after it's been destroyed.

    The race condition manifests when:
    1. A workflow is running in a background thread
    2. DBOS.destroy() is called
    3. The database is closed while the workflow thread is still active
    4. The workflow thread tries to access the (now closed) database

    Expected error without fix:
        DBOSException: System database accessed before DBOS was launched
    """
    DBOS.destroy(destroy_registry=True)
    DBOS(config=sqlite_config)
    DBOS.launch()

    workflow_started = threading.Event()
    destroy_started = threading.Event()

    @DBOS.workflow()
    def blocking_workflow() -> str:
        workflow_started.set()
        destroy_started.wait(timeout=30)
        time.sleep(0.2)
        DBOS.sleep(0.001)
        return "done"

    queue = Queue("destroy_race_queue")
    handle = queue.enqueue(blocking_workflow)
    workflow_id = handle.get_workflow_id()

    workflow_started.wait(timeout=10)

    def call_destroy() -> None:
        destroy_started.set()
        DBOS.destroy(destroy_registry=True)

    destroy_thread = threading.Thread(target=call_destroy)
    destroy_thread.start()
    destroy_thread.join(timeout=10)

    DBOS.destroy(destroy_registry=True)
    DBOS(config=sqlite_config)
    DBOS.launch()

    status = DBOS.get_workflow_status(workflow_id)
    assert status is not None
    assert status.status == "SUCCESS", f"got {status.status}"
    DBOS.destroy(destroy_registry=True)
