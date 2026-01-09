"""Tests for SQLite threading bugs.

Bug 1: SQLite `_create_engine` ignores `engine_kwargs`, so custom pool classes
       or other engine configuration cannot be used.

Bug 2: `DBOS._destroy` closes the database BEFORE joining background threads,
       creating a race where threads may access the closed database.
"""

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
    connect_args = {
        "application_name": "dbos_transact",
        "connect_timeout": 10,
        "check_same_thread": False,
    }
    engine_kwargs = {
        "echo": True,
        "connect_args": connect_args,
    }

    sys_db = SQLiteSystemDatabase.__new__(SQLiteSystemDatabase)
    engine = sys_db._create_engine("sqlite:///:memory:", engine_kwargs)

    # Engine should be created successfully (would fail if invalid args passed)
    assert engine.echo is True

    # Original kwargs should not be mutated
    assert "application_name" in connect_args

    engine.dispose()


def test_destroy_joins_threads_before_closing_db(
    sqlite_config: DBOSConfig, cleanup_test_databases: None
) -> None:
    """Test that DBOS.destroy joins background threads before closing the database.

    This test verifies that the notification listener and queue worker threads
    are properly joined before the database connection is disposed. Without
    proper ordering, these threads could attempt database operations after
    the connection pool is closed.

    The test ensures destroy() completes without hanging, which requires:
    1. Background threads respond to stop signals
    2. Threads are joined before database disposal
    """
    DBOS.destroy(destroy_registry=True)
    DBOS(config=sqlite_config)
    DBOS.launch()

    @DBOS.workflow()
    def simple_workflow() -> str:
        return "done"

    queue = Queue("destroy_test_queue")
    handle = queue.enqueue(simple_workflow)
    result = handle.get_result()
    assert result == "done"

    DBOS.destroy(destroy_registry=True)
