import logging
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any, Iterator

import pytest
from sqlalchemy import event
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from dbos._logger import dbos_logger
from dbos._serialization import DefaultSerializer
from dbos._sys_db import OperationResultInternal, SystemDatabase, db_retry

_POOL_EXHAUSTED = "Database connection pool exhausted:"
_CONNECTION_FAILED = "Database connection failed:"


@contextmanager
def _system_database(
    path: Path, *, retry_connection_errors: bool = True
) -> Iterator[SystemDatabase]:
    database = SystemDatabase.create(
        f"sqlite:///{path}",
        engine_kwargs={"pool_size": 1, "max_overflow": 0, "pool_timeout": 0.05},
        engine=None,
        schema=None,
        serializer=DefaultSerializer(),
        executor_id="test",
        use_listen_notify=False,
        retry_connection_errors=retry_connection_errors,
    )
    try:
        database.run_migrations()
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute(
                "INSERT INTO workflow_status "
                "(workflow_uuid, status, name, created_at, updated_at, priority) "
                "VALUES ('workflow', 'PENDING', 'agent', 1, 1, 0)"
            )
        yield database
    finally:
        database.destroy()


def _checkpoint() -> OperationResultInternal:
    return {
        "workflow_uuid": "workflow",
        "function_id": 1,
        "function_name": "answer",
        "output": DefaultSerializer().serialize(42),
        "error": None,
        "serialization": "py_pickle",
        "started_at_epoch_ms": 1,
        "child_workflow_id": None,
    }


def _checkpoint_count(path: Path) -> int:
    # Use a separate sqlite3 connection, independent of the exhausted DBOS pool.
    with closing(sqlite3.connect(path)) as connection:
        return int(
            connection.execute("SELECT count(*) FROM operation_outputs").fetchone()[0]
        )


class _RetrySignal(logging.Handler):
    def __init__(self, event: threading.Event, prefix: str) -> None:
        super().__init__()
        self.event = event
        self.prefix = prefix
        self.retries = 0

    def emit(self, record: logging.LogRecord) -> None:
        if record.getMessage().startswith(self.prefix):
            self.retries += 1
            self.event.set()


@pytest.mark.parametrize("operation", ["cancelled_ids", "checkpoint"])
@pytest.mark.parametrize("retry_connection_errors", [True, False])
def test_pool_checkout_timeout(
    tmp_path: Path, operation: str, retry_connection_errors: bool
) -> None:
    """A temporary shortage of pool slots retries unless explicitly opted out."""
    path = tmp_path / "pool.sqlite"
    signal = threading.Event()
    observer = _RetrySignal(signal, _POOL_EXHAUSTED)
    with _system_database(
        path, retry_connection_errors=retry_connection_errors
    ) as database:
        if operation == "cancelled_ids":
            with closing(sqlite3.connect(path)) as connection, connection:
                connection.execute("UPDATE workflow_status SET status = 'CANCELLED'")
        dbos_logger.addHandler(observer)
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                # Exhaust the real pool before calling either decorated DBOS operation.
                held = database.engine.connect()

                def run() -> object:
                    try:
                        if operation == "cancelled_ids":
                            return database.get_cancelled_workflow_ids(["workflow"])
                        database.record_operation_result(_checkpoint())
                        return None
                    finally:
                        # Also wake the test if a pool timeout incorrectly escapes.
                        signal.set()

                future = executor.submit(run)
                try:
                    assert signal.wait(5), "Neither a retry nor completion was observed"
                    if retry_connection_errors:
                        assert (
                            observer.retries > 0
                        ), "Pool timeout escaped without retrying"
                        assert (
                            not future.done()
                        ), "Operation finished while the pool was held"
                        assert _checkpoint_count(path) == 0
                    else:
                        with pytest.raises(PoolTimeoutError):
                            future.result(timeout=5)
                        assert observer.retries == 0
                        assert _checkpoint_count(path) == 0
                finally:
                    # Release even on a failed assertion so a retrying worker cannot leak.
                    held.close()

                if retry_connection_errors:
                    result = future.result(timeout=5)
                    if operation == "cancelled_ids":
                        assert result == {"workflow"}
                    else:
                        assert result is None
                        assert _checkpoint_count(path) == 1
                        with closing(sqlite3.connect(path)) as connection:
                            row = connection.execute(
                                "SELECT output FROM operation_outputs"
                            ).fetchone()
                        assert DefaultSerializer().deserialize(row[0]) == 42
        finally:
            dbos_logger.removeHandler(observer)


def test_sqlite_lock_still_retries_when_connection_retries_disabled(
    tmp_path: Path,
) -> None:
    """The connection-error opt-out must not disable existing SQLite lock retries."""
    path = tmp_path / "locked.sqlite"
    signal = threading.Event()
    observer = _RetrySignal(signal, _CONNECTION_FAILED)
    with _system_database(path, retry_connection_errors=False) as database:

        # Runs after DBOS's own connect hook, so every new connection fails fast on the lock.
        @event.listens_for(database.engine, "connect")
        def short_busy_timeout(dbapi_conn: Any, connection_record: Any) -> None:
            dbapi_conn.execute("PRAGMA busy_timeout=5")

        # Drop connections opened before the hook was registered.
        database.engine.dispose()
        lock = sqlite3.connect(path)
        lock.execute("BEGIN IMMEDIATE")
        dbos_logger.addHandler(observer)
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    database.record_operation_result, _checkpoint()
                )
                try:
                    assert signal.wait(5), "The actual SQLite lock was not retried"
                    assert observer.retries > 0
                    assert not future.done()
                    assert _checkpoint_count(path) == 0
                finally:
                    lock.rollback()
                    lock.close()
                future.result(timeout=5)
                assert _checkpoint_count(path) == 1
        finally:
            dbos_logger.removeHandler(observer)


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("business timeout"),
        ValueError("invalid value"),
        InvalidRequestError("invalid request"),
    ],
)
def test_db_retry_does_not_retry_other_errors(error: Exception) -> None:
    calls = 0

    @db_retry(initial_backoff=0, max_backoff=0)
    def fail() -> None:
        nonlocal calls
        calls += 1
        if calls > 1:
            raise AssertionError("A non-retriable error was retried")
        raise error

    with pytest.raises(type(error)) as raised:
        fail()
    assert raised.value is error
    assert calls == 1
