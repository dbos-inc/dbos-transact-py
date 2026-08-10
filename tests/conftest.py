import asyncio
import glob
import os
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
from typing import Any, Callable, Generator, Optional, Tuple, TypeVar, cast

T = TypeVar("T")
from pathlib import Path
from urllib.parse import quote

import pytest
import sqlalchemy as sa
from fastapi import FastAPI
from flask import Flask
from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk import trace as tracesdk
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, InMemoryLogExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from dbos import DBOS, DBOSClient, DBOSConfig
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import SystemDatabase
from dbos._sys_db_postgres import PostgresSystemDatabase


@pytest.fixture(scope="session")
def build_wheel() -> str:
    subprocess.check_call(["pdm", "build"])
    wheel_files = glob.glob(os.path.join("dist", "*.whl"))
    assert len(wheel_files) == 1
    return wheel_files[0]


def using_sqlite() -> bool:
    return os.environ.get("DBOS_DATABASE", None) == "SQLITE"


@pytest.fixture()
def skip_with_sqlite() -> None:
    if using_sqlite():
        pytest.skip("Skipping test when testing SQLite")


@pytest.fixture()
def skip_with_sqlite_imprecise_time() -> None:
    if using_sqlite() and sys.version_info < (3, 12):
        pytest.skip(
            "Skipping test when testing SQLite on Python version <3.12 as SQLite lacks ms-precision timestamps"
        )


def postgres_urls() -> Tuple[str, str]:
    """The (application, system) PostgreSQL URLs shared by every test."""
    password = quote(os.environ.get("PGPASSWORD", "dbos"), safe="")
    return (
        f"postgresql://postgres:{password}@localhost:5432/dbostestpy",
        f"postgresql+psycopg://postgres:{password}@localhost:5432/dbostestpy_dbos_sys",
    )


def default_config(sqlite_path: Path) -> DBOSConfig:
    """Build a test config. sqlite_path must be unique per test, since DBOS.destroy
    leaves workflow threads running and a shared file lets them corrupt the next
    test's database."""
    application_url, system_url = postgres_urls()
    return {
        "name": "test-app",
        "application_database_url": (
            f"sqlite:///{sqlite_path}" if using_sqlite() else application_url
        ),
        "system_database_url": (
            f"sqlite:///{sqlite_path}" if using_sqlite() else system_url
        ),
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
    }


@pytest.fixture()
def sqlite_path(tmp_path: Path) -> Path:
    """A SQLite database for this test alone, pre-created in WAL mode so DBOS
    inherits it from the file header. Under the default rollback journal a writer
    holds an exclusive lock across its commit, stalling the notification poller
    for seconds, so recv waiters miss their wakeup and pay their full timeout."""
    db_path = tmp_path / "test.sqlite"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
    finally:
        connection.close()
    return db_path


@pytest.fixture()
def config(sqlite_path: Path) -> DBOSConfig:
    return default_config(sqlite_path)


@pytest.fixture(scope="session")
def db_engine() -> Generator[sa.Engine, Any, None]:
    if using_sqlite():
        # Requested only by tests that branch away from it under SQLite.
        engine = sa.create_engine("sqlite://")
    else:
        engine = sa.create_engine(
            sa.make_url(postgres_urls()[1]).set(database="postgres"),
            connect_args={
                "connect_timeout": 30,
            },
        )
    yield engine
    engine.dispose()


def _truncate_application_database(application_database_url: str) -> None:
    """Empty every table in the application database, leaving its schemas intact.

    Covers DBOS's own transaction_outputs plus whatever tables tests create, so a
    workflow ID reused across tests cannot replay a stale transaction output.
    """
    engine = sa.create_engine(
        sa.make_url(application_database_url).set(drivername="postgresql+psycopg"),
        connect_args={"connect_timeout": 30},
    )
    try:
        with engine.begin() as connection:
            tables = connection.execute(
                sa.text(
                    "SELECT schemaname, tablename FROM pg_tables "
                    "WHERE schemaname NOT IN ('pg_catalog', 'information_schema')"
                )
            ).all()
            if tables:
                targets = ", ".join(f'"{schema}"."{table}"' for schema, table in tables)
                connection.execute(
                    sa.text(f"TRUNCATE TABLE {targets} RESTART IDENTITY CASCADE")
                )
    finally:
        engine.dispose()


def _reset_test_databases(db_engine: sa.Engine, *, drop: bool) -> None:
    """Hand the test empty shared databases, by dropping them or by emptying them."""
    # Stop any DBOS an earlier test left running before touching its databases.
    DBOS.destroy(destroy_registry=True)
    for var in ("DBOS__VMID", "DBOS__APPVERSION", "DBOS__APPID"):
        os.environ.pop(var, None)

    # SQLite needs no reset here: sqlite_path is a fresh file per test.
    if using_sqlite():
        return

    app_db_url, sys_db_url = postgres_urls()
    names = [str(sa.make_url(url).database) for url in (app_db_url, sys_db_url)]
    with db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        # DROP DATABASE ... WITH (FORCE) evicted leaked connections for us, and
        # truncation needs that too: a thread an earlier test failed to stop can
        # block TRUNCATE, or write into the database the next test just emptied.
        connection.execute(
            sa.text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = ANY(:names) AND pid <> pg_backend_pid()"
            ),
            {"names": names},
        )
        if drop:
            for name in names:
                connection.execute(
                    sa.text(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
                )
            return
        # An absent database holds no state to empty, and connecting to one errors.
        present = set(
            connection.execute(
                sa.text("SELECT datname FROM pg_database WHERE datname = ANY(:names)"),
                {"names": names},
            ).scalars()
        )

    app_db_name, sys_db_name = names
    if sys_db_name in present:
        SystemDatabase.reset_system_database(sys_db_url, truncate=True)
    if app_db_name in present:
        _truncate_application_database(app_db_url)


@pytest.fixture()
def cleanup_test_databases(db_engine: sa.Engine) -> None:
    """Give the test empty databases, without dropping them.

    Truncating is several times faster than dropping, because the next launch
    finds its schema already migrated. Tests that need the databases or the DBOS
    schema to be genuinely absent, or that leave a schema truncation cannot
    repair, must use drop_test_databases instead.
    """
    _reset_test_databases(db_engine, drop=False)


@pytest.fixture()
def drop_test_databases(db_engine: sa.Engine) -> Generator[None, Any, None]:
    """cleanup_test_databases, but dropping the databases outright.

    For tests that need the databases (or the DBOS schema inside them) to not
    exist, that must exercise a from-scratch migration, or that leave behind a
    schema truncation cannot repair. Drops on both sides, so neither this test
    nor the next one inherits the damage.
    """
    _reset_test_databases(db_engine, drop=True)
    yield
    _reset_test_databases(db_engine, drop=True)


@pytest.fixture()
def dbos(
    config: DBOSConfig, cleanup_test_databases: None
) -> Generator[DBOS, Any, None]:
    DBOS.destroy(destroy_registry=True)

    # This launches for test convenience.
    #    Tests add to running DBOS and then call stuff without adding
    #     launch themselves.
    # If your test is tricky and has a problem with this, use a different
    #   fixture that does not launch.
    dbos = DBOS(config=config)
    DBOS.launch()

    yield dbos
    DBOS.destroy(destroy_registry=True)


@pytest.fixture()
def dbos_dropped_databases(
    config: DBOSConfig, drop_test_databases: None
) -> Generator[DBOS, Any, None]:
    """The dbos fixture on dropped databases, so launch migrates from scratch."""
    dbos = DBOS(config=config)
    DBOS.launch()

    yield dbos
    DBOS.destroy(destroy_registry=True)


@pytest.fixture()
def client(config: DBOSConfig, dbos: DBOS) -> Generator[DBOSClient, Any, None]:
    assert config["application_database_url"] is not None
    assert config["system_database_url"] is not None
    client = DBOSClient(
        application_database_url=config["application_database_url"],
        system_database_url=config["system_database_url"],
    )
    yield client
    client.destroy()


@pytest.fixture()
def dbos_fastapi(  # type: ignore
    config: DBOSConfig, cleanup_test_databases: None, setup_in_memory_otlp_collector
) -> Generator[Tuple[DBOS, FastAPI], Any, None]:
    exporter, log_processor, log_exporter = setup_in_memory_otlp_collector
    config["enable_otlp"] = True
    DBOS.destroy(destroy_registry=True)
    app = FastAPI()
    dbos = DBOS(fastapi=app, config=config)

    # This is for test convenience.
    #    Usually fastapi itself does launch, but we are not completing the fastapi lifecycle
    DBOS.launch()

    yield dbos, app
    DBOS.destroy(destroy_registry=True)


@pytest.fixture()
def dbos_flask(
    config: DBOSConfig, cleanup_test_databases: None
) -> Generator[Tuple[DBOS, Flask], Any, None]:
    DBOS.destroy(destroy_registry=True)
    app = Flask(__name__)

    dbos = DBOS(flask=app, config=config)

    # This is for test convenience.
    #    Usually fastapi itself does launch, but we are not completing the fastapi lifecycle
    DBOS.launch()

    yield dbos, app
    DBOS.destroy(destroy_registry=True)


# Type for mypy
# define type
TestOtelType = Tuple[
    InMemorySpanExporter,
    BatchLogRecordProcessor,
    InMemoryLogExporter,
]


@pytest.fixture(scope="session")
def setup_in_memory_otlp_collector() -> Generator[
    TestOtelType,
    Any,
    None,
]:
    exporter = InMemorySpanExporter()
    span_processor = SimpleSpanProcessor(exporter)
    provider = tracesdk.TracerProvider()
    provider.add_span_processor(span_processor)
    trace.set_tracer_provider(provider)

    # Set up in-memory log exporter
    log_exporter = InMemoryLogExporter()  # type: ignore
    log_processor = BatchLogRecordProcessor(log_exporter)
    log_provider = LoggerProvider()
    log_provider.add_log_record_processor(log_processor)
    set_logger_provider(log_provider)

    yield exporter, log_processor, log_exporter


# Pretty-print test names
def pytest_collection_modifyitems(session: Any, config: Any, items: Any) -> None:
    for item in items:
        item._nodeid = "\n" + item.nodeid + "\n"


def set_workflow_status(sys_db: SystemDatabase, workflow_id: str, status: str) -> None:
    # Force a workflow's status directly, bypassing the guards in
    # update_workflow_outcome (which only finalizes PENDING workflows).
    # Used by tests to reset completed workflows to PENDING for recovery.
    with sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": status})
            .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
        )


def queue_entries_are_cleaned_up(dbos: DBOS) -> bool:
    max_tries = 10
    success = False
    for i in range(max_tries):
        with dbos._sys_db.engine.begin() as c:
            query = (
                sa.select(sa.func.count())
                .select_from(SystemSchema.workflow_status)
                .where(
                    sa.and_(
                        SystemSchema.workflow_status.c.queue_name.isnot(None),
                        SystemSchema.workflow_status.c.status.in_(
                            ["DELAYED", "ENQUEUED", "PENDING"]
                        ),
                    )
                )
            )
            row = c.execute(query).fetchone()
            assert row is not None
            count = row[0]
            if count == 0:
                success = True
                break
        time.sleep(1)
    return success


def retry_until_success(
    func: Callable[[], T], interval: float = 1, max_attempts: int = 10
) -> T:
    error: Optional[Exception] = None
    for _ in range(max_attempts):
        try:
            return func()
        except Exception as e:
            error = e
            time.sleep(interval)
    if error is not None:
        raise error
    raise RuntimeError("retry_until_success failed without an exception")


def wait_for_client_listener(client: DBOSClient) -> None:
    """Block until a use_listen_notify client's listener has issued its LISTENs.

    The listener thread starts asynchronously in the constructor, so a notification
    committed before its LISTEN lands is dropped (Postgres does not replay to a
    session that subscribes late) and the waiter then blocks until its 60s fallback
    re-check. Tests that fire a notification must gate on this first.

    The listener holds _listener_thread_lock across connecting and issuing every
    LISTEN, so acquiring that lock and observing a connection proves they are live.
    """
    sys_db = cast(PostgresSystemDatabase, client._sys_db)

    def listener_ready() -> None:
        with sys_db._listener_thread_lock:
            assert sys_db.notification_conn is not None

    retry_until_success(listener_ready, interval=0.05, max_attempts=100)


async def retry_until_success_async(
    func: Callable[[], T], interval: float = 1, max_attempts: int = 10
) -> T:
    """Async sibling of retry_until_success.

    Sleeps with asyncio.sleep between attempts so the event loop stays free to
    make progress (e.g. to run a task we are waiting on). func itself is sync.
    """
    error: Optional[Exception] = None
    for _ in range(max_attempts):
        try:
            return func()
        except Exception as e:
            error = e
            await asyncio.sleep(interval)
    if error is not None:
        raise error
    raise RuntimeError("retry_until_success_async failed without an exception")


def pytest_unconfigure(config: Any) -> None:
    print("Shutting down pytest")
    non_daemon_threads = [
        t
        for t in threading.enumerate()
        if t.is_alive() and not t.daemon and t is not threading.main_thread()
    ]
    if non_daemon_threads:
        frames = sys._current_frames()
        print(f"\n{len(non_daemon_threads)} active non-daemon background thread(s):")
        for t in non_daemon_threads:
            print(f"  - {t.name} (ident={t.ident}, class={type(t).__qualname__})")
            if t.ident:
                frame = frames.get(t.ident)
                if frame:
                    print("    Stack trace:")
                    for line in traceback.format_stack(frame):
                        print(f"    {line}", end="")
    else:
        print("No active non-daemon threads")
