import asyncio
import contextlib
import glob
import importlib
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    cast,
)
from unittest import mock

T = TypeVar("T")
from pathlib import Path
from types import FrameType
from urllib.parse import quote

import psycopg
import pytest
import sqlalchemy as sa
from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk import trace as tracesdk
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import (
    BatchLogRecordProcessor,
    InMemoryLogRecordExporter,
)
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import event as sa_event

import dbos._dbos as dbos_module
import dbos._workflow_commands as workflow_commands
from dbos import (
    DBOS,
    DBOSClient,
    DBOSConfig,
    _enterprise,
    _kafka,
    _workflow_commands,
    run_dbos_database_migrations,
)
from dbos._context import DBOSContext
from dbos._core import execute_dequeued_workflow
from dbos._error import DBOSException
from dbos._queue import queue_worker_thread
from dbos._recovery import startup_recovery_thread
from dbos._scheduler import _ScheduleThread
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import SystemDatabase
from dbos._sys_db_postgres import PostgresSystemDatabase
from dbos._utils import GlobalParams, quote_identifier

if TYPE_CHECKING:
    from dbos._dbos import WorkflowHandle


@pytest.fixture(scope="session")
def build_wheel() -> str:
    subprocess.check_call(["pdm", "build"])
    wheel_files = glob.glob(os.path.join("dist", "*.whl"))
    assert len(wheel_files) == 1
    return wheel_files[0]


def using_sqlite() -> bool:
    return os.environ.get("DBOS_DATABASE", None) == "SQLITE"


def using_sharding() -> bool:
    """Whether the system database is a sharded one behind a router (requires dbos-enterprise)."""
    return os.environ.get("DBOS_DATABASE", None) == "SHARDED"


def sharded_system_url() -> str:
    return os.environ.get(
        "DBOS_SHARDED_SYSTEM_URL",
        "postgresql+psycopg://postgres:dbos@localhost:6432/dbos",
    )


@pytest.fixture()
def skip_with_sqlite() -> None:
    if using_sqlite():
        pytest.skip("Skipping test when testing SQLite")


@pytest.fixture()
def skip_with_sharding() -> None:
    """For tests of features sharding refuses (deduplication, debouncing), that write
    core's unkeyed tables directly, manage databases, or time batched inserts."""
    if using_sharding():
        pytest.skip("Skipping test when testing a sharded system database")


def imprecise_timestamps() -> bool:
    """Whether database-stamped timestamps have second, not millisecond, resolution."""
    return using_sqlite() and sys.version_info < (3, 12)


@pytest.fixture()
def skip_with_sqlite_imprecise_time() -> None:
    if imprecise_timestamps():
        pytest.skip(
            "Skipping test when testing SQLite on Python version <3.12 as SQLite lacks ms-precision timestamps"
        )


def postgres_urls() -> Tuple[str, str]:
    """The (user, system) PostgreSQL URLs shared by every test. The first is a
    plain user database, for tests that attach a datasource to one."""
    password = quote(os.environ.get("PGPASSWORD", "dbos"), safe="")
    return (
        f"postgresql://postgres:{password}@localhost:5432/dbostestpy",
        (
            sharded_system_url()
            if using_sharding()
            else f"postgresql+psycopg://postgres:{password}@localhost:5432/dbostestpy_dbos_sys"
        ),
    )


def default_config(sqlite_path: Path) -> DBOSConfig:
    """Build a test config. sqlite_path must be unique per test, since DBOS.destroy
    leaves workflow threads running and a shared file lets them corrupt the next
    test's database."""
    _, system_url = postgres_urls()
    return {
        "name": "test-app",
        "system_database_url": (
            f"sqlite:///{sqlite_path}" if using_sqlite() else system_url
        ),
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        # Kafka consumers enqueue onto the internal queues, whose 1s default poll
        # otherwise dominates every consumer test. Tests asserting on that default
        # pop this key.
        "kafka_queue_polling_interval_sec": 0.05,
        "sharding": using_sharding(),
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


def _reset_sharded_system_database() -> None:
    """Drop the sharded schema through the router, which runs DDL on every shard; launch recreates it."""
    engine = sa.create_engine(sharded_system_url())
    try:
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            # Only schemas DBOS created, including custom system schemas a test
            # configured; a coordinator's own schemas (citus, columnar) stay.
            schemas = connection.execute(
                sa.text(
                    "SELECT DISTINCT table_schema FROM information_schema.tables "
                    "WHERE table_name = 'dbos_migrations' "
                    "UNION SELECT 'dbos'"
                )
            ).scalars()
            for schema in list(schemas):
                # Escaped for the driver, which reads a bare % as a placeholder.
                name = quote_identifier(schema).replace("%", "%%")
                connection.exec_driver_sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")
    finally:
        engine.dispose()


def _reset_sharded_run_user_database(drop: bool) -> None:
    """The user database stays plain local Postgres in sharded runs, reset as in any other."""
    user_db_url = postgres_urls()[0]
    name = str(sa.make_url(user_db_url).database)
    # db_engine points at the router in sharded runs, which serves only the system database.
    engine = sa.create_engine(
        sa.make_url(user_db_url).set(
            drivername="postgresql+psycopg", database="postgres"
        ),
        connect_args={"connect_timeout": 30},
    )
    try:
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            if drop:
                connection.execute(
                    sa.text(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
                )
                return
            present = connection.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": name},
            ).scalar()
    finally:
        engine.dispose()
    if present:
        _truncate_user_database(user_db_url)


# Keys a sharded run's fallbacks filled in on DBOS's own service paths, for the canary.
lost_shard_keys: List[str] = []


def _on_service_path() -> bool:
    """Whether this thread runs only DBOS's machinery, never test code: a DBOS background
    thread, a queue worker, a schedule, or startup recovery."""
    thread = threading.current_thread()
    instance = dbos_module._dbos_global_instance
    if instance is not None and thread in instance._background_threads:
        return True
    target = getattr(thread, "_target", None)
    if getattr(target, "__func__", target) in (
        queue_worker_thread,
        _ScheduleThread._loop,
    ):
        return True
    # Startup recovery runs on executor threads, which also run test code.
    frame: Optional[FrameType] = sys._getframe()
    while frame is not None:
        if frame.f_code is startup_recovery_thread.__code__:
            return True
        frame = frame.f_back
    return False


@pytest.fixture(autouse=True)
def default_shard_key() -> Generator[Optional[str], Any, None]:
    """Sharded runs only: a per-test shard key for top-level workflows and bare-ID calls
    a test makes without one. The product has no default; this keeps core's tests running
    the keyed SQL. Per test, so tests spread across shards. A child the test starts
    without a key is given its parent's key on the context core builds for it (core itself
    has no such default), with no fallback: from there core must carry it to the row and
    the handle, and a key lost on the way, or a parent running without one, fails the test.
    """
    if not using_sharding():
        yield None
        return
    # Imported by name: core's CI type-checks without dbos-enterprise installed.
    ShardedSystemDatabase: Any = importlib.import_module(
        "dbos_enterprise.sharding"
    ).ShardedSystemDatabase

    # Its own instance, so a test's monkeypatch.undo() cannot remove it.
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(_enterprise, "require_enterprise", lambda *args, **kwargs: None)

    key = f"test-{uuid.uuid4().hex[:8]}"
    # Fallbacks on DBOS's own service paths, which never run test code: there core's
    # machinery lost a key. Other threads also run test code across hops a stack can't
    # follow, so their fallbacks stand for bare IDs the test passed.
    lost_shard_keys.clear()

    def fall_back(what: str) -> str:
        if _on_service_path():
            lost_shard_keys.append(
                f"{what} on thread {threading.current_thread().name}"
            )
        return key

    def no_key(e: DBOSException) -> bool:
        # Only a missing key falls back; an invalid one is the test's error to see.
        return e.message.startswith("No shard key")

    resolve = ShardedSystemDatabase._shard_key_of
    require = ShardedSystemDatabase._require_key
    keyed_messages = ShardedSystemDatabase._keyed_messages
    schedule_values = ShardedSystemDatabase._schedule_values
    scope_key = ShardedSystemDatabase._scope_key

    def _shard_key_of(
        self: Any, workflow_id: str, explicit: Optional[str] = None
    ) -> str:
        try:
            return cast(str, resolve(self, workflow_id, explicit))
        except DBOSException as e:
            if not no_key(e):
                raise
            return fall_back(f"no shard key for workflow {workflow_id}")

    def _require_key(status: Any) -> None:
        # Children are left to core's plumbing; only top-level starts fall back.
        if status["shard_key"] is None and status.get("parent_workflow_id") is None:
            status["shard_key"] = fall_back(
                f"no shard key for new workflow {status['workflow_uuid']}"
            )
        require(status)

    def _keyed_messages(self: Any, messages: Any, *args: Any) -> Any:
        for m in messages:
            if m.shard_key is None:
                m.shard_key = key
        return keyed_messages(self, messages, *args)

    def _scope_key(self: Any) -> str:
        try:
            return cast(str, scope_key(self))
        except DBOSException as e:
            if not no_key(e):
                raise
            return fall_back("no shard key scope")

    def _schedule_values(self: Any, schedule: Any) -> Any:
        if schedule.get("shard_key") is None:
            schedule["shard_key"] = key
        return schedule_values(self, schedule)

    monkeypatch.setattr(ShardedSystemDatabase, "_shard_key_of", _shard_key_of)
    monkeypatch.setattr(
        ShardedSystemDatabase, "_require_key", staticmethod(_require_key)
    )
    monkeypatch.setattr(ShardedSystemDatabase, "_keyed_messages", _keyed_messages)
    monkeypatch.setattr(ShardedSystemDatabase, "_schedule_values", _schedule_values)
    monkeypatch.setattr(ShardedSystemDatabase, "_scope_key", _scope_key)

    kafka_consumer = _kafka.kafka_consumer

    def _kafka_consumer(
        *args: Any, shard_key: Optional[str] = None, **kwargs: Any
    ) -> Any:
        return kafka_consumer(*args, shard_key=shard_key or key, **kwargs)

    monkeypatch.setattr(_kafka, "kafka_consumer", _kafka_consumer)

    # Every new workflow's context gets a key where core builds it: a top-level workflow
    # the per-test key through fall_back, so a dequeue that lost its row's key fails; a
    # child exactly its running parent's, so a key core loses on the way to the row or
    # handle, or a parent running without one, fails.
    start_workflow_child = DBOSContext.create_start_workflow_child

    def _create_start_workflow_child(cur_ctx: Optional[DBOSContext]) -> DBOSContext:
        new_ctx = start_workflow_child(cur_ctx)
        if new_ctx.shard_key is None:
            if cur_ctx is not None and cur_ctx.is_within_workflow():
                new_ctx.shard_key = cur_ctx.shard_key
            else:
                new_ctx.shard_key = fall_back("no shard key for a top-level workflow")
        return new_ctx

    monkeypatch.setattr(
        DBOSContext,
        "create_start_workflow_child",
        staticmethod(_create_start_workflow_child),
    )

    # Clients a test builds itself connect to the sharded database too.
    client_init = DBOSClient.__init__

    def _client_init(self: Any, *args: Any, **kwargs: Any) -> None:
        url = kwargs.get("system_database_url") or (args[0] if args else "")
        if not str(url).startswith("sqlite"):
            kwargs.setdefault("sharding", True)
        client_init(self, *args, **kwargs)

    monkeypatch.setattr(DBOSClient, "__init__", _client_init)
    yield key
    monkeypatch.undo()
    assert not lost_shard_keys, "Core lost a shard key:\n" + "\n".join(
        sorted(set(lost_shard_keys))
    )


# DBOS functions whose statements read or write every shard by design: migrations, queue
# discovery, sweeps, recovery, the children walk, and admin listings.
FAN_OUT_FUNCTIONS = {
    "run_migrations",
    "dequeue_scopes",
    "transition_delayed_workflows",
    "cancel_timed_out_workflows",
    "get_pending_workflows",
    "_children_of",
    "list_workflows",
    "rename_application",
}
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__)) + os.sep
_ORIGIN = re.compile(r"/\* origin: (.*?) \*/")
_FIRST_WORD = re.compile(r"^\s*\w+")


def _statement_origin(dbos_dirs: Tuple[str, ...]) -> str:
    """The DBOS functions on the stack issuing a statement, innermost first, or "test" when
    test code issues it directly."""
    names: List[str] = []
    # Past this function and the listener calling it, both in tests/.
    frame: Optional[FrameType] = sys._getframe(2)
    while frame is not None:
        path = frame.f_code.co_filename
        if path.startswith(_TESTS_DIR):
            break
        if path.startswith(dbos_dirs) and frame.f_code.co_name not in names:
            names.append(frame.f_code.co_name)
        frame = frame.f_back
    return " < ".join(names) or "test"


def _multi_shard_counts(admin_url: str) -> Dict[str, int]:
    """How often the router has sent each statement to more than one shard."""
    with psycopg.connect(admin_url, autocommit=True) as admin:
        cursor = admin.execute("SHOW QUERY_CACHE")
        columns = [d.name for d in cursor.description or []]
        rows = cursor.fetchall()
    query_i, multi_i = columns.index("query"), columns.index("multi")
    return {" ".join(str(r[query_i]).split()): int(r[multi_i] or 0) for r in rows}


@pytest.fixture(autouse=True, scope="session")
def sharded_routing_guard() -> Generator[None, Any, None]:
    """Sharded runs only: every statement DBOS sent to more than one shard came from a
    function that fans out by design. A lost key predicate still returns the right rows
    through the router, so this is what catches it."""
    if not using_sharding():
        yield
        return
    admin_url = os.environ.get(
        "DBOS_ROUTER_ADMIN_URL", "postgresql://admin:dbos@localhost:6432/admin"
    )
    packages = (dbos_module, importlib.import_module("dbos_enterprise"))
    dbos_dirs = tuple(os.path.dirname(str(p.__file__)) + os.sep for p in packages)

    def tag(
        conn: Any, cursor: Any, statement: str, parameters: Any, *args: Any
    ) -> Tuple[str, Any]:
        # After the first keyword, where the router keeps a comment in its statement stats.
        origin = f" /* origin: {_statement_origin(dbos_dirs)} */"
        return _FIRST_WORD.sub(lambda m: m.group(0) + origin, statement, 1), parameters

    before = _multi_shard_counts(admin_url)
    sa_event.listen(sa.Engine, "before_cursor_execute", tag, retval=True)
    try:
        yield
    finally:
        sa_event.remove(sa.Engine, "before_cursor_execute", tag)
    leaked = []
    for statement, multi in _multi_shard_counts(admin_url).items():
        found = _ORIGIN.search(statement)
        if multi <= before.get(statement, 0) or found is None:
            continue
        origin = found.group(1)
        if origin != "test" and not FAN_OUT_FUNCTIONS & set(origin.split(" < ")):
            leaked.append(statement)
    assert not leaked, "Statements reached every shard:\n\n" + "\n\n".join(leaked)


def _truncate_user_database(user_database_url: str) -> None:
    """Empty every table in the shared user database, leaving its schemas intact.

    Stale datasource_outputs would let a reused workflow ID replay."""
    engine = sa.create_engine(
        sa.make_url(user_database_url).set(drivername="postgresql+psycopg"),
        connect_args={"connect_timeout": 30},
    )
    try:
        with engine.begin() as connection:
            # As the system database reset does: a leaked writer's locks block these deletes.
            connection.execute(
                sa.text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND pid <> pg_backend_pid()"
                )
            )
        with engine.begin() as connection:
            tables = connection.execute(
                sa.text(
                    "SELECT schemaname, tablename FROM pg_tables "
                    "WHERE schemaname NOT IN ('pg_catalog', 'information_schema')"
                )
            ).all()
            # Test tables hold a handful of rows and no foreign keys, so unordered
            # deletes are both correct and far cheaper than TRUNCATE's file rewrite.
            for schema, table in tables:
                connection.execute(sa.text(f'DELETE FROM "{schema}"."{table}"'))
    finally:
        engine.dispose()


def reset_global_params() -> None:
    """Return the process identity to its pre-launch defaults.

    Constructing a DBOS does this; a test that reads the identity without building
    one gets it from here, alongside the environment these are read from."""
    GlobalParams.app_version = os.environ.get("DBOS__APPVERSION", "")
    GlobalParams.executor_id = os.environ.get("DBOS__VMID") or "local"
    GlobalParams.app_name = None


@pytest.fixture(autouse=True, scope="session")
def fast_timeout_sweep() -> Generator[None, Any, None]:
    """Sweep for timed-out workflows every 100ms: the product's 1s poll would
    dominate every test that waits for a timeout."""
    original = _workflow_commands._SWEEP_POLLING_INTERVAL_SEC
    _workflow_commands._SWEEP_POLLING_INTERVAL_SEC = 0.1
    yield
    _workflow_commands._SWEEP_POLLING_INTERVAL_SEC = original


@pytest.fixture(autouse=True)
def clean_process_identity() -> Generator[None, Any, None]:
    """Scrub the identity a test leaves behind.

    Autouse, so it tears down after every other fixture. Destroy no longer resets
    the identity, so a test that sets it directly would otherwise leak it onward."""
    yield
    reset_global_params()


# Whether this session has dropped the shared databases yet.
_databases_dropped = False


def _reset_test_databases(db_engine: sa.Engine, *, drop: bool) -> None:
    """Hand the test empty shared databases, by dropping them or by emptying them."""
    # Stop any DBOS an earlier test left running before touching its databases.
    DBOS.destroy(destroy_registry=True)
    for var in ("DBOS__VMID", "DBOS__APPVERSION", "DBOS__APPID"):
        os.environ.pop(var, None)
    reset_global_params()

    # SQLite needs no reset here: sqlite_path is a fresh file per test.
    if using_sqlite():
        return
    # A run killed mid-test can leave the schema half-migrated, which truncation
    # spares and the next launch then fails on. One drop per session repairs it.
    global _databases_dropped
    if not _databases_dropped:
        _databases_dropped = True
        drop = True

    if using_sharding():
        _reset_sharded_system_database()
        _reset_sharded_run_user_database(drop)
        return

    user_db_url, sys_db_url = postgres_urls()
    names = [str(sa.make_url(url).database) for url in (user_db_url, sys_db_url)]
    with db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
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

    user_db_name, sys_db_name = names
    if sys_db_name in present:
        SystemDatabase.reset_system_database(sys_db_url, truncate=True)
    if user_db_name in present:
        _truncate_user_database(user_db_url)


@pytest.fixture()
def cleanup_test_databases(db_engine: sa.Engine) -> None:
    """Give the test empty databases, without dropping them.

    Tests needing them genuinely absent must use drop_test_databases instead."""
    _reset_test_databases(db_engine, drop=False)


@pytest.fixture()
def drop_test_databases(db_engine: sa.Engine) -> Generator[None, Any, None]:
    """cleanup_test_databases, but dropping the databases outright.

    Drops on both sides, so damage escapes into neither test."""
    _reset_test_databases(db_engine, drop=True)
    yield
    _reset_test_databases(db_engine, drop=True)


@pytest.fixture()
def migrated_system_database(db_engine: sa.Engine) -> None:
    """cleanup_test_databases, but the system database is left present and migrated.

    For tests running SQL directly instead of launching DBOS: a preceding
    drop_test_databases test leaves the shared databases absent."""
    _reset_test_databases(db_engine, drop=False)
    if not using_sqlite():
        run_dbos_database_migrations(postgres_urls()[1], sharding=using_sharding())


def ensure_user_database() -> None:
    """Create the shared user database if a drop_test_databases test removed it.

    For tests connecting to it directly, which DBOS is not there to recreate it for."""
    url = sa.make_url(postgres_urls()[0]).set(drivername="postgresql+psycopg")
    engine = sa.create_engine(
        url.set(database="postgres"), connect_args={"connect_timeout": 30}
    )
    try:
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            if not connection.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": url.database},
            ).scalar():
                connection.execute(sa.text(f'CREATE DATABASE "{url.database}"'))
    finally:
        engine.dispose()


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
    assert config["system_database_url"] is not None
    client = DBOSClient(
        system_database_url=config["system_database_url"], sharding=using_sharding()
    )
    yield client
    client.destroy()


# Type for mypy
# define type
TestOtelType = Tuple[
    InMemorySpanExporter,
    BatchLogRecordProcessor,
    InMemoryLogRecordExporter,
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
    log_exporter = InMemoryLogRecordExporter()  # type: ignore
    log_processor = BatchLogRecordProcessor(log_exporter)
    log_provider = LoggerProvider()
    log_provider.add_log_record_processor(log_processor)
    set_logger_provider(log_provider)

    yield exporter, log_processor, log_exporter


# Pretty-print test names
def pytest_collection_modifyitems(session: Any, config: Any, items: Any) -> None:
    for item in items:
        item._nodeid = "\n" + item.nodeid + "\n"


@contextlib.contextmanager
def keep_datasource_checkpoints() -> Iterator[None]:
    """Skip the datasource checkpoint cleanup at workflow completion."""
    with (
        mock.patch.object(
            workflow_commands, "delete_completed_datasource_checkpoints", mock.Mock()
        ),
        mock.patch.object(
            workflow_commands,
            "delete_completed_datasource_checkpoints_async",
            mock.AsyncMock(),
        ),
    ):
        yield


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


def reexecute_workflow_by_id(dbos: DBOS, wfid: str) -> "WorkflowHandle[Any]":
    """Dispatch a workflow off its persisted row, exactly as a queue claim does, taking ownership of it."""
    owner_xid = str(uuid.uuid4())
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values(status="PENDING", owner_xid=owner_xid)
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
        )
    status = dbos._sys_db.get_workflow_status(wfid)
    assert status is not None
    return execute_dequeued_workflow(dbos, status, owner_xid)


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


def explain_with_index_scans_only(
    dbos: DBOS, table: str, statement: str, parameters: Any
) -> list[str]:
    """EXPLAIN with seq and bitmap scans off, so a plan test checks the index covers the
    query rather than a cost race that index bloat or the visibility map can flip."""
    with dbos._sys_db.engine.begin() as conn:
        conn.exec_driver_sql(f'ANALYZE "{dbos._sys_db.schema}".{table}')
        conn.exec_driver_sql("SET LOCAL enable_seqscan = off")
        conn.exec_driver_sql("SET LOCAL enable_bitmapscan = off")
        rows = conn.exec_driver_sql(f"EXPLAIN {statement}", parameters).fetchall()
    return [str(row[0]) for row in rows]


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
