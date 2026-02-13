import glob
import os
import subprocess
import sys
import threading
import time
import traceback
from typing import Any, Callable, Generator, Optional, Tuple, TypeVar

T = TypeVar("T")
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


def default_config() -> DBOSConfig:
    return {
        "name": "test-app",
        "application_database_url": (
            "sqlite:///test.sqlite"
            if using_sqlite()
            else f"postgresql://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'), safe='')}@localhost:5432/dbostestpy"
        ),
        "system_database_url": (
            "sqlite:///test.sqlite"
            if using_sqlite()
            else f"postgresql+psycopg://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'), safe='')}@localhost:5432/dbostestpy_dbos_sys"
        ),
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "scheduler_polling_interval_sec": 1,
    }


@pytest.fixture()
def config() -> DBOSConfig:
    return default_config()


@pytest.fixture(scope="session")
def db_engine() -> Generator[sa.Engine, Any, None]:
    cfg = default_config()
    assert cfg["system_database_url"] is not None
    if using_sqlite():
        engine = sa.create_engine(cfg["system_database_url"])
        yield engine
    else:
        engine = sa.create_engine(
            sa.make_url(cfg["system_database_url"]).set(
                drivername="postgresql+psycopg",
                database="postgres",
            ),
            connect_args={
                "connect_timeout": 30,
            },
        )
        yield engine
    engine.dispose()


@pytest.fixture()
def cleanup_test_databases(config: DBOSConfig, db_engine: sa.Engine) -> None:
    assert config["application_database_url"] is not None
    assert config["system_database_url"] is not None

    if using_sqlite():
        # For SQLite, delete the database files
        # Extract file path from SQLite URL
        parsed_url = sa.make_url(config["system_database_url"])
        db_path = parsed_url.database
        assert db_path is not None

        # Remove the database files if they exist
        if os.path.exists(db_path):
            os.remove(db_path)
    else:
        # For PostgreSQL, drop the databases
        app_db_name = sa.make_url(config["application_database_url"]).database
        sys_db_name = sa.make_url(config["system_database_url"]).database

        with db_engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(
                sa.text(f"DROP DATABASE IF EXISTS {app_db_name} WITH (FORCE)")
            )
            connection.execute(
                sa.text(f"DROP DATABASE IF EXISTS {sys_db_name} WITH (FORCE)")
            )

    # Clean up environment variables
    os.environ.pop("DBOS__VMID") if "DBOS__VMID" in os.environ else None
    os.environ.pop("DBOS__APPVERSION") if "DBOS__APPVERSION" in os.environ else None
    os.environ.pop("DBOS__APPID") if "DBOS__APPID" in os.environ else None


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
                            ["ENQUEUED", "PENDING"]
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
