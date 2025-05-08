import glob
import os
import subprocess
import time
from typing import Any, Generator, Tuple
from urllib.parse import quote

import pytest
import sqlalchemy as sa
from fastapi import FastAPI
from flask import Flask

from dbos import DBOS, DBOSClient, DBOSConfig
from dbos._app_db import ApplicationDatabase
from dbos._dbos_config import translate_dbos_config_to_config_file
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import SystemDatabase


@pytest.fixture(scope="session")
def build_wheel() -> str:
    subprocess.check_call(["pdm", "build"])
    wheel_files = glob.glob(os.path.join("dist", "*.whl"))
    assert len(wheel_files) == 1
    return wheel_files[0]


def default_config() -> DBOSConfig:
    return {
        "name": "test-app",
        "database_url": f"postgresql://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'), safe='')}@localhost:5432/dbostestpy",
    }


@pytest.fixture()
def config() -> DBOSConfig:
    return default_config()


@pytest.fixture()
def sys_db(config: DBOSConfig) -> Generator[SystemDatabase, Any, None]:
    assert config["database_url"] is not None
    sys_db = SystemDatabase(
        database_url=config["database_url"],
        engine_kwargs={
            "pool_timeout": 30,
            "max_overflow": 0,
            "pool_size": 2,
            "connect_args": {"connect_timeout": 30},
        },
    )
    sys_db.run_migrations()
    yield sys_db
    sys_db.destroy()


@pytest.fixture()
def app_db(config: DBOSConfig) -> Generator[ApplicationDatabase, Any, None]:
    assert config["database_url"] is not None
    app_db = ApplicationDatabase(
        database_url=config["database_url"],
        engine_kwargs={
            "pool_timeout": 30,
            "max_overflow": 0,
            "pool_size": 2,
            "connect_args": {"connect_timeout": 30},
        },
    )
    app_db.run_migrations()
    yield app_db
    app_db.destroy()


@pytest.fixture(scope="session")
def postgres_db_engine() -> sa.Engine:
    cfg = default_config()
    assert cfg["database_url"] is not None
    return sa.create_engine(
        sa.make_url(cfg["database_url"]).set(
            drivername="postgresql+psycopg",
            database="postgres",
        ),
        connect_args={
            "connect_timeout": 30,
        },
    )


@pytest.fixture()
def cleanup_test_databases(config: DBOSConfig, postgres_db_engine: sa.Engine) -> None:
    assert config["database_url"] is not None
    app_db_name = sa.make_url(config["database_url"]).database
    sys_db_name = f"{app_db_name}_dbos_sys"

    with postgres_db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(
            sa.text(
                f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{app_db_name}'
            AND pid <> pg_backend_pid()
        """
            )
        )
        connection.execute(sa.text(f"DROP DATABASE IF EXISTS {app_db_name}"))
        connection.execute(
            sa.text(
                f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{sys_db_name}'
            AND pid <> pg_backend_pid()
        """
            )
        )
        connection.execute(sa.text(f"DROP DATABASE IF EXISTS {sys_db_name}"))

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
def client(config: DBOSConfig) -> Generator[DBOSClient, Any, None]:
    assert config["database_url"] is not None
    client = DBOSClient(config["database_url"])
    yield client
    client.destroy()


@pytest.fixture()
def dbos_fastapi(
    config: DBOSConfig, cleanup_test_databases: None
) -> Generator[Tuple[DBOS, FastAPI], Any, None]:
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


# Pretty-print test names
def pytest_collection_modifyitems(session: Any, config: Any, items: Any) -> None:
    for item in items:
        item._nodeid = "\n" + item.nodeid + "\n"


def queue_entries_are_cleaned_up(dbos: DBOS) -> bool:
    max_tries = 10
    success = False
    for i in range(max_tries):
        with dbos._sys_db.engine.begin() as c:
            query = sa.select(sa.func.count()).select_from(SystemSchema.workflow_queue)
            row = c.execute(query).fetchone()
            assert row is not None
            count = row[0]
            if count == 0:
                success = True
                break
        time.sleep(1)
    return success
