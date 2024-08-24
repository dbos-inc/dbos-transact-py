import glob
import os
import subprocess
from typing import Any, Generator, Tuple

import pytest
import sqlalchemy as sa
from fastapi import FastAPI

from dbos import DBOS, ConfigFile


@pytest.fixture(scope="session")
def build_wheel() -> str:
    subprocess.check_call(["pdm", "build"])
    wheel_files = glob.glob(os.path.join("dist", "*.whl"))
    assert len(wheel_files) == 1
    return wheel_files[0]


def default_config() -> ConfigFile:
    return {
        "name": "test-app",
        "language": "python",
        "database": {
            "hostname": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": os.environ["PGPASSWORD"],
            "app_db_name": "dbostestpy",
        },
        "runtimeConfig": {
            "start": ["python3 main.py"],
        },
        "telemetry": {},
        "env": {},
    }


@pytest.fixture()
def config() -> ConfigFile:
    return default_config()


@pytest.fixture(scope="session")
def postgres_db_engine() -> sa.Engine:
    cfg = default_config()
    postgres_db_url = sa.URL.create(
        "postgresql",
        username=cfg["database"]["username"],
        password=cfg["database"]["password"],
        host=cfg["database"]["hostname"],
        port=cfg["database"]["port"],
        database="postgres",
    )
    return sa.create_engine(postgres_db_url)


@pytest.fixture()
def cleanup_test_databases(config: ConfigFile, postgres_db_engine: sa.Engine) -> None:
    app_db_name = config["database"]["app_db_name"]
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
    config: ConfigFile, cleanup_test_databases: None
) -> Generator[DBOS, Any, None]:
    dbos = DBOS(config=config)
    yield dbos
    dbos.destroy()


@pytest.fixture()
def dbos_fastapi(
    config: ConfigFile, cleanup_test_databases: None
) -> Generator[Tuple[DBOS, FastAPI], Any, None]:
    app = FastAPI()
    dbos = DBOS(fastapi=app, config=config)
    yield dbos, app
    dbos.destroy()


# Pretty-print test names
def pytest_collection_modifyitems(session: Any, config: Any, items: Any) -> None:
    for item in items:
        item._nodeid = "\n" + item.nodeid + "\n"
