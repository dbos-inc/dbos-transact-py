import os
from typing import Any, Generator
from urllib.parse import quote

import pytest
import sqlalchemy as sa

from dbos import DBOS, DBOSConfig


def default_config() -> DBOSConfig:
    return {
        "name": "test-app",
        "database_url": f"postgresql://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'), safe='')}@localhost:5432/dbostestpy",
    }


@pytest.fixture()
def config() -> DBOSConfig:
    return default_config()


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


@pytest.fixture()
def dbos(
    config: DBOSConfig, cleanup_test_databases: None
) -> Generator[DBOS, Any, None]:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()

    yield dbos
    DBOS.destroy(destroy_registry=True)
