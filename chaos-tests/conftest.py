import os
from typing import Any, Generator
from urllib.parse import quote

import pytest
import sqlalchemy as sa

from dbos import DBOS, DBOSConfig
from dbos._docker_pg_helper import start_docker_pg, stop_docker_pg


@pytest.fixture()
def config() -> DBOSConfig:
    return {
        "name": "test-app",
        "database_url": f"postgresql://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'), safe='')}@localhost:5432/dbostestpy",
    }


@pytest.fixture()
def postgres_db_engine(config: DBOSConfig) -> Generator[sa.Engine, Any, None]:
    start_docker_pg()
    assert config["database_url"] is not None
    engine = sa.create_engine(
        sa.make_url(config["database_url"]).set(
            drivername="postgresql+psycopg",
            database="postgres",
        ),
        connect_args={
            "connect_timeout": 30,
        },
    )
    yield engine
    engine.dispose()
    stop_docker_pg()


@pytest.fixture()
def cleanup_test_databases(config: DBOSConfig, postgres_db_engine: sa.Engine) -> None:
    assert config["database_url"] is not None
    app_db_name = sa.make_url(config["database_url"]).database
    sys_db_name = f"{app_db_name}_dbos_sys"

    with postgres_db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(
            sa.text(f"DROP DATABASE IF EXISTS {app_db_name} WITH (FORCE)")
        )
        connection.execute(
            sa.text(f"DROP DATABASE IF EXISTS {sys_db_name} WITH (FORCE)")
        )


@pytest.fixture()
def dbos(
    config: DBOSConfig, cleanup_test_databases: None
) -> Generator[DBOS, Any, None]:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()

    yield dbos
    DBOS.destroy(destroy_registry=True)
