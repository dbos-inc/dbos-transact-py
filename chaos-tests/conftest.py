import os
import random
import threading
import time
from typing import Any, Generator, Optional
from urllib.parse import quote

import pytest
import sqlalchemy as sa

from dbos import DBOS, DBOSConfig
from dbos._docker_pg_helper import start_docker_pg, stop_docker_pg


@pytest.fixture()
def config() -> DBOSConfig:
    return {
        "name": "test-app",
        "system_database_url": f"postgresql://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'), safe='')}@localhost:5432/dbostestpy_dbos_sys",
    }


@pytest.fixture()
def postgres(config: DBOSConfig) -> Generator[None, Any, None]:
    start_docker_pg()
    yield
    stop_docker_pg()


@pytest.fixture()
def cleanup_test_databases(config: DBOSConfig, postgres: None) -> None:
    assert config["system_database_url"] is not None
    engine = sa.create_engine(
        sa.make_url(config["system_database_url"]).set(
            drivername="postgresql+psycopg",
            database="postgres",
        ),
        connect_args={
            "connect_timeout": 30,
        },
    )
    sys_db_name = sa.make_url(config["system_database_url"]).database

    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(
            sa.text(f"DROP DATABASE IF EXISTS {sys_db_name} WITH (FORCE)")
        )
    engine.dispose()


class PostgresChaosMonkey:

    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.chaos_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        def _chaos_thread() -> None:
            while not self.stop_event.is_set():
                wait_time = random.uniform(5, 40)
                if not self.stop_event.wait(wait_time):
                    print(
                        f"🐒 ChaosMonkey strikes after {wait_time:.2f} seconds! Restarting Postgres..."
                    )
                    stop_docker_pg()
                    down_time = random.uniform(0, 2)
                    time.sleep(down_time)
                    start_docker_pg()

        self.stop_event.clear()
        self.chaos_thread = threading.Thread(target=_chaos_thread)
        self.chaos_thread.start()

    def stop(self) -> None:
        if self.chaos_thread is None:
            return
        self.stop_event.set()
        self.chaos_thread.join()


@pytest.fixture()
def dbos(
    config: DBOSConfig, cleanup_test_databases: None
) -> Generator[DBOS, Any, None]:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()
    monkey = PostgresChaosMonkey()
    monkey.start()
    yield dbos
    monkey.stop()
    DBOS.destroy(destroy_registry=True)
