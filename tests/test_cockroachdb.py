import os

import pytest
from sqlalchemy import create_engine

from dbos import DBOS, DBOSConfig


def test_cockroachdb() -> None:
    database_url = os.environ.get("DBOS_COCKROACHDB_URL")
    if database_url is None:
        pytest.skip("No CockroachDB database URL provided")

    engine = create_engine(database_url)

    config: DBOSConfig = {
        "name": "cockroachdb-test",
        "system_database_url": database_url,
        "use_listen_notify": False,
        "system_database_engine": engine,
    }
    DBOS(config=config)
    DBOS.launch()
