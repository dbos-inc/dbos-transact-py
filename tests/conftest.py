import glob
import os
import subprocess

import pytest
import sqlalchemy as sa

from dbos_transact import ConfigFile


@pytest.fixture(scope="session")
def build_wheel():
    subprocess.check_call(["pdm", "build"])
    wheel_files = glob.glob(os.path.join("dist", "*.whl"))
    assert len(wheel_files) == 1
    return wheel_files[0]


@pytest.fixture()
def reset_test_database():
    config: ConfigFile = {
        "database": {
            "hostname": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": os.environ["PGPASSWORD"],
            "app_db_name": "dbostestpy",
        }
    }

    postgres_db_url = sa.URL.create(
        "postgresql",
        username=config["database"]["username"],
        password=config["database"]["password"],
        host=config["database"]["hostname"],
        port=config["database"]["port"],
        database="postgres",
    )

    app_db_name = config["database"]["app_db_name"]
    sys_db_name = f"{app_db_name}_dbos_sys"

    postgres_db_engine = sa.create_engine(postgres_db_url)
    with postgres_db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(sa.text(f"DROP DATABASE IF EXISTS {app_db_name}"))
        connection.execute(sa.text(f"DROP DATABASE IF EXISTS {sys_db_name}"))

    yield (config, postgres_db_engine)

    postgres_db_engine.dispose()
