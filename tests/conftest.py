import glob
import os
import subprocess

import pytest
import sqlalchemy as sa

from dbos_transact import ConfigFile

defaultConfig: ConfigFile = {
    "database": {
        "hostname": "localhost",
        "port": 5432,
        "username": "postgres",
        "password": os.environ["PGPASSWORD"],
        "app_db_name": "dbostestpy",
    }
}


@pytest.fixture(scope="session")
def build_wheel():
    subprocess.check_call(["pdm", "build"])
    wheel_files = glob.glob(os.path.join("dist", "*.whl"))
    assert len(wheel_files) == 1
    return wheel_files[0]


def get_db_url(config: ConfigFile) -> sa.URL:
    return sa.URL.create(
        "postgresql",
        username=config["database"]["username"],
        password=config["database"]["password"],
        host=config["database"]["hostname"],
        port=config["database"]["port"],
        database="postgres",
    )
