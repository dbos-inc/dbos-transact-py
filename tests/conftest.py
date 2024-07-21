import glob
import os
import subprocess

import pytest

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
