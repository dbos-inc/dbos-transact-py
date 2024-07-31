import os
import shutil
import subprocess

import sqlalchemy as sa

from dbos_transact.dbos_config import load_config


def test_package(build_wheel: str, postgres_db_engine: sa.Engine) -> None:
    template_path = os.path.abspath(os.path.join("templates", "hello"))

    # Clean up the database from previous runs
    config = load_config(os.path.join(template_path, "dbos-config.yaml"))
    app_db_name = config["database"]["app_db_name"]
    with postgres_db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(sa.text(f"DROP DATABASE IF EXISTS {app_db_name}"))
        connection.execute(sa.text(f"DROP DATABASE IF EXISTS {app_db_name}_dbos_sys"))

    # Create a new virtual environment in the template directory
    venv_path = os.path.join(template_path, ".venv")
    if os.path.exists(venv_path):
        shutil.rmtree(venv_path)
    # To create a venv, we need the system Python executable. TODO: Don't hardcode the path.
    subprocess.check_call(
        [os.path.join("/", "usr", "bin", "python3"), "-m", "venv", venv_path]
    )
    venv = os.environ.copy()
    venv["PATH"] = f"{os.path.join(venv_path, 'bin')}:{venv['PATH']}"
    venv["VIRTUAL_ENV"] = venv_path

    # Install the requirements of the virtual environment
    requirements_path = os.path.join(template_path, "requirements.txt")
    subprocess.check_call(["pip", "install", "-r", requirements_path], env=venv)

    # Install the dbos_transact package into the virtual environment
    subprocess.check_call(["pip", "install", build_wheel], env=venv)

    # Launch the application in the virtual environment
    subprocess.check_call(["dbos", "start"], cwd=template_path, env=venv)
