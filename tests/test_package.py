import json
import os
import shutil
import signal
import subprocess
import time
import urllib.error
import urllib.request

import sqlalchemy as sa

# Public API
from dbos import load_config

# Private API because this is a unit test
pass


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

    # Install the dbos package into the virtual environment
    subprocess.check_call(["pip", "install", build_wheel], env=venv)

    # Run schema migration
    subprocess.check_call(["dbos", "migrate"], cwd=template_path, env=venv)

    # Launch the application in the virtual environment as a background process
    process = subprocess.Popen(["dbos", "start"], cwd=template_path, env=venv)

    try:
        url = "http://localhost:8000/greeting/dbos"
        max_retries = 10
        for attempt in range(max_retries):
            try:
                with urllib.request.urlopen(url, timeout=1) as response:
                    status_code = response.getcode()
                    assert status_code == 200
                    response_data = response.read().decode("utf-8")
                    json_data = json.loads(response_data)
                    assert json_data.get("name") == "dbos1"
                    break
            except (urllib.error.URLError, AssertionError) as e:
                if attempt < max_retries - 1:  # If not the last attempt
                    print(f"Attempt {attempt + 1} failed: {e}. Retrying in 1 second...")
                    time.sleep(1)
                else:
                    print(f"All {max_retries} attempts failed. Last error: {e}")
                    raise
    finally:
        os.kill(process.pid, signal.SIGINT)
        process.wait()
