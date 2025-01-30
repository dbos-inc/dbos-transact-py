import json
import os
import shutil
import signal
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

import sqlalchemy as sa


def test_package(build_wheel: str, postgres_db_engine: sa.Engine) -> None:

    # Clean up the database from previous runs
    for template_name in ["dbos-db-starter", "dbos-app-starter"]:
        db_starter = template_name == "dbos-db-starter"
        app_db_name = template_name.replace("-", "_")
        with postgres_db_engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(sa.text(f"DROP DATABASE IF EXISTS {app_db_name}"))
            connection.execute(
                sa.text(f"DROP DATABASE IF EXISTS {app_db_name}_dbos_sys")
            )

        with tempfile.TemporaryDirectory() as temp_path:
            temp_path = tempfile.mkdtemp(prefix="dbos-")
            wheel_path = os.path.abspath(build_wheel)

            # Create a new virtual environment in the temp directory
            venv_path = os.path.join(temp_path, ".venv")
            if os.path.exists(venv_path):
                shutil.rmtree(venv_path)

            # To create a venv, we need the system Python executable. TODO: Don't hardcode the path.
            subprocess.check_call(
                [os.path.join("/", "usr", "bin", "python3"), "-m", "venv", venv_path]
            )

            venv = os.environ.copy()
            venv["PATH"] = f"{os.path.join(venv_path, 'bin')}:{venv['PATH']}"
            venv["VIRTUAL_ENV"] = venv_path

            # Install the dbos package into the virtual environment
            subprocess.check_call(
                ["pip", "install", wheel_path], cwd=temp_path, env=venv
            )

            # initalize the app with dbos scaffolding
            subprocess.check_call(
                ["dbos", "init", template_name, "--template", "dbos-db-starter"],
                cwd=temp_path,
                env=venv,
            )

            # Run schema migration
            subprocess.check_call(["dbos", "migrate"], cwd=temp_path, env=venv)

            # Launch the application in the virtual environment as a background process
            process = subprocess.Popen(["dbos", "start"], cwd=temp_path, env=venv)

            try:
                url = "http://localhost:8000"
                if db_starter:
                    url += "/greeting/dbos"
                max_retries = 10
                for attempt in range(max_retries):
                    try:
                        with urllib.request.urlopen(url, timeout=1) as response:
                            status_code = response.getcode()
                            assert status_code == 200
                            if db_starter:
                                response_data = response.read().decode("utf-8")
                                data = json.loads(response_data)
                                assert (
                                    data
                                    == "Greetings, dbos! You have been greeted 1 times."
                                )
                            break
                    except (urllib.error.URLError, AssertionError) as e:
                        if attempt < max_retries - 1:  # If not the last attempt
                            print(
                                f"Attempt {attempt + 1} failed: {e}. Retrying in 1 second..."
                            )
                            time.sleep(1)
                        else:
                            print(f"All {max_retries} attempts failed. Last error: {e}")
                            raise
            finally:
                os.kill(process.pid, signal.SIGINT)
                process.wait()
