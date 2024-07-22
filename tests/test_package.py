import os
import shutil
import subprocess

import sqlalchemy as sa

from dbos_transact.dbos_config import load_config

from . import conftest


def test_package(build_wheel):
    # Create a new virtual environment in the template directory
    template_path = os.path.abspath(os.path.join("templates", "hello"))
    venv_path = os.path.join(template_path, ".venv")
    if os.path.exists(venv_path):
        shutil.rmtree(venv_path)
    # To create a venv, we need the system Python executable. TODO: Don't hardcode the path.
    subprocess.check_call(
        [os.path.join("/", "usr", "bin", "python3"), "-m", "venv", venv_path]
    )
    pip_executable = (
        os.path.join(venv_path, "bin", "pip")
        if os.name != "nt"
        else os.path.join(venv_path, "Scripts", "pip.exe")
    )

    # Install the dbos_transact package into the virtual environment
    subprocess.check_call([pip_executable, "install", build_wheel])
    python_executable = (
        os.path.join(venv_path, "bin", "python")
        if os.name != "nt"
        else os.path.join(venv_path, "Scripts", "python.exe")
    )

    # Run the template code and verify it works with the installed package
    subprocess.check_call([python_executable, "main.py"], cwd=template_path)
