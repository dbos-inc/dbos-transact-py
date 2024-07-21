import glob
import os
import shutil
import subprocess


def test_package():
    # Create a new virtual environment in the template
    template_path = os.path.abspath(os.path.join("templates", "hello"))
    venv_path = os.path.join(template_path, ".venv")
    shutil.rmtree(venv_path)
    subprocess.check_call(["/usr/bin/python3", "-m", "venv", venv_path])
    pip_executable = (
        os.path.join(venv_path, "bin", "pip")
        if os.name != "nt"
        else os.path.join(venv_path, "Scripts", "pip.exe")
    )

    # Install the dbos_transact package into the virtual environment
    wheel_files = glob.glob(os.path.join("dist", "*.whl"))
    assert len(wheel_files) == 1
    subprocess.check_call([pip_executable, "install", wheel_files[0]])
    python_executable = (
        os.path.join(venv_path, "bin", "python")
        if os.name != "nt"
        else os.path.join(venv_path, "Scripts", "python.exe")
    )

    # Run the template code and verify it works with the installed package
    subprocess.check_call([python_executable, "main.py"], cwd=template_path)
