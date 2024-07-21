import os
import shutil
import subprocess
import sys

# Define paths
dir_path = os.path.join(
    "templates", "hello"
)  # Path to the directory containing main.py
venv_path = os.path.join(dir_path, ".venv")  # Path to the virtual environment
wheel_path = os.path.join(
    "dist", "dbos_transact-0.1.0-py3-none-any.whl"
)  # Path to the wheel file
main_script_path = os.path.join(dir_path, "main.py")  # Path to the main.py script


def create_venv():
    """Create a virtual environment in the specified directory."""
    print("Creating virtual environment...")
    shutil.rmtree(venv_path)
    subprocess.check_call(["/usr/bin/python3", "-m", "venv", venv_path])


def install_wheel():
    """Install a wheel file into the virtual environment."""
    print("Installing wheel file...")
    # Construct the path to the pip executable within the virtual environment
    pip_executable = (
        os.path.join(venv_path, "bin", "pip")
        if os.name != "nt"
        else os.path.join(venv_path, "Scripts", "pip.exe")
    )
    subprocess.check_call([pip_executable, "install", wheel_path])


def run_script():
    """Run the main.py script using the Python executable in the virtual environment."""
    print("Running main.py...")
    # Construct the path to the Python executable within the virtual environment
    python_executable = (
        os.path.join(".venv", "bin", "python")
        if os.name != "nt"
        else os.path.join(".venv", "Scripts", "python.exe")
    )
    subprocess.check_call([python_executable, "main.py"], cwd=dir_path)


def test_package():
    create_venv()
    install_wheel()
    run_script()
