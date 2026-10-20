import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import List

import pytest

from dbos.cli._template_init import get_templates_directory
from dbos.cli.cli import _resolve_project_name_and_template


def test_resolve_project_name_and_template() -> None:
    git_templates = ["dbos-toolbox", "dbos-app-starter"]
    templates_dir = get_templates_directory()

    # dbos init my-app -t dbos-toolbox
    project_name, template = _resolve_project_name_and_template(
        project_name="my-app",
        template="dbos-toolbox",
        config=False,
        git_templates=git_templates,
        templates_dir=templates_dir,
    )
    assert project_name == "my-app"
    assert template == "dbos-toolbox"

    # dbos init -t dbos-toolbox
    project_name, template = _resolve_project_name_and_template(
        project_name=None,
        template="dbos-toolbox",
        config=False,
        git_templates=git_templates,
        templates_dir=templates_dir,
    )
    assert project_name == "dbos-toolbox"
    assert template == "dbos-toolbox"


def _write_start_config(tmp_path: Path, start_commands: List[str]) -> None:
    commands = "".join(f"    - {c!r}\n" for c in start_commands)
    (tmp_path / "dbos-config.yaml").write_text(
        f"name: start-test\nruntimeConfig:\n  start:\n{commands}"
    )


def _run_dbos_start(
    tmp_path: Path, start_commands: List[str]
) -> "subprocess.CompletedProcess[str]":
    # Run in a child process: `dbos start` installs signal handlers that must not leak into pytest.
    _write_start_config(tmp_path, start_commands)
    return subprocess.run(
        [sys.executable, "-m", "dbos.cli.cli", "start"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_start_exits_zero_when_commands_succeed(tmp_path: Path) -> None:
    result = _run_dbos_start(tmp_path, ["echo first", "echo second"])
    assert result.returncode == 0, result.stdout + result.stderr
    # Match whole lines: "Executing: echo first" also contains "first".
    lines = result.stdout.splitlines()
    assert "first" in lines and "second" in lines


def test_start_propagates_failing_command_exit_code(tmp_path: Path) -> None:
    result = _run_dbos_start(tmp_path, ["echo first; exit 3", "echo second"])
    assert result.returncode == 3, result.stdout + result.stderr
    lines = result.stdout.splitlines()
    assert "first" in lines
    # Commands after the failing one do not run.
    assert "second" not in lines
    assert "Executing: echo second" not in lines


# A start command that writes "ready" once its SIGTERM handler is installed. On SIGTERM it
# takes a moment to shut down, writes "stopped", and exits with the given code.
_GRACEFUL_APP = (
    "import signal, sys, time\n"
    "def stop(*args):\n"
    "    time.sleep(1)\n"
    "    open('stopped', 'w').close()\n"
    "    sys.exit({code})\n"
    "signal.signal(signal.SIGTERM, stop)\n"
    "open('ready', 'w').close()\n"
    "time.sleep(60)\n"
)


def _sigterm_dbos_start(
    tmp_path: Path, start_commands: List[str]
) -> "subprocess.CompletedProcess[str]":
    """Run `dbos start`, send it SIGTERM once the app is ready, and wait for it to exit."""
    _write_start_config(tmp_path, start_commands)
    process = subprocess.Popen(
        [sys.executable, "-m", "dbos.cli.cli", "start"],
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.time() + 30
    while not (tmp_path / "ready").exists():
        assert process.poll() is None, process.communicate()[0]
        assert time.time() < deadline, "the start command never became ready"
        time.sleep(0.05)
    process.send_signal(signal.SIGTERM)
    stdout, _ = process.communicate(timeout=30)
    return subprocess.CompletedProcess(process.args, process.returncode, stdout)


def _app_command(tmp_path: Path, code: int) -> str:
    script = tmp_path / f"app_{code}.py"
    script.write_text(_GRACEFUL_APP.format(code=code))
    return f"{sys.executable} {script}"


@pytest.mark.skipif(
    sys.platform == "win32", reason="dbos start forwards signals only on Unix"
)
def test_start_waits_for_graceful_shutdown(tmp_path: Path) -> None:
    result = _sigterm_dbos_start(tmp_path, [_app_command(tmp_path, 0), "echo second"])
    assert result.returncode == 0, result.stdout
    # The app finished its shutdown before `dbos start` exited.
    assert (tmp_path / "stopped").exists()
    # A shutdown signal stops the run, so later commands don't start.
    assert "second" not in result.stdout.splitlines()


@pytest.mark.skipif(
    sys.platform == "win32", reason="dbos start forwards signals only on Unix"
)
def test_start_reports_exit_code_after_signal(tmp_path: Path) -> None:
    result = _sigterm_dbos_start(tmp_path, [_app_command(tmp_path, 4)])
    assert result.returncode == 4, result.stdout
    assert (tmp_path / "stopped").exists()


@pytest.mark.skipif(
    sys.platform == "win32", reason="dbos start forwards signals only on Unix"
)
def test_start_reports_child_killed_by_signal(tmp_path: Path) -> None:
    # No handler: the forwarded SIGTERM kills the app, which the shell reports as 128+15.
    app = tmp_path / "app.py"
    app.write_text("import time\nopen('ready', 'w').close()\ntime.sleep(60)\n")
    result = _sigterm_dbos_start(tmp_path, [f"{sys.executable} {app}"])
    assert result.returncode == 128 + signal.SIGTERM, result.stdout
