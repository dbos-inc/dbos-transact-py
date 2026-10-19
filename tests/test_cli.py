import subprocess
import sys
from pathlib import Path
from typing import List

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


def _run_dbos_start(
    tmp_path: Path, start_commands: List[str]
) -> "subprocess.CompletedProcess[str]":
    # Run in a child process: `dbos start` installs signal handlers that must not leak into pytest.
    commands = "".join(f"    - {c!r}\n" for c in start_commands)
    (tmp_path / "dbos-config.yaml").write_text(
        f"name: start-test\nruntimeConfig:\n  start:\n{commands}"
    )
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
