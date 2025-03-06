import os
import platform
import signal
import subprocess
import time
import typing
from os import path
from typing import Any

import jsonpickle  # type: ignore
import requests
import sqlalchemy as sa
import typer
from rich import print
from rich.prompt import IntPrompt
from typing_extensions import Annotated

from dbos._debug import debug_workflow, parse_start_command

from .. import load_config
from .._app_db import ApplicationDatabase
from .._dbos_config import _is_valid_app_name
from .._sys_db import SystemDatabase, reset_system_database
from .._workflow_commands import get_workflow, list_queued_workflows, list_workflows
from ..cli._github_init import create_template_from_github
from ._template_init import copy_template, get_project_name, get_templates_directory

app = typer.Typer()
workflow = typer.Typer()
queue = typer.Typer()

app.add_typer(workflow, name="workflow", help="Manage DBOS workflows")
workflow.add_typer(queue, name="queue", help="Manage enqueued workflows")


def _on_windows() -> bool:
    return platform.system() == "Windows"


@app.command(
    help="Start your DBOS application using the start commands in 'dbos-config.yaml'"
)
def start() -> None:
    config = load_config()
    start_commands = config["runtimeConfig"]["start"]
    typer.echo("Executing start commands from 'dbos-config.yaml'")
    for command in start_commands:
        typer.echo(f"Executing: {command}")

        # Run the command in the child process.
        # On Unix-like systems, set its process group
        process = subprocess.Popen(
            command,
            shell=True,
            text=True,
            preexec_fn=os.setsid if not _on_windows() else None,
        )

        def signal_handler(signum: int, frame: Any) -> None:
            """
            Forward kill signals to children.

            When we receive a signal, send it to the entire process group of the child.
            If that doesn't work, SIGKILL them then exit.
            """
            # Send the signal to the child's entire process group
            if process.poll() is None:
                os.killpg(os.getpgid(process.pid), signum)

            # Give some time for the child to terminate
            for _ in range(10):  # Wait up to 1 second
                if process.poll() is not None:
                    break
                time.sleep(0.1)

            # If the child is still running, force kill it
            if process.poll() is None:
                try:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                except Exception:
                    pass

            # Exit immediately
            os._exit(process.returncode if process.returncode is not None else 1)

        # Configure the single handler only on Unix-like systems.
        # TODO: Also kill the children on Windows.
        if not _on_windows():
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
        process.wait()


@app.command(help="Initialize a new DBOS application from a template")
def init(
    project_name: Annotated[
        typing.Optional[str], typer.Argument(help="Specify application name")
    ] = None,
    template: Annotated[
        typing.Optional[str],
        typer.Option("--template", "-t", help="Specify template to use"),
    ] = None,
    config: Annotated[
        bool,
        typer.Option("--config", "-c", help="Only add dbos-config.yaml"),
    ] = False,
) -> None:
    try:

        git_templates = ["dbos-toolbox", "dbos-app-starter", "dbos-cron-starter"]
        templates_dir = get_templates_directory()
        templates = git_templates + [
            x.name for x in os.scandir(templates_dir) if x.is_dir()
        ]

        if config and template is None:
            template = templates[-1]

        if template:
            if template not in templates:
                raise Exception(f"Template {template} not found in {templates_dir}")
        else:
            print("\n[bold]Available templates:[/bold]")
            for idx, template_name in enumerate(templates, 1):
                print(f"  {idx}. {template_name}")
            while True:
                try:
                    choice = IntPrompt.ask(
                        "\nSelect template number",
                        show_choices=False,
                        show_default=False,
                    )
                    if 1 <= choice <= len(templates):
                        template = templates[choice - 1]
                        break
                    else:
                        print(
                            "[red]Invalid selection. Please choose a number from the list.[/red]"
                        )
                except (KeyboardInterrupt, EOFError):
                    raise typer.Abort()
                except ValueError:
                    print("[red]Please enter a valid number.[/red]")

        if template in git_templates:
            project_name = template
        else:
            if project_name is None:
                project_name = typing.cast(
                    str,
                    typer.prompt("What is your project's name?", get_project_name()),
                )

        if not _is_valid_app_name(project_name):
            raise Exception(
                f"{project_name} is an invalid DBOS app name. App names must be between 3 and 30 characters long and contain only lowercase letters, numbers, dashes, and underscores."
            )

        if template in git_templates:
            create_template_from_github(app_name=project_name, template_name=template)
        else:
            copy_template(
                path.join(templates_dir, template), project_name, config_mode=config
            )
    except Exception as e:
        print(f"[red]{e}[/red]")


@app.command(
    help="Run your database schema migrations using the migration commands in 'dbos-config.yaml'"
)
def migrate() -> None:
    config = load_config()
    if not config["database"]["password"]:
        typer.echo(
            "DBOS configuration does not contain database password, please check your config file and retry!"
        )
        raise typer.Exit(code=1)
    app_db_name = config["database"]["app_db_name"]

    typer.echo(f"Starting schema migration for database {app_db_name}")

    # First, run DBOS migrations on the system database and the application database
    app_db = None
    sys_db = None
    try:
        sys_db = SystemDatabase(config)
        app_db = ApplicationDatabase(config)
    except Exception as e:
        typer.echo(f"DBOS system schema migration failed: {e}")
    finally:
        if sys_db:
            sys_db.destroy()
        if app_db:
            app_db.destroy()

    # Next, run any custom migration commands specified in the configuration
    typer.echo("Executing migration commands from 'dbos-config.yaml'")
    try:
        migrate_commands = (
            config["database"]["migrate"]
            if "migrate" in config["database"] and config["database"]["migrate"]
            else []
        )
        for command in migrate_commands:
            typer.echo(f"Executing migration command: {command}")
            result = subprocess.run(command, shell=True, text=True)
            if result.returncode != 0:
                typer.echo(f"Migration command failed: {command}")
                typer.echo(result.stderr)
                raise typer.Exit(1)
            if result.stdout:
                typer.echo(result.stdout.rstrip())
    except Exception as e:
        typer.echo(f"An error occurred during schema migration: {e}")
        raise typer.Exit(code=1)

    typer.echo(f"Completed schema migration for database {app_db_name}")


@app.command(help="Reset the DBOS system database")
def reset(
    yes: bool = typer.Option(False, "-y", "--yes", help="Skip confirmation prompt")
) -> None:
    if not yes:
        confirm = typer.confirm(
            "This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed?"
        )
        if not confirm:
            typer.echo("Operation cancelled.")
            raise typer.Exit()
    config = load_config()
    try:
        reset_system_database(config)
    except sa.exc.SQLAlchemyError as e:
        typer.echo(f"Error resetting system database: {str(e)}")
        return


@app.command(help="Replay Debug a DBOS workflow")
def debug(
    workflow_id: Annotated[str, typer.Argument(help="Workflow ID to debug")],
) -> None:
    config = load_config(silent=True, use_db_wizard=False)
    start = config["runtimeConfig"]["start"]
    if not start:
        typer.echo("No start commands found in 'dbos-config.yaml'")
        raise typer.Exit(code=1)
    if len(start) > 1:
        typer.echo("Multiple start commands found in 'dbos-config.yaml'")
        raise typer.Exit(code=1)
    entrypoint = parse_start_command(start[0])
    debug_workflow(workflow_id, entrypoint)


@workflow.command(help="List workflows for your application")
def list(
    limit: Annotated[
        int,
        typer.Option("--limit", "-l", help="Limit the results returned"),
    ] = 10,
    user: Annotated[
        typing.Optional[str],
        typer.Option("--user", "-u", help="Retrieve workflows run by this user"),
    ] = None,
    starttime: Annotated[
        typing.Optional[str],
        typer.Option(
            "--start-time",
            "-s",
            help="Retrieve workflows starting after this timestamp (ISO 8601 format)",
        ),
    ] = None,
    endtime: Annotated[
        typing.Optional[str],
        typer.Option(
            "--end-time",
            "-e",
            help="Retrieve workflows starting before this timestamp (ISO 8601 format)",
        ),
    ] = None,
    status: Annotated[
        typing.Optional[str],
        typer.Option(
            "--status",
            "-S",
            help="Retrieve workflows with this status (PENDING, SUCCESS, ERROR, RETRIES_EXCEEDED, ENQUEUED, or CANCELLED)",
        ),
    ] = None,
    appversion: Annotated[
        typing.Optional[str],
        typer.Option(
            "--application-version",
            "-v",
            help="Retrieve workflows with this application version",
        ),
    ] = None,
    name: Annotated[
        typing.Optional[str],
        typer.Option(
            "--name",
            "-n",
            help="Retrieve workflows with this name",
        ),
    ] = None,
    request: Annotated[
        bool,
        typer.Option("--request", help="Retrieve workflow request information"),
    ] = False,
) -> None:
    config = load_config(silent=True)
    sys_db = SystemDatabase(config)
    workflows = list_workflows(
        sys_db,
        limit=limit,
        user=user,
        start_time=starttime,
        end_time=endtime,
        status=status,
        request=request,
        app_version=appversion,
        name=name,
    )
    print(jsonpickle.encode(workflows, unpicklable=False))


@workflow.command(help="Retrieve the status of a workflow")
def get(
    workflow_id: Annotated[str, typer.Argument()],
    request: Annotated[
        bool,
        typer.Option("--request", help="Retrieve workflow request information"),
    ] = False,
) -> None:
    config = load_config(silent=True)
    sys_db = SystemDatabase(config)
    print(
        jsonpickle.encode(get_workflow(sys_db, workflow_id, request), unpicklable=False)
    )


@workflow.command(
    help="Cancel a workflow so it is no longer automatically retried or restarted"
)
def cancel(
    uuid: Annotated[str, typer.Argument()],
    host: Annotated[
        typing.Optional[str],
        typer.Option("--host", "-H", help="Specify the admin host"),
    ] = "localhost",
    port: Annotated[
        typing.Optional[int],
        typer.Option("--port", "-p", help="Specify the admin port"),
    ] = 3001,
) -> None:
    response = requests.post(
        f"http://{host}:{port}/workflows/{uuid}/cancel", json=[], timeout=5
    )

    if response.status_code == 204:
        print(f"Workflow {uuid} has been cancelled")
    else:
        print(f"Failed to cancel workflow {uuid}. Status code: {response.status_code}")


@workflow.command(help="Resume a workflow that has been cancelled")
def resume(
    uuid: Annotated[str, typer.Argument()],
    host: Annotated[
        typing.Optional[str],
        typer.Option("--host", "-H", help="Specify the admin host"),
    ] = "localhost",
    port: Annotated[
        typing.Optional[int],
        typer.Option("--port", "-p", help="Specify the admin port"),
    ] = 3001,
) -> None:
    response = requests.post(
        f"http://{host}:{port}/workflows/{uuid}/resume", json=[], timeout=5
    )

    if response.status_code == 204:
        print(f"Workflow {uuid} has been resumed")
    else:
        print(f"Failed to resume workflow {uuid}. Status code: {response.status_code}")


@workflow.command(help="Restart a workflow from the beginning with a new id")
def restart(
    uuid: Annotated[str, typer.Argument()],
    host: Annotated[
        typing.Optional[str],
        typer.Option("--host", "-H", help="Specify the admin host"),
    ] = "localhost",
    port: Annotated[
        typing.Optional[int],
        typer.Option("--port", "-p", help="Specify the admin port"),
    ] = 3001,
) -> None:
    response = requests.post(
        f"http://{host}:{port}/workflows/{uuid}/restart", json=[], timeout=5
    )

    if response.status_code == 204:
        print(f"Workflow {uuid} has been restarted")
    else:
        print(f"Failed to resume workflow {uuid}. Status code: {response.status_code}")


@queue.command(name="list", help="List enqueued functions for your application")
def list_queue(
    limit: Annotated[
        typing.Optional[int],
        typer.Option("--limit", "-l", help="Limit the results returned"),
    ] = None,
    start_time: Annotated[
        typing.Optional[str],
        typer.Option(
            "--start-time",
            "-s",
            help="Retrieve functions starting after this timestamp (ISO 8601 format)",
        ),
    ] = None,
    end_time: Annotated[
        typing.Optional[str],
        typer.Option(
            "--end-time",
            "-e",
            help="Retrieve functions starting before this timestamp (ISO 8601 format)",
        ),
    ] = None,
    status: Annotated[
        typing.Optional[str],
        typer.Option(
            "--status",
            "-S",
            help="Retrieve functions with this status (PENDING, SUCCESS, ERROR, RETRIES_EXCEEDED, ENQUEUED, or CANCELLED)",
        ),
    ] = None,
    queue_name: Annotated[
        typing.Optional[str],
        typer.Option(
            "--queue-name",
            "-q",
            help="Retrieve functions on this queue",
        ),
    ] = None,
    name: Annotated[
        typing.Optional[str],
        typer.Option(
            "--name",
            "-n",
            help="Retrieve functions on this queue",
        ),
    ] = None,
    request: Annotated[
        bool,
        typer.Option("--request", help="Retrieve workflow request information"),
    ] = False,
) -> None:
    config = load_config(silent=True)
    sys_db = SystemDatabase(config)
    workflows = list_queued_workflows(
        sys_db=sys_db,
        limit=limit,
        start_time=start_time,
        end_time=end_time,
        queue_name=queue_name,
        status=status,
        request=request,
        name=name,
    )
    print(jsonpickle.encode(workflows, unpicklable=False))


if __name__ == "__main__":
    app()
