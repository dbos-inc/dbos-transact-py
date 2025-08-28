import os
import platform
import signal
import subprocess
import time
import typing
from os import path
from typing import Any, Optional, Tuple

import jsonpickle  # type: ignore
import sqlalchemy as sa
import typer
from rich import print as richprint
from rich.prompt import IntPrompt
from typing_extensions import Annotated, List

from dbos._context import SetWorkflowID
from dbos._debug import debug_workflow, parse_start_command
from dbos.cli.migration import grant_dbos_schema_permissions, migrate_dbos_databases

from .._client import DBOSClient
from .._dbos_config import (
    ConfigFile,
    _app_name_to_db_name,
    _is_valid_app_name,
    get_application_database_url,
    get_system_database_url,
    load_config,
)
from .._docker_pg_helper import start_docker_pg, stop_docker_pg
from .._sys_db import SystemDatabase
from .._utils import GlobalParams
from ..cli._github_init import create_template_from_github
from ._template_init import copy_template, get_project_name, get_templates_directory


def _get_db_url(
    *, system_database_url: Optional[str], application_database_url: Optional[str]
) -> Tuple[str, str]:
    """
    Get the database URL to use for the DBOS application.
    Order of precedence:
    - In DBOS Cloud, use the environment variables provided.
    - Use database URL arguments if provided.
    - If the `dbos-config.yaml` file is present, use the database URLs from it.

    Otherwise fallback to the same SQLite Postgres URL than the DBOS library.
    Note that for the latter to be possible, a configuration file must have been found, with an application name set.
    """
    if os.environ.get("DBOS__CLOUD") == "true":
        system_database_url = os.environ.get("DBOS_SYSTEM_DATABASE_URL")
        application_database_url = os.environ.get("DBOS_DATABASE_URL")
        assert system_database_url and application_database_url
        return system_database_url, application_database_url
    if system_database_url or application_database_url:
        cfg: ConfigFile = {
            "system_database_url": system_database_url,
            "database_url": application_database_url,
        }
        return get_system_database_url(cfg), get_application_database_url(cfg)
    else:
        # Load from config file if present
        try:
            config = load_config(silent=True)
            if config.get("database_url") or config.get("system_database_url"):
                return get_system_database_url(config), get_application_database_url(
                    config
                )
            else:
                _app_db_name = _app_name_to_db_name(config["name"])
                # Fallback on the same defaults than the DBOS library
                default_url = f"sqlite:///{_app_db_name}.sqlite"
                return default_url, default_url
        except (FileNotFoundError, OSError):
            typer.echo(
                f"Error: Missing database URL: please set it using CLI flags or your dbos-config.yaml file.",
                err=True,
            )
            raise typer.Exit(code=1)


app = typer.Typer()


@app.command(help="Show the version and exit")
def version() -> None:
    """Display the current version of DBOS CLI."""
    typer.echo(f"DBOS CLI version: {GlobalParams.dbos_version}")


workflow = typer.Typer()
queue = typer.Typer()

app.add_typer(workflow, name="workflow", help="Manage DBOS workflows")
workflow.add_typer(queue, name="queue", help="Manage enqueued workflows")

postgres = typer.Typer()
app.add_typer(
    postgres, name="postgres", help="Manage local Postgres database with Docker"
)


@postgres.command(name="start", help="Start a local Postgres database")
def pg_start() -> None:
    start_docker_pg()


@postgres.command(name="stop", help="Stop the local Postgres database")
def pg_stop() -> None:
    stop_docker_pg()


def _on_windows() -> bool:
    return platform.system() == "Windows"


@app.command(
    help="Start your DBOS application using the start commands in 'dbos-config.yaml'"
)
def start() -> None:
    config = load_config(silent=True)
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

        project_name, template = _resolve_project_name_and_template(
            project_name=project_name,
            template=template,
            config=config,
            git_templates=git_templates,
            templates_dir=templates_dir,
        )

        if template in git_templates:
            create_template_from_github(app_name=project_name, template_name=template)
        else:
            copy_template(
                path.join(templates_dir, template), project_name, config_mode=config
            )
    except Exception as e:
        richprint(f"[red]{e}[/red]")


def _resolve_project_name_and_template(
    project_name: Optional[str],
    template: Optional[str],
    config: bool,
    git_templates: List[str],
    templates_dir: str,
) -> tuple[str, str]:
    templates = git_templates + [
        x.name for x in os.scandir(templates_dir) if x.is_dir()
    ]

    if config and template is None:
        template = templates[-1]

    if template:
        if template not in templates:
            raise Exception(f"Template {template} not found in {templates_dir}")
    else:
        richprint("\n[bold]Available templates:[/bold]")
        for idx, template_name in enumerate(templates, 1):
            richprint(f"  {idx}. {template_name}")
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
                    richprint(
                        "[red]Invalid selection. Please choose a number from the list.[/red]"
                    )
            except (KeyboardInterrupt, EOFError):
                raise typer.Abort()
            except ValueError:
                richprint("[red]Please enter a valid number.[/red]")

    if template in git_templates:
        if project_name is None:
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

    assert project_name is not None, "Project name cannot be None"
    assert template is not None, "Template name cannot be None"

    return project_name, template


@app.command(help="Create DBOS system tables.")
def migrate(
    application_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--db-url",
            "-D",
            help="Your DBOS application database URL",
        ),
    ] = None,
    system_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--sys-db-url",
            "-s",
            help="Your DBOS system database URL",
        ),
    ] = None,
    application_role: Annotated[
        typing.Optional[str],
        typer.Option(
            "--app-role",
            "-r",
            help="The role with which you will run your DBOS application",
        ),
    ] = None,
) -> None:
    system_database_url, application_database_url = _get_db_url(
        system_database_url=system_database_url,
        application_database_url=application_database_url,
    )

    typer.echo(f"Starting DBOS migrations")
    typer.echo(f"Application database: {sa.make_url(application_database_url)}")
    typer.echo(f"System database: {sa.make_url(system_database_url)}")

    # First, run DBOS migrations on the system database and the application database
    migrate_dbos_databases(
        app_database_url=application_database_url,
        system_database_url=system_database_url,
    )

    # Next, assign permissions on the DBOS schema to the application role, if any
    if application_role:
        grant_dbos_schema_permissions(
            database_url=application_database_url, role_name=application_role
        )
        grant_dbos_schema_permissions(
            database_url=system_database_url, role_name=application_role
        )

    # Next, run any custom migration commands specified in the configuration
    if os.path.exists("dbos-config.yaml"):
        config = load_config(silent=True)
        if "database" not in config:
            config["database"] = {}
        migrate_commands = (
            config["database"]["migrate"]
            if "migrate" in config["database"] and config["database"]["migrate"]
            else []
        )
        if migrate_commands:
            typer.echo("Executing migration commands from 'dbos-config.yaml'")
            try:
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


@app.command(help="Reset the DBOS system database")
def reset(
    yes: bool = typer.Option(False, "-y", "--yes", help="Skip confirmation prompt"),
    application_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--db-url",
            "-D",
            help="Your DBOS application database URL",
        ),
    ] = None,
    system_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--sys-db-url",
            "-s",
            help="Your DBOS system database URL",
        ),
    ] = None,
) -> None:
    if not yes:
        confirm = typer.confirm(
            "This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed?"
        )
        if not confirm:
            typer.echo("Operation cancelled.")
            raise typer.Exit()
    try:
        system_database_url, application_database_url = _get_db_url(
            system_database_url=system_database_url,
            application_database_url=application_database_url,
        )
        SystemDatabase.reset_system_database(system_database_url)
    except Exception as e:
        typer.echo(f"Error resetting system database: {str(e)}")
        return


@app.command(help="Replay Debug a DBOS workflow")
def debug(
    workflow_id: Annotated[str, typer.Argument(help="Workflow ID to debug")],
) -> None:
    config = load_config(silent=True)
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
    application_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--db-url",
            "-D",
            help="Your DBOS application database URL",
        ),
    ] = None,
    system_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--sys-db-url",
            "-s",
            help="Your DBOS system database URL",
        ),
    ] = None,
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
            "-t",
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
            help="Retrieve workflows with this status (PENDING, SUCCESS, ERROR, ENQUEUED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED)",
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
    sort_desc: Annotated[
        bool,
        typer.Option(
            "--sort-desc",
            "-d",
            help="Sort the results in descending order (older first)",
        ),
    ] = False,
    offset: Annotated[
        typing.Optional[int],
        typer.Option(
            "--offset",
            "-o",
            help="Offset for pagination",
        ),
    ] = None,
) -> None:
    system_database_url, application_database_url = _get_db_url(
        system_database_url=system_database_url,
        application_database_url=application_database_url,
    )
    client = DBOSClient(
        application_database_url=application_database_url,
        system_database_url=system_database_url,
    )
    workflows = client.list_workflows(
        limit=limit,
        offset=offset,
        sort_desc=sort_desc,
        user=user,
        start_time=starttime,
        end_time=endtime,
        status=status,
        app_version=appversion,
        name=name,
    )
    print(jsonpickle.encode(workflows, unpicklable=False))


@workflow.command(help="Retrieve the status of a workflow")
def get(
    workflow_id: Annotated[str, typer.Argument()],
    application_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--db-url",
            "-D",
            help="Your DBOS application database URL",
        ),
    ] = None,
    system_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--sys-db-url",
            "-s",
            help="Your DBOS system database URL",
        ),
    ] = None,
) -> None:
    system_database_url, application_database_url = _get_db_url(
        system_database_url=system_database_url,
        application_database_url=application_database_url,
    )
    client = DBOSClient(
        application_database_url=application_database_url,
        system_database_url=system_database_url,
    )
    status = client.retrieve_workflow(workflow_id=workflow_id).get_status()
    print(jsonpickle.encode(status, unpicklable=False))


@workflow.command(help="List the steps of a workflow")
def steps(
    workflow_id: Annotated[str, typer.Argument()],
    application_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--db-url",
            "-D",
            help="Your DBOS application database URL",
        ),
    ] = None,
    system_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--sys-db-url",
            "-s",
            help="Your DBOS system database URL",
        ),
    ] = None,
) -> None:
    system_database_url, application_database_url = _get_db_url(
        system_database_url=system_database_url,
        application_database_url=application_database_url,
    )
    client = DBOSClient(
        application_database_url=application_database_url,
        system_database_url=system_database_url,
    )
    print(
        jsonpickle.encode(
            client.list_workflow_steps(workflow_id=workflow_id),
            unpicklable=False,
        )
    )


@workflow.command(
    help="Cancel a workflow so it is no longer automatically retried or restarted"
)
def cancel(
    workflow_id: Annotated[str, typer.Argument()],
    application_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--db-url",
            "-D",
            help="Your DBOS application database URL",
        ),
    ] = None,
    system_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--sys-db-url",
            "-s",
            help="Your DBOS system database URL",
        ),
    ] = None,
) -> None:
    system_database_url, application_database_url = _get_db_url(
        system_database_url=system_database_url,
        application_database_url=application_database_url,
    )
    client = DBOSClient(
        application_database_url=application_database_url,
        system_database_url=system_database_url,
    )
    client.cancel_workflow(workflow_id=workflow_id)


@workflow.command(help="Resume a workflow that has been cancelled")
def resume(
    workflow_id: Annotated[str, typer.Argument()],
    application_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--db-url",
            "-D",
            help="Your DBOS application database URL",
        ),
    ] = None,
    system_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--sys-db-url",
            "-s",
            help="Your DBOS system database URL",
        ),
    ] = None,
) -> None:
    system_database_url, application_database_url = _get_db_url(
        system_database_url=system_database_url,
        application_database_url=application_database_url,
    )
    client = DBOSClient(
        application_database_url=application_database_url,
        system_database_url=system_database_url,
    )
    client.resume_workflow(workflow_id=workflow_id)


@workflow.command(
    help="[DEPRECATED - Use fork instead] Restart a workflow from the beginning with a new id"
)
def restart(
    workflow_id: Annotated[str, typer.Argument()],
    application_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--db-url",
            "-D",
            help="Your DBOS application database URL",
        ),
    ] = None,
    system_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--sys-db-url",
            "-s",
            help="Your DBOS system database URL",
        ),
    ] = None,
) -> None:
    system_database_url, application_database_url = _get_db_url(
        system_database_url=system_database_url,
        application_database_url=application_database_url,
    )
    client = DBOSClient(
        application_database_url=application_database_url,
        system_database_url=system_database_url,
    )
    status = client.fork_workflow(workflow_id=workflow_id, start_step=1).get_status()
    print(jsonpickle.encode(status, unpicklable=False))


@workflow.command(
    help="fork a workflow from the beginning with a new id and from a step"
)
def fork(
    workflow_id: Annotated[str, typer.Argument()],
    step: Annotated[
        int,
        typer.Option(
            "--step",
            "-S",
            help="Restart from this step",
        ),
    ] = 1,
    forked_workflow_id: Annotated[
        typing.Optional[str],
        typer.Option(
            "--forked-workflow-id",
            "-f",
            help="Custom ID for the forked workflow",
        ),
    ] = None,
    application_version: Annotated[
        typing.Optional[str],
        typer.Option(
            "--application-version",
            "-v",
            help="Custom application version for the forked workflow",
        ),
    ] = None,
    application_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--db-url",
            "-D",
            help="Your DBOS application database URL",
        ),
    ] = None,
    system_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--sys-db-url",
            "-s",
            help="Your DBOS system database URL",
        ),
    ] = None,
) -> None:
    system_database_url, application_database_url = _get_db_url(
        system_database_url=system_database_url,
        application_database_url=application_database_url,
    )
    client = DBOSClient(
        application_database_url=application_database_url,
        system_database_url=system_database_url,
    )

    if forked_workflow_id is not None:
        with SetWorkflowID(forked_workflow_id):
            status = client.fork_workflow(
                workflow_id=workflow_id,
                start_step=step,
                application_version=application_version,
            ).get_status()
    else:
        status = client.fork_workflow(
            workflow_id=workflow_id,
            start_step=step,
            application_version=application_version,
        ).get_status()
    print(jsonpickle.encode(status, unpicklable=False))


@queue.command(name="list", help="List enqueued functions for your application")
def list_queue(
    application_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--db-url",
            "-D",
            help="Your DBOS application database URL",
        ),
    ] = None,
    system_database_url: Annotated[
        typing.Optional[str],
        typer.Option(
            "--sys-db-url",
            "-s",
            help="Your DBOS system database URL",
        ),
    ] = None,
    limit: Annotated[
        typing.Optional[int],
        typer.Option("--limit", "-l", help="Limit the results returned"),
    ] = None,
    start_time: Annotated[
        typing.Optional[str],
        typer.Option(
            "--start-time",
            "-t",
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
            help="Retrieve functions with this status (PENDING, SUCCESS, ERROR, ENQUEUED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED)",
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
    sort_desc: Annotated[
        bool,
        typer.Option(
            "--sort-desc",
            "-d",
            help="Sort the results in descending order (older first)",
        ),
    ] = False,
    offset: Annotated[
        typing.Optional[int],
        typer.Option(
            "--offset",
            "-o",
            help="Offset for pagination",
        ),
    ] = None,
) -> None:
    system_database_url, application_database_url = _get_db_url(
        system_database_url=system_database_url,
        application_database_url=application_database_url,
    )
    client = DBOSClient(
        application_database_url=application_database_url,
        system_database_url=system_database_url,
    )
    workflows = client.list_queued_workflows(
        limit=limit,
        offset=offset,
        sort_desc=sort_desc,
        start_time=start_time,
        end_time=end_time,
        queue_name=queue_name,
        status=status,
        name=name,
    )
    print(jsonpickle.encode(workflows, unpicklable=False))


if __name__ == "__main__":
    app()
