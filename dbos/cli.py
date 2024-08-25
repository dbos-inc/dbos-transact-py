import os
import platform
import signal
import subprocess
import time
from typing import Any

import typer

from dbos import load_config
from dbos.application_database import ApplicationDatabase
from dbos.system_database import SystemDatabase

app = typer.Typer()


def on_windows() -> bool:
    return platform.system() == "Windows"


@app.command()
def start() -> None:
    config = load_config()
    start_commands = config["runtimeConfig"]["start"]
    for command in start_commands:
        typer.echo(f"Executing: {command}")

        # Run the command in the child process.
        # On Unix-like systems, set its process group
        process = subprocess.Popen(
            command,
            shell=True,
            text=True,
            preexec_fn=os.setsid if not on_windows() else None,
        )

        def signal_handler(signum: int, frame: Any) -> None:
            """
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
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)

            # Exit immediately
            os._exit(process.returncode if process.returncode is not None else 1)

        # Configure the single handler only on Unix-like systems.
        # TODO: Also kill the children on Windows.
        if not on_windows():
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
        process.wait()


@app.command()
def create() -> None:
    typer.echo(f"dbos create coming soon")


@app.command()
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


if __name__ == "__main__":
    app()
