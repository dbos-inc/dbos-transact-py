import subprocess

import typer

from dbos_transact.application_database import ApplicationDatabase
from dbos_transact.dbos_config import load_config
from dbos_transact.system_database import SystemDatabase

app = typer.Typer()


@app.command()
def start() -> None:
    config = load_config()
    start_commands = config["appCommands"]["start"]
    for command in start_commands:
        typer.echo(f"Executing: {command}")
        result = subprocess.run(command, shell=True, text=True)
        if result.returncode != 0:
            typer.echo(f"Command failed: {command}")
            typer.echo(result.stderr)
            raise typer.Exit(code=1)


@app.command()
def create() -> None:
    pass


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
    sys_db = SystemDatabase(config)  # This runs migrations on the system database
    sys_db.destroy()

    app_db = ApplicationDatabase(
        config
    )  # This runs migrations on the application database

    migrate_commands = (
        config["database"]["migrate"]
        if "migrate" in config["database"] and config["database"]["migrate"]
        else []
    )
    try:
        for command in migrate_commands:
            typer.echo(f"Executing migration command: {command}")
            result = subprocess.run(command, shell=True, text=True)
            if result.returncode != 0:
                typer.echo(f"Migration command failed: {command}")
                typer.echo(result.stderr)
                raise typer.Exit(1)
            typer.echo(result.stdout.rstrip())
    except Exception as e:
        typer.echo(f"An error occurred during schema migration: {e}")
        raise typer.Exit(code=1)
    finally:
        app_db.destroy()

    typer.echo(f"Completed schema migration for database {app_db_name}")


if __name__ == "__main__":
    app()
