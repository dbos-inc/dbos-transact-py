import subprocess

import typer

from dbos_transact.dbos_config import load_config

app = typer.Typer()


@app.command()
def start() -> None:
    config = load_config()
    start_commands = config["runtimeConfig"]["start"]
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


if __name__ == "__main__":
    app()
