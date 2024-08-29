import os
import platform
import re
import shutil
import signal
import subprocess
import time
import typing
from os import path
from typing import Any

import mako
import tomlkit
import typer
from rich import print
from rich.prompt import Confirm, Prompt
from typing_extensions import Annotated

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


def get_template_directory() -> str:
    import dbos

    package_dir = path.abspath(path.dirname(dbos.__file__))
    return path.join(package_dir, "templates")


def copy_template_dir(src_dir: str, dst_dir: str, ctx: dict[str, str]):

    for root, dirs, files in os.walk(src_dir, topdown=True):
        dirs[:] = [d for d in dirs if d != "$package"]

        dst_root = path.join(dst_dir, path.relpath(root, src_dir))
        if len(dirs) == 0:
            os.makedirs(dst_root, exist_ok=True)
        else:
            for dir in dirs:
                os.makedirs(path.join(dst_root, dir), exist_ok=True)

        for file in files:
            src = path.join(root, file)
            base, ext = path.splitext(file)

            dst = path.join(dst_root, base if ext == ".dbos" else file)
            if path.exists(dst):
                continue

            if ext == ".dbos":
                makotmp = mako.template.Template(filename=src)
                try:
                    output = makotmp.render(**ctx)
                    with open(dst, "w") as f:
                        f.write(output)
                except:
                    print(src, mako.exceptions.text_error_template().render())
            else:
                shutil.copy(src, dst)


def copy_template(src_dir: str, dst_dir: str, project_name: str):
    package_name = project_name.replace("-", "_")
    db_name = package_name if not package_name[0].isdigit() else f"_{package_name}"
    ctx = {
        "project_name": project_name,
        "package_name": package_name,
        "db_name": db_name,
        "PGPASSWORD": "${PGPASSWORD}",
    }

    copy_template_dir(src_dir, dst_dir, ctx)
    copy_template_dir(
        path.join(src_dir, "$package"), path.join(dst_dir, package_name), ctx
    )


def get_project_name() -> typing.Union[str, None]:
    name = None
    try:
        with open("pyproject.toml", "rb") as file:
            pyproj = typing.cast(dict[str, Any], tomlkit.load(file))
            name = typing.cast(str, pyproj["project"]["name"])
    except:
        pass

    if name == None:
        try:
            _, parent = path.split(path.abspath("."))
            name = parent
        except:
            pass

    return name


def is_valid_app_name(name: str) -> bool:
    name_len = len(name)
    if name_len < 3 or name_len > 30:
        return False
    match = re.match("^[a-z0-9-_]+$", name)
    return True if match != None else False


@app.command()
def init(name: Annotated[typing.Optional[str], typer.Argument()] = None) -> None:
    try:
        project_name = name
        if project_name == None:
            project_name = typing.cast(
                str, typer.prompt("What is your project's name?", get_project_name())
            )

        if project_name == None:
            raise Exception(f"Project name could not be determined")

        project_name = typing.cast(str, project_name)
        if not is_valid_app_name(project_name):
            raise Exception(f"{project_name} is an invalid DBOS app name")

        template_dir = get_template_directory()
        templates = [x.name for x in os.scandir(template_dir) if x.is_dir()]
        templates_len = len(templates)
        if templates_len == 0:
            raise Exception(f"no DBOS templates found in {template_dir} ")
        elif templates_len == 1:
            template = templates[0]
        else:
            template = Prompt.ask(
                "Which project template do you want to use?", choices=templates
            )

        chosen_template = path.join(template_dir, template)
        copy_template(chosen_template, project_name)
    except Exception as e:
        print(f"[red]{e}[/red]")


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
