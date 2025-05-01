import os
import shutil
import typing
from os import path
from typing import Any

import tomlkit
from rich import print

from dbos._dbos_config import _app_name_to_db_name


def get_templates_directory() -> str:
    import dbos

    package_dir = path.abspath(path.dirname(dbos.__file__))
    return path.join(package_dir, "_templates")


def _copy_dbos_template(src: str, dst: str, ctx: dict[str, str]) -> None:
    with open(src, "r") as f:
        content = f.read()

    for key, value in ctx.items():
        content = content.replace(f"${{{key}}}", value)

    with open(dst, "w") as f:
        f.write(content)


def _copy_template_dir(src_dir: str, dst_dir: str, ctx: dict[str, str]) -> None:

    for root, dirs, files in os.walk(src_dir, topdown=True):
        dirs[:] = [d for d in dirs if d != "__package"]

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
                print(f"[yellow]File {dst} already exists, skipping[/yellow]")
                continue

            if ext == ".dbos":
                _copy_dbos_template(src, dst, ctx)
            else:
                shutil.copy(src, dst)


def copy_template(src_dir: str, project_name: str, config_mode: bool) -> None:

    dst_dir = path.abspath(".")

    package_name = project_name.replace("-", "_")
    default_migration_section = """database:
  migrate:
    - alembic upgrade head
"""
    ctx = {
        "project_name": project_name,
        "default_db_name": _app_name_to_db_name(project_name),
        "package_name": package_name,
        "start_command": f"python3 -m {package_name}.main",
        "migration_section": default_migration_section,
    }

    if config_mode:
        ctx["start_command"] = "python3 main.py"
        ctx["migration_section"] = ""
        _copy_dbos_template(
            os.path.join(src_dir, "dbos-config.yaml.dbos"),
            os.path.join(dst_dir, "dbos-config.yaml"),
            ctx,
        )
    else:
        _copy_template_dir(src_dir, dst_dir, ctx)
        _copy_template_dir(
            path.join(src_dir, "__package"), path.join(dst_dir, package_name), ctx
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
