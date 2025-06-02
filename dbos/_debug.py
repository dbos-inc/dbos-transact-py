import re
import runpy
import sys
from pathlib import Path
from typing import Union

from fastapi_cli.discover import get_module_data_from_path

from dbos import DBOS


class PythonModule:
    def __init__(self, module_name: str):
        self.module_name = module_name


def debug_workflow(workflow_id: str, entrypoint: Union[str, PythonModule]) -> None:
    if isinstance(entrypoint, str):
        # ensure the entrypoint parent directory is in sys.path
        parent = str(Path(entrypoint).parent)
        if parent not in sys.path:
            sys.path.insert(0, parent)
        runpy.run_path(entrypoint)
    elif isinstance(entrypoint, PythonModule):
        runpy.run_module(entrypoint.module_name)
    else:
        raise ValueError("Invalid entrypoint type. Must be a string or PythonModule.")

    DBOS.logger.info(f"Debugging workflow {workflow_id}...")
    DBOS.launch(debug_mode=True)
    handle = DBOS._execute_workflow_id(workflow_id)
    handle.get_result()
    DBOS.logger.info("Workflow Debugging complete. Exiting process.")


def parse_start_command(command: str) -> Union[str, PythonModule]:
    match = re.match(r"fastapi\s+run\s+(\.?[\w/]+\.py)", command)
    if match:
        # Mirror the logic in fastapi's run command by converting the path argument to a module
        mod_data = get_module_data_from_path(Path(match.group(1)))
        sys.path.insert(0, str(mod_data.extra_sys_path))
        return PythonModule(mod_data.module_import_str)
    match = re.match(r"python3?\s+(\.?[\w/]+\.py)", command)
    if match:
        return match.group(1)
    match = re.match(r"python3?\s+-m\s+([\w\.]+)", command)
    if match:
        return PythonModule(match.group(1))
    raise ValueError(
        "Invalid command format. Must be 'fastapi run <script>' or 'python <script>' or 'python -m <module>'"
    )
