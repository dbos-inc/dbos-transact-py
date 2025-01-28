import importlib
import os
import sys
from typing import Any, List, Optional, cast

import typer
from rich import print

from dbos import DBOS

from . import _serialization, load_config
from ._core import execute_workflow_by_id
from ._dbos_config import ConfigFile, _is_valid_app_name
from ._sys_db import (
    GetWorkflowsInput,
    GetWorkflowsOutput,
    SystemDatabase,
    WorkflowStatuses,
    WorkflowStatusInternal,
    WorkflowStatusString,
)


class WorkflowInformation:
    workflowUUID: str
    status: WorkflowStatuses
    workflowName: str
    workflowClassName: Optional[str]
    workflowConfigName: Optional[str]
    input: Optional[_serialization.WorkflowInputs]  # JSON (jsonpickle)
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)
    executor_id: Optional[str]
    app_version: Optional[str]
    app_id: Optional[str]
    request: Optional[str]  # JSON (jsonpickle)
    recovery_attempts: Optional[int]
    authenticated_user: Optional[str]
    assumed_role: Optional[str]
    authenticated_roles: Optional[str]  # JSON list of roles.
    queue_name: Optional[str]


def _list_workflows(
    config: ConfigFile,
    li: int,
    user: Optional[str],
    starttime: Optional[str],
    endtime: Optional[str],
    status: Optional[str],
    request: bool,
    appversion: Optional[str],
) -> List[WorkflowInformation]:

    sys_db = None

    try:
        sys_db = SystemDatabase(config)

        input = GetWorkflowsInput()
        input.authenticated_user = user
        input.start_time = starttime
        input.end_time = endtime
        if status is not None:
            input.status = cast(WorkflowStatuses, status)
        input.application_version = appversion
        input.limit = li

        output: GetWorkflowsOutput = sys_db.get_workflows(input)

        infos: List[WorkflowInformation] = []

        if output.workflow_uuids is None:
            typer.echo("No workflows found")
            return {}

        for workflow_id in output.workflow_uuids:
            info = _get_workflow_info(
                sys_db, workflow_id, request
            )  # Call the method for each ID

            if info is not None:
                infos.append(info)

        return infos
    except Exception as e:
        typer.echo(f"Error listing workflows: {e}")
        return []
    finally:
        if sys_db:
            sys_db.destroy()


def _get_workflow(
    config: ConfigFile, uuid: str, request: bool
) -> Optional[WorkflowInformation]:
    sys_db = None

    try:
        sys_db = SystemDatabase(config)

        info = _get_workflow_info(sys_db, uuid, request)
        return info

    except Exception as e:
        typer.echo(f"Error getting workflow: {e}")
        return None
    finally:
        if sys_db:
            sys_db.destroy()


def _cancel_workflow(config: ConfigFile, uuid: str) -> None:
    # config = load_config()
    sys_db = None

    try:
        sys_db = SystemDatabase(config)
        sys_db.set_workflow_status(uuid, WorkflowStatusString.CANCELLED, False)
        return

    except Exception as e:
        typer.echo(f"Failed to connect to DBOS system database: {e}")
        return None
    finally:
        if sys_db:
            sys_db.destroy()


def _get_workflow_info(
    sys_db: SystemDatabase, workflowUUID: str, getRequest: bool
) -> Optional[WorkflowInformation]:

    info = sys_db.get_workflow_status(workflowUUID)
    if info is None:
        return None

    winfo = WorkflowInformation()

    winfo.workflowUUID = workflowUUID
    winfo.status = info["status"]
    winfo.workflowName = info["name"]
    winfo.workflowClassName = info["class_name"]
    winfo.workflowConfigName = info["config_name"]
    winfo.executor_id = info["executor_id"]
    winfo.app_version = info["app_version"]
    winfo.app_id = info["app_id"]
    winfo.recovery_attempts = info["recovery_attempts"]
    winfo.authenticated_user = info["authenticated_user"]
    winfo.assumed_role = info["assumed_role"]
    winfo.authenticated_roles = info["authenticated_roles"]
    winfo.queue_name = info["queue_name"]

    # no input field
    input_data = sys_db.get_workflow_inputs(workflowUUID)
    if input_data is not None:
        winfo.input = input_data

    if info.get("status") == "SUCCESS":
        result = sys_db.await_workflow_result(workflowUUID)
        winfo.output = result
    elif info.get("status") == "ERROR":
        try:
            sys_db.await_workflow_result(workflowUUID)
        except Exception as e:
            winfo.error = str(e)

    if not getRequest:
        winfo.request = None

    return winfo
