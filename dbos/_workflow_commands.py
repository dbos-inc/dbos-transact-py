from typing import List, Optional, cast

import typer

from . import _serialization
from ._dbos_config import ConfigFile
from ._sys_db import (
    GetQueuedWorkflowsInput,
    GetWorkflowsInput,
    GetWorkflowsOutput,
    SystemDatabase,
    WorkflowStatuses,
)


class WorkflowInformation:
    workflowUUID: str
    status: WorkflowStatuses
    workflowName: str
    workflowClassName: Optional[str]
    workflowConfigName: Optional[str]
    input: Optional[_serialization.WorkflowInputs]  # JSON (jsonpickle)
    output: Optional[str] = None  # JSON (jsonpickle)
    error: Optional[str] = None  # JSON (jsonpickle)
    executor_id: Optional[str]
    app_version: Optional[str]
    app_id: Optional[str]
    request: Optional[str]  # JSON (jsonpickle)
    recovery_attempts: Optional[int]
    authenticated_user: Optional[str]
    assumed_role: Optional[str]
    authenticated_roles: Optional[str]  # JSON list of roles.
    queue_name: Optional[str]


def list_workflows(
    config: ConfigFile,
    limit: int,
    user: Optional[str],
    starttime: Optional[str],
    endtime: Optional[str],
    status: Optional[str],
    request: bool,
    appversion: Optional[str],
    name: Optional[str],
) -> List[WorkflowInformation]:
    try:
        sys_db = SystemDatabase(config)

        input = GetWorkflowsInput()
        input.authenticated_user = user
        input.start_time = starttime
        input.end_time = endtime
        if status is not None:
            input.status = cast(WorkflowStatuses, status)
        input.application_version = appversion
        input.limit = limit
        input.name = name

        output: GetWorkflowsOutput = sys_db.get_workflows(input)
        infos: List[WorkflowInformation] = []
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


def list_queued_workflows(
    config: ConfigFile,
    limit: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    queue_name: Optional[str] = None,
    status: Optional[str] = None,
    name: Optional[str] = None,
    request: bool = False,
) -> List[WorkflowInformation]:
    try:
        sys_db = SystemDatabase(config)
        input: GetQueuedWorkflowsInput = {
            "queue_name": queue_name,
            "start_time": start_time,
            "end_time": end_time,
            "status": status,
            "limit": limit,
            "name": name,
        }
        output: GetWorkflowsOutput = sys_db.get_queued_workflows(input)
        infos: List[WorkflowInformation] = []
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


def get_workflow(
    config: ConfigFile, uuid: str, request: bool
) -> Optional[WorkflowInformation]:
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


def cancel_workflow(config: ConfigFile, uuid: str) -> None:
    try:
        sys_db = SystemDatabase(config)
        sys_db.cancel_workflow(uuid)
    except Exception as e:
        typer.echo(f"Failed to connect to DBOS system database: {e}")
        raise e
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
