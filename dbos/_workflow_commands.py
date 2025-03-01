from typing import List, Optional, cast

import typer

from . import _serialization
from ._dbos_config import ConfigFile
from ._logger import dbos_logger
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
    authenticated_user: Optional[str]
    assumed_role: Optional[str]
    authenticated_roles: Optional[str]  # JSON list of roles.
    input: Optional[_serialization.WorkflowInputs]  # JSON (jsonpickle)
    output: Optional[str] = None  # JSON (jsonpickle)
    request: Optional[str]  # JSON (jsonpickle)
    error: Optional[str] = None  # JSON (jsonpickle)
    created_at: Optional[int]  # Unix epoch timestamp in ms
    updated_at: Optional[int]  # Unix epoch timestamp in ms
    queue_name: Optional[str]
    executor_id: Optional[str]
    app_version: Optional[str]
    app_id: Optional[str]
    recovery_attempts: Optional[int]


def list_workflows(
    sys_db: SystemDatabase,
    *,
    workflow_ids: Optional[List[str]] = None,
    user: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    status: Optional[str] = None,
    request: bool = False,
    app_version: Optional[str] = None,
    name: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[WorkflowInformation]:
    try:
        input = GetWorkflowsInput()
        input.workflow_ids = workflow_ids
        input.authenticated_user = user
        input.start_time = start_time
        input.end_time = end_time
        if status is not None:
            input.status = cast(WorkflowStatuses, status)
        input.application_version = app_version
        input.limit = limit
        input.name = name
        input.offset = offset

        output: GetWorkflowsOutput = sys_db.get_workflows(input)
        infos: List[WorkflowInformation] = []
        for workflow_id in output.workflow_uuids:
            info = get_workflow_info(
                sys_db, workflow_id, request
            )  # Call the method for each ID
            if info is not None:
                infos.append(info)

        return infos
    except Exception as e:
        dbos_logger.error(f"Error listing workflows: {e}")
        return []


def list_queued_workflows(
    config: ConfigFile,
    *,
    limit: Optional[int] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    queue_name: Optional[str] = None,
    status: Optional[str] = None,
    name: Optional[str] = None,
    request: bool = False,
    offset: Optional[int] = None,
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
            "offset": offset,
        }
        output: GetWorkflowsOutput = sys_db.get_queued_workflows(input)
        infos: List[WorkflowInformation] = []
        for workflow_id in output.workflow_uuids:
            info = get_workflow_info(
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
        info = get_workflow_info(sys_db, uuid, request)
        return info
    except Exception as e:
        typer.echo(f"Error getting workflow: {e}")
        return None
    finally:
        if sys_db:
            sys_db.destroy()


def get_workflow_info(
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
    winfo.authenticated_user = info["authenticated_user"]
    winfo.assumed_role = info["assumed_role"]
    winfo.authenticated_roles = info["authenticated_roles"]
    winfo.request = info["request"]
    winfo.created_at = info["created_at"]
    winfo.updated_at = info["updated_at"]
    winfo.queue_name = info["queue_name"]
    winfo.executor_id = info["executor_id"]
    winfo.app_version = info["app_version"]
    winfo.app_id = info["app_id"]
    winfo.recovery_attempts = info["recovery_attempts"]

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
