import json
from typing import Any, List, Optional

from . import _serialization
from ._sys_db import (
    GetQueuedWorkflowsInput,
    GetWorkflowsInput,
    GetWorkflowsOutput,
    StepInfo,
    SystemDatabase,
)


class WorkflowStatus:
    # The workflow ID
    workflow_id: str
    # The workflow status. Must be one of ENQUEUED, PENDING, SUCCESS, ERROR, CANCELLED, or RETRIES_EXCEEDED
    status: str
    # The name of the workflow function
    name: str
    # The name of the workflow's class, if any
    class_name: Optional[str]
    # The name with which the workflow's class instance was configured, if any
    config_name: Optional[str]
    # The user who ran the workflow, if specified
    authenticated_user: Optional[str]
    # The role with which the workflow ran, if specified
    assumed_role: Optional[str]
    # All roles which the authenticated user could assume
    authenticated_roles: Optional[list[str]]
    # The deserialized workflow input object
    input: Optional[_serialization.WorkflowInputs]
    # The workflow's output, if any
    output: Optional[Any] = None
    # The error the workflow threw, if any
    error: Optional[Exception] = None
    # Workflow start time, as a Unix epoch timestamp in ms
    created_at: Optional[int]
    # Last time the workflow status was updated, as a Unix epoch timestamp in ms
    updated_at: Optional[int]
    # If this workflow was enqueued, on which queue
    queue_name: Optional[str]
    # The executor to most recently executed this workflow
    executor_id: Optional[str]
    # The application version on which this workflow was started
    app_version: Optional[str]
    # The ID of the application executing this workflow
    app_id: Optional[str]
    # The number of times this workflow's execution has been attempted
    recovery_attempts: Optional[int]
    # The HTTP request that triggered the workflow, if known
    request: Optional[str]


def list_workflows(
    sys_db: SystemDatabase,
    *,
    workflow_ids: Optional[List[str]] = None,
    status: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    name: Optional[str] = None,
    app_version: Optional[str] = None,
    user: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    sort_desc: bool = False,
    request: bool = False,
) -> List[WorkflowStatus]:
    input = GetWorkflowsInput()
    input.workflow_ids = workflow_ids
    input.authenticated_user = user
    input.start_time = start_time
    input.end_time = end_time
    input.status = status
    input.application_version = app_version
    input.limit = limit
    input.name = name
    input.offset = offset
    input.sort_desc = sort_desc

    output: GetWorkflowsOutput = sys_db.get_workflows(input)
    infos: List[WorkflowStatus] = []
    for workflow_id in output.workflow_uuids:
        info = get_workflow(sys_db, workflow_id, request)  # Call the method for each ID
        if info is not None:
            infos.append(info)
    return infos


def list_queued_workflows(
    sys_db: SystemDatabase,
    *,
    queue_name: Optional[str] = None,
    status: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    name: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    sort_desc: bool = False,
    request: bool = False,
) -> List[WorkflowStatus]:
    input: GetQueuedWorkflowsInput = {
        "queue_name": queue_name,
        "start_time": start_time,
        "end_time": end_time,
        "status": status,
        "limit": limit,
        "name": name,
        "offset": offset,
        "sort_desc": sort_desc,
    }
    output: GetWorkflowsOutput = sys_db.get_queued_workflows(input)
    infos: List[WorkflowStatus] = []
    for workflow_id in output.workflow_uuids:
        info = get_workflow(sys_db, workflow_id, request)  # Call the method for each ID
        if info is not None:
            infos.append(info)
    return infos


def get_workflow(
    sys_db: SystemDatabase, workflow_id: str, get_request: bool
) -> Optional[WorkflowStatus]:

    internal_status = sys_db.get_workflow_status(workflow_id)
    if internal_status is None:
        return None

    info = WorkflowStatus()

    info.workflow_id = workflow_id
    info.status = internal_status["status"]
    info.name = internal_status["name"]
    info.class_name = internal_status["class_name"]
    info.config_name = internal_status["config_name"]
    info.authenticated_user = internal_status["authenticated_user"]
    info.assumed_role = internal_status["assumed_role"]
    info.authenticated_roles = (
        json.loads(internal_status["authenticated_roles"])
        if internal_status["authenticated_roles"] is not None
        else None
    )
    info.request = internal_status["request"]
    info.created_at = internal_status["created_at"]
    info.updated_at = internal_status["updated_at"]
    info.queue_name = internal_status["queue_name"]
    info.executor_id = internal_status["executor_id"]
    info.app_version = internal_status["app_version"]
    info.app_id = internal_status["app_id"]
    info.recovery_attempts = internal_status["recovery_attempts"]

    input_data = sys_db.get_workflow_inputs(workflow_id)
    if input_data is not None:
        info.input = input_data

    if internal_status.get("status") == "SUCCESS":
        result = sys_db.await_workflow_result(workflow_id)
        info.output = result
    elif internal_status.get("status") == "ERROR":
        try:
            sys_db.await_workflow_result(workflow_id)
        except Exception as e:
            info.error = e

    if not get_request:
        info.request = None

    return info


def list_workflow_steps(sys_db: SystemDatabase, workflow_id: str) -> List[StepInfo]:
    output = sys_db.get_workflow_steps(workflow_id)
    return output
