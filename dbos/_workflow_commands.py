import uuid
from typing import List, Optional

from dbos._error import DBOSException

from ._app_db import ApplicationDatabase
from ._sys_db import (
    GetQueuedWorkflowsInput,
    GetWorkflowsInput,
    StepInfo,
    SystemDatabase,
    WorkflowStatus,
)


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
    workflow_id_prefix: Optional[str] = None,
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
    input.workflow_id_prefix = workflow_id_prefix

    infos: List[WorkflowStatus] = sys_db.get_workflows(input, request)

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

    infos: List[WorkflowStatus] = sys_db.get_queued_workflows(input, request)
    return infos


def get_workflow(
    sys_db: SystemDatabase, workflow_id: str, get_request: bool
) -> Optional[WorkflowStatus]:
    input = GetWorkflowsInput()
    input.workflow_ids = [workflow_id]

    infos: List[WorkflowStatus] = sys_db.get_workflows(input, get_request)
    if not infos:
        return None

    return infos[0]


def list_workflow_steps(
    sys_db: SystemDatabase, app_db: ApplicationDatabase, workflow_id: str
) -> List[StepInfo]:
    steps = sys_db.get_workflow_steps(workflow_id)
    transactions = app_db.get_transactions(workflow_id)
    merged_steps = steps + transactions
    merged_steps.sort(key=lambda step: step["function_id"])
    return merged_steps


def fork_workflow(
    sys_db: SystemDatabase,
    app_db: ApplicationDatabase,
    workflow_id: str,
    start_step: int,
) -> str:
    def get_max_function_id(workflow_uuid: str) -> int:
        max_transactions = app_db.get_max_function_id(workflow_uuid) or 0
        max_operations = sys_db.get_max_function_id(workflow_uuid) or 0
        return max(max_transactions, max_operations)

    max_function_id = get_max_function_id(workflow_id)
    if max_function_id > 0 and start_step > max_function_id:
        raise DBOSException(
            f"Cannot fork workflow {workflow_id} from step {start_step}. The workflow has {max_function_id} steps."
        )
    forked_workflow_id = str(uuid.uuid4())
    app_db.clone_workflow_transactions(workflow_id, forked_workflow_id, start_step)
    sys_db.fork_workflow(workflow_id, forked_workflow_id, start_step)
    return forked_workflow_id
