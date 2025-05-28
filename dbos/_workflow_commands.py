import uuid
from typing import List, Optional

from dbos._context import get_local_dbos_context

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

    infos: List[WorkflowStatus] = sys_db.get_workflows(input)

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

    infos: List[WorkflowStatus] = sys_db.get_queued_workflows(input)
    return infos


def get_workflow(sys_db: SystemDatabase, workflow_id: str) -> Optional[WorkflowStatus]:
    input = GetWorkflowsInput()
    input.workflow_ids = [workflow_id]

    infos: List[WorkflowStatus] = sys_db.get_workflows(input)
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
    *,
    application_version: Optional[str],
) -> str:

    ctx = get_local_dbos_context()
    if ctx is not None and len(ctx.id_assigned_for_next_workflow) > 0:
        forked_workflow_id = ctx.id_assigned_for_next_workflow
        ctx.id_assigned_for_next_workflow = ""
    else:
        forked_workflow_id = str(uuid.uuid4())
    app_db.clone_workflow_transactions(workflow_id, forked_workflow_id, start_step)
    sys_db.fork_workflow(
        workflow_id,
        forked_workflow_id,
        start_step,
        application_version=application_version,
    )
    return forked_workflow_id
