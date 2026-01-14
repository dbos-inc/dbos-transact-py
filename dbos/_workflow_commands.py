from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Union

from dbos._context import get_local_dbos_context
from dbos._utils import generate_uuid

from ._sys_db import SystemDatabase, WorkflowStatus, WorkflowStatusString

if TYPE_CHECKING:
    from ._dbos import DBOS


def list_workflows(
    sys_db: SystemDatabase,
    *,
    workflow_ids: Optional[List[str]] = None,
    status: Optional[Union[str, List[str]]] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    name: Optional[str] = None,
    app_version: Optional[str] = None,
    forked_from: Optional[str] = None,
    user: Optional[str] = None,
    queue_name: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    sort_desc: bool = False,
    workflow_id_prefix: Optional[str] = None,
    load_input: bool = True,
    load_output: bool = True,
    executor_id: Optional[str] = None,
) -> List[WorkflowStatus]:
    return sys_db.list_workflows(
        workflow_ids=workflow_ids,
        status=status if status is None or isinstance(status, list) else [status],
        start_time=start_time,
        end_time=end_time,
        name=name,
        app_version=app_version,
        forked_from=forked_from,
        user=user,
        queue_name=queue_name,
        limit=limit,
        offset=offset,
        sort_desc=sort_desc,
        workflow_id_prefix=workflow_id_prefix,
        load_input=load_input,
        load_output=load_output,
        executor_id=executor_id,
    )


def list_queued_workflows(
    sys_db: SystemDatabase,
    *,
    queue_name: Optional[str] = None,
    status: Optional[Union[str, List[str]]] = None,
    forked_from: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    name: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    sort_desc: bool = False,
    load_input: bool = True,
    executor_id: Optional[str] = None,
) -> List[WorkflowStatus]:
    return sys_db.list_workflows(
        status=status if status is None or isinstance(status, list) else [status],
        start_time=start_time,
        end_time=end_time,
        forked_from=forked_from,
        name=name,
        queue_name=queue_name,
        limit=limit,
        offset=offset,
        sort_desc=sort_desc,
        load_input=load_input,
        load_output=False,
        executor_id=executor_id,
        queues_only=True,
    )


def get_workflow(sys_db: SystemDatabase, workflow_id: str) -> Optional[WorkflowStatus]:
    infos = sys_db.list_workflows(workflow_ids=[workflow_id])
    if not infos:
        return None
    return infos[0]


def fork_workflow(
    sys_db: SystemDatabase,
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
        forked_workflow_id = generate_uuid()
    sys_db.fork_workflow(
        workflow_id,
        forked_workflow_id,
        start_step,
        application_version=application_version,
    )
    return forked_workflow_id


def garbage_collect(
    dbos: "DBOS",
    cutoff_epoch_timestamp_ms: Optional[int],
    rows_threshold: Optional[int],
) -> None:
    if cutoff_epoch_timestamp_ms is None and rows_threshold is None:
        return
    result = dbos._sys_db.garbage_collect(
        cutoff_epoch_timestamp_ms=cutoff_epoch_timestamp_ms,
        rows_threshold=rows_threshold,
    )
    if result is not None:
        cutoff_epoch_timestamp_ms, pending_workflow_ids = result
        if dbos._app_db:
            dbos._app_db.garbage_collect(
                cutoff_epoch_timestamp_ms, pending_workflow_ids
            )


def global_timeout(dbos: "DBOS", cutoff_epoch_timestamp_ms: int) -> None:
    cutoff_iso = datetime.fromtimestamp(cutoff_epoch_timestamp_ms / 1000).isoformat()
    for workflow in dbos.list_workflows(
        status=WorkflowStatusString.PENDING.value, end_time=cutoff_iso
    ):
        dbos.cancel_workflow(workflow.workflow_id)
    for workflow in dbos.list_workflows(
        status=WorkflowStatusString.ENQUEUED.value, end_time=cutoff_iso
    ):
        dbos.cancel_workflow(workflow.workflow_id)
