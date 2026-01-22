from datetime import datetime
from typing import TYPE_CHECKING, Optional

from dbos._context import get_local_dbos_context
from dbos._utils import generate_uuid

from ._sys_db import SystemDatabase, WorkflowStatus, WorkflowStatusString

if TYPE_CHECKING:
    from ._dbos import DBOS


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


def delete_workflow(dbos: "DBOS", workflow_id: str, *, delete_children: bool) -> None:
    """Delete a workflow and all its associated data.

    If delete_children is True, also deletes all child workflows recursively.
    """
    workflow_ids = [workflow_id]
    if delete_children:
        workflow_ids.extend(dbos._sys_db.get_workflow_children(workflow_id))
    dbos._sys_db.delete_workflows(workflow_ids)
    if dbos._app_db:
        dbos._app_db.delete_transaction_outputs(workflow_ids)


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
