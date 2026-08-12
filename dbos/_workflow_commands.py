from typing import TYPE_CHECKING, Optional

from dbos._context import get_local_dbos_context
from dbos._utils import generate_uuid

from ._sys_db import SystemDatabase, WorkflowStatus

if TYPE_CHECKING:
    from ._dbos import DBOS


def get_workflow(
    sys_db: SystemDatabase,
    workflow_id: str,
    *,
    load_input: bool = True,
    load_output: bool = True,
) -> Optional[WorkflowStatus]:
    infos = sys_db.list_workflows(
        workflow_ids=[workflow_id],
        load_input=load_input,
        load_output=load_output,
    )
    if not infos:
        return None
    return infos[0]


def fork_workflow(
    sys_db: SystemDatabase,
    workflow_id: str,
    start_step: int,
    *,
    application_version: Optional[str],
    queue_name: Optional[str] = None,
    queue_partition_key: Optional[str] = None,
    replacement_children: Optional[dict[str, str]] = None,
    timeout_seconds: Optional[float] = None,
) -> str:
    if timeout_seconds is not None and not timeout_seconds > 0:
        raise Exception(
            f"Invalid workflow timeout {timeout_seconds}. Timeouts must be positive."
        )
    workflow_timeout_ms = (
        int(timeout_seconds * 1000) if timeout_seconds is not None else None
    )

    ctx = get_local_dbos_context()
    if ctx is not None and len(ctx.id_assigned_for_next_workflow) > 0:
        forked_workflow_id = ctx.id_assigned_for_next_workflow
        ctx.id_assigned_for_next_workflow = ""
    else:
        forked_workflow_id = generate_uuid()
    sys_db.fork_workflow(
        [workflow_id],
        [forked_workflow_id],
        [start_step],
        application_version=application_version,
        queue_name=queue_name,
        queue_partition_key=queue_partition_key,
        replacement_children=replacement_children,
        workflow_timeout_ms=workflow_timeout_ms,
    )
    return forked_workflow_id


def delete_workflow(
    dbos: "DBOS", workflow_ids: list[str], *, delete_children: bool
) -> None:
    """Delete workflows and all their associated data.

    If delete_children is True, also deletes all child workflows recursively.
    """
    all_ids = list(workflow_ids)
    if delete_children:
        for wfid in workflow_ids:
            all_ids.extend(dbos._sys_db.get_workflow_children(wfid))
    dbos._sys_db.delete_workflows(all_ids)
    if dbos._app_db:
        dbos._app_db.delete_transaction_outputs(all_ids)


# Default number of rows deleted per garbage collection batch
DEFAULT_GC_BATCH_SIZE = 10_000


def garbage_collect(
    dbos: "DBOS",
    cutoff_epoch_timestamp_ms: Optional[int],
    rows_threshold: Optional[int],
    *,
    batch_size: Optional[int] = DEFAULT_GC_BATCH_SIZE,
) -> None:
    if cutoff_epoch_timestamp_ms is None and rows_threshold is None:
        return
    result = dbos._sys_db.garbage_collect(
        cutoff_epoch_timestamp_ms=cutoff_epoch_timestamp_ms,
        rows_threshold=rows_threshold,
        batch_size=batch_size,
    )
    if result is not None:
        cutoff_epoch_timestamp_ms, pending_workflow_ids = result
        if dbos._app_db:
            dbos._app_db.garbage_collect(
                cutoff_epoch_timestamp_ms, pending_workflow_ids, batch_size=batch_size
            )


def global_timeout(dbos: "DBOS", cutoff_epoch_timestamp_ms: int) -> None:
    # IDs only, so a bulk timeout does not deserialize every row's inputs and outputs.
    for workflow_id in dbos._sys_db.list_timed_out_workflow_ids(
        cutoff_epoch_timestamp_ms
    ):
        dbos.cancel_workflow(workflow_id)
