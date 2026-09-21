import asyncio
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Optional, Sequence, Union

from dbos._context import get_local_dbos_context
from dbos._datasource import AsyncSQLAlchemyDatasource, SQLAlchemyDatasource
from dbos._error import DBOSException, DBOSNonExistentWorkflowError
from dbos._utils import generate_uuid

from ._sys_db import (
    DEFAULT_GC_BATCH_SIZE,
    SystemDatabase,
    WorkflowStatus,
    workflow_is_active,
)

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


def garbage_collect(
    dbos: "DBOS",
    cutoff_epoch_timestamp_ms: Optional[int],
    rows_threshold: Optional[int],
    *,
    batch_size: int = DEFAULT_GC_BATCH_SIZE,
) -> None:
    """Enforce retention across the entire system database."""
    if cutoff_epoch_timestamp_ms is None and rows_threshold is None:
        return
    with dbos._sys_db.retention_lock() as acquired:
        if not acquired:
            dbos.logger.warning(
                "Skipping retention: another round is already running against this "
                "system database."
            )
            return
        cutoff = dbos._sys_db.garbage_collect(
            cutoff_epoch_timestamp_ms=cutoff_epoch_timestamp_ms,
            rows_threshold=rows_threshold,
            batch_size=batch_size,
        )
        if cutoff is None:
            return
        # Strictly after the status sweep: the payload sweep only takes orphans, so
        # this round's are only visible to it once that sweep has committed.
        dbos._sys_db.garbage_collect_payloads(cutoff, batch_size=batch_size)


def global_timeout(dbos: "DBOS", cutoff_epoch_timestamp_ms: int) -> None:
    # IDs only, so a bulk timeout does not deserialize every row's inputs and outputs.
    for workflow_id in dbos._sys_db.list_timed_out_workflow_ids(
        cutoff_epoch_timestamp_ms
    ):
        dbos.cancel_workflow(workflow_id)


Datasource = Union[SQLAlchemyDatasource, AsyncSQLAlchemyDatasource]


def _check_rewindable(sys_db: SystemDatabase, workflow_id: str) -> None:
    """Refuse to touch a datasource's checkpoints for a workflow that is missing or
    still running. The system database rewind repeats this check under its own
    transaction; this one only keeps a running workflow's checkpoints intact."""
    status = sys_db.get_workflow_status(workflow_id)
    if status is None:
        raise DBOSNonExistentWorkflowError("target", workflow_id)
    if workflow_is_active(status["status"]):
        raise DBOSException(
            f"Cannot rewind {workflow_id} ({status['status']}): only a workflow in a "
            "terminal state can be rewound, so cancel it first"
        )


def rewind_workflow(
    sys_db: SystemDatabase,
    datasources: Sequence[Datasource],
    workflow_id: str,
    start_step: int,
    *,
    application_version: Optional[str] = None,
    queue_name: Optional[str] = None,
    queue_partition_key: Optional[str] = None,
    run_coroutine: Optional[Callable[[Coroutine[Any, Any, Any]], Any]] = None,
) -> None:
    """Drop the datasources' checkpoints from start_step on, then rewind the
    workflow in the system database. Best effort: if a step fails the workflow is
    left as it was and the rewind can be retried, which is safe because the
    system database checkpoints are touched last. An async datasource needs
    run_coroutine to bridge to a loop."""
    for ds in datasources:
        if isinstance(ds, AsyncSQLAlchemyDatasource) and run_coroutine is None:
            raise DBOSException(
                "An async datasource cannot be rewound from sync code; "
                "use the async rewind"
            )
    if datasources:
        _check_rewindable(sys_db, workflow_id)
    for ds in datasources:
        if isinstance(ds, AsyncSQLAlchemyDatasource):
            assert run_coroutine is not None
            run_coroutine(ds._delete_checkpoints(workflow_id, start_step))
        else:
            ds._delete_checkpoints(workflow_id, start_step)
    sys_db.rewind_workflow(
        workflow_id,
        start_step,
        application_version=application_version,
        queue_name=queue_name,
        queue_partition_key=queue_partition_key,
    )


async def rewind_workflow_async(
    sys_db: SystemDatabase,
    datasources: Sequence[Datasource],
    workflow_id: str,
    start_step: int,
    *,
    application_version: Optional[str] = None,
    queue_name: Optional[str] = None,
    queue_partition_key: Optional[str] = None,
) -> None:
    """rewind_workflow on the current loop: async datasources are awaited here,
    sync ones and the system database run in a thread."""
    if datasources:
        await asyncio.to_thread(_check_rewindable, sys_db, workflow_id)
    for ds in datasources:
        if isinstance(ds, AsyncSQLAlchemyDatasource):
            await ds._delete_checkpoints(workflow_id, start_step)
        else:
            await asyncio.to_thread(ds._delete_checkpoints, workflow_id, start_step)
    await asyncio.to_thread(
        sys_db.rewind_workflow,
        workflow_id,
        start_step,
        application_version=application_version,
        queue_name=queue_name,
        queue_partition_key=queue_partition_key,
    )
