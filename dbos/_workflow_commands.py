import asyncio
import random
import threading
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Dict,
    Optional,
    Sequence,
    Union,
)

from dbos._context import DBOSContext, take_assigned_workflow_id
from dbos._datasource import AsyncSQLAlchemyDatasource, SQLAlchemyDatasource
from dbos._error import DBOSException, DBOSNonExistentWorkflowError
from dbos._utils import generate_uuid

from ._sys_db import SystemDatabase, WorkflowStatus, workflow_is_active

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
        shard_key=sys_db._key_hint(workflow_id),
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

    assigned = take_assigned_workflow_id()
    forked_workflow_id = (
        assigned.workflow_id if assigned is not None else generate_uuid()
    )
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
    dbos._sys_db.delete_workflows(workflow_ids, delete_children=delete_children)


# Most workflows one sweep transaction cancels; a full batch sweeps again at once.
_SWEEP_BATCH_SIZE = 1000

# How often one of an application's processes runs its background jobs.
_BACKGROUND_JOBS_INTERVAL_SEC = 1.0

BACKGROUND_JOBS_THREAD_NAME = "dbos-background-jobs"


def _run_background_jobs(
    stop_event: threading.Event,
    dbos: "DBOS",
    lock_name: str,
    interval_sec: float,
    jobs: Dict[str, Callable[[], None]],
) -> None:
    """Run jobs once per interval across all of this application's processes: whichever
    takes their lock runs them, holding the lock for the rest of the interval."""
    while not stop_event.is_set():
        started = time.monotonic()
        held = False
        try:
            with dbos._sys_db.background_job_lock(lock_name) as acquired:
                if acquired:
                    for job_name, job in jobs.items():
                        try:
                            job()
                        except Exception as e:
                            dbos.logger.warning(
                                f"Exception in background job {job_name}: {e}"
                            )
                    stop_event.wait(
                        max(0.0, interval_sec - (time.monotonic() - started))
                    )
                    held = True
        except Exception as e:
            dbos.logger.warning(f"Exception taking the {lock_name} lock: {e}")
        # The holder tries again at once and so keeps the jobs; the rest poll for them.
        wait = 0.0 if held else interval_sec * random.uniform(0.9, 1.1)
        if stop_event.wait(timeout=wait):
            break


def background_jobs_thread(stop_event: threading.Event, dbos: "DBOS") -> None:
    """Enqueue DELAYED workflows whose delay has expired, and cancel this application's
    active workflows once their deadline passes."""

    def sweep() -> None:
        while not stop_event.is_set():
            cancelled = dbos._sys_db.cancel_timed_out_workflows(_SWEEP_BATCH_SIZE)
            for workflow_id in cancelled:
                dbos.logger.debug(f"Cancelled workflow {workflow_id}: timed out")
            if len(cancelled) < _SWEEP_BATCH_SIZE:
                break

    _run_background_jobs(
        stop_event,
        dbos,
        "background_jobs",
        _BACKGROUND_JOBS_INTERVAL_SEC,
        {
            "delayed_workflows": lambda: dbos._sys_db.transition_delayed_workflows(),
            "workflow_timeout": sweep,
        },
    )


Datasource = Union[SQLAlchemyDatasource, AsyncSQLAlchemyDatasource]


def _check_rewindable(
    sys_db: SystemDatabase, workflow_id: str, start_step: int
) -> None:
    """Refuse to touch a datasource's checkpoints for a workflow that is missing or
    still running. The system database rewind repeats this check under its own
    transaction; this one only keeps a running workflow's checkpoints intact."""
    if start_step < 1:
        raise ValueError(f"start_step must be >= 1, got {start_step}")
    status = sys_db.get_workflow_status(workflow_id)
    if status is None:
        raise DBOSNonExistentWorkflowError("target", workflow_id)
    if workflow_is_active(status["status"]):
        raise DBOSException(
            f"Cannot rewind {workflow_id} ({status['status']}): only a workflow in a "
            "terminal state can be rewound, so cancel it first"
        )


def delete_completed_datasource_checkpoints(dbos: "DBOS", ctx: DBOSContext) -> None:
    """Drop a finishing sync workflow's datasource checkpoints while it still owns the workflow.
    Best effort: a leftover checkpoint is harmless, so failures only warn."""
    for ds in ctx.used_datasources:
        if isinstance(ds, AsyncSQLAlchemyDatasource):
            continue  # Its connections belong to an event loop this workflow does not run on.
        try:
            # With no token nobody matches, so the delete rolls back.
            owned = ds._delete_checkpoints_if_owner(
                ctx.workflow_id, ctx.owner_xid or ""
            )
        except Exception as e:
            dbos.logger.warning(
                f"Failed to delete datasource checkpoints of workflow {ctx.workflow_id}: {e}"
            )
            continue
        if not owned:
            return  # Another execution owns the workflow now, and its checkpoints are its own.


async def delete_completed_datasource_checkpoints_async(
    dbos: "DBOS", ctx: DBOSContext
) -> None:
    """Async version of delete_completed_datasource_checkpoints, run on the workflow's own loop."""
    for ds in ctx.used_datasources:
        try:
            if isinstance(ds, AsyncSQLAlchemyDatasource):
                owned = await ds._delete_checkpoints_if_owner(
                    ctx.workflow_id, ctx.owner_xid or ""
                )
            else:
                owned = await asyncio.to_thread(
                    ds._delete_checkpoints_if_owner,
                    ctx.workflow_id,
                    ctx.owner_xid or "",
                )
        except Exception as e:
            dbos.logger.warning(
                f"Failed to delete datasource checkpoints of workflow {ctx.workflow_id}: {e}"
            )
            continue
        if not owned:
            return


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
        _check_rewindable(sys_db, workflow_id, start_step)
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
