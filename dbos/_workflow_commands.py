import time
from concurrent.futures import ThreadPoolExecutor
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
    # TEMPORARY [gc-timing]: phase timings for retention benchmarking. Remove.
    t_total = time.monotonic()
    # Before the status sweep: afterwards the index-min this reads has to walk
    # the dead prefix the sweep just created, which grows every round.
    t_cutoff = time.monotonic()
    payload_cutoff = dbos._sys_db._payload_retention_cutoff()
    dbos.logger.warning(
        f"[gc-timing] payload cutoff (pre-sweep): {time.monotonic() - t_cutoff:.3f}s"
    )

    # The two sweeps touch disjoint tables, and the payload cutoff was read
    # before either ran, so neither depends on the other. Sequentially they sum;
    # concurrently the round costs the slower of the two, which is the status
    # sweep -- workflow_status carries thirteen indexes to the payload tables' two.
    def status_sweep() -> None:
        t_status = time.monotonic()
        cutoff = dbos._sys_db.garbage_collect(
            cutoff_epoch_timestamp_ms=cutoff_epoch_timestamp_ms,
            rows_threshold=rows_threshold,
            batch_size=batch_size,
        )
        dbos.logger.warning(
            f"[gc-timing] status total: {time.monotonic() - t_status:.3f}s"
        )
        # The application database is deprecated: only pay for its cleanup when
        # one exists. It needs the status sweep's cutoff, so it stays on this side.
        if cutoff is not None and dbos._app_db is not None:
            t_appdb = time.monotonic()
            retained_ids = dbos._sys_db.list_retained_workflow_ids(cutoff)
            dbos._app_db.garbage_collect(cutoff, retained_ids, batch_size=batch_size)
            dbos.logger.warning(
                f"[gc-timing] appdb: {time.monotonic() - t_appdb:.3f}s, "
                f"{len(retained_ids)} retained ids"
            )

    def payload_sweep() -> None:
        t_payload = time.monotonic()
        inputs_deleted, outputs_deleted, lag_ms = dbos._sys_db.garbage_collect_payloads(
            batch_size=batch_size or DEFAULT_GC_BATCH_SIZE, cutoff=payload_cutoff
        )
        dbos.logger.warning(
            f"[gc-timing] payload total: {time.monotonic() - t_payload:.3f}s, "
            f"{inputs_deleted} inputs, {outputs_deleted} outputs"
        )

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="dbos-gc") as executor:
        futures = [executor.submit(status_sweep), executor.submit(payload_sweep)]
        # Collect both before raising: one sweep failing must not leave the other
        # unobserved, and the pool's shutdown waits for both either way.
        errors = [f.exception() for f in futures]
    for error in errors:
        if error is not None:
            raise error
    dbos.logger.warning(f"[gc-timing] GC TOTAL: {time.monotonic() - t_total:.3f}s")


def global_timeout(dbos: "DBOS", cutoff_epoch_timestamp_ms: int) -> None:
    # IDs only, so a bulk timeout does not deserialize every row's inputs and outputs.
    for workflow_id in dbos._sys_db.list_timed_out_workflow_ids(
        cutoff_epoch_timestamp_ms
    ):
        dbos.cancel_workflow(workflow_id)
