import threading
from typing import TYPE_CHECKING, List

from dbos._context import UseLogAttributes
from dbos._shard_keys import bind_shard_keys
from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams

from ._sys_db import GetPendingWorkflowsOutput

if TYPE_CHECKING:
    from ._dbos import DBOS


def _recover_workflow(
    dbos: "DBOS", workflow: GetPendingWorkflowsOutput, executor_ids: List[str]
) -> None:
    """Make a pending workflow eligible for execution again.

    Recovery re-enqueues rather than executing directly, so that every recovered
    workflow starts through the queue's atomic ENQUEUED->PENDING dequeue. That
    handoff admits exactly one runner, which makes duplicate recovery requests
    idempotent.
    """
    with bind_shard_keys({workflow.workflow_id: workflow.shard_key}):
        dbos._sys_db.reenqueue_for_recovery(
            workflow.workflow_id, executor_ids, INTERNAL_QUEUE_NAME
        )


def startup_recovery_thread(
    dbos: "DBOS", pending_workflows: List[GetPendingWorkflowsOutput]
) -> None:
    """Attempt to recover local pending workflows on startup using a background thread."""
    stop_event = threading.Event()
    dbos.background_thread_stop_events.append(stop_event)
    executor_ids = [GlobalParams.executor_id]
    for pending_workflow in pending_workflows:
        if stop_event.is_set():
            return
        try:
            _recover_workflow(dbos, pending_workflow, executor_ids)
        except Exception as e:
            with UseLogAttributes(workflow_id=pending_workflow.workflow_id):
                dbos.logger.error(
                    f"Exception encountered when recovering workflow {pending_workflow.workflow_id}:",
                    exc_info=e,
                )


def recover_executor_workflows(
    dbos: "DBOS", executor_ids: List[str]
) -> List[GetPendingWorkflowsOutput]:
    """Attempt to recover pending workflows for a list of specific executors and return the workflows recovered."""
    recovered: List[GetPendingWorkflowsOutput] = []
    for executor_id in executor_ids:
        pending_workflows = dbos._sys_db.get_pending_workflows(
            executor_id, GlobalParams.app_version
        )
        for pending_workflow in pending_workflows:
            try:
                _recover_workflow(dbos, pending_workflow, executor_ids)
                recovered.append(pending_workflow)
            except Exception as e:
                with UseLogAttributes(workflow_id=pending_workflow.workflow_id):
                    dbos.logger.error(
                        f"Exception encountered when recovering workflow {pending_workflow.workflow_id}:",
                        exc_info=e,
                    )
        dbos.logger.info(
            f"Recovering {len(pending_workflows)} workflows for executor {executor_id} from version {GlobalParams.app_version}"
        )
    return recovered
