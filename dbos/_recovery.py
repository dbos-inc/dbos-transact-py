import threading
from typing import TYPE_CHECKING, Any, List

from dbos._context import UseLogAttributes
from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams

from ._core import WorkflowHandlePolling
from ._sys_db import GetPendingWorkflowsOutput

if TYPE_CHECKING:
    from ._dbos import DBOS, WorkflowHandle


def _recover_workflow(
    dbos: "DBOS", workflow: GetPendingWorkflowsOutput, executor_ids: List[str]
) -> "WorkflowHandle[Any]":
    """Make a pending workflow eligible for execution again, and return a handle to it.

    Recovery re-enqueues rather than executing directly, so that every recovered
    workflow starts through the queue's atomic ENQUEUED->PENDING dequeue. That
    handoff admits exactly one runner, which makes duplicate recovery requests
    idempotent.
    """
    dbos._sys_db.reenqueue_for_recovery(
        workflow.workflow_id, executor_ids, INTERNAL_QUEUE_NAME
    )
    return WorkflowHandlePolling(workflow.workflow_id, dbos)


def startup_recovery_thread(
    dbos: "DBOS", pending_workflows: List[GetPendingWorkflowsOutput]
) -> None:
    """Attempt to recover local pending workflows on startup using a background thread."""
    stop_event = threading.Event()
    dbos.background_thread_stop_events.append(stop_event)
    executor_ids = [GlobalParams.executor_id]
    while not stop_event.is_set() and len(pending_workflows) > 0:
        for pending_workflow in list(pending_workflows):
            try:
                _recover_workflow(dbos, pending_workflow, executor_ids)
                pending_workflows.remove(pending_workflow)
            except Exception as e:
                with UseLogAttributes(workflow_id=pending_workflow.workflow_id):
                    dbos.logger.error(
                        f"Exception encountered when recovering workflow {pending_workflow.workflow_id}:",
                        exc_info=e,
                    )
                pending_workflows.remove(pending_workflow)


def recover_pending_workflows(
    dbos: "DBOS", executor_ids: List[str] = ["local"]
) -> List["WorkflowHandle[Any]"]:
    """Attempt to recover pending workflows for a list of specific executors and return workflow handles for them."""
    workflow_handles: List["WorkflowHandle[Any]"] = []
    for executor_id in executor_ids:
        pending_workflows = dbos._sys_db.get_pending_workflows(
            executor_id, GlobalParams.app_version
        )
        for pending_workflow in pending_workflows:
            try:
                handle = _recover_workflow(dbos, pending_workflow, executor_ids)
                workflow_handles.append(handle)
            except Exception as e:
                with UseLogAttributes(workflow_id=pending_workflow.workflow_id):
                    dbos.logger.error(
                        f"Exception encountered when recovering workflow {pending_workflow.workflow_id}:",
                        exc_info=e,
                    )
        dbos.logger.info(
            f"Recovering {len(pending_workflows)} workflows for executor {executor_id} from version {GlobalParams.app_version}"
        )
    return workflow_handles
