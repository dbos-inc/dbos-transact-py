import threading
import time
from typing import TYPE_CHECKING, Any, List

from dbos._context import UseLogAttributes
from dbos._utils import GlobalParams

from ._core import execute_workflow_by_id
from ._error import DBOSWorkflowFunctionNotFoundError
from ._sys_db import GetPendingWorkflowsOutput

if TYPE_CHECKING:
    from ._dbos import DBOS, WorkflowHandle


def _recover_workflow(
    dbos: "DBOS", workflow: GetPendingWorkflowsOutput
) -> "WorkflowHandle[Any]":
    if workflow.queue_name:
        cleared = dbos._sys_db.clear_queue_assignment(workflow.workflow_uuid)
        if cleared:
            return dbos.retrieve_workflow(workflow.workflow_uuid)
    return execute_workflow_by_id(dbos, workflow.workflow_uuid)


def startup_recovery_thread(
    dbos: "DBOS", pending_workflows: List[GetPendingWorkflowsOutput]
) -> None:
    """Attempt to recover local pending workflows on startup using a background thread."""
    stop_event = threading.Event()
    dbos.background_thread_stop_events.append(stop_event)
    while not stop_event.is_set() and len(pending_workflows) > 0:
        for pending_workflow in list(pending_workflows):
            try:
                _recover_workflow(dbos, pending_workflow)
                pending_workflows.remove(pending_workflow)
            except DBOSWorkflowFunctionNotFoundError:
                time.sleep(1)
            except Exception as e:
                with UseLogAttributes(workflow_id=pending_workflow.workflow_uuid):
                    dbos.logger.error(
                        f"Exception encountered when recovering workflow {pending_workflow.workflow_uuid}:",
                        exc_info=e,
                    )
                raise


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
                handle = _recover_workflow(dbos, pending_workflow)
                workflow_handles.append(handle)
            except Exception as e:
                with UseLogAttributes(workflow_id=pending_workflow.workflow_uuid):
                    dbos.logger.error(
                        f"Exception encountered when recovering workflow {pending_workflow.workflow_uuid}:",
                        exc_info=e,
                    )
                raise
        dbos.logger.info(
            f"Recovering {len(pending_workflows)} workflows for executor {executor_id} from version {GlobalParams.app_version}"
        )
    return workflow_handles
