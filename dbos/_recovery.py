import os
import threading
import time
import traceback
from typing import TYPE_CHECKING, Any, List

from dbos._utils import GlobalParams

from ._core import execute_workflow_by_id
from ._error import DBOSWorkflowFunctionNotFoundError
from ._sys_db import GetPendingWorkflowsOutput

if TYPE_CHECKING:
    from ._dbos import DBOS, WorkflowHandle


def startup_recovery_thread(
    dbos: "DBOS", pending_workflows: List[GetPendingWorkflowsOutput]
) -> None:
    """Attempt to recover local pending workflows on startup using a background thread."""
    stop_event = threading.Event()
    dbos.stop_events.append(stop_event)
    while not stop_event.is_set() and len(pending_workflows) > 0:
        try:
            for pending_workflow in list(pending_workflows):
                if (
                    pending_workflow.queue_name
                    and pending_workflow.queue_name != "_dbos_internal_queue"
                ):
                    cleared = dbos._sys_db.clear_queue_assignment(pending_workflow.workflow_uuid)
                    if cleared:
                        continue
                execute_workflow_by_id(dbos, pending_workflow.workflow_uuid)
                pending_workflows.remove(pending_workflow)
        except DBOSWorkflowFunctionNotFoundError:
            time.sleep(1)
        except Exception as e:
            dbos.logger.error(
                f"Exception encountered when recovering workflows: {traceback.format_exc()}"
            )
            raise e


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
            if (
                pending_workflow.queue_name
                and pending_workflow.queue_name != "_dbos_internal_queue"
            ):
                try:
                    cleared = dbos._sys_db.clear_queue_assignment(pending_workflow.workflow_uuid)
                    if cleared:
                        workflow_handles.append(
                            dbos.retrieve_workflow(pending_workflow.workflow_uuid)
                        )
                    else:
                        workflow_handles.append(
                            execute_workflow_by_id(dbos, pending_workflow.workflow_uuid)
                        )
                except Exception as e:
                    dbos.logger.error(e)
            else:
                workflow_handles.append(
                    execute_workflow_by_id(dbos, pending_workflow.workflow_uuid)
                )
        dbos.logger.info(
            f"Recovering {len(pending_workflows)} workflows for executor {executor_id} from version {GlobalParams.app_version}"
        )
    return workflow_handles
