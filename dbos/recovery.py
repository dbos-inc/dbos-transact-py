import os
import threading
import time
import traceback
from typing import TYPE_CHECKING, Any, List

from dbos.context import SetWorkflowRecovery
from dbos.core import _execute_workflow_id
from dbos.error import DBOSWorkflowFunctionNotFoundError

if TYPE_CHECKING:
    from dbos.dbos import DBOS, WorkflowHandle


def _startup_recovery_thread(dbos: "DBOS", workflow_ids: List[str]) -> None:
    """Attempt to recover local pending workflows on startup using a background thread."""
    stop_event = threading.Event()
    dbos.stop_events.append(stop_event)
    while not stop_event.is_set() and len(workflow_ids) > 0:
        try:
            for workflowID in list(workflow_ids):
                with SetWorkflowRecovery():
                    _execute_workflow_id(dbos, workflowID)
                workflow_ids.remove(workflowID)
        except DBOSWorkflowFunctionNotFoundError:
            time.sleep(1)
        except Exception as e:
            dbos.logger.error(
                f"Exception encountered when recovering workflows: {traceback.format_exc()}"
            )
            raise e


def _recover_pending_workflows(
    dbos: "DBOS", executor_ids: List[str] = ["local"]
) -> List["WorkflowHandle[Any]"]:
    workflow_handles: List["WorkflowHandle[Any]"] = []
    for executor_id in executor_ids:
        if executor_id == "local" and os.environ.get("DBOS__VMID"):
            dbos.logger.debug(
                f"Skip local recovery because it's running in a VM: {os.environ.get('DBOS__VMID')}"
            )
        dbos.logger.debug(f"Recovering pending workflows for executor: {executor_id}")
        workflow_ids = dbos._sys_db.get_pending_workflows(executor_id)
        dbos.logger.debug(f"Pending workflows: {workflow_ids}")

        for workflowID in workflow_ids:
            with SetWorkflowRecovery():
                handle = _execute_workflow_id(dbos, workflowID)
            workflow_handles.append(handle)

    dbos.logger.info("Recovered pending workflows")
    return workflow_handles
