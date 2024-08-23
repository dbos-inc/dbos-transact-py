import os
import threading
import time
from typing import TYPE_CHECKING, Any, List

from dbos.context import SetWorkflowRecovery
from dbos.error import DBOSWorkflowFunctionNotFoundError
from dbos.workflow import WorkflowHandle

if TYPE_CHECKING:
    from dbos.dbos import DBOS


def startup_recovery_thread(dbos: "DBOS", workflow_ids: List[str]) -> None:
    """
    A background thread that attempts to recover local pending workflows on startup.
    """
    stop_event = threading.Event()
    dbos.stop_events.append(stop_event)
    while not stop_event.is_set() and len(workflow_ids) > 0:
        try:
            for workflowID in list(workflow_ids):
                with SetWorkflowRecovery():
                    dbos.execute_workflow_uuid(workflowID)
                workflow_ids.remove(workflowID)
        except DBOSWorkflowFunctionNotFoundError:
            time.sleep(1)
        except Exception as e:
            dbos.logger.error(
                f"Exception encountered when recovering workflows: {repr(e)}"
            )
            raise e


def recover_pending_workflows(
    dbos: "DBOS", executor_ids: List[str] = ["local"]
) -> List[WorkflowHandle[Any]]:
    workflow_handles: List[WorkflowHandle[Any]] = []
    for executor_id in executor_ids:
        if executor_id == "local" and os.environ.get("DBOS__VMID"):
            dbos.logger.debug(
                f"Skip local recovery because it's running in a VM: {os.environ.get('DBOS__VMID')}"
            )
        dbos.logger.debug(f"Recovering pending workflows for executor: {executor_id}")
        workflow_ids = dbos.sys_db.get_pending_workflows(executor_id)
        dbos.logger.debug(f"Pending workflows: {workflow_ids}")

        for workflowID in workflow_ids:
            with SetWorkflowRecovery():
                handle = dbos.execute_workflow_uuid(workflowID)
            workflow_handles.append(handle)

    dbos.logger.info("Recovered pending workflows")
    return workflow_handles
