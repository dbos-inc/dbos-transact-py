import threading
import time
import traceback
from typing import TYPE_CHECKING, List

from dbos.context import SetWorkflowRecovery
from dbos.error import DBOSWorkflowFunctionNotFoundError

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
                f"Exception encountered when recovering workflows: {traceback.format_exc()}"
            )
            raise e
