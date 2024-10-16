import threading
import traceback
from typing import TYPE_CHECKING

from dbos._core.workflow import execute_workflow_id

if TYPE_CHECKING:
    from dbos.dbos import DBOS


def queue_thread(stop_event: threading.Event, dbos: "DBOS") -> None:
    while not stop_event.is_set():
        if stop_event.wait(timeout=1):
            return
        for _, queue in dbos._registry.queue_info_map.items():
            try:
                wf_ids = dbos._sys_db.start_queued_workflows_sync(queue)
                for id in wf_ids:
                    execute_workflow_id(dbos, id)
            except Exception:
                dbos.logger.warning(
                    f"Exception encountered in queue thread: {traceback.format_exc()}"
                )
