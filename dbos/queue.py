import threading
import time
import traceback
from typing import TYPE_CHECKING, Optional, TypedDict

from pydantic import BaseModel

from dbos.core import P, R, _execute_workflow_id, _start_workflow

if TYPE_CHECKING:
    from dbos.dbos import DBOS, Workflow, WorkflowHandle


class Limiter(TypedDict):
    max: int
    duration: int


class Queue:
    def __init__(
        self,
        name: str,
        concurrency: Optional[int] = None,
        limiter: Optional[Limiter] = None,
    ) -> None:
        self.name = name
        self.concurrency = concurrency
        self.limiter = limiter
        from dbos.dbos import _get_or_create_dbos_registry

        registry = _get_or_create_dbos_registry()
        registry.queue_info_map[self.name] = self

    def enqueue(
        self, func: "Workflow[P, R]", *args: P.args, **kwargs: P.kwargs
    ) -> "WorkflowHandle[R]":
        from dbos.dbos import _get_dbos_instance

        dbos = _get_dbos_instance()
        return _start_workflow(dbos, func, self.name, False, *args, **kwargs)


def queue_thread(stop_event: threading.Event, dbos: "DBOS") -> None:
    while not stop_event.is_set():
        time.sleep(1)
        for queue_name, queue in dbos._registry.queue_info_map.items():
            try:
                wf_ids = dbos._sys_db.start_queued_workflows(queue)
                for id in wf_ids:
                    _execute_workflow_id(dbos, id)
            except Exception:
                dbos.logger.warning(
                    f"Exception encountered in queue thread: {traceback.format_exc()}"
                )
