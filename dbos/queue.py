import threading
import time
from typing import TYPE_CHECKING, Optional

from dbos.core import P, R, _execute_workflow_id, _start_workflow
from dbos.error import DBOSInitializationError

if TYPE_CHECKING:
    from dbos.dbos import DBOS, Workflow, WorkflowHandle


class Queue:
    def __init__(self, name: str, concurrency: Optional[int] = None) -> None:
        self.name = name
        self.concurrency = concurrency
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
            wf_ids = dbos._sys_db.start_queued_workflows(queue_name, queue.concurrency)
            for id in wf_ids:
                _execute_workflow_id(dbos, id)
