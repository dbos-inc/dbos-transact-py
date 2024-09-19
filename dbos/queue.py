import threading
import time
from typing import TYPE_CHECKING, Any, Optional, Tuple

from dbos.context import DBOSContext, get_local_dbos_context
from dbos.core import (
    P,
    R,
    _execute_workflow_id,
    _init_workflow,
    _start_workflow,
    _WorkflowHandlePolling,
)
from dbos.error import DBOSInitializationError, DBOSWorkflowFunctionNotFoundError
from dbos.registrations import (
    get_config_name,
    get_dbos_class_name,
    get_dbos_func_name,
    get_func_info,
    get_temp_workflow_type,
)
from dbos.system_database import WorkflowInputs

if TYPE_CHECKING:
    from dbos.dbos import DBOS, Workflow, WorkflowHandle


class Queue:
    def __init__(self, name: str, concurrency: Optional[int] = None) -> None:
        self.name = name
        self.concurrency = concurrency
        from dbos.dbos import _get_or_create_dbos_registry

        registry = _get_or_create_dbos_registry()
        if self.name in registry.queue_info_map:
            raise DBOSInitializationError(
                f"Failed to register queue {self.name}: a queue of this name already exists."
            )
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
            wf_ids = dbos._sys_db.dequeue(queue_name, queue.concurrency)
            for id in wf_ids:
                _execute_workflow_id(dbos, id)
