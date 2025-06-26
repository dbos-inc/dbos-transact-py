import random
import threading
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Optional, TypedDict

from psycopg import errors
from sqlalchemy.exc import OperationalError

from dbos._context import get_local_dbos_context
from dbos._logger import dbos_logger
from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams

from ._core import P, R, execute_workflow_by_id, start_workflow, start_workflow_async

if TYPE_CHECKING:
    from ._dbos import DBOS, WorkflowHandle, WorkflowHandleAsync


class QueueRateLimit(TypedDict):
    """
    Limit the maximum number of workflows from this queue that can be started in a given period.

    If the limit is 5 and the period is 10, no more than 5 functions can be
    started per 10 seconds.
    """

    limit: int
    period: float


class Queue:
    """
    Workflow queue.

    Workflow queues allow workflows to be started at a later time, based on concurrency and
    rate limits.
    """

    def __init__(
        self,
        name: str,
        concurrency: Optional[int] = None,
        limiter: Optional[QueueRateLimit] = None,
        *,  # Disable positional arguments from here on
        worker_concurrency: Optional[int] = None,
        priority_enabled: bool = False,
    ) -> None:
        if (
            worker_concurrency is not None
            and concurrency is not None
            and worker_concurrency > concurrency
        ):
            raise ValueError(
                "worker_concurrency must be less than or equal to concurrency"
            )
        self.name = name
        self.concurrency = concurrency
        self.worker_concurrency = worker_concurrency
        self.limiter = limiter
        self.priority_enabled = priority_enabled
        from ._dbos import _get_or_create_dbos_registry

        registry = _get_or_create_dbos_registry()
        if self.name in registry.queue_info_map and self.name != INTERNAL_QUEUE_NAME:
            dbos_logger.warning(f"Queue {name} has already been declared")
        registry.queue_info_map[self.name] = self

    def enqueue(
        self, func: "Callable[P, R]", *args: P.args, **kwargs: P.kwargs
    ) -> "WorkflowHandle[R]":
        from ._dbos import _get_dbos_instance

        context = get_local_dbos_context()
        if (
            context is not None
            and context.priority is not None
            and not self.priority_enabled
        ):
            dbos_logger.warning(
                f"Priority is not enabled for queue {self.name}. Setting priority will not have any effect."
            )

        dbos = _get_dbos_instance()
        return start_workflow(dbos, func, self.name, False, *args, **kwargs)

    async def enqueue_async(
        self,
        func: "Callable[P, Coroutine[Any, Any, R]]",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> "WorkflowHandleAsync[R]":
        from ._dbos import _get_dbos_instance

        dbos = _get_dbos_instance()
        return await start_workflow_async(dbos, func, self.name, False, *args, **kwargs)


def queue_thread(stop_event: threading.Event, dbos: "DBOS") -> None:
    polling_interval = 1.0
    min_polling_interval = 1.0
    max_polling_interval = 120.0
    while not stop_event.is_set():
        # Wait for the polling interval with jitter
        if stop_event.wait(timeout=polling_interval * random.uniform(0.95, 1.05)):
            return
        queues = dict(dbos._registry.queue_info_map)
        for _, queue in queues.items():
            try:
                wf_ids = dbos._sys_db.start_queued_workflows(
                    queue, GlobalParams.executor_id, GlobalParams.app_version
                )
                for id in wf_ids:
                    execute_workflow_by_id(dbos, id)
            except OperationalError as e:
                if isinstance(
                    e.orig, (errors.SerializationFailure, errors.LockNotAvailable)
                ):
                    # If a serialization error is encountered, increase the polling interval
                    polling_interval = min(
                        max_polling_interval,
                        polling_interval * 2.0,
                    )
                    dbos.logger.warning(
                        f"Contention detected in queue thread for {queue.name}. Increasing polling interval to {polling_interval:.2f}."
                    )
                else:
                    dbos.logger.warning(f"Exception encountered in queue thread: {e}")
            except Exception as e:
                if not stop_event.is_set():
                    # Only print the error if the thread is not stopping
                    dbos.logger.warning(f"Exception encountered in queue thread: {e}")
        # Attempt to scale back the polling interval on each iteration
        polling_interval = max(min_polling_interval, polling_interval * 0.9)
