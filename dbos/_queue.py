import copy
import random
import threading
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Optional, TypedDict

from psycopg import errors
from sqlalchemy.exc import OperationalError

from dbos._context import DBOSContext, get_local_dbos_context
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
        partition_queue: bool = False,
        polling_interval_sec: float = 1.0,
    ) -> None:
        if (
            worker_concurrency is not None
            and concurrency is not None
            and worker_concurrency > concurrency
        ):
            raise ValueError(
                "worker_concurrency must be less than or equal to concurrency"
            )
        if polling_interval_sec <= 0.0:
            raise ValueError("polling_interval_sec must be positive")
        self.name = name
        self.concurrency = concurrency
        self.worker_concurrency = worker_concurrency
        self.limiter = limiter
        self.priority_enabled = priority_enabled
        self.partition_queue = partition_queue
        self.polling_interval_sec = polling_interval_sec
        from ._dbos import _get_or_create_dbos_registry

        registry = _get_or_create_dbos_registry()
        if self.name in registry.queue_info_map and self.name != INTERNAL_QUEUE_NAME:
            raise Exception(f"Queue {name} has already been declared")
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
            raise Exception(
                f"Priority is not enabled for queue {self.name}. Setting priority will not have any effect."
            )
        if self.partition_queue and (
            context is None or context.queue_partition_key is None
        ):
            raise Exception(
                f"A workflow cannot be enqueued on partitioned queue {self.name} without a partition key"
            )
        if context and context.queue_partition_key and not self.partition_queue:
            raise Exception(
                f"You can only use a partition key on a partition-enabled queue. Key {context.queue_partition_key} was used with non-partitioned queue {self.name}"
            )
        if context and context.queue_partition_key and context.deduplication_id:
            raise Exception("Deduplication is not supported for partitioned queues")

        dbos = _get_dbos_instance()
        return start_workflow(
            dbos, func, args, kwargs, queue_name=self.name, execute_workflow=False
        )

    async def enqueue_async(
        self,
        func: "Callable[P, Coroutine[Any, Any, R]]",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> "WorkflowHandleAsync[R]":
        from ._dbos import _get_dbos_instance

        dbos = _get_dbos_instance()
        ctx = get_local_dbos_context()
        parent_ctx_copy = copy.copy(ctx)
        child_ctx = DBOSContext.create_start_workflow_child(ctx)
        return await start_workflow_async(
            dbos,
            parent_ctx_copy,
            child_ctx,
            func,
            args,
            kwargs,
            queue_name=self.name,
            execute_workflow=False,
        )


def queue_worker_thread(
    stop_event: threading.Event, dbos: "DBOS", queue: Queue
) -> None:
    """Worker thread for processing a single queue."""
    polling_interval = queue.polling_interval_sec
    min_polling_interval = queue.polling_interval_sec
    max_polling_interval = max(queue.polling_interval_sec, 120.0)

    while not stop_event.is_set():
        # Wait for the polling interval with jitter
        if stop_event.wait(timeout=polling_interval * random.uniform(0.95, 1.05)):
            return

        try:
            if queue.partition_queue:
                queue_partition_keys = dbos._sys_db.get_queue_partitions(queue.name)
                for key in queue_partition_keys:
                    dequeued_workflows = dbos._sys_db.start_queued_workflows(
                        queue,
                        GlobalParams.executor_id,
                        GlobalParams.app_version,
                        key,
                    )
                    for id in dequeued_workflows:
                        try:
                            execute_workflow_by_id(dbos, id, False, True)
                        except Exception as e:
                            dbos.logger.error(f"Error executing workflow {id}: {e}")
            else:
                dequeued_workflows = dbos._sys_db.start_queued_workflows(
                    queue, GlobalParams.executor_id, GlobalParams.app_version, None
                )
                for id in dequeued_workflows:
                    try:
                        execute_workflow_by_id(dbos, id, False, True)
                    except Exception as e:
                        dbos.logger.error(f"Error executing workflow {id}: {e}")
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
                dbos.logger.warning(
                    f"Exception encountered in queue thread for {queue.name}: {e}"
                )
        except Exception as e:
            if not stop_event.is_set():
                # Only print the error if the thread is not stopping
                dbos.logger.warning(
                    f"Exception encountered in queue thread for {queue.name}: {e}"
                )

        # Attempt to scale back the polling interval on each iteration
        polling_interval = max(min_polling_interval, polling_interval * 0.9)


def queue_thread(stop_event: threading.Event, dbos: "DBOS") -> None:
    """Main queue manager thread that spawns and monitors worker threads for each queue."""
    queue_threads: dict[str, threading.Thread] = {}
    check_interval = 1.0  # Check for new queues every second

    if dbos._listening_queues is not None:
        listening_queues = dbos._listening_queues
    else:
        listening_queues = list(dbos._registry.queue_info_map.values())
        listening_queues = [
            q for q in listening_queues if q.name != INTERNAL_QUEUE_NAME
        ]
    dbos.logger.info(f"Listening to {len(listening_queues)} queues:")
    log_queues(listening_queues)

    while not stop_event.is_set():
        if dbos._listening_queues is not None:
            # If explicitly listening for queues, only use those queues
            current_queues = {queue.name: queue for queue in dbos._listening_queues}
            # Always listen to the internal queue
            current_queues[INTERNAL_QUEUE_NAME] = dbos._registry.get_internal_queue()
        else:
            # Else, check all declared queues
            current_queues = dict(dbos._registry.queue_info_map)

        # Start threads for new queues
        for queue_name, queue in current_queues.items():
            if (
                queue_name not in queue_threads
                or not queue_threads[queue_name].is_alive()
            ):
                thread = threading.Thread(
                    target=queue_worker_thread,
                    args=(stop_event, dbos, queue),
                    name=f"queue-worker-{queue_name}",
                    daemon=True,
                )
                thread.start()
                queue_threads[queue_name] = thread
                dbos.logger.debug(f"Started worker thread for queue: {queue_name}")

        # Wait for the check interval or stop event
        if stop_event.wait(timeout=check_interval):
            break

    # Join all queue worker threads
    dbos.logger.info("Stopping queue manager, joining all worker threads...")
    for queue_name, thread in queue_threads.items():
        if thread.is_alive():
            thread.join(timeout=10.0)  # Give each thread 10 seconds to finish
            if thread.is_alive():
                dbos.logger.debug(
                    f"Queue worker thread for {queue_name} did not stop in time"
                )
            else:
                dbos.logger.debug(
                    f"Queue worker thread for {queue_name} stopped successfully"
                )


def log_queues(queues: list[Queue]) -> None:
    """Helper function to log queues on DBOS launch."""
    for q in queues:
        opts = []
        if q.concurrency is not None:
            opts.append(f"concurrency={q.concurrency}")
        if q.worker_concurrency is not None:
            opts.append(f"worker_concurrency={q.worker_concurrency}")
        if q.limiter is not None:
            opts.append(f"limit={q.limiter['limit']}/{q.limiter['period']}s")
        if q.priority_enabled:
            opts.append("priority")
        if q.partition_queue:
            opts.append("partitioned")
        opts_str = f" ({', '.join(opts)})" if opts else ""
        dbos_logger.info(f"Queue: {q.name}{opts_str}")
