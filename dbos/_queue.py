import copy
import random
import threading
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Literal, Optional, TypedDict

QueueConflictResolution = Literal[
    "update_if_latest_version", "always_update", "never_update"
]

from psycopg import errors
from sqlalchemy.exc import OperationalError

from dbos._context import DBOSContext, get_local_dbos_context
from dbos._error import DBOSException
from dbos._logger import dbos_logger
from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams

from ._core import P, R, execute_workflow_by_id, start_workflow, start_workflow_async

if TYPE_CHECKING:
    from ._dbos import DBOS, WorkflowHandle, WorkflowHandleAsync
    from ._sys_db import SystemDatabase


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
        database_backed_queue: bool = False,
        client_system_database: Optional["SystemDatabase"] = None,
    ) -> None:
        Queue._validate_queue(
            concurrency=concurrency,
            worker_concurrency=worker_concurrency,
            polling_interval_sec=polling_interval_sec,
            limiter=limiter,
        )
        self.name = name
        self.database_backed_queue = database_backed_queue
        # When set, getters/setters use this SystemDatabase instead of the
        # DBOS singleton's. This allows a DBOSClient to manipulate queues
        # without depending on a launched DBOS process.
        self._client_system_database = client_system_database
        # Local cache of configurable params. Property getters consult this
        # for in-memory queues and the database for database-backed queues.
        self._concurrency = concurrency
        self._worker_concurrency = worker_concurrency
        self._limiter = limiter
        self._priority_enabled = priority_enabled
        self._partition_queue = partition_queue
        self._polling_interval_sec = polling_interval_sec

        # Database-backed queues skip the in-memory global registry; their
        # source of truth is the queues table.
        if database_backed_queue:
            return

        from ._dbos import _get_or_create_dbos_registry

        registry = _get_or_create_dbos_registry()
        if self.name in registry.queue_info_map and self.name != INTERNAL_QUEUE_NAME:
            raise Exception(f"Queue {name} has already been declared")
        registry.queue_info_map[self.name] = self

    @staticmethod
    def _validate_queue(
        *,
        concurrency: Optional[int],
        worker_concurrency: Optional[int],
        polling_interval_sec: float,
        limiter: Optional[QueueRateLimit],
    ) -> None:
        """Validate queue configuration parameters, raising ValueError on bad input."""
        if (
            worker_concurrency is not None
            and concurrency is not None
            and worker_concurrency > concurrency
        ):
            raise ValueError(
                "concurrency must be greater than or equal to worker_concurrency"
            )
        if polling_interval_sec <= 0.0:
            raise ValueError("polling_interval_sec must be positive")
        if limiter is not None and (
            limiter.get("limit") is None or limiter.get("period") is None
        ):
            raise ValueError("limiter must specify both 'limit' and 'period'")

    def _require_database_backed(self) -> None:
        if not self.database_backed_queue:
            raise DBOSException(
                f"Cannot configure queue {self.name}: dynamic configuration is "
                "only supported for queues registered via DBOS.register_queue."
            )

    def _sys_db(self) -> "SystemDatabase":
        if self._client_system_database is not None:
            return self._client_system_database
        from ._dbos import _get_dbos_instance

        return _get_dbos_instance()._sys_db

    def _read_from_db(self) -> "Queue":
        latest = self._sys_db().get_queue(
            self.name, client_system_database=self._client_system_database
        )
        if latest is None:
            raise DBOSException(f"Queue {self.name} not found in the database")
        return latest

    def _write_to_db(self, fields: dict[str, Any]) -> None:
        self._sys_db().update_queue(self.name, fields)

    @property
    def concurrency(self) -> Optional[int]:
        if self.database_backed_queue:
            self._concurrency = self._read_from_db()._concurrency
        return self._concurrency

    def set_concurrency(self, value: Optional[int]) -> None:
        self._require_database_backed()
        if (
            value is not None
            and self._worker_concurrency is not None
            and self._worker_concurrency > value
        ):
            raise ValueError(
                "worker_concurrency must be less than or equal to concurrency"
            )
        self._write_to_db({"concurrency": value})
        self._concurrency = value

    @property
    def worker_concurrency(self) -> Optional[int]:
        if self.database_backed_queue:
            self._worker_concurrency = self._read_from_db()._worker_concurrency
        return self._worker_concurrency

    def set_worker_concurrency(self, value: Optional[int]) -> None:
        self._require_database_backed()
        if (
            value is not None
            and self._concurrency is not None
            and value > self._concurrency
        ):
            raise ValueError(
                "worker_concurrency must be less than or equal to concurrency"
            )
        self._write_to_db({"worker_concurrency": value})
        self._worker_concurrency = value

    @property
    def limiter(self) -> Optional[QueueRateLimit]:
        if self.database_backed_queue:
            self._limiter = self._read_from_db()._limiter
        return self._limiter

    def set_limiter(self, value: Optional[QueueRateLimit]) -> None:
        self._require_database_backed()
        if value is not None and (
            value.get("limit") is None or value.get("period") is None
        ):
            raise ValueError("limiter must specify both 'limit' and 'period'")
        self._write_to_db(
            {
                "rate_limit_max": value["limit"] if value else None,
                "rate_limit_period_sec": value["period"] if value else None,
            }
        )
        self._limiter = value

    @property
    def priority_enabled(self) -> bool:
        if self.database_backed_queue:
            self._priority_enabled = self._read_from_db()._priority_enabled
        return self._priority_enabled

    def set_priority_enabled(self, value: bool) -> None:
        self._require_database_backed()
        self._write_to_db({"priority_enabled": value})
        self._priority_enabled = value

    @property
    def partition_queue(self) -> bool:
        if self.database_backed_queue:
            self._partition_queue = self._read_from_db()._partition_queue
        return self._partition_queue

    def set_partition_queue(self, value: bool) -> None:
        self._require_database_backed()
        self._write_to_db({"partition_queue": value})
        self._partition_queue = value

    @property
    def polling_interval_sec(self) -> float:
        if self.database_backed_queue:
            self._polling_interval_sec = self._read_from_db()._polling_interval_sec
        return self._polling_interval_sec

    def set_polling_interval_sec(self, value: float) -> None:
        self._require_database_backed()
        if value <= 0.0:
            raise ValueError("polling_interval_sec must be positive")
        self._write_to_db({"polling_interval_sec": value})
        self._polling_interval_sec = value

    def _require_dbos_bound(self) -> None:
        if self._client_system_database is not None:
            raise DBOSException(
                f"Cannot enqueue on queue {self.name} from a client-bound Queue "
                "object. Use DBOSClient.enqueue instead."
            )

    def _validate_enqueue(self, ctx: Optional[DBOSContext]) -> None:
        self._require_dbos_bound()
        if ctx is not None and ctx.priority is not None and not self.priority_enabled:
            raise Exception(
                f"Priority is not enabled for queue {self.name}. Setting priority will not have any effect."
            )
        if self.partition_queue and (ctx is None or ctx.queue_partition_key is None):
            raise Exception(
                f"A workflow cannot be enqueued on partitioned queue {self.name} without a partition key"
            )
        if ctx and ctx.queue_partition_key and not self.partition_queue:
            raise Exception(
                f"You can only use a partition key on a partition-enabled queue. Key {ctx.queue_partition_key} was used with non-partitioned queue {self.name}"
            )
        if ctx and ctx.queue_partition_key and ctx.deduplication_id:
            raise Exception("Deduplication is not supported for partitioned queues")

    def enqueue(
        self, func: "Callable[P, R]", *args: P.args, **kwargs: P.kwargs
    ) -> "WorkflowHandle[R]":
        from ._dbos import _get_dbos_instance

        self._validate_enqueue(get_local_dbos_context())
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

        ctx = get_local_dbos_context()
        self._validate_enqueue(ctx)
        dbos = _get_dbos_instance()
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
    polling_interval = queue._polling_interval_sec
    max_polling_interval = max(queue._polling_interval_sec, 120.0)

    while not stop_event.is_set():
        # Reload database-backed queue config once per iteration so dynamic
        # changes (concurrency, polling interval, etc.) take effect without
        # a restart. If the row was deleted, exit the worker.
        if queue.database_backed_queue:
            try:
                latest = dbos._sys_db.get_queue(queue.name)
            except Exception as e:
                dbos.logger.warning(
                    f"Exception reloading queue {queue.name} from database: {e}"
                )
                latest = queue
            if latest is None:
                dbos.logger.info(
                    f"Queue {queue.name} no longer exists in database, stopping worker"
                )
                return
            queue = latest

        min_polling_interval = queue._polling_interval_sec
        max_polling_interval = max(queue._polling_interval_sec, 120.0)
        polling_interval = max(
            min_polling_interval, min(polling_interval, max_polling_interval)
        )

        # Wait for the polling interval with jitter
        if stop_event.wait(timeout=polling_interval * random.uniform(0.95, 1.05)):
            return

        try:
            if queue._partition_queue:
                queue_partition_keys = dbos._sys_db.get_queue_partitions(queue.name)
                for key in queue_partition_keys:
                    local_running_count = dbos._active_workflows_set.count_for_queue(
                        queue.name, key
                    )
                    dequeued_workflows = dbos._sys_db.start_queued_workflows(
                        queue,
                        GlobalParams.executor_id,
                        GlobalParams.app_version,
                        key,
                        local_running_count,
                    )
                    for id in dequeued_workflows:
                        try:
                            execute_workflow_by_id(dbos, id, False, True)
                        except Exception as e:
                            dbos.logger.error(f"Error executing workflow {id}: {e}")
            else:
                local_running_count = dbos._active_workflows_set.count_for_queue(
                    queue.name, None
                )
                dequeued_workflows = dbos._sys_db.start_queued_workflows(
                    queue,
                    GlobalParams.executor_id,
                    GlobalParams.app_version,
                    None,
                    local_running_count,
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
    # Check interval for monitoring queue registration changes
    check_interval = 1.0

    log_queues(dbos, dbos._listening_queues)

    while not stop_event.is_set():
        if dbos._listening_queues is not None:
            # If explicitly listening for queues, resolve each name to a Queue
            # from either the in-memory registry or the database.
            listening_set = set(dbos._listening_queues)
            current_queues = {
                name: q
                for name, q in dbos._registry.queue_info_map.items()
                if name in listening_set
            }
            try:
                for queue in dbos._sys_db.list_queues():
                    if queue.name in listening_set and queue.name not in current_queues:
                        current_queues[queue.name] = queue
            except Exception as e:
                dbos.logger.warning(f"Exception listing database-backed queues: {e}")
            # Always listen to the internal queue
            current_queues[INTERNAL_QUEUE_NAME] = dbos._registry.get_internal_queue()
        else:
            # Else, check all in-memory and database-backed queues
            current_queues = dict(dbos._registry.queue_info_map)
            try:
                for queue in dbos._sys_db.list_queues():
                    if queue.name in dbos._registry.queue_info_map:
                        dbos.logger.warning(
                            f"Database-backed queue {queue.name} has the same "
                            "name as an in-memory queue. The in-memory queue's "
                            "configuration is being used; the database-backed "
                            "queue is ignored. Rename one of them to resolve "
                            "the conflict."
                        )
                        continue
                    if (
                        queue.name in queue_threads
                        and queue_threads[queue.name].is_alive()
                    ):
                        continue
                    current_queues[queue.name] = queue
            except Exception as e:
                dbos.logger.warning(f"Exception listing database-backed queues: {e}")

        # Transition any DELAYED workflows whose delay has expired to ENQUEUED.
        try:
            dbos._sys_db.transition_delayed_workflows()
        except Exception as e:
            dbos.logger.warning(f"Exception transitioning delayed workflows: {e}")

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


def log_queue(q: Queue) -> None:
    """Log a single queue's name and its set parameters. Unset parameters
    are omitted, matching ``Queue: <name> (concurrency=…, worker_concurrency=…,
    limit=N/Ts, priority, partitioned)``."""
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


def log_queues(dbos: "DBOS", listening_queues: Optional[list[str]]) -> None:
    """Log all queues this process will listen to on DBOS launch.

    Combines in-memory registered queues with database-backed queues, applies
    the listen_queues filter if any, and excludes the internal queue.
    """
    queues: dict[str, Queue] = dict(dbos._registry.queue_info_map)
    try:
        for q in dbos._sys_db.list_queues():
            queues.setdefault(q.name, q)
    except Exception as e:
        dbos.logger.warning(f"Exception listing database-backed queues: {e}")

    if listening_queues is not None:
        listening_set = set(listening_queues)
        queues = {n: q for n, q in queues.items() if n in listening_set}

    queues.pop(INTERNAL_QUEUE_NAME, None)

    dbos.logger.info(f"Listening to {len(queues)} queues:")
    for q in queues.values():
        log_queue(q)
