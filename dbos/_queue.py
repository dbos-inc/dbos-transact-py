import asyncio
import copy
import random
import sys
import threading
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    List,
    Literal,
    Optional,
    TypedDict,
)

QueueConflictResolution = Literal[
    "update_if_latest_version", "always_update", "never_update"
]

from psycopg import errors
from sqlalchemy.exc import OperationalError

from dbos._context import DBOSContext, get_local_dbos_context
from dbos._error import DBOSException, DBOSRecoveryError
from dbos._logger import dbos_logger
from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams

from ._core import P, R, execute_dequeued_workflow, start_workflow, start_workflow_async

if TYPE_CHECKING:
    from ._dbos import DBOS, WorkflowHandle, WorkflowHandleAsync
    from ._sys_db import SystemDatabase


def _warn_sync_db_call_in_async_context(
    method_name: str, async_alternative: str
) -> None:
    """Log a warning when a synchronous method that reads or writes the system
    database is called from a thread with a running asyncio event loop. Such
    calls block the loop on a database round-trip; callers in async code
    should use ``async_alternative`` instead.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    dbos_logger.warning(
        f"Synchronous '{method_name}' was called from a running asyncio "
        f"event loop. This blocks the loop on a database round-trip. Use "
        f"'{async_alternative}' instead."
    )


DEFAULT_QUEUE_POLLING_INTERVAL_SEC = 1.0


class QueueRateLimit(TypedDict):
    """
    Limit the maximum number of workflows from this queue that can be started in a given period.

    If the limit is 5 and the period is 10, no more than 5 functions can be
    started per 10 seconds.
    """

    limit: int
    period: float


@dataclass
class ResolvedQueueLimits:
    """A queue's limits, each resolved to the scope it is enforced at."""

    global_concurrency: Optional[int]
    worker_concurrency: Optional[int]
    limiter: Optional[QueueRateLimit]
    partition_concurrency: Optional[int]
    partition_worker_concurrency: Optional[int]
    partition_limiter: Optional[QueueRateLimit]


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
        global_concurrency: Optional[int] = None,
        partition_concurrency: Optional[int] = None,
        partition_worker_concurrency: Optional[int] = None,
        partition_limiter: Optional[QueueRateLimit] = None,
        polling_interval_sec: float = DEFAULT_QUEUE_POLLING_INTERVAL_SEC,
        database_backed_queue: bool = False,
        client_system_database: Optional["SystemDatabase"] = None,
        application_name: Optional[str] = None,
        # Deprecated, retained for backwards compatibility. concurrency and limiter keep their positional slots above: moving them would rebind Queue("q", 5).
        priority_enabled: bool = False,
        partition_queue: bool = False,
    ) -> None:
        # Rows are validated when written, and a row legitimately carries both partition_queue and the partition limits, which no caller may combine.
        if not database_backed_queue:
            Queue._validate_queue(
                concurrency=concurrency,
                worker_concurrency=worker_concurrency,
                global_concurrency=global_concurrency,
                partition_concurrency=partition_concurrency,
                partition_worker_concurrency=partition_worker_concurrency,
                partition_limiter=partition_limiter,
                partition_queue=partition_queue,
                polling_interval_sec=polling_interval_sec,
                limiter=limiter,
            )
        self.name = name
        self.database_backed_queue = database_backed_queue
        # Owner from the queues table; None for in-memory and pre-upgrade queues.
        self.application_name = application_name
        # When set, getters/setters use this SystemDatabase instead of the
        # DBOS singleton's. This allows a DBOSClient to manipulate queues
        # without depending on a launched DBOS process.
        self._client_system_database = client_system_database
        # Local cache of the queues-table columns; getters consult it for in-memory queues and the database for database-backed ones.
        self._concurrency = (
            concurrency if global_concurrency is None else global_concurrency
        )
        self._worker_concurrency = worker_concurrency
        self._limiter = limiter
        self._priority_enabled = priority_enabled
        self._partition_concurrency = partition_concurrency
        self._partition_worker_concurrency = partition_worker_concurrency
        self._partition_limiter = partition_limiter
        # Partitioning is inferred from any per-partition limit; the deprecated flag tracks it.
        self._partition_queue = partition_queue or self._has_partition_limits()
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
        global_concurrency: Optional[int] = None,
        partition_concurrency: Optional[int] = None,
        partition_worker_concurrency: Optional[int] = None,
        partition_limiter: Optional[QueueRateLimit] = None,
        partition_queue: bool = False,
    ) -> None:
        """Validate queue configuration parameters, raising ValueError on bad input."""
        if concurrency is not None and global_concurrency is not None:
            raise ValueError(
                "concurrency is deprecated in favor of global_concurrency; set only one of them"
            )
        if partition_queue and (
            partition_concurrency is not None
            or partition_worker_concurrency is not None
            or partition_limiter is not None
        ):
            raise ValueError(
                "partition_queue is deprecated in favor of the partition_* limits; set only one of them"
            )
        if partition_queue and global_concurrency is not None:
            raise ValueError(
                "partition_queue applies every limit per partition, so it cannot be combined with global_concurrency; use partition_concurrency instead"
            )
        if partition_concurrency is not None and partition_concurrency < 1:
            raise ValueError("partition_concurrency must be at least 1")
        if partition_limiter is not None and (
            partition_limiter.get("limit") is None
            or partition_limiter.get("period") is None
        ):
            raise ValueError("partition_limiter must specify both 'limit' and 'period'")
        if (
            partition_worker_concurrency is not None
            and partition_concurrency is not None
            and partition_worker_concurrency > partition_concurrency
        ):
            raise ValueError(
                "partition_concurrency must be greater than or equal to partition_worker_concurrency"
            )
        if (
            partition_worker_concurrency is not None
            and worker_concurrency is not None
            and partition_worker_concurrency > worker_concurrency
        ):
            raise ValueError(
                "worker_concurrency must be greater than or equal to partition_worker_concurrency"
            )
        # Under the deprecated partition_queue spelling concurrency is itself a per-partition limit, so the worker_concurrency check below compares like with like.
        queue_concurrency = (
            concurrency if global_concurrency is None else global_concurrency
        )
        if (
            worker_concurrency is not None
            and queue_concurrency is not None
            and worker_concurrency > queue_concurrency
        ):
            raise ValueError(
                "concurrency must be greater than or equal to worker_concurrency"
            )
        if (
            partition_concurrency is not None
            and queue_concurrency is not None
            and partition_concurrency > queue_concurrency
        ):
            raise ValueError(
                "global_concurrency must be greater than or equal to partition_concurrency"
            )
        if (
            partition_worker_concurrency is not None
            and queue_concurrency is not None
            and partition_worker_concurrency > queue_concurrency
        ):
            raise ValueError(
                "concurrency must be greater than or equal to partition_worker_concurrency"
            )
        if polling_interval_sec <= 0.0:
            raise ValueError("polling_interval_sec must be positive")
        if limiter is not None and (
            limiter.get("limit") is None or limiter.get("period") is None
        ):
            raise ValueError("limiter must specify both 'limit' and 'period'")

    def _has_partition_limits(self) -> bool:
        """True when any per-partition limit is set, which is what partitions a queue."""
        return self._partitioned_after()

    def _partitioned_after(self, **overrides: Any) -> bool:
        """Whether the queue is still partitioned once these fields take these values."""
        values: dict[str, Any] = {
            "_partition_concurrency": self._partition_concurrency,
            "_partition_worker_concurrency": self._partition_worker_concurrency,
            "_partition_limiter": self._partition_limiter,
        }
        values.update(overrides)
        return any(value is not None for value in values.values())

    def _is_legacy_partitioned(self) -> bool:
        """True when the queue uses the deprecated partition_queue spelling, under
        which concurrency, worker_concurrency, and limiter all apply per partition."""
        return self._partition_queue and not self._has_partition_limits()

    def _resolve_limits(self) -> ResolvedQueueLimits:
        """Resolve every limit to the scope it is enforced at."""
        if self._is_legacy_partitioned():
            return ResolvedQueueLimits(
                global_concurrency=None,
                worker_concurrency=None,
                limiter=None,
                partition_concurrency=self._concurrency,
                partition_worker_concurrency=self._worker_concurrency,
                partition_limiter=self._limiter,
            )
        return ResolvedQueueLimits(
            global_concurrency=self._concurrency,
            worker_concurrency=self._worker_concurrency,
            limiter=self._limiter,
            partition_concurrency=self._partition_concurrency,
            partition_worker_concurrency=self._partition_worker_concurrency,
            partition_limiter=self._partition_limiter,
        )

    def _require_not_legacy_partitioned(self, field: str) -> None:
        """Reject a write that would re-scope the other limits on a legacy queue."""
        if self._is_legacy_partitioned():
            raise DBOSException(
                f"Cannot set {field} on queue {self.name}: it is registered with the "
                "deprecated partition_queue option, under which concurrency, "
                "worker_concurrency, and limiter apply per partition. Re-register the "
                "queue with the partition_* limits instead."
            )

    def _check_concurrency_bounds(self, value: Optional[int]) -> None:
        """Validate a new concurrency against the cached sibling limits."""
        if value is None:
            return
        if self._worker_concurrency is not None and self._worker_concurrency > value:
            raise ValueError(
                "worker_concurrency must be less than or equal to concurrency"
            )
        if (
            self._partition_concurrency is not None
            and self._partition_concurrency > value
        ):
            raise ValueError(
                "partition_concurrency must be less than or equal to global_concurrency"
            )
        if (
            self._partition_worker_concurrency is not None
            and self._partition_worker_concurrency > value
        ):
            raise ValueError(
                "partition_worker_concurrency must be less than or equal to concurrency"
            )

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

    def _refresh_fields(self, latest: "Queue") -> None:
        """Copy every cached configuration field from ``latest`` into ``self``."""
        self._concurrency = latest._concurrency
        self._worker_concurrency = latest._worker_concurrency
        self._limiter = latest._limiter
        self._priority_enabled = latest._priority_enabled
        self._partition_queue = latest._partition_queue
        self._partition_concurrency = latest._partition_concurrency
        self._partition_worker_concurrency = latest._partition_worker_concurrency
        self._partition_limiter = latest._partition_limiter
        self._polling_interval_sec = latest._polling_interval_sec
        self.application_name = latest.application_name

    async def _configure_thread_pool(self) -> None:
        """Route ``asyncio.to_thread`` through DBOS's executor for DBOS-bound
        queues. Client-bound queues use the loop's default executor."""
        if self._client_system_database is None:
            from ._dbos import DBOS

            await DBOS._configure_asyncio_thread_pool()

    @property
    def concurrency(self) -> Optional[int]:
        """Deprecated. Use global_concurrency."""
        if self.database_backed_queue:
            _warn_sync_db_call_in_async_context(
                "Queue.concurrency", "Queue.get_concurrency_async"
            )
            self._refresh_fields(self._read_from_db())
        return self._concurrency

    async def get_concurrency_async(self) -> Optional[int]:
        """Deprecated. Use get_global_concurrency_async."""
        if self.database_backed_queue:
            await self._configure_thread_pool()
            self._refresh_fields(await asyncio.to_thread(self._read_from_db))
        return self._concurrency

    def set_concurrency(self, value: Optional[int]) -> None:
        """Deprecated. Use set_global_concurrency."""
        self._require_database_backed()
        _warn_sync_db_call_in_async_context(
            "Queue.set_concurrency", "Queue.set_concurrency_async"
        )
        # Refresh the local cache so the cross-field check below validates
        # against the latest worker_concurrency stored in the database.
        self._refresh_fields(self._read_from_db())
        self._check_concurrency_bounds(value)
        self._write_to_db({"concurrency": value})
        self._concurrency = value

    async def set_concurrency_async(self, value: Optional[int]) -> None:
        """Deprecated. Use set_global_concurrency_async."""
        await self._configure_thread_pool()
        await asyncio.to_thread(self.set_concurrency, value)

    @property
    def global_concurrency(self) -> Optional[int]:
        if self.database_backed_queue:
            _warn_sync_db_call_in_async_context(
                "Queue.global_concurrency", "Queue.get_global_concurrency_async"
            )
            self._refresh_fields(self._read_from_db())
        return self._resolve_limits().global_concurrency

    async def get_global_concurrency_async(self) -> Optional[int]:
        if self.database_backed_queue:
            await self._configure_thread_pool()
            self._refresh_fields(await asyncio.to_thread(self._read_from_db))
        return self._resolve_limits().global_concurrency

    def set_global_concurrency(self, value: Optional[int]) -> None:
        self._require_database_backed()
        _warn_sync_db_call_in_async_context(
            "Queue.set_global_concurrency", "Queue.set_global_concurrency_async"
        )
        self._refresh_fields(self._read_from_db())
        self._require_not_legacy_partitioned("global_concurrency")
        self._check_concurrency_bounds(value)
        self._write_to_db({"concurrency": value})
        self._concurrency = value

    async def set_global_concurrency_async(self, value: Optional[int]) -> None:
        await self._configure_thread_pool()
        await asyncio.to_thread(self.set_global_concurrency, value)

    @property
    def partition_concurrency(self) -> Optional[int]:
        if self.database_backed_queue:
            _warn_sync_db_call_in_async_context(
                "Queue.partition_concurrency", "Queue.get_partition_concurrency_async"
            )
            self._refresh_fields(self._read_from_db())
        return self._resolve_limits().partition_concurrency

    async def get_partition_concurrency_async(self) -> Optional[int]:
        if self.database_backed_queue:
            await self._configure_thread_pool()
            self._refresh_fields(await asyncio.to_thread(self._read_from_db))
        return self._resolve_limits().partition_concurrency

    def set_partition_concurrency(self, value: Optional[int]) -> None:
        self._require_database_backed()
        _warn_sync_db_call_in_async_context(
            "Queue.set_partition_concurrency", "Queue.set_partition_concurrency_async"
        )
        if value is not None and value < 1:
            raise ValueError("partition_concurrency must be at least 1")
        self._refresh_fields(self._read_from_db())
        self._require_not_legacy_partitioned("partition_concurrency")
        if (
            value is not None
            and self._concurrency is not None
            and value > self._concurrency
        ):
            raise ValueError(
                "partition_concurrency must be less than or equal to global_concurrency"
            )
        # Partitioning is inferred from the limits, so the deprecated flag follows them.
        partitioned = self._partitioned_after(_partition_concurrency=value)
        self._write_to_db(
            {"partition_concurrency": value, "partition_queue": partitioned}
        )
        self._partition_concurrency = value
        self._partition_queue = partitioned

    async def set_partition_concurrency_async(self, value: Optional[int]) -> None:
        await self._configure_thread_pool()
        await asyncio.to_thread(self.set_partition_concurrency, value)

    @property
    def partition_worker_concurrency(self) -> Optional[int]:
        if self.database_backed_queue:
            _warn_sync_db_call_in_async_context(
                "Queue.partition_worker_concurrency",
                "Queue.get_partition_worker_concurrency_async",
            )
            self._refresh_fields(self._read_from_db())
        return self._resolve_limits().partition_worker_concurrency

    async def get_partition_worker_concurrency_async(self) -> Optional[int]:
        if self.database_backed_queue:
            await self._configure_thread_pool()
            self._refresh_fields(await asyncio.to_thread(self._read_from_db))
        return self._resolve_limits().partition_worker_concurrency

    def set_partition_worker_concurrency(self, value: Optional[int]) -> None:
        self._require_database_backed()
        _warn_sync_db_call_in_async_context(
            "Queue.set_partition_worker_concurrency",
            "Queue.set_partition_worker_concurrency_async",
        )
        self._refresh_fields(self._read_from_db())
        self._require_not_legacy_partitioned("partition_worker_concurrency")
        if value is not None:
            if (
                self._partition_concurrency is not None
                and value > self._partition_concurrency
            ):
                raise ValueError(
                    "partition_worker_concurrency must be less than or equal to partition_concurrency"
                )
            if (
                self._worker_concurrency is not None
                and value > self._worker_concurrency
            ):
                raise ValueError(
                    "partition_worker_concurrency must be less than or equal to worker_concurrency"
                )
            if self._concurrency is not None and value > self._concurrency:
                raise ValueError(
                    "partition_worker_concurrency must be less than or equal to concurrency"
                )
        partitioned = self._partitioned_after(_partition_worker_concurrency=value)
        self._write_to_db(
            {"partition_worker_concurrency": value, "partition_queue": partitioned}
        )
        self._partition_worker_concurrency = value
        self._partition_queue = partitioned

    async def set_partition_worker_concurrency_async(
        self, value: Optional[int]
    ) -> None:
        await self._configure_thread_pool()
        await asyncio.to_thread(self.set_partition_worker_concurrency, value)

    @property
    def partition_limiter(self) -> Optional[QueueRateLimit]:
        if self.database_backed_queue:
            _warn_sync_db_call_in_async_context(
                "Queue.partition_limiter", "Queue.get_partition_limiter_async"
            )
            self._refresh_fields(self._read_from_db())
        return self._resolve_limits().partition_limiter

    async def get_partition_limiter_async(self) -> Optional[QueueRateLimit]:
        if self.database_backed_queue:
            await self._configure_thread_pool()
            self._refresh_fields(await asyncio.to_thread(self._read_from_db))
        return self._resolve_limits().partition_limiter

    def set_partition_limiter(self, value: Optional[QueueRateLimit]) -> None:
        self._require_database_backed()
        if value is not None and (
            value.get("limit") is None or value.get("period") is None
        ):
            raise ValueError("partition_limiter must specify both 'limit' and 'period'")
        _warn_sync_db_call_in_async_context(
            "Queue.set_partition_limiter", "Queue.set_partition_limiter_async"
        )
        self._refresh_fields(self._read_from_db())
        self._require_not_legacy_partitioned("partition_limiter")
        partitioned = self._partitioned_after(_partition_limiter=value)
        self._write_to_db(
            {
                "partition_rate_limit_max": value["limit"] if value else None,
                "partition_rate_limit_period_sec": value["period"] if value else None,
                "partition_queue": partitioned,
            }
        )
        self._partition_limiter = value
        self._partition_queue = partitioned

    async def set_partition_limiter_async(
        self, value: Optional[QueueRateLimit]
    ) -> None:
        await self._configure_thread_pool()
        await asyncio.to_thread(self.set_partition_limiter, value)

    @property
    def worker_concurrency(self) -> Optional[int]:
        if self.database_backed_queue:
            _warn_sync_db_call_in_async_context(
                "Queue.worker_concurrency", "Queue.get_worker_concurrency_async"
            )
            self._refresh_fields(self._read_from_db())
        return self._worker_concurrency

    async def get_worker_concurrency_async(self) -> Optional[int]:
        if self.database_backed_queue:
            await self._configure_thread_pool()
            self._refresh_fields(await asyncio.to_thread(self._read_from_db))
        return self._worker_concurrency

    def set_worker_concurrency(self, value: Optional[int]) -> None:
        self._require_database_backed()
        _warn_sync_db_call_in_async_context(
            "Queue.set_worker_concurrency", "Queue.set_worker_concurrency_async"
        )
        # Refresh the local cache so the cross-field check below validates
        # against the latest concurrency stored in the database.
        self._refresh_fields(self._read_from_db())
        if value is not None:
            if self._concurrency is not None and value > self._concurrency:
                raise ValueError(
                    "worker_concurrency must be less than or equal to concurrency"
                )
            if (
                self._partition_worker_concurrency is not None
                and self._partition_worker_concurrency > value
            ):
                raise ValueError(
                    "partition_worker_concurrency must be less than or equal to worker_concurrency"
                )
        self._write_to_db({"worker_concurrency": value})
        self._worker_concurrency = value

    async def set_worker_concurrency_async(self, value: Optional[int]) -> None:
        await self._configure_thread_pool()
        await asyncio.to_thread(self.set_worker_concurrency, value)

    @property
    def limiter(self) -> Optional[QueueRateLimit]:
        if self.database_backed_queue:
            _warn_sync_db_call_in_async_context(
                "Queue.limiter", "Queue.get_limiter_async"
            )
            self._refresh_fields(self._read_from_db())
        return self._limiter

    async def get_limiter_async(self) -> Optional[QueueRateLimit]:
        if self.database_backed_queue:
            await self._configure_thread_pool()
            self._refresh_fields(await asyncio.to_thread(self._read_from_db))
        return self._limiter

    def set_limiter(self, value: Optional[QueueRateLimit]) -> None:
        self._require_database_backed()
        if value is not None and (
            value.get("limit") is None or value.get("period") is None
        ):
            raise ValueError("limiter must specify both 'limit' and 'period'")
        _warn_sync_db_call_in_async_context(
            "Queue.set_limiter", "Queue.set_limiter_async"
        )
        self._write_to_db(
            {
                "rate_limit_max": value["limit"] if value else None,
                "rate_limit_period_sec": value["period"] if value else None,
            }
        )
        self._limiter = value

    async def set_limiter_async(self, value: Optional[QueueRateLimit]) -> None:
        await self._configure_thread_pool()
        await asyncio.to_thread(self.set_limiter, value)

    @property
    def priority_enabled(self) -> bool:
        """Deprecated. Priority is always enabled."""
        if self.database_backed_queue:
            _warn_sync_db_call_in_async_context(
                "Queue.priority_enabled", "Queue.get_priority_enabled_async"
            )
            self._refresh_fields(self._read_from_db())
        return self._priority_enabled

    async def get_priority_enabled_async(self) -> bool:
        """Deprecated. Priority is always enabled."""
        if self.database_backed_queue:
            await self._configure_thread_pool()
            self._refresh_fields(await asyncio.to_thread(self._read_from_db))
        return self._priority_enabled

    def set_priority_enabled(self, value: bool) -> None:
        """Deprecated. Priority is always enabled."""
        self._require_database_backed()
        _warn_sync_db_call_in_async_context(
            "Queue.set_priority_enabled", "Queue.set_priority_enabled_async"
        )
        self._write_to_db({"priority_enabled": value})
        self._priority_enabled = value

    async def set_priority_enabled_async(self, value: bool) -> None:
        """Deprecated. Priority is always enabled."""
        await self._configure_thread_pool()
        await asyncio.to_thread(self.set_priority_enabled, value)

    @property
    def partition_queue(self) -> bool:
        """Deprecated. Use the partition_* limits."""
        if self.database_backed_queue:
            _warn_sync_db_call_in_async_context(
                "Queue.partition_queue", "Queue.get_partition_queue_async"
            )
            self._refresh_fields(self._read_from_db())
        return self._partition_queue

    async def get_partition_queue_async(self) -> bool:
        """Deprecated. Use the partition_* limits."""
        if self.database_backed_queue:
            await self._configure_thread_pool()
            self._refresh_fields(await asyncio.to_thread(self._read_from_db))
        return self._partition_queue

    def set_partition_queue(self, value: bool) -> None:
        """Deprecated. Use the set_partition_* setters."""
        self._require_database_backed()
        _warn_sync_db_call_in_async_context(
            "Queue.set_partition_queue", "Queue.set_partition_queue_async"
        )
        # Refresh so the check below sees the latest partition limits.
        self._refresh_fields(self._read_from_db())
        if self._has_partition_limits():
            raise DBOSException(
                f"Cannot set partition_queue on queue {self.name}: it is partitioned "
                "by its partition_* limits. Clear those instead."
            )
        self._write_to_db({"partition_queue": value})
        self._partition_queue = value

    async def set_partition_queue_async(self, value: bool) -> None:
        """Deprecated. Use the set_partition_*_async setters."""
        await self._configure_thread_pool()
        await asyncio.to_thread(self.set_partition_queue, value)

    @property
    def polling_interval_sec(self) -> float:
        if self.database_backed_queue:
            _warn_sync_db_call_in_async_context(
                "Queue.polling_interval_sec",
                "Queue.get_polling_interval_sec_async",
            )
            self._refresh_fields(self._read_from_db())
        return self._polling_interval_sec

    async def get_polling_interval_sec_async(self) -> float:
        if self.database_backed_queue:
            await self._configure_thread_pool()
            self._refresh_fields(await asyncio.to_thread(self._read_from_db))
        return self._polling_interval_sec

    def set_polling_interval_sec(self, value: float) -> None:
        self._require_database_backed()
        if value <= 0.0:
            raise ValueError("polling_interval_sec must be positive")
        _warn_sync_db_call_in_async_context(
            "Queue.set_polling_interval_sec",
            "Queue.set_polling_interval_sec_async",
        )
        self._write_to_db({"polling_interval_sec": value})
        self._polling_interval_sec = value

    async def set_polling_interval_sec_async(self, value: float) -> None:
        await self._configure_thread_pool()
        await asyncio.to_thread(self.set_polling_interval_sec, value)

    def _require_dbos_bound(self) -> None:
        if self._client_system_database is not None:
            raise DBOSException(
                f"Cannot enqueue on queue {self.name} from a client-bound Queue "
                "object. Use DBOSClient.enqueue instead."
            )

    def _validate_enqueue(self, ctx: Optional[DBOSContext]) -> None:
        self._require_dbos_bound()
        if ctx and ctx.queue_partition_key and ctx.deduplication_id:
            raise Exception("Deduplication is not supported for partitioned queues")
        # Skip validation for database-backed queues to avoid a roundtrip fetching the queue
        if self.database_backed_queue:
            return
        if self._partition_queue and (ctx is None or ctx.queue_partition_key is None):
            raise Exception(
                f"A workflow cannot be enqueued on partitioned queue {self.name} without a partition key"
            )
        if ctx and ctx.queue_partition_key and not self._partition_queue:
            raise Exception(
                f"You can only use a partition key on a partition-enabled queue. Key {ctx.queue_partition_key} was used with non-partitioned queue {self.name}"
            )

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

        # To allow safe concurrent async operations, all context management
        # must run synchronously before the first `await`.
        ctx = get_local_dbos_context()
        parent_ctx_copy = copy.copy(ctx)
        child_ctx = DBOSContext.create_start_workflow_child(ctx)
        self._validate_enqueue(ctx)
        dbos = _get_dbos_instance()
        await self._configure_thread_pool()
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

    def start_dequeued_workflows(workflow_ids: List[str]) -> None:
        """Fetch the claimed workflows' statuses in one round trip, then dispatch each."""
        try:
            found = {
                status["workflow_uuid"]: status
                for status in dbos._sys_db.get_workflow_statuses(workflow_ids)
            }
        except Exception as e:
            dbos.logger.warning(f"Error fetching dequeued workflow statuses: {e}")
            found = {}
        for id in workflow_ids:
            try:
                status = found.get(id) or dbos._sys_db.get_workflow_status(id)
                if status is None:
                    raise DBOSRecoveryError(id, "Workflow status not found")
                execute_dequeued_workflow(dbos, status)
            except Exception as e:
                dbos.logger.error(f"Error executing workflow {id}: {e}")

    def worker_budget(limits: ResolvedQueueLimits, running: int) -> int:
        """Room left under this worker's queue-wide concurrency limit, given how many of
        its workflows are already running or claimed."""
        if limits.partition_worker_concurrency == 0:
            # Zero per partition pauses this worker: no partition may run anything here, and the batched sweep enforces no per-partition worker limit of its own.
            return 0
        if limits.worker_concurrency is None:
            # A non-zero per-partition worker limit is enforced per partition instead.
            return sys.maxsize
        return max(0, limits.worker_concurrency - running)

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
            limits = queue._resolve_limits()
            if not queue._partition_queue:
                dequeued_workflows = dbos._sys_db.start_queued_workflows(
                    queue,
                    GlobalParams.executor_id,
                    GlobalParams.app_version,
                    None,
                    dbos._active_workflows_set.count_for_queue(queue.name),
                )
                start_dequeued_workflows(dequeued_workflows)
            elif (
                limits.partition_concurrency == 1
                and limits.global_concurrency is None
                and limits.limiter is None
                and limits.partition_limiter is None
            ):
                # Optimization: Batch dequeue if partition concurrency is 1
                max_tasks = worker_budget(
                    limits, dbos._active_workflows_set.count_for_queue(queue.name)
                )
                if max_tasks > 0:
                    dequeued_workflows = (
                        dbos._sys_db.start_queued_partitioned_workflows(
                            queue,
                            GlobalParams.executor_id,
                            GlobalParams.app_version,
                            max_tasks,
                        )
                    )
                    start_dequeued_workflows(dequeued_workflows)
            else:
                # Iterate through partitions one at a time in random order to prevent starvation.
                partition_keys = dbos._sys_db.get_queue_partitions(queue.name)
                random.shuffle(partition_keys)
                # Snapshot once: re-reading would count this sweep's own claims twice, since dispatch is asynchronous and `claimed` already accounts for them.
                running = dbos._active_workflows_set.count_for_queue(queue.name)
                claimed = 0
                for key in partition_keys:
                    if worker_budget(limits, running + claimed) <= 0:
                        break
                    try:
                        dequeued_workflows = dbos._sys_db.start_queued_workflows(
                            queue,
                            GlobalParams.executor_id,
                            GlobalParams.app_version,
                            key,
                            running + claimed,
                            dbos._active_workflows_set.count_for_partition(
                                queue.name, key
                            ),
                        )
                    except OperationalError as e:
                        # Lock held or claim raced by another worker: skip just this partition, no queue-wide backoff.
                        if isinstance(
                            e.orig,
                            (errors.LockNotAvailable, errors.SerializationFailure),
                        ):
                            continue
                        raise
                    claimed += len(dequeued_workflows)
                    start_dequeued_workflows(dequeued_workflows)
        except OperationalError as e:
            if isinstance(e.orig, errors.LockNotAvailable):
                # Another worker is dequeueing this queue right now; retry next
                # poll without backing off.
                dbos.logger.debug(
                    f"Queue {queue.name} is locked by another worker; retrying next poll."
                )
            elif isinstance(e.orig, errors.SerializationFailure):
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
                for queue in dbos._sys_db.list_queues(
                    application_name=dbos._sys_db.app_name
                ):
                    if queue.name in listening_set and queue.name not in current_queues:
                        current_queues[queue.name] = queue
            except Exception as e:
                dbos.logger.warning(f"Exception listing database-backed queues: {e}")
            # Always listen to the internal queue
            current_queues[INTERNAL_QUEUE_NAME] = dbos._registry.get_internal_queue()
            # Always poll this process's poller-fed queues (e.g. Kafka), else their workflows sit ENQUEUED forever under a listen_queues filter; snapshot since a late poller may mutate the set.
            for name in list(dbos._registry.poller_queue_names):
                if name in current_queues:
                    continue
                q = dbos._registry.queue_info_map.get(name)
                if q is None:
                    # Database-backed queues (e.g. from register_queue) aren't in the in-memory registry; resolve from the DB.
                    try:
                        q = dbos._sys_db.get_queue(name)
                    except Exception as e:
                        dbos.logger.warning(
                            f"Exception resolving poller queue {name}: {e}"
                        )
                        continue
                if q is not None:
                    current_queues[name] = q
        else:
            # Else, check all in-memory and database-backed queues
            current_queues = dict(dbos._registry.queue_info_map)
            try:
                for queue in dbos._sys_db.list_queues(
                    application_name=dbos._sys_db.app_name
                ):
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
    if q._has_partition_limits():
        if q._concurrency is not None:
            opts.append(f"global_concurrency={q._concurrency}")
    elif q._concurrency is not None:
        opts.append(f"concurrency={q._concurrency}")
    if q._worker_concurrency is not None:
        opts.append(f"worker_concurrency={q._worker_concurrency}")
    if q._limiter is not None:
        opts.append(f"limit={q._limiter['limit']}/{q._limiter['period']}s")
    if q._partition_concurrency is not None:
        opts.append(f"partition_concurrency={q._partition_concurrency}")
    if q._partition_worker_concurrency is not None:
        opts.append(f"partition_worker_concurrency={q._partition_worker_concurrency}")
    if q._partition_limiter is not None:
        opts.append(
            f"partition_limit={q._partition_limiter['limit']}/{q._partition_limiter['period']}s"
        )
    if q._priority_enabled:
        opts.append("priority")
    if q._partition_queue:
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
        for q in dbos._sys_db.list_queues(application_name=dbos._sys_db.app_name):
            queues.setdefault(q.name, q)
    except Exception as e:
        dbos.logger.warning(f"Exception listing database-backed queues: {e}")

    if listening_queues is not None:
        # Poller-fed queues (e.g. Kafka) are always listened to, so reflect them here.
        listening_set = set(listening_queues) | dbos._registry.poller_queue_names
        queues = {n: q for n, q in queues.items() if n in listening_set}

    queues.pop(INTERNAL_QUEUE_NAME, None)

    dbos.logger.info(f"Listening to {len(queues)} queues:")
    for q in queues.values():
        log_queue(q)
