import asyncio
import datetime
import functools
import json
import random
import sys
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from dbos._debug_trigger import DebugTriggers
from dbos._utils import (
    INTERNAL_QUEUE_NAME,
    LoopAwareEvent,
    PollingLimiter,
    generate_uuid,
    retriable_postgres_exception,
    retriable_sqlite_exception,
)

from ._context import DBOSContext, get_local_dbos_context, validate_workflow_attributes
from ._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded,
    DBOSConflictingWorkflowError,
    DBOSException,
    DBOSNonExistentWorkflowError,
    DBOSQueueDeduplicatedError,
    DBOSUnexpectedStepError,
    DBOSWorkflowCancelledError,
    DBOSWorkflowConflictIDError,
)
from ._logger import dbos_logger
from ._outcome import NoResult
from ._schemas import SCHEMA_PLACEHOLDER
from ._schemas.system_database import SystemSchema
from ._serialization import (
    DBOSPortableJSON,
    Serializer,
    WorkflowInputs,
    WorkflowSerializationFormat,
    deserialize_exception,
    deserialize_value,
    safe_deserialize,
    serialize_value,
    serialize_value_as,
)

if TYPE_CHECKING:
    from ._queue import Queue, QueueRateLimit


def queue_from_db_row(
    row: sa.Row[Any],
    client_system_database: Optional["SystemDatabase"] = None,
) -> "Queue":
    """Build a database-backed Queue from a queues-table row."""
    from ._queue import Queue

    m = row._mapping
    limiter: Optional["QueueRateLimit"] = None
    if m["rate_limit_max"] is not None:
        limiter = {
            "limit": m["rate_limit_max"],
            "period": m["rate_limit_period_sec"],
        }
    return Queue(
        m["name"],
        m["concurrency"],
        limiter,
        worker_concurrency=m["worker_concurrency"],
        priority_enabled=bool(m["priority_enabled"]),
        partition_queue=bool(m["partition_queue"]),
        polling_interval_sec=m["polling_interval_sec"],
        application_name=m["application_name"],
        database_backed_queue=True,
        client_system_database=client_system_database,
    )


class WorkflowStatusString(Enum):
    """Enumeration of values allowed for `WorkflowSatusInternal.status`."""

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    MAX_RECOVERY_ATTEMPTS_EXCEEDED = "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
    CANCELLED = "CANCELLED"
    ENQUEUED = "ENQUEUED"
    DELAYED = "DELAYED"


def workflow_is_active(status: str) -> bool:
    return (
        status == WorkflowStatusString.ENQUEUED.value
        or status == WorkflowStatusString.PENDING.value
        or status == WorkflowStatusString.DELAYED.value
    )


WorkflowStatuses = Literal[
    "PENDING",
    "SUCCESS",
    "ERROR",
    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
    "CANCELLED",
    "ENQUEUED",
    "DELAYED",
]


class WorkflowStatus:
    # The workflow ID
    workflow_id: str
    # The workflow status. Must be one of DELAYED, ENQUEUED, PENDING, SUCCESS, ERROR, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED
    status: WorkflowStatuses
    # The name of the workflow function
    name: str
    # The name of the workflow's class, if any
    class_name: Optional[str]
    # The name with which the workflow's class instance was configured, if any
    config_name: Optional[str]
    # The user who ran the workflow, if specified
    authenticated_user: Optional[str]
    # The role with which the workflow ran, if specified
    assumed_role: Optional[str]
    # All roles which the authenticated user could assume
    authenticated_roles: Optional[list[str]]
    # The deserialized workflow input object
    input: Optional[WorkflowInputs]
    # The workflow's output, if any
    output: Optional[Any] = None
    # The error the workflow threw, if any
    error: Optional[Exception] = None
    # Workflow start time, as a Unix epoch timestamp in ms
    created_at: Optional[int]
    # Last time the workflow status was updated, as a Unix epoch timestamp in ms
    updated_at: Optional[int]
    # If this workflow was enqueued, on which queue
    queue_name: Optional[str]
    # The executor to most recently execute this workflow
    executor_id: Optional[str]
    # The application version on which this workflow was started
    app_version: Optional[str]
    # The start-to-close timeout of the workflow in ms
    workflow_timeout_ms: Optional[int]
    # The deadline of a workflow, computed by adding its timeout to its start time.
    workflow_deadline_epoch_ms: Optional[int]
    # Unique ID for deduplication on a queue
    deduplication_id: Optional[str]
    # Priority of the workflow on the queue, starting from 1 ~ 2,147,483,647. Default 0 (highest priority).
    priority: Optional[int]
    # If this workflow is enqueued on a partitioned queue, its partition key
    queue_partition_key: Optional[str]
    # If this workflow was forked from another, that workflow's ID.
    forked_from: Optional[str]
    # Whether this workflow has been forked from by another workflow.
    was_forked_from: bool
    # If this workflow was started as a child of another workflow, that workflow's ID.
    parent_workflow_id: Optional[str]
    # The UNIX epoch timestamp at which the workflow was last dequeued, if it had been enqueued
    dequeued_at: Optional[int]
    # The UNIX epoch timestamp before which the workflow should not be dequeued
    delay_until_epoch_ms: Optional[int]
    # The UNIX epoch timestamp at which the workflow completed (SUCCESS, ERROR,
    # or CANCELLED). None if the workflow has not completed.
    completed_at: Optional[int]
    # Custom key-value attributes attached to the workflow at creation and
    # optionally updated afterward via update_workflow_attributes
    attributes: Optional[Dict[str, Any]]
    # If this workflow was enqueued by a named schedule, that schedule's name
    schedule_name: Optional[str]
    # Owning application; None if unclaimed, in which case any application may run it.
    application_name: Optional[str]

    # INTERNAL FIELDS

    # The ID of the application executing this workflow
    app_id: Optional[str]
    # The number of times this workflow's execution has been attempted
    recovery_attempts: Optional[int]


class WorkflowStatusInternal(TypedDict):
    workflow_uuid: str
    status: WorkflowStatuses
    name: str
    class_name: Optional[str]
    config_name: Optional[str]
    authenticated_user: Optional[str]
    assumed_role: Optional[str]
    authenticated_roles: Optional[str]  # JSON list of roles
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)
    created_at: Optional[int]  # Unix epoch timestamp in ms
    updated_at: Optional[int]  # Unix epoch timestamp in ms
    queue_name: Optional[str]
    executor_id: Optional[str]
    app_version: Optional[str]
    app_id: Optional[str]
    recovery_attempts: Optional[int]
    workflow_timeout_ms: Optional[int]
    workflow_deadline_epoch_ms: Optional[int]
    deduplication_id: Optional[str]
    priority: int
    inputs: str
    queue_partition_key: Optional[str]
    forked_from: Optional[str]
    parent_workflow_id: Optional[str]
    started_at_epoch_ms: Optional[int]
    serialization: Optional[str]
    owner_xid: Optional[str]
    delay_until_epoch_ms: Optional[int]
    attributes: Optional[Dict[str, Any]]
    schedule_name: Optional[str]
    # Absolute cap (Unix epoch ms) beyond which bounces may not extend the delay; None if not debounced or no timeout.
    debounce_deadline_epoch_ms: Optional[int]
    # True if this workflow's dedup ID is a debounce key to clear on the DELAYED->ENQUEUED transition.
    is_debounced: bool
    # Owning application; None writes an unclaimed row.
    application_name: Optional[str]


class MetricData(TypedDict):
    """
    Metrics data for workflows and steps within a time range.
    """

    metric_type: str  # Type of metric: "workflow" or "step"
    metric_name: str  # Name of the workflow or step
    value: int  # Number of times the operation ran in the time interval


class EnqueueOptionsInternal(TypedDict):
    # Unique ID for deduplication on a queue
    deduplication_id: Optional[str]
    # Priority of the workflow on the queue, starting from 1 ~ 2,147,483,647. Default 0 (highest priority).
    priority: Optional[int]
    # On what version the workflow is enqueued. Current version if not specified.
    app_version: Optional[str]
    # If the workflow is enqueued on a partitioned queue, its partition key
    queue_partition_key: Optional[str]
    # The UNIX epoch timestamp before which the workflow should not be dequeued
    delay_until_epoch_ms: Optional[int]
    # Absolute cap (Unix epoch ms) beyond which bounces may not extend the delay; None if not debounced or no timeout.
    debounce_deadline_epoch_ms: Optional[int]
    # True if this workflow's dedup ID is a debounce key to clear on the DELAYED->ENQUEUED transition.
    is_debounced: bool
    # The application the workflow is enqueued for; None means the enqueuer's own.
    application_name: Optional[str]


class DebounceResult(TypedDict):
    # The winner's workflow ID if an existing debounced DELAYED workflow was extended; None if no bounce occurred.
    bounced_workflow_id: Optional[str]
    # The current holder of (queue_name, deduplication_id) when no bounce occurred, or None if the key is unheld.
    holder_workflow_id: Optional[str]
    # Whether the holder is itself a debounced workflow.
    holder_is_debounced: bool
    # The holder's workflow name; a mismatch with the caller's means a debounce-key collision between workflows.
    holder_workflow_name: Optional[str]
    # The holder's owning application; a mismatch means the collision is across applications.
    holder_application_name: Optional[str]


class RecordedResult(TypedDict):
    output: Optional[str]  # Serialized
    error: Optional[str]  # Serialized
    serialization: Optional[str]
    child_workflow_id: Optional[str]


class OperationResultInternal(TypedDict):
    workflow_uuid: str
    function_id: int
    function_name: str
    output: Optional[str]  # Serialized
    error: Optional[str]  # Serialized
    serialization: Optional[str]
    started_at_epoch_ms: int


class GetEventWorkflowContext(TypedDict):
    workflow_uuid: str
    function_id: int
    timeout_function_id: int


class ExportedWorkflow(TypedDict):
    workflow_status: dict[str, Any]
    operation_outputs: list[dict[str, Any]]
    workflow_events: list[dict[str, Any]]
    workflow_events_history: list[dict[str, Any]]
    streams: list[dict[str, Any]]


class GetPendingWorkflowsOutput:
    def __init__(self, *, workflow_id: str):
        self.workflow_id: str = workflow_id


class WorkflowSchedule(TypedDict):
    schedule_id: str
    schedule_name: str
    workflow_name: str
    workflow_class_name: Optional[str]
    schedule: str
    status: str
    context: Any
    last_fired_at: Optional[str]
    automatic_backfill: bool
    cron_timezone: Optional[str]  # IANA timezone name, stored as string in DB
    queue_name: Optional[str]
    # Owning application; None leaves it unclaimed. Writers may name another.
    application_name: Optional[str]


class ClientScheduleInput(TypedDict, total=False):
    schedule_name: str
    workflow_name: str
    workflow_class_name: Optional[str]
    schedule: str
    context: Any
    automatic_backfill: bool
    cron_timezone: Optional[str]
    queue_name: Optional[str]
    # Owning application; unset falls back to the client's own.
    application_name: Optional[str]


class VersionInfo(TypedDict):
    version_id: str
    version_name: str
    version_timestamp: int
    created_at: int
    # Owning application; None if unclaimed.
    application_name: Optional[str]


# Workflows re-owned per transaction by a rename. Matches the GC default.
DEFAULT_RENAME_BATCH_SIZE = 10_000


class ApplicationRowCounts(TypedDict):
    """Rows a rename moved, by table."""

    queues: int
    schedules: int
    versions: int
    workflows: int
    steps: int


class StepInfo(TypedDict):
    # The unique ID of the step in the workflow
    function_id: int
    # The (fully qualified) name of the step
    function_name: str
    # The step's output, if any
    output: Optional[Any]
    # The error the step threw, if any
    error: Optional[Exception]
    # If the step starts or retrieves the result of a workflow, its ID
    child_workflow_id: Optional[str]
    # The Unix epoch timestamp at which this step started
    started_at_epoch_ms: Optional[int]
    # The Unix epoch timestamp at which this step completed
    completed_at_epoch_ms: Optional[int]


class WorkflowAggregateRow(TypedDict):
    group: Dict[str, Optional[str]]
    count: Optional[int]
    min_created_at: Optional[int]
    max_queue_wait_ms: Optional[int]
    max_total_latency_ms: Optional[int]


class StepAggregateRow(TypedDict):
    group: Dict[str, Optional[str]]
    count: Optional[int]
    max_duration_ms: Optional[int]


class NotificationInfo(TypedDict):
    topic: Optional[str]
    message: Any
    created_at_epoch_ms: int
    consumed: bool


_dbos_null_topic = "__null__topic__"
_dbos_stream_closed_sentinel = "__DBOS_STREAM_CLOSED__"

# LISTEN/NOTIFY channels; streams and workflow_events are pushed by run_notifier, while notifications fires from an in-transaction DB trigger so recv is never woken before its row commits.
_dbos_notifications_channel = "dbos_notifications_channel"
_dbos_workflow_events_channel = "dbos_workflow_events_channel"
_dbos_streams_channel = "dbos_streams_channel"

# Returned by read_stream_value when nothing is written at the requested offset. Not None, which is itself a valid stream value.
_no_stream_value = object()


@dataclass
class SendMessage:
    """A single message to send as part of a bulk send operation."""

    destination_id: str
    message: Any
    topic: Optional[str] = None
    idempotency_key: Optional[str] = None


class EventCount(TypedDict):
    event: LoopAwareEvent
    count: int
    components: Tuple[str, str]


class ThreadSafeEventDict:
    def __init__(self) -> None:
        self._dict: Dict[str, EventCount] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[LoopAwareEvent]:
        with self._lock:
            if key not in self._dict:
                return None
            return self._dict[key]["event"]

    def set(
        self,
        key: str,
        value: LoopAwareEvent,
        components: Tuple[str, str],
    ) -> tuple[bool, LoopAwareEvent]:
        with self._lock:
            if key in self._dict:
                # Key already exists, do not overwrite. Increment the wait count.
                ec = self._dict[key]
                ec["count"] += 1
                return False, ec["event"]
            self._dict[key] = EventCount(event=value, count=1, components=components)
            return True, value

    def pop(self, key: str) -> None:
        with self._lock:
            if key in self._dict:
                ec = self._dict[key]
                ec["count"] -= 1
                if ec["count"] == 0:
                    del self._dict[key]
            else:
                dbos_logger.warning(f"Key {key} not found in event dictionary.")

    def snapshot(self) -> List[Tuple[str, Tuple[str, str], LoopAwareEvent]]:
        """Return a snapshot of (key, components, event) for every entry."""
        with self._lock:
            return [
                (key, ec["components"], ec["event"]) for key, ec in self._dict.items()
            ]


# Return type of the recv/get_event setup phases: either a cached result
# (OAOO replay or value already available) or the registered event the
# caller must wait on.
EventSetupResult = Union[
    tuple[Literal[True], Any],
    tuple[Literal[False], LoopAwareEvent, float, str, int],
]


F = TypeVar("F", bound=Callable[..., Any])


def db_retry(
    initial_backoff: float = 1.0,
    max_backoff: float = 60.0,
    *,
    sys_db: Optional["SystemDatabase"] = None,
) -> Callable[[F], F]:
    """
    If a workflow encounters a database connection issue while performing an operation,
    block the workflow and retry the operation until it reconnects and succeeds.

    In other words, if DBOS loses its database connection, everything pauses until the connection is recovered,
    trading off availability for correctness. A system database created with
    retry_connection_errors=False opts out, raising connection errors instead.

    Args:
        sys_db (SystemDatabase): The system database whose retry setting applies, for call sites where it is not the first argument (closures within a method).
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            db = sys_db if sys_db is not None else (args[0] if args else None)
            retries: int = 0
            backoff: float = initial_backoff
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:

                    # Determine if this is a retriable exception
                    postgres_retriable = retriable_postgres_exception(e)
                    # The SQLite heuristic matches rendered error text, which can include program data, so only trust it on an actual SQLite database.
                    sqlite_retriable = getattr(
                        db, "_is_sqlite", True
                    ) and retriable_sqlite_exception(e)
                    if not postgres_retriable and not sqlite_retriable:
                        raise

                    # Connection-error retries are optional; SQLite lock contention is not a connection error and always retries.
                    if postgres_retriable and not getattr(
                        db, "_retry_connection_errors", True
                    ):
                        raise

                    retries += 1
                    # Calculate backoff with jitter
                    actual_backoff: float = backoff * (0.5 + random.random())
                    dbos_logger.warning(
                        f"Database connection failed: {str(e)}. "
                        f"Retrying in {actual_backoff:.2f}s (attempt {retries})"
                    )
                    # Sleep with backoff
                    time.sleep(actual_backoff)
                    # Increase backoff for next attempt (exponential)
                    backoff = min(backoff * 2, max_backoff)

        return cast(F, wrapper)

    return decorator


# Fallback pool size for defaulting polling concurrency when the engine's is unknown (mirrors configure_db_engine_parameters).
DEFAULT_SYS_DB_POOL_SIZE = 20

# Interval for coalescing LISTEN/NOTIFY notifications off the write path; caps the rate of notifying commits regardless of write throughput.
DEFAULT_NOTIFICATION_COALESCE_SEC = 0.01


class SystemDatabase(ABC):

    @staticmethod
    def create(
        system_database_url: str,
        engine_kwargs: Dict[str, Any],
        engine: Optional[sa.Engine],
        schema: Optional[str],
        serializer: Serializer,
        executor_id: Optional[str],
        use_listen_notify: bool = True,
        notification_listener_polling_interval_sec: float = 1.0,
        notification_coalesce_sec: float = DEFAULT_NOTIFICATION_COALESCE_SEC,
        polling_concurrency: Optional[int] = None,
        app_name: Optional[str] = None,
        retry_connection_errors: bool = True,
    ) -> "SystemDatabase":
        """Factory method to create the appropriate SystemDatabase implementation based on URL."""
        if system_database_url.startswith("sqlite"):
            from ._sys_db_sqlite import SQLiteSystemDatabase

            return SQLiteSystemDatabase(
                system_database_url=system_database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
                executor_id=executor_id,
                use_listen_notify=use_listen_notify,
                notification_listener_polling_interval_sec=notification_listener_polling_interval_sec,
                notification_coalesce_sec=notification_coalesce_sec,
                polling_concurrency=polling_concurrency,
                app_name=app_name,
                retry_connection_errors=retry_connection_errors,
            )
        else:
            from ._sys_db_postgres import PostgresSystemDatabase

            return PostgresSystemDatabase(
                system_database_url=system_database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
                executor_id=executor_id,
                use_listen_notify=use_listen_notify,
                notification_listener_polling_interval_sec=notification_listener_polling_interval_sec,
                notification_coalesce_sec=notification_coalesce_sec,
                polling_concurrency=polling_concurrency,
                app_name=app_name,
                retry_connection_errors=retry_connection_errors,
            )

    def __init__(
        self,
        *,
        system_database_url: str,
        engine_kwargs: Dict[str, Any],
        engine: Optional[sa.Engine],
        schema: Optional[str],
        serializer: Serializer,
        executor_id: Optional[str],
        use_listen_notify: bool = True,
        notification_listener_polling_interval_sec: float = 1.0,
        notification_coalesce_sec: float = DEFAULT_NOTIFICATION_COALESCE_SEC,
        polling_concurrency: Optional[int] = None,
        app_name: Optional[str] = None,
        retry_connection_errors: bool = True,
    ):
        import sqlalchemy.dialects.postgresql as pg
        import sqlalchemy.dialects.sqlite as sq

        # Log system database connection information
        if engine:
            printable_sys_db_url = engine.url.render_as_string(hide_password=True)
            dbos_logger.info(
                f"Initializing DBOS system database with custom engine: {printable_sys_db_url} (schema: {schema})"
            )
        else:
            printable_sys_db_url = sa.make_url(system_database_url).render_as_string(
                hide_password=True
            )
            dbos_logger.info(
                f"Initializing DBOS system database with URL: {printable_sys_db_url} (schema: {schema})"
            )
            if system_database_url.startswith("sqlite"):
                dbos_logger.info(
                    f"Using SQLite as a system database. The SQLite system database is for development and testing. PostgreSQL is recommended for production use."
                )
            else:
                dbos_logger.info(
                    f"DBOS system database engine parameters: {engine_kwargs}"
                )

        # Configure and initialize the system database
        self.dialect = sq if system_database_url.startswith("sqlite") else pg
        self.serializer = serializer
        self.use_listen_notify = use_listen_notify
        # Whether db_retry blocks on a lost connection until it recovers, or raises.
        self._retry_connection_errors = retry_connection_errors
        # db_retry trusts the text-based SQLite retriability heuristic only on SQLite.
        self._is_sqlite = system_database_url.startswith("sqlite")

        if system_database_url.startswith("sqlite"):
            self.schema = None
        else:
            self.schema = schema if schema else "dbos"

        if engine:
            base_engine = engine
            self.created_engine = False
        else:
            base_engine = self._create_engine(system_database_url, engine_kwargs)
            self.created_engine = True
        # Translate the placeholder schema to this instance's schema per-engine (None for SQLite = unqualified).
        self.engine = base_engine.execution_options(
            schema_translate_map={SCHEMA_PLACEHOLDER: self.schema}
        )
        self._engine_kwargs = engine_kwargs

        # Cap concurrent polling reads (default half the pool, min 1; non-positive disables) so a storm can't starve the control plane. See PollingLimiter.
        effective_pool_size = self._get_effective_pool_size(base_engine, engine_kwargs)
        polling_limit = (
            polling_concurrency
            if polling_concurrency is not None
            else max(1, effective_pool_size // 2)
        )
        self.poll_limiter = PollingLimiter(polling_limit)

        self.notifications_map = ThreadSafeEventDict()
        self.workflow_events_map = ThreadSafeEventDict()
        self.streams_map = ThreadSafeEventDict()
        self.executor_id = executor_id
        # The application this handle acts for; None writes unclaimed rows.
        self.app_name = app_name
        self._notification_listener_polling_interval_sec = (
            notification_listener_polling_interval_sec
        )
        self._notification_coalesce_sec = notification_coalesce_sec

        # Coalesced NOTIFY payloads keyed by channel, flushed off the write path by run_notifier (Postgres + L/N only).
        self._pending_notifications: Dict[str, Set[str]] = {}
        self._notifier_lock = threading.Lock()

        self._listener_thread_lock = threading.Lock()
        self._listener_running = False

        # Per-(queue, partition key) created_at cursors: keep per-key queue order monotonic across batches
        self._batch_created_at_lock = threading.Lock()
        self._batch_created_at_cursors: Dict[Tuple[Optional[str], str], int] = {}

        # Now we can run background processes
        self._run_background_processes = True

    @staticmethod
    def _get_effective_pool_size(
        engine: sa.Engine, engine_kwargs: Dict[str, Any]
    ) -> int:
        """Determine the system database pool's effective max size, used to
        default the polling concurrency to half the pool.

        Prefer the engine's actual configured pool size (so a custom engine is
        respected), then the configured ``pool_size`` kwarg, then a default. A
        NullPool reports no size; fall through to the default in that case."""
        pool_size = getattr(engine.pool, "size", None)
        if callable(pool_size):
            try:
                actual = pool_size()
                if actual and actual > 0:
                    return int(actual)
            except Exception:
                pass
        configured = engine_kwargs.get("pool_size")
        if configured and configured > 0:
            return int(configured)
        return DEFAULT_SYS_DB_POOL_SIZE

    @abstractmethod
    def _create_engine(
        self, system_database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        """Create a database engine specific to the database type."""
        pass

    @abstractmethod
    def run_migrations(self) -> None:
        """Run database migrations specific to the database type."""
        pass

    # Destroy the pool when finished
    def destroy(self) -> None:
        self._run_background_processes = False
        self._cleanup_connections()
        if self.created_engine:
            self.engine.dispose()

    @abstractmethod
    def _cleanup_connections(self) -> None:
        """Clean up database-specific connections."""
        pass

    def _now_ms_sql(self) -> Any:
        # SQLite's CURRENT_TIMESTAMP is second-precision; use unixepoch('subsec') for ms.
        if self.engine.dialect.name == "sqlite":
            if sys.version_info >= (3, 12):
                return sa.func.unixepoch("subsec") * 1000
            return sa.func.strftime("%s", "now") * 1000
        return sa.func.extract("epoch", sa.func.now()) * 1000

    @staticmethod
    def _name_filter(
        col: sa.ColumnElement[Any], value: Optional[Union[str, List[str]]]
    ) -> sa.ColumnElement[bool]:
        """Rows owned by these applications plus unclaimed ones, which belong to
        every application. Unset, or empty, matches every application."""
        if not value:
            return sa.true()
        names = [value] if isinstance(value, str) else value
        return sa.or_(col.in_(names), col.is_(None))

    def _observability_filter(
        self, col: sa.ColumnElement[Any], value: Optional[Union[str, List[str]]]
    ) -> sa.ColumnElement[bool]:
        """_name_filter defaulted to this handle's own application: an unset filter
        scopes to what this application owns, not to every application's rows. A
        handle with no application of its own still matches every one."""
        return self._name_filter(col, value if value is not None else self.app_name)

    def _resolve_row_owner(
        self,
        conn: sa.Connection,
        table: sa.Table,
        name_col: sa.ColumnElement[Any],
        name: str,
        owner: Optional[str],
        kind: str,
    ) -> Optional[str]:
        """Owner to persist when writing a row that may already exist. A nameless writer
        leaves the owner intact; a named one collides only with a different name."""
        existing = conn.execute(
            sa.select(table.c.application_name).where(name_col == name)
        ).fetchone()
        if existing is None or existing[0] is None:
            return owner
        current: Optional[str] = existing[0]
        if owner is None or current == owner:
            return current
        # A version name is computed or pinned, so "pick another" is config advice, not a rename.
        take_a_new_name = (
            f"set a distinct application_version for '{owner}'"
            if kind == "Application version"
            else f"give '{owner}' a different {kind.lower()} name"
        )
        raise DBOSException(
            f"{kind} '{name}' is already registered by application "
            f"'{current}' in this system database. {kind} names must be "
            "unique across applications sharing a system database. Either "
            f"{take_a_new_name}, or, if '{current}' was renamed to '{owner}', "
            "re-own its rows first with dbos rename-application"
        )

    def _insert_workflow_status(
        self,
        status: WorkflowStatusInternal,
        conn: Union[sa.Connection, Session],
        *,
        owner_xid: Optional[str],
    ) -> tuple[WorkflowStatuses, Optional[int], bool]:
        """Insert or update workflow status using PostgreSQL upsert operations."""
        wf_status: WorkflowStatuses = status["status"]
        workflow_deadline_epoch_ms: Optional[int] = status["workflow_deadline_epoch_ms"]
        should_execute = True
        _enqueued_statuses = [
            WorkflowStatusString.ENQUEUED.value,
            WorkflowStatusString.DELAYED.value,
        ]

        # Values to update when a row already exists for this workflow.
        # recovery_attempts is absent by design: only the queue's claim counts a dispatch.
        update_values: dict[str, Any] = {
            "updated_at": self._now_ms_sql(),
        }
        # Don't update an existing executor ID when enqueueing a workflow.
        if wf_status not in _enqueued_statuses:
            update_values["executor_id"] = status["executor_id"]

        cmd = (
            self.dialect.insert(SystemSchema.workflow_status)
            .values(
                workflow_uuid=status["workflow_uuid"],
                status=status["status"],
                name=status["name"],
                class_name=status["class_name"],
                config_name=status["config_name"],
                output=status["output"],
                error=status["error"],
                executor_id=status["executor_id"],
                application_version=status["app_version"],
                application_id=status["app_id"],
                authenticated_user=status["authenticated_user"],
                authenticated_roles=status["authenticated_roles"],
                assumed_role=status["assumed_role"],
                queue_name=status["queue_name"],
                recovery_attempts=(1 if wf_status not in _enqueued_statuses else 0),
                workflow_timeout_ms=status["workflow_timeout_ms"],
                workflow_deadline_epoch_ms=status["workflow_deadline_epoch_ms"],
                deduplication_id=status["deduplication_id"],
                priority=status["priority"],
                inputs=status["inputs"],
                serialization=status["serialization"],
                queue_partition_key=status["queue_partition_key"],
                parent_workflow_id=status["parent_workflow_id"],
                owner_xid=owner_xid,
                delay_until_epoch_ms=status["delay_until_epoch_ms"],
                attributes=status["attributes"],
                schedule_name=status["schedule_name"],
                debounce_deadline_epoch_ms=status["debounce_deadline_epoch_ms"],
                is_debounced=status["is_debounced"],
                # Absent from update_values: a re-enqueue must not re-own a claimed row.
                application_name=status["application_name"],
            )
            .on_conflict_do_update(
                index_elements=["workflow_uuid"],
                set_=update_values,
            )
        )

        cmd = cmd.returning(
            SystemSchema.workflow_status.c.status,
            SystemSchema.workflow_status.c.workflow_deadline_epoch_ms,
            SystemSchema.workflow_status.c.name,
            SystemSchema.workflow_status.c.class_name,
            SystemSchema.workflow_status.c.config_name,
            SystemSchema.workflow_status.c.queue_name,
            SystemSchema.workflow_status.c.owner_xid,
            SystemSchema.workflow_status.c.serialization,
        )

        try:
            results = conn.execute(cmd)
        except DBAPIError as dbapi_error:
            # Unique constraint violation for the deduplication ID
            if self._is_unique_constraint_violation(dbapi_error):
                assert status["deduplication_id"] is not None
                assert status["queue_name"] is not None
                raise DBOSQueueDeduplicatedError(
                    status["workflow_uuid"],
                    status["queue_name"],
                    status["deduplication_id"],
                )
            else:
                raise

        row = results.fetchone()

        if row is not None:
            m = row._mapping
            # Check the started workflow matches the expected name, class_name, config_name, and queue_name
            # A mismatch indicates a workflow starting with the same UUID but different functions, which would throw an exception.
            wf_status = m["status"]
            workflow_deadline_epoch_ms = m["workflow_deadline_epoch_ms"]
            err_msg: Optional[str] = None
            if m["name"] != status["name"]:
                err_msg = f"Workflow already exists with a different function name: {m['name']}, but the provided function name is: {status['name']}"
            elif m["class_name"] != status["class_name"]:
                err_msg = f"Workflow already exists with a different class name: {m['class_name']}, but the provided class name is: {status['class_name']}"
            elif m["config_name"] != status["config_name"]:
                err_msg = f"Workflow already exists with a different config name: {m['config_name']}, but the provided config name is: {status['config_name']}"
            elif m["queue_name"] != status["queue_name"]:
                # This is a warning because a different queue name is not necessarily an error.
                dbos_logger.warning(
                    f"Workflow already exists in queue: {m['queue_name']}, but the provided queue name is: {status['queue_name']}. The queue is not updated."
                )
            if err_msg is not None:
                raise DBOSConflictingWorkflowError(status["workflow_uuid"], err_msg)

            if owner_xid != m["owner_xid"]:
                should_execute = False

            status["serialization"] = m["serialization"]

        return wf_status, workflow_deadline_epoch_ms, should_execute

    @db_retry()
    def dead_letter_workflows(
        self, workflow_ids: List[str], *, min_recovery_attempts: int
    ) -> None:
        """Move claimed workflows that exhausted their attempts off the queue.

        Guarded on PENDING like every other claim-owning write, and on the attempt
        count the decision was read from: a row someone else has already moved on,
        or given a fresh budget by resume, is left alone.
        """
        if not workflow_ids:
            return
        with self.engine.begin() as c:
            now_ms = self._now_ms_sql()
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid.in_(workflow_ids))
                .where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.PENDING.value
                )
                .where(
                    SystemSchema.workflow_status.c.recovery_attempts
                    >= min_recovery_attempts
                )
                .values(
                    status=WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value,
                    deduplication_id=None,
                    started_at_epoch_ms=None,
                    queue_name=None,
                    updated_at=now_ms,
                    completed_at=now_ms,
                )
            )

    @db_retry()
    def update_workflow_outcome(
        self,
        workflow_id: str,
        status: WorkflowStatuses,
        *,
        output: Optional[str] = None,
        error: Optional[str] = None,
    ) -> bool:
        """Record a workflow's terminal outcome, reporting whether the write landed.

        The write applies only to a PENDING row: a run owns its workflow's
        outcome exactly as long as the row says that run is what the workflow
        is doing. (Note: this does not prevent a write when another concurrent
        execution is already running and the status is PENDING. However, both
        executions should be deterministic and idempotent.)

        Returning False means the row was CANCELLED, dead-lettered, already
        terminal, handed to another execution (ENQUEUED/DELAYED, e.g. by a
        concurrent resume), or gone entirely.
        """
        with self.engine.begin() as c:
            now_ms = self._now_ms_sql()
            result = c.execute(
                sa.update(SystemSchema.workflow_status)
                .values(
                    status=status,
                    output=output,
                    error=error,
                    # As the workflow is complete, remove its deduplication ID
                    deduplication_id=None,
                    updated_at=now_ms,
                    completed_at=now_ms,
                )
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.PENDING.value
                )
            )
            return result.rowcount > 0

    def cancel_workflows(
        self,
        workflow_ids: list[str],
        cancel_children: bool = False,
    ) -> None:
        def _cancel_workflows(ids: list[str]) -> None:
            with self.engine.begin() as c:
                now_ms = self._now_ms_sql()
                # Set the workflows' status to CANCELLED and remove them from any
                # queue, but only if the workflow is not already complete.
                c.execute(
                    sa.update(SystemSchema.workflow_status)
                    .where(SystemSchema.workflow_status.c.workflow_uuid.in_(ids))
                    .where(
                        SystemSchema.workflow_status.c.status.notin_(
                            [
                                WorkflowStatusString.SUCCESS.value,
                                WorkflowStatusString.ERROR.value,
                            ]
                        )
                    )
                    .values(
                        status=WorkflowStatusString.CANCELLED.value,
                        queue_name=None,
                        deduplication_id=None,
                        started_at_epoch_ms=None,
                        updated_at=now_ms,
                        completed_at=now_ms,
                    )
                )

        if not cancel_children:
            _cancel_workflows(workflow_ids)
            return

        # Cascade child workflows level by level
        visited: set[str] = set(workflow_ids)
        frontier: list[str] = list(workflow_ids)
        while frontier:
            _cancel_workflows(frontier)
            children = self._get_direct_children(frontier)
            frontier = [c for c in children if c not in visited]
            visited.update(frontier)

    def resume_workflows(
        self,
        workflow_ids: list[str],
        *,
        queue_name: Optional[str] = None,
    ) -> None:
        with self.engine.begin() as c:
            # Check existence separately: a zero-row update also means "already complete", a legal no-op.
            existing = set(
                c.execute(
                    sa.select(SystemSchema.workflow_status.c.workflow_uuid).where(
                        SystemSchema.workflow_status.c.workflow_uuid.in_(workflow_ids)
                    )
                ).scalars()
            )
            missing = [wfid for wfid in workflow_ids if wfid not in existing]
            if missing:
                raise DBOSNonExistentWorkflowError("target", ", ".join(missing))
            # Set the workflows' status to ENQUEUED and clear recovery attempts and deadline,
            # but only if the workflow is not already complete.
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid.in_(workflow_ids))
                .where(
                    SystemSchema.workflow_status.c.status.notin_(
                        [
                            WorkflowStatusString.SUCCESS.value,
                            WorkflowStatusString.ERROR.value,
                        ]
                    )
                )
                .values(
                    status=WorkflowStatusString.ENQUEUED.value,
                    queue_name=(
                        queue_name if queue_name is not None else INTERNAL_QUEUE_NAME
                    ),
                    recovery_attempts=0,
                    workflow_deadline_epoch_ms=None,
                    deduplication_id=None,
                    started_at_epoch_ms=None,
                    updated_at=self._now_ms_sql(),
                    completed_at=None,
                )
            )

    def set_workflow_delay(
        self,
        workflow_id: str,
        *,
        delay_seconds: Optional[float] = None,
        delay_until_epoch_ms: Optional[int] = None,
    ) -> None:
        """Set or update the delay on a workflow. Only affects DELAYED workflows."""
        if delay_until_epoch_ms is not None and delay_seconds is not None:
            raise DBOSException(
                "Specify either delay_seconds or delay_until_epoch_ms, not both"
            )
        if delay_until_epoch_ms is not None:
            if delay_until_epoch_ms < 0:
                raise DBOSException("delay_until_epoch_ms must be >= 0")
            resolved = delay_until_epoch_ms
        elif delay_seconds is not None:
            if delay_seconds < 0:
                raise DBOSException("delay_seconds must be >= 0")
            resolved = int((time.time() + delay_seconds) * 1000)
        else:
            raise DBOSException(
                "Must specify either delay_seconds or delay_until_epoch_ms"
            )
        with self.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.DELAYED.value
                )
                .values(
                    delay_until_epoch_ms=resolved,
                    updated_at=self._now_ms_sql(),
                )
            )

    def debounce_delayed_workflow(
        self,
        *,
        workflow_name: str,
        queue_name: str,
        deduplication_id: str,
        delay_until_epoch_ms: int,
        inputs: str,
        serialization: Optional[str],
        application_name: Optional[str],
        conn: Optional[sa.Connection] = None,
    ) -> DebounceResult:
        """Extend an existing debounced DELAYED workflow's delay and update its inputs.

        Performed as a single atomic transaction. The new delay is capped at the
        workflow's debounce_deadline_epoch_ms, if one is set. Matching on
        workflow_name ensures a debounce-key collision between different workflows
        (e.g. "a"+"b-c" vs "a-b"+"c") never overwrites another workflow's inputs.
        The bounce acts for ``application_name``: it extends only that
        application's holders plus unclaimed ones, claiming those for it.
        If nothing matched, returns the current holder (or that the key is unheld)
        so the caller can decide whether to start fresh or surface a conflict.

        Runs on ``conn`` if given, joining its transaction (e.g. a checkpointed
        step's via call_txn_as_step); otherwise in its own retried transaction.
        """

        def _do(c: sa.Connection) -> DebounceResult:
            wsc = SystemSchema.workflow_status.c
            # Cap the new delay at the debounce deadline, if any (CASE not LEAST/min, for Postgres/SQLite portability).
            capped_delay = sa.case(
                (
                    sa.and_(
                        wsc.debounce_deadline_epoch_ms.isnot(None),
                        wsc.debounce_deadline_epoch_ms < delay_until_epoch_ms,
                    ),
                    wsc.debounce_deadline_epoch_ms,
                ),
                else_=delay_until_epoch_ms,
            )
            updated = c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(wsc.name == workflow_name)
                .where(wsc.queue_name == queue_name)
                .where(wsc.deduplication_id == deduplication_id)
                .where(wsc.status == WorkflowStatusString.DELAYED.value)
                .where(wsc.is_debounced == True)
                # Never extend a workflow the target application doesn't own; falls through to the holder below.
                .where(self._name_filter(wsc.application_name, application_name))
                .values(
                    delay_until_epoch_ms=capped_delay,
                    inputs=inputs,
                    serialization=serialization,
                    updated_at=self._now_ms_sql(),
                    # Claim it for the target, as its dequeue would: left unclaimed, every peer coalesces onto the one workflow and the last inputs win.
                    application_name=sa.func.coalesce(
                        wsc.application_name, sa.literal(application_name)
                    ),
                )
                .returning(wsc.workflow_uuid)
            ).fetchone()
            if updated is not None:
                return {
                    "bounced_workflow_id": updated[0],
                    "holder_workflow_id": None,
                    "holder_is_debounced": False,
                    "holder_workflow_name": None,
                    "holder_application_name": None,
                }
            # Unscoped, so a holder that blocked the update above is reportable.
            holder = c.execute(
                sa.select(
                    wsc.workflow_uuid, wsc.is_debounced, wsc.name, wsc.application_name
                )
                .where(wsc.queue_name == queue_name)
                .where(wsc.deduplication_id == deduplication_id)
            ).fetchone()
            if holder is None:
                return {
                    "bounced_workflow_id": None,
                    "holder_workflow_id": None,
                    "holder_is_debounced": False,
                    "holder_workflow_name": None,
                    "holder_application_name": None,
                }
            return {
                "bounced_workflow_id": None,
                "holder_workflow_id": holder[0],
                "holder_is_debounced": bool(holder[1]),
                "holder_workflow_name": holder[2],
                "holder_application_name": holder[3],
            }

        if conn is not None:
            return _do(conn)

        @db_retry(sys_db=self)
        def _standalone() -> DebounceResult:
            with self.engine.begin() as c:
                return _do(c)

        return _standalone()

    def update_workflow_attributes(
        self, workflow_id: str, attributes: Optional[Dict[str, Any]]
    ) -> None:
        """Replace the custom attributes attached to a workflow. Pass None to clear all attributes."""
        validate_workflow_attributes(attributes)
        with self.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .values(
                    attributes=attributes,
                    updated_at=self._now_ms_sql(),
                )
            )

    def delete_workflows(self, workflow_ids: list[str]) -> None:
        """Delete workflows and all associated data from the system database."""
        with self.engine.begin() as c:
            c.execute(
                sa.delete(SystemSchema.workflow_status).where(
                    SystemSchema.workflow_status.c.workflow_uuid.in_(workflow_ids)
                )
            )

    def fork_workflow(
        self,
        original_workflow_ids: list[str],
        forked_workflow_ids: list[str],
        start_steps: list[int],
        *,
        application_version: Optional[str],
        queue_name: Optional[str] = None,
        queue_partition_key: Optional[str] = None,
        replacement_children: Optional[dict[str, str]] = None,
        workflow_timeout_ms: Optional[int] = None,
    ) -> list[str]:
        if not original_workflow_ids:
            return []
        if len(original_workflow_ids) != len(forked_workflow_ids) or len(
            original_workflow_ids
        ) != len(start_steps):
            raise ValueError(
                "original_workflow_ids, forked_workflow_ids, and start_steps "
                "must have the same length"
            )

        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.workflow_uuid,
                    SystemSchema.workflow_status.c.name,
                    SystemSchema.workflow_status.c.class_name,
                    SystemSchema.workflow_status.c.config_name,
                    SystemSchema.workflow_status.c.application_id,
                    SystemSchema.workflow_status.c.authenticated_user,
                    SystemSchema.workflow_status.c.authenticated_roles,
                    SystemSchema.workflow_status.c.assumed_role,
                    SystemSchema.workflow_status.c.inputs,
                    SystemSchema.workflow_status.c.serialization,
                    SystemSchema.workflow_status.c.attributes,
                    SystemSchema.workflow_status.c.application_name,
                ).where(
                    SystemSchema.workflow_status.c.workflow_uuid.in_(
                        original_workflow_ids
                    )
                )
            ).fetchall()

            status_by_id = {row[0]: row for row in rows}
            for original_workflow_id in original_workflow_ids:
                if original_workflow_id not in status_by_id:
                    raise DBOSNonExistentWorkflowError("target", original_workflow_id)
            statuses = [status_by_id[wid] for wid in original_workflow_ids]
            # One owner per fork, shared by its status row and its copied steps.
            fork_owners = {
                fork_id: (status[11] if status[11] is not None else self.app_name)
                for fork_id, status in zip(forked_workflow_ids, statuses)
            }
            # Bulk insert all forked workflow status rows in one statement.
            c.execute(
                sa.insert(SystemSchema.workflow_status).values(
                    [
                        dict(
                            workflow_uuid=forked_workflow_id,
                            status=WorkflowStatusString.ENQUEUED.value,
                            name=status[1],
                            class_name=status[2],
                            config_name=status[3],
                            application_version=application_version,
                            application_id=status[4],
                            authenticated_user=status[5],
                            authenticated_roles=status[6],
                            serialization=status[9],
                            queue_name=(
                                queue_name
                                if queue_name is not None
                                else INTERNAL_QUEUE_NAME
                            ),
                            queue_partition_key=queue_partition_key,
                            inputs=status[8],
                            assumed_role=status[7],
                            forked_from=original_workflow_id,
                            attributes=status[10],
                            # Inherit the source's owner so the fork runs on the same application; claim an unclaimed one, as dequeue does.
                            application_name=fork_owners[forked_workflow_id],
                            workflow_timeout_ms=workflow_timeout_ms,
                        )
                        for original_workflow_id, forked_workflow_id, status in zip(
                            original_workflow_ids, forked_workflow_ids, statuses
                        )
                    ]
                )
            )

            # Mark the original workflows as having been forked from.
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(
                    SystemSchema.workflow_status.c.workflow_uuid.in_(
                        original_workflow_ids
                    )
                )
                .values(was_forked_from=True)
            )

            # For workflows with start_step > 1, copy checkpoints/events/streams.
            # Build a mapping subquery of (orig_id, fork_id, start_step) so that
            # each table copy is a single SQL statement regardless of batch size.
            fork_mappings = [
                (orig, fork, step)
                for orig, fork, step in zip(
                    original_workflow_ids, forked_workflow_ids, start_steps
                )
                if step > 1
            ]

            if fork_mappings:
                mapping_subquery = sa.union_all(
                    *[
                        sa.select(
                            sa.literal(orig_id).label("orig_id"),
                            sa.literal(fork_id).label("fork_id"),
                            sa.literal(step).label("start_step"),
                            # Cast, since an unclaimed fork makes this a bare NULL the union cannot type.
                            sa.cast(sa.literal(fork_owners[fork_id]), sa.Text).label(
                                "owner"
                            ),
                        )
                        for orig_id, fork_id, step in fork_mappings
                    ]
                ).subquery("mapping")

                oo = SystemSchema.operation_outputs

                child_wf_expr: sa.ColumnElement[Any] = oo.c.child_workflow_id
                if replacement_children:
                    child_wf_expr = sa.case(
                        *[
                            (
                                oo.c.child_workflow_id == old_id,
                                sa.literal(new_id),
                            )
                            for old_id, new_id in replacement_children.items()
                        ],
                        else_=oo.c.child_workflow_id,
                    )

                # Copy step checkpoints for all applicable workflows.
                c.execute(
                    sa.insert(oo).from_select(
                        [
                            "workflow_uuid",
                            "function_id",
                            "output",
                            "error",
                            "serialization",
                            "function_name",
                            "child_workflow_id",
                            "started_at_epoch_ms",
                            "completed_at_epoch_ms",
                            "application_name",
                        ],
                        sa.select(
                            mapping_subquery.c.fork_id.label("workflow_uuid"),
                            oo.c.function_id,
                            oo.c.output,
                            oo.c.error,
                            oo.c.serialization,
                            oo.c.function_name,
                            child_wf_expr,
                            oo.c.started_at_epoch_ms,
                            oo.c.completed_at_epoch_ms,
                            # Copied steps carry the owner the fork itself resolved to.
                            mapping_subquery.c.owner,
                        ).select_from(
                            mapping_subquery.join(
                                oo,
                                (oo.c.workflow_uuid == mapping_subquery.c.orig_id)
                                & (oo.c.function_id < mapping_subquery.c.start_step),
                            )
                        ),
                    )
                )

                weh = SystemSchema.workflow_events_history

                # Copy the workflow events history for all applicable workflows.
                c.execute(
                    sa.insert(weh).from_select(
                        [
                            "workflow_uuid",
                            "function_id",
                            "key",
                            "value",
                            "serialization",
                        ],
                        sa.select(
                            mapping_subquery.c.fork_id.label("workflow_uuid"),
                            weh.c.function_id,
                            weh.c.key,
                            weh.c.value,
                            weh.c.serialization,
                        ).select_from(
                            mapping_subquery.join(
                                weh,
                                (weh.c.workflow_uuid == mapping_subquery.c.orig_id)
                                & (weh.c.function_id < mapping_subquery.c.start_step),
                            )
                        ),
                    )
                )

                # Copy only the latest version of each workflow event using a window
                # function instead of a per-workflow correlated subquery.
                ranked = (
                    sa.select(
                        mapping_subquery.c.fork_id.label("workflow_uuid"),
                        weh.c.key,
                        weh.c.value,
                        weh.c.serialization,
                        sa.func.row_number()
                        .over(
                            partition_by=[weh.c.workflow_uuid, weh.c.key],
                            order_by=weh.c.function_id.desc(),
                        )
                        .label("rn"),
                    ).select_from(
                        mapping_subquery.join(
                            weh,
                            (weh.c.workflow_uuid == mapping_subquery.c.orig_id)
                            & (weh.c.function_id < mapping_subquery.c.start_step),
                        )
                    )
                ).subquery("ranked")

                c.execute(
                    sa.insert(SystemSchema.workflow_events).from_select(
                        [
                            "workflow_uuid",
                            "key",
                            "value",
                            "serialization",
                        ],
                        sa.select(
                            ranked.c.workflow_uuid,
                            ranked.c.key,
                            ranked.c.value,
                            ranked.c.serialization,
                        ).where(ranked.c.rn == 1),
                    )
                )

                streams = SystemSchema.streams

                # Copy streams for all applicable workflows.
                c.execute(
                    sa.insert(streams).from_select(
                        [
                            "workflow_uuid",
                            "function_id",
                            "key",
                            "value",
                            "serialization",
                            "offset",
                        ],
                        sa.select(
                            mapping_subquery.c.fork_id.label("workflow_uuid"),
                            streams.c.function_id,
                            streams.c.key,
                            streams.c.value,
                            streams.c.serialization,
                            streams.c.offset,
                        ).select_from(
                            mapping_subquery.join(
                                streams,
                                (streams.c.workflow_uuid == mapping_subquery.c.orig_id)
                                & (
                                    streams.c.function_id
                                    < mapping_subquery.c.start_step
                                ),
                            )
                        ),
                    )
                )

        return forked_workflow_ids

    def fork_from_failure(
        self,
        workflow_ids: list[str],
        *,
        application_version: Optional[str],
        queue_name: Optional[str] = None,
        queue_partition_key: Optional[str] = None,
        from_last_failure: bool = False,
        from_last_step: bool = False,
        from_step: Optional[int] = None,
        from_step_name: Optional[str] = None,
    ) -> list[str]:
        modes = sum(
            [
                from_last_failure,
                from_last_step,
                from_step is not None,
                from_step_name is not None,
            ]
        )
        if modes != 1:
            raise ValueError(
                "Exactly one of from_last_failure, from_last_step, from_step, "
                "or from_step_name must be specified"
            )

        if from_step is not None:
            start_steps = [from_step] * len(workflow_ids)
        else:
            oo = SystemSchema.operation_outputs
            with self.engine.begin() as c:
                if from_last_failure:
                    agg = sa.func.coalesce(
                        sa.func.max(oo.c.function_id).filter(oo.c.error.is_not(None)),
                        sa.func.max(oo.c.function_id),
                    ).label("start_step")
                    query = (
                        sa.select(oo.c.workflow_uuid, agg)
                        .where(oo.c.workflow_uuid.in_(workflow_ids))
                        .group_by(oo.c.workflow_uuid)
                    )
                elif from_last_step:
                    query = (
                        sa.select(
                            oo.c.workflow_uuid,
                            sa.func.max(oo.c.function_id).label("start_step"),
                        )
                        .where(oo.c.workflow_uuid.in_(workflow_ids))
                        .group_by(oo.c.workflow_uuid)
                    )
                else:
                    # from_step_name: find the last occurrence of the named step
                    query = (
                        sa.select(
                            oo.c.workflow_uuid,
                            sa.func.max(oo.c.function_id).label("start_step"),
                        )
                        .where(
                            oo.c.workflow_uuid.in_(workflow_ids)
                            & (oo.c.function_name == from_step_name)
                        )
                        .group_by(oo.c.workflow_uuid)
                    )

                rows = c.execute(query).fetchall()

            start_step_by_id = {row[0]: row[1] for row in rows}
            if from_step_name is not None:
                for wid in workflow_ids:
                    if wid not in start_step_by_id:
                        raise Exception(
                            f"Workflow {wid} has no step named '{from_step_name}'"
                        )

            # A workflow with no recorded steps has nothing to resume from, so restart it from step 1, the beginning.
            start_steps = [start_step_by_id.get(wid, 1) for wid in workflow_ids]

        forked_ids = [generate_uuid() for _ in workflow_ids]
        return self.fork_workflow(
            workflow_ids,
            forked_ids,
            start_steps,
            application_version=application_version,
            queue_name=queue_name,
            queue_partition_key=queue_partition_key,
        )

    def get_workflow_status(
        self, workflow_uuid: str
    ) -> Optional[WorkflowStatusInternal]:
        statuses = self.get_workflow_statuses([workflow_uuid])
        return statuses[0] if statuses else None

    def get_workflow_statuses(
        self, workflow_ids: List[str]
    ) -> List[WorkflowStatusInternal]:
        """Fetch many statuses in one round trip per chunk, in the order requested.

        IDs with no row are omitted, so the result may be shorter than the input.
        """
        ws = SystemSchema.workflow_status

        # Decorated per chunk so a reconnect retries one chunk, not the whole loop.
        @db_retry(sys_db=self)
        def fetch_chunk(chunk: List[str]) -> List[WorkflowStatusInternal]:
            with self.engine.begin() as c:
                rows = c.execute(
                    sa.select(
                        ws.c.workflow_uuid,
                        ws.c.status,
                        ws.c.name,
                        ws.c.recovery_attempts,
                        ws.c.config_name,
                        ws.c.class_name,
                        ws.c.authenticated_user,
                        ws.c.authenticated_roles,
                        ws.c.assumed_role,
                        ws.c.queue_name,
                        ws.c.executor_id,
                        ws.c.created_at,
                        ws.c.updated_at,
                        ws.c.application_version,
                        ws.c.application_id,
                        ws.c.workflow_deadline_epoch_ms,
                        ws.c.workflow_timeout_ms,
                        ws.c.deduplication_id,
                        ws.c.priority,
                        ws.c.inputs,
                        ws.c.queue_partition_key,
                        ws.c.forked_from,
                        ws.c.parent_workflow_id,
                        ws.c.started_at_epoch_ms,
                        ws.c.serialization,
                        ws.c.delay_until_epoch_ms,
                        ws.c.attributes,
                        ws.c.schedule_name,
                        ws.c.debounce_deadline_epoch_ms,
                        ws.c.is_debounced,
                        ws.c.application_name,
                    ).where(ws.c.workflow_uuid.in_(chunk))
                ).fetchall()
            # Keyed by column name, not position, so adding a column above cannot
            # silently shift every field. output/error/owner_xid are never selected.
            return [
                {
                    "workflow_uuid": m["workflow_uuid"],
                    "output": None,
                    "error": None,
                    "owner_xid": None,
                    "status": m["status"],
                    "name": m["name"],
                    "recovery_attempts": m["recovery_attempts"],
                    "config_name": m["config_name"],
                    "class_name": m["class_name"],
                    "authenticated_user": m["authenticated_user"],
                    "authenticated_roles": m["authenticated_roles"],
                    "assumed_role": m["assumed_role"],
                    "queue_name": m["queue_name"],
                    "executor_id": m["executor_id"],
                    "created_at": m["created_at"],
                    "updated_at": m["updated_at"],
                    "app_version": m["application_version"],
                    "app_id": m["application_id"],
                    "workflow_deadline_epoch_ms": m["workflow_deadline_epoch_ms"],
                    "workflow_timeout_ms": m["workflow_timeout_ms"],
                    "deduplication_id": m["deduplication_id"],
                    "priority": m["priority"],
                    "inputs": m["inputs"],
                    "queue_partition_key": m["queue_partition_key"],
                    "forked_from": m["forked_from"],
                    "parent_workflow_id": m["parent_workflow_id"],
                    "started_at_epoch_ms": m["started_at_epoch_ms"],
                    "serialization": m["serialization"],
                    "delay_until_epoch_ms": m["delay_until_epoch_ms"],
                    "attributes": m["attributes"],
                    "schedule_name": m["schedule_name"],
                    "debounce_deadline_epoch_ms": m["debounce_deadline_epoch_ms"],
                    "is_debounced": bool(m["is_debounced"]),
                    "application_name": m["application_name"],
                }
                for m in (row._mapping for row in rows)
            ]

        found: Dict[str, WorkflowStatusInternal] = {}
        # Chunk the IN list to stay under bind-parameter limits (SQLite caps at 32766, libpq at 65535).
        chunk_size = 4096
        for start in range(0, len(workflow_ids), chunk_size):
            for status in fetch_chunk(workflow_ids[start : start + chunk_size]):
                found[status["workflow_uuid"]] = status
        return [found[id] for id in workflow_ids if id in found]

    @db_retry()
    def _read_workflow_result_row(self, workflow_id: str) -> Optional[Any]:
        # Polling read under the limiter; acquired inside the db_retry body so the permit frees across backoff (like check_first_workflow_id).
        with self.poll_limiter, self.engine.begin() as c:
            return c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.status,
                    SystemSchema.workflow_status.c.output,
                    SystemSchema.workflow_status.c.error,
                    SystemSchema.workflow_status.c.serialization,
                ).where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
            ).fetchone()

    def check_workflow_result(
        self, workflow_id: str, *, fail_if_missing: bool = False
    ) -> Union[NoResult, Any]:
        """Check if a workflow has completed and return its result.

        Returns NoResult() if the workflow is still pending/enqueued/delayed/not found.
        Returns the deserialized output on success.
        Raises on error, cancellation, or max recovery attempts exceeded.

        A missing row normally means the workflow has not been inserted yet, so
        it reports NoResult() and the poll keeps waiting for the row to appear.
        Callers that know the row must already exist (e.g. a run parking on an
        outcome it just failed to write) pass fail_if_missing to get a
        DBOSNonExistentWorkflowError instead of polling forever.
        """
        row = self._read_workflow_result_row(workflow_id)
        if row is None and fail_if_missing:
            raise DBOSNonExistentWorkflowError("target", workflow_id)
        if row is not None:
            status = row[0]
            if status == WorkflowStatusString.SUCCESS.value:
                output = row[1]
                return deserialize_value(output, row[3], self.serializer)
            elif status == WorkflowStatusString.ERROR.value:
                error = row[2]
                e: Exception = deserialize_exception(error, row[3], self.serializer)
                raise e
            elif status == WorkflowStatusString.CANCELLED.value:
                # Raise AwaitedWorkflowCancelledError here, not the cancellation exception
                # because the awaiting workflow is not being cancelled.
                raise DBOSAwaitedWorkflowCancelledError(workflow_id)
            elif status == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value:
                raise DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded(workflow_id)
        return NoResult()

    def await_workflow_result(
        self,
        workflow_id: str,
        polling_interval: float,
        *,
        fail_if_missing: bool = False,
    ) -> Any:
        while True:
            result = self.check_workflow_result(
                workflow_id, fail_if_missing=fail_if_missing
            )
            if not isinstance(result, NoResult):
                return result
            time.sleep(polling_interval)

    async def await_workflow_result_async(
        self,
        workflow_id: str,
        polling_interval: float,
        *,
        fail_if_missing: bool = False,
    ) -> Any:
        while True:
            result = await asyncio.to_thread(
                self.check_workflow_result, workflow_id, fail_if_missing=fail_if_missing
            )
            if not isinstance(result, NoResult):
                return result
            await asyncio.sleep(polling_interval)

    @db_retry()
    def check_first_workflow_id(self, workflow_ids: List[str]) -> Union[NoResult, str]:
        """Check if at least one of the given workflows has completed.

        A workflow is considered complete when its status is not PENDING
        not ENQUEUED, and not DELAYED.  Returns the workflow_uuid of the first
        completed workflow found, or NoResult() if none have completed.
        """
        if not workflow_ids:
            raise ValueError("workflow_ids must not be empty")
        # This is a polling read (wait_first): run it under the polling limiter.
        with self.poll_limiter, self.engine.begin() as c:
            row = c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.workflow_uuid,
                )
                .where(
                    SystemSchema.workflow_status.c.workflow_uuid.in_(workflow_ids),
                    ~SystemSchema.workflow_status.c.status.in_(
                        [
                            WorkflowStatusString.PENDING.value,
                            WorkflowStatusString.ENQUEUED.value,
                            WorkflowStatusString.DELAYED.value,
                        ]
                    ),
                )
                .limit(1)
            ).fetchone()
            if row is not None:
                result: str = row[0]
                return result
        return NoResult()

    def await_first_workflow_id(
        self, workflow_ids: List[str], polling_interval: float
    ) -> str:
        while True:
            result = self.check_first_workflow_id(workflow_ids)
            if not isinstance(result, NoResult):
                return result
            time.sleep(polling_interval)

    async def await_first_workflow_id_async(
        self, workflow_ids: List[str], polling_interval: float
    ) -> str:
        while True:
            result = await asyncio.to_thread(self.check_first_workflow_id, workflow_ids)
            if not isinstance(result, NoResult):
                return result
            await asyncio.sleep(polling_interval)

    def list_workflows(
        self,
        *,
        workflow_ids: Optional[List[str]] = None,
        status: Optional[str | list[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        completed_after: Optional[str] = None,
        completed_before: Optional[str] = None,
        dequeued_after: Optional[str] = None,
        dequeued_before: Optional[str] = None,
        name: Optional[str | list[str]] = None,
        app_version: Optional[str | list[str]] = None,
        forked_from: Optional[str | list[str]] = None,
        parent_workflow_id: Optional[str | list[str]] = None,
        user: Optional[str | list[str]] = None,
        queue_name: Optional[str | list[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
        workflow_id_prefix: Optional[str | list[str]] = None,
        load_input: bool = True,
        load_output: bool = True,
        executor_id: Optional[str | list[str]] = None,
        queues_only: bool = False,
        was_forked_from: Optional[bool] = None,
        has_parent: Optional[bool] = None,
        attributes: Optional[Dict[str, Any]] = None,
        schedule_name: Optional[str | list[str]] = None,
        application_name: Optional[str | list[str]] = None,
    ) -> List[WorkflowStatus]:
        """
        Retrieve a list of workflows based on the search criteria.
        Returns a list of WorkflowStatus objects.
        """

        # Normalize string-or-list parameters to lists
        def _to_list(val: Optional[str | list[str]]) -> Optional[list[str]]:
            if val is None:
                return None
            return val if isinstance(val, list) else [val]

        status_list = _to_list(status)
        name_list = _to_list(name)
        app_version_list = _to_list(app_version)
        forked_from_list = _to_list(forked_from)
        parent_workflow_id_list = _to_list(parent_workflow_id)
        user_list = _to_list(user)
        queue_name_list = _to_list(queue_name)
        executor_id_list = _to_list(executor_id)
        prefix_list = _to_list(workflow_id_prefix)
        schedule_name_list = _to_list(schedule_name)

        load_columns = [
            SystemSchema.workflow_status.c.workflow_uuid,
            SystemSchema.workflow_status.c.status,
            SystemSchema.workflow_status.c.name,
            SystemSchema.workflow_status.c.recovery_attempts,
            SystemSchema.workflow_status.c.config_name,
            SystemSchema.workflow_status.c.class_name,
            SystemSchema.workflow_status.c.authenticated_user,
            SystemSchema.workflow_status.c.authenticated_roles,
            SystemSchema.workflow_status.c.assumed_role,
            SystemSchema.workflow_status.c.queue_name,
            SystemSchema.workflow_status.c.executor_id,
            SystemSchema.workflow_status.c.created_at,
            SystemSchema.workflow_status.c.updated_at,
            SystemSchema.workflow_status.c.application_version,
            SystemSchema.workflow_status.c.application_id,
            SystemSchema.workflow_status.c.workflow_deadline_epoch_ms,
            SystemSchema.workflow_status.c.workflow_timeout_ms,
            SystemSchema.workflow_status.c.deduplication_id,
            SystemSchema.workflow_status.c.priority,
            SystemSchema.workflow_status.c.queue_partition_key,
            SystemSchema.workflow_status.c.forked_from,
            SystemSchema.workflow_status.c.parent_workflow_id,
            SystemSchema.workflow_status.c.started_at_epoch_ms,
            SystemSchema.workflow_status.c.delay_until_epoch_ms,
            SystemSchema.workflow_status.c.was_forked_from,
            SystemSchema.workflow_status.c.completed_at,
            SystemSchema.workflow_status.c.attributes,
            SystemSchema.workflow_status.c.schedule_name,
            SystemSchema.workflow_status.c.application_name,
        ]
        if load_input:
            load_columns.append(SystemSchema.workflow_status.c.inputs)
        if load_output:
            load_columns.append(SystemSchema.workflow_status.c.output)
            load_columns.append(SystemSchema.workflow_status.c.error)
        if load_input or load_output:
            load_columns.append(SystemSchema.workflow_status.c.serialization)

        if queues_only:
            query = sa.select(*load_columns).where(
                SystemSchema.workflow_status.c.queue_name.isnot(None),
            )
            if not status_list:
                query = query.where(
                    SystemSchema.workflow_status.c.status.in_(
                        ["DELAYED", "ENQUEUED", "PENDING"]
                    )
                )
        else:
            query = sa.select(*load_columns)
        if sort_desc:
            query = query.order_by(SystemSchema.workflow_status.c.created_at.desc())
        else:
            query = query.order_by(SystemSchema.workflow_status.c.created_at.asc())
        if name_list:
            query = query.where(SystemSchema.workflow_status.c.name.in_(name_list))
        if schedule_name_list:
            query = query.where(
                SystemSchema.workflow_status.c.schedule_name.in_(schedule_name_list)
            )
        query = query.where(
            # A workflow ID is a global address, so an ID-keyed read is an identity
            # read: it takes an explicit filter but is never defaulted to this one.
            self._name_filter(
                SystemSchema.workflow_status.c.application_name, application_name
            )
            if workflow_ids
            else self._observability_filter(
                SystemSchema.workflow_status.c.application_name, application_name
            )
        )
        if user_list:
            query = query.where(
                SystemSchema.workflow_status.c.authenticated_user.in_(user_list)
            )
        if attributes:
            query = query.where(self._attributes_contains_clause(attributes))
        if start_time:
            query = query.where(
                SystemSchema.workflow_status.c.created_at
                >= datetime.datetime.fromisoformat(start_time).timestamp() * 1000
            )
        if end_time:
            query = query.where(
                SystemSchema.workflow_status.c.created_at
                <= datetime.datetime.fromisoformat(end_time).timestamp() * 1000
            )
        if completed_after:
            query = query.where(
                SystemSchema.workflow_status.c.completed_at
                >= datetime.datetime.fromisoformat(completed_after).timestamp() * 1000
            )
        if completed_before:
            query = query.where(
                SystemSchema.workflow_status.c.completed_at
                <= datetime.datetime.fromisoformat(completed_before).timestamp() * 1000
            )
        # dequeued_after/before filter on started_at_epoch_ms: that column is
        # populated on dequeue and surfaced as WorkflowStatus.dequeued_at.
        if dequeued_after:
            query = query.where(
                SystemSchema.workflow_status.c.started_at_epoch_ms
                >= datetime.datetime.fromisoformat(dequeued_after).timestamp() * 1000
            )
        if dequeued_before:
            query = query.where(
                SystemSchema.workflow_status.c.started_at_epoch_ms
                <= datetime.datetime.fromisoformat(dequeued_before).timestamp() * 1000
            )
        if status_list:
            query = query.where(SystemSchema.workflow_status.c.status.in_(status_list))
        if app_version_list:
            query = query.where(
                SystemSchema.workflow_status.c.application_version.in_(app_version_list)
            )
        if forked_from_list:
            query = query.where(
                SystemSchema.workflow_status.c.forked_from.in_(forked_from_list)
            )
        if parent_workflow_id_list:
            query = query.where(
                SystemSchema.workflow_status.c.parent_workflow_id.in_(
                    parent_workflow_id_list
                )
            )
        if workflow_ids:
            query = query.where(
                SystemSchema.workflow_status.c.workflow_uuid.in_(workflow_ids)
            )
        if prefix_list:
            query = query.where(
                sa.or_(
                    *[
                        SystemSchema.workflow_status.c.workflow_uuid.startswith(
                            p, autoescape=True
                        )
                        for p in prefix_list
                    ]
                )
            )
        if queue_name_list:
            query = query.where(
                SystemSchema.workflow_status.c.queue_name.in_(queue_name_list)
            )
        if executor_id_list:
            query = query.where(
                SystemSchema.workflow_status.c.executor_id.in_(executor_id_list)
            )
        if was_forked_from is not None:
            query = query.where(
                SystemSchema.workflow_status.c.was_forked_from == was_forked_from
            )
        if has_parent is not None:
            if has_parent:
                query = query.where(
                    SystemSchema.workflow_status.c.parent_workflow_id.isnot(None)
                )
            else:
                query = query.where(
                    SystemSchema.workflow_status.c.parent_workflow_id.is_(None)
                )
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)

        with self.engine.begin() as c:
            rows = c.execute(query).fetchall()

        infos: List[WorkflowStatus] = []
        for row in rows:
            info = WorkflowStatus()
            info.workflow_id = row[0]
            info.status = row[1]
            info.name = row[2]
            info.recovery_attempts = row[3]
            info.config_name = row[4]
            info.class_name = row[5]
            info.authenticated_user = row[6]
            info.authenticated_roles = (
                json.loads(row[7]) if row[7] is not None else None
            )
            info.assumed_role = row[8]
            info.queue_name = row[9]
            info.executor_id = row[10]
            info.created_at = row[11]
            info.updated_at = row[12]
            info.app_version = row[13]
            info.app_id = row[14]
            info.workflow_deadline_epoch_ms = row[15]
            info.workflow_timeout_ms = row[16]
            info.deduplication_id = row[17]
            info.priority = row[18]
            info.queue_partition_key = row[19]
            info.forked_from = row[20]
            info.parent_workflow_id = row[21]
            info.dequeued_at = row[22]
            info.delay_until_epoch_ms = row[23]
            info.was_forked_from = row[24]
            info.completed_at = row[25]
            info.attributes = row[26]
            info.schedule_name = row[27]
            info.application_name = row[28]

            idx = 29
            raw_input = row[idx] if load_input else None
            if load_input:
                idx += 1
            raw_output = row[idx] if load_output else None
            raw_error = row[idx + 1] if load_output else None
            if load_output:
                idx += 2
            serialization = row[idx] if load_input or load_output else None
            if load_input or load_output:
                idx += 1
            inputs, output, exception = safe_deserialize(
                self.serializer,
                serialization,
                info.workflow_id,
                serialized_input=raw_input,
                serialized_output=raw_output,
                serialized_exception=raw_error,
            )
            info.input = inputs
            info.output = output
            info.error = exception

            infos.append(info)
        return infos

    @db_retry()
    def get_pending_workflows(
        self, executor_id: str, app_version: str
    ) -> list[GetPendingWorkflowsOutput]:
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.workflow_uuid,
                ).where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.PENDING.value,
                    SystemSchema.workflow_status.c.executor_id == executor_id,
                    SystemSchema.workflow_status.c.application_version == app_version,
                    # executor_id defaults to "local", so it collides across applications.
                    self._name_filter(
                        SystemSchema.workflow_status.c.application_name, self.app_name
                    ),
                )
            ).fetchall()

            return [
                GetPendingWorkflowsOutput(workflow_id=row.workflow_uuid) for row in rows
            ]

    def list_workflow_steps(
        self,
        workflow_id: str,
        *,
        load_output: bool = True,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[StepInfo]:
        with self.engine.begin() as c:
            query = (
                sa.select(
                    SystemSchema.operation_outputs.c.function_id,
                    SystemSchema.operation_outputs.c.function_name,
                    SystemSchema.operation_outputs.c.output,
                    SystemSchema.operation_outputs.c.error,
                    SystemSchema.operation_outputs.c.child_workflow_id,
                    SystemSchema.operation_outputs.c.started_at_epoch_ms,
                    SystemSchema.operation_outputs.c.completed_at_epoch_ms,
                    SystemSchema.operation_outputs.c.serialization,
                )
                .where(SystemSchema.operation_outputs.c.workflow_uuid == workflow_id)
                .order_by(SystemSchema.operation_outputs.c.function_id)
            )
            if limit is not None:
                query = query.limit(limit)
            if offset is not None:
                query = query.offset(offset)
            rows = c.execute(query).fetchall()
            steps = []
            for row in rows:
                if load_output:
                    _, output, exception = safe_deserialize(
                        self.serializer,
                        row[7],
                        workflow_id,
                        serialized_input=None,
                        serialized_output=row[2],
                        serialized_exception=row[3],
                    )
                else:
                    output = None
                    exception = None
                step = StepInfo(
                    function_id=row[0],
                    function_name=row[1],
                    output=output,
                    error=exception,
                    child_workflow_id=row[4],
                    started_at_epoch_ms=row[5],
                    completed_at_epoch_ms=row[6],
                )
                steps.append(step)
            return steps

    def get_workflow_aggregates(
        self,
        *,
        group_by_status: bool = False,
        group_by_name: bool = False,
        group_by_queue_name: bool = False,
        group_by_executor_id: bool = False,
        group_by_application_version: bool = False,
        group_by_application_name: bool = False,
        select_count: bool = False,
        select_min_created_at: bool = False,
        select_max_queue_wait_ms: bool = False,
        select_max_total_latency_ms: bool = False,
        time_bucket_size_ms: Optional[int] = None,
        status: Optional[List[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        completed_after: Optional[str] = None,
        completed_before: Optional[str] = None,
        dequeued_after: Optional[str] = None,
        dequeued_before: Optional[str] = None,
        name: Optional[List[str]] = None,
        app_version: Optional[List[str]] = None,
        executor_id: Optional[List[str]] = None,
        queue_name: Optional[List[str]] = None,
        workflow_id_prefix: Optional[List[str]] = None,
        workflow_ids: Optional[List[str]] = None,
        forked_from: Optional[List[str]] = None,
        parent_workflow_id: Optional[List[str]] = None,
        user: Optional[List[str]] = None,
        schedule_name: Optional[List[str]] = None,
        application_name: Optional[List[str]] = None,
        was_forked_from: Optional[bool] = None,
        has_parent: Optional[bool] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> List[WorkflowAggregateRow]:
        if time_bucket_size_ms is not None and time_bucket_size_ms <= 0:
            raise ValueError("time_bucket_size_ms must be > 0")

        # Build group_by columns from boolean flags
        group_by_flags = [
            ("status", group_by_status, SystemSchema.workflow_status.c.status),
            ("name", group_by_name, SystemSchema.workflow_status.c.name),
            (
                "queue_name",
                group_by_queue_name,
                SystemSchema.workflow_status.c.queue_name,
            ),
            (
                "executor_id",
                group_by_executor_id,
                SystemSchema.workflow_status.c.executor_id,
            ),
            (
                "application_version",
                group_by_application_version,
                SystemSchema.workflow_status.c.application_version,
            ),
            (
                "application_name",
                group_by_application_name,
                SystemSchema.workflow_status.c.application_name,
            ),
        ]
        group_names: List[str] = []
        group_columns: List[sa.sql.ColumnElement[Any]] = []
        for col_name, enabled, col in group_by_flags:
            if enabled:
                group_names.append(col_name)
                group_columns.append(col)

        if time_bucket_size_ms is not None:
            created_at = SystemSchema.workflow_status.c.created_at
            bucket = sa.literal(time_bucket_size_ms)
            time_bucket_col = (
                sa.cast(func.floor(created_at / bucket), sa.BigInteger) * bucket
            ).label("time_bucket")
            group_names.append("time_bucket")
            group_columns.append(time_bucket_col)

        if not group_columns:
            raise ValueError("At least one group_by flag must be set to True")

        # Build select columns from boolean flags. MAX ignores NULLs, so rows
        # missing started_at_epoch_ms or completed_at naturally drop out of the
        # latency maxes.
        select_flags: List[Tuple[str, bool, sa.sql.ColumnElement[Any]]] = [
            ("count", select_count, func.count()),
            (
                "min_created_at",
                select_min_created_at,
                func.min(SystemSchema.workflow_status.c.created_at),
            ),
            (
                "max_queue_wait_ms",
                select_max_queue_wait_ms,
                func.max(
                    SystemSchema.workflow_status.c.started_at_epoch_ms
                    - SystemSchema.workflow_status.c.created_at
                ),
            ),
            (
                "max_total_latency_ms",
                select_max_total_latency_ms,
                func.max(
                    SystemSchema.workflow_status.c.completed_at
                    - SystemSchema.workflow_status.c.created_at
                ),
            ),
        ]
        select_names: List[str] = []
        select_columns: List[sa.sql.ColumnElement[Any]] = []
        for select_name, enabled, agg in select_flags:
            if enabled:
                select_names.append(select_name)
                select_columns.append(agg.label(select_name))

        if not select_columns:
            raise ValueError("At least one select_ flag must be set to True")

        query = sa.select(*group_columns, *select_columns)

        # Apply filters
        if status:
            query = query.where(SystemSchema.workflow_status.c.status.in_(status))
        if start_time:
            query = query.where(
                SystemSchema.workflow_status.c.created_at
                >= datetime.datetime.fromisoformat(start_time).timestamp() * 1000
            )
        if end_time:
            query = query.where(
                SystemSchema.workflow_status.c.created_at
                <= datetime.datetime.fromisoformat(end_time).timestamp() * 1000
            )
        if completed_after:
            query = query.where(
                SystemSchema.workflow_status.c.completed_at
                >= datetime.datetime.fromisoformat(completed_after).timestamp() * 1000
            )
        if completed_before:
            query = query.where(
                SystemSchema.workflow_status.c.completed_at
                <= datetime.datetime.fromisoformat(completed_before).timestamp() * 1000
            )
        # dequeued_after/before filter on started_at_epoch_ms: that column is
        # populated on dequeue and surfaced as WorkflowStatus.dequeued_at.
        if dequeued_after:
            query = query.where(
                SystemSchema.workflow_status.c.started_at_epoch_ms
                >= datetime.datetime.fromisoformat(dequeued_after).timestamp() * 1000
            )
        if dequeued_before:
            query = query.where(
                SystemSchema.workflow_status.c.started_at_epoch_ms
                <= datetime.datetime.fromisoformat(dequeued_before).timestamp() * 1000
            )
        if name:
            query = query.where(SystemSchema.workflow_status.c.name.in_(name))
        if app_version:
            query = query.where(
                SystemSchema.workflow_status.c.application_version.in_(app_version)
            )
        if executor_id:
            query = query.where(
                SystemSchema.workflow_status.c.executor_id.in_(executor_id)
            )
        if queue_name:
            query = query.where(
                SystemSchema.workflow_status.c.queue_name.in_(queue_name)
            )
        if workflow_id_prefix:
            query = query.where(
                sa.or_(
                    *[
                        SystemSchema.workflow_status.c.workflow_uuid.startswith(
                            p, autoescape=True
                        )
                        for p in workflow_id_prefix
                    ]
                )
            )
        if workflow_ids:
            query = query.where(
                SystemSchema.workflow_status.c.workflow_uuid.in_(workflow_ids)
            )
        if forked_from:
            query = query.where(
                SystemSchema.workflow_status.c.forked_from.in_(forked_from)
            )
        if parent_workflow_id:
            query = query.where(
                SystemSchema.workflow_status.c.parent_workflow_id.in_(
                    parent_workflow_id
                )
            )
        if user:
            query = query.where(
                SystemSchema.workflow_status.c.authenticated_user.in_(user)
            )
        if schedule_name:
            query = query.where(
                SystemSchema.workflow_status.c.schedule_name.in_(schedule_name)
            )
        query = query.where(
            self._observability_filter(
                SystemSchema.workflow_status.c.application_name, application_name
            )
        )
        if was_forked_from is not None:
            query = query.where(
                SystemSchema.workflow_status.c.was_forked_from == was_forked_from
            )
        if has_parent is not None:
            if has_parent:
                query = query.where(
                    SystemSchema.workflow_status.c.parent_workflow_id.isnot(None)
                )
            else:
                query = query.where(
                    SystemSchema.workflow_status.c.parent_workflow_id.is_(None)
                )
        if attributes:
            query = query.where(self._attributes_contains_clause(attributes))

        query = query.group_by(*group_columns)

        with self.engine.begin() as c:
            rows = c.execute(query).fetchall()

        results: List[WorkflowAggregateRow] = []
        group_offset = len(group_names)
        select_idx = {name: i for i, name in enumerate(select_names)}
        for row in rows:
            group: Dict[str, Optional[str]] = {
                group_names[i]: str(row[i]) if row[i] is not None else None
                for i in range(len(group_names))
            }
            count_val: Optional[int] = None
            if (i := select_idx.get("count")) is not None:
                v = row[group_offset + i]
                count_val = int(v) if v is not None else None
            min_created_at_val: Optional[int] = None
            if (i := select_idx.get("min_created_at")) is not None:
                v = row[group_offset + i]
                min_created_at_val = int(v) if v is not None else None
            max_queue_wait_val: Optional[int] = None
            if (i := select_idx.get("max_queue_wait_ms")) is not None:
                v = row[group_offset + i]
                max_queue_wait_val = int(v) if v is not None else None
            max_total_latency_val: Optional[int] = None
            if (i := select_idx.get("max_total_latency_ms")) is not None:
                v = row[group_offset + i]
                max_total_latency_val = int(v) if v is not None else None
            results.append(
                WorkflowAggregateRow(
                    group=group,
                    count=count_val,
                    min_created_at=min_created_at_val,
                    max_queue_wait_ms=max_queue_wait_val,
                    max_total_latency_ms=max_total_latency_val,
                )
            )
        return results

    def get_step_aggregates(
        self,
        *,
        group_by_function_name: bool = False,
        group_by_status: bool = False,
        select_count: bool = False,
        select_max_duration_ms: bool = False,
        time_bucket_size_ms: Optional[int] = None,
        status: Optional[List[str]] = None,
        function_name: Optional[List[str]] = None,
        workflow_id_prefix: Optional[List[str]] = None,
        completed_after: Optional[str] = None,
        completed_before: Optional[str] = None,
        application_name: Optional[List[str]] = None,
    ) -> List[StepAggregateRow]:
        if time_bucket_size_ms is not None and time_bucket_size_ms <= 0:
            raise ValueError("time_bucket_size_ms must be > 0")

        # operation_outputs has no explicit status column; derive it from
        # whether `error` is populated. Bookkeeping rows from record_child_workflow
        # have NULL error and NULL output, so they appear as SUCCESS here —
        # callers can filter them by function_name.
        status_expr = sa.case(
            (
                SystemSchema.operation_outputs.c.error.is_(None),
                sa.literal("SUCCESS"),
            ),
            else_=sa.literal("ERROR"),
        )

        # Build group_by columns from boolean flags
        group_by_flags: List[Tuple[str, bool, sa.sql.ColumnElement[Any]]] = [
            (
                "function_name",
                group_by_function_name,
                SystemSchema.operation_outputs.c.function_name,
            ),
            ("status", group_by_status, status_expr),
        ]
        group_names: List[str] = []
        group_columns: List[sa.sql.ColumnElement[Any]] = []
        for group_col_name, enabled, group_col in group_by_flags:
            if enabled:
                group_names.append(group_col_name)
                group_columns.append(group_col.label(group_col_name))

        if time_bucket_size_ms is not None:
            # Bucket on completed_at_epoch_ms — it's the indexed timestamp on
            # this table.
            completed_at = SystemSchema.operation_outputs.c.completed_at_epoch_ms
            bucket = sa.literal(time_bucket_size_ms)
            time_bucket_col = (
                sa.cast(func.floor(completed_at / bucket), sa.BigInteger) * bucket
            ).label("time_bucket")
            group_names.append("time_bucket")
            group_columns.append(time_bucket_col)

        if not group_columns:
            raise ValueError("At least one group_by flag must be set to True")

        # Build select columns from boolean flags. MAX ignores NULLs, so rows
        # without start/complete timestamps drop out of the duration max.
        select_flags: List[Tuple[str, bool, sa.sql.ColumnElement[Any]]] = [
            ("count", select_count, func.count()),
            (
                "max_duration_ms",
                select_max_duration_ms,
                func.max(
                    SystemSchema.operation_outputs.c.completed_at_epoch_ms
                    - SystemSchema.operation_outputs.c.started_at_epoch_ms
                ),
            ),
        ]
        select_names: List[str] = []
        select_columns: List[sa.sql.ColumnElement[Any]] = []
        for select_name, enabled, agg in select_flags:
            if enabled:
                select_names.append(select_name)
                select_columns.append(agg.label(select_name))

        if not select_columns:
            raise ValueError("At least one select_ flag must be set to True")

        query = sa.select(*group_columns, *select_columns)

        # Apply filters
        if status:
            query = query.where(status_expr.in_(status))
        if function_name:
            query = query.where(
                SystemSchema.operation_outputs.c.function_name.in_(function_name)
            )
        if workflow_id_prefix:
            query = query.where(
                sa.or_(
                    *[
                        SystemSchema.operation_outputs.c.workflow_uuid.startswith(
                            p, autoescape=True
                        )
                        for p in workflow_id_prefix
                    ]
                )
            )
        if completed_after:
            query = query.where(
                SystemSchema.operation_outputs.c.completed_at_epoch_ms
                >= datetime.datetime.fromisoformat(completed_after).timestamp() * 1000
            )
        if completed_before:
            query = query.where(
                SystemSchema.operation_outputs.c.completed_at_epoch_ms
                <= datetime.datetime.fromisoformat(completed_before).timestamp() * 1000
            )
        query = query.where(
            self._observability_filter(
                SystemSchema.operation_outputs.c.application_name, application_name
            )
        )

        query = query.group_by(*group_columns)

        with self.engine.begin() as c:
            rows = c.execute(query).fetchall()

        results: List[StepAggregateRow] = []
        group_offset = len(group_names)
        select_idx = {name: i for i, name in enumerate(select_names)}
        for row in rows:
            group: Dict[str, Optional[str]] = {
                group_names[i]: str(row[i]) if row[i] is not None else None
                for i in range(len(group_names))
            }
            count_val: Optional[int] = None
            if (i := select_idx.get("count")) is not None:
                v = row[group_offset + i]
                count_val = int(v) if v is not None else None
            max_duration_val: Optional[int] = None
            if (i := select_idx.get("max_duration_ms")) is not None:
                v = row[group_offset + i]
                max_duration_val = int(v) if v is not None else None
            results.append(
                StepAggregateRow(
                    group=group,
                    count=count_val,
                    max_duration_ms=max_duration_val,
                )
            )
        return results

    def _record_operation_result_txn(
        self,
        result: OperationResultInternal,
        completed_at_epoch_ms: int,
        conn: Union[sa.Connection, Session],
    ) -> None:
        error = result["error"]
        output = result["output"]
        assert error is None or output is None, "Only one of error or output can be set"

        # Check if the executor ID belong to another process.
        # Reset it to this process's executor ID if so.
        wf_executor_id_row = conn.execute(
            sa.select(
                SystemSchema.workflow_status.c.executor_id,
            ).where(
                SystemSchema.workflow_status.c.workflow_uuid == result["workflow_uuid"]
            )
        ).fetchone()
        assert wf_executor_id_row is not None
        wf_executor_id = wf_executor_id_row[0]
        if self.executor_id is not None and wf_executor_id != self.executor_id:
            dbos_logger.debug(
                f'Resetting executor_id from {wf_executor_id} to {self.executor_id} for workflow {result["workflow_uuid"]}'
            )
            conn.execute(
                sa.update(SystemSchema.workflow_status)
                .values(executor_id=self.executor_id)
                .where(
                    SystemSchema.workflow_status.c.workflow_uuid
                    == result["workflow_uuid"]
                )
            )

        # Record the outcome, throwing DBOSWorkflowConflictIDError if it is already present
        try:
            stmt = (
                self.dialect.insert(SystemSchema.operation_outputs)
                .values(
                    workflow_uuid=result["workflow_uuid"],
                    function_id=result["function_id"],
                    function_name=result["function_name"],
                    started_at_epoch_ms=result["started_at_epoch_ms"],
                    completed_at_epoch_ms=completed_at_epoch_ms,
                    output=output,
                    error=error,
                    serialization=result["serialization"],
                    # Mirrors the parent: only the running application records its steps.
                    application_name=self.app_name,
                )
                .on_conflict_do_update(
                    index_elements=[
                        SystemSchema.operation_outputs.c.workflow_uuid,
                        SystemSchema.operation_outputs.c.function_id,
                    ],
                    set_={
                        "completed_at_epoch_ms": SystemSchema.operation_outputs.c.completed_at_epoch_ms,
                    },
                )
                .returning(SystemSchema.operation_outputs.c.completed_at_epoch_ms)
            )

            res = conn.execute(stmt)
            rows = res.fetchall()
            if len(rows) > 0:
                existing_completed_at = rows[0][0]
                if (
                    existing_completed_at is None
                    or int(existing_completed_at) != completed_at_epoch_ms
                ):
                    raise DBOSWorkflowConflictIDError(result["workflow_uuid"])

        except DBAPIError as dbapi_error:
            if self._is_unique_constraint_violation(dbapi_error):
                raise DBOSWorkflowConflictIDError(result["workflow_uuid"])
            raise

    def record_operation_result(
        self,
        result: OperationResultInternal,
        *,
        completed_at_epoch_ms: Optional[int] = None,
    ) -> None:
        # Outside the retry: the conflict check compares the stored completion to ours.
        completed_at = (
            completed_at_epoch_ms
            if completed_at_epoch_ms is not None
            else int(time.time() * 1000)
        )

        @db_retry(sys_db=self)
        def record_operation_result_retry() -> None:
            with self.engine.begin() as c:
                self._record_operation_result_txn(result, completed_at, c)
            DebugTriggers.debug_trigger_point(DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT)

        record_operation_result_retry()

    def record_get_result(
        self,
        result_workflow_id: str,
        output: Optional[str],
        error: Optional[str],
        serialization: Optional[str],
        ctx: Optional["DBOSContext"] = None,
        *,
        started_at_epoch_ms: int,
    ) -> None:
        if ctx is None:
            ctx = get_local_dbos_context()
            # Only record get_result called in workflow functions
            if ctx is None or not ctx.is_workflow():
                return
            ctx.function_id += 1  # Record the get_result as a step
        # Capture ids outside the retry: db_retry may re-run its body, but function_id must increment only once.
        workflow_id = ctx.workflow_id
        function_id = ctx.function_id

        @db_retry(sys_db=self)
        def record() -> None:
            # Because there's no corresponding check, we do nothing on conflict
            # and do not raise a DBOSWorkflowConflictIDError
            sql = (
                self.dialect.insert(SystemSchema.operation_outputs)
                .values(
                    workflow_uuid=workflow_id,
                    function_id=function_id,
                    function_name="DBOS.getResult",
                    output=output,
                    error=error,
                    child_workflow_id=result_workflow_id,
                    started_at_epoch_ms=started_at_epoch_ms,
                    completed_at_epoch_ms=int(time.time() * 1000),
                    serialization=serialization,
                    application_name=self.app_name,
                )
                .on_conflict_do_nothing()
            )
            with self.engine.begin() as c:
                c.execute(sql)

        record()

    @db_retry()
    def record_child_workflow(
        self,
        parentUUID: str,
        childUUID: str,
        functionID: int,
        functionName: str,
        *,
        started_at_epoch_ms: int,
    ) -> None:
        # An empty child id is never valid; fail loudly instead of silently wedging the parent on recovery.
        if not childUUID:
            raise DBOSException(
                f"Attempted to record an empty child workflow ID for parent "
                f"{parentUUID} (function {functionID}, {functionName})."
            )
        # Spans the launch only: the parent does not wait for the child here.
        sql = sa.insert(SystemSchema.operation_outputs).values(
            workflow_uuid=parentUUID,
            function_id=functionID,
            function_name=functionName,
            child_workflow_id=childUUID,
            started_at_epoch_ms=started_at_epoch_ms,
            completed_at_epoch_ms=int(time.time() * 1000),
            application_name=self.app_name,
        )
        try:
            with self.engine.begin() as c:
                c.execute(sql)
        except DBAPIError as dbapi_error:
            if self._is_unique_constraint_violation(dbapi_error):
                # Same child means an idempotent db_retry; a different child means nondeterminism (a real conflict).
                with self.engine.begin() as c:
                    existing = c.execute(
                        sa.select(
                            SystemSchema.operation_outputs.c.child_workflow_id
                        ).where(
                            SystemSchema.operation_outputs.c.workflow_uuid
                            == parentUUID,
                            SystemSchema.operation_outputs.c.function_id == functionID,
                        )
                    ).fetchone()
                if existing is not None and existing[0] == childUUID:
                    return
                raise DBOSWorkflowConflictIDError(parentUUID)
            raise

    @abstractmethod
    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation."""
        pass

    @abstractmethod
    def _attributes_contains_clause(
        self, attributes: Dict[str, Any]
    ) -> sa.ColumnElement[bool]:
        """Build a clause matching workflows whose attributes contain all the given key-value pairs."""
        pass

    @abstractmethod
    def _is_foreign_key_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a foreign key violation."""
        pass

    def _check_operation_execution_txn(
        self,
        workflow_id: str,
        function_id: int,
        function_name: str,
        conn: Union[sa.Connection, Session],
    ) -> Optional[RecordedResult]:
        # First query: Retrieve the workflow status
        workflow_status_sql = sa.select(
            SystemSchema.workflow_status.c.status,
        ).where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)

        # Second query: Retrieve operation outputs if they exist
        operation_output_sql = sa.select(
            SystemSchema.operation_outputs.c.output,
            SystemSchema.operation_outputs.c.error,
            SystemSchema.operation_outputs.c.function_name,
            SystemSchema.operation_outputs.c.child_workflow_id,
            SystemSchema.operation_outputs.c.serialization,
        ).where(
            (SystemSchema.operation_outputs.c.workflow_uuid == workflow_id)
            & (SystemSchema.operation_outputs.c.function_id == function_id)
        )

        # Execute both queries
        workflow_status_rows = conn.execute(workflow_status_sql).all()
        operation_output_rows = conn.execute(operation_output_sql).all()

        # Check if the workflow exists
        assert (
            len(workflow_status_rows) > 0
        ), f"Error: Workflow {workflow_id} does not exist"

        # Get workflow status
        workflow_status = workflow_status_rows[0][0]

        # If the workflow is cancelled, raise the exception
        if workflow_status == WorkflowStatusString.CANCELLED.value:
            raise DBOSWorkflowCancelledError(
                f"Workflow {workflow_id} is cancelled. Aborting function."
            )

        # If there are no operation outputs, return None
        if not operation_output_rows:
            return None

        # Extract operation output data
        output, error, recorded_function_name, child_workflow_id, serialization = (
            operation_output_rows[0][0],
            operation_output_rows[0][1],
            operation_output_rows[0][2],
            operation_output_rows[0][3],
            operation_output_rows[0][4],
        )

        # If the provided and recorded function name are different, throw an exception
        if function_name != recorded_function_name:
            raise DBOSUnexpectedStepError(
                workflow_id=workflow_id,
                step_id=function_id,
                expected_name=function_name,
                recorded_name=recorded_function_name,
            )

        result: RecordedResult = {
            "output": output,
            "error": error,
            "serialization": serialization,
            "child_workflow_id": child_workflow_id,
        }
        return result

    @db_retry()
    def check_operation_execution(
        self, workflow_id: str, function_id: int, function_name: str
    ) -> Optional[RecordedResult]:
        with self.engine.begin() as c:
            return self._check_operation_execution_txn(
                workflow_id, function_id, function_name, c
            )

    def _find_fork_descendants_txn(
        self, workflow_ids: List[str], conn: Union[sa.Connection, Session]
    ) -> Dict[str, Set[str]]:
        """Return every workflow recursively forked from each of `workflow_ids`.

        Resolves all of the given roots together: one query per level of the fork
        forest covers every root at once, rather than a separate traversal per
        root. The `forked_from` edges discovered are accumulated into an adjacency
        map, then each root's descendant set (direct forks, forks of forks, ...,
        excluding the root itself) is computed in memory. Self-references and
        cycles (which should not occur) are ignored.
        """
        # Bulk breadth-first walk over the union of all roots, recording the
        # parent -> children edges of every reachable fork.
        children: Dict[str, List[str]] = {}
        seen: Set[str] = set(workflow_ids)
        frontier = list(dict.fromkeys(workflow_ids))
        while frontier:
            rows = conn.execute(
                sa.select(
                    SystemSchema.workflow_status.c.workflow_uuid,
                    SystemSchema.workflow_status.c.forked_from,
                ).where(SystemSchema.workflow_status.c.forked_from.in_(frontier))
            ).all()
            next_frontier = []
            for forked_id, forked_from in rows:
                children.setdefault(forked_from, []).append(forked_id)
                if forked_id not in seen:
                    seen.add(forked_id)
                    next_frontier.append(forked_id)
            frontier = next_frontier

        # Compute each root's descendants in memory from the adjacency map.
        result: Dict[str, Set[str]] = {}
        for root in workflow_ids:
            if root in result:
                continue
            descendants: Set[str] = set()
            stack = list(children.get(root, []))
            while stack:
                node = stack.pop()
                if node != root and node not in descendants:
                    descendants.add(node)
                    stack.extend(children.get(node, []))
            result[root] = descendants
        return result

    @db_retry()
    def send_bulk(
        self,
        messages: List[SendMessage],
        *,
        serialization_type: Optional["WorkflowSerializationFormat"],
        workflow_id: Optional[str],
        function_id: Optional[int],
        function_name: str,
        send_to_forks: bool,
    ) -> None:
        """Send one or more messages in a single transaction.

        This is the single implementation underlying both `DBOS.send`/`send_bulk`
        (inside and outside a workflow) and `DBOSClient.send`/`send_bulk`. When
        called from a workflow, `workflow_id` and `function_id` identify the
        single step recording the operation, which makes it idempotent on replay;
        `function_name` is the name recorded for that step. Each message also
        provides its own idempotency via the primary key constraint on
        `message_uuid`.

        When `send_to_forks` is set, every message is delivered not only to its
        `destination_id` but also to every workflow recursively forked from it
        (forks, forks of forks, ...) that exists at send time.
        """
        with self.engine.begin() as c:
            self._send_bulk_txn(
                messages,
                c,
                serialization_type=serialization_type,
                workflow_id=workflow_id,
                function_id=function_id,
                function_name=function_name,
                send_to_forks=send_to_forks,
            )

    def send_bulk_with_connection(
        self,
        messages: List[SendMessage],
        conn: Union[sa.Connection, Session],
        *,
        serialization_type: Optional["WorkflowSerializationFormat"],
        function_name: str,
        send_to_forks: bool,
    ) -> None:
        """Send one or more messages using a caller-owned SQLAlchemy
        Connection or ORM Session.

        Does not begin, commit, rollback, or retry. The caller owns the
        transaction; the messages are not visible to their destinations until
        it commits. The connection or session must target the DBOS system
        database.
        """
        self._apply_caller_schema(conn)
        self._send_bulk_txn(
            messages,
            conn,
            serialization_type=serialization_type,
            workflow_id=None,
            function_id=None,
            function_name=function_name,
            send_to_forks=send_to_forks,
        )

    def _send_bulk_txn(
        self,
        messages: List[SendMessage],
        conn: Union[sa.Connection, Session],
        *,
        serialization_type: Optional["WorkflowSerializationFormat"],
        workflow_id: Optional[str],
        function_id: Optional[int],
        function_name: str,
        send_to_forks: bool,
    ) -> None:
        start_time = int(time.time() * 1000)

        # Reject duplicate idempotency keys
        provided_keys = [m.idempotency_key for m in messages if m.idempotency_key]
        if len(provided_keys) != len(set(provided_keys)):
            duplicates = sorted(
                {k for k in provided_keys if provided_keys.count(k) > 1}
            )
            raise DBOSException(
                f"send_bulk received duplicate idempotency keys: {', '.join(duplicates)}"
            )

        # Serialize each message once (independent of how many destinations it
        # fans out to once forks are resolved).
        prepared = [
            (m, *serialize_value(m.message, serialization_type, self.serializer))
            for m in messages
        ]

        if workflow_id is not None:
            assert function_id is not None
            recorded_output = self._check_operation_execution_txn(
                workflow_id, function_id, function_name, conn=conn
            )
            if recorded_output is not None:
                dbos_logger.debug(
                    f"Replaying {function_name}, id: {function_id}, messages: {len(messages)}"
                )
                return  # Already sent before
            else:
                dbos_logger.debug(
                    f"Running {function_name}, id: {function_id}, messages: {len(messages)}"
                )

        # Expand each message to its destination set (the workflow itself plus,
        # if requested, every workflow recursively forked from it). Forks for
        # all destinations are resolved in a single bulk walk, inside the
        # transaction so the recipient set is consistent with the insert.
        fork_descendants: Dict[str, Set[str]] = {}
        if send_to_forks:
            fork_descendants = self._find_fork_descendants_txn(
                [m.destination_id for m, _, _ in prepared], conn
            )

        rows = []
        for m, serval, serialization in prepared:
            destinations = [m.destination_id]
            if send_to_forks:
                destinations.extend(
                    sorted(fork_descendants.get(m.destination_id, set()))
                )
            for dest in destinations:
                if m.idempotency_key is None:
                    message_uuid = str(generate_uuid())
                else:
                    # An idempotency key is scoped per destination: suffix it
                    # with the recipient's workflow ID. This gives each recipient
                    # a distinct, deterministic message_uuid (so a single key can
                    # fan out across forks) while replays stay idempotent, and
                    # makes the message_uuid independent of whether the send fanned
                    # out to forks.
                    message_uuid = f"{m.idempotency_key}::{dest}"
                rows.append(
                    {
                        "destination_uuid": dest,
                        "topic": (m.topic if m.topic is not None else _dbos_null_topic),
                        "message": serval,
                        "message_uuid": message_uuid,
                        "serialization": serialization,
                    }
                )

        try:
            if rows:
                conn.execute(
                    self.dialect.insert(SystemSchema.notifications)
                    .values(rows)
                    .on_conflict_do_nothing(
                        index_elements=[
                            SystemSchema.notifications.c.message_uuid,
                        ]
                    )
                )
        except DBAPIError as dbapi_error:
            if self._is_foreign_key_violation(dbapi_error):
                raise DBOSNonExistentWorkflowError(
                    "`send` destination",
                    ", ".join(sorted({m.destination_id for m in messages})),
                )
            raise

        if workflow_id is not None:
            assert function_id is not None
            output: OperationResultInternal = {
                "workflow_uuid": workflow_id,
                "function_id": function_id,
                "function_name": function_name,
                "started_at_epoch_ms": start_time,
                "output": None,
                "error": None,
                "serialization": None,
            }
            self._record_operation_result_txn(
                output, int(time.time() * 1000), conn=conn
            )

    @db_retry()
    def recv_setup(
        self,
        workflow_uuid: str,
        function_id: int,
        timeout_function_id: int,
        topic: Optional[str],
        timeout_seconds: float = 60,
    ) -> EventSetupResult:
        """Setup phase of recv. Returns either:
        - (True, result) if a cached result was found (OAOO replay or message already available)
        - (False, event, actual_timeout, payload, start_time) if caller must wait on the event
        """
        function_name = "DBOS.recv"
        start_time = int(time.time() * 1000)
        topic = topic if topic is not None else _dbos_null_topic

        # First, check for previous executions.
        recorded_output = self.check_operation_execution(
            workflow_uuid, function_id, function_name
        )
        if recorded_output is not None:
            dbos_logger.debug(f"Replaying recv, id: {function_id}, topic: {topic}")
            if recorded_output["output"] is not None:
                return True, deserialize_value(
                    recorded_output["output"],
                    recorded_output["serialization"],
                    self.serializer,
                )
            else:
                raise Exception("No output recorded in the last recv")
        else:
            dbos_logger.debug(f"Running recv, id: {function_id}, topic: {topic}")

        # Insert an event to the notifications map, so the listener can signal it when a message is received.
        payload = f"{workflow_uuid}::{topic}"
        event = LoopAwareEvent()
        success, _ = self.notifications_map.set(payload, event, (workflow_uuid, topic))
        if not success:
            # This should not happen, but if it does, it means the workflow is executed concurrently.
            # set() incremented the existing entry's count, so undo that before raising.
            self.notifications_map.pop(payload)
            raise DBOSWorkflowConflictIDError(workflow_uuid)

        try:
            # Check if an unconsumed message is already in the database.
            self.recv_check(workflow_uuid, topic, event)

            # Record the durable sleep timeout
            actual_timeout = self.record_sleep(
                workflow_uuid, timeout_function_id, timeout_seconds
            )
        except:
            self.notifications_map.pop(payload)
            raise

        return False, event, actual_timeout, payload, start_time

    @db_retry()
    def recv_consume(
        self,
        workflow_uuid: str,
        function_id: int,
        topic: Optional[str],
        start_time: int,
    ) -> Any:
        """Consume phase of recv. Transactionally consumes the oldest unconsumed
        message and records the operation result."""
        function_name = "DBOS.recv"
        topic = topic if topic is not None else _dbos_null_topic

        with self.engine.begin() as c:
            # Idempotency: if a prior db_retry attempt already committed, return the recorded message, not a new one.
            recorded = c.execute(
                sa.select(
                    SystemSchema.operation_outputs.c.output,
                    SystemSchema.operation_outputs.c.serialization,
                ).where(
                    SystemSchema.operation_outputs.c.workflow_uuid == workflow_uuid,
                    SystemSchema.operation_outputs.c.function_id == function_id,
                )
            ).fetchall()
            if len(recorded) > 0:
                return deserialize_value(
                    recorded[0][0], recorded[0][1], self.serializer
                )

            consume_stmt = (
                sa.update(SystemSchema.notifications)
                .where(
                    SystemSchema.notifications.c.destination_uuid == workflow_uuid,
                    SystemSchema.notifications.c.topic == topic,
                    SystemSchema.notifications.c.consumed == False,
                    SystemSchema.notifications.c.message_uuid
                    == (
                        sa.select(SystemSchema.notifications.c.message_uuid)
                        .where(
                            SystemSchema.notifications.c.destination_uuid
                            == workflow_uuid,
                            SystemSchema.notifications.c.topic == topic,
                            SystemSchema.notifications.c.consumed == False,
                        )
                        .order_by(
                            SystemSchema.notifications.c.created_at_epoch_ms.asc()
                        )
                        .limit(1)
                        .scalar_subquery()
                    ),
                )
                .values(consumed=True)
                .returning(
                    SystemSchema.notifications.c.message,
                    SystemSchema.notifications.c.serialization,
                )
            )
            rows = c.execute(consume_stmt).fetchall()
            message: Any = None
            serialization: Optional[str] = None
            if len(rows) > 0:
                message = deserialize_value(rows[0][0], rows[0][1], self.serializer)
                serialization = rows[0][1]

            sermsg, serialization = serialize_value_as(
                message, serialization, self.serializer
            )
            self._record_operation_result_txn(
                {
                    "workflow_uuid": workflow_uuid,
                    "function_id": function_id,
                    "function_name": function_name,
                    "started_at_epoch_ms": start_time,
                    "output": sermsg,
                    "serialization": serialization,
                    "error": None,
                },
                int(time.time() * 1000),
                conn=c,
            )
        return message

    def recv_check(
        self,
        workflow_uuid: str,
        topic: Optional[str],
        event: LoopAwareEvent,
    ) -> None:
        """Poll the database directly for a pending notification and signal the event if found.
        Used as a fallback in case the notification listener thread drops a notification.
        """
        normalized_topic = topic if topic is not None else _dbos_null_topic
        try:
            # This is a polling read: run it under the polling limiter.
            with self.poll_limiter, self.engine.begin() as c:
                rows = c.execute(
                    sa.select(SystemSchema.notifications.c.topic).where(
                        SystemSchema.notifications.c.destination_uuid == workflow_uuid,
                        SystemSchema.notifications.c.topic == normalized_topic,
                        SystemSchema.notifications.c.consumed == False,
                    )
                ).fetchall()
            if len(rows) > 0:
                event.set()
        except Exception:
            dbos_logger.warning("Fallback notification poll failed", exc_info=True)

    # The interval that recv and get_event poll on as a fallback to catch dropped notifications
    _notification_fallback_polling_interval: float = 60.0

    def _event_recheck_interval(self) -> float:
        """How long recv and get_event wait on their in-memory event before
        re-checking the database.

        With a notification listener running, the listener signals the event
        promptly and the re-check is only a safety net against dropped
        notifications. Without one (e.g. in DBOSClient, which starts a listener
        thread only when created with use_listen_notify=True), the re-check is
        the only delivery mechanism, so use the much shorter polling interval
        instead."""
        if self._listener_running:
            return self._notification_fallback_polling_interval
        return self._notification_listener_polling_interval_sec

    def run_notification_listener(self) -> None:
        """Run the notification listener, marking it active so event waits
        only re-check the database as a fallback."""
        self._listener_running = True
        self._notification_listener()

    def _signal_notification(self, channel: str, payload: str) -> None:
        """Hint that `payload` was written on `channel`; push-notification backends override to coalesce a wakeup, pollers ignore it."""
        pass

    def run_notifier(self) -> None:
        """Background loop flushing coalesced NOTIFY payloads across all channels; no-op except on push-notification backends (Postgres + L/N)."""
        pass

    def recv(
        self,
        workflow_uuid: str,
        function_id: int,
        timeout_function_id: int,
        topic: Optional[str],
        timeout_seconds: float = 60,
    ) -> Any:
        setup = self.recv_setup(
            workflow_uuid, function_id, timeout_function_id, topic, timeout_seconds
        )
        if setup[0]:
            return setup[1]
        _, event, actual_timeout, payload, start_time = setup
        try:
            deadline = time.time() + actual_timeout
            while not event.is_set():
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                event.wait(timeout=min(remaining, self._event_recheck_interval()))
                if not event.is_set():
                    self.recv_check(workflow_uuid, topic, event)
            return self.recv_consume(workflow_uuid, function_id, topic, start_time)
        finally:
            self.notifications_map.pop(payload)

    async def _run_event_setup_async(
        self,
        event_map: ThreadSafeEventDict,
        setup_fn: Callable[..., EventSetupResult],
        *args: Any,
    ) -> EventSetupResult:
        """Run a recv/get_event setup function in a worker thread, cleaning up
        synchronously if this coroutine is cancelled while the thread is in
        flight.

        The worker thread cannot be cancelled, so it finishes registering in
        event_map even after cancellation abandons the coroutine before its
        try/finally cleanup is in place. A leftover recv entry makes the next
        recv on the same workflow and topic fail with
        DBOSWorkflowConflictIDError -- a spurious "duplicate execution" that
        parks the caller in await_workflow_result forever; a leftover
        get_event entry leaks. So on cancellation, wait for the thread inline
        and undo its registration *before* re-raising CancelledError: once the
        cancelled call returns, no stale entry remains, so there is no window
        for a concurrent recv to trip over. If a further cancellation
        interrupts that wait, fall back to deferred cleanup via a done-callback
        so an impatient caller is not blocked on the thread.
        """
        setup_task = asyncio.create_task(asyncio.to_thread(setup_fn, *args))
        try:
            return await asyncio.shield(setup_task)
        except asyncio.CancelledError:

            def unregister(task: "asyncio.Task[EventSetupResult]") -> None:
                if task.cancelled() or task.exception() is not None:
                    # Setup never registered, or registered then cleaned up
                    # after its own failure; nothing to undo.
                    return
                result = task.result()
                if not result[0]:
                    event_map.pop(result[3])

            # Wait for the thread to finish, then undo its registration. A
            # second cancellation lands in the except below; an exception from
            # setup is ignored because setup already cleaned up after itself.
            try:
                await asyncio.shield(setup_task)
            except asyncio.CancelledError:
                if not setup_task.done():
                    # Cancelled again while still waiting on the thread: hand
                    # cleanup to a done-callback rather than block any longer.
                    setup_task.add_done_callback(unregister)
                    raise
            except Exception:
                pass
            unregister(setup_task)
            raise

    async def recv_async(
        self,
        workflow_uuid: str,
        function_id: int,
        timeout_function_id: int,
        topic: Optional[str],
        timeout_seconds: float = 60,
    ) -> Any:
        setup = await self._run_event_setup_async(
            self.notifications_map,
            self.recv_setup,
            workflow_uuid,
            function_id,
            timeout_function_id,
            topic,
            timeout_seconds,
        )
        if setup[0]:
            return setup[1]
        _, event, actual_timeout, payload, start_time = setup
        try:
            deadline = time.time() + actual_timeout
            while not event.is_set():
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                await event.wait_async(
                    timeout=min(remaining, self._event_recheck_interval())
                )
                if not event.is_set() and time.time() < deadline:
                    await asyncio.to_thread(
                        self.recv_check, workflow_uuid, topic, event
                    )
            return await asyncio.to_thread(
                self.recv_consume,
                workflow_uuid,
                function_id,
                topic,
                start_time,
            )
        finally:
            self.notifications_map.pop(payload)

    @abstractmethod
    def _notification_listener(self) -> None:
        """Listen for database notifications using database-specific mechanisms."""
        pass

    def _notification_listener_polling(self) -> None:
        """Poll for notifications and workflow events"""

        while self._run_background_processes:
            try:
                # Poll at the configured interval
                time.sleep(self._notification_listener_polling_interval_sec)

                # Check all entries in the notifications_map
                for (
                    payload,
                    (dest_uuid, topic),
                    event,
                ) in self.notifications_map.snapshot():
                    with self.engine.begin() as conn:
                        result = conn.execute(
                            sa.select(sa.literal(1))
                            .where(
                                SystemSchema.notifications.c.destination_uuid
                                == dest_uuid,
                                SystemSchema.notifications.c.topic == topic,
                                SystemSchema.notifications.c.consumed == False,
                            )
                            .limit(1)
                        )
                        if result.fetchone():
                            event.set()
                            dbos_logger.debug(f"Signaled event for {payload}")

                # Check all entries in the workflow_events_map
                for (
                    payload,
                    (workflow_uuid, key),
                    event,
                ) in self.workflow_events_map.snapshot():
                    with self.engine.begin() as conn:
                        result = conn.execute(
                            sa.select(sa.literal(1))
                            .where(
                                SystemSchema.workflow_events.c.workflow_uuid
                                == workflow_uuid,
                                SystemSchema.workflow_events.c.key == key,
                            )
                            .limit(1)
                        )
                        if result.fetchone():
                            event.set()
                            dbos_logger.debug(f"Signaled event for {payload}")

                # Check all entries in the streams_map. A stream reader re-reads
                # at its own offset on wakeup, so any row for (workflow_uuid, key)
                # is a sufficient hint to re-check.
                for (
                    payload,
                    (workflow_uuid, key),
                    event,
                ) in self.streams_map.snapshot():
                    with self.engine.begin() as conn:
                        result = conn.execute(
                            sa.select(sa.literal(1))
                            .where(
                                SystemSchema.streams.c.workflow_uuid == workflow_uuid,
                                SystemSchema.streams.c.key == key,
                            )
                            .limit(1)
                        )
                        if result.fetchone():
                            event.set()
                            dbos_logger.debug(f"Signaled event for {payload}")

            except Exception as e:
                if self._run_background_processes:
                    dbos_logger.warning(f"Notification poller error: {e}")
                    time.sleep(self._notification_listener_polling_interval_sec)

    @staticmethod
    def reset_system_database(
        database_url: str, *, truncate: bool = False, schema: Optional[str] = None
    ) -> None:
        """Reset the system database by calling the appropriate implementation.

        truncate=True empties the tables instead, keeping the migrated schema."""
        if database_url.startswith("sqlite"):
            from ._sys_db_sqlite import SQLiteSystemDatabase

            SQLiteSystemDatabase._reset_system_database(database_url, truncate=truncate)
        else:
            from ._sys_db_postgres import PostgresSystemDatabase

            PostgresSystemDatabase._reset_system_database(
                database_url, truncate=truncate, schema=schema
            )

    @db_retry()
    def record_sleep(
        self,
        workflow_uuid: str,
        function_id: int,
        seconds: float,
        *,
        project_completion_time: bool = False,
    ) -> float:
        """Checkpoint a durable sleep, returning the seconds still left to wait.

        The row precedes the sleep because it stores the wake time recovery resumes
        from. `project_completion_time` records that wake time as the completion, so
        the duration is the sleep; callers registering a timeout they may abandon
        early (recv, get_event) leave it off and record an instant.
        """
        function_name = "DBOS.sleep"
        start_time = int(time.time() * 1000)
        recorded_output = self.check_operation_execution(
            workflow_uuid, function_id, function_name
        )
        end_time: float
        if recorded_output is not None:
            dbos_logger.debug(f"Replaying sleep, id: {function_id}, seconds: {seconds}")
            assert recorded_output["output"] is not None, "no recorded end time"
            end_time = cast(
                float,
                deserialize_value(
                    recorded_output["output"],
                    recorded_output["serialization"],
                    self.serializer,
                ),
            )
        else:
            dbos_logger.debug(f"Running sleep, id: {function_id}, seconds: {seconds}")
            end_time = time.time() + seconds
            try:
                self.record_operation_result(
                    {
                        "workflow_uuid": workflow_uuid,
                        "function_id": function_id,
                        "function_name": function_name,
                        "started_at_epoch_ms": start_time,
                        "output": DBOSPortableJSON.serialize(end_time),
                        "error": None,
                        "serialization": DBOSPortableJSON.name(),
                    },
                    completed_at_epoch_ms=(
                        int(end_time * 1000) if project_completion_time else None
                    ),
                )
            except DBOSWorkflowConflictIDError:
                pass
        return max(0, end_time - time.time())

    @db_retry()
    def set_event_from_workflow(
        self,
        workflow_uuid: str,
        function_id: int,
        key: str,
        message: Any,
        *,
        serialization_type: WorkflowSerializationFormat,
    ) -> None:
        serval, serialization = serialize_value(
            message,
            serialization_type,
            self.serializer,
        )
        function_name = "DBOS.setEvent"
        start_time = int(time.time() * 1000)
        with self.engine.begin() as c:
            recorded_output = self._check_operation_execution_txn(
                workflow_uuid, function_id, function_name, conn=c
            )
            if recorded_output is not None:
                dbos_logger.debug(f"Replaying set_event, id: {function_id}, key: {key}")
                return  # Already sent before
            else:
                dbos_logger.debug(f"Running set_event, id: {function_id}, key: {key}")
            c.execute(
                self.dialect.insert(SystemSchema.workflow_events)
                .values(
                    workflow_uuid=workflow_uuid,
                    key=key,
                    value=serval,
                    serialization=serialization,
                )
                .on_conflict_do_update(
                    index_elements=["workflow_uuid", "key"],
                    set_={
                        "value": serval,
                        "serialization": serialization,
                    },
                )
            )
            c.execute(
                self.dialect.insert(SystemSchema.workflow_events_history)
                .values(
                    workflow_uuid=workflow_uuid,
                    function_id=function_id,
                    key=key,
                    value=serval,
                    serialization=serialization,
                )
                .on_conflict_do_update(
                    index_elements=["workflow_uuid", "key", "function_id"],
                    set_={
                        "value": serval,
                        "serialization": serialization,
                    },
                )
            )
            output: OperationResultInternal = {
                "workflow_uuid": workflow_uuid,
                "function_id": function_id,
                "function_name": function_name,
                "started_at_epoch_ms": start_time,
                "output": None,
                "error": None,
                "serialization": None,
            }
            self._record_operation_result_txn(output, int(time.time() * 1000), conn=c)
        # Notify only after commit, so a woken get_event sees the value.
        self._signal_notification(
            _dbos_workflow_events_channel, f"{workflow_uuid}::{key}"
        )

    def set_event_from_step(
        self,
        workflow_uuid: str,
        function_id: int,
        key: str,
        message: Any,
        *,
        serialization_type: WorkflowSerializationFormat,
    ) -> None:
        serval, serialization = serialize_value(
            message,
            serialization_type,
            self.serializer,
        )

        with self.engine.begin() as c:
            c.execute(
                self.dialect.insert(SystemSchema.workflow_events)
                .values(
                    workflow_uuid=workflow_uuid,
                    key=key,
                    value=serval,
                    serialization=serialization,
                )
                .on_conflict_do_update(
                    index_elements=["workflow_uuid", "key"],
                    set_={
                        "value": serval,
                        "serialization": serialization,
                    },
                )
            )
            c.execute(
                self.dialect.insert(SystemSchema.workflow_events_history)
                .values(
                    workflow_uuid=workflow_uuid,
                    function_id=function_id,
                    key=key,
                    value=serval,
                    serialization=serialization,
                )
                .on_conflict_do_update(
                    index_elements=["workflow_uuid", "key", "function_id"],
                    set_={
                        "value": serval,
                        "serialization": serialization,
                    },
                )
            )
        # Notify only after commit, so a woken get_event sees the value.
        self._signal_notification(
            _dbos_workflow_events_channel, f"{workflow_uuid}::{key}"
        )

    def get_all_events(self, workflow_id: str) -> Dict[str, Any]:
        """
        Get all events currently present for a workflow ID.

        Args:
            workflow_id: The workflow UUID to get events for

        Returns:
            A dictionary mapping event keys to their deserialized values
        """
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(
                    SystemSchema.workflow_events.c.key,
                    SystemSchema.workflow_events.c.value,
                    SystemSchema.workflow_events.c.serialization,
                ).where(SystemSchema.workflow_events.c.workflow_uuid == workflow_id)
            ).fetchall()
            events: Dict[str, Any] = {}
            for row in rows:
                key = row[0]
                value = deserialize_value(row[1], row[2], self.serializer)
                events[key] = value

            return events

    def get_all_notifications(self, workflow_id: str) -> List[NotificationInfo]:
        """Get all notifications sent to a workflow."""
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(
                    SystemSchema.notifications.c.topic,
                    SystemSchema.notifications.c.message,
                    SystemSchema.notifications.c.serialization,
                    SystemSchema.notifications.c.created_at_epoch_ms,
                    SystemSchema.notifications.c.consumed,
                )
                .where(SystemSchema.notifications.c.destination_uuid == workflow_id)
                .order_by(SystemSchema.notifications.c.created_at_epoch_ms)
            ).fetchall()
            results: List[NotificationInfo] = []
            for row in rows:
                topic = row[0]
                if topic == _dbos_null_topic:
                    topic = None
                results.append(
                    {
                        "topic": topic,
                        "message": deserialize_value(row[1], row[2], self.serializer),
                        "created_at_epoch_ms": row[3],
                        "consumed": row[4],
                    }
                )
            return results

    def get_all_stream_entries(self, workflow_id: str) -> Dict[str, List[Any]]:
        """Get all stream entries for a workflow.

        Returns a dict mapping stream keys to lists of deserialized values (ordered by offset).
        """
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(
                    SystemSchema.streams.c.key,
                    SystemSchema.streams.c.value,
                    SystemSchema.streams.c.offset,
                    SystemSchema.streams.c.serialization,
                )
                .where(SystemSchema.streams.c.workflow_uuid == workflow_id)
                .order_by(
                    SystemSchema.streams.c.key,
                    SystemSchema.streams.c.offset,
                )
            ).fetchall()
            streams: Dict[str, List[Any]] = {}
            for row in rows:
                key = row[0]
                value_str = row[1]
                serialization = row[3]
                value = deserialize_value(value_str, serialization, self.serializer)
                if value == _dbos_stream_closed_sentinel:
                    continue
                if key not in streams:
                    streams[key] = []
                streams[key].append(value)
            return streams

    @db_retry()
    def get_event_setup(
        self,
        target_uuid: str,
        key: str,
        timeout_seconds: float = 60,
        caller_ctx: Optional[GetEventWorkflowContext] = None,
    ) -> EventSetupResult:
        """Setup phase of get_event. Returns either:
        - (True, result) if a cached result was found (OAOO replay)
        - (False, event, actual_timeout, payload, start_time) if caller must wait on the event
        """
        function_name = "DBOS.getEvent"
        start_time = int(time.time() * 1000)

        # Check for previous executions only if it's in a workflow
        if caller_ctx is not None:
            recorded_output = self.check_operation_execution(
                caller_ctx["workflow_uuid"], caller_ctx["function_id"], function_name
            )
            if recorded_output is not None:
                dbos_logger.debug(
                    f"Replaying get_event, id: {caller_ctx['function_id']}, key: {key}"
                )
                if recorded_output["output"] is not None:
                    return True, deserialize_value(
                        recorded_output["output"],
                        recorded_output["serialization"],
                        self.serializer,
                    )
                else:
                    raise Exception("No output recorded in the last get_event")
            else:
                dbos_logger.debug(
                    f"Running get_event, id: {caller_ctx['function_id']}, key: {key}"
                )

        payload = f"{target_uuid}::{key}"
        event = LoopAwareEvent()
        success, existing_event = self.workflow_events_map.set(
            payload, event, (target_uuid, key)
        )
        if not success:
            # Key already exists, wait on the existing event
            event = existing_event

        try:
            # Check if the key is already in the database
            self.get_event_check(target_uuid, key, event)

            # Record the durable sleep timeout
            actual_timeout = timeout_seconds
            if caller_ctx is not None:
                actual_timeout = self.record_sleep(
                    caller_ctx["workflow_uuid"],
                    caller_ctx["timeout_function_id"],
                    timeout_seconds,
                )
        except:
            self.workflow_events_map.pop(payload)
            raise

        return False, event, actual_timeout, payload, start_time

    @db_retry()
    def get_event_consume(
        self,
        target_uuid: str,
        key: str,
        start_time: int,
        caller_ctx: Optional[GetEventWorkflowContext] = None,
    ) -> Any:
        """Consume phase of get_event. Reads the value from the database
        and records the operation result if in a workflow."""
        function_name = "DBOS.getEvent"

        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(
                    SystemSchema.workflow_events.c.value,
                    SystemSchema.workflow_events.c.serialization,
                ).where(
                    SystemSchema.workflow_events.c.workflow_uuid == target_uuid,
                    SystemSchema.workflow_events.c.key == key,
                )
            ).fetchall()

        value: Any = None
        serialization: Optional[str] = None
        if len(rows) > 0:
            serialization = rows[0][1]
            value = deserialize_value(rows[0][0], serialization, self.serializer)

        # Record the output if it's in a workflow
        if caller_ctx is not None:
            serval, serialization = serialize_value_as(
                value, serialization, self.serializer
            )
            self.record_operation_result(
                {
                    "workflow_uuid": caller_ctx["workflow_uuid"],
                    "function_id": caller_ctx["function_id"],
                    "function_name": function_name,
                    "started_at_epoch_ms": start_time,
                    "output": serval,
                    "serialization": serialization,
                    "error": None,
                }
            )
        return value

    def get_event_check(
        self,
        target_uuid: str,
        key: str,
        event: LoopAwareEvent,
    ) -> None:
        """Poll the database directly for a workflow event and signal the event if found.
        Used as a fallback in case the notification listener thread drops a notification.
        """
        try:
            # This is a polling read: run it under the polling limiter.
            with self.poll_limiter, self.engine.begin() as c:
                rows = c.execute(
                    sa.select(
                        SystemSchema.workflow_events.c.value,
                    ).where(
                        SystemSchema.workflow_events.c.workflow_uuid == target_uuid,
                        SystemSchema.workflow_events.c.key == key,
                    )
                ).fetchall()
            if len(rows) > 0:
                event.set()
        except Exception:
            dbos_logger.warning("Fallback workflow event poll failed", exc_info=True)

    def get_event(
        self,
        target_uuid: str,
        key: str,
        timeout_seconds: float = 60,
        caller_ctx: Optional[GetEventWorkflowContext] = None,
    ) -> Any:
        setup = self.get_event_setup(target_uuid, key, timeout_seconds, caller_ctx)
        if setup[0]:
            return setup[1]
        _, event, actual_timeout, payload, start_time = setup
        try:
            deadline = time.time() + actual_timeout
            while not event.is_set():
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                event.wait(timeout=min(remaining, self._event_recheck_interval()))
                if not event.is_set():
                    self.get_event_check(target_uuid, key, event)
            return self.get_event_consume(target_uuid, key, start_time, caller_ctx)
        finally:
            self.workflow_events_map.pop(payload)

    async def get_event_async(
        self,
        target_uuid: str,
        key: str,
        timeout_seconds: float = 60,
        caller_ctx: Optional[GetEventWorkflowContext] = None,
    ) -> Any:
        setup = await self._run_event_setup_async(
            self.workflow_events_map,
            self.get_event_setup,
            target_uuid,
            key,
            timeout_seconds,
            caller_ctx,
        )
        if setup[0]:
            return setup[1]
        _, event, actual_timeout, payload, start_time = setup
        try:
            deadline = time.time() + actual_timeout
            while not event.is_set():
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                await event.wait_async(
                    timeout=min(remaining, self._event_recheck_interval())
                )
                if not event.is_set() and time.time() < deadline:
                    await asyncio.to_thread(
                        self.get_event_check, target_uuid, key, event
                    )
            return await asyncio.to_thread(
                self.get_event_consume,
                target_uuid,
                key,
                start_time,
                caller_ctx,
            )
        finally:
            self.workflow_events_map.pop(payload)

    @db_retry()
    def get_queue_partitions(self, queue_name: str) -> List[str]:
        """
        Get all unique partition names associated with a queue for ENQUEUED workflows.

        Args:
            queue_name: The name of the queue to get partitions for

        Returns:
            A list of unique partition names for the queue
        """
        # Recursive-CTE loose index scan: neither Postgres nor SQLite can skip to the
        # next distinct value inside a plain SELECT DISTINCT, which degenerates into a
        # scan of every ENQUEUED row. Each iteration here is instead one index seek on
        # idx_workflow_status_partition_dequeue_v2, so cost scales with the number of
        # partitions rather than the backlog depth.
        ws = SystemSchema.workflow_status
        base_filter = sa.and_(
            ws.c.queue_name == queue_name,
            ws.c.status == WorkflowStatusString.ENQUEUED.value,
            # Redundant literal IN mirroring the index's own predicate: SQLite's partial-index prover runs at prepare time, so it can't see bound params and can't derive IN membership from =.
            ws.c.status.in_(
                [
                    sa.literal_column(f"'{WorkflowStatusString.ENQUEUED.value}'"),
                    sa.literal_column(f"'{WorkflowStatusString.PENDING.value}'"),
                ]
            ),
            # Only partitions this application can actually dequeue from.
            self._name_filter(ws.c.application_name, self.app_name),
        )
        partitions = (
            sa.select(sa.func.min(ws.c.queue_partition_key).label("pk"))
            .where(base_filter)
            .where(ws.c.queue_partition_key.isnot(None))
            .cte("partitions", recursive=True)
        )
        # Next key strictly after the previous one; > implies IS NOT NULL.
        next_pk = (
            sa.select(sa.func.min(ws.c.queue_partition_key))
            .where(base_filter)
            .where(ws.c.queue_partition_key > partitions.c.pk)
            .scalar_subquery()
        )
        partitions = partitions.union_all(
            sa.select(next_pk).where(partitions.c.pk.isnot(None))
        )
        query = sa.select(partitions.c.pk).where(partitions.c.pk.isnot(None))
        with self.engine.begin() as c:
            rows = c.execute(query).fetchall()
            return [row[0] for row in rows]

    def transition_delayed_workflows(self) -> None:
        """Transition DELAYED workflows whose delay has expired to ENQUEUED.

        For debounced workflows, clear the deduplication_id in the same atomic
        update: the ID is a debounce key held only while the workflow is DELAYED,
        so a later debounce with the same key starts a fresh workflow instead of
        bouncing this one (which is now committed to running).
        """
        now_ms = int(time.time() * 1000)
        with self.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.DELAYED.value
                )
                .where(SystemSchema.workflow_status.c.delay_until_epoch_ms <= now_ms)
                # Only what this application would dequeue: a peer's debounce key is not ours to clear.
                .where(
                    self._name_filter(
                        SystemSchema.workflow_status.c.application_name, self.app_name
                    )
                )
                .values(
                    status=WorkflowStatusString.ENQUEUED.value,
                    deduplication_id=sa.case(
                        (
                            SystemSchema.workflow_status.c.is_debounced == True,
                            None,
                        ),
                        else_=SystemSchema.workflow_status.c.deduplication_id,
                    ),
                )
            )

    def start_queued_workflows(
        self,
        queue: "Queue",
        executor_id: str,
        app_version: str,
        queue_partition_key: Optional[str],
        local_running_count: int = 0,
    ) -> List[str]:
        start_time_ms = int(time.time() * 1000)
        # Use the queue's locally cached private state to avoid recursive DB
        # reads via the @property getters within this transaction.
        if queue._limiter is not None:
            limiter_period_ms = int(queue._limiter["period"] * 1000)
        with self.engine.begin() as c:
            # Default to READ COMMITTED except with global concurrency limits or rate limits
            if self.engine.dialect.name == "postgresql" and (
                queue._concurrency is not None or queue._limiter is not None
            ):
                c.execute(sa.text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))

            # If there is a limiter, compute how many functions have started in its period.
            if queue._limiter is not None:
                query = (
                    sa.select(sa.func.count())
                    .select_from(SystemSchema.workflow_status)
                    .where(SystemSchema.workflow_status.c.queue_name == queue.name)
                    .where(SystemSchema.workflow_status.c.rate_limited == True)
                    .where(
                        SystemSchema.workflow_status.c.status.notin_(
                            [
                                WorkflowStatusString.ENQUEUED.value,
                                WorkflowStatusString.DELAYED.value,
                            ]
                        )
                    )
                    # Database clock on both sides, as the claim stamps started_at_epoch_ms with it.
                    .where(
                        SystemSchema.workflow_status.c.started_at_epoch_ms
                        > self._now_ms_sql() - limiter_period_ms
                    )
                    # Count only what this application would dequeue, matching the select below.
                    .where(
                        self._name_filter(
                            SystemSchema.workflow_status.c.application_name,
                            self.app_name,
                        )
                    )
                )
                if queue_partition_key is not None:
                    query = query.where(
                        SystemSchema.workflow_status.c.queue_partition_key
                        == queue_partition_key
                    )
                num_recent_queries = c.execute(query).fetchone()[0]  # type: ignore
                if num_recent_queries >= queue._limiter["limit"]:
                    return []

            # Compute max_tasks, the number of workflows that can be dequeued given the rate limit and the local and global concurrency limits.
            max_tasks = sys.maxsize

            if queue._limiter is not None:
                # Bound the claim by the limiter's remaining slots so a backlogged queue locks only what it can start.
                max_tasks = queue._limiter["limit"] - num_recent_queries

            if queue._worker_concurrency is not None:
                # Use the in-memory registry for this worker's running count — avoids a DB round trip.
                max_tasks = min(
                    max_tasks, max(0, queue._worker_concurrency - local_running_count)
                )

            if queue._concurrency is not None:
                # Global concurrency still requires a DB query since other workers may be running workflows too.
                global_pending_query = (
                    sa.select(sa.func.count())
                    .select_from(SystemSchema.workflow_status)
                    .where(SystemSchema.workflow_status.c.queue_name == queue.name)
                    .where(
                        SystemSchema.workflow_status.c.status
                        == WorkflowStatusString.PENDING.value
                    )
                    .where(
                        self._name_filter(
                            SystemSchema.workflow_status.c.application_name,
                            self.app_name,
                        )
                    )
                )
                if queue_partition_key is not None:
                    global_pending_query = global_pending_query.where(
                        SystemSchema.workflow_status.c.queue_partition_key
                        == queue_partition_key
                    )
                global_pending_workflows = c.execute(global_pending_query).scalar() or 0
                if global_pending_workflows > queue._concurrency:
                    dbos_logger.warning(
                        f"The total number of pending workflows ({global_pending_workflows}) on queue {queue.name} exceeds the global concurrency limit ({queue._concurrency})"
                    )
                available_tasks = max(0, queue._concurrency - global_pending_workflows)
                max_tasks = min(max_tasks, available_tasks)

            latest_version = c.execute(
                sa.select(SystemSchema.application_versions.c.version_name)
                # Own plus unclaimed: a named peer's deploy must not demote this one.
                .where(
                    self._name_filter(
                        SystemSchema.application_versions.c.application_name,
                        self.app_name,
                    )
                )
                .order_by(SystemSchema.application_versions.c.version_timestamp.desc())
                .limit(1)
            ).scalar()
            is_latest_version = latest_version is None or latest_version == app_version

            version_predicate = (
                SystemSchema.workflow_status.c.application_version == app_version
            )
            if is_latest_version:
                version_predicate = sa.or_(
                    SystemSchema.workflow_status.c.application_version == app_version,
                    SystemSchema.workflow_status.c.application_version.is_(None),
                )

            # Retrieve the first max_tasks workflows in the queue.
            # Only dequeue workflows of the local version; version-less ones only when this worker runs the latest version.
            # A rate limit is a global budget like concurrency: skip_locked would hand a peer
            # disjoint rows, letting it spend the same budget against its own pre-claim snapshot.
            skip_locks = queue._concurrency is None and queue._limiter is None
            query = (
                sa.select(
                    SystemSchema.workflow_status.c.workflow_uuid,
                )
                .select_from(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.queue_name == queue.name)
                .where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.ENQUEUED.value
                )
                .where(version_predicate)
                .where(
                    self._name_filter(
                        SystemSchema.workflow_status.c.application_name, self.app_name
                    )
                )
                # Without a global budget, use skip_locked to only select rows that can be
                # locked. With one, use no_wait so all processes see a consistent table.
                .with_for_update(skip_locked=skip_locks, nowait=(not skip_locks))
            )
            if queue_partition_key is not None:
                query = query.where(
                    SystemSchema.workflow_status.c.queue_partition_key
                    == queue_partition_key
                )
            query = query.order_by(
                SystemSchema.workflow_status.c.priority.asc(),
                SystemSchema.workflow_status.c.created_at.asc(),
            )
            if max_tasks != sys.maxsize:
                query = query.limit(int(max_tasks))

            rows = c.execute(query).fetchall()

            # Get the workflow IDs
            dequeued_ids: List[str] = [row[0] for row in rows]
            if len(dequeued_ids) > 0:
                dbos_logger.debug(
                    f"[{queue.name}] dequeueing {len(dequeued_ids)} task(s)"
                )
            claimed: Set[str] = set()
            # Chunk dequeues to stay under bind-parameter limits (SQLite caps at 32766, libpq at 65535).
            chunk_size = 4096
            for start in range(0, len(dequeued_ids), chunk_size):
                # Start the workflows by marking them PENDING and updating their executor ID.
                # RETURNING reports exactly the rows this statement flipped (requires SQLite >= 3.35).
                flipped_rows = c.execute(
                    SystemSchema.workflow_status.update()
                    .where(
                        SystemSchema.workflow_status.c.workflow_uuid.in_(
                            dequeued_ids[start : start + chunk_size]
                        )
                    )
                    .where(
                        SystemSchema.workflow_status.c.status
                        == WorkflowStatusString.ENQUEUED.value
                    )
                    # Re-check ownership alongside status, as the partitioned claim_guard does.
                    .where(
                        self._name_filter(
                            SystemSchema.workflow_status.c.application_name,
                            self.app_name,
                        )
                    )
                    .values(
                        status=WorkflowStatusString.PENDING.value,
                        application_version=app_version,
                        executor_id=executor_id,
                        # Claim it, so the unclaimed partition drains as workflows run.
                        application_name=self.app_name,
                        started_at_epoch_ms=self._now_ms_sql(),
                        rate_limited=queue._limiter is not None,
                        # Count this dispatch against the DLQ limit; no later insert does it.
                        recovery_attempts=SystemSchema.workflow_status.c.recovery_attempts
                        + 1,
                        updated_at=self._now_ms_sql(),
                        # If a timeout is set, set the deadline on dequeue
                        workflow_deadline_epoch_ms=sa.case(
                            (
                                sa.and_(
                                    SystemSchema.workflow_status.c.workflow_timeout_ms.isnot(
                                        None
                                    ),
                                    SystemSchema.workflow_status.c.workflow_deadline_epoch_ms.is_(
                                        None
                                    ),
                                ),
                                start_time_ms
                                + SystemSchema.workflow_status.c.workflow_timeout_ms,
                            ),
                            else_=SystemSchema.workflow_status.c.workflow_deadline_epoch_ms,
                        ),
                    )
                    .returning(SystemSchema.workflow_status.c.workflow_uuid)
                ).fetchall()
                claimed.update(row[0] for row in flipped_rows)

            # Return the IDs of all functions we started, in dequeue order: RETURNING order is unspecified.
            return [id for id in dequeued_ids if id in claimed]

    # Max heads dequeued per partitioned sweep: bounds the IN-list bind params below (SQLite caps at 32766, libpq at 65535); leftover partitions rotate in on later polls via the PENDING gate.
    PARTITIONED_DEQUEUE_SWEEP_CAP = 8192

    def start_queued_partitioned_workflows(
        self,
        queue: "Queue",
        executor_id: str,
        app_version: str,
    ) -> List[str]:
        """Dequeue every partition's head-of-line workflow in one transaction, at most
        PARTITIONED_DEQUEUE_SWEEP_CAP per sweep. Valid only for concurrency=1, no-limiter queues:
        all workers rank the same head, so guarded flips admit at most one row per partition.
        """
        assert queue._concurrency == 1
        assert queue._limiter is None
        assert queue._partition_queue
        start_time_ms = int(time.time() * 1000)
        ws = SystemSchema.workflow_status
        with self.engine.begin() as c:
            latest_version = c.execute(
                sa.select(SystemSchema.application_versions.c.version_name)
                # Own plus unclaimed: a named peer's deploy must not demote this one.
                .where(
                    self._name_filter(
                        SystemSchema.application_versions.c.application_name,
                        self.app_name,
                    )
                )
                .order_by(SystemSchema.application_versions.c.version_timestamp.desc())
                .limit(1)
            ).scalar()
            is_latest_version = latest_version is None or latest_version == app_version
            version_predicate = ws.c.application_version == app_version
            if is_latest_version:
                version_predicate = sa.or_(
                    ws.c.application_version == app_version,
                    ws.c.application_version.is_(None),
                )
            # Redundant literal IN mirroring idx_workflow_status_partition_dequeue_v2's predicate: SQLite's partial-index prover runs at prepare time, so it can't see bound params and can't derive IN membership from =.
            status_prover = ws.c.status.in_(
                [
                    sa.literal_column(f"'{WorkflowStatusString.ENQUEUED.value}'"),
                    sa.literal_column(f"'{WorkflowStatusString.PENDING.value}'"),
                ]
            )
            enq = sa.and_(
                ws.c.queue_name == queue.name,
                ws.c.status == WorkflowStatusString.ENQUEUED.value,
                status_prover,
                self._name_filter(ws.c.application_name, self.app_name),
            )
            # Walk distinct partition keys with a recursive-CTE loose index scan (one seek per key, mirroring get_queue_partitions) so sweep cost scales with partition count, not backlog depth.
            partitions = (
                sa.select(sa.func.min(ws.c.queue_partition_key).label("pk"))
                .where(enq)
                .where(ws.c.queue_partition_key.isnot(None))
                .cte("partitions", recursive=True)
            )
            next_pk = (
                sa.select(sa.func.min(ws.c.queue_partition_key))
                .where(enq)
                .where(ws.c.queue_partition_key > partitions.c.pk)
                .scalar_subquery()
            )
            partitions = partitions.union_all(
                sa.select(next_pk).where(partitions.c.pk.isnot(None))
            )
            # A partition's head is its first version-eligible row; workflow_uuid totalizes the order (same head for every worker under created_at ties), and the index's trailing workflow_uuid makes this a pure top-1 probe.
            head_query = (
                sa.select(ws.c.workflow_uuid)
                .where(enq)
                .where(ws.c.queue_partition_key == partitions.c.pk)
                .where(version_predicate)
                .order_by(
                    ws.c.priority.asc(),
                    ws.c.created_at.asc(),
                    ws.c.workflow_uuid.asc(),
                )
                .limit(1)
            )
            # Unscoped by design: a mutual-exclusion probe must block on any owner's row.
            pending_probe = (
                sa.select(sa.literal(1))
                .where(ws.c.queue_name == queue.name)
                .where(ws.c.status == WorkflowStatusString.PENDING.value)
                .where(status_prover)
                .where(ws.c.queue_partition_key.isnot(None))
                .where(ws.c.queue_partition_key == partitions.c.pk)
                .exists()
            )
            if self.engine.dialect.name == "postgresql":
                # LATERAL joins plan as tight nested loops; correlated scalar subqueries run as slower per-row SubPlans on Postgres.
                head = head_query.lateral("head")
                cand_query = (
                    sa.select(head.c.workflow_uuid)
                    .select_from(partitions.join(head, sa.true()))
                    .where(partitions.c.pk.isnot(None))
                    .where(~pending_probe)
                    .order_by(partitions.c.pk.asc())
                    .limit(self.PARTITIONED_DEQUEUE_SWEEP_CAP)
                )
            else:
                # SQLite has no LATERAL; a correlated scalar subquery probes each head, with version-ineligible (NULL-head) partitions filtered in the outer select.
                heads = (
                    sa.select(
                        head_query.scalar_subquery().label("workflow_uuid"),
                        partitions.c.pk,
                    )
                    .where(partitions.c.pk.isnot(None))
                    .where(~pending_probe)
                    .subquery("heads")
                )
                cand_query = (
                    sa.select(heads.c.workflow_uuid)
                    .where(heads.c.workflow_uuid.isnot(None))
                    .order_by(heads.c.pk.asc())
                    .limit(self.PARTITIONED_DEQUEUE_SWEEP_CAP)
                )
            candidate_ids = [row[0] for row in c.execute(cand_query).fetchall()]
            if not candidate_ids:
                return []

            # Re-check queue/partition/version alongside status so a row resume_workflows moved to another queue mid-sweep is dropped, not hijacked.
            claim_guard = sa.and_(
                ws.c.status == WorkflowStatusString.ENQUEUED.value,
                ws.c.queue_name == queue.name,
                ws.c.queue_partition_key.isnot(None),
                version_predicate,
                self._name_filter(ws.c.application_name, self.app_name),
            )
            # Lock the fixed candidate set -- never a LIMIT query, whose SKIP LOCKED could slide past a locked head and admit out of order. On SQLite this is an unlocked re-read; the RETURNING flip below is the guard.
            locked_rows = c.execute(
                sa.select(ws.c.workflow_uuid)
                .where(ws.c.workflow_uuid.in_(candidate_ids))
                .where(claim_guard)
                .with_for_update(skip_locked=True)
            ).fetchall()
            locked_ids = {row[0] for row in locked_rows}
            claim_ids = [id for id in candidate_ids if id in locked_ids]
            if not claim_ids:
                return []

            # Start the workflows by marking them PENDING; RETURNING reports exactly the rows this statement flipped (requires SQLite >= 3.35).
            flipped_rows = c.execute(
                ws.update()
                .where(ws.c.workflow_uuid.in_(claim_ids))
                .where(claim_guard)
                .values(
                    status=WorkflowStatusString.PENDING.value,
                    application_version=app_version,
                    executor_id=executor_id,
                    # Claim the row, as the unpartitioned dequeue does.
                    application_name=self.app_name,
                    started_at_epoch_ms=self._now_ms_sql(),
                    rate_limited=False,
                    # Count this dispatch against the DLQ limit; no later insert does it.
                    recovery_attempts=ws.c.recovery_attempts + 1,
                    updated_at=self._now_ms_sql(),
                    # If a timeout is set, set the deadline on dequeue
                    workflow_deadline_epoch_ms=sa.case(
                        (
                            sa.and_(
                                ws.c.workflow_timeout_ms.isnot(None),
                                ws.c.workflow_deadline_epoch_ms.is_(None),
                            ),
                            start_time_ms + ws.c.workflow_timeout_ms,
                        ),
                        else_=ws.c.workflow_deadline_epoch_ms,
                    ),
                )
                .returning(ws.c.workflow_uuid)
            ).fetchall()
            flipped_ids = {row[0] for row in flipped_rows}
            # Preserve partition order for submission.
            ret_ids = [id for id in claim_ids if id in flipped_ids]
            if ret_ids:
                dbos_logger.debug(f"[{queue.name}] dequeueing {len(ret_ids)} task(s)")
            return ret_ids

    @db_retry()
    def reenqueue_for_recovery(
        self, workflow_id: str, executor_ids: List[str], recovery_queue_name: str
    ) -> bool:
        """Return a PENDING workflow to a queue so it is re-dispatched. Returns
        whether the row was re-enqueued.

        The executor_ids predicate makes recovery idempotent. Recovery declares a
        set of executors dead; once any live executor dequeues the workflow,
        start_queued_workflows sets the workflow's executor ID on the row, so a
        duplicate recovery request for the original (dead) executor matches
        nothing instead of re-enqueueing a running workflow.
        """
        if not executor_ids:
            return False
        with self.engine.begin() as c:
            res = c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.PENDING.value
                )
                .where(SystemSchema.workflow_status.c.executor_id.in_(executor_ids))
                .values(
                    status=WorkflowStatusString.ENQUEUED.value,
                    started_at_epoch_ms=None,
                    updated_at=self._now_ms_sql(),
                    queue_name=sa.func.coalesce(
                        SystemSchema.workflow_status.c.queue_name, recovery_queue_name
                    ),
                )
            )
            return res.rowcount > 0

    T = TypeVar("T")

    def call_function_as_step(
        self, fn: Callable[[], T], function_name: str, ctx: Optional[DBOSContext]
    ) -> T:
        start_time = int(time.time() * 1000)
        if ctx and ctx.is_transaction():
            raise Exception(f"Invalid call to `{function_name}` inside a transaction")
        if ctx and ctx.is_workflow():
            res = self.check_operation_execution(
                ctx.workflow_id, ctx.function_id, function_name
            )
            if res is not None:
                if res["output"] is not None:
                    resstat: SystemDatabase.T = cast(
                        SystemDatabase.T,
                        deserialize_value(
                            res["output"],
                            res["serialization"],
                            self.serializer,
                        ),
                    )
                    return resstat
                elif res["error"] is not None:
                    e: Exception = deserialize_exception(
                        res["error"], res["serialization"], self.serializer
                    )
                    raise e
                else:
                    raise Exception(
                        f"Recorded output and error are both None for {function_name}"
                    )
        result = fn()
        if ctx and ctx.is_workflow():
            serval, serialization = serialize_value(result, None, self.serializer)
            self.record_operation_result(
                {
                    "workflow_uuid": ctx.workflow_id,
                    "function_id": ctx.function_id,
                    "function_name": function_name,
                    "started_at_epoch_ms": start_time,
                    "output": serval,
                    "serialization": serialization,
                    "error": None,
                }
            )
        return result

    async def call_coroutine_as_step(
        self,
        fn: Callable[[], Awaitable[T]],
        function_name: str,
        ctx: Optional[DBOSContext],
    ) -> T:
        start_time = int(time.time() * 1000)
        if ctx and ctx.is_transaction():
            raise Exception(f"Invalid call to `{function_name}` inside a transaction")
        if ctx and ctx.is_workflow():
            res = await asyncio.to_thread(
                self.check_operation_execution,
                ctx.workflow_id,
                ctx.function_id,
                function_name,
            )
            if res is not None:
                if res["output"] is not None:
                    return cast(
                        SystemDatabase.T,
                        deserialize_value(
                            res["output"],
                            res["serialization"],
                            self.serializer,
                        ),
                    )
                elif res["error"] is not None:
                    e: Exception = deserialize_exception(
                        res["error"], res["serialization"], self.serializer
                    )
                    raise e
                else:
                    raise Exception(
                        f"Recorded output and error are both None for {function_name}"
                    )
        result = await fn()
        if ctx and ctx.is_workflow():
            serval, serialization = serialize_value(result, None, self.serializer)
            await asyncio.to_thread(
                self.record_operation_result,
                {
                    "workflow_uuid": ctx.workflow_id,
                    "function_id": ctx.function_id,
                    "function_name": function_name,
                    "started_at_epoch_ms": start_time,
                    "output": serval,
                    "serialization": serialization,
                    "error": None,
                },
            )
        return result

    @db_retry()
    def init_workflow(
        self,
        status: WorkflowStatusInternal,
        *,
        owner_xid: Optional[str],
    ) -> tuple[WorkflowStatuses, Optional[int], bool]:
        """
        Record the initial status and inputs for a workflow, and indicate if this is a new record
        """
        with self.engine.begin() as conn:
            wf_status, workflow_deadline_epoch_ms, should_execute = (
                self._insert_workflow_status(
                    status,
                    conn,
                    owner_xid=owner_xid,
                )
            )
        DebugTriggers.debug_trigger_point(DebugTriggers.DEBUG_TRIGGER_INITWF_COMMIT)
        return wf_status, workflow_deadline_epoch_ms, should_execute

    def _max_partition_key_created_at(
        self, keys: List[Tuple[Optional[str], str]]
    ) -> Dict[Tuple[Optional[str], str], int]:
        """Highest created_at among still-active (ENQUEUED/PENDING) rows per (queue, partition key), to seed the in-memory cursor so per-key order survives a restart/rebalance."""
        if not keys:
            return {}
        queue_names = {queue_name for queue_name, _ in keys}
        partition_keys = {partition_key for _, partition_key in keys}
        # One arm per status so idx_workflow_status_partition_dequeue_v2 can seek: a status IN (...) matching that index's own predicate is dropped as redundant, leaving its status column unbound and blocking the seek on queue_partition_key.
        arms = [
            sa.select(
                SystemSchema.workflow_status.c.queue_name,
                SystemSchema.workflow_status.c.queue_partition_key,
                sa.func.max(SystemSchema.workflow_status.c.created_at),
            )
            .where(SystemSchema.workflow_status.c.queue_name.in_(queue_names))
            .where(
                SystemSchema.workflow_status.c.queue_partition_key.in_(partition_keys)
            )
            .where(SystemSchema.workflow_status.c.status == status)
            .group_by(
                SystemSchema.workflow_status.c.queue_name,
                SystemSchema.workflow_status.c.queue_partition_key,
            )
            for status in (
                WorkflowStatusString.ENQUEUED.value,
                WorkflowStatusString.PENDING.value,
            )
        ]
        with self.engine.begin() as c:
            rows = c.execute(sa.union_all(*arms)).fetchall()
        # The IN lists cross-product, so discard groups for pairs this batch never asked about.
        requested = set(keys)
        seeds: Dict[Tuple[Optional[str], str], int] = {}
        for queue_name, partition_key, max_created_at in rows:
            key = (queue_name, partition_key)
            if max_created_at is None or key not in requested:
                continue
            # Fold the per-status arms back into one high-water mark per key.
            seeds[key] = max(seeds.get(key, 0), max_created_at)
        return seeds

    def init_workflows(self, statuses: List[WorkflowStatusInternal]) -> Set[str]:
        """
        Batch-insert ENQUEUED workflow status rows in a single transaction.

        Rows whose workflow_uuid already exists are skipped rather than updated,
        making this idempotent under redelivery (e.g. Kafka). Returns the IDs of
        the rows actually inserted.
        """
        if len(statuses) == 0:
            return set()
        # Stamp created_at monotonic within each partition key so per-key order holds across batches; unordered rows (no key) get wall-clock time.
        now_ms = int(time.time() * 1000)
        # On first sight of a key, seed its cursor from the DB high-water mark so per-key order survives a restart/rebalance instead of resetting to wall-clock.
        # Cursors are scoped per (queue, partition key), matching how the dequeue query orders rows.
        batch_keys = {
            (s["queue_name"], s["queue_partition_key"])
            for s in statuses
            if s["queue_partition_key"] is not None
        }
        with self._batch_created_at_lock:
            unseen_keys = [
                k for k in batch_keys if k not in self._batch_created_at_cursors
            ]
        seeds = self._max_partition_key_created_at(unseen_keys)
        created_ats: List[int] = []
        with self._batch_created_at_lock:
            for seeded_key, seeded_max in seeds.items():
                # Advance past the DB high-water mark; max() guards a concurrent batch that already advanced this key.
                self._batch_created_at_cursors[seeded_key] = max(
                    self._batch_created_at_cursors.get(seeded_key, 0), seeded_max + 1
                )
            next_for_key: Dict[Tuple[Optional[str], str], int] = {}
            for status in statuses:
                partition_key = status["queue_partition_key"]
                if partition_key is None:
                    created_ats.append(now_ms)
                    continue
                key = (status["queue_name"], partition_key)
                value = next_for_key.get(key)
                if value is None:
                    value = max(now_ms, self._batch_created_at_cursors.get(key, 0))
                created_ats.append(value)
                next_for_key[key] = value + 1
            self._batch_created_at_cursors.update(next_for_key)
        rows: List[Dict[str, Any]] = []
        for i, status in enumerate(statuses):
            assert status["status"] == WorkflowStatusString.ENQUEUED.value
            assert status["deduplication_id"] is None
            rows.append(
                {
                    "workflow_uuid": status["workflow_uuid"],
                    "status": status["status"],
                    "name": status["name"],
                    "class_name": status["class_name"],
                    "config_name": status["config_name"],
                    "output": None,
                    "error": None,
                    "executor_id": status["executor_id"],
                    "application_version": status["app_version"],
                    "application_id": status["app_id"],
                    "authenticated_user": status["authenticated_user"],
                    "authenticated_roles": status["authenticated_roles"],
                    "assumed_role": status["assumed_role"],
                    "queue_name": status["queue_name"],
                    "recovery_attempts": 0,
                    "workflow_timeout_ms": status["workflow_timeout_ms"],
                    "workflow_deadline_epoch_ms": status["workflow_deadline_epoch_ms"],
                    "deduplication_id": None,
                    "priority": status["priority"],
                    "inputs": status["inputs"],
                    "serialization": status["serialization"],
                    "queue_partition_key": status["queue_partition_key"],
                    "parent_workflow_id": status["parent_workflow_id"],
                    "owner_xid": None,
                    "delay_until_epoch_ms": status["delay_until_epoch_ms"],
                    "attributes": status["attributes"],
                    "schedule_name": status["schedule_name"],
                    "application_name": status["application_name"],
                    "created_at": created_ats[i],
                    "updated_at": created_ats[i],
                }
            )
        inserted: Set[str] = set()
        # Chunk to stay well under bind-parameter limits (~30 params per row).
        chunk_size = 500
        with self.engine.begin() as conn:
            for start in range(0, len(rows), chunk_size):
                chunk = rows[start : start + chunk_size]
                result = conn.execute(
                    self.dialect.insert(SystemSchema.workflow_status)
                    .values(chunk)
                    .on_conflict_do_nothing(index_elements=["workflow_uuid"])
                    .returning(SystemSchema.workflow_status.c.workflow_uuid)
                )
                inserted.update(row[0] for row in result)
        return inserted

    def _apply_caller_schema(self, conn: Union[sa.Connection, Session]) -> None:
        """Translate the placeholder schema on a caller-owned Connection/Session (the caller's own statements are unaffected)."""
        # Set the option in place on the underlying Connection. Session.connection(execution_options=...)
        # is silently ignored once the caller has already procured the connection (run any statement) in
        # this transaction, which is the normal case for a caller-owned transaction.
        if isinstance(conn, Session):
            conn = conn.connection()
        existing = conn.get_execution_options().get("schema_translate_map") or {}
        conn.execution_options(
            schema_translate_map={**existing, SCHEMA_PLACEHOLDER: self.schema}
        )

    def init_workflow_with_connection(
        self,
        status: WorkflowStatusInternal,
        conn: Union[sa.Connection, Session],
        *,
        owner_xid: Optional[str] = None,
    ) -> tuple[WorkflowStatuses, Optional[int], bool]:
        """
        Record the initial status and inputs for a workflow using a caller-owned
        SQLAlchemy Connection or ORM Session.

        Does not begin, commit, rollback, or retry. The caller owns the
        transaction. The connection or session must target the DBOS system
        database; it cannot atomically span a separate application database.
        """
        self._apply_caller_schema(conn)
        return self._insert_workflow_status(
            status,
            conn,
            owner_xid=owner_xid,
        )

    def check_connection(self) -> None:
        try:
            with self.engine.begin() as conn:
                conn.execute(sa.text("SELECT 1")).fetchall()
        except Exception as e:
            dbos_logger.error(f"Error connecting to the DBOS system database: {e}")
            raise

    def _stream_insert_stmt(
        self,
        workflow_uuid: str,
        function_id: int,
        key: str,
        serialized_value: Optional[str],
        serialization: Optional[str],
    ) -> sa.Insert:
        """Build an atomic INSERT...SELECT that computes the next stream offset."""
        return sa.insert(SystemSchema.streams).from_select(
            ["workflow_uuid", "function_id", "key", "value", "serialization", "offset"],
            sa.select(
                sa.literal(workflow_uuid).label("workflow_uuid"),
                sa.literal(function_id).label("function_id"),
                sa.literal(key).label("key"),
                sa.literal(serialized_value).label("value"),
                sa.literal(serialization).label("serialization"),
                (
                    sa.func.coalesce(
                        sa.select(sa.func.max(SystemSchema.streams.c.offset))
                        .where(
                            SystemSchema.streams.c.workflow_uuid == workflow_uuid,
                            SystemSchema.streams.c.key == key,
                        )
                        .correlate(None)
                        .scalar_subquery(),
                        -1,
                    )
                    + 1
                ).label("offset"),
            ),
        )

    def write_stream_from_step(
        self,
        workflow_uuid: str,
        function_id: int,
        key: str,
        value: Any,
        *,
        serialization_type: WorkflowSerializationFormat,
    ) -> None:
        """
        Write a key-value pair to the stream at the first unused offset.
        """
        # Serialize the value before storing
        serialized_value, serialization = serialize_value(
            value,
            serialization_type,
            self.serializer,
        )

        stmt = self._stream_insert_stmt(
            workflow_uuid, function_id, key, serialized_value, serialization
        )

        while True:
            try:
                with self.engine.begin() as c:
                    c.execute(stmt)
                self._signal_notification(
                    _dbos_streams_channel, f"{workflow_uuid}::{key}"
                )
                return
            except sa.exc.IntegrityError:
                dbos_logger.warning(
                    f"Stream offset conflict for workflow {workflow_uuid}, key {key}; retrying"
                )
                time.sleep(0.1)
                continue

    @db_retry()
    def write_stream_from_workflow(
        self,
        workflow_uuid: str,
        function_id: int,
        key: str,
        value: Any,
        *,
        serialization_type: WorkflowSerializationFormat,
    ) -> None:
        serialized_value, serialization = serialize_value(
            value,
            serialization_type,
            self.serializer,
        )

        """
        Write a key-value pair to the stream at the first unused offset.
        """
        function_name = (
            "DBOS.closeStream"
            if value == _dbos_stream_closed_sentinel
            else "DBOS.writeStream"
        )
        start_time = int(time.time() * 1000)
        stmt = self._stream_insert_stmt(
            workflow_uuid,
            function_id,
            key,
            serialized_value,
            serialization,
        )
        while True:
            with self.engine.begin() as c:

                recorded_output = self._check_operation_execution_txn(
                    workflow_uuid, function_id, function_name, conn=c
                )
                if recorded_output is not None:
                    dbos_logger.debug(
                        f"Replaying writeStream, id: {function_id}, key: {key}"
                    )
                    return

                try:
                    c.execute(stmt)
                except sa.exc.IntegrityError:
                    dbos_logger.warning(
                        f"Stream offset conflict for workflow {workflow_uuid}, key {key}; retrying"
                    )
                    time.sleep(0.1)
                    continue

                output: OperationResultInternal = {
                    "workflow_uuid": workflow_uuid,
                    "function_id": function_id,
                    "function_name": function_name,
                    "started_at_epoch_ms": start_time,
                    "output": None,
                    "error": None,
                    "serialization": None,
                }
                self._record_operation_result_txn(
                    output, int(time.time() * 1000), conn=c
                )
            self._signal_notification(_dbos_streams_channel, f"{workflow_uuid}::{key}")
            return

    def close_stream(self, workflow_uuid: str, function_id: int, key: str) -> None:
        """Write a sentinel value to the stream at the first unused offset to mark it as closed."""
        self.write_stream_from_workflow(
            workflow_uuid,
            function_id,
            key,
            _dbos_stream_closed_sentinel,
            serialization_type=WorkflowSerializationFormat.PORTABLE,
        )

    def register_stream_listener(
        self, workflow_uuid: str, key: str
    ) -> Tuple[LoopAwareEvent, str]:
        """Register an event for the listener to signal when the stream is written.

        Returns the event to wait on and the payload key to unregister later.
        Must be called before reading so a notification arriving between a read
        and the wait is not lost.
        """
        payload = f"{workflow_uuid}::{key}"
        _, event = self.streams_map.set(payload, LoopAwareEvent(), (workflow_uuid, key))
        return event, payload

    def unregister_stream_listener(self, payload: str) -> None:
        """Drop a previously registered stream listener event."""
        self.streams_map.pop(payload)

    @db_retry()
    def read_stream_value(
        self, workflow_uuid: str, key: str, offset: int
    ) -> Tuple[Optional[str], Any]:
        """Read the stream value at offset and the owning workflow's status in one round trip.

        Returns (status, value). status is None if the workflow does not exist; value is
        _no_stream_value if nothing is written at offset. Both come from one statement, so they
        share a snapshot. A terminal status does not imply the stream is complete: cancel and
        timeout set it out-of-band while the workflow is still running, so a caller that stops
        reading must first drain to the first empty offset.
        """
        # LEFT JOIN so a workflow with nothing at offset still reports its status. Matching offset
        # exactly keeps this a single index lookup on the (workflow_uuid, key, offset) primary key.
        join = SystemSchema.workflow_status.outerjoin(
            SystemSchema.streams,
            sa.and_(
                SystemSchema.streams.c.workflow_uuid
                == SystemSchema.workflow_status.c.workflow_uuid,
                SystemSchema.streams.c.key == key,
                SystemSchema.streams.c.offset == offset,
            ),
        )
        # Polling read (listener-less clients poll the offset) under the limiter; inside db_retry so the permit frees across backoff.
        with self.poll_limiter, self.engine.begin() as c:
            row = c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.status,
                    SystemSchema.streams.c.value,
                    SystemSchema.streams.c.serialization,
                    SystemSchema.streams.c.offset,
                )
                .select_from(join)
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_uuid)
            ).fetchone()

        if row is None:
            return None, _no_stream_value
        # streams.offset is non-nullable, so a NULL here means the join matched nothing at offset.
        if row[3] is None:
            return row[0], _no_stream_value
        return row[0], deserialize_value(row[1], row[2], self.serializer)

    def garbage_collect(
        self,
        cutoff_epoch_timestamp_ms: Optional[int],
        rows_threshold: Optional[int],
        batch_size: Optional[int],
    ) -> Optional[tuple[int, list[str]]]:
        if batch_size is not None and batch_size < 1:
            raise ValueError(f"batch_size must be a positive integer, got {batch_size}")
        if rows_threshold is not None:
            with self.engine.begin() as c:
                # Get the created_at timestamp of the rows_threshold newest row
                result = c.execute(
                    sa.select(SystemSchema.workflow_status.c.created_at)
                    .where(
                        self._name_filter(
                            SystemSchema.workflow_status.c.application_name,
                            self.app_name,
                        )
                    )
                    .order_by(SystemSchema.workflow_status.c.created_at.desc())
                    .limit(1)
                    .offset(rows_threshold - 1)
                ).fetchone()

                if result is not None:
                    rows_based_cutoff = result[0]
                    # Use the more restrictive cutoff (higher timestamp = more recent = more deletion)
                    if (
                        cutoff_epoch_timestamp_ms is None
                        or rows_based_cutoff > cutoff_epoch_timestamp_ms
                    ):
                        cutoff_epoch_timestamp_ms = rows_based_cutoff

        if cutoff_epoch_timestamp_ms is None:
            return None

        # Delete all workflows older than cutoff that are NOT PENDING, ENQUEUED, or DELAYED
        gc_filter = sa.and_(
            SystemSchema.workflow_status.c.created_at < cutoff_epoch_timestamp_ms,
            ~SystemSchema.workflow_status.c.status.in_(
                [
                    WorkflowStatusString.PENDING.value,
                    WorkflowStatusString.ENQUEUED.value,
                    WorkflowStatusString.DELAYED.value,
                ]
            ),
            # Unclaimed rows included: excluding them would leak pre-upgrade rows forever.
            self._name_filter(
                SystemSchema.workflow_status.c.application_name, self.app_name
            ),
        )

        if batch_size is None:
            with self.engine.begin() as c:
                c.execute(sa.delete(SystemSchema.workflow_status).where(gc_filter))
        else:
            # Batch-delete by advancing a created_at watermark, one committed transaction per batch
            watermark = 0
            while True:
                with self.engine.begin() as c:
                    # Find the created_at of the batch_size-th oldest eligible row above the watermark
                    step = c.execute(
                        sa.select(SystemSchema.workflow_status.c.created_at)
                        .where(
                            gc_filter,
                            SystemSchema.workflow_status.c.created_at > watermark,
                        )
                        .order_by(SystemSchema.workflow_status.c.created_at)
                        .limit(1)
                        .offset(batch_size - 1)
                    ).scalar()
                    if step is None:
                        # Final batch: delete every remaining eligible row, even below the watermark
                        c.execute(
                            sa.delete(SystemSchema.workflow_status).where(gc_filter)
                        )
                        break
                    # Delete the batch; created_at ties may push it slightly over batch_size
                    c.execute(
                        sa.delete(SystemSchema.workflow_status).where(
                            gc_filter,
                            SystemSchema.workflow_status.c.created_at > watermark,
                            SystemSchema.workflow_status.c.created_at <= step,
                        )
                    )
                watermark = step

        with self.engine.begin() as c:
            # Then, get the IDs of all remaining old workflows
            pending_enqueued_result = c.execute(
                sa.select(SystemSchema.workflow_status.c.workflow_uuid).where(
                    SystemSchema.workflow_status.c.created_at
                    < cutoff_epoch_timestamp_ms,
                    self._name_filter(
                        SystemSchema.workflow_status.c.application_name, self.app_name
                    ),
                )
            ).fetchall()

            # Return the final cutoff and workflow IDs
            return cutoff_epoch_timestamp_ms, [
                row[0] for row in pending_enqueued_result
            ]

    def list_timed_out_workflow_ids(self, cutoff_epoch_timestamp_ms: int) -> List[str]:
        """IDs of this application's in-flight workflows created before the cutoff.
        Claiming-scoped, so an upgrade still times out its own unclaimed workflows."""
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(SystemSchema.workflow_status.c.workflow_uuid).where(
                    SystemSchema.workflow_status.c.status.in_(
                        [
                            WorkflowStatusString.PENDING.value,
                            WorkflowStatusString.ENQUEUED.value,
                            WorkflowStatusString.DELAYED.value,
                        ]
                    ),
                    SystemSchema.workflow_status.c.created_at
                    <= cutoff_epoch_timestamp_ms,
                    self._name_filter(
                        SystemSchema.workflow_status.c.application_name, self.app_name
                    ),
                )
            ).fetchall()
            return [row[0] for row in rows]

    def get_metrics(
        self,
        start_time: str,
        end_time: str,
        application_name: Optional[List[str]] = None,
    ) -> List[MetricData]:
        """
        Retrieve the number of workflows and steps that ran in a time range.

        Args:
            start_time: ISO 8601 formatted start time
            end_time: ISO 8601 formatted end time
            application_name: Count only workflows and steps owned by these
                applications. By default, only count this application's.
        """
        # Convert ISO 8601 times to epoch milliseconds
        start_epoch_ms = int(
            datetime.datetime.fromisoformat(start_time).timestamp() * 1000
        )
        end_epoch_ms = int(datetime.datetime.fromisoformat(end_time).timestamp() * 1000)

        metrics: List[MetricData] = []

        with self.engine.begin() as c:
            # Query workflow metrics
            workflow_query = (
                sa.select(
                    SystemSchema.workflow_status.c.name,
                    func.count(SystemSchema.workflow_status.c.workflow_uuid).label(
                        "count"
                    ),
                )
                .where(
                    sa.and_(
                        SystemSchema.workflow_status.c.created_at >= start_epoch_ms,
                        SystemSchema.workflow_status.c.created_at < end_epoch_ms,
                    )
                )
                .group_by(SystemSchema.workflow_status.c.name)
            )
            workflow_query = workflow_query.where(
                self._observability_filter(
                    SystemSchema.workflow_status.c.application_name, application_name
                )
            )

            workflow_results = c.execute(workflow_query).fetchall()
            for row in workflow_results:
                metrics.append(
                    MetricData(
                        metric_type="workflow_count",
                        metric_name=row[0],
                        value=row[1],
                    )
                )

            # Query step metrics
            step_query = (
                sa.select(
                    SystemSchema.operation_outputs.c.function_name,
                    func.count().label("count"),
                )
                .where(
                    sa.and_(
                        SystemSchema.operation_outputs.c.completed_at_epoch_ms
                        >= start_epoch_ms,
                        SystemSchema.operation_outputs.c.completed_at_epoch_ms
                        < end_epoch_ms,
                    )
                )
                .group_by(SystemSchema.operation_outputs.c.function_name)
            )
            step_query = step_query.where(
                self._observability_filter(
                    SystemSchema.operation_outputs.c.application_name, application_name
                )
            )

            step_results = c.execute(step_query).fetchall()
            for row in step_results:
                metrics.append(
                    MetricData(
                        metric_type="step_count",
                        metric_name=row[0],
                        value=row[1],
                    )
                )

        return metrics

    @db_retry()
    def get_checkpoint_name(
        self, *, workflow_id: str, function_id: int
    ) -> Optional[str]:
        """Return the name of the checkpoint recorded at this point in history, if any."""
        with self.engine.begin() as c:
            checkpoint_name: str | None = c.execute(
                sa.select(SystemSchema.operation_outputs.c.function_name).where(
                    (SystemSchema.operation_outputs.c.workflow_uuid == workflow_id)
                    & (SystemSchema.operation_outputs.c.function_id == function_id)
                )
            ).scalar()
            return checkpoint_name

    @db_retry()
    def patch(self, *, workflow_id: str, function_id: int, patch_name: str) -> bool:
        """If there is no checkpoint for this point in history,
        insert a patch marker and return True.
        Otherwise, return whether the checkpoint is this patch marker."""
        with self.engine.begin() as c:
            checkpoint_name: str | None = c.execute(
                sa.select(SystemSchema.operation_outputs.c.function_name).where(
                    (SystemSchema.operation_outputs.c.workflow_uuid == workflow_id)
                    & (SystemSchema.operation_outputs.c.function_id == function_id)
                )
            ).scalar()
            if checkpoint_name is None:
                result: OperationResultInternal = {
                    "workflow_uuid": workflow_id,
                    "function_id": function_id,
                    "function_name": patch_name,
                    "output": None,
                    "error": None,
                    "serialization": None,
                    "started_at_epoch_ms": int(time.time() * 1000),
                }
                self._record_operation_result_txn(result, int(time.time() * 1000), c)
                return True
            else:
                return checkpoint_name == patch_name

    @db_retry()
    def deprecate_patch(
        self, *, workflow_id: str, function_id: int, patch_name: str
    ) -> bool:
        """Respect patch markers in history, but do not introduce new patch markers"""
        with self.engine.begin() as c:
            checkpoint_name: str | None = c.execute(
                sa.select(SystemSchema.operation_outputs.c.function_name).where(
                    (SystemSchema.operation_outputs.c.workflow_uuid == workflow_id)
                    & (SystemSchema.operation_outputs.c.function_id == function_id)
                )
            ).scalar()
            return checkpoint_name == patch_name

    def _get_direct_children(self, workflow_ids: list[str]) -> list[str]:
        """
        Get the immediate (one-level) child workflow IDs for a set of workflows.

        Args:
            workflow_ids: The workflow UUIDs to get the direct children of

        Returns:
            A list of the direct child workflow IDs
        """
        if not workflow_ids:
            return []
        with self.engine.begin() as c:
            child_rows = c.execute(
                sa.select(SystemSchema.workflow_status.c.workflow_uuid).where(
                    SystemSchema.workflow_status.c.parent_workflow_id.in_(workflow_ids)
                )
            ).fetchall()
        return [row[0] for row in child_rows]

    def get_workflow_children(self, workflow_id: str) -> list[str]:
        """
        Recursively get all child workflow IDs for a workflow.

        Args:
            workflow_id: The workflow UUID to get children for

        Returns:
            A list of all child (and grandchild, etc.) workflow IDs
        """
        descendants: set[str] = set()
        frontier: list[str] = [workflow_id]
        while frontier:
            children = self._get_direct_children(frontier)
            frontier = [c for c in children if c not in descendants]
            descendants.update(frontier)
        return list(descendants)

    def export_workflow(
        self, workflow_id: str, *, export_children: bool
    ) -> list[ExportedWorkflow]:
        """
        Export all entries for a workflow in a portable format.

        Args:
            workflow_id: The workflow UUID to export
            export_children: If True, also export all child workflows recursively

        Returns:
            A list of ExportedWorkflow containing all workflow data
        """
        workflow_ids = [workflow_id]
        if export_children:
            workflow_ids.extend(self.get_workflow_children(workflow_id))

        exported_workflows: list[ExportedWorkflow] = []

        with self.engine.begin() as c:
            for wf_id in workflow_ids:
                # Export workflow_status
                status_row = c.execute(
                    sa.select(
                        SystemSchema.workflow_status.c.workflow_uuid,
                        SystemSchema.workflow_status.c.status,
                        SystemSchema.workflow_status.c.name,
                        SystemSchema.workflow_status.c.authenticated_user,
                        SystemSchema.workflow_status.c.assumed_role,
                        SystemSchema.workflow_status.c.authenticated_roles,
                        SystemSchema.workflow_status.c.output,
                        SystemSchema.workflow_status.c.error,
                        SystemSchema.workflow_status.c.executor_id,
                        SystemSchema.workflow_status.c.created_at,
                        SystemSchema.workflow_status.c.updated_at,
                        SystemSchema.workflow_status.c.application_version,
                        SystemSchema.workflow_status.c.application_id,
                        SystemSchema.workflow_status.c.class_name,
                        SystemSchema.workflow_status.c.config_name,
                        SystemSchema.workflow_status.c.recovery_attempts,
                        SystemSchema.workflow_status.c.queue_name,
                        SystemSchema.workflow_status.c.workflow_timeout_ms,
                        SystemSchema.workflow_status.c.workflow_deadline_epoch_ms,
                        SystemSchema.workflow_status.c.started_at_epoch_ms,
                        SystemSchema.workflow_status.c.deduplication_id,
                        SystemSchema.workflow_status.c.inputs,
                        SystemSchema.workflow_status.c.priority,
                        SystemSchema.workflow_status.c.queue_partition_key,
                        SystemSchema.workflow_status.c.forked_from,
                        SystemSchema.workflow_status.c.parent_workflow_id,
                        SystemSchema.workflow_status.c.serialization,
                        SystemSchema.workflow_status.c.delay_until_epoch_ms,
                        SystemSchema.workflow_status.c.was_forked_from,
                        SystemSchema.workflow_status.c.rate_limited,
                        SystemSchema.workflow_status.c.completed_at,
                        SystemSchema.workflow_status.c.attributes,
                        SystemSchema.workflow_status.c.schedule_name,
                        SystemSchema.workflow_status.c.debounce_deadline_epoch_ms,
                        SystemSchema.workflow_status.c.is_debounced,
                        SystemSchema.workflow_status.c.application_name,
                        # owner_xid is intentionally omitted: it is a transient
                        # transaction-ownership token, not logical workflow state
                        # (get_workflow_status also returns None for it), and a
                        # source database's xid is meaningless in the target.
                    ).where(SystemSchema.workflow_status.c.workflow_uuid == wf_id)
                ).fetchone()

                if status_row is None:
                    raise DBOSNonExistentWorkflowError("export", wf_id)

                workflow_status: dict[str, Any] = {
                    "workflow_uuid": status_row[0],
                    "status": status_row[1],
                    "name": status_row[2],
                    "authenticated_user": status_row[3],
                    "assumed_role": status_row[4],
                    "authenticated_roles": status_row[5],
                    "output": status_row[6],
                    "error": status_row[7],
                    "executor_id": status_row[8],
                    "created_at": status_row[9],
                    "updated_at": status_row[10],
                    "application_version": status_row[11],
                    "application_id": status_row[12],
                    "class_name": status_row[13],
                    "config_name": status_row[14],
                    "recovery_attempts": status_row[15],
                    "queue_name": status_row[16],
                    "workflow_timeout_ms": status_row[17],
                    "workflow_deadline_epoch_ms": status_row[18],
                    "started_at_epoch_ms": status_row[19],
                    "deduplication_id": status_row[20],
                    "inputs": status_row[21],
                    "priority": status_row[22],
                    "queue_partition_key": status_row[23],
                    "forked_from": status_row[24],
                    "parent_workflow_id": status_row[25],
                    "serialization": status_row[26],
                    "delay_until_epoch_ms": status_row[27],
                    "was_forked_from": status_row[28],
                    "rate_limited": status_row[29],
                    "completed_at": status_row[30],
                    "attributes": status_row[31],
                    "schedule_name": status_row[32],
                    "debounce_deadline_epoch_ms": status_row[33],
                    "is_debounced": status_row[34],
                    "application_name": status_row[35],
                }

                # Export operation_outputs
                output_rows = c.execute(
                    sa.select(
                        SystemSchema.operation_outputs.c.workflow_uuid,
                        SystemSchema.operation_outputs.c.function_id,
                        SystemSchema.operation_outputs.c.function_name,
                        SystemSchema.operation_outputs.c.output,
                        SystemSchema.operation_outputs.c.error,
                        SystemSchema.operation_outputs.c.child_workflow_id,
                        SystemSchema.operation_outputs.c.started_at_epoch_ms,
                        SystemSchema.operation_outputs.c.completed_at_epoch_ms,
                        SystemSchema.operation_outputs.c.serialization,
                        SystemSchema.operation_outputs.c.application_name,
                    ).where(SystemSchema.operation_outputs.c.workflow_uuid == wf_id)
                ).fetchall()

                operation_outputs: list[dict[str, Any]] = [
                    {
                        "workflow_uuid": row[0],
                        "function_id": row[1],
                        "function_name": row[2],
                        "output": row[3],
                        "error": row[4],
                        "child_workflow_id": row[5],
                        "started_at_epoch_ms": row[6],
                        "completed_at_epoch_ms": row[7],
                        "serialization": row[8],
                        "application_name": row[9],
                    }
                    for row in output_rows
                ]

                # Export workflow_events
                event_rows = c.execute(
                    sa.select(
                        SystemSchema.workflow_events.c.workflow_uuid,
                        SystemSchema.workflow_events.c.key,
                        SystemSchema.workflow_events.c.value,
                        SystemSchema.workflow_events.c.serialization,
                    ).where(SystemSchema.workflow_events.c.workflow_uuid == wf_id)
                ).fetchall()

                workflow_events: list[dict[str, Any]] = [
                    {
                        "workflow_uuid": row[0],
                        "key": row[1],
                        "value": row[2],
                        "serialization": row[3],
                    }
                    for row in event_rows
                ]

                # Export workflow_events_history
                history_rows = c.execute(
                    sa.select(
                        SystemSchema.workflow_events_history.c.workflow_uuid,
                        SystemSchema.workflow_events_history.c.key,
                        SystemSchema.workflow_events_history.c.value,
                        SystemSchema.workflow_events_history.c.function_id,
                        SystemSchema.workflow_events_history.c.serialization,
                    ).where(
                        SystemSchema.workflow_events_history.c.workflow_uuid == wf_id
                    )
                ).fetchall()

                workflow_events_history: list[dict[str, Any]] = [
                    {
                        "workflow_uuid": row[0],
                        "key": row[1],
                        "value": row[2],
                        "function_id": row[3],
                        "serialization": row[4],
                    }
                    for row in history_rows
                ]

                # Export streams
                stream_rows = c.execute(
                    sa.select(
                        SystemSchema.streams.c.workflow_uuid,
                        SystemSchema.streams.c.key,
                        SystemSchema.streams.c.value,
                        SystemSchema.streams.c.offset,
                        SystemSchema.streams.c.function_id,
                        SystemSchema.streams.c.serialization,
                    ).where(SystemSchema.streams.c.workflow_uuid == wf_id)
                ).fetchall()

                streams: list[dict[str, Any]] = [
                    {
                        "workflow_uuid": row[0],
                        "key": row[1],
                        "value": row[2],
                        "offset": row[3],
                        "function_id": row[4],
                        "serialization": row[5],
                    }
                    for row in stream_rows
                ]

                exported_workflows.append(
                    ExportedWorkflow(
                        workflow_status=workflow_status,
                        operation_outputs=operation_outputs,
                        workflow_events=workflow_events,
                        workflow_events_history=workflow_events_history,
                        streams=streams,
                    )
                )

        return exported_workflows

    def import_workflow(self, workflows: list[ExportedWorkflow]) -> None:
        """
        Import workflows from an exported format.

        Args:
            workflows: The list of exported workflow data to import
        """
        with self.engine.begin() as c:
            for workflow in workflows:
                status = workflow["workflow_status"]

                # Import workflow_status
                c.execute(
                    sa.insert(SystemSchema.workflow_status).values(
                        workflow_uuid=status["workflow_uuid"],
                        status=status["status"],
                        name=status["name"],
                        authenticated_user=status["authenticated_user"],
                        assumed_role=status["assumed_role"],
                        authenticated_roles=status["authenticated_roles"],
                        output=status["output"],
                        error=status["error"],
                        executor_id=status["executor_id"],
                        created_at=status["created_at"],
                        updated_at=status["updated_at"],
                        application_version=status["application_version"],
                        application_id=status["application_id"],
                        class_name=status["class_name"],
                        config_name=status["config_name"],
                        recovery_attempts=status["recovery_attempts"],
                        queue_name=status["queue_name"],
                        workflow_timeout_ms=status["workflow_timeout_ms"],
                        workflow_deadline_epoch_ms=status["workflow_deadline_epoch_ms"],
                        started_at_epoch_ms=status["started_at_epoch_ms"],
                        deduplication_id=status["deduplication_id"],
                        inputs=status["inputs"],
                        priority=status["priority"],
                        queue_partition_key=status["queue_partition_key"],
                        forked_from=status["forked_from"],
                        parent_workflow_id=status.get("parent_workflow_id"),
                        serialization=status.get("serialization"),
                        delay_until_epoch_ms=status.get("delay_until_epoch_ms"),
                        # NOT NULL columns: fall back to False for payloads
                        # exported before these fields were included.
                        was_forked_from=status.get("was_forked_from", False),
                        rate_limited=status.get("rate_limited", False),
                        completed_at=status.get("completed_at"),
                        attributes=status.get("attributes"),
                        schedule_name=status.get("schedule_name"),
                        debounce_deadline_epoch_ms=status.get(
                            "debounce_deadline_epoch_ms"
                        ),
                        is_debounced=status.get("is_debounced", False),
                        application_name=status.get("application_name"),
                    )
                )

                # Import operation_outputs
                for output in workflow["operation_outputs"]:
                    c.execute(
                        sa.insert(SystemSchema.operation_outputs).values(
                            workflow_uuid=output["workflow_uuid"],
                            function_id=output["function_id"],
                            function_name=output["function_name"],
                            output=output["output"],
                            error=output["error"],
                            child_workflow_id=output["child_workflow_id"],
                            started_at_epoch_ms=output["started_at_epoch_ms"],
                            completed_at_epoch_ms=output["completed_at_epoch_ms"],
                            serialization=output["serialization"],
                            application_name=output.get("application_name"),
                        )
                    )

                # Import workflow_events
                for event in workflow["workflow_events"]:
                    c.execute(
                        sa.insert(SystemSchema.workflow_events).values(
                            workflow_uuid=event["workflow_uuid"],
                            key=event["key"],
                            value=event["value"],
                            serialization=event["serialization"],
                        )
                    )

                # Import workflow_events_history
                for history in workflow["workflow_events_history"]:
                    c.execute(
                        sa.insert(SystemSchema.workflow_events_history).values(
                            workflow_uuid=history["workflow_uuid"],
                            key=history["key"],
                            value=history["value"],
                            function_id=history["function_id"],
                            serialization=history["serialization"],
                        )
                    )

                # Import streams
                for stream in workflow["streams"]:
                    c.execute(
                        sa.insert(SystemSchema.streams).values(
                            workflow_uuid=stream["workflow_uuid"],
                            key=stream["key"],
                            value=stream["value"],
                            offset=stream["offset"],
                            function_id=stream["function_id"],
                            serialization=stream["serialization"],
                        )
                    )

    # ── Schedule CRUD ─────────────────────────────────────────────

    def create_schedule(
        self, schedule: WorkflowSchedule, conn: Optional[sa.Connection] = None
    ) -> None:
        def _do(c: sa.Connection) -> None:
            owner = self._resolve_row_owner(
                c,
                SystemSchema.workflow_schedules,
                SystemSchema.workflow_schedules.c.schedule_name,
                schedule["schedule_name"],
                schedule.get("application_name"),
                "Schedule",
            )
            try:
                c.execute(
                    sa.insert(SystemSchema.workflow_schedules).values(
                        schedule_id=schedule["schedule_id"],
                        schedule_name=schedule["schedule_name"],
                        workflow_name=schedule["workflow_name"],
                        workflow_class_name=schedule["workflow_class_name"],
                        schedule=schedule["schedule"],
                        status=schedule["status"],
                        context=schedule["context"],
                        last_fired_at=schedule.get("last_fired_at"),
                        automatic_backfill=schedule.get("automatic_backfill", False),
                        cron_timezone=schedule.get("cron_timezone"),
                        queue_name=schedule.get("queue_name"),
                        application_name=owner,
                    )
                )
            except sa.exc.IntegrityError:
                raise DBOSException(
                    f"Schedule '{schedule['schedule_name']}' already exists"
                )

        if conn is not None:
            _do(conn)
        else:
            with self.engine.begin() as c:
                _do(c)

    def upsert_schedule(
        self, schedule: WorkflowSchedule, conn: Optional[sa.Connection] = None
    ) -> None:
        # Idempotent upsert by schedule_name; preserves schedule_id, status, and last_fired_at on conflict. The scheduler loop detects the changed definition and restarts the thread.
        def _do(c: sa.Connection) -> None:
            owner = self._resolve_row_owner(
                c,
                SystemSchema.workflow_schedules,
                SystemSchema.workflow_schedules.c.schedule_name,
                schedule["schedule_name"],
                schedule.get("application_name"),
                "Schedule",
            )
            c.execute(
                self.dialect.insert(SystemSchema.workflow_schedules)
                .values(
                    schedule_id=schedule["schedule_id"],
                    schedule_name=schedule["schedule_name"],
                    workflow_name=schedule["workflow_name"],
                    workflow_class_name=schedule["workflow_class_name"],
                    schedule=schedule["schedule"],
                    status=schedule["status"],
                    context=schedule["context"],
                    last_fired_at=schedule.get("last_fired_at"),
                    automatic_backfill=schedule.get("automatic_backfill", False),
                    cron_timezone=schedule.get("cron_timezone"),
                    queue_name=schedule.get("queue_name"),
                    application_name=owner,
                )
                .on_conflict_do_update(
                    index_elements=["schedule_name"],
                    set_={
                        "workflow_name": schedule["workflow_name"],
                        "workflow_class_name": schedule["workflow_class_name"],
                        "schedule": schedule["schedule"],
                        "context": schedule["context"],
                        "automatic_backfill": schedule.get("automatic_backfill", False),
                        "cron_timezone": schedule.get("cron_timezone"),
                        "queue_name": schedule.get("queue_name"),
                        # Claim only an unclaimed row, so a registration landing between the check above and this write keeps the name it took.
                        "application_name": sa.func.coalesce(
                            SystemSchema.workflow_schedules.c.application_name, owner
                        ),
                    },
                )
            )
            # Read back, since the guard above is silent about why it declined to claim.
            self._resolve_row_owner(
                c,
                SystemSchema.workflow_schedules,
                SystemSchema.workflow_schedules.c.schedule_name,
                schedule["schedule_name"],
                schedule.get("application_name"),
                "Schedule",
            )

        if conn is not None:
            _do(conn)
        else:
            with self.engine.begin() as c:
                _do(c)

    def list_schedules(
        self,
        *,
        status: Optional[Union[str, List[str]]] = None,
        workflow_name: Optional[Union[str, List[str]]] = None,
        schedule_name_prefix: Optional[Union[str, List[str]]] = None,
        application_name: Optional[Union[str, List[str]]] = None,
        conn: Optional[sa.Connection] = None,
    ) -> List[WorkflowSchedule]:
        """List only schedules owned by these applications, plus unclaimed ones.
        By default, only list this application's schedules."""
        return self._list_schedules(
            self._observability_filter(
                SystemSchema.workflow_schedules.c.application_name, application_name
            ),
            status=status,
            workflow_name=workflow_name,
            schedule_name_prefix=schedule_name_prefix,
            conn=conn,
        )

    def _list_schedules(
        self,
        scope: sa.ColumnElement[bool],
        *,
        status: Optional[Union[str, List[str]]] = None,
        workflow_name: Optional[Union[str, List[str]]] = None,
        schedule_name_prefix: Optional[Union[str, List[str]]] = None,
        conn: Optional[sa.Connection] = None,
    ) -> List[WorkflowSchedule]:
        def _do(c: sa.Connection) -> List[WorkflowSchedule]:
            query = sa.select(
                SystemSchema.workflow_schedules.c.schedule_id,
                SystemSchema.workflow_schedules.c.schedule_name,
                SystemSchema.workflow_schedules.c.workflow_name,
                SystemSchema.workflow_schedules.c.workflow_class_name,
                SystemSchema.workflow_schedules.c.schedule,
                SystemSchema.workflow_schedules.c.status,
                SystemSchema.workflow_schedules.c.context,
                SystemSchema.workflow_schedules.c.last_fired_at,
                SystemSchema.workflow_schedules.c.automatic_backfill,
                SystemSchema.workflow_schedules.c.cron_timezone,
                SystemSchema.workflow_schedules.c.queue_name,
                SystemSchema.workflow_schedules.c.application_name,
            ).where(scope)
            if status is not None:
                vals = [status] if isinstance(status, str) else status
                query = query.where(SystemSchema.workflow_schedules.c.status.in_(vals))
            if workflow_name is not None:
                vals = (
                    [workflow_name] if isinstance(workflow_name, str) else workflow_name
                )
                query = query.where(
                    SystemSchema.workflow_schedules.c.workflow_name.in_(vals)
                )
            if schedule_name_prefix is not None:
                prefixes = (
                    [schedule_name_prefix]
                    if isinstance(schedule_name_prefix, str)
                    else schedule_name_prefix
                )
                query = query.where(
                    sa.or_(
                        *(
                            SystemSchema.workflow_schedules.c.schedule_name.startswith(
                                p
                            )
                            for p in prefixes
                        )
                    )
                )
            rows = c.execute(query).fetchall()
            return [
                WorkflowSchedule(
                    schedule_id=row[0],
                    schedule_name=row[1],
                    workflow_name=row[2],
                    workflow_class_name=row[3],
                    schedule=row[4],
                    status=row[5],
                    context=row[6],
                    last_fired_at=row[7],
                    automatic_backfill=bool(row[8]),
                    cron_timezone=row[9],
                    queue_name=row[10],
                    application_name=row[11],
                )
                for row in rows
            ]

        if conn is not None:
            return _do(conn)
        with self.engine.begin() as c:
            return _do(c)

    def get_schedule(
        self, name: str, conn: Optional[sa.Connection] = None
    ) -> Optional[WorkflowSchedule]:
        def _do(c: sa.Connection) -> Optional[WorkflowSchedule]:
            row = c.execute(
                sa.select(
                    SystemSchema.workflow_schedules.c.schedule_id,
                    SystemSchema.workflow_schedules.c.schedule_name,
                    SystemSchema.workflow_schedules.c.workflow_name,
                    SystemSchema.workflow_schedules.c.workflow_class_name,
                    SystemSchema.workflow_schedules.c.schedule,
                    SystemSchema.workflow_schedules.c.status,
                    SystemSchema.workflow_schedules.c.context,
                    SystemSchema.workflow_schedules.c.last_fired_at,
                    SystemSchema.workflow_schedules.c.automatic_backfill,
                    SystemSchema.workflow_schedules.c.cron_timezone,
                    SystemSchema.workflow_schedules.c.queue_name,
                    SystemSchema.workflow_schedules.c.application_name,
                ).where(SystemSchema.workflow_schedules.c.schedule_name == name)
            ).fetchone()
            if row is None:
                return None
            return WorkflowSchedule(
                schedule_id=row[0],
                schedule_name=row[1],
                workflow_name=row[2],
                workflow_class_name=row[3],
                schedule=row[4],
                status=row[5],
                context=row[6],
                last_fired_at=row[7],
                automatic_backfill=bool(row[8]),
                cron_timezone=row[9],
                queue_name=row[10],
                application_name=row[11],
            )

        if conn is not None:
            return _do(conn)
        with self.engine.begin() as c:
            return _do(c)

    def _set_schedule_status(
        self, name: str, status: str, conn: Optional[sa.Connection] = None
    ) -> None:
        def _do(c: sa.Connection) -> None:
            c.execute(
                sa.update(SystemSchema.workflow_schedules)
                .where(SystemSchema.workflow_schedules.c.schedule_name == name)
                .values(status=status)
            )

        if conn is not None:
            _do(conn)
        else:
            with self.engine.begin() as c:
                _do(c)

    def pause_schedule(self, name: str, conn: Optional[sa.Connection] = None) -> None:
        self._set_schedule_status(name, "PAUSED", conn)

    def resume_schedule(self, name: str, conn: Optional[sa.Connection] = None) -> None:
        self._set_schedule_status(name, "ACTIVE", conn)

    def update_last_fired_at(self, name: str, last_fired_at: str) -> None:
        with self.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_schedules)
                .where(SystemSchema.workflow_schedules.c.schedule_name == name)
                .values(last_fired_at=last_fired_at)
            )

    def delete_schedule(self, name: str, conn: Optional[sa.Connection] = None) -> None:
        def _do(c: sa.Connection) -> None:
            c.execute(
                sa.delete(SystemSchema.workflow_schedules).where(
                    SystemSchema.workflow_schedules.c.schedule_name == name
                )
            )

        if conn is not None:
            _do(conn)
        else:
            with self.engine.begin() as c:
                _do(c)

    # ── Application Version CRUD ────────────────────────────────

    def create_application_version(
        self, version_name: str, application_name: Optional[str] = None
    ) -> None:
        """Register this version, claiming the row if nobody owns it yet so a pinned version
        does not stay unclaimed. A peer's name is a collision, which is why this raises.
        """
        owner = application_name if application_name is not None else self.app_name
        av = SystemSchema.application_versions
        with self.engine.begin() as c:
            # Claim a pre-upgrade row in place, so the version is not recreated or retimed.
            claimed = c.execute(
                sa.update(av)
                .where(av.c.version_name == version_name)
                .where(av.c.application_name.is_(None))
                .values(application_name=owner)
            ).rowcount
            if not claimed:
                # Targetless DO NOTHING: names no arbiter, so it survives version_name's uniqueness being dropped while still absorbing a concurrent registrar.
                c.execute(
                    self.dialect.insert(av)
                    .values(
                        version_id=generate_uuid(),
                        version_name=version_name,
                        application_name=owner,
                    )
                    .on_conflict_do_nothing()
                )
            # Read back, since the writes above are silent about why they declined to claim.
            self._resolve_row_owner(
                c, av, av.c.version_name, version_name, owner, "Application version"
            )

    def update_application_version_timestamp(
        self,
        version_name: str,
        new_timestamp: int,
        application_name: Optional[str] = None,
    ) -> None:
        """Promote a version to latest. Promoting a peer's is a collision, not a retiming;
        promotion claims an unclaimed row, which would otherwise be every peer's latest.
        """
        owner = application_name if application_name is not None else self.app_name
        av = SystemSchema.application_versions
        with self.engine.begin() as c:
            resolved = self._resolve_row_owner(
                c,
                av,
                av.c.version_name,
                version_name,
                owner,
                "Application version",
            )
            # Scoped to the row this writer resolved to: once version_name is no longer globally unique, a bare name match would retime every peer's version.
            scope: sa.ColumnElement[bool] = av.c.application_name.is_(None)
            if resolved is not None:
                scope = sa.or_(av.c.application_name == resolved, scope)
            c.execute(
                sa.update(av)
                .where(av.c.version_name == version_name)
                .where(scope)
                .values(version_timestamp=new_timestamp, application_name=resolved)
            )

    def list_application_versions(self) -> List[VersionInfo]:
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(
                    SystemSchema.application_versions.c.version_id,
                    SystemSchema.application_versions.c.version_name,
                    SystemSchema.application_versions.c.version_timestamp,
                    SystemSchema.application_versions.c.created_at,
                    SystemSchema.application_versions.c.application_name,
                )
                .where(
                    self._name_filter(
                        SystemSchema.application_versions.c.application_name,
                        self.app_name,
                    )
                )
                .order_by(SystemSchema.application_versions.c.version_timestamp.desc())
            ).fetchall()
            return [
                VersionInfo(
                    version_id=row[0],
                    version_name=row[1],
                    version_timestamp=row[2],
                    created_at=row[3],
                    application_name=row[4],
                )
                for row in rows
            ]

    # ── Queue Registration ──────────────────────────────────────

    def get_queue(
        self,
        name: str,
        *,
        client_system_database: Optional["SystemDatabase"] = None,
    ) -> Optional["Queue"]:
        with self.engine.begin() as c:
            row = c.execute(
                sa.select(SystemSchema.queues).where(SystemSchema.queues.c.name == name)
            ).fetchone()
            if row is None:
                return None
            return queue_from_db_row(row, client_system_database=client_system_database)

    def list_queues(
        self,
        *,
        application_name: Optional[Union[str, List[str]]] = None,
        client_system_database: Optional["SystemDatabase"] = None,
    ) -> List["Queue"]:
        """List only queues owned by these applications, plus unclaimed ones.
        By default, only list this application's queues."""
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(SystemSchema.queues).where(
                    self._observability_filter(
                        SystemSchema.queues.c.application_name, application_name
                    )
                )
            ).fetchall()
            return [
                queue_from_db_row(row, client_system_database=client_system_database)
                for row in rows
            ]

    def delete_queue(self, name: str) -> None:
        """Delete a database-backed queue's row, if it exists."""
        with self.engine.begin() as c:
            c.execute(
                sa.delete(SystemSchema.queues).where(SystemSchema.queues.c.name == name)
            )

    def update_queue(self, name: str, fields: Dict[str, Any]) -> None:
        """Apply a partial update to a database-backed queue's row."""
        if not fields:
            return
        values = dict(fields)
        values["updated_at"] = int(time.time() * 1000)
        with self.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.queues)
                .where(SystemSchema.queues.c.name == name)
                .values(**values)
            )

    def upsert_queue(
        self,
        *,
        name: str,
        concurrency: Optional[int],
        worker_concurrency: Optional[int],
        rate_limit_max: Optional[int],
        rate_limit_period_sec: Optional[float],
        priority_enabled: bool,
        partition_queue: bool,
        polling_interval_sec: float,
        update_existing: bool,
        application_name: Optional[str] = None,
    ) -> bool:
        """Upsert a queue row. Returns True iff a new row was inserted (i.e.
        the queue did not previously exist). False if the row already existed,
        regardless of whether it was updated."""
        owner = application_name if application_name is not None else self.app_name
        values = {
            "name": name,
            "concurrency": concurrency,
            "worker_concurrency": worker_concurrency,
            "rate_limit_max": rate_limit_max,
            "rate_limit_period_sec": rate_limit_period_sec,
            "priority_enabled": priority_enabled,
            "partition_queue": partition_queue,
            "polling_interval_sec": polling_interval_sec,
            "updated_at": int(time.time() * 1000),
            "application_name": owner,
        }
        with self.engine.begin() as c:
            existed = (
                c.execute(
                    sa.select(SystemSchema.queues.c.name).where(
                        SystemSchema.queues.c.name == name
                    )
                ).fetchone()
                is not None
            )
            # A name collision is a conflict in every mode: the name is the queue's address.
            values["application_name"] = self._resolve_row_owner(
                c,
                SystemSchema.queues,
                SystemSchema.queues.c.name,
                name,
                owner,
                "Queue",
            )
            stmt = self.dialect.insert(SystemSchema.queues).values(**values)
            if update_existing:
                update_set: Dict[str, Any] = {
                    k: v for k, v in values.items() if k != "name"
                }
                # Claim only an unclaimed row, so a registration landing between the check above and this write keeps the name it just took.
                update_set["application_name"] = sa.func.coalesce(
                    SystemSchema.queues.c.application_name, values["application_name"]
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["name"],
                    set_=update_set,
                )
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=["name"])
            c.execute(stmt)
            # Read back, since the guard above is silent about why it declined to claim.
            self._resolve_row_owner(
                c,
                SystemSchema.queues,
                SystemSchema.queues.c.name,
                name,
                owner,
                "Queue",
            )
            return not existed

    def get_latest_application_version(
        self, application_name: Optional[str] = None
    ) -> VersionInfo:
        """Latest version registered by an application. Defaults to this handle's, so
        a caller acting for another one — firing its schedule — must name it."""
        owner = application_name if application_name is not None else self.app_name
        with self.engine.begin() as c:
            row = c.execute(
                sa.select(
                    SystemSchema.application_versions.c.version_id,
                    SystemSchema.application_versions.c.version_name,
                    SystemSchema.application_versions.c.version_timestamp,
                    SystemSchema.application_versions.c.created_at,
                    SystemSchema.application_versions.c.application_name,
                )
                .where(
                    self._name_filter(
                        SystemSchema.application_versions.c.application_name,
                        owner,
                    )
                )
                .order_by(SystemSchema.application_versions.c.version_timestamp.desc())
                .limit(1)
            ).fetchone()
            if row is None:
                raise DBOSException("No application versions found")
            return VersionInfo(
                version_id=row[0],
                version_name=row[1],
                version_timestamp=row[2],
                created_at=row[3],
                application_name=row[4],
            )

    # ── Application Rename ──────────────────────────────────────

    # Rows a rename must move atomically: a half-owned application dequeues work whose version row it can no longer see.
    _RENAME_ATOMIC_STATUSES = [
        WorkflowStatusString.PENDING.value,
        WorkflowStatusString.ENQUEUED.value,
        WorkflowStatusString.DELAYED.value,
    ]

    @staticmethod
    def _rename_source(
        col: sa.ColumnElement[Any],
        old_name: Optional[str],
        adopt_unclaimed_rows: bool,
    ) -> sa.ColumnElement[bool]:
        """Rows a rename moves: an application's own, unclaimed ones, or both. Unlike
        _name_filter, unclaimed rows are not implied; they move only when asked."""
        clauses = []
        if old_name is not None:
            clauses.append(col == old_name)
        if adopt_unclaimed_rows:
            clauses.append(col.is_(None))
        # Callers validate that at least one source is named.
        return sa.or_(*clauses)

    def _rename_rows_in_batches(
        self,
        table: sa.Table,
        key_col: sa.ColumnElement[Any],
        old_name: Optional[str],
        new_name: str,
        batch_size: Optional[int],
        adopt_unclaimed_rows: bool,
    ) -> int:
        """Re-own a table's rows in half-open key ranges, so a long history neither
        moves in one transaction nor rescans what it already moved; a re-run resumes."""
        predicate = self._rename_source(
            table.c.application_name, old_name, adopt_unclaimed_rows
        )
        if batch_size is None:
            with self.engine.begin() as c:
                return c.execute(
                    sa.update(table).where(predicate).values(application_name=new_name)
                ).rowcount
        total = 0
        # Ranges, not LIMIT: a LIMIT repages every row already moved, and an IN list of keys plans as a whole-table hash join.
        watermark: Optional[Any] = None
        while True:
            scope = (
                predicate
                if watermark is None
                else sa.and_(predicate, key_col > watermark)
            )
            with self.engine.begin() as c:
                # The batch_size-th matching key bounds this range; distinct, so a key's rows are never split across batches.
                upper = c.execute(
                    sa.select(key_col)
                    .where(scope)
                    .distinct()
                    .order_by(key_col)
                    .limit(1)
                    .offset(batch_size - 1)
                ).scalar()
                # The final batch drops the watermark, so rows that appeared below it still move.
                batch = predicate if upper is None else sa.and_(scope, key_col <= upper)
                total += c.execute(
                    sa.update(table).where(batch).values(application_name=new_name)
                ).rowcount
            # Fewer than a full batch remained, so that update took the rest.
            if upper is None:
                return total
            watermark = upper

    def rename_application(
        self,
        old_name: Optional[str],
        new_name: str,
        *,
        batch_size: Optional[int] = DEFAULT_RENAME_BATCH_SIZE,
        adopt_unclaimed_rows: bool = False,
    ) -> ApplicationRowCounts:
        """Give ``new_name`` ownership of rows ``old_name`` holds, of unclaimed rows, or
        of both. The renamed application must be stopped, or its dequeues race this."""
        from ._dbos_config import _is_valid_app_name

        if old_name is not None and not old_name:
            raise DBOSException("The application's previous name cannot be empty.")
        if old_name is None and not adopt_unclaimed_rows:
            raise DBOSException(
                "Nothing to re-own: name the application to rename, adopt unclaimed "
                "rows, or both."
            )
        if not _is_valid_app_name(new_name):
            raise DBOSException(
                f"Invalid application name '{new_name}'. Application names must be "
                "between 3 and 30 characters long and contain only lowercase letters, "
                "numbers, dashes, and underscores."
            )
        if old_name == new_name:
            raise DBOSException(
                f"Application '{new_name}' already holds that name; nothing to rename."
            )
        if batch_size is not None and batch_size < 1:
            raise ValueError(f"batch_size must be a positive integer, got {batch_size}")

        ws = SystemSchema.workflow_status
        # Never a merge: queue, schedule, and version names are globally unique whatever their owner, so this cannot collide.
        with self.engine.begin() as c:

            def move(table: sa.Table, *extra: sa.ColumnElement[bool]) -> int:
                return c.execute(
                    sa.update(table)
                    .where(
                        self._rename_source(
                            table.c.application_name, old_name, adopt_unclaimed_rows
                        ),
                        *extra,
                    )
                    .values(application_name=new_name)
                ).rowcount

            queues = move(SystemSchema.queues)
            schedules = move(SystemSchema.workflow_schedules)
            versions = move(SystemSchema.application_versions)
            in_flight = move(ws, ws.c.status.in_(self._RENAME_ATOMIC_STATUSES))

        # Only terminal rows are left to match, and they scope observability and GC alone, so they may lag behind the commit above.
        terminal = self._rename_rows_in_batches(
            ws, ws.c.workflow_uuid, old_name, new_name, batch_size, adopt_unclaimed_rows
        )
        oo = SystemSchema.operation_outputs
        steps = self._rename_rows_in_batches(
            oo, oo.c.workflow_uuid, old_name, new_name, batch_size, adopt_unclaimed_rows
        )
        return ApplicationRowCounts(
            queues=queues,
            schedules=schedules,
            versions=versions,
            workflows=in_flight + terminal,
            steps=steps,
        )

    @db_retry()
    def call_txn_as_step(
        self,
        workflow_uuid: str,
        function_id: int,
        function_name: str,
        op: Callable[[sa.Connection], T],
    ) -> T:
        start_time = int(time.time() * 1000)
        with self.engine.begin() as c:
            recorded = self._check_operation_execution_txn(
                workflow_uuid, function_id, function_name, conn=c
            )
            if recorded is not None:
                assert recorded["output"] is not None
                recorded_output: SystemDatabase.T = self.serializer.deserialize(
                    recorded["output"]
                )
                return recorded_output
            result = op(c)
            output: OperationResultInternal = {
                "workflow_uuid": workflow_uuid,
                "function_id": function_id,
                "function_name": function_name,
                "started_at_epoch_ms": start_time,
                "output": (self.serializer.serialize(result)),
                "serialization": None,
                "error": None,
            }
            self._record_operation_result_txn(output, int(time.time() * 1000), conn=c)
        DebugTriggers.debug_trigger_point(DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT)
        return result
