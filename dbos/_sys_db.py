import asyncio
import datetime
import functools
import json
import random
import sys
import threading
import time
from abc import ABC, abstractmethod
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
    Sequence,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import func

from dbos._debug_trigger import DebugTriggers
from dbos._utils import (
    INTERNAL_QUEUE_NAME,
    generate_uuid,
    retriable_postgres_exception,
    retriable_sqlite_exception,
)

from ._context import DBOSContext, get_local_dbos_context
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
    MaxRecoveryAttemptsExceededError,
)
from ._logger import dbos_logger
from ._outcome import NoResult
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
    from ._queue import Queue


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
    def __init__(self, *, workflow_id: str, queue_name: Optional[str] = None):
        self.workflow_id: str = workflow_id
        self.queue_name: Optional[str] = queue_name


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


class ClientScheduleInput(TypedDict, total=False):
    schedule_name: str
    workflow_name: str
    workflow_class_name: Optional[str]
    schedule: str
    context: Any
    automatic_backfill: bool
    cron_timezone: Optional[str]
    queue_name: Optional[str]


class VersionInfo(TypedDict):
    version_id: str
    version_name: str
    version_timestamp: int
    created_at: int


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
    count: int


class NotificationInfo(TypedDict):
    topic: Optional[str]
    message: Any
    created_at_epoch_ms: int
    consumed: bool


_dbos_null_topic = "__null__topic__"
_dbos_stream_closed_sentinel = "__DBOS_STREAM_CLOSED__"


class EventCount(TypedDict):
    event: threading.Event
    count: int


class ThreadSafeEventDict:
    def __init__(self) -> None:
        self._dict: Dict[str, EventCount] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[threading.Event]:
        with self._lock:
            if key not in self._dict:
                return None
            return self._dict[key]["event"]

    def set(self, key: str, value: threading.Event) -> tuple[bool, threading.Event]:
        with self._lock:
            if key in self._dict:
                # Key already exists, do not overwrite. Increment the wait count.
                ec = self._dict[key]
                ec["count"] += 1
                return False, ec["event"]
            self._dict[key] = EventCount(event=value, count=1)
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


F = TypeVar("F", bound=Callable[..., Any])


def db_retry(
    initial_backoff: float = 1.0, max_backoff: float = 60.0
) -> Callable[[F], F]:
    """
    If a workflow encounters a database connection issue while performing an operation,
    block the workflow and retry the operation until it reconnects and succeeds.

    In other words, if DBOS loses its database connection, everything pauses until the connection is recovered,
    trading off availability for correctness.
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            retries: int = 0
            backoff: float = initial_backoff
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:

                    # Determine if this is a retriable exception
                    if not retriable_postgres_exception(
                        e
                    ) and not retriable_sqlite_exception(e):
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
    ):
        import sqlalchemy.dialects.postgresql as pg
        import sqlalchemy.dialects.sqlite as sq

        # Log system database connection information
        if engine:
            dbos_logger.info("Initializing DBOS system database with custom engine")
        else:
            printable_sys_db_url = sa.make_url(system_database_url).render_as_string(
                hide_password=True
            )
            dbos_logger.info(
                f"Initializing DBOS system database with URL: {printable_sys_db_url}"
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

        if system_database_url.startswith("sqlite"):
            self.schema = None
        else:
            self.schema = schema if schema else "dbos"
        SystemSchema.set_schema(self.schema)

        if engine:
            self.engine = engine
            self.created_engine = False
        else:
            self.engine = self._create_engine(system_database_url, engine_kwargs)
            self.created_engine = True
        self._engine_kwargs = engine_kwargs

        self.notifications_map = ThreadSafeEventDict()
        self.workflow_events_map = ThreadSafeEventDict()
        self.executor_id = executor_id
        self._notification_listener_polling_interval_sec = (
            notification_listener_polling_interval_sec
        )

        self._listener_thread_lock = threading.Lock()

        # Now we can run background processes
        self._run_background_processes = True

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
        self.engine.dispose()

    @abstractmethod
    def _cleanup_connections(self) -> None:
        """Clean up database-specific connections."""
        pass

    def _insert_workflow_status(
        self,
        status: WorkflowStatusInternal,
        conn: sa.Connection,
        *,
        max_recovery_attempts: Optional[int],
        owner_xid: Optional[str],
        is_recovery_request: Optional[bool],
        is_dequeued_request: Optional[bool],
    ) -> tuple[WorkflowStatuses, Optional[int], bool]:
        """Insert or update workflow status using PostgreSQL upsert operations."""
        wf_status: WorkflowStatuses = status["status"]
        workflow_deadline_epoch_ms: Optional[int] = status["workflow_deadline_epoch_ms"]
        force_execute = is_recovery_request or is_dequeued_request
        should_execute = True
        _enqueued_statuses = [
            WorkflowStatusString.ENQUEUED.value,
            WorkflowStatusString.DELAYED.value,
        ]

        # Values to update when a row already exists for this workflow
        update_values: dict[str, Any] = {
            "recovery_attempts": sa.case(
                (
                    SystemSchema.workflow_status.c.status.notin_(_enqueued_statuses),
                    SystemSchema.workflow_status.c.recovery_attempts
                    + (1 if force_execute else 0),
                ),
                else_=SystemSchema.workflow_status.c.recovery_attempts,
            ),
            "updated_at": sa.func.extract("epoch", sa.func.now()) * 1000,
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
            )
            .on_conflict_do_update(
                index_elements=["workflow_uuid"],
                set_=update_values,
            )
        )

        cmd = cmd.returning(
            SystemSchema.workflow_status.c.recovery_attempts,
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
            # Check the started workflow matches the expected name, class_name, config_name, and queue_name
            # A mismatch indicates a workflow starting with the same UUID but different functions, which would throw an exception.
            recovery_attempts: int = row[0]
            wf_status = row[1]
            workflow_deadline_epoch_ms = row[2]
            err_msg: Optional[str] = None
            if row[3] != status["name"]:
                err_msg = f"Workflow already exists with a different function name: {row[3]}, but the provided function name is: {status['name']}"
            elif row[4] != status["class_name"]:
                err_msg = f"Workflow already exists with a different class name: {row[4]}, but the provided class name is: {status['class_name']}"
            elif row[5] != status["config_name"]:
                err_msg = f"Workflow already exists with a different config name: {row[5]}, but the provided config name is: {status['config_name']}"
            elif row[6] != status["queue_name"]:
                # This is a warning because a different queue name is not necessarily an error.
                dbos_logger.warning(
                    f"Workflow already exists in queue: {row[6]}, but the provided queue name is: {status['queue_name']}. The queue is not updated."
                )
            if err_msg is not None:
                raise DBOSConflictingWorkflowError(status["workflow_uuid"], err_msg)

            # Every time we start executing a workflow (and thus attempt to insert its status), we increment `recovery_attempts` by 1.
            # When this number becomes equal to `maxRetries + 1`, we mark the workflow as `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.
            if (
                (wf_status != "SUCCESS" and wf_status != "ERROR")
                and max_recovery_attempts is not None
                and recovery_attempts > max_recovery_attempts + 1
                and owner_xid != row[7]
            ):
                dlq_cmd = (
                    sa.update(SystemSchema.workflow_status)
                    .where(
                        SystemSchema.workflow_status.c.workflow_uuid
                        == status["workflow_uuid"]
                    )
                    .where(
                        SystemSchema.workflow_status.c.status
                        == WorkflowStatusString.PENDING.value
                    )
                    .values(
                        status=WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value,
                        deduplication_id=None,
                        started_at_epoch_ms=None,
                        queue_name=None,
                    )
                )
                conn.execute(dlq_cmd)
                # Need to commit here because we're throwing an exception
                conn.commit()
                raise MaxRecoveryAttemptsExceededError(
                    status["workflow_uuid"], max_recovery_attempts
                )

            if (
                owner_xid != row[7]
                and not is_dequeued_request
                and not is_recovery_request
            ):
                should_execute = False

            status["serialization"] = row[8]

        return wf_status, workflow_deadline_epoch_ms, should_execute

    @db_retry()
    def update_workflow_outcome(
        self,
        workflow_id: str,
        status: WorkflowStatuses,
        *,
        output: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        with self.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .values(
                    status=status,
                    output=output,
                    error=error,
                    # As the workflow is complete, remove its deduplication ID
                    deduplication_id=None,
                    updated_at=func.extract("epoch", func.now()) * 1000,
                )
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
            )

    def cancel_workflows(
        self,
        workflow_ids: list[str],
    ) -> None:
        with self.engine.begin() as c:
            # Set the workflows' status to CANCELLED and remove them from any queue,
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
                    status=WorkflowStatusString.CANCELLED.value,
                    queue_name=None,
                    deduplication_id=None,
                    started_at_epoch_ms=None,
                    updated_at=func.extract("epoch", func.now()) * 1000,
                )
            )

    def resume_workflows(
        self,
        workflow_ids: list[str],
        *,
        queue_name: Optional[str] = None,
    ) -> None:
        with self.engine.begin() as c:
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
                    updated_at=func.extract("epoch", func.now()) * 1000,
                )
            )

    def set_workflow_delay(
        self,
        workflow_id: str,
        *,
        delay_seconds: Optional[float] = None,
        delay_until_epoch_ms: Optional[int] = None,
    ) -> None:
        """Set or update the delay on a workflow. Only affects DELAYED or ENQUEUED workflows."""
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
                    SystemSchema.workflow_status.c.status.in_(
                        [
                            WorkflowStatusString.DELAYED.value,
                            WorkflowStatusString.ENQUEUED.value,
                        ]
                    )
                )
                .values(
                    status=WorkflowStatusString.DELAYED.value,
                    delay_until_epoch_ms=resolved,
                    updated_at=func.extract("epoch", func.now()) * 1000,
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
                ).where(
                    SystemSchema.workflow_status.c.workflow_uuid.in_(
                        original_workflow_ids
                    )
                )
            ).fetchall()

            status_by_id = {row[0]: row for row in rows}
            for original_workflow_id in original_workflow_ids:
                if original_workflow_id not in status_by_id:
                    raise Exception(f"Workflow {original_workflow_id} not found")
            statuses = [status_by_id[wid] for wid in original_workflow_ids]
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
            for wid in workflow_ids:
                if wid not in start_step_by_id:
                    if from_step_name is not None:
                        raise Exception(
                            f"Workflow {wid} has no step named '{from_step_name}'"
                        )
                    raise Exception(f"Workflow {wid} has no steps")

            start_steps = [start_step_by_id[wid] for wid in workflow_ids]

        forked_ids = [generate_uuid() for _ in workflow_ids]
        return self.fork_workflow(
            workflow_ids,
            forked_ids,
            start_steps,
            application_version=application_version,
            queue_name=queue_name,
            queue_partition_key=queue_partition_key,
        )

    @db_retry()
    def get_workflow_status(
        self, workflow_uuid: str
    ) -> Optional[WorkflowStatusInternal]:
        with self.engine.begin() as c:
            row = c.execute(
                sa.select(
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
                    SystemSchema.workflow_status.c.inputs,
                    SystemSchema.workflow_status.c.queue_partition_key,
                    SystemSchema.workflow_status.c.forked_from,
                    SystemSchema.workflow_status.c.parent_workflow_id,
                    SystemSchema.workflow_status.c.started_at_epoch_ms,
                    SystemSchema.workflow_status.c.serialization,
                    SystemSchema.workflow_status.c.delay_until_epoch_ms,
                ).where(SystemSchema.workflow_status.c.workflow_uuid == workflow_uuid)
            ).fetchone()
            if row is None:
                return None
            status: WorkflowStatusInternal = {
                "workflow_uuid": workflow_uuid,
                "output": None,
                "error": None,
                "status": row[0],
                "name": row[1],
                "recovery_attempts": row[2],
                "config_name": row[3],
                "class_name": row[4],
                "authenticated_user": row[5],
                "authenticated_roles": row[6],
                "assumed_role": row[7],
                "queue_name": row[8],
                "executor_id": row[9],
                "created_at": row[10],
                "updated_at": row[11],
                "app_version": row[12],
                "app_id": row[13],
                "workflow_deadline_epoch_ms": row[14],
                "workflow_timeout_ms": row[15],
                "deduplication_id": row[16],
                "priority": row[17],
                "inputs": row[18],
                "queue_partition_key": row[19],
                "forked_from": row[20],
                "parent_workflow_id": row[21],
                "started_at_epoch_ms": row[22],
                "serialization": row[23],
                "owner_xid": None,
                "delay_until_epoch_ms": row[24],
            }
            return status

    @db_retry()
    def get_deduplicated_workflow(
        self, queue_name: str, deduplication_id: str
    ) -> Optional[str]:
        """
        Get the workflow ID associated with a given queue name and deduplication ID.

        Args:
            queue_name: The name of the queue
            deduplication_id: The deduplication ID

        Returns:
            The workflow UUID if found, None otherwise
        """
        with self.engine.begin() as c:
            row = c.execute(
                sa.select(SystemSchema.workflow_status.c.workflow_uuid).where(
                    SystemSchema.workflow_status.c.queue_name == queue_name,
                    SystemSchema.workflow_status.c.deduplication_id == deduplication_id,
                )
            ).fetchone()

            if row is None:
                return None
            workflow_id: str = row[0]
            return workflow_id

    @db_retry()
    def check_workflow_result(self, workflow_id: str) -> Union[NoResult, Any]:
        """Check if a workflow has completed and return its result.

        Returns NoResult() if the workflow is still pending/enqueued/delayed/not found.
        Returns the deserialized output on success.
        Raises on error, cancellation, or max recovery attempts exceeded.
        """
        with self.engine.begin() as c:
            row = c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.status,
                    SystemSchema.workflow_status.c.output,
                    SystemSchema.workflow_status.c.error,
                    SystemSchema.workflow_status.c.serialization,
                ).where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
            ).fetchone()
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
                elif (
                    status == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value
                ):
                    raise DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded(workflow_id)
        return NoResult()

    def await_workflow_result(self, workflow_id: str, polling_interval: float) -> Any:
        while True:
            result = self.check_workflow_result(workflow_id)
            if not isinstance(result, NoResult):
                return result
            time.sleep(polling_interval)

    async def await_workflow_result_async(
        self, workflow_id: str, polling_interval: float
    ) -> Any:
        while True:
            result = await asyncio.to_thread(self.check_workflow_result, workflow_id)
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
        with self.engine.begin() as c:
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
        if user_list:
            query = query.where(
                SystemSchema.workflow_status.c.authenticated_user.in_(user_list)
            )
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

            idx = 25
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

    def get_pending_workflows(
        self, executor_id: str, app_version: str
    ) -> list[GetPendingWorkflowsOutput]:
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.workflow_uuid,
                    SystemSchema.workflow_status.c.queue_name,
                ).where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.PENDING.value,
                    SystemSchema.workflow_status.c.executor_id == executor_id,
                    SystemSchema.workflow_status.c.application_version == app_version,
                )
            ).fetchall()

            return [
                GetPendingWorkflowsOutput(
                    workflow_id=row.workflow_uuid,
                    queue_name=row.queue_name,
                )
                for row in rows
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
        status: Optional[List[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[List[str]] = None,
        app_version: Optional[List[str]] = None,
        executor_id: Optional[List[str]] = None,
        queue_name: Optional[List[str]] = None,
        workflow_id_prefix: Optional[List[str]] = None,
    ) -> List[WorkflowAggregateRow]:
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
        ]
        group_names = []
        group_columns = []
        for col_name, enabled, col in group_by_flags:
            if enabled:
                group_names.append(col_name)
                group_columns.append(col)

        if not group_columns:
            raise ValueError("At least one group_by flag must be set to True")

        query = sa.select(*group_columns, func.count().label("count"))

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

        query = query.group_by(*group_columns)

        with self.engine.begin() as c:
            rows = c.execute(query).fetchall()

        results: List[WorkflowAggregateRow] = []
        for row in rows:
            group = {group_names[i]: row[i] for i in range(len(group_names))}
            results.append(
                WorkflowAggregateRow(group=group, count=row[len(group_names)])
            )
        return results

    def _record_operation_result_txn(
        self,
        result: OperationResultInternal,
        completed_at_epoch_ms: int,
        conn: sa.Connection,
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
            if len(rows) > 0 and int(rows[0][0]) != completed_at_epoch_ms:
                raise DBOSWorkflowConflictIDError(result["workflow_uuid"])

        except DBAPIError as dbapi_error:
            if self._is_unique_constraint_violation(dbapi_error):
                raise DBOSWorkflowConflictIDError(result["workflow_uuid"])
            raise

    def record_operation_result(self, result: OperationResultInternal) -> None:
        completed_at_epoch_ms = int(time.time() * 1000)

        @db_retry()
        def record_operation_result_retry() -> None:
            with self.engine.begin() as c:
                self._record_operation_result_txn(result, completed_at_epoch_ms, c)
            DebugTriggers.debug_trigger_point(DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT)

        record_operation_result_retry()

    @db_retry()
    def record_get_result(
        self,
        result_workflow_id: str,
        output: Optional[str],
        error: Optional[str],
        serialization: Optional[str],
        ctx: Optional["DBOSContext"] = None,
    ) -> None:
        if ctx is None:
            ctx = get_local_dbos_context()
            # Only record get_result called in workflow functions
            if ctx is None or not ctx.is_workflow():
                return
            ctx.function_id += 1  # Record the get_result as a step
        # Because there's no corresponding check, we do nothing on conflict
        # and do not raise a DBOSWorkflowConflictIDError
        sql = (
            self.dialect.insert(SystemSchema.operation_outputs)
            .values(
                workflow_uuid=ctx.workflow_id,
                function_id=ctx.function_id,
                function_name="DBOS.getResult",
                output=output,
                error=error,
                child_workflow_id=result_workflow_id,
                serialization=serialization,
            )
            .on_conflict_do_nothing()
        )
        with self.engine.begin() as c:
            c.execute(sql)

    @db_retry()
    def record_child_workflow(
        self,
        parentUUID: str,
        childUUID: str,
        functionID: int,
        functionName: str,
    ) -> None:
        sql = sa.insert(SystemSchema.operation_outputs).values(
            workflow_uuid=parentUUID,
            function_id=functionID,
            function_name=functionName,
            child_workflow_id=childUUID,
        )
        try:
            with self.engine.begin() as c:
                c.execute(sql)
        except DBAPIError as dbapi_error:
            if self._is_unique_constraint_violation(dbapi_error):
                raise DBOSWorkflowConflictIDError(parentUUID)
            raise

    @abstractmethod
    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation."""
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
        conn: sa.Connection,
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

    @db_retry()
    def send(
        self,
        workflow_uuid: str,
        function_id: int,
        destination_uuid: str,
        message: Any,
        topic: Optional[str],
        *,
        serialization_type: Optional["WorkflowSerializationFormat"],
        message_uuid: Optional[str],
    ) -> None:
        function_name = "DBOS.send"
        start_time = int(time.time() * 1000)
        topic = topic if topic is not None else _dbos_null_topic
        if message_uuid is None:
            message_uuid = str(generate_uuid())
        serval, serialization = serialize_value(
            message,
            serialization_type,
            self.serializer,
        )
        with self.engine.begin() as c:
            recorded_output = self._check_operation_execution_txn(
                workflow_uuid, function_id, function_name, conn=c
            )
            if recorded_output is not None:
                dbos_logger.debug(
                    f"Replaying send, id: {function_id}, destination_uuid: {destination_uuid}, topic: {topic}"
                )
                return  # Already sent before
            else:
                dbos_logger.debug(
                    f"Running send, id: {function_id}, destination_uuid: {destination_uuid}, topic: {topic}"
                )

            try:
                c.execute(
                    self.dialect.insert(SystemSchema.notifications)
                    .values(
                        destination_uuid=destination_uuid,
                        topic=topic,
                        message=serval,
                        message_uuid=message_uuid,
                        serialization=serialization,
                    )
                    .on_conflict_do_nothing(
                        index_elements=[
                            SystemSchema.notifications.c.message_uuid,
                        ]
                    )
                )
            except DBAPIError as dbapi_error:
                if self._is_foreign_key_violation(dbapi_error):
                    raise DBOSNonExistentWorkflowError(
                        "`send` destination", destination_uuid
                    )
                raise
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

    @db_retry()
    def send_direct(
        self,
        destination_uuid: str,
        message: Any,
        topic: Optional[str] = None,
        message_uuid: Optional[str] = None,
        *,
        serialization_type: Optional["WorkflowSerializationFormat"] = None,
    ) -> None:
        """Send a message without requiring a workflow context.

        Idempotency is provided by the primary key constraint on message_uuid.
        On duplicate message_uuid, silently returns (idempotent replay).
        """

        topic = topic if topic is not None else _dbos_null_topic
        if message_uuid is None:
            message_uuid = str(generate_uuid())
        serval, serialization = serialize_value(
            message,
            serialization_type,
            self.serializer,
        )
        try:
            with self.engine.begin() as c:
                c.execute(
                    self.dialect.insert(SystemSchema.notifications)
                    .values(
                        destination_uuid=destination_uuid,
                        topic=topic,
                        message=serval,
                        message_uuid=message_uuid,
                        serialization=serialization,
                    )
                    .on_conflict_do_nothing(
                        index_elements=[
                            SystemSchema.notifications.c.message_uuid,
                        ]
                    )
                )
        except DBAPIError as dbapi_error:
            if self._is_foreign_key_violation(dbapi_error):
                raise DBOSNonExistentWorkflowError(
                    "`send` destination", destination_uuid
                )
            raise

    @db_retry()
    def recv_setup(
        self,
        workflow_uuid: str,
        function_id: int,
        timeout_function_id: int,
        topic: Optional[str],
        timeout_seconds: float = 60,
    ) -> Union[
        tuple[Literal[True], Any],
        tuple[Literal[False], threading.Event, float, str, int],
    ]:
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
        event = threading.Event()
        success, _ = self.notifications_map.set(payload, event)
        if not success:
            # This should not happen, but if it does, it means the workflow is executed concurrently.
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
        event: threading.Event,
    ) -> None:
        """Poll the database directly for a pending notification and signal the event if found.
        Used as a fallback in case the notification listener thread drops a notification.
        """
        normalized_topic = topic if topic is not None else _dbos_null_topic
        try:
            with self.engine.begin() as c:
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
                event.wait(
                    timeout=min(remaining, self._notification_fallback_polling_interval)
                )
                if not event.is_set():
                    self.recv_check(workflow_uuid, topic, event)
            return self.recv_consume(workflow_uuid, function_id, topic, start_time)
        finally:
            self.notifications_map.pop(payload)

    async def recv_async(
        self,
        workflow_uuid: str,
        function_id: int,
        timeout_function_id: int,
        topic: Optional[str],
        timeout_seconds: float = 60,
    ) -> Any:
        setup = await asyncio.to_thread(
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
            last_poll = time.time()
            while not event.is_set():
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(remaining, 0.1))
                now = time.time()
                if (
                    not event.is_set()
                    and now - last_poll >= self._notification_fallback_polling_interval
                ):
                    last_poll = now
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

        def split_payload(payload: str) -> Tuple[str, Optional[str]]:
            """Split payload into components (first::second format)."""
            if "::" in payload:
                parts = payload.split("::", 1)
                return parts[0], parts[1]
            return payload, None

        def signal_event(event_map: ThreadSafeEventDict, payload: str) -> None:
            """Signal an event if it exists."""
            event = event_map.get(payload)
            if event:
                event.set()
                dbos_logger.debug(f"Signaled event for {payload}")

        while self._run_background_processes:
            try:
                # Poll at the configured interval
                time.sleep(self._notification_listener_polling_interval_sec)

                # Check all payloads in the notifications_map
                for payload in list(self.notifications_map._dict.keys()):
                    dest_uuid, topic = split_payload(payload)
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
                            signal_event(self.notifications_map, payload)

                # Check all payloads in the workflow_events_map
                for payload in list(self.workflow_events_map._dict.keys()):
                    workflow_uuid, key = split_payload(payload)
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
                            signal_event(self.workflow_events_map, payload)

            except Exception as e:
                if self._run_background_processes:
                    dbos_logger.warning(f"Notification poller error: {e}")
                    time.sleep(self._notification_listener_polling_interval_sec)

    @staticmethod
    def reset_system_database(database_url: str) -> None:
        """Reset the system database by calling the appropriate implementation."""
        if database_url.startswith("sqlite"):
            from ._sys_db_sqlite import SQLiteSystemDatabase

            SQLiteSystemDatabase._reset_system_database(database_url)
        else:
            from ._sys_db_postgres import PostgresSystemDatabase

            PostgresSystemDatabase._reset_system_database(database_url)

    @db_retry()
    def record_sleep(
        self,
        workflow_uuid: str,
        function_id: int,
        seconds: float,
    ) -> float:
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
                    }
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
    ) -> Union[
        tuple[Literal[True], Any],
        tuple[Literal[False], threading.Event, float, str, int],
    ]:
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
        event = threading.Event()
        success, existing_event = self.workflow_events_map.set(payload, event)
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
        event: threading.Event,
    ) -> None:
        """Poll the database directly for a workflow event and signal the event if found.
        Used as a fallback in case the notification listener thread drops a notification.
        """
        try:
            with self.engine.begin() as c:
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
                event.wait(
                    timeout=min(remaining, self._notification_fallback_polling_interval)
                )
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
        setup = await asyncio.to_thread(
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
            last_poll = time.time()
            while not event.is_set():
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(remaining, 0.1))
                now = time.time()
                if (
                    not event.is_set()
                    and now - last_poll >= self._notification_fallback_polling_interval
                ):
                    last_poll = now
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
        with self.engine.begin() as c:
            query = (
                sa.select(SystemSchema.workflow_status.c.queue_partition_key)
                .distinct()
                .where(SystemSchema.workflow_status.c.queue_name == queue_name)
                .where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.ENQUEUED.value
                )
                .where(SystemSchema.workflow_status.c.queue_partition_key.isnot(None))
            )

            rows = c.execute(query).fetchall()
            return [row[0] for row in rows]

    def transition_delayed_workflows(self) -> None:
        """Transition DELAYED workflows whose delay has expired to ENQUEUED."""
        now_ms = int(time.time() * 1000)
        with self.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.DELAYED.value
                )
                .where(SystemSchema.workflow_status.c.delay_until_epoch_ms <= now_ms)
                .values(status=WorkflowStatusString.ENQUEUED.value)
            )

    def start_queued_workflows(
        self,
        queue: "Queue",
        executor_id: str,
        app_version: str,
        queue_partition_key: Optional[str],
    ) -> List[str]:
        start_time_ms = int(time.time() * 1000)
        if queue.limiter is not None:
            limiter_period_ms = int(queue.limiter["period"] * 1000)
        with self.engine.begin() as c:
            # Execute with snapshot isolation to ensure multiple workers respect limits
            if self.engine.dialect.name == "postgresql":
                c.execute(sa.text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))

            # If there is a limiter, compute how many functions have started in its period.
            if queue.limiter is not None:
                query = (
                    sa.select(sa.func.count())
                    .select_from(SystemSchema.workflow_status)
                    .where(SystemSchema.workflow_status.c.queue_name == queue.name)
                    .where(
                        SystemSchema.workflow_status.c.status.notin_(
                            [
                                WorkflowStatusString.ENQUEUED.value,
                                WorkflowStatusString.DELAYED.value,
                            ]
                        )
                    )
                    .where(
                        SystemSchema.workflow_status.c.started_at_epoch_ms
                        > start_time_ms - limiter_period_ms
                    )
                )
                if queue_partition_key is not None:
                    query = query.where(
                        SystemSchema.workflow_status.c.queue_partition_key
                        == queue_partition_key
                    )
                num_recent_queries = c.execute(query).fetchone()[0]  # type: ignore
                if num_recent_queries >= queue.limiter["limit"]:
                    return []

            # Compute max_tasks, the number of workflows that can be dequeued given local and global concurrency limits,
            max_tasks = sys.maxsize
            if queue.worker_concurrency is not None or queue.concurrency is not None:
                # Count how many workflows on this queue are currently PENDING both locally and globally.
                pending_tasks_query = (
                    sa.select(
                        SystemSchema.workflow_status.c.executor_id,
                        sa.func.count().label("task_count"),
                    )
                    .select_from(SystemSchema.workflow_status)
                    .where(SystemSchema.workflow_status.c.queue_name == queue.name)
                    .where(
                        SystemSchema.workflow_status.c.status
                        == WorkflowStatusString.PENDING.value
                    )
                    .group_by(SystemSchema.workflow_status.c.executor_id)
                )
                if queue_partition_key is not None:
                    pending_tasks_query = pending_tasks_query.where(
                        SystemSchema.workflow_status.c.queue_partition_key
                        == queue_partition_key
                    )
                pending_workflows = c.execute(pending_tasks_query).fetchall()
                pending_workflows_dict = {row[0]: row[1] for row in pending_workflows}
                local_pending_workflows = pending_workflows_dict.get(executor_id, 0)

                if queue.worker_concurrency is not None:
                    # Print a warning if the local concurrency limit is violated
                    if local_pending_workflows > queue.worker_concurrency:
                        dbos_logger.warning(
                            f"The number of local pending workflows ({local_pending_workflows}) on queue {queue.name} exceeds the local concurrency limit ({queue.worker_concurrency})"
                        )
                    max_tasks = max(
                        0, queue.worker_concurrency - local_pending_workflows
                    )

                if queue.concurrency is not None:
                    global_pending_workflows = sum(pending_workflows_dict.values())
                    # Print a warning if the global concurrency limit is violated
                    if global_pending_workflows > queue.concurrency:
                        dbos_logger.warning(
                            f"The total number of pending workflows ({global_pending_workflows}) on queue {queue.name} exceeds the global concurrency limit ({queue.concurrency})"
                        )
                    available_tasks = max(
                        0, queue.concurrency - global_pending_workflows
                    )
                    max_tasks = min(max_tasks, available_tasks)

            # Retrieve the first max_tasks workflows in the queue.
            # Only retrieve workflows of the local version (or without version set)
            skip_locks = queue.concurrency is None
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
                .where(
                    sa.or_(
                        SystemSchema.workflow_status.c.application_version
                        == app_version,
                        SystemSchema.workflow_status.c.application_version.is_(None),
                    )
                )
                # Unless global concurrency is set, use skip_locked to only select
                # rows that can be locked. If global concurrency is set, use no_wait
                # to ensure all processes have a consistent view of the table.
                .with_for_update(skip_locked=skip_locks, nowait=(not skip_locks))
            )
            if queue_partition_key is not None:
                query = query.where(
                    SystemSchema.workflow_status.c.queue_partition_key
                    == queue_partition_key
                )
            if queue.priority_enabled:
                query = query.order_by(
                    SystemSchema.workflow_status.c.priority.asc(),
                    SystemSchema.workflow_status.c.created_at.asc(),
                )
            else:
                query = query.order_by(SystemSchema.workflow_status.c.created_at.asc())
            if max_tasks != sys.maxsize:
                query = query.limit(int(max_tasks))

            rows = c.execute(query).fetchall()

            # Get the workflow IDs
            dequeued_ids: List[str] = [row[0] for row in rows]
            if len(dequeued_ids) > 0:
                dbos_logger.debug(
                    f"[{queue.name}] dequeueing {len(dequeued_ids)} task(s)"
                )
            ret_ids: list[str] = []

            for id in dequeued_ids:
                # If we have a limiter, stop dequeueing workflows when the number
                # of workflows started this period exceeds the limit.
                if queue.limiter is not None:
                    if len(ret_ids) + num_recent_queries >= queue.limiter["limit"]:
                        break

                # To start a workflow, first set its status to PENDING and update its executor ID
                c.execute(
                    SystemSchema.workflow_status.update()
                    .where(SystemSchema.workflow_status.c.workflow_uuid == id)
                    .values(
                        status=WorkflowStatusString.PENDING.value,
                        application_version=app_version,
                        executor_id=executor_id,
                        started_at_epoch_ms=start_time_ms,
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
                                sa.cast(
                                    sa.func.extract("epoch", sa.func.now()) * 1000,
                                    sa.BigInteger,
                                )
                                + SystemSchema.workflow_status.c.workflow_timeout_ms,
                            ),
                            else_=SystemSchema.workflow_status.c.workflow_deadline_epoch_ms,
                        ),
                    )
                )
                # Then give it a start time
                ret_ids.append(id)

            # Return the IDs of all functions we started
            return ret_ids

    def clear_queue_assignment(self, workflow_id: str) -> bool:
        with self.engine.begin() as c:
            # Reset the status of the task to "ENQUEUED"
            res = c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .where(SystemSchema.workflow_status.c.queue_name.isnot(None))
                .where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.PENDING.value
                )
                .values(
                    status=WorkflowStatusString.ENQUEUED.value, started_at_epoch_ms=None
                )
            )
            # If no rows were affected, the workflow is not anymore in the queue or was already completed
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
        max_recovery_attempts: Optional[int],
        owner_xid: Optional[str],
        is_recovery_request: Optional[bool],
        is_dequeued_request: Optional[bool],
    ) -> tuple[WorkflowStatuses, Optional[int], bool]:
        """
        Record the initial status and inputs for a workflow, and indicate if this is a new record
        """
        with self.engine.begin() as conn:
            wf_status, workflow_deadline_epoch_ms, should_execute = (
                self._insert_workflow_status(
                    status,
                    conn,
                    max_recovery_attempts=max_recovery_attempts,
                    owner_xid=owner_xid,
                    is_recovery_request=is_recovery_request,
                    is_dequeued_request=is_dequeued_request,
                )
            )
        DebugTriggers.debug_trigger_point(DebugTriggers.DEBUG_TRIGGER_INITWF_COMMIT)
        return wf_status, workflow_deadline_epoch_ms, should_execute

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

    @db_retry()
    def read_stream(self, workflow_uuid: str, key: str, offset: int) -> Any:
        """Read the value at the specified offset for the given workflow_uuid and key."""

        with self.engine.begin() as c:
            result = c.execute(
                sa.select(
                    SystemSchema.streams.c.value, SystemSchema.streams.c.serialization
                ).where(
                    SystemSchema.streams.c.workflow_uuid == workflow_uuid,
                    SystemSchema.streams.c.key == key,
                    SystemSchema.streams.c.offset == offset,
                )
            ).fetchone()

            if result is None:
                raise ValueError(
                    f"No value found for workflow_uuid={workflow_uuid}, key={key}, offset={offset}"
                )

            # Deserialize the value before returning
            return deserialize_value(result[0], result[1], self.serializer)

    def garbage_collect(
        self, cutoff_epoch_timestamp_ms: Optional[int], rows_threshold: Optional[int]
    ) -> Optional[tuple[int, list[str]]]:
        if rows_threshold is not None:
            with self.engine.begin() as c:
                # Get the created_at timestamp of the rows_threshold newest row
                result = c.execute(
                    sa.select(SystemSchema.workflow_status.c.created_at)
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

        with self.engine.begin() as c:
            # Delete all workflows older than cutoff that are NOT PENDING, ENQUEUED, or DELAYED
            c.execute(
                sa.delete(SystemSchema.workflow_status)
                .where(
                    SystemSchema.workflow_status.c.created_at
                    < cutoff_epoch_timestamp_ms
                )
                .where(
                    ~SystemSchema.workflow_status.c.status.in_(
                        [
                            WorkflowStatusString.PENDING.value,
                            WorkflowStatusString.ENQUEUED.value,
                            WorkflowStatusString.DELAYED.value,
                        ]
                    )
                )
            )

            # Then, get the IDs of all remaining old workflows
            pending_enqueued_result = c.execute(
                sa.select(SystemSchema.workflow_status.c.workflow_uuid).where(
                    SystemSchema.workflow_status.c.created_at
                    < cutoff_epoch_timestamp_ms
                )
            ).fetchall()

            # Return the final cutoff and workflow IDs
            return cutoff_epoch_timestamp_ms, [
                row[0] for row in pending_enqueued_result
            ]

    def get_metrics(self, start_time: str, end_time: str) -> List[MetricData]:
        """
        Retrieve the number of workflows and steps that ran in a time range.

        Args:
            start_time: ISO 8601 formatted start time
            end_time: ISO 8601 formatted end time
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

    def get_workflow_children(self, workflow_id: str) -> list[str]:
        """
        Recursively get all child workflow IDs for a workflow.

        Args:
            workflow_id: The workflow UUID to get children for

        Returns:
            A list of all child (and grandchild, etc.) workflow IDs
        """
        children: set[str] = set()
        to_process: list[str] = [workflow_id]

        with self.engine.begin() as c:
            while to_process:
                current_id = to_process.pop()
                # Find all child workflows for the current workflow
                child_rows = c.execute(
                    sa.select(SystemSchema.operation_outputs.c.child_workflow_id).where(
                        (SystemSchema.operation_outputs.c.workflow_uuid == current_id)
                        & (
                            SystemSchema.operation_outputs.c.child_workflow_id.isnot(
                                None
                            )
                        )
                    )
                ).fetchall()

                for row in child_rows:
                    child_id = row[0]
                    if child_id not in children:
                        children.add(child_id)
                        to_process.append(child_id)

        return list(children)

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

    def list_schedules(
        self,
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
            )
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

    def create_application_version(self, version_name: str) -> None:
        with self.engine.begin() as c:
            c.execute(
                self.dialect.insert(SystemSchema.application_versions)
                .values(
                    version_id=generate_uuid(),
                    version_name=version_name,
                )
                .on_conflict_do_nothing(index_elements=["version_name"])
            )

    def update_application_version_timestamp(
        self, version_name: str, new_timestamp: int
    ) -> None:
        with self.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.application_versions)
                .where(SystemSchema.application_versions.c.version_name == version_name)
                .values(version_timestamp=new_timestamp)
            )

    def list_application_versions(self) -> List[VersionInfo]:
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(
                    SystemSchema.application_versions.c.version_id,
                    SystemSchema.application_versions.c.version_name,
                    SystemSchema.application_versions.c.version_timestamp,
                    SystemSchema.application_versions.c.created_at,
                ).order_by(SystemSchema.application_versions.c.version_timestamp.desc())
            ).fetchall()
            return [
                VersionInfo(
                    version_id=row[0],
                    version_name=row[1],
                    version_timestamp=row[2],
                    created_at=row[3],
                )
                for row in rows
            ]

    def get_latest_application_version(self) -> VersionInfo:
        with self.engine.begin() as c:
            row = c.execute(
                sa.select(
                    SystemSchema.application_versions.c.version_id,
                    SystemSchema.application_versions.c.version_name,
                    SystemSchema.application_versions.c.version_timestamp,
                    SystemSchema.application_versions.c.created_at,
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
            return result
