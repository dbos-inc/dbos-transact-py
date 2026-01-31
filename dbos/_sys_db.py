import asyncio
import datetime
import functools
import json
import random
import threading
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
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
    retriable_postgres_exception,
    retriable_sqlite_exception,
)

from ._context import DBOSContext, get_local_dbos_context
from ._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSConflictingWorkflowError,
    DBOSNonExistentWorkflowError,
    DBOSQueueDeduplicatedError,
    DBOSUnexpectedStepError,
    DBOSWorkflowCancelledError,
    DBOSWorkflowConflictIDError,
    MaxRecoveryAttemptsExceededError,
)
from ._logger import dbos_logger
from ._schemas.system_database import SystemSchema
from ._serialization import Serializer, WorkflowInputs, safe_deserialize

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


def workflow_is_active(status: str) -> bool:
    return (
        status == WorkflowStatusString.ENQUEUED.value
        or status == WorkflowStatusString.PENDING.value
    )


WorkflowStatuses = Literal[
    "PENDING",
    "SUCCESS",
    "ERROR",
    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
    "CANCELLED",
    "ENQUEUED",
]


class WorkflowStatus:
    # The workflow ID
    workflow_id: str
    # The workflow status. Must be one of ENQUEUED, PENDING, SUCCESS, ERROR, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED
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
    # If this workflow was started as a child of another workflow, that workflow's ID.
    parent_workflow_id: Optional[str]
    # The UNIX epoch timestamp at which the workflow was last dequeued, if it had been enqueued
    dequeued_at: Optional[int]

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
    owner_xid: Optional[str]


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


class RecordedResult(TypedDict):
    output: Optional[str]  # Serialized
    error: Optional[str]  # Serialized
    child_workflow_id: Optional[str]


class OperationResultInternal(TypedDict):
    workflow_uuid: str
    function_id: int
    function_name: str
    output: Optional[str]  # Serialized
    error: Optional[str]  # Serialized
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
    def __init__(self, *, workflow_uuid: str, queue_name: Optional[str] = None):
        self.workflow_uuid: str = workflow_uuid
        self.queue_name: Optional[str] = queue_name


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


_dbos_null_topic = "__null__topic__"
_dbos_stream_closed_sentinel = "__DBOS_STREAM_CLOSED__"


class ConditionCount(TypedDict):
    condition: threading.Condition
    count: int


class ThreadSafeConditionDict:
    def __init__(self) -> None:
        self._dict: Dict[str, ConditionCount] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[threading.Condition]:
        with self._lock:
            if key not in self._dict:
                # Key does not exist, return None
                return None
            return self._dict[key]["condition"]

    def set(
        self, key: str, value: threading.Condition
    ) -> tuple[bool, threading.Condition]:
        with self._lock:
            if key in self._dict:
                # Key already exists, do not overwrite. Increment the wait count.
                cc = self._dict[key]
                cc["count"] += 1
                return False, cc["condition"]
            self._dict[key] = ConditionCount(condition=value, count=1)
            return True, value

    def pop(self, key: str) -> None:
        with self._lock:
            if key in self._dict:
                cc = self._dict[key]
                cc["count"] -= 1
                if cc["count"] == 0:
                    # No more threads waiting on this condition, remove it
                    del self._dict[key]
            else:
                dbos_logger.warning(f"Key {key} not found in condition dictionary.")


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

        self.notifications_map = ThreadSafeConditionDict()
        self.workflow_events_map = ThreadSafeConditionDict()
        self.executor_id = executor_id

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

        # Values to update when a row already exists for this workflow
        update_values: dict[str, Any] = {
            "recovery_attempts": sa.case(
                (
                    SystemSchema.workflow_status.c.status
                    != WorkflowStatusString.ENQUEUED.value,
                    SystemSchema.workflow_status.c.recovery_attempts
                    + (1 if force_execute else 0),
                ),
                else_=SystemSchema.workflow_status.c.recovery_attempts,
            ),
            "updated_at": sa.func.extract("epoch", sa.func.now()) * 1000,
        }
        # Don't update an existing executor ID when enqueueing a workflow.
        if wf_status != WorkflowStatusString.ENQUEUED.value:
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
                recovery_attempts=(
                    1 if wf_status != WorkflowStatusString.ENQUEUED.value else 0
                ),
                workflow_timeout_ms=status["workflow_timeout_ms"],
                workflow_deadline_epoch_ms=status["workflow_deadline_epoch_ms"],
                deduplication_id=status["deduplication_id"],
                priority=status["priority"],
                inputs=status["inputs"],
                queue_partition_key=status["queue_partition_key"],
                parent_workflow_id=status["parent_workflow_id"],
                owner_xid=owner_xid,
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

    def cancel_workflow(
        self,
        workflow_id: str,
    ) -> None:
        with self.engine.begin() as c:
            # Check the status of the workflow. If it is complete, do nothing.
            row = c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.status,
                ).where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
            ).fetchone()
            if (
                row is None
                or row[0] == WorkflowStatusString.SUCCESS.value
                or row[0] == WorkflowStatusString.ERROR.value
            ):
                return
            # Set the workflow's status to CANCELLED and remove it from any queue it is on
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .values(
                    status=WorkflowStatusString.CANCELLED.value,
                    queue_name=None,
                    deduplication_id=None,
                    started_at_epoch_ms=None,
                    updated_at=func.extract("epoch", func.now()) * 1000,
                )
            )

    def resume_workflow(self, workflow_id: str) -> None:
        with self.engine.begin() as c:
            # Execute with snapshot isolation in case of concurrent calls on the same workflow
            if self.engine.dialect.name == "postgresql":
                c.execute(sa.text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            # Check the status of the workflow. If it is complete, do nothing.
            status_row = c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.status,
                ).where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
            ).fetchone()
            if status_row is None:
                return
            status = status_row[0]
            if (
                status == WorkflowStatusString.SUCCESS.value
                or status == WorkflowStatusString.ERROR.value
            ):
                return
            # Set the workflow's status to ENQUEUED and clear its recovery attempts and deadline.
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .values(
                    status=WorkflowStatusString.ENQUEUED.value,
                    queue_name=INTERNAL_QUEUE_NAME,
                    recovery_attempts=0,
                    workflow_deadline_epoch_ms=None,
                    deduplication_id=None,
                    started_at_epoch_ms=None,
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
        original_workflow_id: str,
        forked_workflow_id: str,
        start_step: int,
        *,
        application_version: Optional[str],
    ) -> str:

        status = self.get_workflow_status(original_workflow_id)
        if status is None:
            raise Exception(f"Workflow {original_workflow_id} not found")

        with self.engine.begin() as c:
            # Create an entry for the forked workflow with the same
            # initial values as the original.
            c.execute(
                sa.insert(SystemSchema.workflow_status).values(
                    workflow_uuid=forked_workflow_id,
                    status=WorkflowStatusString.ENQUEUED.value,
                    name=status["name"],
                    class_name=status["class_name"],
                    config_name=status["config_name"],
                    application_version=application_version,
                    application_id=status["app_id"],
                    authenticated_user=status["authenticated_user"],
                    authenticated_roles=status["authenticated_roles"],
                    assumed_role=status["assumed_role"],
                    queue_name=INTERNAL_QUEUE_NAME,
                    inputs=status["inputs"],
                    forked_from=original_workflow_id,
                )
            )

            if start_step > 1:
                # Copy the original workflow's step checkpoints
                c.execute(
                    sa.insert(SystemSchema.operation_outputs).from_select(
                        [
                            "workflow_uuid",
                            "function_id",
                            "output",
                            "error",
                            "function_name",
                            "child_workflow_id",
                            "started_at_epoch_ms",
                            "completed_at_epoch_ms",
                        ],
                        sa.select(
                            sa.literal(forked_workflow_id).label("workflow_uuid"),
                            SystemSchema.operation_outputs.c.function_id,
                            SystemSchema.operation_outputs.c.output,
                            SystemSchema.operation_outputs.c.error,
                            SystemSchema.operation_outputs.c.function_name,
                            SystemSchema.operation_outputs.c.child_workflow_id,
                            SystemSchema.operation_outputs.c.started_at_epoch_ms,
                            SystemSchema.operation_outputs.c.completed_at_epoch_ms,
                        ).where(
                            (
                                SystemSchema.operation_outputs.c.workflow_uuid
                                == original_workflow_id
                            )
                            & (
                                SystemSchema.operation_outputs.c.function_id
                                < start_step
                            )
                        ),
                    )
                )
                # Copy the original workflow's events
                c.execute(
                    sa.insert(SystemSchema.workflow_events_history).from_select(
                        [
                            "workflow_uuid",
                            "function_id",
                            "key",
                            "value",
                        ],
                        sa.select(
                            sa.literal(forked_workflow_id).label("workflow_uuid"),
                            SystemSchema.workflow_events_history.c.function_id,
                            SystemSchema.workflow_events_history.c.key,
                            SystemSchema.workflow_events_history.c.value,
                        ).where(
                            (
                                SystemSchema.workflow_events_history.c.workflow_uuid
                                == original_workflow_id
                            )
                            & (
                                SystemSchema.workflow_events_history.c.function_id
                                < start_step
                            )
                        ),
                    )
                )
                # Copy only the latest version of each workflow event from the history table
                # (the one with the maximum function_id for each key where function_id < start_step)
                weh1 = SystemSchema.workflow_events_history.alias("weh1")
                weh2 = SystemSchema.workflow_events_history.alias("weh2")

                max_function_id_subquery = (
                    sa.select(sa.func.max(weh2.c.function_id))
                    .where(
                        (weh2.c.workflow_uuid == original_workflow_id)
                        & (weh2.c.key == weh1.c.key)
                        & (weh2.c.function_id < start_step)
                    )
                    .scalar_subquery()
                )

                c.execute(
                    sa.insert(SystemSchema.workflow_events).from_select(
                        [
                            "workflow_uuid",
                            "key",
                            "value",
                        ],
                        sa.select(
                            sa.literal(forked_workflow_id).label("workflow_uuid"),
                            weh1.c.key,
                            weh1.c.value,
                        ).where(
                            (weh1.c.workflow_uuid == original_workflow_id)
                            & (weh1.c.function_id == max_function_id_subquery)
                        ),
                    )
                )
                # Copy the original workflow's streams
                c.execute(
                    sa.insert(SystemSchema.streams).from_select(
                        [
                            "workflow_uuid",
                            "function_id",
                            "key",
                            "value",
                            "offset",
                        ],
                        sa.select(
                            sa.literal(forked_workflow_id).label("workflow_uuid"),
                            SystemSchema.streams.c.function_id,
                            SystemSchema.streams.c.key,
                            SystemSchema.streams.c.value,
                            SystemSchema.streams.c.offset,
                        ).where(
                            (
                                SystemSchema.streams.c.workflow_uuid
                                == original_workflow_id
                            )
                            & (SystemSchema.streams.c.function_id < start_step)
                        ),
                    )
                )

        return forked_workflow_id

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
                "owner_xid": None,
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
    def await_workflow_result(self, workflow_id: str, polling_interval: float) -> Any:
        while True:
            with self.engine.begin() as c:
                row = c.execute(
                    sa.select(
                        SystemSchema.workflow_status.c.status,
                        SystemSchema.workflow_status.c.output,
                        SystemSchema.workflow_status.c.error,
                    ).where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                ).fetchone()
                if row is not None:
                    status = row[0]
                    if status == WorkflowStatusString.SUCCESS.value:
                        output = row[1]
                        return self.serializer.deserialize(output)
                    elif status == WorkflowStatusString.ERROR.value:
                        error = row[2]
                        e: Exception = self.serializer.deserialize(error)
                        raise e
                    elif status == WorkflowStatusString.CANCELLED.value:
                        # Raise AwaitedWorkflowCancelledError here, not the cancellation exception
                        # because the awaiting workflow is not being cancelled.
                        raise DBOSAwaitedWorkflowCancelledError(workflow_id)
                else:
                    pass  # CB: I guess we're assuming the WF will show up eventually.
            time.sleep(polling_interval)

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
        ]
        if load_input:
            load_columns.append(SystemSchema.workflow_status.c.inputs)
        if load_output:
            load_columns.append(SystemSchema.workflow_status.c.output)
            load_columns.append(SystemSchema.workflow_status.c.error)

        if queues_only:
            query = sa.select(*load_columns).where(
                SystemSchema.workflow_status.c.queue_name.isnot(None),
            )
            if not status_list:
                query = query.where(
                    SystemSchema.workflow_status.c.status.in_(["ENQUEUED", "PENDING"])
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

            idx = 23
            raw_input = row[idx] if load_input else None
            if load_input:
                idx += 1
            raw_output = row[idx] if load_output else None
            raw_error = row[idx + 1] if load_output else None
            inputs, output, exception = safe_deserialize(
                self.serializer,
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
                    workflow_uuid=row.workflow_uuid,
                    queue_name=row.queue_name,
                )
                for row in rows
            ]

    def list_workflow_steps(self, workflow_id: str) -> List[StepInfo]:
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(
                    SystemSchema.operation_outputs.c.function_id,
                    SystemSchema.operation_outputs.c.function_name,
                    SystemSchema.operation_outputs.c.output,
                    SystemSchema.operation_outputs.c.error,
                    SystemSchema.operation_outputs.c.child_workflow_id,
                    SystemSchema.operation_outputs.c.started_at_epoch_ms,
                    SystemSchema.operation_outputs.c.completed_at_epoch_ms,
                )
                .where(SystemSchema.operation_outputs.c.workflow_uuid == workflow_id)
                .order_by(SystemSchema.operation_outputs.c.function_id)
            ).fetchall()
            steps = []
            for row in rows:
                _, output, exception = safe_deserialize(
                    self.serializer,
                    workflow_id,
                    serialized_input=None,
                    serialized_output=row[2],
                    serialized_exception=row[3],
                )
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
                )
                .on_conflict_do_nothing(
                    index_elements=[
                        SystemSchema.operation_outputs.c.workflow_uuid,
                        SystemSchema.operation_outputs.c.function_id,
                    ]
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
        self._record_operation_result_retry(result, completed_at_epoch_ms)

    @db_retry()
    def _record_operation_result_retry(
        self, result: OperationResultInternal, completed_at_epoch_ms: int
    ) -> None:
        with self.engine.begin() as c:
            self._record_operation_result_txn(result, completed_at_epoch_ms, c)
        DebugTriggers.debug_trigger_point(DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT)

    @db_retry()
    def record_get_result(
        self,
        result_workflow_id: str,
        output: Optional[str],
        error: Optional[str],
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
        output, error, recorded_function_name, child_workflow_id = (
            operation_output_rows[0][0],
            operation_output_rows[0][1],
            operation_output_rows[0][2],
            operation_output_rows[0][3],
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
        topic: Optional[str] = None,
    ) -> None:
        function_name = "DBOS.send"
        start_time = int(time.time() * 1000)
        topic = topic if topic is not None else _dbos_null_topic
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
                    sa.insert(SystemSchema.notifications).values(
                        destination_uuid=destination_uuid,
                        topic=topic,
                        message=self.serializer.serialize(message),
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
            }
            self._record_operation_result_txn(output, int(time.time() * 1000), conn=c)

    @db_retry()
    def recv(
        self,
        workflow_uuid: str,
        function_id: int,
        timeout_function_id: int,
        topic: Optional[str],
        timeout_seconds: float = 60,
    ) -> Any:
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
                return self.serializer.deserialize(recorded_output["output"])
            else:
                raise Exception("No output recorded in the last recv")
        else:
            dbos_logger.debug(f"Running recv, id: {function_id}, topic: {topic}")

        # Insert a condition to the notifications map, so the listener can notify it when a message is received.
        payload = f"{workflow_uuid}::{topic}"
        condition = threading.Condition()
        # Must acquire first before adding to the map. Otherwise, the notification listener may notify it before the condition is acquired and waited.
        try:
            condition.acquire()
            success, _ = self.notifications_map.set(payload, condition)
            if not success:
                # This should not happen, but if it does, it means the workflow is executed concurrently.
                raise DBOSWorkflowConflictIDError(workflow_uuid)

            # Check if the key is already in the database. If not, wait for the notification.
            init_recv: Sequence[Any]
            with self.engine.begin() as c:
                init_recv = c.execute(
                    sa.select(
                        SystemSchema.notifications.c.topic,
                    ).where(
                        SystemSchema.notifications.c.destination_uuid == workflow_uuid,
                        SystemSchema.notifications.c.topic == topic,
                    )
                ).fetchall()

            if len(init_recv) == 0:
                # Wait for the notification
                # Support OAOO sleep
                actual_timeout = self.sleep(
                    workflow_uuid, timeout_function_id, timeout_seconds, skip_sleep=True
                )
                condition.wait(timeout=actual_timeout)
        finally:
            condition.release()
            self.notifications_map.pop(payload)

        # Transactionally consume and return the message if it's in the database, otherwise return null.
        with self.engine.begin() as c:
            delete_stmt = (
                sa.delete(SystemSchema.notifications)
                .where(
                    SystemSchema.notifications.c.destination_uuid == workflow_uuid,
                    SystemSchema.notifications.c.topic == topic,
                    SystemSchema.notifications.c.message_uuid
                    == (
                        sa.select(SystemSchema.notifications.c.message_uuid)
                        .where(
                            SystemSchema.notifications.c.destination_uuid
                            == workflow_uuid,
                            SystemSchema.notifications.c.topic == topic,
                        )
                        .order_by(
                            SystemSchema.notifications.c.created_at_epoch_ms.asc()
                        )
                        .limit(1)
                        .scalar_subquery()
                    ),
                )
                .returning(SystemSchema.notifications.c.message)
            )
            rows = c.execute(delete_stmt).fetchall()
            message: Any = None
            if len(rows) > 0:
                message = self.serializer.deserialize(rows[0][0])
            self._record_operation_result_txn(
                {
                    "workflow_uuid": workflow_uuid,
                    "function_id": function_id,
                    "function_name": function_name,
                    "started_at_epoch_ms": start_time,
                    "output": self.serializer.serialize(
                        message
                    ),  # None will be serialized to 'null'
                    "error": None,
                },
                int(time.time() * 1000),
                conn=c,
            )
        return message

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

        def signal_condition(condition_map: Any, payload: str) -> None:
            """Signal a condition variable if it exists."""
            condition = condition_map.get(payload)
            if condition:
                condition.acquire()
                condition.notify_all()
                condition.release()
                dbos_logger.debug(f"Signaled condition for {payload}")

        while self._run_background_processes:
            try:
                # Poll every second
                time.sleep(1)

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
                            )
                            .limit(1)
                        )
                        if result.fetchone():
                            signal_condition(self.notifications_map, payload)

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
                            signal_condition(self.workflow_events_map, payload)

            except Exception as e:
                if self._run_background_processes:
                    dbos_logger.warning(f"Notification poller error: {e}")
                    time.sleep(1)

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
    def sleep(
        self,
        workflow_uuid: str,
        function_id: int,
        seconds: float,
        skip_sleep: bool = False,
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
            end_time = self.serializer.deserialize(recorded_output["output"])
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
                        "output": self.serializer.serialize(end_time),
                        "error": None,
                    }
                )
            except DBOSWorkflowConflictIDError:
                pass
        duration = max(0, end_time - time.time())
        if not skip_sleep:
            time.sleep(duration)
        return duration

    @db_retry()
    def set_event_from_workflow(
        self,
        workflow_uuid: str,
        function_id: int,
        key: str,
        message: Any,
    ) -> None:
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
                    value=self.serializer.serialize(message),
                )
                .on_conflict_do_update(
                    index_elements=["workflow_uuid", "key"],
                    set_={"value": self.serializer.serialize(message)},
                )
            )
            c.execute(
                self.dialect.insert(SystemSchema.workflow_events_history)
                .values(
                    workflow_uuid=workflow_uuid,
                    function_id=function_id,
                    key=key,
                    value=self.serializer.serialize(message),
                )
                .on_conflict_do_update(
                    index_elements=["workflow_uuid", "key", "function_id"],
                    set_={"value": self.serializer.serialize(message)},
                )
            )
            output: OperationResultInternal = {
                "workflow_uuid": workflow_uuid,
                "function_id": function_id,
                "function_name": function_name,
                "started_at_epoch_ms": start_time,
                "output": None,
                "error": None,
            }
            self._record_operation_result_txn(output, int(time.time() * 1000), conn=c)

    def set_event_from_step(
        self,
        workflow_uuid: str,
        function_id: int,
        key: str,
        message: Any,
    ) -> None:
        with self.engine.begin() as c:
            c.execute(
                self.dialect.insert(SystemSchema.workflow_events)
                .values(
                    workflow_uuid=workflow_uuid,
                    key=key,
                    value=self.serializer.serialize(message),
                )
                .on_conflict_do_update(
                    index_elements=["workflow_uuid", "key"],
                    set_={"value": self.serializer.serialize(message)},
                )
            )
            c.execute(
                self.dialect.insert(SystemSchema.workflow_events_history)
                .values(
                    workflow_uuid=workflow_uuid,
                    function_id=function_id,
                    key=key,
                    value=self.serializer.serialize(message),
                )
                .on_conflict_do_update(
                    index_elements=["workflow_uuid", "key", "function_id"],
                    set_={"value": self.serializer.serialize(message)},
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
                ).where(SystemSchema.workflow_events.c.workflow_uuid == workflow_id)
            ).fetchall()
            events: Dict[str, Any] = {}
            for row in rows:
                key = row[0]
                value = self.serializer.deserialize(row[1])
                events[key] = value

            return events

    @db_retry()
    def get_event(
        self,
        target_uuid: str,
        key: str,
        timeout_seconds: float = 60,
        caller_ctx: Optional[GetEventWorkflowContext] = None,
    ) -> Any:
        function_name = "DBOS.getEvent"
        start_time = int(time.time() * 1000)
        get_sql = sa.select(
            SystemSchema.workflow_events.c.value,
        ).where(
            SystemSchema.workflow_events.c.workflow_uuid == target_uuid,
            SystemSchema.workflow_events.c.key == key,
        )
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
                    return self.serializer.deserialize(recorded_output["output"])
                else:
                    raise Exception("No output recorded in the last get_event")
            else:
                dbos_logger.debug(
                    f"Running get_event, id: {caller_ctx['function_id']}, key: {key}"
                )

        payload = f"{target_uuid}::{key}"
        condition = threading.Condition()
        condition.acquire()
        success, existing_condition = self.workflow_events_map.set(payload, condition)
        if not success:
            # Wait on the existing condition
            condition.release()
            condition = existing_condition
            condition.acquire()

        # Check if the key is already in the database. If not, wait for the notification.
        init_recv: Sequence[Any]
        with self.engine.begin() as c:
            init_recv = c.execute(get_sql).fetchall()

        value: Any = None
        if len(init_recv) > 0:
            value = self.serializer.deserialize(init_recv[0][0])
        else:
            # Wait for the notification
            actual_timeout = timeout_seconds
            if caller_ctx is not None:
                # Support OAOO sleep for workflows
                actual_timeout = self.sleep(
                    caller_ctx["workflow_uuid"],
                    caller_ctx["timeout_function_id"],
                    timeout_seconds,
                    skip_sleep=True,
                )
            condition.wait(timeout=actual_timeout)

            # Read the value from the database
            with self.engine.begin() as c:
                final_recv = c.execute(get_sql).fetchall()
                if len(final_recv) > 0:
                    value = self.serializer.deserialize(final_recv[0][0])
        condition.release()
        self.workflow_events_map.pop(payload)

        # Record the output if it's in a workflow
        if caller_ctx is not None:
            self.record_operation_result(
                {
                    "workflow_uuid": caller_ctx["workflow_uuid"],
                    "function_id": caller_ctx["function_id"],
                    "function_name": function_name,
                    "started_at_epoch_ms": start_time,
                    "output": self.serializer.serialize(
                        value
                    ),  # None will be serialized to 'null'
                    "error": None,
                }
            )
        return value

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
                    SystemSchema.workflow_status.c.status.in_(
                        [
                            WorkflowStatusString.ENQUEUED.value,
                        ]
                    )
                )
                .where(SystemSchema.workflow_status.c.queue_partition_key.isnot(None))
            )

            rows = c.execute(query).fetchall()
            return [row[0] for row in rows]

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
                        SystemSchema.workflow_status.c.status
                        != WorkflowStatusString.ENQUEUED.value
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
            max_tasks = 100  # To minimize contention with large queues, never dequeue more than 100 tasks
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
                    resstat: SystemDatabase.T = self.serializer.deserialize(
                        res["output"]
                    )
                    return resstat
                elif res["error"] is not None:
                    e: Exception = self.serializer.deserialize(res["error"])
                    raise e
                else:
                    raise Exception(
                        f"Recorded output and error are both None for {function_name}"
                    )
        result = fn()
        if ctx and ctx.is_workflow():
            self.record_operation_result(
                {
                    "workflow_uuid": ctx.workflow_id,
                    "function_id": ctx.function_id,
                    "function_name": function_name,
                    "started_at_epoch_ms": start_time,
                    "output": self.serializer.serialize(result),
                    "error": None,
                }
            )
        return result

    async def call_function_as_step_from_async(
        self, fn: Callable[[], T], function_name: str, ctx: Optional[DBOSContext]
    ) -> T:
        return await asyncio.to_thread(
            self.call_function_as_step, fn, function_name, ctx
        )

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
        Synchronously record the status and inputs for workflows in a single transaction
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

    def write_stream_from_step(
        self, workflow_uuid: str, function_id: int, key: str, value: Any
    ) -> None:
        """
        Write a key-value pair to the stream at the first unused offset.
        """
        with self.engine.begin() as c:
            # Find the maximum offset for this workflow_uuid and key combination
            max_offset_result = c.execute(
                sa.select(sa.func.max(SystemSchema.streams.c.offset)).where(
                    SystemSchema.streams.c.workflow_uuid == workflow_uuid,
                    SystemSchema.streams.c.key == key,
                )
            ).fetchone()

            # Next offset is max + 1, or 0 if no records exist
            next_offset = (
                (max_offset_result[0] + 1)
                if max_offset_result is not None and max_offset_result[0] is not None
                else 0
            )

            # Serialize the value before storing
            serialized_value = self.serializer.serialize(value)

            # Insert the new stream entry
            c.execute(
                sa.insert(SystemSchema.streams).values(
                    workflow_uuid=workflow_uuid,
                    function_id=function_id,
                    key=key,
                    value=serialized_value,
                    offset=next_offset,
                )
            )

    @db_retry()
    def write_stream_from_workflow(
        self, workflow_uuid: str, function_id: int, key: str, value: Any
    ) -> None:
        """
        Write a key-value pair to the stream at the first unused offset.
        """
        function_name = (
            "DBOS.closeStream"
            if value == _dbos_stream_closed_sentinel
            else "DBOS.writeStream"
        )
        start_time = int(time.time() * 1000)

        with self.engine.begin() as c:

            recorded_output = self._check_operation_execution_txn(
                workflow_uuid, function_id, function_name, conn=c
            )
            if recorded_output is not None:
                dbos_logger.debug(
                    f"Replaying writeStream, id: {function_id}, key: {key}"
                )
                return
            # Find the maximum offset for this workflow_uuid and key combination
            max_offset_result = c.execute(
                sa.select(sa.func.max(SystemSchema.streams.c.offset)).where(
                    SystemSchema.streams.c.workflow_uuid == workflow_uuid,
                    SystemSchema.streams.c.key == key,
                )
            ).fetchone()

            # Next offset is max + 1, or 0 if no records exist
            next_offset = (
                (max_offset_result[0] + 1)
                if max_offset_result is not None and max_offset_result[0] is not None
                else 0
            )

            # Serialize the value before storing
            serialized_value = self.serializer.serialize(value)

            # Insert the new stream entry
            c.execute(
                sa.insert(SystemSchema.streams).values(
                    workflow_uuid=workflow_uuid,
                    function_id=function_id,
                    key=key,
                    value=serialized_value,
                    offset=next_offset,
                )
            )
            output: OperationResultInternal = {
                "workflow_uuid": workflow_uuid,
                "function_id": function_id,
                "function_name": function_name,
                "started_at_epoch_ms": start_time,
                "output": None,
                "error": None,
            }
            self._record_operation_result_txn(output, int(time.time() * 1000), conn=c)

    def close_stream(self, workflow_uuid: str, function_id: int, key: str) -> None:
        """Write a sentinel value to the stream at the first unused offset to mark it as closed."""
        self.write_stream_from_workflow(
            workflow_uuid, function_id, key, _dbos_stream_closed_sentinel
        )

    @db_retry()
    def read_stream(self, workflow_uuid: str, key: str, offset: int) -> Any:
        """Read the value at the specified offset for the given workflow_uuid and key."""

        with self.engine.begin() as c:
            result = c.execute(
                sa.select(SystemSchema.streams.c.value).where(
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
            return self.serializer.deserialize(result[0])

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
            # Delete all workflows older than cutoff that are NOT PENDING or ENQUEUED
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
                    }
                    for row in output_rows
                ]

                # Export workflow_events
                event_rows = c.execute(
                    sa.select(
                        SystemSchema.workflow_events.c.workflow_uuid,
                        SystemSchema.workflow_events.c.key,
                        SystemSchema.workflow_events.c.value,
                    ).where(SystemSchema.workflow_events.c.workflow_uuid == wf_id)
                ).fetchall()

                workflow_events: list[dict[str, Any]] = [
                    {
                        "workflow_uuid": row[0],
                        "key": row[1],
                        "value": row[2],
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
                    ).where(SystemSchema.streams.c.workflow_uuid == wf_id)
                ).fetchall()

                streams: list[dict[str, Any]] = [
                    {
                        "workflow_uuid": row[0],
                        "key": row[1],
                        "value": row[2],
                        "offset": row[3],
                        "function_id": row[4],
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
                        )
                    )

                # Import workflow_events
                for event in workflow["workflow_events"]:
                    c.execute(
                        sa.insert(SystemSchema.workflow_events).values(
                            workflow_uuid=event["workflow_uuid"],
                            key=event["key"],
                            value=event["value"],
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
                        )
                    )
