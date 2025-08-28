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
    TypedDict,
    TypeVar,
    cast,
)

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import func

from dbos._utils import (
    INTERNAL_QUEUE_NAME,
    retriable_postgres_exception,
    retriable_sqlite_exception,
)

from . import _serialization
from ._context import get_local_dbos_context
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
    status: str
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
    input: Optional[_serialization.WorkflowInputs]
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
    # The executor to most recently executed this workflow
    executor_id: Optional[str]
    # The application version on which this workflow was started
    app_version: Optional[str]
    # The start-to-close timeout of the workflow in ms
    workflow_timeout_ms: Optional[int]
    # The deadline of a workflow, computed by adding its timeout to its start time.
    workflow_deadline_epoch_ms: Optional[int]

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
    # The start-to-close timeout of the workflow in ms
    workflow_timeout_ms: Optional[int]
    # The deadline of a workflow, computed by adding its timeout to its start time.
    # Deadlines propagate to children. When the deadline is reached, the workflow is cancelled.
    workflow_deadline_epoch_ms: Optional[int]
    # Unique ID for deduplication on a queue
    deduplication_id: Optional[str]
    # Priority of the workflow on the queue, starting from 1 ~ 2,147,483,647. Default 0 (highest priority).
    priority: int
    # Serialized workflow inputs
    inputs: str


class EnqueueOptionsInternal(TypedDict):
    # Unique ID for deduplication on a queue
    deduplication_id: Optional[str]
    # Priority of the workflow on the queue, starting from 1 ~ 2,147,483,647. Default 0 (highest priority).
    priority: Optional[int]
    # On what version the workflow is enqueued. Current version if not specified.
    app_version: Optional[str]


class RecordedResult(TypedDict):
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)


class OperationResultInternal(TypedDict):
    workflow_uuid: str
    function_id: int
    function_name: str
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)


class GetEventWorkflowContext(TypedDict):
    workflow_uuid: str
    function_id: int
    timeout_function_id: int


class GetWorkflowsInput:
    """
    Structure for argument to `get_workflows` function.

    This specifies the search criteria for workflow retrieval by `get_workflows`.
    """

    def __init__(self) -> None:
        self.workflow_ids: Optional[List[str]] = (
            None  # Search only in these workflow IDs
        )
        self.name: Optional[str] = None  # The name of the workflow function
        self.authenticated_user: Optional[str] = None  # The user who ran the workflow.
        self.start_time: Optional[str] = None  # Timestamp in ISO 8601 format
        self.end_time: Optional[str] = None  # Timestamp in ISO 8601 format
        self.status: Optional[List[str]] = (
            None  # Get workflows with one of these statuses
        )
        self.application_version: Optional[str] = (
            None  # The application version that ran this workflow. = None
        )
        self.limit: Optional[int] = (
            None  # Return up to this many workflows IDs. IDs are ordered by workflow creation time.
        )
        self.offset: Optional[int] = (
            None  # Offset into the matching records for pagination
        )
        self.sort_desc: bool = (
            False  # If true, sort by created_at in DESC order. Default false (in ASC order).
        )
        self.workflow_id_prefix: Optional[str] = (
            None  # If set, search for workflow IDs starting with this string
        )


class GetQueuedWorkflowsInput(TypedDict):
    queue_name: Optional[str]  # Get workflows belonging to this queue
    status: Optional[list[str]]  # Get workflows with one of these statuses
    start_time: Optional[str]  # Timestamp in ISO 8601 format
    end_time: Optional[str]  # Timestamp in ISO 8601 format
    limit: Optional[int]  # Return up to this many workflows IDs.
    offset: Optional[int]  # Offset into the matching records for pagination
    name: Optional[str]  # The name of the workflow function
    sort_desc: Optional[bool]  # Sort by created_at in DESC or ASC order


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

    def __init__(
        self,
        *,
        system_database_url: str,
        engine_kwargs: Dict[str, Any],
        debug_mode: bool = False,
    ):
        import sqlalchemy.dialects.postgresql as pg
        import sqlalchemy.dialects.sqlite as sq

        self.dialect = sq if system_database_url.startswith("sqlite") else pg
        self.engine = self._create_engine(system_database_url, engine_kwargs)
        self._engine_kwargs = engine_kwargs

        self.notifications_map = ThreadSafeConditionDict()
        self.workflow_events_map = ThreadSafeConditionDict()

        # Now we can run background processes
        self._run_background_processes = True
        self._debug_mode = debug_mode

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
    ) -> tuple[WorkflowStatuses, Optional[int]]:
        """Insert or update workflow status using PostgreSQL upsert operations."""
        if self._debug_mode:
            raise Exception("called insert_workflow_status in debug mode")
        wf_status: WorkflowStatuses = status["status"]
        workflow_deadline_epoch_ms: Optional[int] = status["workflow_deadline_epoch_ms"]

        # Values to update when a row already exists for this workflow
        update_values: dict[str, Any] = {
            "recovery_attempts": sa.case(
                (
                    SystemSchema.workflow_status.c.status
                    != WorkflowStatusString.ENQUEUED.value,
                    SystemSchema.workflow_status.c.recovery_attempts + 1,
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

        return wf_status, workflow_deadline_epoch_ms

    @db_retry()
    def update_workflow_outcome(
        self,
        workflow_id: str,
        status: WorkflowStatuses,
        *,
        output: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        if self._debug_mode:
            raise Exception("called update_workflow_status in debug mode")
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
        if self._debug_mode:
            raise Exception("called cancel_workflow in debug mode")
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
                )
            )

    def resume_workflow(self, workflow_id: str) -> None:
        if self._debug_mode:
            raise Exception("called resume_workflow in debug mode")
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
                    application_version=(
                        application_version
                        if application_version is not None
                        else status["app_version"]
                    ),
                    application_id=status["app_id"],
                    authenticated_user=status["authenticated_user"],
                    authenticated_roles=status["authenticated_roles"],
                    assumed_role=status["assumed_role"],
                    queue_name=INTERNAL_QUEUE_NAME,
                    inputs=status["inputs"],
                )
            )

            if start_step > 1:

                # Copy the original workflow's outputs into the forked workflow
                insert_stmt = sa.insert(SystemSchema.operation_outputs).from_select(
                    [
                        "workflow_uuid",
                        "function_id",
                        "output",
                        "error",
                        "function_name",
                        "child_workflow_id",
                    ],
                    sa.select(
                        sa.literal(forked_workflow_id).label("workflow_uuid"),
                        SystemSchema.operation_outputs.c.function_id,
                        SystemSchema.operation_outputs.c.output,
                        SystemSchema.operation_outputs.c.error,
                        SystemSchema.operation_outputs.c.function_name,
                        SystemSchema.operation_outputs.c.child_workflow_id,
                    ).where(
                        (
                            SystemSchema.operation_outputs.c.workflow_uuid
                            == original_workflow_id
                        )
                        & (SystemSchema.operation_outputs.c.function_id < start_step)
                    ),
                )

                c.execute(insert_stmt)
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
            }
            return status

    @db_retry()
    def await_workflow_result(self, workflow_id: str) -> Any:
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
                        return _serialization.deserialize(output)
                    elif status == WorkflowStatusString.ERROR.value:
                        error = row[2]
                        raise _serialization.deserialize_exception(error)
                    elif status == WorkflowStatusString.CANCELLED.value:
                        # Raise AwaitedWorkflowCancelledError here, not the cancellation exception
                        # because the awaiting workflow is not being cancelled.
                        raise DBOSAwaitedWorkflowCancelledError(workflow_id)
                else:
                    pass  # CB: I guess we're assuming the WF will show up eventually.
            time.sleep(1)

    def get_workflows(
        self,
        input: GetWorkflowsInput,
        *,
        load_input: bool = True,
        load_output: bool = True,
    ) -> List[WorkflowStatus]:
        """
        Retrieve a list of workflows result and inputs based on the input criteria. The result is a list of external-facing workflow status objects.
        """
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
        ]
        if load_input:
            load_columns.append(SystemSchema.workflow_status.c.inputs)
        if load_output:
            load_columns.append(SystemSchema.workflow_status.c.output)
            load_columns.append(SystemSchema.workflow_status.c.error)

        query = sa.select(*load_columns)
        if input.sort_desc:
            query = query.order_by(SystemSchema.workflow_status.c.created_at.desc())
        else:
            query = query.order_by(SystemSchema.workflow_status.c.created_at.asc())
        if input.name:
            query = query.where(SystemSchema.workflow_status.c.name == input.name)
        if input.authenticated_user:
            query = query.where(
                SystemSchema.workflow_status.c.authenticated_user
                == input.authenticated_user
            )
        if input.start_time:
            query = query.where(
                SystemSchema.workflow_status.c.created_at
                >= datetime.datetime.fromisoformat(input.start_time).timestamp() * 1000
            )
        if input.end_time:
            query = query.where(
                SystemSchema.workflow_status.c.created_at
                <= datetime.datetime.fromisoformat(input.end_time).timestamp() * 1000
            )
        if input.status:
            query = query.where(SystemSchema.workflow_status.c.status.in_(input.status))
        if input.application_version:
            query = query.where(
                SystemSchema.workflow_status.c.application_version
                == input.application_version
            )
        if input.workflow_ids:
            query = query.where(
                SystemSchema.workflow_status.c.workflow_uuid.in_(input.workflow_ids)
            )
        if input.workflow_id_prefix:
            query = query.where(
                SystemSchema.workflow_status.c.workflow_uuid.startswith(
                    input.workflow_id_prefix
                )
            )
        if input.limit:
            query = query.limit(input.limit)
        if input.offset:
            query = query.offset(input.offset)

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

            raw_input = row[17] if load_input else None
            raw_output = row[18] if load_output else None
            raw_error = row[19] if load_output else None
            inputs, output, exception = _serialization.safe_deserialize(
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

    def get_queued_workflows(
        self,
        input: GetQueuedWorkflowsInput,
        *,
        load_input: bool = True,
    ) -> List[WorkflowStatus]:
        """
        Retrieve a list of queued workflows result and inputs based on the input criteria. The result is a list of external-facing workflow status objects.
        """
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
        ]
        if load_input:
            load_columns.append(SystemSchema.workflow_status.c.inputs)

        query = sa.select(*load_columns).where(
            sa.and_(
                SystemSchema.workflow_status.c.queue_name.isnot(None),
                SystemSchema.workflow_status.c.status.in_(["ENQUEUED", "PENDING"]),
            )
        )
        if input["sort_desc"]:
            query = query.order_by(SystemSchema.workflow_status.c.created_at.desc())
        else:
            query = query.order_by(SystemSchema.workflow_status.c.created_at.asc())

        if input.get("name"):
            query = query.where(SystemSchema.workflow_status.c.name == input["name"])

        if input.get("queue_name"):
            query = query.where(
                SystemSchema.workflow_status.c.queue_name == input["queue_name"]
            )

        status = input.get("status", None)
        if status:
            query = query.where(SystemSchema.workflow_status.c.status.in_(status))
        if "start_time" in input and input["start_time"] is not None:
            query = query.where(
                SystemSchema.workflow_status.c.created_at
                >= datetime.datetime.fromisoformat(input["start_time"]).timestamp()
                * 1000
            )
        if "end_time" in input and input["end_time"] is not None:
            query = query.where(
                SystemSchema.workflow_status.c.created_at
                <= datetime.datetime.fromisoformat(input["end_time"]).timestamp() * 1000
            )
        if input.get("limit"):
            query = query.limit(input["limit"])
        if input.get("offset"):
            query = query.offset(input["offset"])

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

            raw_input = row[17] if load_input else None

            # Error and Output are not loaded because they should always be None for queued workflows.
            inputs, output, exception = _serialization.safe_deserialize(
                info.workflow_id,
                serialized_input=raw_input,
                serialized_output=None,
                serialized_exception=None,
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

    def get_workflow_steps(self, workflow_id: str) -> List[StepInfo]:
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(
                    SystemSchema.operation_outputs.c.function_id,
                    SystemSchema.operation_outputs.c.function_name,
                    SystemSchema.operation_outputs.c.output,
                    SystemSchema.operation_outputs.c.error,
                    SystemSchema.operation_outputs.c.child_workflow_id,
                ).where(SystemSchema.operation_outputs.c.workflow_uuid == workflow_id)
            ).fetchall()
            return [
                StepInfo(
                    function_id=row[0],
                    function_name=row[1],
                    output=(
                        _serialization.deserialize(row[2])
                        if row[2] is not None
                        else row[2]
                    ),
                    error=(
                        _serialization.deserialize_exception(row[3])
                        if row[3] is not None
                        else row[3]
                    ),
                    child_workflow_id=row[4],
                )
                for row in rows
            ]

    def _record_operation_result_txn(
        self, result: OperationResultInternal, conn: sa.Connection
    ) -> None:
        if self._debug_mode:
            raise Exception("called record_operation_result in debug mode")
        error = result["error"]
        output = result["output"]
        assert error is None or output is None, "Only one of error or output can be set"
        sql = sa.insert(SystemSchema.operation_outputs).values(
            workflow_uuid=result["workflow_uuid"],
            function_id=result["function_id"],
            function_name=result["function_name"],
            output=output,
            error=error,
        )
        try:
            conn.execute(sql)
        except DBAPIError as dbapi_error:
            if self._is_unique_constraint_violation(dbapi_error):
                raise DBOSWorkflowConflictIDError(result["workflow_uuid"])
            raise

    @db_retry()
    def record_operation_result(self, result: OperationResultInternal) -> None:
        with self.engine.begin() as c:
            self._record_operation_result_txn(result, c)

    @db_retry()
    def record_get_result(
        self, result_workflow_id: str, output: Optional[str], error: Optional[str]
    ) -> None:
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
        if self._debug_mode:
            raise Exception("called record_child_workflow in debug mode")

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
        output, error, recorded_function_name = (
            operation_output_rows[0][0],
            operation_output_rows[0][1],
            operation_output_rows[0][2],
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
    def check_child_workflow(
        self, workflow_uuid: str, function_id: int
    ) -> Optional[str]:
        sql = sa.select(SystemSchema.operation_outputs.c.child_workflow_id).where(
            SystemSchema.operation_outputs.c.workflow_uuid == workflow_uuid,
            SystemSchema.operation_outputs.c.function_id == function_id,
        )

        # If in a transaction, use the provided connection
        row: Any
        with self.engine.begin() as c:
            row = c.execute(sql).fetchone()

        if row is None:
            return None
        return str(row[0])

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
        topic = topic if topic is not None else _dbos_null_topic
        with self.engine.begin() as c:
            recorded_output = self._check_operation_execution_txn(
                workflow_uuid, function_id, function_name, conn=c
            )
            if self._debug_mode and recorded_output is None:
                raise Exception(
                    "called send in debug mode without a previous execution"
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
                        message=_serialization.serialize(message),
                    )
                )
            except DBAPIError as dbapi_error:
                if self._is_foreign_key_violation(dbapi_error):
                    raise DBOSNonExistentWorkflowError(destination_uuid)
                raise
            output: OperationResultInternal = {
                "workflow_uuid": workflow_uuid,
                "function_id": function_id,
                "function_name": function_name,
                "output": None,
                "error": None,
            }
            self._record_operation_result_txn(output, conn=c)

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
        topic = topic if topic is not None else _dbos_null_topic

        # First, check for previous executions.
        recorded_output = self.check_operation_execution(
            workflow_uuid, function_id, function_name
        )
        if self._debug_mode and recorded_output is None:
            raise Exception("called recv in debug mode without a previous execution")
        if recorded_output is not None:
            dbos_logger.debug(f"Replaying recv, id: {function_id}, topic: {topic}")
            if recorded_output["output"] is not None:
                return _serialization.deserialize(recorded_output["output"])
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
                message = _serialization.deserialize(rows[0][0])
            self._record_operation_result_txn(
                {
                    "workflow_uuid": workflow_uuid,
                    "function_id": function_id,
                    "function_name": function_name,
                    "output": _serialization.serialize(
                        message
                    ),  # None will be serialized to 'null'
                    "error": None,
                },
                conn=c,
            )
        return message

    @abstractmethod
    def _notification_listener(self) -> None:
        """Listen for database notifications using database-specific mechanisms."""
        pass

    @staticmethod
    def reset_system_database(database_url: str) -> None:
        """Reset the system database by calling the appropriate implementation."""
        if database_url.startswith("sqlite"):
            from ._sys_db_sqlite import SQLiteSystemDatabase

            SQLiteSystemDatabase._reset_system_database(database_url)
        else:
            from ._sys_db_postgres import PostgresSystemDatabase

            PostgresSystemDatabase._reset_system_database(database_url)

    @staticmethod
    def create(
        system_database_url: str,
        engine_kwargs: Dict[str, Any],
        debug_mode: bool = False,
    ) -> "SystemDatabase":
        """Factory method to create the appropriate SystemDatabase implementation based on URL."""
        if system_database_url.startswith("sqlite"):
            from ._sys_db_sqlite import SQLiteSystemDatabase

            return SQLiteSystemDatabase(
                system_database_url=system_database_url,
                engine_kwargs=engine_kwargs,
                debug_mode=debug_mode,
            )
        else:
            from ._sys_db_postgres import PostgresSystemDatabase

            return PostgresSystemDatabase(
                system_database_url=system_database_url,
                engine_kwargs=engine_kwargs,
                debug_mode=debug_mode,
            )

    @db_retry()
    def sleep(
        self,
        workflow_uuid: str,
        function_id: int,
        seconds: float,
        skip_sleep: bool = False,
    ) -> float:
        function_name = "DBOS.sleep"
        recorded_output = self.check_operation_execution(
            workflow_uuid, function_id, function_name
        )
        end_time: float
        if self._debug_mode and recorded_output is None:
            raise Exception("called sleep in debug mode without a previous execution")

        if recorded_output is not None:
            dbos_logger.debug(f"Replaying sleep, id: {function_id}, seconds: {seconds}")
            assert recorded_output["output"] is not None, "no recorded end time"
            end_time = _serialization.deserialize(recorded_output["output"])
        else:
            dbos_logger.debug(f"Running sleep, id: {function_id}, seconds: {seconds}")
            end_time = time.time() + seconds
            try:
                self.record_operation_result(
                    {
                        "workflow_uuid": workflow_uuid,
                        "function_id": function_id,
                        "function_name": function_name,
                        "output": _serialization.serialize(end_time),
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
    def set_event(
        self,
        workflow_uuid: str,
        function_id: int,
        key: str,
        message: Any,
    ) -> None:
        function_name = "DBOS.setEvent"
        with self.engine.begin() as c:
            recorded_output = self._check_operation_execution_txn(
                workflow_uuid, function_id, function_name, conn=c
            )
            if self._debug_mode and recorded_output is None:
                raise Exception(
                    "called set_event in debug mode without a previous execution"
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
                    value=_serialization.serialize(message),
                )
                .on_conflict_do_update(
                    index_elements=["workflow_uuid", "key"],
                    set_={"value": _serialization.serialize(message)},
                )
            )
            output: OperationResultInternal = {
                "workflow_uuid": workflow_uuid,
                "function_id": function_id,
                "function_name": function_name,
                "output": None,
                "error": None,
            }
            self._record_operation_result_txn(output, conn=c)

    @db_retry()
    def get_event(
        self,
        target_uuid: str,
        key: str,
        timeout_seconds: float = 60,
        caller_ctx: Optional[GetEventWorkflowContext] = None,
    ) -> Any:
        function_name = "DBOS.getEvent"
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
            if self._debug_mode and recorded_output is None:
                raise Exception(
                    "called get_event in debug mode without a previous execution"
                )
            if recorded_output is not None:
                dbos_logger.debug(
                    f"Replaying get_event, id: {caller_ctx['function_id']}, key: {key}"
                )
                if recorded_output["output"] is not None:
                    return _serialization.deserialize(recorded_output["output"])
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
            value = _serialization.deserialize(init_recv[0][0])
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
                    value = _serialization.deserialize(final_recv[0][0])
        condition.release()
        self.workflow_events_map.pop(payload)

        # Record the output if it's in a workflow
        if caller_ctx is not None:
            self.record_operation_result(
                {
                    "workflow_uuid": caller_ctx["workflow_uuid"],
                    "function_id": caller_ctx["function_id"],
                    "function_name": function_name,
                    "output": _serialization.serialize(
                        value
                    ),  # None will be serialized to 'null'
                    "error": None,
                }
            )
        return value

    def start_queued_workflows(
        self, queue: "Queue", executor_id: str, app_version: str
    ) -> List[str]:
        if self._debug_mode:
            return []

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
                                sa.func.extract("epoch", sa.func.now()) * 1000
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
        if self._debug_mode:
            raise Exception("called clear_queue_assignment in debug mode")

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

    def call_function_as_step(self, fn: Callable[[], T], function_name: str) -> T:
        ctx = get_local_dbos_context()
        if ctx and ctx.is_transaction():
            raise Exception(f"Invalid call to `{function_name}` inside a transaction")
        if ctx and ctx.is_workflow():
            ctx.function_id += 1
            res = self.check_operation_execution(
                ctx.workflow_id, ctx.function_id, function_name
            )
            if res is not None:
                if res["output"] is not None:
                    resstat: SystemDatabase.T = _serialization.deserialize(
                        res["output"]
                    )
                    return resstat
                elif res["error"] is not None:
                    raise _serialization.deserialize_exception(res["error"])
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
                    "output": _serialization.serialize(result),
                    "error": None,
                }
            )
        return result

    @db_retry()
    def init_workflow(
        self,
        status: WorkflowStatusInternal,
        *,
        max_recovery_attempts: Optional[int],
    ) -> tuple[WorkflowStatuses, Optional[int]]:
        """
        Synchronously record the status and inputs for workflows in a single transaction
        """
        with self.engine.begin() as conn:
            wf_status, workflow_deadline_epoch_ms = self._insert_workflow_status(
                status, conn, max_recovery_attempts=max_recovery_attempts
            )
        return wf_status, workflow_deadline_epoch_ms

    def check_connection(self) -> None:
        try:
            with self.engine.begin() as conn:
                conn.execute(sa.text("SELECT 1")).fetchall()
        except Exception as e:
            dbos_logger.error(f"Error connecting to the DBOS system database: {e}")
            raise

    def write_stream_from_step(self, workflow_uuid: str, key: str, value: Any) -> None:
        """
        Write a key-value pair to the stream at the first unused offset.
        """
        if self._debug_mode:
            raise Exception("called write_stream in debug mode")

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
            serialized_value = _serialization.serialize(value)

            # Insert the new stream entry
            c.execute(
                sa.insert(SystemSchema.streams).values(
                    workflow_uuid=workflow_uuid,
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

        with self.engine.begin() as c:

            recorded_output = self._check_operation_execution_txn(
                workflow_uuid, function_id, function_name, conn=c
            )
            if self._debug_mode and recorded_output is None:
                raise Exception(
                    "called set_event in debug mode without a previous execution"
                )
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
            serialized_value = _serialization.serialize(value)

            # Insert the new stream entry
            c.execute(
                sa.insert(SystemSchema.streams).values(
                    workflow_uuid=workflow_uuid,
                    key=key,
                    value=serialized_value,
                    offset=next_offset,
                )
            )
            output: OperationResultInternal = {
                "workflow_uuid": workflow_uuid,
                "function_id": function_id,
                "function_name": function_name,
                "output": None,
                "error": None,
            }
            self._record_operation_result_txn(output, conn=c)

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
            return _serialization.deserialize(result[0])

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
