import datetime
import logging
import os
import re
import threading
import time
import uuid
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
)

import psycopg
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import func

from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams

from . import _serialization
from ._context import get_local_dbos_context
from ._dbos_config import ConfigFile, DatabaseConfig
from ._error import (
    DBOSConflictingWorkflowError,
    DBOSDeadLetterQueueError,
    DBOSNonExistentWorkflowError,
    DBOSWorkflowCancelledError,
    DBOSWorkflowConflictIDError,
)
from ._logger import dbos_logger
from ._registrations import DEFAULT_MAX_RECOVERY_ATTEMPTS
from ._schemas.system_database import SystemSchema

if TYPE_CHECKING:
    from ._queue import Queue


class WorkflowStatusString(Enum):
    """Enumeration of values allowed for `WorkflowSatusInternal.status`."""

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    RETRIES_EXCEEDED = "RETRIES_EXCEEDED"
    CANCELLED = "CANCELLED"
    ENQUEUED = "ENQUEUED"


WorkflowStatuses = Literal[
    "PENDING", "SUCCESS", "ERROR", "RETRIES_EXCEEDED", "CANCELLED", "ENQUEUED"
]


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
    request: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)
    created_at: Optional[int]  # Unix epoch timestamp in ms
    updated_at: Optional[int]  # Unix epoch timestamp in ms
    queue_name: Optional[str]
    executor_id: Optional[str]
    app_version: Optional[str]
    app_id: Optional[str]
    recovery_attempts: Optional[int]


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
        self.status: Optional[str] = None
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


class GetQueuedWorkflowsInput(TypedDict):
    queue_name: Optional[str]  # Get workflows belonging to this queue
    status: Optional[str]  # Get workflows with this status
    start_time: Optional[str]  # Timestamp in ISO 8601 format
    end_time: Optional[str]  # Timestamp in ISO 8601 format
    limit: Optional[int]  # Return up to this many workflows IDs.
    offset: Optional[int]  # Offset into the matching records for pagination
    name: Optional[str]  # The name of the workflow function
    sort_desc: Optional[bool]  # Sort by created_at in DESC or ASC order


class GetWorkflowsOutput:
    def __init__(self, workflow_uuids: List[str]):
        self.workflow_uuids = workflow_uuids


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


class SystemDatabase:

    def __init__(self, database: DatabaseConfig, *, debug_mode: bool = False):
        sysdb_name = (
            database["sys_db_name"]
            if "sys_db_name" in database and database["sys_db_name"]
            else database["app_db_name"] + SystemSchema.sysdb_suffix
        )

        if not debug_mode:
            # If the system database does not already exist, create it
            postgres_db_url = sa.URL.create(
                "postgresql+psycopg",
                username=database["username"],
                password=database["password"],
                host=database["hostname"],
                port=database["port"],
                database="postgres",
                # fills the "application_name" column in pg_stat_activity
                query={"application_name": f"dbos_transact_{GlobalParams.executor_id}"},
            )
            engine = sa.create_engine(postgres_db_url)
            with engine.connect() as conn:
                conn.execution_options(isolation_level="AUTOCOMMIT")
                if not conn.execute(
                    sa.text("SELECT 1 FROM pg_database WHERE datname=:db_name"),
                    parameters={"db_name": sysdb_name},
                ).scalar():
                    conn.execute(sa.text(f"CREATE DATABASE {sysdb_name}"))
            engine.dispose()

        system_db_url = sa.URL.create(
            "postgresql+psycopg",
            username=database["username"],
            password=database["password"],
            host=database["hostname"],
            port=database["port"],
            database=sysdb_name,
            # fills the "application_name" column in pg_stat_activity
            query={"application_name": f"dbos_transact_{GlobalParams.executor_id}"},
        )

        # Create a connection pool for the system database
        pool_size = database.get("sys_db_pool_size")
        if pool_size is None:
            pool_size = 20

        self.engine = sa.create_engine(
            system_db_url,
            pool_size=pool_size,
            max_overflow=0,
            pool_timeout=30,
            connect_args={"connect_timeout": 10},
        )

        # Run a schema migration for the system database
        if not debug_mode:
            migration_dir = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "_migrations"
            )
            alembic_cfg = Config()
            alembic_cfg.set_main_option("script_location", migration_dir)
            logging.getLogger("alembic").setLevel(logging.WARNING)
            # Alembic requires the % in URL-escaped parameters to itself be escaped to %%.
            escaped_conn_string = re.sub(
                r"%(?=[0-9A-Fa-f]{2})",
                "%%",
                self.engine.url.render_as_string(hide_password=False),
            )
            alembic_cfg.set_main_option("sqlalchemy.url", escaped_conn_string)
            try:
                command.upgrade(alembic_cfg, "head")
            except Exception as e:
                dbos_logger.warning(
                    f"Exception during system database construction. This is most likely because the system database was configured using a later version of DBOS: {e}"
                )
                alembic_cfg = Config()
                alembic_cfg.set_main_option("script_location", migration_dir)
                # Alembic requires the % in URL-escaped parameters to itself be escaped to %%.
                escaped_conn_string = re.sub(
                    r"%(?=[0-9A-Fa-f]{2})",
                    "%%",
                    self.engine.url.render_as_string(hide_password=False),
                )
                alembic_cfg.set_main_option("sqlalchemy.url", escaped_conn_string)
                try:
                    command.upgrade(alembic_cfg, "head")
                except Exception as e:
                    dbos_logger.warning(
                        f"Exception during system database construction. This is most likely because the system database was configured using a later version of DBOS: {e}"
                    )

        self.notification_conn: Optional[psycopg.connection.Connection] = None
        self.notifications_map: Dict[str, threading.Condition] = {}
        self.workflow_events_map: Dict[str, threading.Condition] = {}

        # Now we can run background processes
        self._run_background_processes = True
        self._debug_mode = debug_mode

    # Destroy the pool when finished
    def destroy(self) -> None:
        self._run_background_processes = False
        if self.notification_conn is not None:
            self.notification_conn.close()
        self.engine.dispose()

    def insert_workflow_status(
        self,
        status: WorkflowStatusInternal,
        *,
        max_recovery_attempts: int = DEFAULT_MAX_RECOVERY_ATTEMPTS,
    ) -> WorkflowStatuses:
        if self._debug_mode:
            raise Exception("called insert_workflow_status in debug mode")
        wf_status: WorkflowStatuses = status["status"]

        cmd = (
            pg.insert(SystemSchema.workflow_status)
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
                request=status["request"],
                authenticated_user=status["authenticated_user"],
                authenticated_roles=status["authenticated_roles"],
                assumed_role=status["assumed_role"],
                queue_name=status["queue_name"],
                recovery_attempts=(
                    1 if wf_status != WorkflowStatusString.ENQUEUED.value else 0
                ),
            )
            .on_conflict_do_update(
                index_elements=["workflow_uuid"],
                set_=dict(
                    executor_id=status["executor_id"],
                    recovery_attempts=(
                        SystemSchema.workflow_status.c.recovery_attempts + 1
                    ),
                    updated_at=func.extract("epoch", func.now()) * 1000,
                ),
            )
        )

        cmd = cmd.returning(SystemSchema.workflow_status.c.recovery_attempts, SystemSchema.workflow_status.c.status, SystemSchema.workflow_status.c.name, SystemSchema.workflow_status.c.class_name, SystemSchema.workflow_status.c.config_name, SystemSchema.workflow_status.c.queue_name)  # type: ignore

        with self.engine.begin() as c:
            results = c.execute(cmd)

        row = results.fetchone()
        if row is not None:
            # Check the started workflow matches the expected name, class_name, config_name, and queue_name
            # A mismatch indicates a workflow starting with the same UUID but different functions, which would throw an exception.
            recovery_attempts: int = row[0]
            wf_status = row[1]
            err_msg: Optional[str] = None
            if row[2] != status["name"]:
                err_msg = f"Workflow already exists with a different function name: {row[2]}, but the provided function name is: {status['name']}"
            elif row[3] != status["class_name"]:
                err_msg = f"Workflow already exists with a different class name: {row[3]}, but the provided class name is: {status['class_name']}"
            elif row[4] != status["config_name"]:
                err_msg = f"Workflow already exists with a different config name: {row[4]}, but the provided config name is: {status['config_name']}"
            elif row[5] != status["queue_name"]:
                # This is a warning because a different queue name is not necessarily an error.
                dbos_logger.warning(
                    f"Workflow already exists in queue: {row[5]}, but the provided queue name is: {status['queue_name']}. The queue is not updated."
                )
            if err_msg is not None:
                raise DBOSConflictingWorkflowError(status["workflow_uuid"], err_msg)

            # Every time we start executing a workflow (and thus attempt to insert its status), we increment `recovery_attempts` by 1.
            # When this number becomes equal to `maxRetries + 1`, we mark the workflow as `RETRIES_EXCEEDED`.
            if recovery_attempts > max_recovery_attempts + 1:
                with self.engine.begin() as c:
                    c.execute(
                        sa.delete(SystemSchema.workflow_queue).where(
                            SystemSchema.workflow_queue.c.workflow_uuid
                            == status["workflow_uuid"]
                        )
                    )
                    c.execute(
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
                            status=WorkflowStatusString.RETRIES_EXCEEDED.value,
                            queue_name=None,
                        )
                    )
                raise DBOSDeadLetterQueueError(
                    status["workflow_uuid"], max_recovery_attempts
                )

        return wf_status

    def update_workflow_status(
        self,
        status: WorkflowStatusInternal,
        *,
        conn: Optional[sa.Connection] = None,
    ) -> None:
        if self._debug_mode:
            raise Exception("called update_workflow_status in debug mode")
        wf_status: WorkflowStatuses = status["status"]

        cmd = (
            pg.insert(SystemSchema.workflow_status)
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
                request=status["request"],
                authenticated_user=status["authenticated_user"],
                authenticated_roles=status["authenticated_roles"],
                assumed_role=status["assumed_role"],
                queue_name=status["queue_name"],
                recovery_attempts=(
                    1 if wf_status != WorkflowStatusString.ENQUEUED.value else 0
                ),
            )
            .on_conflict_do_update(
                index_elements=["workflow_uuid"],
                set_=dict(
                    status=status["status"],
                    output=status["output"],
                    error=status["error"],
                    updated_at=func.extract("epoch", func.now()) * 1000,
                ),
            )
        )

        if conn is not None:
            conn.execute(cmd)
        else:
            with self.engine.begin() as c:
                c.execute(cmd)

    def cancel_workflow(
        self,
        workflow_id: str,
    ) -> None:
        if self._debug_mode:
            raise Exception("called cancel_workflow in debug mode")
        with self.engine.begin() as c:
            # Remove the workflow from the queues table so it does not block the table
            c.execute(
                sa.delete(SystemSchema.workflow_queue).where(
                    SystemSchema.workflow_queue.c.workflow_uuid == workflow_id
                )
            )
            # Set the workflow's status to CANCELLED
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .values(
                    status=WorkflowStatusString.CANCELLED.value,
                )
            )

    def resume_workflow(self, workflow_id: str) -> None:
        if self._debug_mode:
            raise Exception("called resume_workflow in debug mode")
        with self.engine.begin() as c:
            # Execute with snapshot isolation in case of concurrent calls on the same workflow
            c.execute(sa.text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
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
            # Remove the workflow from the queues table so resume can safely be called on an ENQUEUED workflow
            c.execute(
                sa.delete(SystemSchema.workflow_queue).where(
                    SystemSchema.workflow_queue.c.workflow_uuid == workflow_id
                )
            )
            # Enqueue the workflow on the internal queue
            c.execute(
                pg.insert(SystemSchema.workflow_queue).values(
                    workflow_uuid=workflow_id,
                    queue_name=INTERNAL_QUEUE_NAME,
                )
            )
            # Set the workflow's status to ENQUEUED and clear its recovery attempts.
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .values(status=WorkflowStatusString.ENQUEUED.value, recovery_attempts=0)
            )

    def fork_workflow(self, original_workflow_id: str) -> str:
        status = self.get_workflow_status(original_workflow_id)
        if status is None:
            raise Exception(f"Workflow {original_workflow_id} not found")
        inputs = self.get_workflow_inputs(original_workflow_id)
        if inputs is None:
            raise Exception(f"Workflow {original_workflow_id} not found")
        # Generate a random ID for the forked workflow
        forked_workflow_id = str(uuid.uuid4())
        with self.engine.begin() as c:
            # Create an entry for the forked workflow with the same
            # initial values as the original.
            c.execute(
                pg.insert(SystemSchema.workflow_status).values(
                    workflow_uuid=forked_workflow_id,
                    status=WorkflowStatusString.ENQUEUED.value,
                    name=status["name"],
                    class_name=status["class_name"],
                    config_name=status["config_name"],
                    application_version=status["app_version"],
                    application_id=status["app_id"],
                    request=status["request"],
                    authenticated_user=status["authenticated_user"],
                    authenticated_roles=status["authenticated_roles"],
                    assumed_role=status["assumed_role"],
                    queue_name=INTERNAL_QUEUE_NAME,
                )
            )
            # Copy the original workflow's inputs into the forked workflow
            c.execute(
                pg.insert(SystemSchema.workflow_inputs).values(
                    workflow_uuid=forked_workflow_id,
                    inputs=_serialization.serialize_args(inputs),
                )
            )
            # Enqueue the forked workflow on the internal queue
            c.execute(
                pg.insert(SystemSchema.workflow_queue).values(
                    workflow_uuid=forked_workflow_id,
                    queue_name=INTERNAL_QUEUE_NAME,
                )
            )
        return forked_workflow_id

    def get_workflow_status(
        self, workflow_uuid: str
    ) -> Optional[WorkflowStatusInternal]:
        with self.engine.begin() as c:
            row = c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.status,
                    SystemSchema.workflow_status.c.name,
                    SystemSchema.workflow_status.c.request,
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
                "request": row[2],
                "recovery_attempts": row[3],
                "config_name": row[4],
                "class_name": row[5],
                "authenticated_user": row[6],
                "authenticated_roles": row[7],
                "assumed_role": row[8],
                "queue_name": row[9],
                "executor_id": row[10],
                "created_at": row[11],
                "updated_at": row[12],
                "app_version": row[13],
                "app_id": row[14],
            }
            return status

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
                        # Raise a normal exception here, not the cancellation exception
                        # because the awaiting workflow is not being cancelled.
                        raise Exception(f"Awaited workflow {workflow_id} was cancelled")
                else:
                    pass  # CB: I guess we're assuming the WF will show up eventually.
            time.sleep(1)

    def update_workflow_inputs(
        self, workflow_uuid: str, inputs: str, conn: Optional[sa.Connection] = None
    ) -> None:
        if self._debug_mode:
            raise Exception("called update_workflow_inputs in debug mode")

        cmd = (
            pg.insert(SystemSchema.workflow_inputs)
            .values(
                workflow_uuid=workflow_uuid,
                inputs=inputs,
            )
            .on_conflict_do_update(
                index_elements=["workflow_uuid"],
                set_=dict(workflow_uuid=SystemSchema.workflow_inputs.c.workflow_uuid),
            )
            .returning(SystemSchema.workflow_inputs.c.inputs)
        )
        if conn is not None:
            row = conn.execute(cmd).fetchone()
        else:
            with self.engine.begin() as c:
                row = c.execute(cmd).fetchone()
        if row is not None and row[0] != inputs:
            # In a distributed environment, scheduled workflows are enqueued multiple times with slightly different timestamps
            if not workflow_uuid.startswith("sched-"):
                dbos_logger.warning(
                    f"Workflow {workflow_uuid} has been called multiple times with different inputs"
                )
            # TODO: actually changing the input

        return

    def get_workflow_inputs(
        self, workflow_uuid: str
    ) -> Optional[_serialization.WorkflowInputs]:
        with self.engine.begin() as c:
            row = c.execute(
                sa.select(SystemSchema.workflow_inputs.c.inputs).where(
                    SystemSchema.workflow_inputs.c.workflow_uuid == workflow_uuid
                )
            ).fetchone()
            if row is None:
                return None
            inputs: _serialization.WorkflowInputs = _serialization.deserialize_args(
                row[0]
            )
            return inputs

    def get_workflows(self, input: GetWorkflowsInput) -> GetWorkflowsOutput:
        query = sa.select(SystemSchema.workflow_status.c.workflow_uuid)
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
            query = query.where(SystemSchema.workflow_status.c.status == input.status)
        if input.application_version:
            query = query.where(
                SystemSchema.workflow_status.c.application_version
                == input.application_version
            )
        if input.workflow_ids:
            query = query.where(
                SystemSchema.workflow_status.c.workflow_uuid.in_(input.workflow_ids)
            )
        if input.limit:
            query = query.limit(input.limit)
        if input.offset:
            query = query.offset(input.offset)

        with self.engine.begin() as c:
            rows = c.execute(query)
        workflow_ids = [row[0] for row in rows]

        return GetWorkflowsOutput(workflow_ids)

    def get_queued_workflows(
        self, input: GetQueuedWorkflowsInput
    ) -> GetWorkflowsOutput:

        query = sa.select(SystemSchema.workflow_queue.c.workflow_uuid).join(
            SystemSchema.workflow_status,
            SystemSchema.workflow_queue.c.workflow_uuid
            == SystemSchema.workflow_status.c.workflow_uuid,
        )
        if input["sort_desc"]:
            query = query.order_by(SystemSchema.workflow_status.c.created_at.desc())
        else:
            query = query.order_by(SystemSchema.workflow_status.c.created_at.asc())

        if input.get("name"):
            query = query.where(SystemSchema.workflow_status.c.name == input["name"])

        if input.get("queue_name"):
            query = query.where(
                SystemSchema.workflow_queue.c.queue_name == input["queue_name"]
            )

        if input.get("status"):
            query = query.where(
                SystemSchema.workflow_status.c.status == input["status"]
            )
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
            rows = c.execute(query)
        workflow_uuids = [row[0] for row in rows]

        return GetWorkflowsOutput(workflow_uuids)

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

    def record_operation_result(
        self, result: OperationResultInternal, conn: Optional[sa.Connection] = None
    ) -> None:
        if self._debug_mode:
            raise Exception("called record_operation_result in debug mode")
        error = result["error"]
        output = result["output"]
        assert error is None or output is None, "Only one of error or output can be set"
        sql = pg.insert(SystemSchema.operation_outputs).values(
            workflow_uuid=result["workflow_uuid"],
            function_id=result["function_id"],
            function_name=result["function_name"],
            output=output,
            error=error,
        )
        try:
            if conn is not None:
                conn.execute(sql)
            else:
                with self.engine.begin() as c:
                    c.execute(sql)
        except DBAPIError as dbapi_error:
            if dbapi_error.orig.sqlstate == "23505":  # type: ignore
                raise DBOSWorkflowConflictIDError(result["workflow_uuid"])
            raise

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
            pg.insert(SystemSchema.operation_outputs)
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

    def record_child_workflow(
        self,
        parentUUID: str,
        childUUID: str,
        functionID: int,
        functionName: str,
    ) -> None:
        if self._debug_mode:
            raise Exception("called record_child_workflow in debug mode")

        sql = pg.insert(SystemSchema.operation_outputs).values(
            workflow_uuid=parentUUID,
            function_id=functionID,
            function_name=functionName,
            child_workflow_id=childUUID,
        )
        try:
            with self.engine.begin() as c:
                c.execute(sql)
        except DBAPIError as dbapi_error:
            if dbapi_error.orig.sqlstate == "23505":  # type: ignore
                raise DBOSWorkflowConflictIDError(parentUUID)
            raise

    def check_operation_execution(
        self,
        workflow_id: str,
        function_id: int,
        *,
        conn: Optional[sa.Connection] = None,
    ) -> Optional[RecordedResult]:
        # Retrieve the status of the workflow. Additionally, if this step
        # has run before, retrieve its name, output, and error.
        sql = (
            sa.select(
                SystemSchema.workflow_status.c.status,
                SystemSchema.operation_outputs.c.output,
                SystemSchema.operation_outputs.c.error,
                SystemSchema.operation_outputs.c.function_name,
            )
            .select_from(
                SystemSchema.workflow_status.outerjoin(
                    SystemSchema.operation_outputs,
                    (
                        SystemSchema.workflow_status.c.workflow_uuid
                        == SystemSchema.operation_outputs.c.workflow_uuid
                    )
                    & (SystemSchema.operation_outputs.c.function_id == function_id),
                )
            )
            .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
        )
        # If in a transaction, use the provided connection
        rows: Sequence[Any]
        if conn is not None:
            rows = conn.execute(sql).all()
        else:
            with self.engine.begin() as c:
                rows = c.execute(sql).all()
        assert len(rows) > 0, f"Error: Workflow {workflow_id} does not exist"
        workflow_status, output, error, function_name = (
            rows[0][0],
            rows[0][1],
            rows[0][2],
            rows[0][3],
        )
        # If the workflow is cancelled, raise the exception
        if workflow_status == WorkflowStatusString.CANCELLED.value:
            raise DBOSWorkflowCancelledError(
                f"Workflow {workflow_id} is cancelled. Aborting function."
            )
        # If there is no row for the function, return None
        if function_name is None:
            return None
        result: RecordedResult = {
            "output": output,
            "error": error,
        }
        return result

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

    def send(
        self,
        workflow_uuid: str,
        function_id: int,
        destination_uuid: str,
        message: Any,
        topic: Optional[str] = None,
    ) -> None:
        topic = topic if topic is not None else _dbos_null_topic
        with self.engine.begin() as c:
            recorded_output = self.check_operation_execution(
                workflow_uuid, function_id, conn=c
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
                    pg.insert(SystemSchema.notifications).values(
                        destination_uuid=destination_uuid,
                        topic=topic,
                        message=_serialization.serialize(message),
                    )
                )
            except DBAPIError as dbapi_error:
                # Foreign key violation
                if dbapi_error.orig.sqlstate == "23503":  # type: ignore
                    raise DBOSNonExistentWorkflowError(destination_uuid)
                raise
            output: OperationResultInternal = {
                "workflow_uuid": workflow_uuid,
                "function_id": function_id,
                "function_name": "DBOS.send",
                "output": None,
                "error": None,
            }
            self.record_operation_result(output, conn=c)

    def recv(
        self,
        workflow_uuid: str,
        function_id: int,
        timeout_function_id: int,
        topic: Optional[str],
        timeout_seconds: float = 60,
    ) -> Any:
        topic = topic if topic is not None else _dbos_null_topic

        # First, check for previous executions.
        recorded_output = self.check_operation_execution(workflow_uuid, function_id)
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
        condition.acquire()
        self.notifications_map[payload] = condition

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
        condition.release()
        self.notifications_map.pop(payload)

        # Transactionally consume and return the message if it's in the database, otherwise return null.
        with self.engine.begin() as c:
            oldest_entry_cte = (
                sa.select(
                    SystemSchema.notifications.c.destination_uuid,
                    SystemSchema.notifications.c.topic,
                    SystemSchema.notifications.c.message,
                    SystemSchema.notifications.c.created_at_epoch_ms,
                )
                .where(
                    SystemSchema.notifications.c.destination_uuid == workflow_uuid,
                    SystemSchema.notifications.c.topic == topic,
                )
                .order_by(SystemSchema.notifications.c.created_at_epoch_ms.asc())
                .limit(1)
                .cte("oldest_entry")
            )
            delete_stmt = (
                sa.delete(SystemSchema.notifications)
                .where(
                    SystemSchema.notifications.c.destination_uuid
                    == oldest_entry_cte.c.destination_uuid,
                    SystemSchema.notifications.c.topic == oldest_entry_cte.c.topic,
                    SystemSchema.notifications.c.created_at_epoch_ms
                    == oldest_entry_cte.c.created_at_epoch_ms,
                )
                .returning(SystemSchema.notifications.c.message)
            )
            rows = c.execute(delete_stmt).fetchall()
            message: Any = None
            if len(rows) > 0:
                message = _serialization.deserialize(rows[0][0])
            self.record_operation_result(
                {
                    "workflow_uuid": workflow_uuid,
                    "function_id": function_id,
                    "function_name": "DBOS.recv",
                    "output": _serialization.serialize(
                        message
                    ),  # None will be serialized to 'null'
                    "error": None,
                },
                conn=c,
            )
        return message

    def _notification_listener(self) -> None:
        while self._run_background_processes:
            try:
                # since we're using the psycopg connection directly, we need a url without the "+pycopg" suffix
                url = sa.URL.create(
                    "postgresql", **self.engine.url.translate_connect_args()
                )
                # Listen to notifications
                self.notification_conn = psycopg.connect(
                    url.render_as_string(hide_password=False), autocommit=True
                )

                self.notification_conn.execute("LISTEN dbos_notifications_channel")
                self.notification_conn.execute("LISTEN dbos_workflow_events_channel")

                while self._run_background_processes:
                    gen = self.notification_conn.notifies()
                    for notify in gen:
                        channel = notify.channel
                        dbos_logger.debug(
                            f"Received notification on channel: {channel}, payload: {notify.payload}"
                        )
                        if channel == "dbos_notifications_channel":
                            if (
                                notify.payload
                                and notify.payload in self.notifications_map
                            ):
                                condition = self.notifications_map[notify.payload]
                                condition.acquire()
                                condition.notify_all()
                                condition.release()
                                dbos_logger.debug(
                                    f"Signaled notifications condition for {notify.payload}"
                                )
                        elif channel == "dbos_workflow_events_channel":
                            if (
                                notify.payload
                                and notify.payload in self.workflow_events_map
                            ):
                                condition = self.workflow_events_map[notify.payload]
                                condition.acquire()
                                condition.notify_all()
                                condition.release()
                                dbos_logger.debug(
                                    f"Signaled workflow_events condition for {notify.payload}"
                                )
                        else:
                            dbos_logger.error(f"Unknown channel: {channel}")
            except Exception as e:
                if self._run_background_processes:
                    dbos_logger.error(f"Notification listener error: {e}")
                    time.sleep(1)
                    # Then the loop will try to reconnect and restart the listener
            finally:
                if self.notification_conn is not None:
                    self.notification_conn.close()

    def sleep(
        self,
        workflow_uuid: str,
        function_id: int,
        seconds: float,
        skip_sleep: bool = False,
    ) -> float:
        recorded_output = self.check_operation_execution(workflow_uuid, function_id)
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
                        "function_name": "DBOS.sleep",
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

    def set_event(
        self,
        workflow_uuid: str,
        function_id: int,
        key: str,
        message: Any,
    ) -> None:
        with self.engine.begin() as c:
            recorded_output = self.check_operation_execution(
                workflow_uuid, function_id, conn=c
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
                pg.insert(SystemSchema.workflow_events)
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
                "function_name": "DBOS.setEvent",
                "output": None,
                "error": None,
            }
            self.record_operation_result(output, conn=c)

    def get_event(
        self,
        target_uuid: str,
        key: str,
        timeout_seconds: float = 60,
        caller_ctx: Optional[GetEventWorkflowContext] = None,
    ) -> Any:
        get_sql = sa.select(
            SystemSchema.workflow_events.c.value,
        ).where(
            SystemSchema.workflow_events.c.workflow_uuid == target_uuid,
            SystemSchema.workflow_events.c.key == key,
        )
        # Check for previous executions only if it's in a workflow
        if caller_ctx is not None:
            recorded_output = self.check_operation_execution(
                caller_ctx["workflow_uuid"], caller_ctx["function_id"]
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
        self.workflow_events_map[payload] = condition
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
                    "function_name": "DBOS.getEvent",
                    "output": _serialization.serialize(
                        value
                    ),  # None will be serialized to 'null'
                    "error": None,
                }
            )
        return value

    def enqueue(self, workflow_id: str, queue_name: str) -> None:
        if self._debug_mode:
            raise Exception("called enqueue in debug mode")
        with self.engine.begin() as c:
            c.execute(
                pg.insert(SystemSchema.workflow_queue)
                .values(
                    workflow_uuid=workflow_id,
                    queue_name=queue_name,
                )
                .on_conflict_do_nothing()
            )

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
            c.execute(sa.text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))

            # If there is a limiter, compute how many functions have started in its period.
            if queue.limiter is not None:
                query = (
                    sa.select(sa.func.count())
                    .select_from(SystemSchema.workflow_queue)
                    .where(SystemSchema.workflow_queue.c.queue_name == queue.name)
                    .where(
                        SystemSchema.workflow_queue.c.started_at_epoch_ms.isnot(None)
                    )
                    .where(
                        SystemSchema.workflow_queue.c.started_at_epoch_ms
                        > start_time_ms - limiter_period_ms
                    )
                )
                num_recent_queries = c.execute(query).fetchone()[0]  # type: ignore
                if num_recent_queries >= queue.limiter["limit"]:
                    return []

            # Dequeue functions eligible for this worker and ordered by the time at which they were enqueued.
            # If there is a global or local concurrency limit N, select only the N oldest enqueued
            # functions, else select all of them.

            # First lets figure out how many tasks are eligible for dequeue.
            # This means figuring out how many unstarted tasks are within the local and global concurrency limits
            running_tasks_query = (
                sa.select(
                    SystemSchema.workflow_status.c.executor_id,
                    sa.func.count().label("task_count"),
                )
                .select_from(
                    SystemSchema.workflow_queue.join(
                        SystemSchema.workflow_status,
                        SystemSchema.workflow_queue.c.workflow_uuid
                        == SystemSchema.workflow_status.c.workflow_uuid,
                    )
                )
                .where(SystemSchema.workflow_queue.c.queue_name == queue.name)
                .where(
                    SystemSchema.workflow_queue.c.started_at_epoch_ms.isnot(
                        None
                    )  # Task is started
                )
                .where(
                    SystemSchema.workflow_queue.c.completed_at_epoch_ms.is_(
                        None
                    )  # Task is not completed.
                )
                .group_by(SystemSchema.workflow_status.c.executor_id)
            )
            running_tasks_result = c.execute(running_tasks_query).fetchall()
            running_tasks_result_dict = {row[0]: row[1] for row in running_tasks_result}
            running_tasks_for_this_worker = running_tasks_result_dict.get(
                executor_id, 0
            )  # Get count for current executor

            max_tasks = float("inf")
            if queue.worker_concurrency is not None:
                max_tasks = max(
                    0, queue.worker_concurrency - running_tasks_for_this_worker
                )
            if queue.concurrency is not None:
                total_running_tasks = sum(running_tasks_result_dict.values())
                # Queue global concurrency limit should always be >= running_tasks_count
                # This should never happen but a check + warning doesn't hurt
                if total_running_tasks > queue.concurrency:
                    dbos_logger.warning(
                        f"Total running tasks ({total_running_tasks}) exceeds the global concurrency limit ({queue.concurrency})"
                    )
                available_tasks = max(0, queue.concurrency - total_running_tasks)
                max_tasks = min(max_tasks, available_tasks)

            # Lookup unstarted/uncompleted tasks (not running)
            query = (
                sa.select(
                    SystemSchema.workflow_queue.c.workflow_uuid,
                )
                .where(SystemSchema.workflow_queue.c.queue_name == queue.name)
                .where(SystemSchema.workflow_queue.c.started_at_epoch_ms == None)
                .where(SystemSchema.workflow_queue.c.completed_at_epoch_ms == None)
                .order_by(SystemSchema.workflow_queue.c.created_at_epoch_ms.asc())
                .with_for_update(nowait=True)  # Error out early
            )
            # Apply limit only if max_tasks is finite
            if max_tasks != float("inf"):
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
                # If we have a limiter, stop starting functions when the number
                # of functions started this period exceeds the limit.
                if queue.limiter is not None:
                    if len(ret_ids) + num_recent_queries >= queue.limiter["limit"]:
                        break

                # To start a function, first set its status to PENDING and update its executor ID
                res = c.execute(
                    SystemSchema.workflow_status.update()
                    .where(SystemSchema.workflow_status.c.workflow_uuid == id)
                    .where(
                        SystemSchema.workflow_status.c.status
                        == WorkflowStatusString.ENQUEUED.value
                    )
                    .where(
                        sa.or_(
                            SystemSchema.workflow_status.c.application_version
                            == app_version,
                            SystemSchema.workflow_status.c.application_version.is_(
                                None
                            ),
                        )
                    )
                    .values(
                        status=WorkflowStatusString.PENDING.value,
                        application_version=app_version,
                        executor_id=executor_id,
                    )
                )
                if res.rowcount > 0:
                    # Then give it a start time and assign the executor ID
                    c.execute(
                        SystemSchema.workflow_queue.update()
                        .where(SystemSchema.workflow_queue.c.workflow_uuid == id)
                        .values(started_at_epoch_ms=start_time_ms)
                    )
                    ret_ids.append(id)

            # If we have a limiter, garbage-collect all completed functions started
            # before the period. If there's no limiter, there's no need--they were
            # deleted on completion.
            if queue.limiter is not None:
                c.execute(
                    sa.delete(SystemSchema.workflow_queue)
                    .where(SystemSchema.workflow_queue.c.completed_at_epoch_ms != None)
                    .where(SystemSchema.workflow_queue.c.queue_name == queue.name)
                    .where(
                        SystemSchema.workflow_queue.c.started_at_epoch_ms
                        < start_time_ms - limiter_period_ms
                    )
                )

            # Return the IDs of all functions we started
            return ret_ids

    def remove_from_queue(self, workflow_id: str, queue: "Queue") -> None:
        if self._debug_mode:
            raise Exception("called remove_from_queue in debug mode")

        with self.engine.begin() as c:
            if queue.limiter is None:
                c.execute(
                    sa.delete(SystemSchema.workflow_queue).where(
                        SystemSchema.workflow_queue.c.workflow_uuid == workflow_id
                    )
                )
            else:
                c.execute(
                    sa.update(SystemSchema.workflow_queue)
                    .where(SystemSchema.workflow_queue.c.workflow_uuid == workflow_id)
                    .values(completed_at_epoch_ms=int(time.time() * 1000))
                )

    def clear_queue_assignment(self, workflow_id: str) -> bool:
        if self._debug_mode:
            raise Exception("called clear_queue_assignment in debug mode")

        with self.engine.connect() as conn:
            with conn.begin() as transaction:
                # Reset the start time in the queue to mark it as not started
                res = conn.execute(
                    sa.update(SystemSchema.workflow_queue)
                    .where(SystemSchema.workflow_queue.c.workflow_uuid == workflow_id)
                    .where(
                        SystemSchema.workflow_queue.c.completed_at_epoch_ms.is_(None)
                    )
                    .values(started_at_epoch_ms=None)
                )

                # If no rows were affected, the workflow is not anymore in the queue or was already completed
                if res.rowcount == 0:
                    transaction.rollback()
                    return False

                # Reset the status of the task to "ENQUEUED"
                res = conn.execute(
                    sa.update(SystemSchema.workflow_status)
                    .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                    .values(status=WorkflowStatusString.ENQUEUED.value)
                )
                if res.rowcount == 0:
                    # This should never happen
                    raise Exception(
                        f"UNREACHABLE: Workflow {workflow_id} is found in the workflow_queue table but not found in the workflow_status table"
                    )
                return True

    T = TypeVar("T")

    def call_function_as_step(self, fn: Callable[[], T], function_name: str) -> T:
        ctx = get_local_dbos_context()
        if ctx and ctx.is_within_workflow():
            ctx.function_id += 1
            res = self.check_operation_execution(ctx.workflow_id, ctx.function_id)
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
        if ctx and ctx.is_within_workflow():
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


def reset_system_database(config: ConfigFile) -> None:
    sysdb_name = (
        config["database"]["sys_db_name"]
        if "sys_db_name" in config["database"] and config["database"]["sys_db_name"]
        else config["database"]["app_db_name"] + SystemSchema.sysdb_suffix
    )
    postgres_db_url = sa.URL.create(
        "postgresql+psycopg",
        username=config["database"]["username"],
        password=config["database"]["password"],
        host=config["database"]["hostname"],
        port=config["database"]["port"],
        database="postgres",
    )
    try:
        # Connect to postgres default database
        engine = sa.create_engine(postgres_db_url)

        with engine.connect() as conn:
            # Set autocommit required for database dropping
            conn.execution_options(isolation_level="AUTOCOMMIT")

            # Terminate existing connections
            conn.execute(
                sa.text(
                    """
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = :db_name
                AND pid <> pg_backend_pid()
            """
                ),
                {"db_name": sysdb_name},
            )

            # Drop the database
            conn.execute(sa.text(f"DROP DATABASE IF EXISTS {sysdb_name}"))

    except sa.exc.SQLAlchemyError as e:
        dbos_logger.error(f"Error resetting system database: {str(e)}")
        raise e
