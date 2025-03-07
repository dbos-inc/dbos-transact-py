import datetime
import logging
import os
import re
import threading
import time
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    TypedDict,
)

import psycopg
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import func

from dbos._utils import GlobalParams

from . import _serialization
from ._dbos_config import ConfigFile
from ._error import (
    DBOSConflictingWorkflowError,
    DBOSDeadLetterQueueError,
    DBOSException,
    DBOSNonExistentWorkflowError,
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
        self.status: Optional[WorkflowStatuses] = None
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


_dbos_null_topic = "__null__topic__"
_buffer_flush_batch_size = 100
_buffer_flush_interval_secs = 1.0


class SystemDatabase:

    def __init__(self, config: ConfigFile, *, debug_mode: bool = False):
        self.config = config

        sysdb_name = (
            config["database"]["sys_db_name"]
            if "sys_db_name" in config["database"] and config["database"]["sys_db_name"]
            else config["database"]["app_db_name"] + SystemSchema.sysdb_suffix
        )

        if not debug_mode:
            # If the system database does not already exist, create it
            postgres_db_url = sa.URL.create(
                "postgresql+psycopg",
                username=config["database"]["username"],
                password=config["database"]["password"],
                host=config["database"]["hostname"],
                port=config["database"]["port"],
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
            username=config["database"]["username"],
            password=config["database"]["password"],
            host=config["database"]["hostname"],
            port=config["database"]["port"],
            database=sysdb_name,
            # fills the "application_name" column in pg_stat_activity
            query={"application_name": f"dbos_transact_{GlobalParams.executor_id}"},
        )

        # Create a connection pool for the system database
        self.engine = sa.create_engine(
            system_db_url, pool_size=20, max_overflow=5, pool_timeout=30
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

        # Initialize the workflow status and inputs buffers
        self._workflow_status_buffer: Dict[str, WorkflowStatusInternal] = {}
        self._workflow_inputs_buffer: Dict[str, str] = {}
        # Two sets for tracking which single-transaction workflows have been exported to the status table
        self._exported_temp_txn_wf_status: Set[str] = set()
        self._temp_txn_wf_ids: Set[str] = set()
        self._is_flushing_status_buffer = False

        # Now we can run background processes
        self._run_background_processes = True
        self._debug_mode = debug_mode

    # Destroy the pool when finished
    def destroy(self) -> None:
        self.wait_for_buffer_flush()
        self._run_background_processes = False
        if self.notification_conn is not None:
            self.notification_conn.close()
        self.engine.dispose()

    def wait_for_buffer_flush(self) -> None:
        # Wait until the buffers are flushed.
        while self._is_flushing_status_buffer or not self._is_buffers_empty:
            dbos_logger.debug("Waiting for system buffers to be exported")
            time.sleep(1)

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

        # If this is a single-transaction workflow, record that its status has been exported
        if status["workflow_uuid"] in self._temp_txn_wf_ids:
            self._exported_temp_txn_wf_status.add(status["workflow_uuid"])

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

    def resume_workflow(
        self,
        workflow_id: str,
    ) -> None:
        if self._debug_mode:
            raise Exception("called resume_workflow in debug mode")
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
            # Remove the workflow from the queues table so resume can safely be called on an ENQUEUED workflow
            c.execute(
                sa.delete(SystemSchema.workflow_queue).where(
                    SystemSchema.workflow_queue.c.workflow_uuid == workflow_id
                )
            )
            # Set the workflow's status to PENDING and clear its recovery attempts.
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .values(status=WorkflowStatusString.PENDING.value, recovery_attempts=0)
            )

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

    def get_workflow_status_within_wf(
        self, workflow_uuid: str, calling_wf: str, calling_wf_fn: int
    ) -> Optional[WorkflowStatusInternal]:
        res = self.check_operation_execution(calling_wf, calling_wf_fn)
        if res is not None:
            if res["output"]:
                resstat: WorkflowStatusInternal = _serialization.deserialize(
                    res["output"]
                )
                return resstat
            else:
                raise DBOSException(
                    "Workflow status record not found. This should not happen! \033[1m Hint: Check if your workflow is deterministic.\033[0m"
                )
        stat = self.get_workflow_status(workflow_uuid)
        self.record_operation_result(
            {
                "workflow_uuid": calling_wf,
                "function_id": calling_wf_fn,
                "output": _serialization.serialize(stat),
                "error": None,
            }
        )
        return stat

    def await_workflow_result_internal(self, workflow_uuid: str) -> dict[str, Any]:
        polling_interval_secs: float = 1.000

        while True:
            with self.engine.begin() as c:
                row = c.execute(
                    sa.select(
                        SystemSchema.workflow_status.c.status,
                        SystemSchema.workflow_status.c.output,
                        SystemSchema.workflow_status.c.error,
                    ).where(
                        SystemSchema.workflow_status.c.workflow_uuid == workflow_uuid
                    )
                ).fetchone()
                if row is not None:
                    status = row[0]
                    if status == str(WorkflowStatusString.SUCCESS.value):
                        return {
                            "status": status,
                            "output": row[1],
                            "workflow_uuid": workflow_uuid,
                        }

                    elif status == str(WorkflowStatusString.ERROR.value):
                        return {
                            "status": status,
                            "error": row[2],
                            "workflow_uuid": workflow_uuid,
                        }

                else:
                    pass  # CB: I guess we're assuming the WF will show up eventually.

            time.sleep(polling_interval_secs)

    def await_workflow_result(self, workflow_uuid: str) -> Any:
        stat = self.await_workflow_result_internal(workflow_uuid)
        if not stat:
            return None
        status: str = stat["status"]
        if status == str(WorkflowStatusString.SUCCESS.value):
            return _serialization.deserialize(stat["output"])
        elif status == str(WorkflowStatusString.ERROR.value):
            raise _serialization.deserialize_exception(stat["error"])
        return None

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
            dbos_logger.warning(
                f"Workflow inputs for {workflow_uuid} changed since the first call! Use the original inputs."
            )
            # TODO: actually changing the input
        if workflow_uuid in self._temp_txn_wf_ids:
            # Clean up the single-transaction tracking sets
            self._exported_temp_txn_wf_status.discard(workflow_uuid)
            self._temp_txn_wf_ids.discard(workflow_uuid)
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

    def check_operation_execution(
        self, workflow_uuid: str, function_id: int, conn: Optional[sa.Connection] = None
    ) -> Optional[RecordedResult]:
        sql = sa.select(
            SystemSchema.operation_outputs.c.output,
            SystemSchema.operation_outputs.c.error,
        ).where(
            SystemSchema.operation_outputs.c.workflow_uuid == workflow_uuid,
            SystemSchema.operation_outputs.c.function_id == function_id,
        )

        # If in a transaction, use the provided connection
        rows: Sequence[Any]
        if conn is not None:
            rows = conn.execute(sql).all()
        else:
            with self.engine.begin() as c:
                rows = c.execute(sql).all()
        if len(rows) == 0:
            return None
        result: RecordedResult = {
            "output": rows[0][0],
            "error": rows[0][1],
        }
        return result

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
                    "output": _serialization.serialize(
                        value
                    ),  # None will be serialized to 'null'
                    "error": None,
                }
            )
        return value

    def _flush_workflow_status_buffer(self) -> None:
        if self._debug_mode:
            raise Exception("called _flush_workflow_status_buffer in debug mode")

        """Export the workflow status buffer to the database, up to the batch size."""
        if len(self._workflow_status_buffer) == 0:
            return

        # Record the exported status so far, and add them back on errors.
        exported_status: Dict[str, WorkflowStatusInternal] = {}
        with self.engine.begin() as c:
            exported = 0
            status_iter = iter(list(self._workflow_status_buffer))
            wf_id: Optional[str] = None
            while (
                exported < _buffer_flush_batch_size
                and (wf_id := next(status_iter, None)) is not None
            ):
                # Pop the first key in the buffer (FIFO)
                status = self._workflow_status_buffer.pop(wf_id, None)
                if status is None:
                    continue
                exported_status[wf_id] = status
                try:
                    self.update_workflow_status(status, conn=c)
                    exported += 1
                except Exception as e:
                    dbos_logger.error(f"Error while flushing status buffer: {e}")
                    c.rollback()
                    # Add the exported status back to the buffer, so they can be retried next time
                    self._workflow_status_buffer.update(exported_status)
                    break

    def _flush_workflow_inputs_buffer(self) -> None:
        if self._debug_mode:
            raise Exception("called _flush_workflow_inputs_buffer in debug mode")

        """Export the workflow inputs buffer to the database, up to the batch size."""
        if len(self._workflow_inputs_buffer) == 0:
            return

        # Record exported inputs so far, and add them back on errors.
        exported_inputs: Dict[str, str] = {}
        with self.engine.begin() as c:
            exported = 0
            input_iter = iter(list(self._workflow_inputs_buffer))
            wf_id: Optional[str] = None
            while (
                exported < _buffer_flush_batch_size
                and (wf_id := next(input_iter, None)) is not None
            ):
                if wf_id not in self._exported_temp_txn_wf_status:
                    # Skip exporting inputs if the status has not been exported yet
                    continue
                inputs = self._workflow_inputs_buffer.pop(wf_id, None)
                if inputs is None:
                    continue
                exported_inputs[wf_id] = inputs
                try:
                    self.update_workflow_inputs(wf_id, inputs, conn=c)
                    exported += 1
                except Exception as e:
                    dbos_logger.error(f"Error while flushing inputs buffer: {e}")
                    c.rollback()
                    # Add the exported inputs back to the buffer, so they can be retried next time
                    self._workflow_inputs_buffer.update(exported_inputs)
                    break

    def flush_workflow_buffers(self) -> None:
        """Flush the workflow status and inputs buffers periodically, via a background thread."""
        while self._run_background_processes:
            try:
                self._is_flushing_status_buffer = True
                # Must flush the status buffer first, as the inputs table has a foreign key constraint on the status table.
                self._flush_workflow_status_buffer()
                self._flush_workflow_inputs_buffer()
                self._is_flushing_status_buffer = False
                if self._is_buffers_empty:
                    # Only sleep if both buffers are empty
                    time.sleep(_buffer_flush_interval_secs)
            except Exception as e:
                dbos_logger.error(f"Error while flushing buffers: {e}")
                time.sleep(_buffer_flush_interval_secs)
                # Will retry next time

    def buffer_workflow_status(self, status: WorkflowStatusInternal) -> None:
        self._workflow_status_buffer[status["workflow_uuid"]] = status

    def buffer_workflow_inputs(self, workflow_id: str, inputs: str) -> None:
        # inputs is a serialized WorkflowInputs string
        self._workflow_inputs_buffer[workflow_id] = inputs
        self._temp_txn_wf_ids.add(workflow_id)

    @property
    def _is_buffers_empty(self) -> bool:
        return (
            len(self._workflow_status_buffer) == 0
            and len(self._workflow_inputs_buffer) == 0
        )

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

    def start_queued_workflows(self, queue: "Queue", executor_id: str) -> List[str]:
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

            # First lets figure out how many tasks the worker can dequeue
            running_tasks_query = (
                sa.select(
                    SystemSchema.workflow_queue.c.executor_id,
                    sa.func.count().label("task_count"),
                )
                .where(SystemSchema.workflow_queue.c.queue_name == queue.name)
                .where(
                    SystemSchema.workflow_queue.c.executor_id.isnot(
                        None
                    )  # Task is dequeued
                )
                .where(
                    SystemSchema.workflow_queue.c.completed_at_epoch_ms.is_(
                        None
                    )  # Task is not completed
                )
                .group_by(SystemSchema.workflow_queue.c.executor_id)
            )
            running_tasks_result = c.execute(running_tasks_query).fetchall()
            running_tasks_result_dict = {row[0]: row[1] for row in running_tasks_result}
            running_tasks_for_this_worker = running_tasks_result_dict.get(
                executor_id, 0
            )  # Get count for current executor

            max_tasks = float("inf")
            if queue.worker_concurrency is not None:
                # Worker local concurrency limit should always be >= running_tasks_for_this_worker
                # This should never happen but a check + warning doesn't hurt
                if running_tasks_for_this_worker > queue.worker_concurrency:
                    dbos_logger.warning(
                        f"Number of tasks on this worker ({running_tasks_for_this_worker}) exceeds the worker concurrency limit ({queue.worker_concurrency})"
                    )
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

            # Lookup tasks
            query = (
                sa.select(
                    SystemSchema.workflow_queue.c.workflow_uuid,
                    SystemSchema.workflow_queue.c.started_at_epoch_ms,
                    SystemSchema.workflow_queue.c.executor_id,
                )
                .where(SystemSchema.workflow_queue.c.queue_name == queue.name)
                .where(SystemSchema.workflow_queue.c.completed_at_epoch_ms == None)
                .where(SystemSchema.workflow_queue.c.executor_id == None)
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
                c.execute(
                    SystemSchema.workflow_status.update()
                    .where(SystemSchema.workflow_status.c.workflow_uuid == id)
                    .where(
                        SystemSchema.workflow_status.c.status
                        == WorkflowStatusString.ENQUEUED.value
                    )
                    .values(
                        status=WorkflowStatusString.PENDING.value,
                        executor_id=executor_id,
                    )
                )

                # Then give it a start time and assign the executor ID
                c.execute(
                    SystemSchema.workflow_queue.update()
                    .where(SystemSchema.workflow_queue.c.workflow_uuid == id)
                    .values(started_at_epoch_ms=start_time_ms, executor_id=executor_id)
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
                res = conn.execute(
                    sa.update(SystemSchema.workflow_queue)
                    .where(SystemSchema.workflow_queue.c.workflow_uuid == workflow_id)
                    .values(executor_id=None, started_at_epoch_ms=None)
                )

                # If no rows were affected, the workflow is not anymore in the queue
                if res.rowcount == 0:
                    transaction.rollback()
                    return False

                res = conn.execute(
                    sa.update(SystemSchema.workflow_status)
                    .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                    .values(
                        executor_id=None, status=WorkflowStatusString.ENQUEUED.value
                    )
                )
                if res.rowcount == 0:
                    # This should never happen
                    raise Exception(
                        f"UNREACHABLE: Workflow {workflow_id} is found in the workflow_queue table but not found in the workflow_status table"
                    )
                return True


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
