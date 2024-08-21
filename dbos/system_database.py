import datetime
import os
import select
import threading
import time
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Sequence, TypedDict, cast

import psycopg2
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
from alembic import command
from alembic.config import Config

import dbos.utils as utils
from dbos.error import (
    DBOSDuplicateWorkflowEventError,
    DBOSNonExistentWorkflowError,
    DBOSWorkflowConflictUUIDError,
)

from .dbos_config import ConfigFile
from .logger import dbos_logger
from .schemas.system_database import SystemSchema


class WorkflowStatusString(Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    RETRIES_EXCEEDED = "RETRIES_EXCEEDED"
    CANCELLED = "CANCELLED"


WorkflowStatuses = Literal[
    "PENDING", "SUCCESS", "ERROR", "RETRIES_EXCEEDED", "CANCELLED"
]


class WorkflowInputs(TypedDict):
    args: Any
    kwargs: Any


class WorkflowStatusInternal(TypedDict):
    workflow_uuid: str
    status: WorkflowStatuses
    name: str
    class_name: Optional[str]
    config_name: Optional[str]
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)
    executor_id: Optional[str]
    app_version: Optional[str]
    app_id: Optional[str]
    request: Optional[str]  # JSON (jsonpickle)
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
    def __init__(self) -> None:
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


class GetWorkflowsOutput:
    def __init__(self, workflow_uuids: List[str]):
        self.workflow_uuids = workflow_uuids


class WorkflowInformation(TypedDict, total=False):
    workflow_uuid: str
    status: WorkflowStatuses  # The status of the workflow.
    name: str  # The name of the workflow function.
    workflow_class_name: str  # The class name holding the workflow function.
    workflow_config_name: (
        str  # The name of the configuration, if the class needs configuration
    )
    authenticated_user: str  # The user who ran the workflow. Empty string if not set.
    assumed_role: str
    # The role used to run this workflow.  Empty string if authorization is not required.
    authenticatedRoles: List[str]
    # All roles the authenticated user has, if any.
    input: Optional[WorkflowInputs]
    output: Optional[str]
    error: Optional[str]
    request: Optional[str]


dbos_null_topic = "__null__topic__"


class SystemDatabase:

    def __init__(self, config: ConfigFile):
        self.config = config

        sysdb_name = (
            config["database"]["sys_db_name"]
            if "sys_db_name" in config["database"] and config["database"]["sys_db_name"]
            else config["database"]["app_db_name"] + SystemSchema.sysdb_suffix
        )

        # If the system database does not already exist, create it
        postgres_db_url = sa.URL.create(
            "postgresql",
            username=config["database"]["username"],
            password=config["database"]["password"],
            host=config["database"]["hostname"],
            port=config["database"]["port"],
            database="postgres",
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
            "postgresql",
            username=config["database"]["username"],
            password=config["database"]["password"],
            host=config["database"]["hostname"],
            port=config["database"]["port"],
            database=sysdb_name,
        )

        # Create a connection pool for the system database
        self.engine = sa.create_engine(system_db_url, pool_size=10, pool_timeout=30)

        # Run a schema migration for the system database
        migration_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "migrations"
        )
        alembic_cfg = Config()
        alembic_cfg.set_main_option("script_location", migration_dir)
        alembic_cfg.set_main_option(
            "sqlalchemy.url", self.engine.url.render_as_string(hide_password=False)
        )
        command.upgrade(alembic_cfg, "head")

        # Start the notification listener
        self.notification_conn = psycopg2.connect(
            system_db_url.render_as_string(hide_password=False)
        )
        self.notification_conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        )
        self.notification_cursor = self.notification_conn.cursor()
        self.notifications_map: Dict[str, threading.Condition] = {}
        self.workflow_events_map: Dict[str, threading.Condition] = {}
        self._run_notification_listener = True

    # Destroy the pool when finished
    def destroy(self) -> None:
        self._run_notification_listener = False
        self.notification_cursor.close()
        self.notification_conn.close()
        self.engine.dispose()

    def update_workflow_status(
        self,
        status: WorkflowStatusInternal,
        replace: bool = True,
        in_recovery: bool = False,
    ) -> None:
        cmd = pg.insert(SystemSchema.workflow_status).values(
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
        )
        if replace:
            cmd = cmd.on_conflict_do_update(
                index_elements=["workflow_uuid"],
                set_=dict(
                    status=status["status"],
                    output=status["output"],
                    error=status["error"],
                ),
            )
        elif in_recovery:
            cmd = cmd.on_conflict_do_update(
                index_elements=["workflow_uuid"],
                set_=dict(
                    recovery_attempts=SystemSchema.workflow_status.c.recovery_attempts
                    + 1,
                ),
            )
        else:
            cmd = cmd.on_conflict_do_nothing()

        with self.engine.begin() as c:
            c.execute(cmd)

    def set_workflow_status(
        self,
        workflow_uuid: str,
        status: WorkflowStatusString,
        reset_recovery_attempts: bool,
    ) -> None:
        with self.engine.begin() as c:
            stmt = (
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_inputs.c.workflow_uuid == workflow_uuid)
                .values(
                    status=status,
                )
            )
            c.execute(stmt)

        if reset_recovery_attempts:
            with self.engine.begin() as c:
                stmt = (
                    sa.update(SystemSchema.workflow_status)
                    .where(
                        SystemSchema.workflow_inputs.c.workflow_uuid == workflow_uuid
                    )
                    .values(recovery_attempts=reset_recovery_attempts)
                )
                c.execute(stmt)

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
                ).where(SystemSchema.workflow_status.c.workflow_uuid == workflow_uuid)
            ).fetchone()
            if row is None:
                return None
            status: WorkflowStatusInternal = {
                "workflow_uuid": workflow_uuid,
                "status": row[0],
                "name": row[1],
                "class_name": row[5],
                "config_name": row[4],
                "output": None,
                "error": None,
                "app_id": None,
                "app_version": None,
                "executor_id": None,
                "request": row[2],
                "recovery_attempts": row[3],
            }
            return status

    def get_workflow_status_within_wf(
        self, workflow_uuid: str, calling_wf: str, calling_wf_fn: int
    ) -> Optional[WorkflowStatusInternal]:
        res = self.check_operation_execution(calling_wf, calling_wf_fn)
        if res is not None:
            if res["output"]:
                resstat: WorkflowStatusInternal = utils.deserialize(res["output"])
                return resstat
            return None
        stat = self.get_workflow_status(workflow_uuid)
        self.record_operation_result(
            {
                "workflow_uuid": calling_wf,
                "function_id": calling_wf_fn,
                "output": utils.serialize(stat),
                "error": None,
            }
        )
        return stat

    def get_workflow_status_w_outputs(
        self, workflow_uuid: str
    ) -> Optional[WorkflowStatusInternal]:
        with self.engine.begin() as c:
            row = c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.status,
                    SystemSchema.workflow_status.c.name,
                    SystemSchema.workflow_status.c.request,
                    SystemSchema.workflow_status.c.output,
                    SystemSchema.workflow_status.c.error,
                    SystemSchema.workflow_status.c.config_name,
                    SystemSchema.workflow_status.c.class_name,
                ).where(SystemSchema.workflow_status.c.workflow_uuid == workflow_uuid)
            ).fetchone()
            if row is None:
                return None
            status: WorkflowStatusInternal = {
                "workflow_uuid": workflow_uuid,
                "status": row[0],
                "name": row[1],
                "config_name": row[5],
                "class_name": row[6],
                "output": row[3],
                "error": row[4],
                "app_id": None,
                "app_version": None,
                "executor_id": None,
                "request": row[2],
                "recovery_attempts": None,
            }
            return status

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
            return utils.deserialize(stat["output"])
        elif status == str(WorkflowStatusString.ERROR.value):
            raise utils.deserialize(stat["error"])
        return None

    def get_workflow_info(
        self, workflow_uuid: str, get_request: bool
    ) -> Optional[WorkflowInformation]:
        stat = self.get_workflow_status_w_outputs(workflow_uuid)
        if stat is None:
            return None
        info = cast(WorkflowInformation, stat)
        input = self.get_workflow_inputs(workflow_uuid)
        if input is not None:
            info["input"] = input
        if not get_request:
            info.pop("request", None)

        return info

    def update_workflow_inputs(self, workflow_uuid: str, inputs: str) -> None:
        with self.engine.begin() as c:
            c.execute(
                pg.insert(SystemSchema.workflow_inputs)
                .values(
                    workflow_uuid=workflow_uuid,
                    inputs=inputs,
                )
                .on_conflict_do_nothing()
            )

    def get_workflow_inputs(self, workflow_uuid: str) -> Optional[WorkflowInputs]:
        with self.engine.begin() as c:
            row = c.execute(
                sa.select(SystemSchema.workflow_inputs.c.inputs).where(
                    SystemSchema.workflow_inputs.c.workflow_uuid == workflow_uuid
                )
            ).fetchone()
            if row is None:
                return None
            inputs: WorkflowInputs = utils.deserialize(row[0])
            return inputs

    def get_workflows(self, input: GetWorkflowsInput) -> GetWorkflowsOutput:
        query = sa.select(SystemSchema.workflow_status.c.workflow_uuid).order_by(
            SystemSchema.workflow_status.c.created_at.desc()
        )

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
                >= datetime.datetime.fromisoformat(input.start_time).timestamp()
            )
        if input.end_time:
            query = query.where(
                SystemSchema.workflow_status.c.created_at
                <= datetime.datetime.fromisoformat(input.end_time).timestamp()
            )
        if input.status:
            query = query.where(SystemSchema.workflow_status.c.status == input.status)
        if input.application_version:
            query = query.where(
                SystemSchema.workflow_status.c.application_version
                == input.application_version
            )
        if input.limit:
            query = query.limit(input.limit)

        with self.engine.begin() as c:
            rows = c.execute(query)
        workflow_uuids = [row[0] for row in rows]

        return GetWorkflowsOutput(workflow_uuids)

    def get_pending_workflows(self, executor_id: str) -> list[str]:
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(SystemSchema.workflow_status.c.workflow_uuid).where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.PENDING.value,
                    SystemSchema.workflow_status.c.executor_id == executor_id,
                )
            ).fetchall()
            return [row[0] for row in rows]

    def record_operation_result(
        self, result: OperationResultInternal, conn: Optional[sa.Connection] = None
    ) -> None:
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
        except sa.exc.IntegrityError:
            raise DBOSWorkflowConflictUUIDError(result["workflow_uuid"])
        except Exception as e:
            raise e

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
        topic = topic if topic is not None else dbos_null_topic
        with self.engine.begin() as c:
            recorded_output = self.check_operation_execution(
                workflow_uuid, function_id, conn=c
            )
            if recorded_output is not None:
                return  # Already sent before

            try:
                c.execute(
                    pg.insert(SystemSchema.notifications).values(
                        destination_uuid=destination_uuid,
                        topic=topic,
                        message=utils.serialize(message),
                    )
                )
            except sa.exc.IntegrityError:
                raise DBOSNonExistentWorkflowError(destination_uuid)
            except Exception as e:
                raise e
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
        topic = topic if topic is not None else dbos_null_topic

        # First, check for previous executions.
        recorded_output = self.check_operation_execution(workflow_uuid, function_id)
        if recorded_output is not None:
            if recorded_output["output"] is not None:
                return utils.deserialize(recorded_output["output"])
            else:
                raise Exception("No output recorded in the last recv")

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
            payload = f"{workflow_uuid}::{topic}"
            condition = threading.Condition()
            self.notifications_map[payload] = condition
            condition.acquire()
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
                message = utils.deserialize(rows[0][0])
            self.record_operation_result(
                {
                    "workflow_uuid": workflow_uuid,
                    "function_id": function_id,
                    "output": utils.serialize(
                        message
                    ),  # None will be serialized to 'null'
                    "error": None,
                },
                conn=c,
            )
        return message

    def _notification_listener(self) -> None:
        # Listen to notifications
        dbos_logger.info("Listening to notifications")
        self.notification_cursor.execute("LISTEN dbos_notifications_channel")
        self.notification_cursor.execute("LISTEN dbos_workflow_events_channel")
        while self._run_notification_listener:
            if select.select([self.notification_conn], [], [], 60) == ([], [], []):
                continue
            else:
                self.notification_conn.poll()
                while self.notification_conn.notifies:
                    notify = self.notification_conn.notifies.pop(0)
                    channel = notify.channel
                    dbos_logger.debug(
                        f"Received notification on channel: {channel}, payload: {notify.payload}"
                    )
                    if channel == "dbos_notifications_channel":
                        if notify.payload and notify.payload in self.notifications_map:
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

    def sleep(
        self,
        workflow_uuid: str,
        function_id: int,
        seconds: float,
        skip_sleep: bool = False,
    ) -> float:
        recorded_output = self.check_operation_execution(workflow_uuid, function_id)
        end_time: float
        if recorded_output is not None:
            assert recorded_output["output"] is not None, "no recorded end time"
            end_time = utils.deserialize(recorded_output["output"])
        else:
            end_time = time.time() + seconds
            try:
                self.record_operation_result(
                    {
                        "workflow_uuid": workflow_uuid,
                        "function_id": function_id,
                        "output": utils.serialize(end_time),
                        "error": None,
                    }
                )
            except DBOSWorkflowConflictUUIDError:
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
            if recorded_output is not None:
                return  # Already sent before

            try:
                c.execute(
                    pg.insert(SystemSchema.workflow_events).values(
                        workflow_uuid=workflow_uuid,
                        key=key,
                        value=utils.serialize(message),
                    )
                )
            except sa.exc.IntegrityError:
                raise DBOSDuplicateWorkflowEventError(workflow_uuid, key)
            except Exception as e:
                raise e
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
            if recorded_output is not None:
                if recorded_output["output"] is not None:
                    return utils.deserialize(recorded_output["output"])
                else:
                    raise Exception("No output recorded in the last get_event")

        # Check if the key is already in the database. If not, wait for the notification.
        init_recv: Sequence[Any]
        with self.engine.begin() as c:
            init_recv = c.execute(get_sql).fetchall()

        value: Any = None
        if len(init_recv) > 0:
            value = utils.deserialize(init_recv[0][0])
        else:
            # Wait for the notification
            payload = f"{target_uuid}::{key}"
            condition = threading.Condition()
            self.workflow_events_map[payload] = condition
            condition.acquire()
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
            condition.release()
            self.workflow_events_map.pop(payload)

            # Read the value from the database
            with self.engine.begin() as c:
                final_recv = c.execute(get_sql).fetchall()
                if len(final_recv) > 0:
                    value = utils.deserialize(final_recv[0][0])

        # Record the output if it's in a workflow
        if caller_ctx is not None:
            self.record_operation_result(
                {
                    "workflow_uuid": caller_ctx["workflow_uuid"],
                    "function_id": caller_ctx["function_id"],
                    "output": utils.serialize(
                        value
                    ),  # None will be serialized to 'null'
                    "error": None,
                }
            )
        return value
