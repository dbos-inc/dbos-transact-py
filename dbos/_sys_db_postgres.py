import time
from typing import Any, Dict, Optional

import psycopg
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
from sqlalchemy.exc import DBAPIError

from dbos._migration import (
    ensure_dbos_schema,
    run_alembic_migrations,
    run_dbos_migrations,
)

from ._error import (
    DBOSConflictingWorkflowError,
    DBOSQueueDeduplicatedError,
    MaxRecoveryAttemptsExceededError,
)
from ._logger import dbos_logger
from ._schemas.system_database import SystemSchema
from ._sys_db import (
    SystemDatabase,
    WorkflowStatuses,
    WorkflowStatusInternal,
    WorkflowStatusString,
)


class PostgresSystemDatabase(SystemDatabase):
    """PostgreSQL-specific implementation of SystemDatabase."""

    def __init__(
        self,
        *,
        system_database_url: str,
        engine_kwargs: Dict[str, Any],
        debug_mode: bool = False,
    ):
        super().__init__(
            system_database_url=system_database_url,
            engine_kwargs=engine_kwargs,
            debug_mode=debug_mode,
        )
        self.notification_conn: Optional[psycopg.connection.Connection] = None

    def _create_engine(
        self, system_database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        """Create a PostgreSQL engine."""
        url = sa.make_url(system_database_url).set(drivername="postgresql+psycopg")
        return sa.create_engine(url, **engine_kwargs)

    def run_migrations(self) -> None:
        """Run PostgreSQL-specific migrations."""
        if self._debug_mode:
            dbos_logger.warning("System database migrations are skipped in debug mode.")
            return
        system_db_url = self.engine.url
        sysdb_name = system_db_url.database
        # If the system database does not already exist, create it
        engine = sa.create_engine(
            system_db_url.set(database="postgres"), **self._engine_kwargs
        )
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            if not conn.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname=:db_name"),
                parameters={"db_name": sysdb_name},
            ).scalar():
                dbos_logger.info(f"Creating system database {sysdb_name}")
                conn.execute(sa.text(f"CREATE DATABASE {sysdb_name}"))
        engine.dispose()

        using_dbos_migrations = ensure_dbos_schema(self.engine)
        if not using_dbos_migrations:
            # Complete the Alembic migrations, create the dbos_migrations table
            run_alembic_migrations(self.engine)
        run_dbos_migrations(self.engine)

    def _cleanup_connections(self) -> None:
        """Clean up PostgreSQL-specific connections."""
        if self.notification_conn is not None:
            self.notification_conn.close()

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
        )  # type: ignore

        try:
            results = conn.execute(cmd)
        except DBAPIError as dbapi_error:
            # Unique constraint violation for the deduplication ID
            if dbapi_error.orig.sqlstate == "23505":  # type: ignore
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

    def _record_get_result_txn(
        self,
        conn: sa.Connection,
        workflow_uuid: str,
        function_id: int,
        output: Optional[str],
        error: Optional[str],
        child_workflow_id: str,
    ) -> None:
        """Record get result using PostgreSQL insert operations."""
        sql = (
            pg.insert(SystemSchema.operation_outputs)
            .values(
                workflow_uuid=workflow_uuid,
                function_id=function_id,
                function_name="DBOS.getResult",
                output=output,
                error=error,
                child_workflow_id=child_workflow_id,
            )
            .on_conflict_do_nothing()
        )
        conn.execute(sql)

    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation in PostgreSQL."""
        return dbapi_error.orig.sqlstate == "23505"  # type: ignore

    def _is_foreign_key_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a foreign key violation in PostgreSQL."""
        return dbapi_error.orig.sqlstate == "23503"  # type: ignore

    def _set_event_txn(
        self, conn: sa.Connection, workflow_uuid: str, key: str, value: str
    ) -> None:
        """Set event using PostgreSQL upsert operations."""
        conn.execute(
            pg.insert(SystemSchema.workflow_events)
            .values(
                workflow_uuid=workflow_uuid,
                key=key,
                value=value,
            )
            .on_conflict_do_update(
                index_elements=["workflow_uuid", "key"],
                set_={"value": value},
            )
        )

    def _notification_listener(self) -> None:
        """Listen for PostgreSQL notifications using psycopg."""
        while self._run_background_processes:
            try:
                # since we're using the psycopg connection directly, we need a url without the "+psycopg" suffix
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
                            if notify.payload:
                                condition = self.notifications_map.get(notify.payload)
                                if condition is None:
                                    # No condition found for this payload
                                    continue
                                condition.acquire()
                                condition.notify_all()
                                condition.release()
                                dbos_logger.debug(
                                    f"Signaled notifications condition for {notify.payload}"
                                )
                        elif channel == "dbos_workflow_events_channel":
                            if notify.payload:
                                condition = self.workflow_events_map.get(notify.payload)
                                if condition is None:
                                    # No condition found for this payload
                                    continue
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
                    dbos_logger.warning(f"Notification listener error: {e}")
                    time.sleep(1)
                    # Then the loop will try to reconnect and restart the listener
            finally:
                if self.notification_conn is not None:
                    self.notification_conn.close()
