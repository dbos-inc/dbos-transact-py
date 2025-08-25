import time
from typing import Any, Dict, Optional

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

from dbos._migration import sqlite_migrations

from ._logger import dbos_logger
from ._sys_db import SystemDatabase, WorkflowStatuses, WorkflowStatusInternal


class SQLiteSystemDatabase(SystemDatabase):
    """SQLite-specific implementation of SystemDatabase."""

    def _create_engine(
        self, system_database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        """Create a SQLite engine."""
        # SQLite URLs should be sqlite:///path/to/database.db
        return sa.create_engine(system_database_url)

    def run_migrations(self) -> None:
        """Run SQLite-specific migrations."""
        if self._debug_mode:
            dbos_logger.warning("System database migrations are skipped in debug mode.")
            return

        with self.engine.begin() as conn:
            # Enable foreign keys for SQLite
            conn.execute(sa.text("PRAGMA foreign_keys = ON"))

            # Check if migrations table exists
            result = conn.execute(
                sa.text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='dbos_migrations'"
                )
            ).fetchone()

            if result is None:
                # Create migrations table
                conn.execute(
                    sa.text(
                        "CREATE TABLE dbos_migrations (version INTEGER NOT NULL PRIMARY KEY)"
                    )
                )
                last_applied = 0
            else:
                # Get current migration version
                version_result = conn.execute(
                    sa.text("SELECT version FROM dbos_migrations")
                ).fetchone()
                last_applied = version_result[0] if version_result else 0

            # Apply migrations starting from the next version
            for i, migration_sql in enumerate(sqlite_migrations, 1):
                if i <= last_applied:
                    continue

                # Execute the migration
                dbos_logger.info(
                    f"Applying DBOS SQLite system database schema migration {i}"
                )

                # SQLite only allows one statement at a time, so split by semicolon
                statements = [
                    stmt.strip() for stmt in migration_sql.split(";") if stmt.strip()
                ]
                for statement in statements:
                    conn.execute(sa.text(statement))

                # Update the single row with the new version
                if last_applied == 0:
                    conn.execute(
                        sa.text(
                            "INSERT INTO dbos_migrations (version) VALUES (:version)"
                        ),
                        {"version": i},
                    )
                else:
                    conn.execute(
                        sa.text("UPDATE dbos_migrations SET version = :version"),
                        {"version": i},
                    )
                last_applied = i

    def _cleanup_connections(self) -> None:
        """Clean up SQLite-specific connections."""
        # SQLite doesn't require special connection cleanup
        pass

    def _insert_workflow_status(
        self,
        status: WorkflowStatusInternal,
        conn: sa.Connection,
        *,
        max_recovery_attempts: Optional[int],
    ) -> tuple[WorkflowStatuses, Optional[int]]:
        """Insert or update workflow status using SQLite upsert operations."""
        if self._debug_mode:
            raise Exception("called insert_workflow_status in debug mode")
        wf_status: WorkflowStatuses = status["status"]
        workflow_deadline_epoch_ms: Optional[int] = status["workflow_deadline_epoch_ms"]

        # Use SQLite's UPSERT syntax (INSERT ... ON CONFLICT ... DO UPDATE)
        upsert_sql = sa.text(
            """
            INSERT INTO workflow_status (
                workflow_uuid, status, name, class_name, config_name, output, error,
                executor_id, application_version, application_id, authenticated_user,
                authenticated_roles, assumed_role, queue_name, recovery_attempts,
                workflow_timeout_ms, workflow_deadline_epoch_ms, deduplication_id,
                priority, inputs, created_at, updated_at
            ) VALUES (
                :workflow_uuid, :status, :name, :class_name, :config_name, :output, :error,
                :executor_id, :application_version, :application_id, :authenticated_user,
                :authenticated_roles, :assumed_role, :queue_name, :recovery_attempts,
                :workflow_timeout_ms, :workflow_deadline_epoch_ms, :deduplication_id,
                :priority, :inputs, :created_at, :updated_at
            )
            ON CONFLICT(workflow_uuid) DO UPDATE SET
                recovery_attempts = CASE 
                    WHEN excluded.status != 'ENQUEUED' THEN workflow_status.recovery_attempts + 1
                    ELSE workflow_status.recovery_attempts
                END,
                updated_at = strftime('%s','now') * 1000,
                executor_id = CASE 
                    WHEN excluded.status != 'ENQUEUED' THEN excluded.executor_id
                    ELSE workflow_status.executor_id
                END
            RETURNING recovery_attempts, status, workflow_deadline_epoch_ms, name, 
                     class_name, config_name, queue_name
        """
        )

        try:
            results = conn.execute(
                upsert_sql,
                {
                    "workflow_uuid": status["workflow_uuid"],
                    "status": status["status"],
                    "name": status["name"],
                    "class_name": status["class_name"],
                    "config_name": status["config_name"],
                    "output": status["output"],
                    "error": status["error"],
                    "executor_id": status["executor_id"],
                    "application_version": status["app_version"],
                    "application_id": status["app_id"],
                    "authenticated_user": status["authenticated_user"],
                    "authenticated_roles": status["authenticated_roles"],
                    "assumed_role": status["assumed_role"],
                    "queue_name": status["queue_name"],
                    "recovery_attempts": 1 if wf_status != "ENQUEUED" else 0,
                    "workflow_timeout_ms": status["workflow_timeout_ms"],
                    "workflow_deadline_epoch_ms": status["workflow_deadline_epoch_ms"],
                    "deduplication_id": status["deduplication_id"],
                    "priority": status["priority"],
                    "inputs": status["inputs"],
                    "created_at": int(time.time() * 1000),
                    "updated_at": int(time.time() * 1000),
                },
            )
        except DBAPIError as dbapi_error:
            # Handle unique constraint violation for deduplication ID
            if "UNIQUE constraint failed" in str(
                dbapi_error.orig
            ) and "queue_name" in str(dbapi_error.orig):
                assert status["deduplication_id"] is not None
                assert status["queue_name"] is not None
                from ._error import DBOSQueueDeduplicatedError

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
                dbos_logger.warning(
                    f"Workflow already exists in queue: {row[6]}, but the provided queue name is: {status['queue_name']}. The queue is not updated."
                )
            if err_msg is not None:
                from ._error import DBOSConflictingWorkflowError

                raise DBOSConflictingWorkflowError(status["workflow_uuid"], err_msg)

            # Check for max recovery attempts exceeded
            if (
                (wf_status != "SUCCESS" and wf_status != "ERROR")
                and max_recovery_attempts is not None
                and recovery_attempts > max_recovery_attempts + 1
            ):
                dlq_sql = sa.text(
                    """
                    UPDATE workflow_status 
                    SET status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED',
                        deduplication_id = NULL,
                        started_at_epoch_ms = NULL,
                        queue_name = NULL
                    WHERE workflow_uuid = :workflow_uuid AND status = 'PENDING'
                """
                )
                conn.execute(dlq_sql, {"workflow_uuid": status["workflow_uuid"]})
                conn.commit()
                from ._error import MaxRecoveryAttemptsExceededError

                raise MaxRecoveryAttemptsExceededError(
                    status["workflow_uuid"], max_recovery_attempts
                )

        return wf_status, workflow_deadline_epoch_ms

    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation in SQLite."""
        # SQLite UNIQUE constraint error
        return "UNIQUE constraint failed" in str(dbapi_error.orig)

    def _is_foreign_key_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a foreign key violation in SQLite."""
        # SQLite FOREIGN KEY constraint error
        return "FOREIGN KEY constraint failed" in str(dbapi_error.orig)

    def _record_get_result_txn(
        self,
        conn: sa.Connection,
        workflow_uuid: str,
        function_id: int,
        output: Optional[str],
        error: Optional[str],
        child_workflow_id: str,
    ) -> None:
        """Record get result using SQLite upsert operations."""
        # Use SQLite's INSERT OR IGNORE syntax for "ON CONFLICT DO NOTHING" behavior
        sql = sa.text(
            """
            INSERT OR IGNORE INTO operation_outputs (
                workflow_uuid, function_id, function_name, output, error, child_workflow_id
            ) VALUES (
                :workflow_uuid, :function_id, 'DBOS.getResult', :output, :error, :child_workflow_id
            )
        """
        )
        conn.execute(
            sql,
            {
                "workflow_uuid": workflow_uuid,
                "function_id": function_id,
                "output": output,
                "error": error,
                "child_workflow_id": child_workflow_id,
            },
        )

    def _set_event_txn(
        self, conn: sa.Connection, workflow_uuid: str, key: str, value: str
    ) -> None:
        """Set event using SQLite upsert operations."""
        # Use SQLite's INSERT ... ON CONFLICT DO UPDATE for upsert behavior
        sql = sa.text(
            """
            INSERT INTO workflow_events (workflow_uuid, key, value)
            VALUES (:workflow_uuid, :key, :value)
            ON CONFLICT(workflow_uuid, key) DO UPDATE SET
                value = excluded.value
        """
        )
        conn.execute(
            sql,
            {
                "workflow_uuid": workflow_uuid,
                "key": key,
                "value": value,
            },
        )

    def _notification_listener(self) -> None:
        """SQLite doesn't support real-time notifications."""
        # SQLite doesn't have LISTEN/NOTIFY, so this is a no-op
        # Real-time features would need to be implemented differently (polling, etc.)
        pass
