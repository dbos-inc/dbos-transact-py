import time
from typing import Any, Dict

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

from dbos._migration import sqlite_migrations
from dbos._schemas.system_database import SystemSchema

from ._logger import dbos_logger
from ._sys_db import SystemDatabase


class SQLiteSystemDatabase(SystemDatabase):
    """SQLite-specific implementation of SystemDatabase."""

    def _create_engine(
        self, system_database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        """Create a SQLite engine."""
        # TODO: Make the schema dynamic so this isn't needed
        SystemSchema.workflow_status.schema = None
        SystemSchema.operation_outputs.schema = None
        SystemSchema.notifications.schema = None
        SystemSchema.workflow_events.schema = None
        SystemSchema.streams.schema = None
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

    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation in SQLite."""
        # SQLite UNIQUE constraint error
        return "UNIQUE constraint failed" in str(dbapi_error.orig)

    def _is_foreign_key_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a foreign key violation in SQLite."""
        # SQLite FOREIGN KEY constraint error
        return "FOREIGN KEY constraint failed" in str(dbapi_error.orig)

    def _notification_listener(self) -> None:
        """Poll for notifications and workflow events in SQLite."""
        while self._run_background_processes:
            try:
                # Poll every second
                time.sleep(1)

                # Check all payloads in the notifications_map
                for payload in list(self.notifications_map._dict.keys()):
                    # Check if this notification exists in the database
                    with self.engine.begin() as conn:
                        result = conn.execute(
                            sa.text(
                                "SELECT 1 FROM notifications "
                                "WHERE destination_uuid = :dest_uuid AND topic = :topic "
                                "LIMIT 1"
                            ),
                            {
                                "dest_uuid": (
                                    payload.split("::")[0]
                                    if "::" in payload
                                    else payload
                                ),
                                "topic": (
                                    payload.split("::")[1] if "::" in payload else None
                                ),
                            },
                        )
                        if result.fetchone():
                            # Signal the condition variable
                            condition = self.notifications_map.get(payload)
                            if condition:
                                condition.acquire()
                                condition.notify_all()
                                condition.release()
                                dbos_logger.debug(
                                    f"Signaled notifications condition for {payload}"
                                )

                # Check all payloads in the workflow_events_map
                for payload in list(self.workflow_events_map._dict.keys()):
                    # Check if this workflow event exists in the database
                    with self.engine.begin() as conn:
                        result = conn.execute(
                            sa.text(
                                "SELECT 1 FROM workflow_events "
                                "WHERE workflow_uuid = :workflow_uuid AND key = :key "
                                "LIMIT 1"
                            ),
                            {
                                "workflow_uuid": (
                                    payload.split("::")[0]
                                    if "::" in payload
                                    else payload
                                ),
                                "key": (
                                    payload.split("::")[1] if "::" in payload else None
                                ),
                            },
                        )
                        if result.fetchone():
                            # Signal the condition variable
                            condition = self.workflow_events_map.get(payload)
                            if condition:
                                condition.acquire()
                                condition.notify_all()
                                condition.release()
                                dbos_logger.debug(
                                    f"Signaled workflow_events condition for {payload}"
                                )

            except Exception as e:
                if self._run_background_processes:
                    dbos_logger.warning(f"SQLite notification poller error: {e}")
                    time.sleep(1)
