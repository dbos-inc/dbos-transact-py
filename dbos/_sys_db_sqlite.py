import os
import time
from typing import Any, Dict, Optional, Tuple

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
        # SQLite doesn't require special connection cleanup
        pass

    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation in SQLite."""
        return "UNIQUE constraint failed" in str(dbapi_error.orig)

    def _is_foreign_key_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a foreign key violation in SQLite."""
        return "FOREIGN KEY constraint failed" in str(dbapi_error.orig)

    @staticmethod
    def _reset_system_database(database_url: str) -> None:
        """Reset the SQLite system database by deleting the database file."""

        # Parse the SQLite database URL to get the file path
        url = sa.make_url(database_url)
        db_path = url.database

        if db_path is None:
            raise ValueError(f"System database path not found in URL {url}")

        try:
            if os.path.exists(db_path):
                os.remove(db_path)
                dbos_logger.info(f"Deleted SQLite database file: {db_path}")
            else:
                dbos_logger.info(f"SQLite database file does not exist: {db_path}")
        except OSError as e:
            dbos_logger.error(
                f"Error deleting SQLite database file {db_path}: {str(e)}"
            )
            raise e

    def _notification_listener(self) -> None:
        """Poll for notifications and workflow events in SQLite."""

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
                            sa.text(
                                "SELECT 1 FROM notifications WHERE destination_uuid = :dest_uuid AND topic = :topic LIMIT 1"
                            ),
                            {"dest_uuid": dest_uuid, "topic": topic},
                        )
                        if result.fetchone():
                            signal_condition(self.notifications_map, payload)

                # Check all payloads in the workflow_events_map
                for payload in list(self.workflow_events_map._dict.keys()):
                    workflow_uuid, key = split_payload(payload)
                    with self.engine.begin() as conn:
                        result = conn.execute(
                            sa.text(
                                "SELECT 1 FROM workflow_events WHERE workflow_uuid = :workflow_uuid AND key = :key LIMIT 1"
                            ),
                            {"workflow_uuid": workflow_uuid, "key": key},
                        )
                        if result.fetchone():
                            signal_condition(self.workflow_events_map, payload)

            except Exception as e:
                if self._run_background_processes:
                    dbos_logger.warning(f"SQLite notification poller error: {e}")
                    time.sleep(1)
