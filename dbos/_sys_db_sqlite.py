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
        """SQLite doesn't support real-time notifications."""
        # SQLite doesn't have LISTEN/NOTIFY, so this is a no-op
        # Real-time features would need to be implemented differently (polling, etc.)
        pass
