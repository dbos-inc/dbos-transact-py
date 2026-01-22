import os
import time
from typing import Any, Dict, Optional, Tuple

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.exc import DBAPIError

from dbos._migration import sqlite_migrations

from ._logger import dbos_logger
from ._sys_db import SystemDatabase


class SQLiteSystemDatabase(SystemDatabase):
    """SQLite-specific implementation of SystemDatabase."""

    def _create_engine(
        self, system_database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        """Create a SQLite engine."""
        sqlite_kwargs = engine_kwargs.copy()
        connect_args = sqlite_kwargs.get("connect_args", {})
        if connect_args:
            filtered_keys = [
                k for k in connect_args if k in ("application_name", "connect_timeout")
            ]
            if filtered_keys:
                dbos_logger.debug(
                    f"Ignoring PostgreSQL-specific connect_args for SQLite: {filtered_keys}"
                )
            sqlite_connect_args = {
                k: v
                for k, v in connect_args.items()
                if k not in ("application_name", "connect_timeout")
            }
            sqlite_kwargs["connect_args"] = sqlite_connect_args
        engine = sa.create_engine(system_database_url, **sqlite_kwargs)

        # Use IMMEDIATE transactions to serialize writers and prevent race conditions
        @event.listens_for(engine, "connect")
        def set_sqlite_immediate(dbapi_conn: Any, connection_record: Any) -> None:
            dbapi_conn.isolation_level = "IMMEDIATE"

        return engine

    def run_migrations(self) -> None:
        """Run SQLite-specific migrations."""
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
        self._notification_listener_polling()
