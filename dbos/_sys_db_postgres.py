import time
from typing import Any, Dict, Optional

import psycopg
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

from dbos._migration import (
    ensure_dbos_schema,
    run_alembic_migrations,
    run_dbos_migrations,
)
from dbos._schemas.system_database import SystemSchema

from ._logger import dbos_logger
from ._sys_db import SystemDatabase


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
        # TODO: Make the schema dynamic so this isn't needed
        SystemSchema.workflow_status.schema = "dbos"
        SystemSchema.operation_outputs.schema = "dbos"
        SystemSchema.notifications.schema = "dbos"
        SystemSchema.workflow_events.schema = "dbos"
        SystemSchema.streams.schema = "dbos"
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

    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation in PostgreSQL."""
        return dbapi_error.orig.sqlstate == "23505"  # type: ignore

    def _is_foreign_key_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a foreign key violation in PostgreSQL."""
        return dbapi_error.orig.sqlstate == "23503"  # type: ignore

    @staticmethod
    def _reset_system_database(database_url: str) -> None:
        """Reset the PostgreSQL system database by dropping it."""
        system_db_url = sa.make_url(database_url)
        sysdb_name = system_db_url.database

        if sysdb_name is None:
            raise ValueError(f"System database name not found in URL {system_db_url}")

        try:
            # Connect to postgres default database
            engine = sa.create_engine(
                system_db_url.set(database="postgres", drivername="postgresql+psycopg"),
                connect_args={"connect_timeout": 10},
            )

            with engine.connect() as conn:
                # Set autocommit required for database dropping
                conn.execution_options(isolation_level="AUTOCOMMIT")

                # Drop the database
                conn.execute(
                    sa.text(f"DROP DATABASE IF EXISTS {sysdb_name} WITH (FORCE)")
                )
            engine.dispose()
        except Exception as e:
            dbos_logger.error(f"Error resetting PostgreSQL system database: {str(e)}")
            raise e

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
