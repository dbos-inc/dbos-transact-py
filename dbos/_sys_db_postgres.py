import time
from typing import Any, Dict, Optional, cast

import psycopg
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

from dbos._migration import ensure_dbos_schema, run_dbos_migrations

from ._logger import dbos_logger
from ._sys_db import SystemDatabase


class PostgresSystemDatabase(SystemDatabase):
    """PostgreSQL-specific implementation of SystemDatabase."""

    notification_conn: Optional[sa.PoolProxiedConnection] = None

    def _create_engine(
        self, system_database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        url = sa.make_url(system_database_url).set(drivername="postgresql+psycopg")
        return sa.create_engine(url, **engine_kwargs)

    def run_migrations(self) -> None:
        """Run PostgreSQL-specific migrations."""
        system_db_url = self.engine.url
        sysdb_name = system_db_url.database
        # Unless we were provided an engine, if the system database does not already exist, create it
        if self.created_engine:
            try:
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
                        conn.execute(sa.text(f'CREATE DATABASE "{sysdb_name}"'))
            except Exception:
                dbos_logger.warning(
                    f"Could not connect to postgres database to verify existence of {sysdb_name}. Continuing..."
                )
            finally:
                engine.dispose()
        else:
            # If we were provided an engine, validate it can connect
            with self.engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))

        assert self.schema
        ensure_dbos_schema(self.engine, self.schema)
        run_dbos_migrations(self.engine, self.schema, self.use_listen_notify)

    def _cleanup_connections(self) -> None:
        """Clean up PostgreSQL-specific connections."""
        with self._listener_thread_lock:
            if self.notification_conn and self.notification_conn.dbapi_connection:
                self.notification_conn.dbapi_connection.close()
                self.notification_conn.invalidate()

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
        if not self.use_listen_notify:
            return self._notification_listener_polling()
        while self._run_background_processes:
            try:
                with self._listener_thread_lock:
                    self.notification_conn = self.engine.raw_connection()
                    self.notification_conn.detach()
                    psycopg_conn = cast(
                        psycopg.connection.Connection, self.notification_conn
                    )
                    psycopg_conn.set_autocommit(True)

                    psycopg_conn.execute("LISTEN dbos_notifications_channel")
                    psycopg_conn.execute("LISTEN dbos_workflow_events_channel")
                while self._run_background_processes:
                    gen = psycopg_conn.notifies()
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
                self._cleanup_connections()
