import time
from typing import Any, Dict, Optional, cast

try:
    import psycopg
except ImportError:  # optional: only the psycopg driver and LISTEN/NOTIFY use it
    psycopg = None  # type: ignore[assignment]
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import DBAPIError

from dbos._migration import ensure_dbos_schema, run_dbos_migrations, should_migrate
from dbos._pg_errors import get_sqlstate

from ._logger import dbos_logger
from ._schemas.system_database import SystemSchema
from ._sys_db import (
    SystemDatabase,
    _dbos_notifications_channel,
    _dbos_streams_channel,
    _dbos_workflow_events_channel,
)


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
        # Skip the advisory lock and migration work entirely if the schema
        # and dbos_migrations table already exist and are at the latest version.
        if not should_migrate(self.engine, self.schema, self.use_listen_notify):
            return

        # Use an advisory lock to serialize concurrent migrations.
        # Try to acquire the lock for up to 30 seconds. If we can't, log a
        # warning and proceed to run migrations without it, rather than
        # hanging startup indefinitely.
        MIGRATION_LOCK_ID = 1234567890
        MIGRATION_LOCK_TIMEOUT_SEC = 30
        locked = False
        # Use AUTOCOMMIT so the lock connection does not sit idle-in-transaction
        # while migrations run. Session-level advisory locks are independent of
        # transaction state, so AUTOCOMMIT is safe.
        with self.engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            try:
                deadline = time.monotonic() + MIGRATION_LOCK_TIMEOUT_SEC
                while True:
                    got = conn.execute(
                        sa.text("SELECT pg_try_advisory_lock(:lock_id)"),
                        {"lock_id": MIGRATION_LOCK_ID},
                    ).scalar()
                    if got:
                        locked = True
                        break
                    if time.monotonic() >= deadline:
                        dbos_logger.warning(
                            f"Could not acquire migration advisory lock within {MIGRATION_LOCK_TIMEOUT_SEC}s. "
                            f"Attempting migrations without lock."
                        )
                        break
                    time.sleep(1)
                ensure_dbos_schema(self.engine, self.schema)
                run_dbos_migrations(self.engine, self.schema, self.use_listen_notify)
            finally:
                if locked:
                    conn.execute(
                        sa.text("SELECT pg_advisory_unlock(:lock_id)"),
                        {"lock_id": MIGRATION_LOCK_ID},
                    )

    def _cleanup_connections(self) -> None:
        """Clean up PostgreSQL-specific connections."""
        with self._listener_thread_lock:
            if self.notification_conn and self.notification_conn.dbapi_connection:
                self.notification_conn.dbapi_connection.close()
                self.notification_conn.invalidate()

    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation in PostgreSQL."""
        return get_sqlstate(dbapi_error.orig) == "23505"

    def _attributes_contains_clause(
        self, attributes: Dict[str, Any]
    ) -> sa.ColumnElement[bool]:
        # Coerce to JSONB so contains() renders the @> containment operator,
        # which is served by the GIN index on the attributes column. The
        # column's generic JSON type would render contains() as a string LIKE.
        return sa.type_coerce(
            SystemSchema.workflow_status.c.attributes, JSONB
        ).contains(attributes)

    def _is_foreign_key_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a foreign key violation in PostgreSQL."""
        return get_sqlstate(dbapi_error.orig) == "23503"

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

                    psycopg_conn.execute(f"LISTEN {_dbos_notifications_channel}")
                    psycopg_conn.execute(f"LISTEN {_dbos_workflow_events_channel}")
                    psycopg_conn.execute(f"LISTEN {_dbos_streams_channel}")
                while self._run_background_processes:
                    gen = psycopg_conn.notifies()
                    for notify in gen:
                        channel = notify.channel
                        dbos_logger.debug(
                            f"Received notification on channel: {channel}, payload: {notify.payload}"
                        )
                        if channel == _dbos_notifications_channel:
                            if notify.payload:
                                event = self.notifications_map.get(notify.payload)
                                if event is None:
                                    continue
                                event.set()
                                dbos_logger.debug(
                                    f"Signaled notifications event for {notify.payload}"
                                )
                        elif channel == _dbos_workflow_events_channel:
                            if notify.payload:
                                event = self.workflow_events_map.get(notify.payload)
                                if event is None:
                                    continue
                                event.set()
                                dbos_logger.debug(
                                    f"Signaled workflow_events event for {notify.payload}"
                                )
                        elif channel == _dbos_streams_channel:
                            if notify.payload:
                                event = self.streams_map.get(notify.payload)
                                if event is None:
                                    continue
                                event.set()
                                dbos_logger.debug(
                                    f"Signaled streams event for {notify.payload}"
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

    def _signal_notification(self, channel: str, payload: str) -> None:
        """Accumulate a coalesced wakeup on `channel` for `payload`; run_notifier pushes it off the write path (no-op without L/N)."""
        if not self.use_listen_notify:
            return
        with self._notifier_lock:
            self._pending_notifications.setdefault(channel, set()).add(payload)

    def run_notifier(self) -> None:
        """Periodically flush coalesced notifications across all channels, keeping the async-notify queue lock (which serializes notifying commits) off the write path."""
        if not self.use_listen_notify:
            return
        while self._run_background_processes:
            try:
                time.sleep(self._notification_coalesce_sec)
                self._flush_notifications()
            except Exception as e:
                # Last resort: a failing flush logs and drops its batch internally, so this catches only unexpected errors, which must not kill the sole push path.
                if self._run_background_processes:
                    dbos_logger.warning(f"Notifier error: {e}")
                    time.sleep(1)
        # Final flush so values written just before shutdown still wake readers promptly.
        try:
            self._flush_notifications()
        except Exception as e:
            dbos_logger.warning(f"Notifier final flush error: {e}")

    def _flush_notifications(self) -> None:
        """Emit one coalesced notifying transaction per channel for all pending payloads; drop a channel's batch if it fails."""
        with self._notifier_lock:
            if not any(self._pending_notifications.values()):
                return
            batches = self._pending_notifications
            self._pending_notifications = {}
        # One transaction per channel so an unsendable payload on one channel drops only its own batch.
        for channel, batch in batches.items():
            if not batch:
                continue
            try:
                # One statement, one transaction: one round trip and one acquisition of the async-notify queue lock; unnest emits one notification per payload.
                with self.engine.begin() as c:
                    c.execute(
                        sa.text(
                            "SELECT pg_notify(:channel, p) FROM unnest(CAST(:payloads AS text[])) AS p"
                        ),
                        {"channel": channel, "payloads": list(batch)},
                    )
            except Exception as e:
                # Drop the batch (do not requeue) on failure, e.g. a payload over pg_notify's 8000-byte limit; readers' polling fallback delivers these values, so one poison payload can't stall the notifier forever.
                dbos_logger.warning(f"Notifier flush error on {channel}: {e}")
