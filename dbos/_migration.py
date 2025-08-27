import logging
import os
import re
import sys

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from ._logger import dbos_logger


def ensure_dbos_schema(engine: sa.Engine) -> bool:
    """
    True if using DBOS migrations (DBOS schema and migrations table already exist or were created)
    False if using Alembic migrations (DBOS schema exists, but dbos_migrations table doesn't)
    """
    with engine.begin() as conn:
        # Check if dbos schema exists
        schema_result = conn.execute(
            sa.text(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'dbos'"
            )
        )
        schema_existed = schema_result.fetchone() is not None

        # Create schema if it doesn't exist
        if not schema_existed:
            conn.execute(sa.text("CREATE SCHEMA dbos"))

        # Check if dbos_migrations table exists
        table_result = conn.execute(
            sa.text(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'dbos_migrations'"
            )
        )
        table_exists = table_result.fetchone() is not None

        if table_exists:
            return True
        elif schema_existed:
            return False
        else:
            conn.execute(
                sa.text(
                    "CREATE TABLE dbos.dbos_migrations (version BIGINT NOT NULL PRIMARY KEY)"
                )
            )
            return True


def run_alembic_migrations(engine: sa.Engine) -> None:
    """Run system database schema migrations with Alembic.
    This is DEPRECATED in favor of DBOS-managed migrations.
    It is retained only for backwards compatibility and
    will be removed in the next major version."""
    # Run a schema migration for the system database
    migration_dir = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "_alembic_migrations"
    )
    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", migration_dir)
    logging.getLogger("alembic").setLevel(logging.WARNING)
    # Alembic requires the % in URL-escaped parameters to itself be escaped to %%.
    escaped_conn_string = re.sub(
        r"%(?=[0-9A-Fa-f]{2})",
        "%%",
        engine.url.render_as_string(hide_password=False),
    )
    alembic_cfg.set_main_option("sqlalchemy.url", escaped_conn_string)
    try:
        command.upgrade(alembic_cfg, "head")
    except Exception as e:
        dbos_logger.warning(
            f"Exception during system database construction. This is most likely because the system database was configured using a later version of DBOS: {e}"
        )


def run_dbos_migrations(engine: sa.Engine) -> None:
    """Run DBOS-managed migrations by executing each SQL command in dbos_migrations."""
    with engine.begin() as conn:
        # Get current migration version
        result = conn.execute(sa.text("SELECT version FROM dbos.dbos_migrations"))
        current_version = result.fetchone()
        last_applied = current_version[0] if current_version else 0

        # Apply migrations starting from the next version
        for i, migration_sql in enumerate(dbos_migrations, 1):
            if i <= last_applied:
                continue

            # Execute the migration
            dbos_logger.info(f"Applying DBOS system database schema migration {i}")
            conn.execute(sa.text(migration_sql))

            # Update the single row with the new version
            if last_applied == 0:
                conn.execute(
                    sa.text(
                        "INSERT INTO dbos.dbos_migrations (version) VALUES (:version)"
                    ),
                    {"version": i},
                )
            else:
                conn.execute(
                    sa.text("UPDATE dbos.dbos_migrations SET version = :version"),
                    {"version": i},
                )
            last_applied = i


dbos_migration_one = """
-- Enable uuid extension for generating UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE dbos.workflow_status (
    workflow_uuid TEXT PRIMARY KEY,
    status TEXT,
    name TEXT,
    authenticated_user TEXT,
    assumed_role TEXT,
    authenticated_roles TEXT,
    request TEXT,
    output TEXT,
    error TEXT,
    executor_id TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
    application_version TEXT,
    application_id TEXT,
    class_name VARCHAR(255) DEFAULT NULL,
    config_name VARCHAR(255) DEFAULT NULL,
    recovery_attempts BIGINT DEFAULT 0,
    queue_name TEXT,
    workflow_timeout_ms BIGINT,
    workflow_deadline_epoch_ms BIGINT,
    inputs TEXT,
    started_at_epoch_ms BIGINT,
    deduplication_id TEXT,
    priority INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX workflow_status_created_at_index ON dbos.workflow_status (created_at);
CREATE INDEX workflow_status_executor_id_index ON dbos.workflow_status (executor_id);
CREATE INDEX workflow_status_status_index ON dbos.workflow_status (status);

ALTER TABLE dbos.workflow_status 
ADD CONSTRAINT uq_workflow_status_queue_name_dedup_id 
UNIQUE (queue_name, deduplication_id);

CREATE TABLE dbos.operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    child_workflow_id TEXT,
    PRIMARY KEY (workflow_uuid, function_id),
    FOREIGN KEY (workflow_uuid) REFERENCES dbos.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE dbos.notifications (
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
    message_uuid TEXT NOT NULL DEFAULT gen_random_uuid(), -- Built-in function
    FOREIGN KEY (destination_uuid) REFERENCES dbos.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX idx_workflow_topic ON dbos.notifications (destination_uuid, topic);

-- Create notification function
CREATE OR REPLACE FUNCTION dbos.notifications_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.destination_uuid || '::' || NEW.topic;
BEGIN
    PERFORM pg_notify('dbos_notifications_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create notification trigger
CREATE TRIGGER dbos_notifications_trigger
AFTER INSERT ON dbos.notifications
FOR EACH ROW EXECUTE FUNCTION dbos.notifications_function();

CREATE TABLE dbos.workflow_events (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, key),
    FOREIGN KEY (workflow_uuid) REFERENCES dbos.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

-- Create events function
CREATE OR REPLACE FUNCTION dbos.workflow_events_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.workflow_uuid || '::' || NEW.key;
BEGIN
    PERFORM pg_notify('dbos_workflow_events_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create events trigger
CREATE TRIGGER dbos_workflow_events_trigger
AFTER INSERT ON dbos.workflow_events
FOR EACH ROW EXECUTE FUNCTION dbos.workflow_events_function();

CREATE TABLE dbos.streams (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    "offset" INTEGER NOT NULL,
    PRIMARY KEY (workflow_uuid, key, "offset"),
    FOREIGN KEY (workflow_uuid) REFERENCES dbos.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE dbos.event_dispatch_kv (
    service_name TEXT NOT NULL,
    workflow_fn_name TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    update_seq NUMERIC(38,0),
    update_time NUMERIC(38,15),
    PRIMARY KEY (service_name, workflow_fn_name, key)
);
"""


def get_sqlite_timestamp_expr() -> str:
    """Get SQLite timestamp expression with millisecond precision for Python >= 3.12."""
    if sys.version_info >= (3, 12):
        return "(unixepoch('subsec') * 1000)"
    else:
        return "(strftime('%s','now') * 1000)"


sqlite_migration_one = f"""
CREATE TABLE workflow_status (
    workflow_uuid TEXT PRIMARY KEY,
    status TEXT,
    name TEXT,
    authenticated_user TEXT,
    assumed_role TEXT,
    authenticated_roles TEXT,
    request TEXT,
    output TEXT,
    error TEXT,
    executor_id TEXT,
    created_at INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()},
    updated_at INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()},
    application_version TEXT,
    application_id TEXT,
    class_name TEXT DEFAULT NULL,
    config_name TEXT DEFAULT NULL,
    recovery_attempts INTEGER DEFAULT 0,
    queue_name TEXT,
    workflow_timeout_ms INTEGER,
    workflow_deadline_epoch_ms INTEGER,
    inputs TEXT,
    started_at_epoch_ms INTEGER,
    deduplication_id TEXT,
    priority INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX workflow_status_created_at_index ON workflow_status (created_at);
CREATE INDEX workflow_status_executor_id_index ON workflow_status (executor_id);
CREATE INDEX workflow_status_status_index ON workflow_status (status);

CREATE UNIQUE INDEX uq_workflow_status_queue_name_dedup_id 
ON workflow_status (queue_name, deduplication_id);

CREATE TABLE operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    child_workflow_id TEXT,
    PRIMARY KEY (workflow_uuid, function_id),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE notifications (
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()},
    message_uuid TEXT NOT NULL DEFAULT (hex(randomblob(16))),
    FOREIGN KEY (destination_uuid) REFERENCES workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX idx_workflow_topic ON notifications (destination_uuid, topic);

CREATE TABLE workflow_events (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, key),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE streams (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    "offset" INTEGER NOT NULL,
    PRIMARY KEY (workflow_uuid, key, "offset"),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);
"""

dbos_migrations = [dbos_migration_one]
sqlite_migrations = [sqlite_migration_one]
