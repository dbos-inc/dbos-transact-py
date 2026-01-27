import sys

import sqlalchemy as sa

from ._logger import dbos_logger


def ensure_dbos_schema(engine: sa.Engine, schema: str) -> None:
    """
    True if using DBOS migrations (DBOS schema and migrations table already exist or were created)
    False if using Alembic migrations (DBOS schema exists, but dbos_migrations table doesn't)
    """
    with engine.begin() as conn:
        # Check if dbos schema exists
        schema_result = conn.execute(
            sa.text(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema"
            ),
            {"schema": schema},
        )
        schema_exists = schema_result.fetchone() is not None

        # Create schema if it doesn't exist
        if not schema_exists:
            conn.execute(sa.text(f'CREATE SCHEMA "{schema}"'))

        # Check if dbos_migrations table exists
        table_result = conn.execute(
            sa.text(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = :schema AND table_name = 'dbos_migrations'"
            ),
            {"schema": schema},
        )
        table_exists = table_result.fetchone() is not None

        if not table_exists:
            conn.execute(
                sa.text(
                    f'CREATE TABLE "{schema}".dbos_migrations (version BIGINT NOT NULL PRIMARY KEY)'
                )
            )


def run_dbos_migrations(
    engine: sa.Engine, schema: str, use_listen_notify: bool
) -> None:
    """Run DBOS-managed migrations by executing each SQL command in dbos_migrations."""
    with engine.begin() as conn:
        # Get current migration version
        result = conn.execute(
            sa.text(f'SELECT version FROM "{schema}".dbos_migrations')
        )
        current_version = result.fetchone()
        last_applied = current_version[0] if current_version else 0

        # Apply migrations starting from the next version
        migrations = get_dbos_migrations(schema, use_listen_notify)
        for i, migration_sql in enumerate(migrations, 1):
            if i <= last_applied:
                continue

            # Execute the migration
            dbos_logger.info(f"Applying DBOS system database schema migration {i}")
            conn.execute(sa.text(migration_sql))

            # Update the single row with the new version
            if last_applied == 0:
                conn.execute(
                    sa.text(
                        f'INSERT INTO "{schema}".dbos_migrations (version) VALUES (:version)'
                    ),
                    {"version": i},
                )
            else:
                conn.execute(
                    sa.text(
                        f'UPDATE "{schema}".dbos_migrations SET version = :version'
                    ),
                    {"version": i},
                )
            last_applied = i


def get_dbos_migration_one(schema: str, use_listen_notify: bool) -> str:
    migration = f"""
-- Enable uuid extension for generating UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE \"{schema}\".workflow_status (
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
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
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

CREATE INDEX workflow_status_created_at_index ON \"{schema}\".workflow_status (created_at);
CREATE INDEX workflow_status_executor_id_index ON \"{schema}\".workflow_status (executor_id);
CREATE INDEX workflow_status_status_index ON \"{schema}\".workflow_status (status);

ALTER TABLE \"{schema}\".workflow_status 
ADD CONSTRAINT uq_workflow_status_queue_name_dedup_id 
UNIQUE (queue_name, deduplication_id);

CREATE TABLE \"{schema}\".operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    child_workflow_id TEXT,
    PRIMARY KEY (workflow_uuid, function_id),
    FOREIGN KEY (workflow_uuid) REFERENCES \"{schema}\".workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE \"{schema}\".notifications (
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
    message_uuid TEXT NOT NULL DEFAULT gen_random_uuid(), -- Built-in function
    FOREIGN KEY (destination_uuid) REFERENCES \"{schema}\".workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX idx_workflow_topic ON \"{schema}\".notifications (destination_uuid, topic);

CREATE TABLE \"{schema}\".workflow_events (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, key),
    FOREIGN KEY (workflow_uuid) REFERENCES \"{schema}\".workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE \"{schema}\".streams (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    "offset" INTEGER NOT NULL,
    PRIMARY KEY (workflow_uuid, key, "offset"),
    FOREIGN KEY (workflow_uuid) REFERENCES \"{schema}\".workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE \"{schema}\".event_dispatch_kv (
    service_name TEXT NOT NULL,
    workflow_fn_name TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    update_seq NUMERIC(38,0),
    update_time NUMERIC(38,15),
    PRIMARY KEY (service_name, workflow_fn_name, key)
);
"""
    if use_listen_notify:
        migration += f"""
-- Create notification function
CREATE OR REPLACE FUNCTION \"{schema}\".notifications_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.destination_uuid || '::' || NEW.topic;
BEGIN
    PERFORM pg_notify('dbos_notifications_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create notification trigger
CREATE TRIGGER dbos_notifications_trigger
AFTER INSERT ON \"{schema}\".notifications
FOR EACH ROW EXECUTE FUNCTION \"{schema}\".notifications_function();

-- Create events function
CREATE OR REPLACE FUNCTION \"{schema}\".workflow_events_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.workflow_uuid || '::' || NEW.key;
BEGIN
    PERFORM pg_notify('dbos_workflow_events_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create events trigger
CREATE TRIGGER dbos_workflow_events_trigger
AFTER INSERT ON \"{schema}\".workflow_events
FOR EACH ROW EXECUTE FUNCTION \"{schema}\".workflow_events_function();
"""
    return migration


def get_dbos_migration_two(schema: str) -> str:
    return f"""
ALTER TABLE \"{schema}\".workflow_status ADD COLUMN queue_partition_key TEXT;
"""


def get_dbos_migration_three(schema: str) -> str:
    return f"""
create index "idx_workflow_status_queue_status_started" on \"{schema}\"."workflow_status" ("queue_name", "status", "started_at_epoch_ms")
"""


def get_dbos_migration_four(schema: str) -> str:
    return f"""
ALTER TABLE \"{schema}\".workflow_status ADD COLUMN forked_from TEXT;
CREATE INDEX "idx_workflow_status_forked_from" ON \"{schema}\"."workflow_status" ("forked_from")
"""


def get_dbos_migration_five(schema: str) -> str:
    return f"""
ALTER TABLE \"{schema}\".operation_outputs ADD COLUMN started_at_epoch_ms BIGINT, ADD COLUMN completed_at_epoch_ms BIGINT;
"""


def get_dbos_migration_six(schema: str) -> str:
    return f"""
CREATE TABLE \"{schema}\".workflow_events_history (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, function_id, key),
    FOREIGN KEY (workflow_uuid) REFERENCES \"{schema}\".workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE \"{schema}\".streams ADD COLUMN function_id INTEGER NOT NULL DEFAULT 0;
"""


def get_dbos_migration_seven(schema: str) -> str:
    return f"""ALTER TABLE "{schema}"."workflow_status" ADD COLUMN "owner_xid" TEXT DEFAULT NULL;"""


def get_dbos_migration_eight(schema: str) -> str:
    return f"""
ALTER TABLE "{schema}"."workflow_status" ADD COLUMN "parent_workflow_id" TEXT DEFAULT NULL;
CREATE INDEX "idx_workflow_status_parent_workflow_id" ON "{schema}"."workflow_status" ("parent_workflow_id");
"""


def get_dbos_migrations(schema: str, use_listen_notify: bool) -> list[str]:
    return [
        get_dbos_migration_one(schema, use_listen_notify),
        get_dbos_migration_two(schema),
        get_dbos_migration_three(schema),
        get_dbos_migration_four(schema),
        get_dbos_migration_five(schema),
        get_dbos_migration_six(schema),
        get_dbos_migration_seven(schema),
        get_dbos_migration_eight(schema),
    ]


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

sqlite_migration_two = """
ALTER TABLE workflow_status ADD COLUMN queue_partition_key TEXT;
"""

sqlite_migration_three = """
CREATE INDEX "idx_workflow_status_queue_status_started"
ON "workflow_status" ("queue_name", "status", "started_at_epoch_ms")
"""

sqlite_migration_four = """
ALTER TABLE workflow_status ADD COLUMN forked_from TEXT;
CREATE INDEX "idx_workflow_status_forked_from" ON "workflow_status" ("forked_from")
"""

sqlite_migration_five = """
ALTER TABLE operation_outputs ADD COLUMN started_at_epoch_ms BIGINT;
ALTER TABLE operation_outputs ADD COLUMN completed_at_epoch_ms BIGINT;
"""

sqlite_migration_six = """
CREATE TABLE workflow_events_history (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, function_id, key),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE streams ADD COLUMN function_id INTEGER NOT NULL DEFAULT 0;
"""

sqlite_migration_seven = (
    """ALTER TABLE workflow_status ADD COLUMN "owner_xid" TEXT DEFAULT NULL;"""
)

sqlite_migration_eight = """
ALTER TABLE workflow_status ADD COLUMN "parent_workflow_id" TEXT DEFAULT NULL;
CREATE INDEX "idx_workflow_status_parent_workflow_id" ON "workflow_status" ("parent_workflow_id");
"""

sqlite_migrations = [
    sqlite_migration_one,
    sqlite_migration_two,
    sqlite_migration_three,
    sqlite_migration_four,
    sqlite_migration_five,
    sqlite_migration_six,
    sqlite_migration_seven,
    sqlite_migration_eight,
]
