import sys

import sqlalchemy as sa

from ._logger import dbos_logger
from ._utils import quote_identifier

# Migration versions that contain CONCURRENTLY index DDL and must run with
# autocommit (CREATE/DROP INDEX CONCURRENTLY cannot run inside a transaction
# block on Postgres). On CockroachDB, schema changes are inherently online,
# so this set is ignored and the regular transactional path is used.
_ONLINE_MIGRATIONS = {
    22,
    23,
    24,
    25,
    26,
    27,
    29,
    30,
    31,
    32,
    34,
    35,
    37,
    45,
    46,
    47,
    107,
    111,
}

# From this index on, every SDK defines the same migration at the same index.
SHARED_MIGRATION_BASE = 100


def _pad_to_shared_base(migrations: list[str]) -> list[str]:
    """Pad a language's own history out to SHARED_MIGRATION_BASE - 1. Earlier
    indices stay per-language, safe to skip only because the schemas converge."""
    return migrations + [""] * (SHARED_MIGRATION_BASE - 1 - len(migrations))


def _concurrently(is_cockroach: bool) -> str:
    """Render the CONCURRENTLY keyword for online index DDL.

    Empty on CockroachDB, where schema changes are online by default and the
    keyword is not supported."""
    return "" if is_cockroach else "CONCURRENTLY"


def _cleanup_invalid_indexes(engine: sa.Engine, schema: str) -> None:
    """Drop indexes left in an INVALID state by a prior failed CONCURRENTLY run.

    A failed CREATE INDEX CONCURRENTLY leaves an index marked invalid that
    will not be used by the planner but blocks recreating the same name.
    Must be called before retrying an online migration."""
    with engine.connect() as raw_conn:
        conn = raw_conn.execution_options(isolation_level="AUTOCOMMIT")
        rows = conn.execute(
            sa.text(
                "SELECT i.relname FROM pg_index ix "
                "JOIN pg_class i ON i.oid = ix.indexrelid "
                "JOIN pg_class t ON t.oid = ix.indrelid "
                "JOIN pg_namespace n ON n.oid = t.relnamespace "
                "WHERE NOT ix.indisvalid AND n.nspname = :schema"
            ),
            {"schema": schema},
        ).fetchall()
        for (idx_name,) in rows:
            dbos_logger.warning(
                f"Dropping invalid index {schema}.{idx_name} left by a prior failed migration"
            )
            conn.execute(
                sa.text(
                    f"DROP INDEX CONCURRENTLY IF EXISTS {quote_identifier(schema)}.{quote_identifier(idx_name)}"
                )
            )


def _bump_migration_version(
    engine: sa.Engine, schema: str, version: int, last_applied: int
) -> None:
    """Update the dbos_migrations version row in its own transaction."""
    quoted_schema = quote_identifier(schema)
    with engine.begin() as conn:
        if last_applied == 0:
            conn.execute(
                sa.text(
                    f"INSERT INTO {quoted_schema}.dbos_migrations (version) VALUES (:version)"
                ),
                {"version": version},
            )
        else:
            conn.execute(
                sa.text(
                    f"UPDATE {quoted_schema}.dbos_migrations SET version = :version"
                ),
                {"version": version},
            )


def get_migration_versions(
    engine: sa.Engine, schema: str, use_listen_notify: bool
) -> tuple[int, int]:
    """The (recorded, latest) migration versions of this system database. Postgres-only.

    A missing schema or dbos_migrations table reads as version 0.
    """
    with engine.begin() as conn:
        version_str = conn.execute(sa.text("SELECT version()")).scalar() or ""
        is_cockroach = "cockroachdb" in version_str.lower()
        latest_version = len(
            get_dbos_migrations(schema, use_listen_notify, is_cockroach)
        )

        table_exists = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_name = 'dbos_migrations'"
            ),
            {"schema": schema},
        ).fetchone()
        if table_exists is None:
            return 0, latest_version

        current_version_row = conn.execute(
            sa.text(f"SELECT version FROM {quote_identifier(schema)}.dbos_migrations")
        ).fetchone()
        current_version = current_version_row[0] if current_version_row else 0
        return current_version, latest_version


def should_migrate(engine: sa.Engine, schema: str, use_listen_notify: bool) -> bool:
    """Return True if the schema or dbos_migrations table is missing, or if
    the recorded migration version is behind the latest. Postgres-only."""
    current_version, latest_version = get_migration_versions(
        engine, schema, use_listen_notify
    )
    return current_version < latest_version


def get_sqlite_migration_versions(engine: sa.Engine) -> tuple[int, int]:
    """The (recorded, latest) migration versions of this SQLite system database.

    A missing dbos_migrations table reads as version 0.
    """
    latest_version = len(sqlite_migrations)
    with engine.begin() as conn:
        table_exists = conn.execute(
            sa.text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='dbos_migrations'"
            )
        ).fetchone()
        if table_exists is None:
            return 0, latest_version

        current_version_row = conn.execute(
            sa.text("SELECT version FROM dbos_migrations")
        ).fetchone()
        current_version = current_version_row[0] if current_version_row else 0
        return current_version, latest_version


def ensure_dbos_schema(engine: sa.Engine, schema: str) -> None:
    """
    True if using DBOS migrations (DBOS schema and migrations table already exist or were created)
    False if using Alembic migrations (DBOS schema exists, but dbos_migrations table doesn't)
    """
    quoted_schema = quote_identifier(schema)
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
            conn.execute(sa.text(f"CREATE SCHEMA {quoted_schema}"))

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
                    f"CREATE TABLE {quoted_schema}.dbos_migrations (version BIGINT NOT NULL PRIMARY KEY)"
                )
            )


def run_dbos_migrations(
    engine: sa.Engine, schema: str, use_listen_notify: bool
) -> None:
    """Run DBOS-managed migrations by executing each SQL command in dbos_migrations."""
    quoted_schema = quote_identifier(schema)
    # Get current migration version and detect CockroachDB via server version string
    with engine.begin() as conn:
        result = conn.execute(
            sa.text(f"SELECT version FROM {quoted_schema}.dbos_migrations")
        )
        current_version = result.fetchone()
        last_applied = current_version[0] if current_version else 0

        version_str = conn.execute(sa.text("SELECT version()")).scalar() or ""
        is_cockroach = "cockroachdb" in version_str.lower()

    # Apply each migration in its own transaction (or autocommit for online ones)
    migrations = get_dbos_migrations(schema, use_listen_notify, is_cockroach)
    for i, migration_sql in enumerate(migrations, 1):
        if i <= last_applied:
            continue

        # Renumbering left long runs of empty migrations; skip them without a round trip.
        if not migration_sql.strip():
            continue

        dbos_logger.info(f"Applying DBOS system database schema migration {i}")

        # Online migrations contain CONCURRENTLY index DDL and must run with
        # autocommit. On CockroachDB, schema changes are inherently online, so
        # we use the regular transactional path.
        if i in _ONLINE_MIGRATIONS and not is_cockroach:
            # Clean up any invalid indexes left by a prior failed attempt at
            # this or a later online migration before retrying.
            _cleanup_invalid_indexes(engine, schema)

            with engine.connect() as raw_conn:
                conn = raw_conn.execution_options(isolation_level="AUTOCOMMIT")
                conn.execute(sa.text(migration_sql))

            _bump_migration_version(engine, schema, i, last_applied)
            last_applied = i
            continue

        with engine.begin() as conn:
            # Migration 10 adds a primary key to the notifications table.
            # Skip it if the table already has one.
            if (
                i == 10
                and conn.execute(
                    sa.text(
                        "SELECT 1 FROM information_schema.table_constraints "
                        "WHERE table_schema = :schema "
                        "AND table_name = 'notifications' "
                        "AND constraint_type = 'PRIMARY KEY'"
                    ),
                    {"schema": schema},
                ).scalar()
            ):
                dbos_logger.info("Migration 10 skipped, primary key already exists")
            else:
                conn.execute(sa.text(migration_sql))

            # Update the single row with the new version
            if last_applied == 0:
                conn.execute(
                    sa.text(
                        f"INSERT INTO {quoted_schema}.dbos_migrations (version) VALUES (:version)"
                    ),
                    {"version": i},
                )
            else:
                conn.execute(
                    sa.text(
                        f"UPDATE {quoted_schema}.dbos_migrations SET version = :version"
                    ),
                    {"version": i},
                )
            last_applied = i

    # Empty migrations at the end still count as applied, so record them in one write.
    if len(migrations) > last_applied:
        _bump_migration_version(engine, schema, len(migrations), last_applied)


def get_dbos_migration_one(quoted_schema: str, use_listen_notify: bool) -> str:
    migration = f"""
-- Enable uuid extension for generating UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE {quoted_schema}.workflow_status (
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
    priority INT4 NOT NULL DEFAULT 0
);

CREATE INDEX workflow_status_created_at_index ON {quoted_schema}.workflow_status (created_at);
CREATE INDEX workflow_status_executor_id_index ON {quoted_schema}.workflow_status (executor_id);
CREATE INDEX workflow_status_status_index ON {quoted_schema}.workflow_status (status);

ALTER TABLE {quoted_schema}.workflow_status 
ADD CONSTRAINT uq_workflow_status_queue_name_dedup_id 
UNIQUE (queue_name, deduplication_id);

CREATE TABLE {quoted_schema}.operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INT4 NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    child_workflow_id TEXT,
    PRIMARY KEY (workflow_uuid, function_id),
    FOREIGN KEY (workflow_uuid) REFERENCES {quoted_schema}.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE {quoted_schema}.notifications (
    message_uuid TEXT NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY, -- Built-in function
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
    FOREIGN KEY (destination_uuid) REFERENCES {quoted_schema}.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX idx_workflow_topic ON {quoted_schema}.notifications (destination_uuid, topic);

CREATE TABLE {quoted_schema}.workflow_events (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, key),
    FOREIGN KEY (workflow_uuid) REFERENCES {quoted_schema}.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE {quoted_schema}.streams (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    "offset" INT4 NOT NULL,
    PRIMARY KEY (workflow_uuid, key, "offset"),
    FOREIGN KEY (workflow_uuid) REFERENCES {quoted_schema}.workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE {quoted_schema}.event_dispatch_kv (
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
CREATE OR REPLACE FUNCTION {quoted_schema}.notifications_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.destination_uuid || '::' || NEW.topic;
BEGIN
    PERFORM pg_notify('dbos_notifications_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create notification trigger
CREATE TRIGGER dbos_notifications_trigger
AFTER INSERT ON {quoted_schema}.notifications
FOR EACH ROW EXECUTE FUNCTION {quoted_schema}.notifications_function();

-- Create events function
CREATE OR REPLACE FUNCTION {quoted_schema}.workflow_events_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.workflow_uuid || '::' || NEW.key;
BEGIN
    PERFORM pg_notify('dbos_workflow_events_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create events trigger
CREATE TRIGGER dbos_workflow_events_trigger
AFTER INSERT ON {quoted_schema}.workflow_events
FOR EACH ROW EXECUTE FUNCTION {quoted_schema}.workflow_events_function();
"""
    return migration


def get_dbos_migration_two(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}.workflow_status ADD COLUMN queue_partition_key TEXT;
"""


def get_dbos_migration_three(quoted_schema: str) -> str:
    return f"""
create index "idx_workflow_status_queue_status_started" on {quoted_schema}."workflow_status" ("queue_name", "status", "started_at_epoch_ms")
"""


def get_dbos_migration_four(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}.workflow_status ADD COLUMN forked_from TEXT;
CREATE INDEX "idx_workflow_status_forked_from" ON {quoted_schema}."workflow_status" ("forked_from")
"""


def get_dbos_migration_five(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}.operation_outputs ADD COLUMN started_at_epoch_ms BIGINT, ADD COLUMN completed_at_epoch_ms BIGINT;
"""


def get_dbos_migration_six(quoted_schema: str) -> str:
    return f"""
CREATE TABLE {quoted_schema}.workflow_events_history (
    workflow_uuid TEXT NOT NULL,
    function_id INT4 NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, function_id, key),
    FOREIGN KEY (workflow_uuid) REFERENCES {quoted_schema}.workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE {quoted_schema}.streams ADD COLUMN function_id INT4 NOT NULL DEFAULT 0;
"""


def get_dbos_migration_seven(quoted_schema: str) -> str:
    return f"""ALTER TABLE {quoted_schema}."workflow_status" ADD COLUMN "owner_xid" TEXT DEFAULT NULL;"""


def get_dbos_migration_eight(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}."workflow_status" ADD COLUMN "parent_workflow_id" TEXT DEFAULT NULL;
CREATE INDEX "idx_workflow_status_parent_workflow_id" ON {quoted_schema}."workflow_status" ("parent_workflow_id");
"""


def get_dbos_migration_nine(quoted_schema: str) -> str:
    return f"""
CREATE TABLE {quoted_schema}.workflow_schedules (
    schedule_id TEXT PRIMARY KEY,
    schedule_name TEXT NOT NULL UNIQUE,
    workflow_name TEXT NOT NULL,
    workflow_class_name TEXT,
    schedule TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    context TEXT NOT NULL
);
"""


# An earlier version of DBOS had a bug where this table was created without a primary key.
# The initial migration has been changed to create a key, and this migration creates the key
# for existing applications.
def get_dbos_migration_ten(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}.notifications ADD PRIMARY KEY (message_uuid);
"""


def get_dbos_migration_eleven(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}."workflow_status" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE {quoted_schema}."notifications" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE {quoted_schema}."workflow_events" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE {quoted_schema}."workflow_events_history" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE {quoted_schema}."operation_outputs" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE {quoted_schema}."streams" ADD COLUMN "serialization" TEXT DEFAULT NULL;
"""


def get_dbos_migration_twelve(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}."notifications" ADD COLUMN "consumed" BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX "idx_notifications" ON {quoted_schema}."notifications" ("destination_uuid", "topic");
"""


def get_dbos_migration_thirteen(quoted_schema: str) -> str:
    return f"""
CREATE TABLE {quoted_schema}.application_versions (
    version_id TEXT NOT NULL PRIMARY KEY,
    version_name TEXT NOT NULL UNIQUE,
    version_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);
"""


def get_dbos_migration_fourteen(quoted_schema: str) -> str:
    return f"""
CREATE FUNCTION {quoted_schema}.enqueue_workflow(
    workflow_name TEXT,
    queue_name TEXT,
    positional_args JSON[] DEFAULT ARRAY[]::JSON[],
    named_args JSON DEFAULT '{{}}'::JSON,
    class_name TEXT DEFAULT NULL,
    config_name TEXT DEFAULT NULL,
    workflow_id TEXT DEFAULT NULL,
    app_version TEXT DEFAULT NULL,
    timeout_ms BIGINT DEFAULT NULL,
    deadline_epoch_ms BIGINT DEFAULT NULL,
    deduplication_id TEXT DEFAULT NULL,
    priority INTEGER DEFAULT NULL,
    queue_partition_key TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    v_workflow_id TEXT;
    v_serialized_inputs TEXT;
    v_owner_xid TEXT;
    v_now BIGINT;
    v_recovery_attempts INTEGER := 0;
    v_priority INTEGER;
BEGIN

    -- Validate required parameters
    IF workflow_name IS NULL OR workflow_name = '' THEN
        RAISE EXCEPTION 'Workflow name cannot be null or empty';
    END IF;
    IF queue_name IS NULL OR queue_name = '' THEN
        RAISE EXCEPTION 'Queue name cannot be null or empty';
    END IF;
    IF named_args IS NOT NULL AND jsonb_typeof(named_args::jsonb) != 'object' THEN
        RAISE EXCEPTION 'Named args must be a JSON object';
    END IF;
    IF workflow_id IS NOT NULL AND workflow_id = '' THEN
        RAISE EXCEPTION 'Workflow ID cannot be an empty string if provided.';
    END IF;

    v_workflow_id := COALESCE(workflow_id, gen_random_uuid()::TEXT);
    v_owner_xid := gen_random_uuid()::TEXT;
    v_priority := COALESCE(priority, 0);
    v_serialized_inputs := json_build_object(
        'positionalArgs', positional_args,
        'namedArgs', named_args
    )::TEXT;
    v_now := EXTRACT(epoch FROM now()) * 1000;

    INSERT INTO {quoted_schema}.workflow_status (
        workflow_uuid, status, inputs,
        name, class_name, config_name,
        queue_name, deduplication_id, priority, queue_partition_key,
        application_version,
        created_at, updated_at, recovery_attempts,
        workflow_timeout_ms, workflow_deadline_epoch_ms,
        parent_workflow_id, owner_xid, serialization
    ) VALUES (
        v_workflow_id, 'ENQUEUED', v_serialized_inputs,
        workflow_name, class_name, config_name,
        queue_name, deduplication_id, v_priority, queue_partition_key,
        app_version,
        v_now, v_now, v_recovery_attempts,
        timeout_ms, deadline_epoch_ms,
        NULL, v_owner_xid, 'portable_json'
    )
    ON CONFLICT (workflow_uuid)
    DO UPDATE SET
        updated_at = EXCLUDED.updated_at;

    RETURN v_workflow_id;

EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'DBOS queue duplicated'
            USING DETAIL = format('Workflow %s with queue %s and deduplication ID %s already exists', v_workflow_id, queue_name, deduplication_id),
                ERRCODE = 'unique_violation';
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION {quoted_schema}.send_message(
    destination_id TEXT,
    message JSON,
    topic TEXT DEFAULT NULL,
    message_id TEXT DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    v_topic TEXT := COALESCE(topic, '__null__topic__');
    v_message_id TEXT := COALESCE(message_id, gen_random_uuid()::TEXT);
BEGIN
    INSERT INTO {quoted_schema}.notifications (
        destination_uuid, topic, message, message_uuid, serialization
    ) VALUES (
        destination_id, v_topic, message, v_message_id, 'portable_json'
    )
    ON CONFLICT (message_uuid) DO NOTHING;
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE EXCEPTION 'DBOS non-existent workflow'
            USING DETAIL = format('Destination workflow %s does not exist', destination_id),
                ERRCODE = 'foreign_key_violation';
END;
$$ LANGUAGE plpgsql;
"""


def get_dbos_migration_fifteen(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}.workflow_schedules ADD COLUMN "last_fired_at" TEXT DEFAULT NULL;
ALTER TABLE {quoted_schema}.workflow_schedules ADD COLUMN "automatic_backfill" BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE {quoted_schema}.workflow_schedules ADD COLUMN "cron_timezone" TEXT DEFAULT NULL;
"""


def get_dbos_migration_sixteen(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}."workflow_status" ADD COLUMN "delay_until_epoch_ms" BIGINT DEFAULT NULL;
CREATE INDEX "idx_workflow_status_delayed" ON {quoted_schema}."workflow_status" ("delay_until_epoch_ms") WHERE status = 'DELAYED';
"""


def get_dbos_migration_seventeen(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}.workflow_schedules ADD COLUMN "queue_name" TEXT DEFAULT NULL;
"""


def get_dbos_migration_eighteen(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}."workflow_status" ADD COLUMN "was_forked_from" BOOLEAN NOT NULL DEFAULT FALSE;
"""


def get_dbos_migration_nineteen(quoted_schema: str) -> str:
    return f"""
CREATE INDEX "idx_operation_outputs_completed_at_function_name" ON {quoted_schema}."operation_outputs" ("completed_at_epoch_ms", "function_name");
"""


def get_dbos_migration_twenty(
    quoted_schema: str, use_listen_notify: bool, is_cockroach: bool
) -> str:
    if is_cockroach:
        return ""
    migration = f"""
ALTER FUNCTION {quoted_schema}.enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INTEGER, TEXT
) SET search_path = pg_catalog, pg_temp;

ALTER FUNCTION {quoted_schema}.send_message(
    TEXT, JSON, TEXT, TEXT
) SET search_path = pg_catalog, pg_temp;
"""
    if use_listen_notify:
        migration += f"""
ALTER FUNCTION {quoted_schema}.notifications_function() SET search_path = pg_catalog, pg_temp;
ALTER FUNCTION {quoted_schema}.workflow_events_function() SET search_path = pg_catalog, pg_temp;
"""
    return migration


def get_dbos_migration_twentyone(quoted_schema: str) -> str:
    return f"""
CREATE TABLE {quoted_schema}.queues (
    queue_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
    name TEXT NOT NULL UNIQUE,
    concurrency INT4,
    worker_concurrency INT4,
    rate_limit_max INT4,
    rate_limit_period_sec DOUBLE PRECISION,
    priority_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    partition_queue BOOLEAN NOT NULL DEFAULT FALSE,
    polling_interval_sec DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);
"""


def get_dbos_migration_twentytwo(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'DROP INDEX {c} IF EXISTS {quoted_schema}."idx_workflow_status_forked_from"'


def get_dbos_migration_twentythree(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} IF NOT EXISTS "idx_workflow_status_forked_from" ON {quoted_schema}."workflow_status" ("forked_from") WHERE "forked_from" IS NOT NULL'


def get_dbos_migration_twentyfour(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'DROP INDEX {c} IF EXISTS {quoted_schema}."idx_workflow_status_parent_workflow_id"'


def get_dbos_migration_twentyfive(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} IF NOT EXISTS "idx_workflow_status_parent_workflow_id" ON {quoted_schema}."workflow_status" ("parent_workflow_id") WHERE "parent_workflow_id" IS NOT NULL'


def get_dbos_migration_twentysix(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return (
        f'DROP INDEX {c} IF EXISTS {quoted_schema}."workflow_status_executor_id_index"'
    )


def get_dbos_migration_twentyseven(quoted_schema: str, is_cockroach: bool) -> str:
    # The new partial unique index uses a different name from the original
    # constraint to avoid a naming collision
    c = _concurrently(is_cockroach)
    return f'CREATE UNIQUE INDEX {c} IF NOT EXISTS "uq_workflow_status_dedup_id" ON {quoted_schema}."workflow_status" ("queue_name", "deduplication_id") WHERE "deduplication_id" IS NOT NULL'


def get_dbos_migration_twentyeight(quoted_schema: str, is_cockroach: bool) -> str:
    # CockroachDB implements unique constraints as indexes and rejects
    # ALTER TABLE DROP CONSTRAINT for them; Postgres rejects DROP INDEX on a
    # constraint-backed index. Both paths are fast catalog operations, no
    # CONCURRENTLY needed.
    if is_cockroach:
        return f'DROP INDEX IF EXISTS {quoted_schema}."uq_workflow_status_queue_name_dedup_id" CASCADE'
    return f"ALTER TABLE {quoted_schema}.workflow_status DROP CONSTRAINT IF EXISTS uq_workflow_status_queue_name_dedup_id"


def get_dbos_migration_twentynine(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} IF NOT EXISTS "idx_workflow_status_pending" ON {quoted_schema}."workflow_status" ("created_at") WHERE "status" = \'PENDING\''


def get_dbos_migration_thirty(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} IF NOT EXISTS "idx_workflow_status_failed" ON {quoted_schema}."workflow_status" ("status", "created_at") WHERE "status" IN (\'ERROR\', \'CANCELLED\', \'MAX_RECOVERY_ATTEMPTS_EXCEEDED\')'


def get_dbos_migration_thirtyone(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'DROP INDEX {c} IF EXISTS {quoted_schema}."workflow_status_status_index"'


def get_dbos_migration_thirtytwo(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} IF NOT EXISTS "idx_workflow_status_in_flight" ON {quoted_schema}."workflow_status" ("queue_name", "status", "priority", "created_at") WHERE "status" IN (\'ENQUEUED\', \'PENDING\')'


def get_dbos_migration_thirtythree(quoted_schema: str) -> str:
    # ALTER TABLE ADD COLUMN with constant default is fast (catalog-only update via attmissingval).
    return f'ALTER TABLE {quoted_schema}."workflow_status" ADD COLUMN IF NOT EXISTS "rate_limited" BOOLEAN NOT NULL DEFAULT FALSE'


def get_dbos_migration_thirtyfour(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} IF NOT EXISTS "idx_workflow_status_rate_limited" ON {quoted_schema}."workflow_status" ("queue_name", "started_at_epoch_ms") WHERE "rate_limited" = TRUE'


def get_dbos_migration_thirtyfive(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'DROP INDEX {c} IF EXISTS {quoted_schema}."idx_workflow_status_queue_status_started"'


def get_dbos_migration_thirtysix(quoted_schema: str) -> str:
    # ADD COLUMN with no default is catalog-only; the partial index built in
    # the same transaction covers zero rows, so no CONCURRENTLY is needed.
    return f"""
ALTER TABLE {quoted_schema}."workflow_status" ADD COLUMN IF NOT EXISTS "completed_at" BIGINT;
CREATE INDEX IF NOT EXISTS "idx_workflow_status_completed_at" ON {quoted_schema}."workflow_status" ("completed_at") WHERE "completed_at" IS NOT NULL;
"""


def get_dbos_migration_thirtyseven(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} IF NOT EXISTS "idx_workflow_status_started_at" ON {quoted_schema}."workflow_status" ("started_at_epoch_ms") WHERE "started_at_epoch_ms" IS NOT NULL'


def get_dbos_migration_thirtyeight(quoted_schema: str, is_cockroach: bool) -> str:
    migration = f"""
DROP FUNCTION IF EXISTS {quoted_schema}.enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INTEGER, TEXT
);

CREATE OR REPLACE FUNCTION {quoted_schema}.enqueue_workflow(
    workflow_name TEXT,
    queue_name TEXT,
    positional_args JSON[] DEFAULT ARRAY[]::JSON[],
    named_args JSON DEFAULT '{{}}'::JSON,
    class_name TEXT DEFAULT NULL,
    config_name TEXT DEFAULT NULL,
    workflow_id TEXT DEFAULT NULL,
    app_version TEXT DEFAULT NULL,
    timeout_ms BIGINT DEFAULT NULL,
    deadline_epoch_ms BIGINT DEFAULT NULL,
    deduplication_id TEXT DEFAULT NULL,
    priority INT4 DEFAULT NULL,
    queue_partition_key TEXT DEFAULT NULL,
    authenticated_user TEXT DEFAULT NULL,
    authenticated_roles TEXT DEFAULT NULL,
    delay_until_epoch_ms BIGINT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    v_workflow_id TEXT;
    v_serialized_inputs TEXT;
    v_owner_xid TEXT;
    v_now BIGINT;
    v_recovery_attempts INT4 := 0;
    v_priority INT4;
    v_status TEXT;
BEGIN

    -- Validate required parameters
    IF workflow_name IS NULL OR workflow_name = '' THEN
        RAISE EXCEPTION 'Workflow name cannot be null or empty';
    END IF;
    IF queue_name IS NULL OR queue_name = '' THEN
        RAISE EXCEPTION 'Queue name cannot be null or empty';
    END IF;
    IF named_args IS NOT NULL AND jsonb_typeof(named_args::jsonb) != 'object' THEN
        RAISE EXCEPTION 'Named args must be a JSON object';
    END IF;
    IF workflow_id IS NOT NULL AND workflow_id = '' THEN
        RAISE EXCEPTION 'Workflow ID cannot be an empty string if provided.';
    END IF;
    IF delay_until_epoch_ms IS NOT NULL AND delay_until_epoch_ms < 0 THEN
        RAISE EXCEPTION 'delay_until_epoch_ms must be >= 0';
    END IF;

    v_workflow_id := COALESCE(workflow_id, gen_random_uuid()::TEXT);
    v_owner_xid := gen_random_uuid()::TEXT;
    v_priority := COALESCE(priority, 0);
    v_serialized_inputs := json_build_object(
        'positionalArgs', positional_args,
        'namedArgs', named_args
    )::TEXT;
    v_now := EXTRACT(epoch FROM now()) * 1000;
    v_status := CASE WHEN delay_until_epoch_ms IS NULL THEN 'ENQUEUED' ELSE 'DELAYED' END;

    INSERT INTO {quoted_schema}.workflow_status (
        workflow_uuid, status, inputs,
        name, class_name, config_name,
        queue_name, deduplication_id, priority, queue_partition_key,
        application_version,
        created_at, updated_at, recovery_attempts,
        workflow_timeout_ms, workflow_deadline_epoch_ms,
        parent_workflow_id, owner_xid, serialization,
        authenticated_user, authenticated_roles,
        delay_until_epoch_ms
    ) VALUES (
        v_workflow_id, v_status, v_serialized_inputs,
        workflow_name, class_name, config_name,
        queue_name, deduplication_id, v_priority, queue_partition_key,
        app_version,
        v_now, v_now, v_recovery_attempts,
        timeout_ms, deadline_epoch_ms,
        NULL, v_owner_xid, 'portable_json',
        authenticated_user, authenticated_roles,
        delay_until_epoch_ms
    )
    ON CONFLICT (workflow_uuid)
    DO UPDATE SET
        updated_at = EXCLUDED.updated_at;

    RETURN v_workflow_id;

EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'DBOS queue duplicated'
            USING DETAIL = format('Workflow %s with queue %s and deduplication ID %s already exists', v_workflow_id, queue_name, deduplication_id),
                ERRCODE = 'unique_violation';
END;
$$ LANGUAGE plpgsql;
"""
    if not is_cockroach:
        migration += f"""
ALTER FUNCTION {quoted_schema}.enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INT4, TEXT, TEXT, TEXT, BIGINT
) SET search_path = pg_catalog, pg_temp;
"""
    return migration


def get_dbos_migration_thirtynine(quoted_schema: str, use_listen_notify: bool) -> str:
    # Gated on use_listen_notify only, matching the notifications/workflow_events
    # triggers in migration one. Deployments without LISTEN/NOTIFY (e.g.
    # CockroachDB) set use_listen_notify=False and use the polling fallback.
    if not use_listen_notify:
        return ""
    return f"""
-- Create streams notification function
CREATE OR REPLACE FUNCTION {quoted_schema}.streams_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.workflow_uuid || '::' || NEW.key;
BEGIN
    PERFORM pg_notify('dbos_streams_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION {quoted_schema}.streams_function() SET search_path = pg_catalog, pg_temp;

-- Create streams trigger
DROP TRIGGER IF EXISTS dbos_streams_trigger ON {quoted_schema}.streams;
CREATE TRIGGER dbos_streams_trigger
AFTER INSERT ON {quoted_schema}.streams
FOR EACH ROW EXECUTE FUNCTION {quoted_schema}.streams_function();
"""


def get_dbos_migration_forty(quoted_schema: str) -> str:
    # ADD COLUMN with no default is catalog-only; the partial index built in
    # the same transaction covers zero rows, so no CONCURRENTLY is needed.
    # The index supports containment (@>) filters on workflow attributes; on
    # CockroachDB, USING GIN creates an inverted index.
    return f"""
ALTER TABLE {quoted_schema}."workflow_status" ADD COLUMN IF NOT EXISTS "attributes" JSONB;
CREATE INDEX IF NOT EXISTS "idx_workflow_status_attributes" ON {quoted_schema}."workflow_status" USING GIN ("attributes") WHERE "attributes" IS NOT NULL;
"""


def get_dbos_migration_fortyone(quoted_schema: str) -> str:
    # ADD COLUMN with no default is catalog-only; the partial index built in
    # the same transaction covers zero rows (no existing row has a non-NULL
    # schedule_name), so no CONCURRENTLY is needed. The index supports
    # filtering workflows by the named schedule that enqueued them.
    return f"""
ALTER TABLE {quoted_schema}."workflow_status" ADD COLUMN IF NOT EXISTS "schedule_name" TEXT;
CREATE INDEX IF NOT EXISTS "idx_workflow_status_schedule_name" ON {quoted_schema}."workflow_status" ("schedule_name") WHERE "schedule_name" IS NOT NULL;
"""


def get_dbos_migration_fortytwo(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}."workflow_status" ADD COLUMN IF NOT EXISTS "debounce_deadline_epoch_ms" BIGINT DEFAULT NULL;
ALTER TABLE {quoted_schema}."workflow_status" ADD COLUMN IF NOT EXISTS "is_debounced" BOOLEAN NOT NULL DEFAULT FALSE;
"""


def get_dbos_migration_fortythree(quoted_schema: str, use_listen_notify: bool) -> str:
    # Drop the streams NOTIFY trigger; stream writes are pushed by run_notifier off the write path.
    if not use_listen_notify:
        return ""
    return f"""
DROP TRIGGER IF EXISTS dbos_streams_trigger ON {quoted_schema}.streams;
DROP FUNCTION IF EXISTS {quoted_schema}.streams_function();
"""


def get_dbos_migration_fortyfour(quoted_schema: str, use_listen_notify: bool) -> str:
    # Drop the workflow_events NOTIFY trigger (events are pushed by run_notifier)
    if not use_listen_notify:
        return ""
    return f"""
DROP TRIGGER IF EXISTS dbos_workflow_events_trigger ON {quoted_schema}.workflow_events;
DROP FUNCTION IF EXISTS {quoted_schema}.workflow_events_function();
"""


def get_dbos_migration_fortyfive(quoted_schema: str, is_cockroach: bool) -> str:
    # Partitioned-queue dequeue index: extends idx_workflow_status_in_flight with
    # queue_partition_key so lookups scoped to one partition stay selective when
    # many partitions are active.
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} IF NOT EXISTS "idx_workflow_status_partition_dequeue" ON {quoted_schema}."workflow_status" ("queue_name", "status", "queue_partition_key", "priority", "created_at") WHERE "status" IN (\'ENQUEUED\', \'PENDING\') AND "queue_partition_key" IS NOT NULL'


def get_dbos_migration_fortysix(quoted_schema: str, is_cockroach: bool) -> str:
    # Trailing workflow ID totalizes the dequeue order
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} IF NOT EXISTS "idx_workflow_status_partition_dequeue_v2" ON {quoted_schema}."workflow_status" ("queue_name", "status", "queue_partition_key", "priority", "created_at", "workflow_uuid") WHERE "status" IN (\'ENQUEUED\', \'PENDING\') AND "queue_partition_key" IS NOT NULL'


def get_dbos_migration_fortyseven(quoted_schema: str, is_cockroach: bool) -> str:
    # Superseded by idx_workflow_status_partition_dequeue_v2
    c = _concurrently(is_cockroach)
    return f'DROP INDEX {c} IF EXISTS {quoted_schema}."idx_workflow_status_partition_dequeue"'


def get_dbos_migration_hundred(quoted_schema: str) -> str:
    # NULL means unclaimed: any application may read and claim the row. One table per migration, so a blocked table does not hold the others' locks.
    return f"""
ALTER TABLE {quoted_schema}."workflow_status" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
"""


def get_dbos_migration_hundredone(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}."queues" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
"""


def get_dbos_migration_hundredtwo(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}."workflow_schedules" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
"""


def get_dbos_migration_hundredthree(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}."application_versions" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
"""


def get_dbos_migration_hundredfour(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}."operation_outputs" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
"""


def get_dbos_migration_hundredfive(quoted_schema: str, is_cockroach: bool) -> str:
    # Callers omitting the trailing application_name enqueue an unclaimed workflow.
    migration = f"""
DROP FUNCTION IF EXISTS {quoted_schema}.enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INT4, TEXT, TEXT, TEXT, BIGINT
);

CREATE OR REPLACE FUNCTION {quoted_schema}.enqueue_workflow(
    workflow_name TEXT,
    queue_name TEXT,
    positional_args JSON[] DEFAULT ARRAY[]::JSON[],
    named_args JSON DEFAULT '{{}}'::JSON,
    class_name TEXT DEFAULT NULL,
    config_name TEXT DEFAULT NULL,
    workflow_id TEXT DEFAULT NULL,
    app_version TEXT DEFAULT NULL,
    timeout_ms BIGINT DEFAULT NULL,
    deadline_epoch_ms BIGINT DEFAULT NULL,
    deduplication_id TEXT DEFAULT NULL,
    priority INT4 DEFAULT NULL,
    queue_partition_key TEXT DEFAULT NULL,
    authenticated_user TEXT DEFAULT NULL,
    authenticated_roles TEXT DEFAULT NULL,
    delay_until_epoch_ms BIGINT DEFAULT NULL,
    application_name TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    v_workflow_id TEXT;
    v_serialized_inputs TEXT;
    v_owner_xid TEXT;
    v_now BIGINT;
    v_recovery_attempts INT4 := 0;
    v_priority INT4;
    v_status TEXT;
BEGIN

    -- Validate required parameters
    IF workflow_name IS NULL OR workflow_name = '' THEN
        RAISE EXCEPTION 'Workflow name cannot be null or empty';
    END IF;
    IF queue_name IS NULL OR queue_name = '' THEN
        RAISE EXCEPTION 'Queue name cannot be null or empty';
    END IF;
    IF named_args IS NOT NULL AND jsonb_typeof(named_args::jsonb) != 'object' THEN
        RAISE EXCEPTION 'Named args must be a JSON object';
    END IF;
    IF workflow_id IS NOT NULL AND workflow_id = '' THEN
        RAISE EXCEPTION 'Workflow ID cannot be an empty string if provided.';
    END IF;
    IF delay_until_epoch_ms IS NOT NULL AND delay_until_epoch_ms < 0 THEN
        RAISE EXCEPTION 'delay_until_epoch_ms must be >= 0';
    END IF;

    v_workflow_id := COALESCE(workflow_id, gen_random_uuid()::TEXT);
    v_owner_xid := gen_random_uuid()::TEXT;
    v_priority := COALESCE(priority, 0);
    v_serialized_inputs := json_build_object(
        'positionalArgs', positional_args,
        'namedArgs', named_args
    )::TEXT;
    v_now := EXTRACT(epoch FROM now()) * 1000;
    v_status := CASE WHEN delay_until_epoch_ms IS NULL THEN 'ENQUEUED' ELSE 'DELAYED' END;

    INSERT INTO {quoted_schema}.workflow_status (
        workflow_uuid, status, inputs,
        name, class_name, config_name,
        queue_name, deduplication_id, priority, queue_partition_key,
        application_version,
        created_at, updated_at, recovery_attempts,
        workflow_timeout_ms, workflow_deadline_epoch_ms,
        parent_workflow_id, owner_xid, serialization,
        authenticated_user, authenticated_roles,
        delay_until_epoch_ms, application_name
    ) VALUES (
        v_workflow_id, v_status, v_serialized_inputs,
        workflow_name, class_name, config_name,
        queue_name, deduplication_id, v_priority, queue_partition_key,
        app_version,
        v_now, v_now, v_recovery_attempts,
        timeout_ms, deadline_epoch_ms,
        NULL, v_owner_xid, 'portable_json',
        authenticated_user, authenticated_roles,
        delay_until_epoch_ms, application_name
    )
    ON CONFLICT (workflow_uuid)
    DO UPDATE SET
        updated_at = EXCLUDED.updated_at;

    RETURN v_workflow_id;

EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'DBOS queue duplicated'
            USING DETAIL = format('Workflow %s with queue %s and deduplication ID %s already exists', v_workflow_id, queue_name, deduplication_id),
                ERRCODE = 'unique_violation';
END;
$$ LANGUAGE plpgsql;
"""
    if not is_cockroach:
        migration += f"""
ALTER FUNCTION {quoted_schema}.enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INT4, TEXT, TEXT, TEXT, BIGINT, TEXT
) SET search_path = pg_catalog, pg_temp;
"""
    return migration


def get_dbos_migration_hundredsix(quoted_schema: str) -> str:
    # With 107, the key replacing version_name's retiring global uniqueness, unclaimed counting as its own owner. The constraint may not be dropped until every SDK reaching this database is past 107, which runs online because every unclaimed row matches its predicate.
    return f"""
CREATE UNIQUE INDEX IF NOT EXISTS "uq_application_versions_owner_version"
    ON {quoted_schema}."application_versions" ("application_name", "version_name")
    WHERE "application_name" IS NOT NULL;
"""


def get_dbos_migration_hundredseven(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f"""CREATE UNIQUE INDEX {c} IF NOT EXISTS "uq_application_versions_unclaimed_version"
    ON {quoted_schema}."application_versions" ("version_name")
    WHERE "application_name" IS NULL"""


def get_dbos_migration_hundredeight(quoted_schema: str) -> str:
    # Any of these being set partitions the queue; each applies per partition.
    return f"""
ALTER TABLE {quoted_schema}."queues" ADD COLUMN IF NOT EXISTS "partition_concurrency" INT4 DEFAULT NULL;
ALTER TABLE {quoted_schema}."queues" ADD COLUMN IF NOT EXISTS "partition_worker_concurrency" INT4 DEFAULT NULL;
ALTER TABLE {quoted_schema}."queues" ADD COLUMN IF NOT EXISTS "partition_rate_limit_max" INT4 DEFAULT NULL;
ALTER TABLE {quoted_schema}."queues" ADD COLUMN IF NOT EXISTS "partition_rate_limit_period_sec" DOUBLE PRECISION DEFAULT NULL;
"""


def get_dbos_migration_hundrednine(quoted_schema: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {quoted_schema}."workflow_input" (
    workflow_uuid TEXT NOT NULL PRIMARY KEY,
    inputs TEXT,
    retention_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);

CREATE TABLE IF NOT EXISTS {quoted_schema}."workflow_output" (
    workflow_uuid TEXT NOT NULL PRIMARY KEY,
    output TEXT,
    error TEXT,
    retention_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);

CREATE INDEX IF NOT EXISTS "idx_workflow_input_retention"
    ON {quoted_schema}."workflow_input" ("retention_timestamp");

CREATE INDEX IF NOT EXISTS "idx_workflow_output_retention"
    ON {quoted_schema}."workflow_output" ("retention_timestamp");
"""


def get_dbos_migration_hundredten(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}."operation_outputs"
    ADD COLUMN IF NOT EXISTS "retention_timestamp" BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint;
"""


def get_dbos_migration_hundredeleven(quoted_schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f"""CREATE INDEX {c} IF NOT EXISTS "idx_operation_outputs_retention"
    ON {quoted_schema}."operation_outputs" ("retention_timestamp")"""


def get_dbos_migration_hundredtwelve(quoted_schema: str) -> str:
    return f"""
ALTER TABLE {quoted_schema}."operation_outputs"
    DROP CONSTRAINT IF EXISTS "operation_outputs_workflow_uuid_foreign";

ALTER TABLE {quoted_schema}."operation_outputs"
    DROP CONSTRAINT IF EXISTS "operation_outputs_workflow_uuid_fkey";
"""


def get_dbos_migration_hundredthirteen(quoted_schema: str, is_cockroach: bool) -> str:
    migration = f"""
CREATE OR REPLACE FUNCTION {quoted_schema}.enqueue_workflow(
    workflow_name TEXT,
    queue_name TEXT,
    positional_args JSON[] DEFAULT ARRAY[]::JSON[],
    named_args JSON DEFAULT '{{}}'::JSON,
    class_name TEXT DEFAULT NULL,
    config_name TEXT DEFAULT NULL,
    workflow_id TEXT DEFAULT NULL,
    app_version TEXT DEFAULT NULL,
    timeout_ms BIGINT DEFAULT NULL,
    deadline_epoch_ms BIGINT DEFAULT NULL,
    deduplication_id TEXT DEFAULT NULL,
    priority INT4 DEFAULT NULL,
    queue_partition_key TEXT DEFAULT NULL,
    authenticated_user TEXT DEFAULT NULL,
    authenticated_roles TEXT DEFAULT NULL,
    delay_until_epoch_ms BIGINT DEFAULT NULL,
    application_name TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    v_workflow_id TEXT;
    v_serialized_inputs TEXT;
    v_owner_xid TEXT;
    v_now BIGINT;
    v_recovery_attempts INT4 := 0;
    v_priority INT4;
    v_status TEXT;
BEGIN

    -- Validate required parameters
    IF workflow_name IS NULL OR workflow_name = '' THEN
        RAISE EXCEPTION 'Workflow name cannot be null or empty';
    END IF;
    IF queue_name IS NULL OR queue_name = '' THEN
        RAISE EXCEPTION 'Queue name cannot be null or empty';
    END IF;
    IF named_args IS NOT NULL AND jsonb_typeof(named_args::jsonb) != 'object' THEN
        RAISE EXCEPTION 'Named args must be a JSON object';
    END IF;
    IF workflow_id IS NOT NULL AND workflow_id = '' THEN
        RAISE EXCEPTION 'Workflow ID cannot be an empty string if provided.';
    END IF;
    IF delay_until_epoch_ms IS NOT NULL AND delay_until_epoch_ms < 0 THEN
        RAISE EXCEPTION 'delay_until_epoch_ms must be >= 0';
    END IF;

    v_workflow_id := COALESCE(workflow_id, gen_random_uuid()::TEXT);
    v_owner_xid := gen_random_uuid()::TEXT;
    v_priority := COALESCE(priority, 0);
    v_serialized_inputs := json_build_object(
        'positionalArgs', positional_args,
        'namedArgs', named_args
    )::TEXT;
    v_now := EXTRACT(epoch FROM now()) * 1000;
    v_status := CASE WHEN delay_until_epoch_ms IS NULL THEN 'ENQUEUED' ELSE 'DELAYED' END;

    INSERT INTO {quoted_schema}.workflow_status (
        workflow_uuid, status, inputs,
        name, class_name, config_name,
        queue_name, deduplication_id, priority, queue_partition_key,
        application_version,
        created_at, updated_at, recovery_attempts,
        workflow_timeout_ms, workflow_deadline_epoch_ms,
        parent_workflow_id, owner_xid, serialization,
        authenticated_user, authenticated_roles,
        delay_until_epoch_ms, application_name
    ) VALUES (
        v_workflow_id, v_status, v_serialized_inputs,
        workflow_name, class_name, config_name,
        queue_name, deduplication_id, v_priority, queue_partition_key,
        app_version,
        v_now, v_now, v_recovery_attempts,
        timeout_ms, deadline_epoch_ms,
        NULL, v_owner_xid, 'portable_json',
        authenticated_user, authenticated_roles,
        delay_until_epoch_ms, application_name
    )
    ON CONFLICT (workflow_uuid)
    DO UPDATE SET
        updated_at = EXCLUDED.updated_at;

    INSERT INTO {quoted_schema}.workflow_input (
        workflow_uuid, inputs, retention_timestamp
    ) VALUES (
        v_workflow_id, v_serialized_inputs, v_now
    )
    ON CONFLICT (workflow_uuid) DO NOTHING;

    RETURN v_workflow_id;

EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'DBOS queue duplicated'
            USING DETAIL = format('Workflow %s with queue %s and deduplication ID %s already exists', v_workflow_id, queue_name, deduplication_id),
                ERRCODE = 'unique_violation';
END;
$$ LANGUAGE plpgsql;
"""
    if not is_cockroach:
        migration += f"""
ALTER FUNCTION {quoted_schema}.enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INT4, TEXT, TEXT, TEXT, BIGINT, TEXT
) SET search_path = pg_catalog, pg_temp;
"""
    return migration


def get_dbos_migrations(
    schema: str, use_listen_notify: bool, is_cockroach: bool = False
) -> list[str]:
    # Every migration interpolates the schema straight into DDL, so quote it once here.
    quoted_schema = quote_identifier(schema)
    history = [
        get_dbos_migration_one(quoted_schema, use_listen_notify),
        get_dbos_migration_two(quoted_schema),
        get_dbos_migration_three(quoted_schema),
        get_dbos_migration_four(quoted_schema),
        get_dbos_migration_five(quoted_schema),
        get_dbos_migration_six(quoted_schema),
        get_dbos_migration_seven(quoted_schema),
        get_dbos_migration_eight(quoted_schema),
        get_dbos_migration_nine(quoted_schema),
        get_dbos_migration_ten(quoted_schema),
        get_dbos_migration_eleven(quoted_schema),
        get_dbos_migration_twelve(quoted_schema),
        get_dbos_migration_thirteen(quoted_schema),
        get_dbos_migration_fourteen(quoted_schema),
        get_dbos_migration_fifteen(quoted_schema),
        get_dbos_migration_sixteen(quoted_schema),
        get_dbos_migration_seventeen(quoted_schema),
        get_dbos_migration_eighteen(quoted_schema),
        get_dbos_migration_nineteen(quoted_schema),
        get_dbos_migration_twenty(quoted_schema, use_listen_notify, is_cockroach),
        get_dbos_migration_twentyone(quoted_schema),
        get_dbos_migration_twentytwo(quoted_schema, is_cockroach),
        get_dbos_migration_twentythree(quoted_schema, is_cockroach),
        get_dbos_migration_twentyfour(quoted_schema, is_cockroach),
        get_dbos_migration_twentyfive(quoted_schema, is_cockroach),
        get_dbos_migration_twentysix(quoted_schema, is_cockroach),
        get_dbos_migration_twentyseven(quoted_schema, is_cockroach),
        get_dbos_migration_twentyeight(quoted_schema, is_cockroach),
        get_dbos_migration_twentynine(quoted_schema, is_cockroach),
        get_dbos_migration_thirty(quoted_schema, is_cockroach),
        get_dbos_migration_thirtyone(quoted_schema, is_cockroach),
        get_dbos_migration_thirtytwo(quoted_schema, is_cockroach),
        get_dbos_migration_thirtythree(quoted_schema),
        get_dbos_migration_thirtyfour(quoted_schema, is_cockroach),
        get_dbos_migration_thirtyfive(quoted_schema, is_cockroach),
        get_dbos_migration_thirtysix(quoted_schema),
        get_dbos_migration_thirtyseven(quoted_schema, is_cockroach),
        get_dbos_migration_thirtyeight(quoted_schema, is_cockroach),
        get_dbos_migration_thirtynine(quoted_schema, use_listen_notify),
        get_dbos_migration_forty(quoted_schema),
        get_dbos_migration_fortyone(quoted_schema),
        get_dbos_migration_fortytwo(quoted_schema),
        get_dbos_migration_fortythree(quoted_schema, use_listen_notify),
        get_dbos_migration_fortyfour(quoted_schema, use_listen_notify),
        get_dbos_migration_fortyfive(quoted_schema, is_cockroach),
        get_dbos_migration_fortysix(quoted_schema, is_cockroach),
        get_dbos_migration_fortyseven(quoted_schema, is_cockroach),
    ]
    return [
        *_pad_to_shared_base(history),
        get_dbos_migration_hundred(quoted_schema),
        get_dbos_migration_hundredone(quoted_schema),
        get_dbos_migration_hundredtwo(quoted_schema),
        get_dbos_migration_hundredthree(quoted_schema),
        get_dbos_migration_hundredfour(quoted_schema),
        get_dbos_migration_hundredfive(quoted_schema, is_cockroach),
        get_dbos_migration_hundredsix(quoted_schema),
        get_dbos_migration_hundredseven(quoted_schema, is_cockroach),
        get_dbos_migration_hundredeight(quoted_schema),
        get_dbos_migration_hundrednine(quoted_schema),
        get_dbos_migration_hundredten(quoted_schema),
        get_dbos_migration_hundredeleven(quoted_schema, is_cockroach),
        get_dbos_migration_hundredtwelve(quoted_schema),
        get_dbos_migration_hundredthirteen(quoted_schema, is_cockroach),
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
    message_uuid TEXT NOT NULL DEFAULT (hex(randomblob(16))) PRIMARY KEY,
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()},
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

sqlite_migration_nine = """
CREATE TABLE workflow_schedules (
    schedule_id TEXT PRIMARY KEY,
    schedule_name TEXT NOT NULL UNIQUE,
    workflow_name TEXT NOT NULL,
    workflow_class_name TEXT,
    schedule TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    context TEXT NOT NULL
);
"""

sqlite_migration_eleven = """
ALTER TABLE "workflow_status" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "notifications" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "workflow_events" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "workflow_events_history" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "operation_outputs" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "streams" ADD COLUMN "serialization" TEXT DEFAULT NULL;
"""


sqlite_migration_twelve = """
ALTER TABLE "notifications" ADD COLUMN "consumed" BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX "idx_notifications" ON "notifications" ("destination_uuid", "topic");
"""


sqlite_migration_thirteen = f"""
CREATE TABLE application_versions (
    version_id TEXT NOT NULL PRIMARY KEY,
    version_name TEXT NOT NULL UNIQUE,
    version_timestamp INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()},
    created_at INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()}
);
"""
sqlite_migration_fifteen = """
ALTER TABLE workflow_schedules ADD COLUMN "last_fired_at" TEXT DEFAULT NULL;
ALTER TABLE workflow_schedules ADD COLUMN "automatic_backfill" BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE workflow_schedules ADD COLUMN "cron_timezone" TEXT DEFAULT NULL;
"""

sqlite_migration_sixteen = """
ALTER TABLE workflow_status ADD COLUMN "delay_until_epoch_ms" BIGINT DEFAULT NULL;
CREATE INDEX "idx_workflow_status_delayed" ON "workflow_status" ("delay_until_epoch_ms") WHERE status = 'DELAYED';
"""

sqlite_migration_seventeen = """
ALTER TABLE workflow_schedules ADD COLUMN "queue_name" TEXT DEFAULT NULL;
"""

sqlite_migration_eighteen = """
ALTER TABLE workflow_status ADD COLUMN "was_forked_from" BOOLEAN NOT NULL DEFAULT FALSE;
"""

sqlite_migration_nineteen = """
CREATE INDEX "idx_operation_outputs_completed_at_function_name" ON "operation_outputs" ("completed_at_epoch_ms", "function_name");
"""

sqlite_migration_twentyone = f"""
CREATE TABLE queues (
    queue_id TEXT PRIMARY KEY DEFAULT (hex(randomblob(16))),
    name TEXT NOT NULL UNIQUE,
    concurrency INTEGER,
    worker_concurrency INTEGER,
    rate_limit_max INTEGER,
    rate_limit_period_sec REAL,
    priority_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    partition_queue BOOLEAN NOT NULL DEFAULT FALSE,
    polling_interval_sec REAL NOT NULL DEFAULT 1.0,
    created_at INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()},
    updated_at INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()}
);
"""

sqlite_migration_twentytwo = 'DROP INDEX IF EXISTS "idx_workflow_status_forked_from"'

sqlite_migration_twentythree = 'CREATE INDEX IF NOT EXISTS "idx_workflow_status_forked_from" ON "workflow_status" ("forked_from") WHERE "forked_from" IS NOT NULL'

sqlite_migration_twentyfour = (
    'DROP INDEX IF EXISTS "idx_workflow_status_parent_workflow_id"'
)

sqlite_migration_twentyfive = 'CREATE INDEX IF NOT EXISTS "idx_workflow_status_parent_workflow_id" ON "workflow_status" ("parent_workflow_id") WHERE "parent_workflow_id" IS NOT NULL'

sqlite_migration_twentysix = 'DROP INDEX IF EXISTS "workflow_status_executor_id_index"'

sqlite_migration_twentyseven = 'CREATE UNIQUE INDEX IF NOT EXISTS "uq_workflow_status_dedup_id" ON "workflow_status" ("queue_name", "deduplication_id") WHERE "deduplication_id" IS NOT NULL'

sqlite_migration_twentyeight = (
    'DROP INDEX IF EXISTS "uq_workflow_status_queue_name_dedup_id"'
)

sqlite_migration_twentynine = 'CREATE INDEX IF NOT EXISTS "idx_workflow_status_pending" ON "workflow_status" ("created_at") WHERE "status" = \'PENDING\''

sqlite_migration_thirty = 'CREATE INDEX IF NOT EXISTS "idx_workflow_status_failed" ON "workflow_status" ("status", "created_at") WHERE "status" IN (\'ERROR\', \'CANCELLED\', \'MAX_RECOVERY_ATTEMPTS_EXCEEDED\')'

sqlite_migration_thirtyone = 'DROP INDEX IF EXISTS "workflow_status_status_index"'

sqlite_migration_thirtytwo = 'CREATE INDEX IF NOT EXISTS "idx_workflow_status_in_flight" ON "workflow_status" ("queue_name", "status", "priority", "created_at") WHERE "status" IN (\'ENQUEUED\', \'PENDING\')'

sqlite_migration_thirtythree = 'ALTER TABLE "workflow_status" ADD COLUMN "rate_limited" BOOLEAN NOT NULL DEFAULT FALSE'

sqlite_migration_thirtyfour = 'CREATE INDEX IF NOT EXISTS "idx_workflow_status_rate_limited" ON "workflow_status" ("queue_name", "started_at_epoch_ms") WHERE "rate_limited" = TRUE'

sqlite_migration_thirtyfive = (
    'DROP INDEX IF EXISTS "idx_workflow_status_queue_status_started"'
)

sqlite_migration_thirtysix = """
ALTER TABLE workflow_status ADD COLUMN "completed_at" BIGINT;
CREATE INDEX IF NOT EXISTS "idx_workflow_status_completed_at" ON "workflow_status" ("completed_at") WHERE "completed_at" IS NOT NULL;
"""

sqlite_migration_thirtyseven = 'CREATE INDEX IF NOT EXISTS "idx_workflow_status_started_at" ON "workflow_status" ("started_at_epoch_ms") WHERE "started_at_epoch_ms" IS NOT NULL'

sqlite_migration_forty = 'ALTER TABLE workflow_status ADD COLUMN "attributes" TEXT'

sqlite_migration_fortyone = """
ALTER TABLE workflow_status ADD COLUMN "schedule_name" TEXT;
CREATE INDEX IF NOT EXISTS "idx_workflow_status_schedule_name" ON "workflow_status" ("schedule_name") WHERE "schedule_name" IS NOT NULL;
"""

sqlite_migration_fortytwo = """
ALTER TABLE workflow_status ADD COLUMN "debounce_deadline_epoch_ms" BIGINT DEFAULT NULL;
ALTER TABLE workflow_status ADD COLUMN "is_debounced" BOOLEAN NOT NULL DEFAULT FALSE;
"""

sqlite_migration_fortyfive = 'CREATE INDEX IF NOT EXISTS "idx_workflow_status_partition_dequeue" ON "workflow_status" ("queue_name", "status", "queue_partition_key", "priority", "created_at") WHERE "status" IN (\'ENQUEUED\', \'PENDING\') AND "queue_partition_key" IS NOT NULL'

# Trailing workflow ID totalizes the dequeue order
sqlite_migration_fortysix = 'CREATE INDEX IF NOT EXISTS "idx_workflow_status_partition_dequeue_v2" ON "workflow_status" ("queue_name", "status", "queue_partition_key", "priority", "created_at", "workflow_uuid") WHERE "status" IN (\'ENQUEUED\', \'PENDING\') AND "queue_partition_key" IS NOT NULL'

# Superseded by idx_workflow_status_partition_dequeue_v2
sqlite_migration_fortyseven = (
    'DROP INDEX IF EXISTS "idx_workflow_status_partition_dequeue"'
)

sqlite_migration_hundred = (
    'ALTER TABLE workflow_status ADD COLUMN "application_name" TEXT DEFAULT NULL'
)

sqlite_migration_hundredone = (
    'ALTER TABLE queues ADD COLUMN "application_name" TEXT DEFAULT NULL'
)

sqlite_migration_hundredtwo = (
    'ALTER TABLE workflow_schedules ADD COLUMN "application_name" TEXT DEFAULT NULL'
)

sqlite_migration_hundredthree = (
    'ALTER TABLE application_versions ADD COLUMN "application_name" TEXT DEFAULT NULL'
)

sqlite_migration_hundredfour = (
    'ALTER TABLE operation_outputs ADD COLUMN "application_name" TEXT DEFAULT NULL'
)

# See get_dbos_migration_hundredsix: the same key, built while the constraint still implies it.
sqlite_migration_hundredsix = """
CREATE UNIQUE INDEX IF NOT EXISTS "uq_application_versions_owner_version"
    ON application_versions ("application_name", "version_name")
    WHERE "application_name" IS NOT NULL;
"""

sqlite_migration_hundredseven = """
CREATE UNIQUE INDEX IF NOT EXISTS "uq_application_versions_unclaimed_version"
    ON application_versions ("version_name")
    WHERE "application_name" IS NULL;
"""

# Any of these being set partitions the queue; each applies per partition.
sqlite_migration_hundredeight = """
ALTER TABLE queues ADD COLUMN "partition_concurrency" INTEGER DEFAULT NULL;
ALTER TABLE queues ADD COLUMN "partition_worker_concurrency" INTEGER DEFAULT NULL;
ALTER TABLE queues ADD COLUMN "partition_rate_limit_max" INTEGER DEFAULT NULL;
ALTER TABLE queues ADD COLUMN "partition_rate_limit_period_sec" REAL DEFAULT NULL;
"""

_sqlite_history = [
    sqlite_migration_one,
    sqlite_migration_two,
    sqlite_migration_three,
    sqlite_migration_four,
    sqlite_migration_five,
    sqlite_migration_six,
    sqlite_migration_seven,
    sqlite_migration_eight,
    sqlite_migration_nine,
    sqlite_migration_eleven,
    sqlite_migration_twelve,
    sqlite_migration_thirteen,
    # There is no SQLite version of migration fourteen
    sqlite_migration_fifteen,
    sqlite_migration_sixteen,
    sqlite_migration_seventeen,
    sqlite_migration_eighteen,
    sqlite_migration_nineteen,
    # There is no SQLite version of migration twenty
    sqlite_migration_twentyone,
    sqlite_migration_twentytwo,
    sqlite_migration_twentythree,
    sqlite_migration_twentyfour,
    sqlite_migration_twentyfive,
    sqlite_migration_twentysix,
    sqlite_migration_twentyseven,
    sqlite_migration_twentyeight,
    sqlite_migration_twentynine,
    sqlite_migration_thirty,
    sqlite_migration_thirtyone,
    sqlite_migration_thirtytwo,
    sqlite_migration_thirtythree,
    sqlite_migration_thirtyfour,
    sqlite_migration_thirtyfive,
    sqlite_migration_thirtysix,
    sqlite_migration_thirtyseven,
    # There is no SQLite version of migrations thirty-eight and thirty-nine
    # Unlike Postgres migration forty, this creates no index (no GIN equivalent)
    sqlite_migration_forty,
    sqlite_migration_fortyone,
    sqlite_migration_fortytwo,
    # There is no SQLite version of migrations forty-three and forty-four
    sqlite_migration_fortyfive,
    sqlite_migration_fortysix,
    sqlite_migration_fortyseven,
]

sqlite_migration_hundrednine = f"""
CREATE TABLE IF NOT EXISTS workflow_input (
    workflow_uuid TEXT NOT NULL PRIMARY KEY,
    inputs TEXT,
    retention_timestamp INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()}
);

CREATE TABLE IF NOT EXISTS workflow_output (
    workflow_uuid TEXT NOT NULL PRIMARY KEY,
    output TEXT,
    error TEXT,
    retention_timestamp INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()}
);

CREATE INDEX IF NOT EXISTS idx_workflow_input_retention
    ON workflow_input (retention_timestamp);
CREATE INDEX IF NOT EXISTS idx_workflow_output_retention
    ON workflow_output (retention_timestamp);
"""

sqlite_migration_hundredten = """
ALTER TABLE operation_outputs ADD COLUMN retention_timestamp INTEGER;
"""

sqlite_migration_hundredeleven = """
CREATE INDEX IF NOT EXISTS idx_operation_outputs_retention
    ON operation_outputs (retention_timestamp);
"""


sqlite_migration_hundredtwelve = f"""
CREATE TABLE operation_outputs_new (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    child_workflow_id TEXT,
    started_at_epoch_ms INTEGER,
    completed_at_epoch_ms INTEGER,
    serialization TEXT,
    application_name TEXT DEFAULT NULL,
    retention_timestamp INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()},
    PRIMARY KEY (workflow_uuid, function_id)
);
INSERT INTO operation_outputs_new (workflow_uuid, function_id, function_name, output,
    error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization,
    application_name, retention_timestamp)
SELECT workflow_uuid, function_id, function_name, output, error, child_workflow_id,
    started_at_epoch_ms, completed_at_epoch_ms, serialization, application_name,
    COALESCE(retention_timestamp, {get_sqlite_timestamp_expr()})
FROM operation_outputs;
DROP TABLE operation_outputs;
ALTER TABLE operation_outputs_new RENAME TO operation_outputs;
CREATE INDEX IF NOT EXISTS idx_operation_outputs_retention
    ON operation_outputs (retention_timestamp);
CREATE INDEX IF NOT EXISTS idx_operation_outputs_completed_at_function_name
    ON operation_outputs (completed_at_epoch_ms, function_name);
"""

sqlite_migrations = [
    *_pad_to_shared_base(_sqlite_history),
    sqlite_migration_hundred,
    sqlite_migration_hundredone,
    sqlite_migration_hundredtwo,
    sqlite_migration_hundredthree,
    sqlite_migration_hundredfour,
    # Postgres migration 105 rewrites a stored function; SQLite has none.
    "",
    sqlite_migration_hundredsix,
    sqlite_migration_hundredseven,
    sqlite_migration_hundredeight,
    sqlite_migration_hundrednine,
    sqlite_migration_hundredten,
    sqlite_migration_hundredeleven,
    sqlite_migration_hundredtwelve,
    # Postgres migration 113 rewrites a stored function; SQLite has none.
    "",
]
