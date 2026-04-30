import sys

import sqlalchemy as sa

from ._logger import dbos_logger

# Migration versions that contain CONCURRENTLY index DDL and must run with
# autocommit (CREATE/DROP INDEX CONCURRENTLY cannot run inside a transaction
# block on Postgres). On CockroachDB, schema changes are inherently online,
# so this set is ignored and the regular transactional path is used.
_ONLINE_MIGRATIONS = {22, 23, 24, 25, 26, 27, 29, 30, 31, 32, 34, 35}


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
                sa.text(f'DROP INDEX CONCURRENTLY IF EXISTS "{schema}"."{idx_name}"')
            )


def _bump_migration_version(
    engine: sa.Engine, schema: str, version: int, last_applied: int
) -> None:
    """Update the dbos_migrations version row in its own transaction."""
    with engine.begin() as conn:
        if last_applied == 0:
            conn.execute(
                sa.text(
                    f'INSERT INTO "{schema}".dbos_migrations (version) VALUES (:version)'
                ),
                {"version": version},
            )
        else:
            conn.execute(
                sa.text(f'UPDATE "{schema}".dbos_migrations SET version = :version'),
                {"version": version},
            )


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
    # Get current migration version and detect CockroachDB via server version string
    with engine.begin() as conn:
        result = conn.execute(
            sa.text(f'SELECT version FROM "{schema}".dbos_migrations')
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

        dbos_logger.info(f"Applying DBOS system database schema migration {i}")

        if not migration_sql.strip():
            dbos_logger.info(f"Migration {i} has no statements; skipping.")
            _bump_migration_version(engine, schema, i, last_applied)
            last_applied = i
            continue

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
                        f"SELECT 1 FROM information_schema.table_constraints "
                        f"WHERE table_schema = '{schema}' "
                        f"AND table_name = 'notifications' "
                        f"AND constraint_type = 'PRIMARY KEY'"
                    )
                ).scalar()
            ):
                dbos_logger.info("Migration 10 skipped, primary key already exists")
            else:
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
    priority INT4 NOT NULL DEFAULT 0
);

CREATE INDEX workflow_status_created_at_index ON \"{schema}\".workflow_status (created_at);
CREATE INDEX workflow_status_executor_id_index ON \"{schema}\".workflow_status (executor_id);
CREATE INDEX workflow_status_status_index ON \"{schema}\".workflow_status (status);

ALTER TABLE \"{schema}\".workflow_status 
ADD CONSTRAINT uq_workflow_status_queue_name_dedup_id 
UNIQUE (queue_name, deduplication_id);

CREATE TABLE \"{schema}\".operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INT4 NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    child_workflow_id TEXT,
    PRIMARY KEY (workflow_uuid, function_id),
    FOREIGN KEY (workflow_uuid) REFERENCES \"{schema}\".workflow_status(workflow_uuid) 
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE \"{schema}\".notifications (
    message_uuid TEXT NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY, -- Built-in function
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
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
    "offset" INT4 NOT NULL,
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
    function_id INT4 NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, function_id, key),
    FOREIGN KEY (workflow_uuid) REFERENCES \"{schema}\".workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE \"{schema}\".streams ADD COLUMN function_id INT4 NOT NULL DEFAULT 0;
"""


def get_dbos_migration_seven(schema: str) -> str:
    return f"""ALTER TABLE "{schema}"."workflow_status" ADD COLUMN "owner_xid" TEXT DEFAULT NULL;"""


def get_dbos_migration_eight(schema: str) -> str:
    return f"""
ALTER TABLE "{schema}"."workflow_status" ADD COLUMN "parent_workflow_id" TEXT DEFAULT NULL;
CREATE INDEX "idx_workflow_status_parent_workflow_id" ON "{schema}"."workflow_status" ("parent_workflow_id");
"""


def get_dbos_migration_nine(schema: str) -> str:
    return f"""
CREATE TABLE "{schema}".workflow_schedules (
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
def get_dbos_migration_ten(schema: str) -> str:
    return f"""
ALTER TABLE "{schema}".notifications ADD PRIMARY KEY (message_uuid);
"""


def get_dbos_migration_eleven(schema: str) -> str:
    return f"""
ALTER TABLE "{schema}"."workflow_status" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "{schema}"."notifications" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "{schema}"."workflow_events" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "{schema}"."workflow_events_history" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "{schema}"."operation_outputs" ADD COLUMN "serialization" TEXT DEFAULT NULL;
ALTER TABLE "{schema}"."streams" ADD COLUMN "serialization" TEXT DEFAULT NULL;
"""


def get_dbos_migration_twelve(schema: str) -> str:
    return f"""
ALTER TABLE "{schema}"."notifications" ADD COLUMN "consumed" BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX "idx_notifications" ON "{schema}"."notifications" ("destination_uuid", "topic");
"""


def get_dbos_migration_thirteen(schema: str) -> str:
    return f"""
CREATE TABLE "{schema}".application_versions (
    version_id TEXT NOT NULL PRIMARY KEY,
    version_name TEXT NOT NULL UNIQUE,
    version_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);
"""


def get_dbos_migration_fourteen(schema: str) -> str:
    return f"""
CREATE FUNCTION "{schema}".enqueue_workflow(
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

    INSERT INTO "{schema}".workflow_status (
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

CREATE FUNCTION "{schema}".send_message(
    destination_id TEXT,
    message JSON,
    topic TEXT DEFAULT NULL,
    message_id TEXT DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    v_topic TEXT := COALESCE(topic, '__null__topic__');
    v_message_id TEXT := COALESCE(message_id, gen_random_uuid()::TEXT);
BEGIN
    INSERT INTO "{schema}".notifications (
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


def get_dbos_migration_fifteen(schema: str) -> str:
    return f"""
ALTER TABLE "{schema}".workflow_schedules ADD COLUMN "last_fired_at" TEXT DEFAULT NULL;
ALTER TABLE "{schema}".workflow_schedules ADD COLUMN "automatic_backfill" BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE "{schema}".workflow_schedules ADD COLUMN "cron_timezone" TEXT DEFAULT NULL;
"""


def get_dbos_migration_sixteen(schema: str) -> str:
    return f"""
ALTER TABLE "{schema}"."workflow_status" ADD COLUMN "delay_until_epoch_ms" BIGINT DEFAULT NULL;
CREATE INDEX "idx_workflow_status_delayed" ON "{schema}"."workflow_status" ("delay_until_epoch_ms") WHERE status = 'DELAYED';
"""


def get_dbos_migration_seventeen(schema: str) -> str:
    return f"""
ALTER TABLE "{schema}".workflow_schedules ADD COLUMN "queue_name" TEXT DEFAULT NULL;
"""


def get_dbos_migration_eighteen(schema: str) -> str:
    return f"""
ALTER TABLE "{schema}"."workflow_status" ADD COLUMN "was_forked_from" BOOLEAN NOT NULL DEFAULT FALSE;
"""


def get_dbos_migration_nineteen(schema: str) -> str:
    return f"""
CREATE INDEX "idx_operation_outputs_completed_at_function_name" ON "{schema}"."operation_outputs" ("completed_at_epoch_ms", "function_name");
"""


def get_dbos_migration_twenty(
    schema: str, use_listen_notify: bool, is_cockroach: bool
) -> str:
    if is_cockroach:
        return ""
    migration = f"""
ALTER FUNCTION "{schema}".enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INTEGER, TEXT
) SET search_path = pg_catalog, pg_temp;

ALTER FUNCTION "{schema}".send_message(
    TEXT, JSON, TEXT, TEXT
) SET search_path = pg_catalog, pg_temp;
"""
    if use_listen_notify:
        migration += f"""
ALTER FUNCTION "{schema}".notifications_function() SET search_path = pg_catalog, pg_temp;
ALTER FUNCTION "{schema}".workflow_events_function() SET search_path = pg_catalog, pg_temp;
"""
    return migration


def get_dbos_migration_twentyone(schema: str) -> str:
    return f"""
CREATE TABLE "{schema}".queues (
    queue_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
    name TEXT NOT NULL UNIQUE,
    concurrency INTEGER,
    worker_concurrency INTEGER,
    rate_limit_max INTEGER,
    rate_limit_period_sec DOUBLE PRECISION,
    priority_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    partition_queue BOOLEAN NOT NULL DEFAULT FALSE,
    polling_interval_sec DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);
"""


def get_dbos_migration_twentytwo(schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'DROP INDEX {c} IF EXISTS "{schema}"."idx_workflow_status_forked_from"'


def get_dbos_migration_twentythree(schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} "idx_workflow_status_forked_from" ON "{schema}"."workflow_status" ("forked_from") WHERE "forked_from" IS NOT NULL'


def get_dbos_migration_twentyfour(schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return (
        f'DROP INDEX {c} IF EXISTS "{schema}"."idx_workflow_status_parent_workflow_id"'
    )


def get_dbos_migration_twentyfive(schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} "idx_workflow_status_parent_workflow_id" ON "{schema}"."workflow_status" ("parent_workflow_id") WHERE "parent_workflow_id" IS NOT NULL'


def get_dbos_migration_twentysix(schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'DROP INDEX {c} IF EXISTS "{schema}"."workflow_status_executor_id_index"'


def get_dbos_migration_twentyseven(schema: str, is_cockroach: bool) -> str:
    # The new partial unique index uses a different name from the original
    # constraint to avoid a naming collision: Postgres auto-creates an index
    # with the constraint name, so we can't reuse it until the constraint is
    # dropped. Creating with a new name first preserves uniqueness throughout
    # the transition (both the old constraint and new index enforce identical
    # semantics, since NULLs are SQL-distinct in the old constraint).
    c = _concurrently(is_cockroach)
    return f'CREATE UNIQUE INDEX {c} "uq_workflow_status_dedup_id" ON "{schema}"."workflow_status" ("queue_name", "deduplication_id") WHERE "deduplication_id" IS NOT NULL'


def get_dbos_migration_twentyeight(schema: str, is_cockroach: bool) -> str:
    # CockroachDB implements unique constraints as indexes and rejects
    # ALTER TABLE DROP CONSTRAINT for them; Postgres rejects DROP INDEX on a
    # constraint-backed index. Both paths are fast catalog operations, no
    # CONCURRENTLY needed.
    if is_cockroach:
        return f'DROP INDEX IF EXISTS "{schema}"."uq_workflow_status_queue_name_dedup_id" CASCADE'
    return f'ALTER TABLE "{schema}".workflow_status DROP CONSTRAINT IF EXISTS uq_workflow_status_queue_name_dedup_id'


def get_dbos_migration_twentynine(schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} "idx_workflow_status_pending" ON "{schema}"."workflow_status" ("workflow_uuid") WHERE "status" = \'PENDING\''


def get_dbos_migration_thirty(schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} "idx_workflow_status_failed" ON "{schema}"."workflow_status" ("status", "created_at") WHERE "status" IN (\'ERROR\', \'CANCELLED\', \'MAX_RECOVERY_ATTEMPTS_EXCEEDED\')'


def get_dbos_migration_thirtyone(schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'DROP INDEX {c} IF EXISTS "{schema}"."workflow_status_status_index"'


def get_dbos_migration_thirtytwo(schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} "idx_workflow_status_in_flight" ON "{schema}"."workflow_status" ("queue_name", "status", "priority", "created_at") WHERE "status" IN (\'ENQUEUED\', \'PENDING\')'


def get_dbos_migration_thirtythree(schema: str) -> str:
    # ALTER TABLE ADD COLUMN with constant default is fast on Postgres 11+
    # (catalog-only update via attmissingval); runs in a regular transaction.
    return f'ALTER TABLE "{schema}"."workflow_status" ADD COLUMN "rate_limited" BOOLEAN NOT NULL DEFAULT FALSE'


def get_dbos_migration_thirtyfour(schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'CREATE INDEX {c} "idx_workflow_status_rate_limited" ON "{schema}"."workflow_status" ("queue_name", "started_at_epoch_ms") WHERE "rate_limited" = TRUE'


def get_dbos_migration_thirtyfive(schema: str, is_cockroach: bool) -> str:
    c = _concurrently(is_cockroach)
    return f'DROP INDEX {c} IF EXISTS "{schema}"."idx_workflow_status_queue_status_started"'


def get_dbos_migrations(
    schema: str, use_listen_notify: bool, is_cockroach: bool = False
) -> list[str]:
    return [
        get_dbos_migration_one(schema, use_listen_notify),
        get_dbos_migration_two(schema),
        get_dbos_migration_three(schema),
        get_dbos_migration_four(schema),
        get_dbos_migration_five(schema),
        get_dbos_migration_six(schema),
        get_dbos_migration_seven(schema),
        get_dbos_migration_eight(schema),
        get_dbos_migration_nine(schema),
        get_dbos_migration_ten(schema),
        get_dbos_migration_eleven(schema),
        get_dbos_migration_twelve(schema),
        get_dbos_migration_thirteen(schema),
        get_dbos_migration_fourteen(schema),
        get_dbos_migration_fifteen(schema),
        get_dbos_migration_sixteen(schema),
        get_dbos_migration_seventeen(schema),
        get_dbos_migration_eighteen(schema),
        get_dbos_migration_nineteen(schema),
        get_dbos_migration_twenty(schema, use_listen_notify, is_cockroach),
        get_dbos_migration_twentyone(schema),
        get_dbos_migration_twentytwo(schema, is_cockroach),
        get_dbos_migration_twentythree(schema, is_cockroach),
        get_dbos_migration_twentyfour(schema, is_cockroach),
        get_dbos_migration_twentyfive(schema, is_cockroach),
        get_dbos_migration_twentysix(schema, is_cockroach),
        get_dbos_migration_twentyseven(schema, is_cockroach),
        get_dbos_migration_twentyeight(schema, is_cockroach),
        get_dbos_migration_twentynine(schema, is_cockroach),
        get_dbos_migration_thirty(schema, is_cockroach),
        get_dbos_migration_thirtyone(schema, is_cockroach),
        get_dbos_migration_thirtytwo(schema, is_cockroach),
        get_dbos_migration_thirtythree(schema),
        get_dbos_migration_thirtyfour(schema, is_cockroach),
        get_dbos_migration_thirtyfive(schema, is_cockroach),
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

sqlite_migration_twentythree = 'CREATE INDEX "idx_workflow_status_forked_from" ON "workflow_status" ("forked_from") WHERE "forked_from" IS NOT NULL'

sqlite_migration_twentyfour = (
    'DROP INDEX IF EXISTS "idx_workflow_status_parent_workflow_id"'
)

sqlite_migration_twentyfive = 'CREATE INDEX "idx_workflow_status_parent_workflow_id" ON "workflow_status" ("parent_workflow_id") WHERE "parent_workflow_id" IS NOT NULL'

sqlite_migration_twentysix = 'DROP INDEX IF EXISTS "workflow_status_executor_id_index"'

sqlite_migration_twentyseven = 'CREATE UNIQUE INDEX "uq_workflow_status_dedup_id" ON "workflow_status" ("queue_name", "deduplication_id") WHERE "deduplication_id" IS NOT NULL'

sqlite_migration_twentyeight = (
    'DROP INDEX IF EXISTS "uq_workflow_status_queue_name_dedup_id"'
)

sqlite_migration_twentynine = 'CREATE INDEX "idx_workflow_status_pending" ON "workflow_status" ("workflow_uuid") WHERE "status" = \'PENDING\''

sqlite_migration_thirty = 'CREATE INDEX "idx_workflow_status_failed" ON "workflow_status" ("status", "created_at") WHERE "status" IN (\'ERROR\', \'CANCELLED\', \'MAX_RECOVERY_ATTEMPTS_EXCEEDED\')'

sqlite_migration_thirtyone = 'DROP INDEX IF EXISTS "workflow_status_status_index"'

sqlite_migration_thirtytwo = 'CREATE INDEX "idx_workflow_status_in_flight" ON "workflow_status" ("queue_name", "status", "priority", "created_at") WHERE "status" IN (\'ENQUEUED\', \'PENDING\')'

sqlite_migration_thirtythree = 'ALTER TABLE "workflow_status" ADD COLUMN "rate_limited" BOOLEAN NOT NULL DEFAULT FALSE'

sqlite_migration_thirtyfour = 'CREATE INDEX "idx_workflow_status_rate_limited" ON "workflow_status" ("queue_name", "started_at_epoch_ms") WHERE "rate_limited" = TRUE'

sqlite_migration_thirtyfive = (
    'DROP INDEX IF EXISTS "idx_workflow_status_queue_status_started"'
)

sqlite_migrations = [
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
]
