import hashlib
from typing import List, Optional, Tuple

import sqlalchemy as sa

from ._error import DBOSException, DBOSInitializationError
from ._logger import dbos_logger
from ._migration import get_sqlite_timestamp_expr
from ._utils import quote_identifier

# Separate from the system database's dbos_migrations, since a datasource may share its schema.
DATASOURCE_MIGRATIONS_TABLE = "dbos_datasource_migrations"

# Longest a migrating transaction may sit idle while holding the migration lock.
MIGRATION_IDLE_TIMEOUT = "30s"


def get_postgres_datasource_migrations(schema: str) -> List[str]:
    # Migration 1 is IF NOT EXISTS so datasources created before versioning adopt it cleanly.
    quoted_schema = quote_identifier(schema)
    return [
        f"""
        CREATE TABLE IF NOT EXISTS {quoted_schema}.datasource_outputs (
            workflow_id TEXT NOT NULL,
            step_id INT NOT NULL,
            output TEXT,
            error TEXT,
            serialization TEXT,
            created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
            PRIMARY KEY (workflow_id, step_id)
        )""",
    ]


def get_sqlite_datasource_migrations() -> List[str]:
    return [
        f"""
        CREATE TABLE IF NOT EXISTS datasource_outputs (
            workflow_id TEXT NOT NULL,
            step_id INTEGER NOT NULL,
            output TEXT,
            error TEXT,
            serialization TEXT,
            created_at INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()},
            PRIMARY KEY (workflow_id, step_id)
        )""",
    ]


def _postgres_table_exists(conn: sa.Connection, schema: str, table: str) -> bool:
    # pg_catalog, not information_schema, which hides tables the role holds no grant on.
    return (
        conn.execute(
            sa.text(
                "SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = :schema AND tablename = :table"
            ),
            {"schema": schema, "table": table},
        ).fetchone()
        is not None
    )


def _postgres_version(conn: sa.Connection, schema: str) -> int:
    if not _postgres_table_exists(conn, schema, DATASOURCE_MIGRATIONS_TABLE):
        return 0
    row = conn.execute(
        sa.text(
            f"SELECT version FROM {quote_identifier(schema)}.{DATASOURCE_MIGRATIONS_TABLE}"
        )
    ).fetchone()
    return int(row[0]) if row else 0


def _sqlite_version(conn: sa.Connection) -> int:
    exists = conn.execute(
        sa.text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table"),
        {"table": DATASOURCE_MIGRATIONS_TABLE},
    ).fetchone()
    if exists is None:
        return 0
    row = conn.execute(
        sa.text(f"SELECT version FROM {DATASOURCE_MIGRATIONS_TABLE}")
    ).fetchone()
    return int(row[0]) if row else 0


def get_datasource_migration_versions(
    conn: sa.Connection, schema: Optional[str]
) -> Tuple[int, int]:
    """The (recorded, latest) datasource migration versions; missing tables read as 0."""
    if conn.dialect.name == "sqlite":
        return _sqlite_version(conn), len(get_sqlite_datasource_migrations())
    assert schema is not None
    return _postgres_version(conn, schema), len(
        get_postgres_datasource_migrations(schema)
    )


def _record_version(
    conn: sa.Connection, table: str, previous: int, version: int
) -> None:
    if previous == 0:
        conn.execute(
            sa.text(f"INSERT INTO {table} (version) VALUES (:version)"),
            {"version": version},
        )
    else:
        conn.execute(
            sa.text(f"UPDATE {table} SET version = :version"), {"version": version}
        )


def _migration_lock_key(schema: str) -> int:
    # Leading 8 bytes of SHA-256, big-endian signed, as for the retention lock.
    return int.from_bytes(
        hashlib.sha256(f"dbos.datasource_migrations.{schema}".encode()).digest()[:8],
        "big",
        signed=True,
    )


def migrate_datasource(conn: sa.Connection, schema: Optional[str]) -> None:
    """Bring the datasource schema to the latest version inside conn's transaction.

    Issues no DDL when the database is already at or ahead of the latest version."""
    current, latest = get_datasource_migration_versions(conn, schema)
    if current >= latest:
        return

    if conn.dialect.name == "sqlite":
        # Connections begin IMMEDIATE transactions, so concurrent migrators already serialize.
        migrations = get_sqlite_datasource_migrations()
        table = DATASOURCE_MIGRATIONS_TABLE
        conn.execute(
            sa.text(
                f"CREATE TABLE IF NOT EXISTS {table} (version INTEGER NOT NULL PRIMARY KEY)"
            )
        )
        current = _sqlite_version(conn)
    else:
        assert schema is not None
        quoted_schema = quote_identifier(schema)
        migrations = get_postgres_datasource_migrations(schema)
        table = f"{quoted_schema}.{DATASOURCE_MIGRATIONS_TABLE}"
        # A frozen or partitioned migrator's session is killed, rolling back and releasing the lock.
        conn.execute(
            sa.text(
                f"SET LOCAL idle_in_transaction_session_timeout = '{MIGRATION_IDLE_TIMEOUT}'"
            )
        )
        # Transaction-scoped, so it releases with the migration's commit or rollback.
        conn.execute(
            sa.text("SELECT pg_advisory_xact_lock(:key)"),
            {"key": _migration_lock_key(schema)},
        )
        current = _postgres_version(conn, schema)
        if current >= latest:
            return
        # Check first: CREATE ... IF NOT EXISTS still demands the CREATE privilege.
        if (
            conn.execute(
                sa.text(
                    "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = :schema"
                ),
                {"schema": schema},
            ).fetchone()
            is None
        ):
            conn.execute(sa.text(f"CREATE SCHEMA {quoted_schema}"))
        if not _postgres_table_exists(conn, schema, DATASOURCE_MIGRATIONS_TABLE):
            conn.execute(
                sa.text(f"CREATE TABLE {table} (version BIGINT NOT NULL PRIMARY KEY)")
            )

    previous = current
    for i, migration_sql in enumerate(migrations, 1):
        if i <= current:
            continue
        dbos_logger.info(f"Applying DBOS datasource schema migration {i}")
        conn.execute(sa.text(migration_sql))
    if latest > previous:
        _record_version(conn, table, previous, latest)


def verify_datasource_migrations(
    conn: sa.Connection, schema: Optional[str], url: sa.URL
) -> None:
    """Raise unless the datasource schema is migrated, creating and changing nothing."""
    current, latest = get_datasource_migration_versions(conn, schema)
    # A database ahead of this build belongs to a newer peer, which migration also tolerates.
    if current < latest:
        printable_url = url.render_as_string(hide_password=True)
        raise DBOSInitializationError(
            f"Datasource database {printable_url} is at datasource schema version {current}, but this "
            f"version of DBOS requires {latest}. This datasource was created with run_migrations "
            f"disabled, so it will not migrate it: either migrate it out of band "
            f"(the datasource class's `migrate`) or create it with run_migrations enabled."
        )
    dbos_logger.debug(
        f"Datasource schema version {current} satisfies the required version {latest}"
    )


def get_datasource_permissions_sql(schema: str, role_name: str) -> List[str]:
    """The minimal grants a datasource needs at runtime: read the version, read/write checkpoints."""
    quoted_schema = quote_identifier(schema)
    quoted_role = quote_identifier(role_name)
    return [
        f"GRANT USAGE ON SCHEMA {quoted_schema} TO {quoted_role}",
        f"GRANT SELECT, INSERT, DELETE ON {quoted_schema}.datasource_outputs TO {quoted_role}",
        f"GRANT SELECT ON {quoted_schema}.{DATASOURCE_MIGRATIONS_TABLE} TO {quoted_role}",
    ]


def migrate_datasource_database(
    database_url: str, schema: Optional[str], application_role: Optional[str]
) -> None:
    """Migrate a datasource database, then grant application_role its runtime permissions."""
    is_sqlite = database_url.startswith("sqlite")
    if is_sqlite and application_role:
        raise DBOSException(
            "application_role is only supported for Postgres datasources"
        )
    url = sa.make_url(database_url).set(
        drivername="sqlite" if is_sqlite else "postgresql+psycopg"
    )
    resolved_schema = None if is_sqlite else (schema or "dbos")
    # A bare engine, not a datasource, which would register itself with DBOS.
    engine = sa.create_engine(url)
    try:
        with engine.begin() as conn:
            migrate_datasource(conn, resolved_schema)
            if application_role:
                assert resolved_schema is not None
                dbos_logger.info(
                    f"Granting datasource permissions on schema {resolved_schema} to {application_role}"
                )
                for sql in get_datasource_permissions_sql(
                    resolved_schema, application_role
                ):
                    conn.execute(sa.text(sql))
    finally:
        engine.dispose()
