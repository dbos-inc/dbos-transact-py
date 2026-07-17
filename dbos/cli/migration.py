from typing import List, Optional

import sqlalchemy as sa
import typer

from dbos._app_db import ApplicationDatabase
from dbos._migration import get_dbos_migrations
from dbos._schemas.application_database import ApplicationSchema
from dbos._serialization import DefaultSerializer
from dbos._sys_db import SystemDatabase


def run_dbos_database_migrations(
    system_database_url: str,
    *,
    app_database_url: Optional[str] = None,
    schema: str = "dbos",
    application_role: Optional[str] = None,
) -> None:
    # First, run DBOS migrations on the system database and (optionally) the application database
    migrate_dbos_databases(
        system_database_url=system_database_url,
        app_database_url=app_database_url,
        schema=schema,
    )

    # Then, assign permissions on the DBOS schema to the application role, if any
    if application_role:
        if app_database_url:
            grant_dbos_schema_permissions(
                database_url=app_database_url,
                role_name=application_role,
                schema=schema,
            )
        grant_dbos_schema_permissions(
            database_url=system_database_url, role_name=application_role, schema=schema
        )


def migrate_dbos_databases(
    system_database_url: str, app_database_url: Optional[str], schema: str
) -> None:
    app_db = None
    sys_db = None
    try:
        sys_db = SystemDatabase.create(
            system_database_url=system_database_url,
            engine_kwargs={
                "pool_timeout": 30,
                "max_overflow": 0,
                "pool_size": 2,
            },
            engine=None,
            schema=schema,
            serializer=DefaultSerializer(),
            executor_id=None,
        )
        sys_db.run_migrations()
        if app_database_url:
            app_db = ApplicationDatabase.create(
                database_url=app_database_url,
                engine_kwargs={
                    "pool_timeout": 30,
                    "max_overflow": 0,
                    "pool_size": 2,
                },
                schema=schema,
                serializer=DefaultSerializer(),
            )
            app_db.run_migrations()
    except Exception as e:
        typer.echo(f"DBOS migrations failed: {e}")
        raise typer.Exit(code=1)
    finally:
        if sys_db:
            sys_db.destroy()
        if app_db:
            app_db.destroy()


def get_dbos_schema_permissions_sql(schema: str, role_name: str) -> List[str]:
    """The statements granting permissions on all entities in the system schema to a role."""
    return [
        # Grant usage on the system schema
        f'GRANT USAGE ON SCHEMA "{schema}" TO "{role_name}"',
        # Grant all privileges on all existing tables in the system schema (includes views)
        f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "{schema}" TO "{role_name}"',
        # Grant all privileges on all sequences in the system schema
        f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA "{schema}" TO "{role_name}"',
        # Grant execute on all functions and procedures in the system schema
        f'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA "{schema}" TO "{role_name}"',
        # Grant default privileges for future objects in the system schema
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT ALL ON TABLES TO "{role_name}"',
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT ALL ON SEQUENCES TO "{role_name}"',
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT EXECUTE ON FUNCTIONS TO "{role_name}"',
    ]


def grant_dbos_schema_permissions(
    database_url: str, role_name: str, schema: str
) -> None:
    """
    Grant all permissions on all entities in the system schema to the specified role.
    """
    typer.echo(
        f"Granting permissions for the {schema} schema to {role_name} in database {sa.make_url(database_url)}"
    )
    engine = None
    try:
        engine = sa.create_engine(
            sa.make_url(database_url).set(drivername="postgresql+psycopg")
        )
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            for sql in get_dbos_schema_permissions_sql(schema, role_name):
                typer.echo(sql)
                connection.execute(sa.text(sql))
    except Exception as e:
        typer.echo(f"Failed to grant permissions to role {role_name}: {e}")
        raise typer.Exit(code=1)
    finally:
        if engine:
            engine.dispose()


def _get_current_dbos_schema_version(
    system_database_url: str, schema: str
) -> Optional[int]:
    """Read the current DBOS schema version with read-only queries.

    Returns 0 if the schema or dbos_migrations table is missing or empty
    (fresh database), and None if no connection could be established."""
    engine = None
    try:
        engine = sa.create_engine(
            sa.make_url(system_database_url).set(drivername="postgresql+psycopg"),
            connect_args={"connect_timeout": 10},
        )
        with engine.connect() as conn:
            table_exists = conn.execute(
                sa.text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = :schema AND table_name = 'dbos_migrations'"
                ),
                {"schema": schema},
            ).fetchone()
            if table_exists is None:
                return 0
            row = conn.execute(
                sa.text(f'SELECT version FROM "{schema}".dbos_migrations')
            ).fetchone()
            return int(row[0]) if row else 0
    except Exception:
        return None
    finally:
        if engine:
            engine.dispose()


def print_dbos_database_migrations(
    system_database_url: str,
    *,
    app_database_url: Optional[str] = None,
    schema: str = "dbos",
    application_role: Optional[str] = None,
) -> None:
    """Print to stdout the SQL that DBOS migrations would execute, without
    writing to any database. If the system database is reachable, only the
    migrations past its current version are printed; otherwise a full
    fresh-database script is printed. Stdout is pure SQL and comments."""
    if system_database_url.startswith("sqlite") or (
        app_database_url and app_database_url.startswith("sqlite")
    ):
        typer.echo("--print-only is only supported for Postgres databases", err=True)
        raise typer.Exit(code=1)
    # The execute path interpolates the schema into quoted identifiers and
    # string literals, so names containing quotes fail there too.
    if '"' in schema or "'" in schema:
        typer.echo("Schema names containing quotes are not supported", err=True)
        raise typer.Exit(code=1)

    migrations = get_dbos_migrations(schema, use_listen_notify=True)
    latest_version = len(migrations)

    # If the database is unreachable, silently fall back to a fresh-database
    # script: the apply-time guard below protects against misuse.
    current_version = _get_current_dbos_schema_version(system_database_url, schema)
    if current_version is None:
        current_version = 0

    if current_version >= latest_version:
        typer.echo(
            f"-- Database is already at the latest DBOS schema version ({current_version}); nothing to do."
        )
        return

    def emit(sql: str) -> None:
        sql = sql.strip()
        if sql:
            typer.echo(sql if sql.endswith(";") else sql + ";")

    typer.echo(
        f"-- DBOS system database migrations for {sa.make_url(system_database_url)}"
    )
    typer.echo(
        "-- Contains CREATE/DROP INDEX CONCURRENTLY: run outside a transaction block (e.g. plain psql, not psql --single-transaction)."
    )
    if current_version == 0:
        typer.echo(
            "-- This script is for FRESH databases only and aborts if DBOS migrations were already applied."
        )
        emit(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        emit(
            f'CREATE TABLE IF NOT EXISTS "{schema}".dbos_migrations (version BIGINT NOT NULL PRIMARY KEY)'
        )
        typer.echo("-- Abort if this is not a fresh database")
        emit(f"""DO $$
DECLARE
    existing_version BIGINT;
BEGIN
    SELECT version INTO existing_version FROM "{schema}".dbos_migrations LIMIT 1;
    IF existing_version IS NOT NULL THEN
        RAISE EXCEPTION 'DBOS schema % is already at version %; this script is for fresh databases only. Use dbos migrate instead.', '{schema}', existing_version;
    END IF;
END $$""")
    else:
        typer.echo(
            f"-- Delta script: migrations {current_version + 1} through {latest_version}, generated for a database at version {current_version}."
        )
        typer.echo(
            f"-- Abort unless the database is exactly at version {current_version}"
        )
        emit(f"""DO $$
DECLARE
    v BIGINT;
BEGIN
    SELECT version INTO v FROM "{schema}".dbos_migrations LIMIT 1;
    IF v IS DISTINCT FROM {current_version} THEN
        RAISE EXCEPTION 'expected DBOS schema version {current_version}, found %; regenerate this script with dbos migrate --print-only', v;
    END IF;
END $$""")

    version_row_exists = current_version > 0
    for i, migration_sql in enumerate(migrations, 1):
        if i <= current_version:
            continue
        if migration_sql.strip():
            typer.echo(f"-- Migration {i}")
            if i == 10:
                # Mirrors the runner's guard in run_dbos_migrations: migration
                # 10 backfills the notifications primary key, which migration
                # 1 already creates on a fresh database.
                typer.echo(
                    "-- Applied only if the notifications table has no primary key, mirroring the migration runner's guard"
                )
                emit(f"""DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE table_schema = '{schema}' AND table_name = 'notifications' AND constraint_type = 'PRIMARY KEY') THEN
        {migration_sql.strip()}
    END IF;
END $$""")
            else:
                emit(migration_sql)
        # Per-migration version bookkeeping, mirroring the runner: an
        # interrupted apply can be resumed by regenerating the delta.
        if version_row_exists:
            emit(f'UPDATE "{schema}".dbos_migrations SET version = {i}')
        else:
            emit(f'INSERT INTO "{schema}".dbos_migrations (version) VALUES ({i})')
            version_row_exists = True
    if application_role:
        typer.echo(f"-- Permissions for role {application_role}")
        for sql in get_dbos_schema_permissions_sql(schema, application_role):
            emit(sql)

    if app_database_url:
        typer.echo(
            f"-- DBOS application database migrations for {sa.make_url(app_database_url)}: run these against the application database"
        )
        dialect = sa.make_url("postgresql+psycopg://").get_dialect()()
        emit(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        # Clone the table into the real schema (it is defined with a placeholder schema)
        table = ApplicationSchema.transaction_outputs.to_metadata(
            sa.MetaData(), schema=schema
        )
        emit(
            str(
                sa.schema.CreateTable(table, if_not_exists=True).compile(
                    dialect=dialect
                )
            )
        )
        for index in table.indexes:
            emit(
                str(
                    sa.schema.CreateIndex(index, if_not_exists=True).compile(
                        dialect=dialect
                    )
                )
            )
        if application_role:
            typer.echo(f"-- Permissions for role {application_role}")
            for sql in get_dbos_schema_permissions_sql(schema, application_role):
                emit(sql)
