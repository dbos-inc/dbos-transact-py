from typing import List, Optional

import click
import sqlalchemy as sa

from dbos._app_db import ApplicationDatabase
from dbos._migration import get_dbos_migrations
from dbos._serialization import DefaultSerializer
from dbos._sys_db import SystemDatabase
from dbos._utils import quote_identifier


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
        click.echo(f"DBOS migrations failed: {e}")
        raise click.exceptions.Exit(code=1)
    finally:
        if sys_db:
            sys_db.destroy()
        if app_db:
            app_db.destroy()


def get_dbos_schema_permissions_sql(schema: str, role_name: str) -> List[str]:
    """The statements granting permissions on all entities in the system schema to a role."""
    quoted_schema = quote_identifier(schema)
    quoted_role = quote_identifier(role_name)
    return [
        # Grant usage on the system schema
        f"GRANT USAGE ON SCHEMA {quoted_schema} TO {quoted_role}",
        # Grant all privileges on all existing tables in the system schema (includes views)
        f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {quoted_schema} TO {quoted_role}",
        # Grant all privileges on all sequences in the system schema
        f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {quoted_schema} TO {quoted_role}",
        # Grant execute on all functions and procedures in the system schema
        f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {quoted_schema} TO {quoted_role}",
        # Grant default privileges for future objects in the system schema
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {quoted_schema} GRANT ALL ON TABLES TO {quoted_role}",
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {quoted_schema} GRANT ALL ON SEQUENCES TO {quoted_role}",
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {quoted_schema} GRANT EXECUTE ON FUNCTIONS TO {quoted_role}",
    ]


def grant_dbos_schema_permissions(
    database_url: str, role_name: str, schema: str
) -> None:
    """
    Grant all permissions on all entities in the system schema to the specified role.
    """
    click.echo(
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
                click.echo(sql)
                connection.execute(sa.text(sql))
    except Exception as e:
        click.echo(f"Failed to grant permissions to role {role_name}: {e}")
        raise click.exceptions.Exit(code=1)
    finally:
        if engine:
            engine.dispose()


def _emit_sql(sql: str) -> None:
    sql = sql.strip()
    if sql:
        click.echo(sql if sql.endswith(";") else sql + ";")


def print_dbos_migrations(
    system_database_url: Optional[str],
    *,
    schema: str = "dbos",
    migration: str = "all",
) -> None:
    """Print to stdout the SQL of the DBOS system database migrations, all of
    them (migration is "all") or starting from a number (migration is N),
    without touching any database. Stdout is pure SQL and comments.

    system_database_url may be None: it is only read to reject SQLite."""
    if system_database_url is not None and system_database_url.startswith("sqlite"):
        click.echo(
            "--print-migrations is only supported for Postgres databases", err=True
        )
        raise click.exceptions.Exit(code=1)

    quoted_schema = quote_identifier(schema)
    migrations = get_dbos_migrations(schema, use_listen_notify=True)
    latest_version = len(migrations)
    if migration == "all":
        start = 1
    else:
        try:
            start = int(migration)
        except ValueError:
            click.echo(
                f"Invalid --print-migrations value '{migration}': expected 'all' or a migration number",
                err=True,
            )
            raise click.exceptions.Exit(code=1)
        if not 1 <= start <= latest_version:
            click.echo(
                f"Migration {start} does not exist: valid migrations are 1 through {latest_version}",
                err=True,
            )
            raise click.exceptions.Exit(code=1)

    click.echo("-- DBOS system database migrations")
    click.echo(
        "-- Contains CREATE/DROP INDEX CONCURRENTLY: run outside a transaction block (e.g. plain psql, not psql --single-transaction)."
    )
    if start == 1:
        click.echo("-- This script is for FRESH databases only.")
        _emit_sql(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")
        _emit_sql(
            f"CREATE TABLE IF NOT EXISTS {quoted_schema}.dbos_migrations (version BIGINT NOT NULL PRIMARY KEY)"
        )

    version_row_exists = start > 1
    last_emitted = start - 1

    def _emit_version(version: int) -> None:
        nonlocal version_row_exists, last_emitted
        if version_row_exists:
            _emit_sql(f"UPDATE {quoted_schema}.dbos_migrations SET version = {version}")
        else:
            _emit_sql(
                f"INSERT INTO {quoted_schema}.dbos_migrations (version) VALUES ({version})"
            )
            version_row_exists = True
        last_emitted = version

    for i in range(start, latest_version + 1):
        migration_sql = migrations[i - 1]
        if i == 10:
            # Migration 10 backfills the notifications primary key, which
            # migration 1 already creates on a fresh database.
            click.echo("-- Migration 10 skipped: not applicable on fresh databases")
        elif migration_sql.strip():
            click.echo(f"-- Migration {i}")
            _emit_sql(migration_sql)
        else:
            # Renumbering left long runs of empty migrations; nothing to emit.
            continue
        # Per-migration version bookkeeping, mirroring the runner: an
        # interrupted apply can be resumed from the next migration number.
        _emit_version(i)

    # Empty migrations at the end still count as applied.
    if latest_version > last_emitted:
        _emit_version(latest_version)


def print_dbos_user_role_sql(*, schema: str = "dbos", role_name: str) -> None:
    """Print to stdout the SQL granting an application role access to the DBOS
    system schema, without touching any database."""
    click.echo("-- Permissions on the DBOS system schema")
    for sql in get_dbos_schema_permissions_sql(schema, role_name):
        _emit_sql(sql)
