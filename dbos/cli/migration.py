from typing import Optional

import sqlalchemy as sa
import typer

from dbos._app_db import ApplicationDatabase
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

            # Grant usage on the system schema
            sql = f'GRANT USAGE ON SCHEMA "{schema}" TO "{role_name}"'
            typer.echo(sql)
            connection.execute(sa.text(sql))

            # Grant all privileges on all existing tables in the system schema (includes views)
            sql = f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "{schema}" TO "{role_name}"'
            typer.echo(sql)
            connection.execute(sa.text(sql))

            # Grant all privileges on all sequences in the system schema
            sql = f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA "{schema}" TO "{role_name}"'
            typer.echo(sql)
            connection.execute(sa.text(sql))

            # Grant execute on all functions and procedures in the system schema
            sql = (
                f'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA "{schema}" TO "{role_name}"'
            )
            typer.echo(sql)
            connection.execute(sa.text(sql))

            # Grant default privileges for future objects in the system schema
            sql = f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT ALL ON TABLES TO "{role_name}"'
            typer.echo(sql)
            connection.execute(sa.text(sql))

            sql = f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT ALL ON SEQUENCES TO "{role_name}"'
            typer.echo(sql)
            connection.execute(sa.text(sql))

            sql = f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT EXECUTE ON FUNCTIONS TO "{role_name}"'
            typer.echo(sql)
            connection.execute(sa.text(sql))

    except Exception as e:
        typer.echo(f"Failed to grant permissions to role {role_name}: {e}")
        raise typer.Exit(code=1)
    finally:
        if engine:
            engine.dispose()
