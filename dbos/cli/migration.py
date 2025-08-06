import sqlalchemy as sa
import typer

from dbos._app_db import ApplicationDatabase
from dbos._sys_db import SystemDatabase


def migrate_dbos_databases(app_database_url: str, system_database_url: str) -> None:
    app_db = None
    sys_db = None
    try:
        sys_db = SystemDatabase(
            system_database_url=system_database_url,
            engine_kwargs={
                "pool_timeout": 30,
                "max_overflow": 0,
                "pool_size": 2,
            },
        )
        app_db = ApplicationDatabase(
            database_url=app_database_url,
            engine_kwargs={
                "pool_timeout": 30,
                "max_overflow": 0,
                "pool_size": 2,
            },
        )
        sys_db.run_migrations()
        app_db.run_migrations()
    except Exception as e:
        typer.echo(f"DBOS migrations failed: {e}")
        raise typer.Exit(code=1)
    finally:
        if sys_db:
            sys_db.destroy()
        if app_db:
            app_db.destroy()


def grant_dbos_schema_permissions(database_url: str, role_name: str) -> None:
    """
    Grant all permissions on all tables in the dbos schema to the specified role.

    Args:
        database_url: The database connection URL
        role_name: The name of the role to grant permissions to
    """
    engine = None
    try:
        engine = sa.create_engine(database_url)
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")

            # Grant usage on the dbos schema
            connection.execute(sa.text(f"GRANT USAGE ON SCHEMA dbos TO {role_name}"))

            # Grant all privileges on all existing tables in dbos schema (includes views)
            connection.execute(
                sa.text(
                    f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dbos TO {role_name}"
                )
            )

            # Grant all privileges on all sequences in dbos schema
            connection.execute(
                sa.text(
                    f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dbos TO {role_name}"
                )
            )

            # Grant execute on all functions and procedures in dbos schema
            connection.execute(
                sa.text(f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbos TO {role_name}")
            )

            # Grant default privileges for future objects in dbos schema
            connection.execute(
                sa.text(
                    f"ALTER DEFAULT PRIVILEGES IN SCHEMA dbos GRANT ALL ON TABLES TO {role_name}"
                )
            )
            connection.execute(
                sa.text(
                    f"ALTER DEFAULT PRIVILEGES IN SCHEMA dbos GRANT ALL ON SEQUENCES TO {role_name}"
                )
            )
            connection.execute(
                sa.text(
                    f"ALTER DEFAULT PRIVILEGES IN SCHEMA dbos GRANT EXECUTE ON FUNCTIONS TO {role_name}"
                )
            )

    except Exception as e:
        typer.echo(f"Failed to grant permissions to role {role_name}: {e}")
        raise typer.Exit(code=1)
    finally:
        if engine:
            engine.dispose()
