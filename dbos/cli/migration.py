import typer

from dbos._app_db import ApplicationDatabase
from dbos._sys_db import SystemDatabase


def migrate_dbos_databases(app_database_url: str, system_database_url: str):
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
