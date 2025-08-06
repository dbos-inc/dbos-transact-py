import subprocess

import sqlalchemy as sa


def test_migrate(postgres_db_engine: sa.Engine) -> None:
    database_name = "migrate_test"
    db_url = postgres_db_engine.url.set(database=database_name).render_as_string(
        hide_password=False
    )

    with postgres_db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(
            sa.text(f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE)")
        )

    # Create a system database and verify it exists
    subprocess.check_call(["dbos", "migrate", "-D", db_url, "-s", db_url])
    with postgres_db_engine.connect() as c:
        c.execution_options(isolation_level="AUTOCOMMIT")
        result = c.execute(
            sa.text(
                f"SELECT COUNT(*) FROM pg_database WHERE datname = '{database_name}'"
            )
        ).scalar()
        assert result == 1
