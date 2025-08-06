import subprocess

import sqlalchemy as sa

from dbos import DBOS, DBOSConfig


def test_migrate(postgres_db_engine: sa.Engine) -> None:
    database_name = "migrate_test"
    role_name = "migrate_test_role"
    role_password = "migrate_test_password"

    db_url = postgres_db_engine.url.set(database=database_name).render_as_string(
        hide_password=False
    )

    with postgres_db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")

        # Drop role if it exists
        connection.execute(sa.text(f"DROP ROLE IF EXISTS {role_name}"))

        # Create the role
        connection.execute(
            sa.text(f"CREATE ROLE {role_name} WITH LOGIN PASSWORD '{role_password}'")
        )

        connection.execute(
            sa.text(f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE)")
        )

    # Create a system database and verify it exists
    subprocess.check_call(
        ["dbos", "migrate", "-D", db_url, "-s", db_url, "-r", role_name]
    )
    with postgres_db_engine.connect() as c:
        c.execution_options(isolation_level="AUTOCOMMIT")
        result = c.execute(
            sa.text(
                f"SELECT COUNT(*) FROM pg_database WHERE datname = '{database_name}'"
            )
        ).scalar()
        assert result == 1

    test_db_url = (
        postgres_db_engine.url.set(database=database_name)
        .set(username=role_name)
        .set(password=role_password)
    )
    DBOS.destroy(destroy_registry=True)
    config: DBOSConfig = {
        "name": "test_migrate",
        "database_url": test_db_url,
        "system_database_url": test_db_url,
    }
    DBOS(config=config)

    @DBOS.workflow()
    def test_workflow() -> str:
        id = DBOS.workflow_id
        assert id
        return id

    DBOS.launch()

    assert test_workflow()
