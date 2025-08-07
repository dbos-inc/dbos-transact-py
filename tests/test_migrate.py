import subprocess

import sqlalchemy as sa

from dbos import DBOS, DBOSConfig


def test_migrate(postgres_db_engine: sa.Engine) -> None:
    """Test that you can migrate with a privileged role and run DBOS with a less-privileged role"""
    database_name = "migrate_test"
    role_name = "migrate-test-role"
    role_password = "migrate_test_password"

    # Verify migration is agnostic to driver name (under the hood it uses postgresql+psycopg)
    db_url = postgres_db_engine.url.set(database=database_name).set(
        drivername="postgresql"
    )
    db_url_string = db_url.render_as_string(hide_password=False)

    # Drop the DBOS database if it exists. Create a test role with no permissions.
    with postgres_db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(
            sa.text(f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE)")
        )
        connection.execute(sa.text(f'DROP ROLE IF EXISTS "{role_name}"'))
        connection.execute(
            sa.text(
                f"CREATE ROLE \"{role_name}\" WITH LOGIN PASSWORD '{role_password}'"
            )
        )

    # Using the admin role, create the DBOS database and verify it exists.
    # Set permissions for the test role.
    subprocess.check_call(
        ["dbos", "migrate", "-D", db_url_string, "-s", db_url_string, "-r", role_name]
    )
    with postgres_db_engine.connect() as c:
        c.execution_options(isolation_level="AUTOCOMMIT")
        result = c.execute(
            sa.text(
                f"SELECT COUNT(*) FROM pg_database WHERE datname = '{database_name}'"
            )
        ).scalar()
        assert result == 1

    # Initialize DBOS with the test role. Verify various operations work.
    test_db_url = (
        db_url.set(username=role_name).set(password=role_password)
    ).render_as_string(hide_password=False)
    DBOS.destroy(destroy_registry=True)
    config: DBOSConfig = {
        "name": "test_migrate",
        "database_url": test_db_url,
        "system_database_url": test_db_url,
    }
    DBOS(config=config)

    @DBOS.transaction()
    def test_transaction() -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return str(rows[0][0])

    @DBOS.workflow()
    def test_workflow() -> str:
        assert test_transaction() == "1"
        id = DBOS.workflow_id
        assert id
        DBOS.set_event(id, id)
        return id

    DBOS.launch()

    workflow_id = test_workflow()
    assert workflow_id
    assert DBOS.get_event(workflow_id, workflow_id) == workflow_id

    steps = DBOS.list_workflow_steps(workflow_id)
    assert len(steps) == 2
    assert steps[0]["function_name"] == test_transaction.__qualname__
    assert steps[1]["function_name"] == "DBOS.setEvent"
