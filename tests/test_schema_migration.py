import os
import subprocess
import tempfile

import pytest
import sqlalchemy as sa

# Public API
from dbos import DBOS, DBOSConfig
from dbos._migration import dbos_migrations, run_alembic_migrations, sqlite_migrations

# Private API because this is a unit test
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import SystemDatabase
from dbos._sys_db_postgres import PostgresSystemDatabase


def test_systemdb_migration(dbos: DBOS, skip_with_sqlite: None) -> None:
    # Make sure all tables exist
    with dbos._sys_db.engine.connect() as connection:
        sql = SystemSchema.workflow_status.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        sql = SystemSchema.operation_outputs.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        sql = SystemSchema.workflow_events.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        sql = SystemSchema.notifications.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        # Check dbos_migrations table exists, has one row, and has the right version
        migrations_result = connection.execute(
            sa.text("SELECT version FROM dbos.dbos_migrations")
        )
        migrations_rows = migrations_result.fetchall()
        assert len(migrations_rows) == 1
        assert migrations_rows[0][0] == len(dbos_migrations)


def test_alembic_migrations_compatibility(
    config: DBOSConfig, db_engine: sa.Engine, skip_with_sqlite: None
) -> None:
    system_database_url = config["system_database_url"]
    assert system_database_url
    sysdb_name = sa.make_url(system_database_url).database

    # Drop and recreate the system database
    with db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(
            sa.text(f'DROP DATABASE IF EXISTS "{sysdb_name}" WITH (FORCE)')
        )
        connection.execute(sa.text(f'CREATE DATABASE "{sysdb_name}"'))

    sys_db = PostgresSystemDatabase(
        system_database_url=system_database_url, engine_kwargs={}
    )
    # Run the deprecated Alembic migrations
    run_alembic_migrations(sys_db.engine)
    # Then, run the new migrations to verify they work from a system database
    # that started in Alembic.
    dbos = DBOS(config=config)
    DBOS.launch()
    # Make sure all tables exist
    with dbos._sys_db.engine.connect() as connection:
        sql = SystemSchema.workflow_status.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        sql = SystemSchema.operation_outputs.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        sql = SystemSchema.workflow_events.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        sql = SystemSchema.notifications.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        # Check dbos_migrations table exists, has one row, and has the right version
        migrations_result = connection.execute(
            sa.text("SELECT version FROM dbos.dbos_migrations")
        )
        migrations_rows = migrations_result.fetchall()
        assert len(migrations_rows) == 1
        assert migrations_rows[0][0] == len(dbos_migrations)

    assert DBOS.list_workflows() == []


def test_custom_sysdb_name_migration(
    config: DBOSConfig, db_engine: sa.Engine, skip_with_sqlite: None
) -> None:
    sysdb_name = "custom_sysdb_name"
    config["sys_db_name"] = sysdb_name

    # Clean up from previous runs
    with db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(sa.text(f"DROP DATABASE IF EXISTS {sysdb_name}"))

    # Test migrating up
    DBOS.destroy()  # In case of other tests leaving it
    dbos = DBOS(config=config)
    DBOS.launch()

    # Make sure all tables exist
    with dbos._sys_db.engine.connect() as connection:
        sql = SystemSchema.workflow_status.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

    DBOS.destroy()


def test_reset(
    config: DBOSConfig, db_engine: sa.Engine, skip_with_sqlite: None
) -> None:
    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.launch()

    # Make sure the system database exists
    with dbos._sys_db.engine.connect() as c:
        sql = SystemSchema.workflow_status.select()
        result = c.execute(sql)
        assert result.fetchall() == []
    sysdb_name = dbos._sys_db.engine.url.database

    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.reset_system_database()

    with db_engine.connect() as c:
        c.execution_options(isolation_level="AUTOCOMMIT")
        count: int = c.execute(
            sa.text(f"SELECT COUNT(*) FROM pg_database WHERE datname = '{sysdb_name}'")
        ).scalar_one()
        assert count == 0

    DBOS.launch()

    # Make sure the system database is recreated
    with dbos._sys_db.engine.connect() as c:
        sql = SystemSchema.workflow_status.select()
        result = c.execute(sql)
        assert result.fetchall() == []

    # Verify that resetting after launch throws
    with pytest.raises(AssertionError):
        DBOS.reset_system_database()


def test_sqlite_systemdb_migration() -> None:
    """Test SQLite system database migration."""
    # Create a temporary SQLite database file
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as temp_db:
        temp_db_path = temp_db.name

        # Create SQLite system database URL
        sqlite_url = f"sqlite:///{temp_db_path}"

        # Create and run migrations
        sys_db = SystemDatabase.create(
            system_database_url=sqlite_url, engine_kwargs={}, debug_mode=False
        )

        # Run migrations
        sys_db.run_migrations()

        # Verify all tables exist and are empty
        with sys_db.engine.connect() as connection:
            # Note: SQLite schema doesn't use "dbos." prefix

            # Test workflow_status table
            sql = sa.text("SELECT * FROM workflow_status")
            result = connection.execute(sql)
            assert result.fetchall() == []

            # Test operation_outputs table
            sql = sa.text("SELECT * FROM operation_outputs")
            result = connection.execute(sql)
            assert result.fetchall() == []

            # Test workflow_events table
            sql = sa.text("SELECT * FROM workflow_events")
            result = connection.execute(sql)
            assert result.fetchall() == []

            # Test notifications table
            sql = sa.text("SELECT * FROM notifications")
            result = connection.execute(sql)
            assert result.fetchall() == []

            # Test streams table
            sql = sa.text("SELECT * FROM streams")
            result = connection.execute(sql)
            assert result.fetchall() == []

            # Check dbos_migrations table exists, has one row, and has the right version
            migrations_result = connection.execute(
                sa.text("SELECT version FROM dbos_migrations")
            )
            migrations_rows = migrations_result.fetchall()
            assert len(migrations_rows) == 1
            assert migrations_rows[0][0] == len(sqlite_migrations)

            # Verify foreign keys are enabled
            fk_result = connection.execute(sa.text("PRAGMA foreign_keys"))
            fk_enabled = fk_result.fetchone()
            assert fk_enabled and fk_enabled[0] == 1  # 1 means enabled

        # Clean up
        sys_db.destroy()

    # Test resetting the system database
    assert os.path.exists(temp_db_path)
    DBOS.destroy()
    DBOS(config={"name": "sqlite_test", "database_url": sqlite_url})
    DBOS.reset_system_database()
    assert not os.path.exists(temp_db_path)
    DBOS.destroy()


def test_migrate(db_engine: sa.Engine, skip_with_sqlite: None) -> None:
    """Test that you can migrate with a privileged role and run DBOS with a less-privileged role"""
    database_name = "migrate_test"
    role_name = "migrate-test-role"
    role_password = "migrate_test_password"

    # Verify migration is agnostic to driver name (under the hood it uses postgresql+psycopg)
    db_url = db_engine.url.set(database=database_name).set(drivername="postgresql")
    db_url_string = db_url.render_as_string(hide_password=False)

    # Drop the DBOS database if it exists. Create a test role with no permissions.
    with db_engine.connect() as connection:
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
    with db_engine.connect() as c:
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
