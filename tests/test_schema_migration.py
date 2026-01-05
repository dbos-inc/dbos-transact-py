import os
import subprocess
import tempfile

import pytest
import sqlalchemy as sa

# Public API
from dbos import DBOS, DBOSConfig, run_dbos_database_migrations
from dbos._migration import get_dbos_migrations, sqlite_migrations

# Private API because this is a unit test
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import DefaultSerializer
from dbos._sys_db import SystemDatabase


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
        assert migrations_rows[0][0] == len(get_dbos_migrations("dbos", True))


def test_systemdb_migration_custom_schema(
    config: DBOSConfig,
    skip_with_sqlite: None,
    cleanup_test_databases: None,
) -> None:

    config["application_database_url"] = None
    schema = "F8nny_sCHem@-n@m3"
    config["dbos_system_schema"] = schema
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()
    # Make sure all tables exist
    with dbos._sys_db.engine.connect() as connection:
        result = connection.execute(
            sa.text(f'SELECT * FROM "{schema}".workflow_status')
        )
        rows = result.fetchall()
        assert len(rows) == 0
        # Check dbos_migrations table exists, has one row, and has the right version
        result = connection.execute(
            sa.text(f'SELECT version FROM "{schema}".dbos_migrations')
        )
        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == len(get_dbos_migrations(schema, True))

        # Check that the 'dbos' schema does not exist
        result = connection.execute(
            sa.text(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'dbos'"
            )
        )
        rows = result.fetchall()
        assert len(rows) == 0

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
            system_database_url=sqlite_url,
            engine_kwargs={},
            engine=None,
            schema=None,
            executor_id=None,
            serializer=DefaultSerializer(),
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

    # Test with different system schema names
    for schema in ["dbos", "public", "F8nny_sCHem@-n@m3"]:
        for use_app_db in [True, False]:
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
            if use_app_db:
                subprocess.check_call(
                    [
                        "dbos",
                        "migrate",
                        "-D",
                        db_url_string,
                        "-s",
                        db_url_string,
                        "-r",
                        role_name,
                        "--schema",
                        schema,
                    ]
                )
            else:
                subprocess.check_call(
                    [
                        "dbos",
                        "migrate",
                        "-s",
                        db_url_string,
                        "-r",
                        role_name,
                        "--schema",
                        schema,
                    ]
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
                "database_url": test_db_url if use_app_db else None,
                "system_database_url": test_db_url,
                "dbos_system_schema": schema,
            }
            dbos = DBOS(config=config)
            if not use_app_db:
                assert dbos._app_db is None

            @DBOS.transaction()
            def test_transaction() -> str:
                rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
                return str(rows[0][0])

            @DBOS.step()
            def test_step() -> str:
                return "1"

            @DBOS.workflow()
            def test_workflow() -> str:
                if use_app_db:
                    assert test_transaction() == "1"
                else:
                    assert test_step() == "1"
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
            assert (
                steps[0]["function_name"] == test_transaction.__qualname__
                if use_app_db
                else test_step.__qualname__
            )
            assert steps[1]["function_name"] == "DBOS.setEvent"
            DBOS.destroy()


def test_programmatic_migration(db_engine: sa.Engine, skip_with_sqlite: None) -> None:
    database_name = "migrate_test"
    migrate_role = "migrate-test-role"
    app_role = "app-test-role"
    role_password = "migrate_test_password"

    # Verify migration is agnostic to driver name (under the hood it uses postgresql+psycopg)
    db_url = db_engine.url.set(database=database_name).set(drivername="postgresql")

    # Drop the DBOS database if it exists. Create a test role with no permissions.
    with db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(
            sa.text(f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE)")
        )
        for role in [migrate_role, app_role]:
            connection.execute(sa.text(f'DROP ROLE IF EXISTS "{role}"'))
            connection.execute(
                sa.text(f"CREATE ROLE \"{role}\" WITH LOGIN PASSWORD '{role_password}'")
            )
            connection.execute(
                sa.text(f'REVOKE CONNECT ON DATABASE postgres FROM "{role}"')
            )
        connection.execute(
            sa.text(f'CREATE DATABASE {database_name} OWNER "{migrate_role}"')
        )

    # Using the admin role, create the DBOS database and verify it exists.
    # Set permissions for the test role.
    schema = "F8nny_sCHem@-n@m3"
    migrate_url = (
        db_url.set(username=migrate_role)
        .set(password=role_password)
        .render_as_string(hide_password=False)
    )
    run_dbos_database_migrations(
        migrate_url,
        app_database_url=migrate_url,
        schema=schema,
        application_role=app_role,
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
        db_url.set(username=app_role).set(password=role_password)
    ).render_as_string(hide_password=False)
    DBOS.destroy(destroy_registry=True)
    config: DBOSConfig = {
        "name": "test_migrate",
        "database_url": test_db_url,
        "system_database_url": test_db_url,
        "dbos_system_schema": schema,
    }
    DBOS(config=config)

    @DBOS.transaction()
    def test_transaction() -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return str(rows[0][0])

    @DBOS.step()
    def test_step() -> str:
        return "1"

    @DBOS.workflow()
    def test_workflow() -> str:
        assert test_transaction() == "1"
        assert test_step() == "1"
        id = DBOS.workflow_id
        assert id
        DBOS.set_event(id, id)
        return id

    DBOS.launch()

    workflow_id = test_workflow()
    assert workflow_id
    assert DBOS.get_event(workflow_id, workflow_id) == workflow_id
    DBOS.destroy()
