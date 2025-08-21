import pytest
import sqlalchemy as sa

# Public API
from dbos import DBOS, DBOSConfig
from dbos._migration import dbos_migrations, run_alembic_migrations

# Private API because this is a unit test
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import SystemDatabase


def test_systemdb_migration(dbos: DBOS) -> None:
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
    config: DBOSConfig, postgres_db_engine: sa.Engine
) -> None:
    system_database_url = f"{config['database_url']}_dbos_sys"
    sysdb_name = sa.make_url(system_database_url).database

    # Drop and recreate the system database
    with postgres_db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(sa.text(f"DROP DATABASE IF EXISTS {sysdb_name}"))
        connection.execute(sa.text(f"CREATE DATABASE {sysdb_name}"))

    sys_db = SystemDatabase(system_database_url=system_database_url, engine_kwargs={})
    # Run the deprecated Alembic migrations
    run_alembic_migrations(sys_db.engine)
    # Then, run the new migrations
    # Now launch DBOS, make sure it actually works
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
    config: DBOSConfig, postgres_db_engine: sa.Engine
) -> None:
    sysdb_name = "custom_sysdb_name"
    config["sys_db_name"] = sysdb_name

    # Clean up from previous runs
    with postgres_db_engine.connect() as connection:
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


def test_reset(config: DBOSConfig, postgres_db_engine: sa.Engine) -> None:
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

    with postgres_db_engine.connect() as c:
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
