import os
import re

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

# Public API
from dbos import DBOS, DBOSConfig

# Private API because this is a unit test
from dbos._schemas.system_database import SystemSchema


def test_systemdb_migration(dbos: DBOS) -> None:
    # Make sure all tables exist
    with dbos._sys_db.engine.connect() as connection:
        sql = SystemSchema.workflow_status.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        sql = SystemSchema.workflow_inputs.select()
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

        sql = SystemSchema.scheduler_state.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

    # Test migrating down
    rollback_system_db(
        sysdb_url=dbos._sys_db.engine.url.render_as_string(hide_password=False)
    )

    with dbos._sys_db.engine.connect() as connection:
        with pytest.raises(sa.exc.ProgrammingError) as exc_info:
            sql = SystemSchema.workflow_status.select()
            result = connection.execute(sql)
        assert "does not exist" in str(exc_info.value)


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

    # Test migrating down
    rollback_system_db(
        sysdb_url=dbos._sys_db.engine.url.render_as_string(hide_password=False)
    )

    with dbos._sys_db.engine.connect() as connection:
        with pytest.raises(sa.exc.ProgrammingError) as exc_info:
            sql = SystemSchema.workflow_status.select()
            result = connection.execute(sql)
        assert "does not exist" in str(exc_info.value)
    DBOS.destroy()


def rollback_system_db(sysdb_url: str) -> None:
    migration_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        "dbos",
        "_migrations",
    )
    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", migration_dir)
    escaped_conn_string = re.sub(
        r"%(?=[0-9A-Fa-f]{2})",
        "%%",
        sysdb_url,
    )
    alembic_cfg.set_main_option("sqlalchemy.url", escaped_conn_string)
    command.downgrade(alembic_cfg, "base")  # Rollback all migrations


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
