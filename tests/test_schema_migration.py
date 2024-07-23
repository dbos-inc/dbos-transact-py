import os

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from dbos_transact import DBOS
from dbos_transact.schemas.system_database import SystemSchema


def test_systemdb_migration(dbos):
    # Make sure all tables exist
    sysdb_url = dbos.system_database.system_db_url
    sys_db_engine = sa.create_engine(sysdb_url)
    with sys_db_engine.connect() as connection:
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
    rollback_system_db(sysdb_url=sysdb_url)

    with sys_db_engine.connect() as connection:
        with pytest.raises(sa.exc.ProgrammingError) as exc_info:
            sql = SystemSchema.workflow_status.select()
            result = connection.execute(sql)
        assert "does not exist" in str(exc_info.value)
    sys_db_engine.dispose()


def test_custom_sysdb_name_migration(config, postgres_db_engine):
    sysdb_name = "custom_sysdb_name"
    config["database"]["sys_db_name"] = sysdb_name

    # Clean up from previous runs
    with postgres_db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(sa.text(f"DROP DATABASE IF EXISTS {sysdb_name}"))

    # Test migrating up
    dbos = DBOS(config)

    # Make sure all tables exist
    with dbos.system_database.engine.connect() as connection:
        sql = SystemSchema.workflow_status.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

    # Test migrating down
    rollback_system_db(sysdb_url=dbos.system_database.system_db_url)

    with dbos.system_database.engine.connect() as connection:
        with pytest.raises(sa.exc.ProgrammingError) as exc_info:
            sql = SystemSchema.workflow_status.select()
            result = connection.execute(sql)
        assert "does not exist" in str(exc_info.value)
    dbos.destroy()


def rollback_system_db(sysdb_url: str) -> None:
    migration_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        "dbos_transact",
        "migrations",
    )
    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", migration_dir)
    alembic_cfg.set_main_option("sqlalchemy.url", sysdb_url)
    command.downgrade(alembic_cfg, "base")  # Rollback all migrations
