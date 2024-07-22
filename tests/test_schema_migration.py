import os

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from dbos_transact import DBOS
from dbos_transact.dbos_config import ConfigFile
from dbos_transact.system_database import SystemSchema, get_sysdb_url

from . import conftest


def test_systemdb_migration():
    config = conftest.defaultConfig
    dbos = DBOS(config)

    # Clean up from previous runs
    db_url = conftest.get_db_url(config)
    engine = sa.create_engine(db_url)
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(sa.text("DROP DATABASE IF EXISTS dbostestpy_dbos_sys"))
        connection.execute(sa.text("CREATE DATABASE dbostestpy_dbos_sys"))
    engine.dispose()

    # Test migrating up
    dbos.migrate()

    # Make sure all tables exist
    sysdb_url = get_sysdb_url(config)
    engine = sa.create_engine(sysdb_url)
    with engine.connect() as connection:
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

    with engine.connect() as connection:
        with pytest.raises(sa.exc.ProgrammingError) as exc_info:
            sql = SystemSchema.workflow_status.select()
            result = connection.execute(sql)
        assert "does not exist" in str(exc_info.value)
    engine.dispose()


""" Make sure we support system DB with a custom name """


def test_custom_sysdb_name_migration():
    config = conftest.defaultConfig
    sysdb_name = "custom_sysdb_name"
    config["database"]["sys_db_name"] = sysdb_name
    dbos = DBOS(config)

    # Clean up from previous runs
    db_url = conftest.get_db_url(config)
    engine = sa.create_engine(db_url)
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(sa.text(f"DROP DATABASE IF EXISTS {sysdb_name}"))
        connection.execute(sa.text(f"CREATE DATABASE {sysdb_name}"))
    engine.dispose()

    # Test migrating up
    dbos.migrate()

    # Make sure all tables exist
    sysdb_url = get_sysdb_url(config)
    engine = sa.create_engine(sysdb_url)
    with engine.connect() as connection:
        sql = SystemSchema.workflow_status.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

    # Test migrating down
    rollback_system_db(sysdb_url=sysdb_url)

    with engine.connect() as connection:
        with pytest.raises(sa.exc.ProgrammingError) as exc_info:
            sql = SystemSchema.workflow_status.select()
            result = connection.execute(sql)
        assert "does not exist" in str(exc_info.value)
    engine.dispose()


""" 
    Utility functions for tests
"""


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
