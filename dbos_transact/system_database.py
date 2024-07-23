import os

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from .dbos_config import ConfigFile
from .logger import dbos_logger
from .schemas.system_database import SystemSchema


class SystemDatabase:

    def __init__(self, config: ConfigFile):
        self.config = config

        sysdb_name = (
            config["database"]["sys_db_name"]
            if "sys_db_name" in config["database"] and config["database"]["sys_db_name"]
            else config["database"]["app_db_name"] + SystemSchema.sysdb_suffix
        )

        # If the system database does not already exist, create it
        postgres_db_url = sa.URL.create(
            "postgresql",
            username=config["database"]["username"],
            password=config["database"]["password"],
            host=config["database"]["hostname"],
            port=config["database"]["port"],
            database="postgres",
        )
        engine = sa.create_engine(postgres_db_url)
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            if not conn.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname=:db_name"),
                parameters={"db_name": sysdb_name},
            ).scalar():
                conn.execute(sa.text(f"CREATE DATABASE {sysdb_name}"))
        engine.dispose()

        system_db_url = sa.URL.create(
            "postgresql",
            username=config["database"]["username"],
            password=config["database"]["password"],
            host=config["database"]["hostname"],
            port=config["database"]["port"],
            database=sysdb_name,
        )
        self.system_db_url = system_db_url.render_as_string(hide_password=False)

        # Create a pool
        self.engine = sa.create_engine(system_db_url, pool_size=10, pool_timeout=30)

        # Run the migration
        self.migrate()

    # Destroy the pool when finished
    def destroy(self) -> None:
        self.engine.dispose()

    def migrate(self) -> None:
        dbos_logger.info("Migrating system database!")
        migration_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "migrations"
        )
        alembic_cfg = Config()
        alembic_cfg.set_main_option("script_location", migration_dir)
        alembic_cfg.set_main_option("sqlalchemy.url", self.system_db_url)
        command.upgrade(alembic_cfg, "head")
