from alembic import command
from alembic.config import Config
from sqlalchemy import URL

from .dbos_config import ConfigFile
from .schemas.system_database import SystemSchema


class SystemDatabase:

    def __init__(self, config: ConfigFile):
        self.config = config

        sysdb_name = config["database"]["app_db_name"] + SystemSchema.sysdb_suffix
        if "sys_db_name" in config["database"] and config["database"]["sys_db_name"]:
            sysdb_name = config["database"]["sys_db_name"]
        db_url = URL.create(
            "postgresql",
            username=config["database"]["username"],
            password=config["database"]["password"],
            host=config["database"]["hostname"],
            port=config["database"]["port"],
            database=sysdb_name,
        )
        self.db_url = db_url.render_as_string(hide_password=False)

    def migrate(self, migration_dir: str) -> None:
        alembic_cfg = Config()
        alembic_cfg.set_main_option("script_location", migration_dir)
        alembic_cfg.set_main_option("sqlalchemy.url", self.db_url)
        command.upgrade(alembic_cfg, "head")
