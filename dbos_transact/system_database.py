from alembic import command
from alembic.config import Config
from sqlalchemy import URL

from .dbos_config import ConfigFile
from .schemas.system_database import SystemSchema


def migrate_system_db(sysdb_url: str, migration_dir: str) -> None:
    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", migration_dir)
    alembic_cfg.set_main_option("sqlalchemy.url", sysdb_url)
    command.upgrade(alembic_cfg, "head")  # Upgrade to the latest migration


def get_sysdb_url(config: ConfigFile) -> str:
    sysdb_name = config["database"]["app_db_name"] + SystemSchema.sysdb_suffix
    if "sys_db_name" in config["database"] and config["database"]["sys_db_name"]:
        sysdb_name = config["database"][
            "sys_db_name"
        ]  # Use the custom name if provided
    db_url = URL.create(
        "postgresql",
        username=config["database"]["username"],
        password=config["database"]["password"],
        host=config["database"]["hostname"],
        port=config["database"]["port"],
        database=sysdb_name,
    )
    return db_url.render_as_string(hide_password=False)
