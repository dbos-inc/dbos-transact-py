import logging
import os
from typing import Optional

from .dbos_config import ConfigFile, load_config
from .system_database import get_sysdb_url, migrate_system_db


class DBOS:
    logger = logging.getLogger("dbos")

    def __init__(self, config: Optional[ConfigFile] = None) -> None:
        self.logger.info("Initializing DBOS!")
        if config is None:
            config = load_config()
        self.config = config

    def example(self) -> str:
        return self.config["database"]["username"]

    def run_migrations(self) -> None:
        self.logger.info("Migrating system database!")
        sysdb_url = get_sysdb_url(self.config)
        migration_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "migrations")
        migrate_system_db(sysdb_url=sysdb_url, migration_dir=migration_dir)
