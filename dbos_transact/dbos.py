import logging
import os
from typing import Optional

from .dbos_config import ConfigFile, load_config
from .system_database import SystemDatabase


class DBOS:
    logger = logging.getLogger("dbos")

    def __init__(self, config: Optional[ConfigFile] = None) -> None:
        self.logger.info("Initializing DBOS!")
        if config is None:
            config = load_config()
        self.config = config
        self.system_database = SystemDatabase(config)

    def example(self) -> str:
        return self.config["database"]["username"]

    def migrate(self) -> None:
        self.logger.info("Migrating system database!")
        migration_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "migrations"
        )
        self.system_database.migrate(migration_dir=migration_dir)
