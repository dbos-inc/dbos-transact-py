import logging
import os
from typing import Optional

from .dbos_config import ConfigFile, load_config
from .system_database import SystemDatabase

dbos_logger = logging.getLogger("dbos")


class DBOS:
    def __init__(self, config: Optional[ConfigFile] = None) -> None:
        if config is None:
            config = load_config()

        # Configure the DBOS logger. Log to the console by default.
        if not dbos_logger.handlers:
            dbos_logger.propagate = False
            console_handler = logging.StreamHandler()
            log_level = config.get("telemetry", {}).get("logs", {}).get("logLevel")
            if log_level is not None:
                console_handler.setLevel(log_level)
            console_formatter = logging.Formatter(
                "%(asctime)s [%(levelname)8s] (%(name)s:%(filename)s:%(lineno)s) %(message)s",
                datefmt="%H:%M:%S",
            )
            console_handler.setFormatter(console_formatter)
            dbos_logger.addHandler(console_handler)

        dbos_logger.info("Initializing DBOS!")
        self.config = config
        self.system_database = SystemDatabase(config)

    def example(self) -> str:
        return self.config["database"]["username"]

    def migrate(self) -> None:
        dbos_logger.info("Migrating system database!")
        migration_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "migrations"
        )
        self.system_database.migrate(migration_dir=migration_dir)
