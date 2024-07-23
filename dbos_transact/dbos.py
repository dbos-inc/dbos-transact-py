from typing import Optional

from .application_database import ApplicationDatabase
from .dbos_config import ConfigFile, load_config
from .logger import config_logger, dbos_logger
from .system_database import SystemDatabase


class DBOS:
    def __init__(self, config: Optional[ConfigFile] = None) -> None:
        if config is None:
            config = load_config()
        config_logger(config)
        dbos_logger.info("Initializing DBOS!")
        self.config = config
        self.system_database = SystemDatabase(config)
        self.application_database = ApplicationDatabase(config)

    def example(self) -> str:
        return self.config["database"]["username"]

    def destroy(self) -> None:
        self.system_database.destroy()
        self.application_database.destroy()
