import logging
from typing import Optional

from .dbos_config import ConfigFile, load_config


class DBOS:
    logger = logging.getLogger("dbos")

    def __init__(self, config: Optional[ConfigFile] = None) -> None:
        self.logger.info("Initializing DBOS!")
        if config is None:
            config = load_config()
        self.config = config

    def example(self) -> str:
        return self.config["database"]["username"]
