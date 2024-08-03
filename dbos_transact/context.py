import os

from .logger import dbos_logger


class DBOSContext:
    def __init__(self) -> None:
        self.executor_id = os.environ.get("DBOS__VMID", "local")
        self.app_version = os.environ.get("DBOS__APPVERSION", "")
        self.app_id = os.environ.get("DBOS__APPID", "")
        self.logger = dbos_logger
