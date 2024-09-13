from typing import Optional

from dbos.system_database import SystemDatabase


class Queue:

    def __init__(
        self, sys_db: SystemDatabase, name: str, concurrency: Optional[int]
    ) -> None:
        self.sys_db = sys_db
        self.name = name
        self.concurrency = concurrency
