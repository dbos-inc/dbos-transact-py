from dbos._sys_db import SystemDatabase
from dbos._utils import run_coroutine


def wait_for_buffer_flush_sync(sys_db: SystemDatabase) -> None:
    run_coroutine(sys_db.wait_for_buffer_flush())
