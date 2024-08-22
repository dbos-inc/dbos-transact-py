import threading
from datetime import datetime, timezone
from typing import Callable

from ..context import SetWorkflowUUID
from ..logger import dbos_logger
from .croniter import croniter  # type: ignore

ScheduledWorkflow = Callable[[datetime, datetime], None]


def scheduler_loop(
    func: ScheduledWorkflow, cron: str, stop_event: threading.Event
) -> None:
    iter = croniter(cron, datetime.now(timezone.utc))
    while not stop_event.is_set():
        nextExecTime = iter.get_next(datetime)
        sleepTime = nextExecTime - datetime.now(timezone.utc)
        if stop_event.wait(timeout=sleepTime.total_seconds()):
            return
        with SetWorkflowUUID(f"sched-{func.__qualname__}-{nextExecTime.isoformat()}"):
            try:
                func(nextExecTime, datetime.now(timezone.utc))
            except Exception as e:
                dbos_logger.error(
                    f"Exception encountered in scheduled workflow: {repr(e)}"
                )
                pass  # Let the thread keep running
