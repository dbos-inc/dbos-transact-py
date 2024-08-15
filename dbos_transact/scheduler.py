import threading
import time
from datetime import datetime, timedelta
from typing import Callable

from .context import SetWorkflowUUID

ScheduledWorkflow = Callable[[datetime, datetime], None]


def scheduler_loop(
    func: ScheduledWorkflow, interval: float, stop_event: threading.Event
) -> None:
    while not stop_event.is_set():
        nextExecTime = datetime.now() + timedelta(seconds=interval)
        sleepTime = nextExecTime - datetime.now()
        if stop_event.wait(timeout=sleepTime.total_seconds()):
            return
        with SetWorkflowUUID(f"sched-{func.__qualname__}-{nextExecTime.isoformat()}"):
            func(nextExecTime, datetime.now())
