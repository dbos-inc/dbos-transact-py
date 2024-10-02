import threading
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable

from dbos.logger import dbos_logger
from dbos.queue import Queue

if TYPE_CHECKING:
    from dbos.dbos import _DBOSRegistry

from ..context import SetWorkflowID
from .croniter import croniter  # type: ignore

ScheduledWorkflow = Callable[[datetime, datetime], None]

scheduler_queue: Queue


def scheduler_loop(
    func: ScheduledWorkflow, cron: str, stop_event: threading.Event
) -> None:
    try:
        iter = croniter(cron, datetime.now(timezone.utc), second_at_beginning=True)
    except Exception as e:
        dbos_logger.error(
            f'Cannot run scheduled function {func.__name__}. Invalid crontab "{cron}"'
        )
    while not stop_event.is_set():
        nextExecTime = iter.get_next(datetime)
        sleepTime = nextExecTime - datetime.now(timezone.utc)
        if stop_event.wait(timeout=sleepTime.total_seconds()):
            return
        with SetWorkflowID(f"sched-{func.__qualname__}-{nextExecTime.isoformat()}"):
            scheduler_queue.enqueue(func, nextExecTime, datetime.now(timezone.utc))


def scheduled(
    dbosreg: "_DBOSRegistry", cron: str
) -> Callable[[ScheduledWorkflow], ScheduledWorkflow]:
    def decorator(func: ScheduledWorkflow) -> ScheduledWorkflow:
        global scheduler_queue
        scheduler_queue = Queue("_dbos_internal_queue")
        stop_event = threading.Event()
        dbosreg.register_poller(stop_event, scheduler_loop, func, cron, stop_event)
        return func

    return decorator
