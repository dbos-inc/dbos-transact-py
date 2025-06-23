import threading
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable

from ._logger import dbos_logger
from ._queue import Queue

if TYPE_CHECKING:
    from ._dbos import DBOSRegistry

from ._context import SetWorkflowID
from ._croniter import croniter  # type: ignore
from ._registrations import get_dbos_func_name

ScheduledWorkflow = Callable[[datetime, datetime], None]

scheduler_queue: Queue


def scheduler_loop(
    func: ScheduledWorkflow, cron: str, stop_event: threading.Event
) -> None:
    try:
        iter = croniter(cron, datetime.now(timezone.utc), second_at_beginning=True)
    except Exception as e:
        dbos_logger.error(
            f'Cannot run scheduled function {get_dbos_func_name(func)}. Invalid crontab "{cron}"'
        )
    while not stop_event.is_set():
        nextExecTime = iter.get_next(datetime)
        sleepTime = nextExecTime - datetime.now(timezone.utc)
        if stop_event.wait(timeout=sleepTime.total_seconds()):
            return
        try:
            with SetWorkflowID(
                f"sched-{get_dbos_func_name(func)}-{nextExecTime.isoformat()}"
            ):
                scheduler_queue.enqueue(func, nextExecTime, datetime.now(timezone.utc))
        except Exception:
            dbos_logger.warning(
                f"Exception encountered in scheduler thread: {traceback.format_exc()})"
            )


def scheduled(
    dbosreg: "DBOSRegistry", cron: str
) -> Callable[[ScheduledWorkflow], ScheduledWorkflow]:
    def decorator(func: ScheduledWorkflow) -> ScheduledWorkflow:
        try:
            croniter(cron, datetime.now(timezone.utc), second_at_beginning=True)
        except Exception as e:
            raise ValueError(
                f'Invalid crontab "{cron}" for scheduled function function {get_dbos_func_name(func)}.'
            )

        global scheduler_queue
        scheduler_queue = dbosreg.get_internal_queue()
        stop_event = threading.Event()
        dbosreg.register_poller(stop_event, scheduler_loop, func, cron, stop_event)
        return func

    return decorator
