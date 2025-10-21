import random
import threading
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Coroutine

from ._logger import dbos_logger
from ._queue import Queue

if TYPE_CHECKING:
    from ._dbos import DBOSRegistry

from ._context import SetWorkflowID
from ._croniter import croniter  # type: ignore
from ._registrations import get_dbos_func_name

ScheduledWorkflow = (
    Callable[[datetime, datetime], None]
    | Callable[[datetime, datetime], Coroutine[Any, Any, None]]
)


def scheduler_loop(
    func: ScheduledWorkflow, cron: str, stop_event: threading.Event
) -> None:
    from dbos._dbos import _get_dbos_instance

    dbos = _get_dbos_instance()
    scheduler_queue = dbos._registry.get_internal_queue()
    try:
        iter = croniter(cron, datetime.now(timezone.utc), second_at_beginning=True)
    except Exception:
        dbos_logger.error(
            f'Cannot run scheduled function {get_dbos_func_name(func)}. Invalid crontab "{cron}"'
        )
        raise
    while not stop_event.is_set():
        next_exec_time = iter.get_next(datetime)
        sleep_time = (next_exec_time - datetime.now(timezone.utc)).total_seconds()
        sleep_time = max(0, sleep_time)
        # To prevent a "thundering herd" problem in a distributed setting,
        # apply jitter of up to 10% the sleep time, capped at 10 seconds
        max_jitter = min(sleep_time / 10, 10)
        jitter = random.uniform(0, max_jitter)
        if stop_event.wait(timeout=sleep_time + jitter):
            return
        try:
            workflowID = (
                f"sched-{get_dbos_func_name(func)}-{next_exec_time.isoformat()}"
            )
            if not dbos._sys_db.get_workflow_status(workflowID):
                with SetWorkflowID(workflowID):
                    scheduler_queue.enqueue(
                        func, next_exec_time, datetime.now(timezone.utc)
                    )
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
        except Exception:
            raise ValueError(
                f'Invalid crontab "{cron}" for scheduled function function {get_dbos_func_name(func)}.'
            )
        stop_event = threading.Event()
        dbosreg.register_poller(stop_event, scheduler_loop, func, cron, stop_event)
        return func

    return decorator
