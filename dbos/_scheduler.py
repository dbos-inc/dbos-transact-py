import random
import threading
import traceback
from datetime import datetime, timezone

from ._context import SetWorkflowID
from ._croniter import croniter  # type: ignore
from ._logger import dbos_logger
from ._sys_db import WorkflowSchedule


def _run_scheduled_workflow(
    schedule: WorkflowSchedule,
    next_exec_time: datetime,
) -> None:
    from ._dbos import _get_dbos_instance

    dbos = _get_dbos_instance()
    workflow_name = schedule["workflow_name"]
    func = dbos._registry.workflow_info_map.get(workflow_name)
    if func is None:
        dbos_logger.warning(
            f"Scheduled workflow '{workflow_name}' is not registered, skipping"
        )
        return
    scheduler_queue = dbos._registry.get_internal_queue()
    workflow_id = f"sched-{schedule["schedule_name"]}-{next_exec_time.isoformat()}"
    if not dbos._sys_db.get_workflow_status(workflow_id):
        with SetWorkflowID(workflow_id):
            scheduler_queue.enqueue(func, next_exec_time, datetime.now(timezone.utc))


def dynamic_scheduler_loop(stop_event: threading.Event) -> None:
    from ._dbos import _get_dbos_instance

    dbos = _get_dbos_instance()

    while not stop_event.is_set():
        try:
            schedules = dbos._sys_db.list_schedules()
        except Exception:
            dbos_logger.warning(
                f"Exception polling schedules: {traceback.format_exc()}"
            )
            if stop_event.wait(timeout=1.0):
                return
            continue

        now = datetime.now(timezone.utc)
        earliest_wait: float = 60.0  # default poll interval

        for schedule in schedules:
            cron_expr = schedule["schedule"]
            try:
                it = croniter(cron_expr, now, second_at_beginning=True)
            except Exception:
                dbos_logger.warning(
                    f"Invalid cron expression '{cron_expr}' for schedule "
                    f"'{schedule['schedule_name']}', skipping"
                )
                continue

            next_exec_time: datetime = it.get_next(datetime)
            wait = (next_exec_time - now).total_seconds()
            if wait <= 0:
                try:
                    _run_scheduled_workflow(schedule, next_exec_time)
                except Exception:
                    dbos_logger.warning(
                        f"Exception running scheduled workflow "
                        f"'{schedule['schedule_name']}': {traceback.format_exc()}"
                    )
            else:
                earliest_wait = min(earliest_wait, wait)

        # Sleep until the earliest upcoming execution (with jitter)
        max_jitter = min(earliest_wait / 10, 10)
        jitter = random.uniform(0, max_jitter)
        if stop_event.wait(timeout=earliest_wait + jitter):
            return
