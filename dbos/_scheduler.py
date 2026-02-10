import random
import threading
import traceback
from datetime import datetime, timezone

from ._context import SetWorkflowID
from ._croniter import croniter  # type: ignore
from ._logger import dbos_logger
from ._sys_db import WorkflowSchedule


class _ScheduleThread:
    """Manages a dedicated thread for a single cron schedule."""

    def __init__(self, schedule: WorkflowSchedule):
        self.schedule_name: str = schedule["schedule_name"]
        self.workflow_name: str = schedule["workflow_name"]
        self.cron: str = schedule["schedule"]
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        from ._dbos import _get_dbos_instance

        dbos = _get_dbos_instance()
        func = dbos._registry.workflow_info_map.get(self.workflow_name)
        if func is None:
            dbos_logger.warning(
                f"Scheduled workflow '{self.workflow_name}' is not registered, skipping"
            )
            return
        scheduler_queue = dbos._registry.get_internal_queue()
        try:
            it = croniter(
                self.cron, datetime.now(timezone.utc), second_at_beginning=True
            )
        except Exception:
            dbos_logger.error(
                f"Cannot run schedule '{self.schedule_name}'. "
                f'Invalid crontab "{self.cron}"'
            )
            return
        while not self._stop_event.is_set():
            next_exec_time = it.get_next(datetime)
            sleep_time = (next_exec_time - datetime.now(timezone.utc)).total_seconds()
            # To prevent a "thundering herd" problem in a distributed setting,
            # apply jitter of up to 10% the sleep time, capped at 10 seconds
            sleep_time = max(0, sleep_time)
            max_jitter = min(sleep_time / 10, 10)
            jitter = random.uniform(0, max_jitter)
            if self._stop_event.wait(timeout=sleep_time + jitter):
                return
            try:
                workflow_id = f"sched-{self.schedule_name}-{next_exec_time.isoformat()}"
                if not dbos._sys_db.get_workflow_status(workflow_id):
                    with SetWorkflowID(workflow_id):
                        scheduler_queue.enqueue(func, next_exec_time)
            except Exception:
                dbos_logger.warning(
                    f"Exception in schedule '{self.schedule_name}': "
                    f"{traceback.format_exc()}"
                )

    def changed(self, schedule: WorkflowSchedule) -> bool:
        return (
            self.workflow_name != schedule["workflow_name"]
            or self.cron != schedule["schedule"]
        )

    def stop(self, join: bool = False) -> None:
        self._stop_event.set()
        if join:
            self._thread.join(timeout=5.0)


def dynamic_scheduler_loop(
    stop_event: threading.Event, polling_interval_sec: float
) -> None:
    from ._dbos import _get_dbos_instance

    dbos = _get_dbos_instance()

    # Active schedule threads keyed by schedule_id
    active_schedules: dict[str, _ScheduleThread] = {}

    while not stop_event.is_set():
        try:
            schedules = dbos._sys_db.list_schedules()
        except Exception:
            dbos_logger.warning(
                f"Exception polling schedules: {traceback.format_exc()}"
            )
            if stop_event.wait(timeout=polling_interval_sec):
                break
            continue

        current_ids = {s["schedule_id"] for s in schedules}

        # Stop threads for deleted schedules
        for schedule_id in list(active_schedules.keys()):
            if schedule_id not in current_ids:
                active_schedules[schedule_id].stop()
                del active_schedules[schedule_id]

        # Start or restart threads for new/changed schedules
        for schedule in schedules:
            schedule_id = schedule["schedule_id"]
            existing = active_schedules.get(schedule_id)
            if existing is None:
                active_schedules[schedule_id] = _ScheduleThread(schedule)
            elif existing.changed(schedule):
                existing.stop()
                active_schedules[schedule_id] = _ScheduleThread(schedule)

        if stop_event.wait(timeout=polling_interval_sec):
            break

    # Clean up all threads on shutdown
    for st in active_schedules.values():
        st.stop(join=True)
