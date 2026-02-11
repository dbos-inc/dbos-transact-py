import random
import threading
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ._context import SetWorkflowID
from ._croniter import croniter  # type: ignore
from ._error import DBOSException
from ._logger import dbos_logger
from ._serialization import Serializer, WorkflowInputs
from ._sys_db import WorkflowSchedule, WorkflowStatusInternal, WorkflowStatusString
from ._utils import INTERNAL_QUEUE_NAME

if TYPE_CHECKING:
    from ._sys_db import SystemDatabase


class _ScheduleThread:
    """Manages a dedicated thread for a single cron schedule."""

    def __init__(self, schedule: WorkflowSchedule, serializer: Serializer):
        self.schedule_name: str = schedule["schedule_name"]
        self.workflow_name: str = schedule["workflow_name"]
        self.cron: str = schedule["schedule"]
        self.serialized_context: str = schedule["context"]
        self.context: Any = serializer.deserialize(self.serialized_context)
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
                        scheduler_queue.enqueue(func, next_exec_time, self.context)
            except Exception:
                dbos_logger.warning(
                    f"Exception in schedule '{self.schedule_name}': "
                    f"{traceback.format_exc()}"
                )

    @staticmethod
    def is_active(schedule: WorkflowSchedule) -> bool:
        return schedule.get("status", "ACTIVE") == "ACTIVE"

    def stop(self, join: bool = False) -> None:
        self._stop_event.set()
        if join:
            self._thread.join(timeout=5.0)


def _enqueue_scheduled_workflow(
    sys_db: "SystemDatabase",
    workflow_name: str,
    scheduled_at: datetime,
    workflow_id: str,
    context: Any = None,
) -> None:
    """Enqueue a single scheduled workflow execution via init_workflow."""
    inputs: WorkflowInputs = {"args": (scheduled_at, context), "kwargs": {}}
    status: WorkflowStatusInternal = {
        "workflow_uuid": workflow_id,
        "status": WorkflowStatusString.ENQUEUED.value,
        "name": workflow_name,
        "class_name": None,
        "queue_name": INTERNAL_QUEUE_NAME,
        "app_version": None,
        "config_name": None,
        "authenticated_user": None,
        "assumed_role": None,
        "authenticated_roles": None,
        "output": None,
        "error": None,
        "created_at": None,
        "updated_at": None,
        "executor_id": None,
        "recovery_attempts": None,
        "app_id": None,
        "workflow_timeout_ms": None,
        "workflow_deadline_epoch_ms": None,
        "deduplication_id": None,
        "priority": 0,
        "inputs": sys_db.serializer.serialize(inputs),
        "queue_partition_key": None,
        "forked_from": None,
        "parent_workflow_id": None,
        "started_at_epoch_ms": None,
        "owner_xid": None,
    }
    sys_db.init_workflow(
        status,
        max_recovery_attempts=None,
        owner_xid=None,
        is_dequeued_request=False,
        is_recovery_request=False,
    )


def backfill_schedule(
    sys_db: "SystemDatabase",
    schedule_name: str,
    start: datetime,
    end: datetime,
) -> list[str]:
    """Enqueue all scheduled executions between start and end. Returns workflow IDs."""
    schedule = sys_db.get_schedule(schedule_name)
    if schedule is None:
        raise DBOSException(f"Schedule '{schedule_name}' does not exist")
    context = sys_db.serializer.deserialize(schedule["context"])
    it = croniter(schedule["schedule"], start, second_at_beginning=True)
    workflow_ids: list[str] = []
    while True:
        next_time = it.get_next(datetime)
        if next_time >= end:
            break
        workflow_id = f"sched-{schedule_name}-{next_time.isoformat()}"
        if not sys_db.get_workflow_status(workflow_id):
            _enqueue_scheduled_workflow(
                sys_db, schedule["workflow_name"], next_time, workflow_id, context
            )
        workflow_ids.append(workflow_id)
    return workflow_ids


def trigger_schedule(sys_db: "SystemDatabase", schedule_name: str) -> str:
    """Enqueue the scheduled workflow at the current time. Returns the workflow ID."""
    schedule = sys_db.get_schedule(schedule_name)
    if schedule is None:
        raise DBOSException(f"Schedule '{schedule_name}' does not exist")
    context = sys_db.serializer.deserialize(schedule["context"])
    now = datetime.now(timezone.utc)
    workflow_id = f"sched-{schedule_name}-trigger-{now.isoformat()}"
    _enqueue_scheduled_workflow(
        sys_db, schedule["workflow_name"], now, workflow_id, context
    )
    return workflow_id


def dynamic_scheduler_loop(
    stop_event: threading.Event, polling_interval_sec: float
) -> None:
    from ._dbos import _get_dbos_instance

    dbos = _get_dbos_instance()

    # Active schedule threads keyed by schedule_id
    schedule_threads: dict[str, _ScheduleThread] = {}

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
        for schedule_id in list(schedule_threads.keys()):
            if schedule_id not in current_ids:
                schedule_threads[schedule_id].stop()
                del schedule_threads[schedule_id]

        # Start, restart, or stop threads based on schedule state
        for schedule in schedules:
            schedule_id = schedule["schedule_id"]
            schedule_thread = schedule_threads.get(schedule_id)
            if not _ScheduleThread.is_active(schedule):
                # Paused — stop the thread if running
                if schedule_thread is not None:
                    schedule_thread.stop()
                    del schedule_threads[schedule_id]
            elif schedule_thread is None:
                schedule_threads[schedule_id] = _ScheduleThread(
                    schedule, dbos._sys_db.serializer
                )

        if stop_event.wait(timeout=polling_interval_sec):
            break

    # Clean up all threads on shutdown
    for st in schedule_threads.values():
        st.stop(join=True)
