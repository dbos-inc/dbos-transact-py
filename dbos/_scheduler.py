import heapq
import itertools
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    Tuple,
)
from zoneinfo import ZoneInfo

from croniter import croniter

from ._error import DBOSException
from ._logger import dbos_logger
from ._serialization import Serializer, WorkflowInputs
from ._sys_db import WorkflowSchedule, WorkflowStatusInternal, WorkflowStatusString
from ._utils import INTERNAL_QUEUE_NAME

if TYPE_CHECKING:
    from ._sys_db import SystemDatabase

ScheduledWorkflow = (
    Callable[[datetime, Any], None]
    | Callable[[datetime, Any], Coroutine[Any, Any, None]]
)

# Most scheduled runs enqueued in one transaction.
_FIRE_BATCH_SIZE = 1000


@dataclass
class _ScheduleDefinition:
    """What a schedule's runs are enqueued with."""

    schedule_name: str
    workflow_name: str
    class_name: Optional[str]
    context: Any
    queue_name: Optional[str]
    application_name: Optional[str]
    shard_key: Optional[str]

    @staticmethod
    def of(schedule: WorkflowSchedule, serializer: Serializer) -> "_ScheduleDefinition":
        return _ScheduleDefinition(
            schedule_name=schedule["schedule_name"],
            workflow_name=schedule["workflow_name"],
            class_name=schedule["workflow_class_name"],
            context=serializer.deserialize(schedule["context"]),
            queue_name=schedule.get("queue_name"),
            application_name=schedule.get("application_name"),
            shard_key=schedule.get("shard_key"),
        )


class _ScheduleEntry:
    """An active schedule in the scheduler loop and its next tick."""

    def __init__(self, schedule: WorkflowSchedule, serializer: Serializer):
        self.schedule_id: str = schedule["schedule_id"]
        # Definition snapshot; the loop re-arms the entry when it changes.
        self.signature: tuple[Any, ...] = self.compute_signature(schedule)
        tz_name = schedule.get("cron_timezone")
        tzinfo = ZoneInfo(tz_name) if tz_name else timezone.utc
        self.definition = _ScheduleDefinition.of(schedule, serializer)
        self._iter = croniter(
            schedule["schedule"], datetime.now(tzinfo), second_at_beginning=True
        )
        self.next_time: datetime = self._iter.get_next(datetime)

    def advance(self) -> None:
        self.next_time = self._iter.get_next(datetime)

    def advance_past(self, last_fired: datetime) -> None:
        while self.next_time <= last_fired:
            self.advance()

    @staticmethod
    def compute_signature(schedule: WorkflowSchedule) -> tuple[Any, ...]:
        # Definition fields only; excludes identity/lifecycle/runtime state.
        return (
            schedule["workflow_name"],
            schedule["workflow_class_name"],
            schedule["schedule"],
            schedule["context"],
            schedule.get("cron_timezone"),
            schedule.get("queue_name"),
            schedule.get("shard_key"),
        )

    @staticmethod
    def is_active(schedule: WorkflowSchedule) -> bool:
        return schedule.get("status", "ACTIVE") == "ACTIVE"


def _scheduled_status(
    definition: _ScheduleDefinition,
    scheduled_at: datetime,
    workflow_id: str,
    app_version: str,
    owner: Optional[str],
    serializer: Serializer,
) -> WorkflowStatusInternal:
    """The ENQUEUED status row of one scheduled run."""
    inputs: WorkflowInputs = {"args": (scheduled_at, definition.context), "kwargs": {}}
    return {
        "workflow_uuid": workflow_id,
        "status": WorkflowStatusString.ENQUEUED.value,
        "name": definition.workflow_name,
        "class_name": definition.class_name,
        "queue_name": (
            definition.queue_name if definition.queue_name else INTERNAL_QUEUE_NAME
        ),
        "app_version": app_version,
        "config_name": None,
        "authenticated_user": None,
        "assumed_role": None,
        "authenticated_roles": None,
        "created_at": None,
        "updated_at": None,
        "executor_id": None,
        "recovery_attempts": None,
        "app_id": None,
        "workflow_timeout_ms": None,
        "workflow_deadline_epoch_ms": None,
        "deduplication_id": None,
        "priority": 0,
        "inputs": serializer.serialize(inputs),
        "serialization": None,
        "queue_partition_key": None,
        "forked_from": None,
        "parent_workflow_id": None,
        "started_at_epoch_ms": None,
        "delay_until_epoch_ms": None,
        "attributes": None,
        "schedule_name": definition.schedule_name,
        "debounce_deadline_epoch_ms": None,
        "is_debounced": False,
        # Owned where possible: the internal queue's shared name cannot route an unclaimed row.
        "application_name": owner,
        "shard_key": definition.shard_key,
    }


def _owner(sys_db: "SystemDatabase", definition: _ScheduleDefinition) -> Optional[str]:
    # The schedule's owner routes its runs, whoever fires them; an unclaimed one falls back to the firing handle, which may have no identity itself.
    if definition.application_name is not None:
        return definition.application_name
    return sys_db.app_name


def _enqueue_ticks(
    sys_db: "SystemDatabase",
    ticks: List[Tuple[_ScheduleDefinition, datetime]],
    *,
    stamp_last_fired: bool,
) -> List[str]:
    """Enqueue these scheduled runs in one batch, skipping existing ones. Returns their workflow IDs."""
    # Scheduled workflows are always enqueued to their owner's latest application version.
    versions: Dict[Optional[str], str] = {}
    statuses: List[WorkflowStatusInternal] = []
    fired_at: Dict[str, str] = {}
    for definition, scheduled_at in ticks:
        owner = _owner(sys_db, definition)
        if owner not in versions:
            versions[owner] = sys_db.get_latest_application_version(
                application_name=owner
            )["version_name"]
        workflow_id = f"sched-{definition.schedule_name}-{scheduled_at.isoformat()}"
        statuses.append(
            _scheduled_status(
                definition,
                scheduled_at,
                workflow_id,
                versions[owner],
                owner,
                sys_db.serializer,
            )
        )
        if stamp_last_fired:
            fired_at[workflow_id] = scheduled_at.isoformat()
    sys_db.enqueue_scheduled_workflows(statuses, fired_at)
    return [status["workflow_uuid"] for status in statuses]


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
    definition = _ScheduleDefinition.of(schedule, sys_db.serializer)
    tz_name = schedule.get("cron_timezone")
    tz = ZoneInfo(tz_name) if tz_name else timezone.utc
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    start_in_tz = start.astimezone(tz)
    it = croniter(schedule["schedule"], start_in_tz, second_at_beginning=True)
    times: List[datetime] = []
    while True:
        next_time = it.get_next(datetime)
        if next_time >= end:
            break
        times.append(next_time)
    workflow_ids: list[str] = []
    for i in range(0, len(times), _FIRE_BATCH_SIZE):
        workflow_ids.extend(
            _enqueue_ticks(
                sys_db,
                [(definition, t) for t in times[i : i + _FIRE_BATCH_SIZE]],
                stamp_last_fired=False,
            )
        )
    return workflow_ids


def schedule_shard_key(sys_db: "SystemDatabase", schedule_name: str) -> Optional[str]:
    """The shard key a schedule's workflows run under."""
    schedule = sys_db.get_schedule(schedule_name)
    return schedule.get("shard_key") if schedule is not None else None


def trigger_schedule(sys_db: "SystemDatabase", schedule_name: str) -> str:
    """Enqueue the scheduled workflow at the current time. Returns the workflow ID."""
    schedule = sys_db.get_schedule(schedule_name)
    if schedule is None:
        raise DBOSException(f"Schedule '{schedule_name}' does not exist")
    definition = _ScheduleDefinition.of(schedule, sys_db.serializer)
    now = datetime.now(timezone.utc)
    workflow_id = f"sched-{schedule_name}-trigger-{now.isoformat()}"
    owner = _owner(sys_db, definition)
    app_version = sys_db.get_latest_application_version(application_name=owner)[
        "version_name"
    ]
    sys_db.init_workflow(
        _scheduled_status(
            definition, now, workflow_id, app_version, owner, sys_db.serializer
        ),
        creator_xid=None,
    )
    return workflow_id


# During the first minute after startup, poll for schedules every second so
# schedules registered around launch are picked up promptly rather than after
# a full polling interval.
_STARTUP_FAST_POLL_DURATION_SEC = 60.0
_STARTUP_FAST_POLL_INTERVAL_SEC = 1.0

# How long a standby keeps a tick no poll has shown fired, or three polling intervals if longer.
_BUFFER_MIN_SEC = 3600.0
# How often a process that isn't firing checks whether it has taken over.
_STANDBY_CHECK_SEC = 1.0


class _Scheduler:
    """Fires every active schedule from one thread, enqueueing each tick's runs in batches."""

    def __init__(self, sys_db: "SystemDatabase"):
        self._sys_db = sys_db
        # Active schedules keyed by schedule_id.
        self._entries: Dict[str, _ScheduleEntry] = {}
        # (next tick epoch, seq, entry), one item per entry in _entries; rebuilt by every poll.
        self._heap: List[Tuple[float, int, _ScheduleEntry]] = []
        self._seq = itertools.count()

    def _push(self, entry: _ScheduleEntry) -> None:
        heapq.heappush(
            self._heap, (entry.next_time.timestamp(), next(self._seq), entry)
        )

    def poll(self) -> None:
        """Sync the armed schedules with the database, then rebuild the heap from them."""
        try:
            self._reconcile()
        finally:
            # Even after a partial reconcile, so the heap never holds dropped entries or misses one.
            self._heap = [
                (entry.next_time.timestamp(), next(self._seq), entry)
                for entry in self._entries.values()
            ]
            heapq.heapify(self._heap)

    def _reconcile(self) -> None:
        schedules = self._sys_db.list_schedules(application_name=self._sys_db.app_name)
        current_ids = {s["schedule_id"] for s in schedules}
        for schedule_id in list(self._entries):
            if schedule_id not in current_ids:
                del self._entries[schedule_id]
        for schedule in schedules:
            schedule_id = schedule["schedule_id"]
            entry = self._entries.get(schedule_id)
            if not _ScheduleEntry.is_active(schedule):
                # Paused: drop it; resuming re-arms it, backfilling if enabled.
                self._entries.pop(schedule_id, None)
                continue
            if entry is not None:
                # Running: re-arm on a changed definition, without backfill.
                if entry.signature != _ScheduleEntry.compute_signature(schedule):
                    del self._entries[schedule_id]
                    self._arm(schedule)
                else:
                    self._skip_fired(entry, schedule)
                continue
            # Not running: arm it, then backfill missed executions if enabled; a broken one is retried next poll.
            entry = self._arm(schedule)
            if entry is None:
                continue
            if schedule.get("automatic_backfill") and schedule.get("last_fired_at"):
                try:
                    assert schedule["last_fired_at"]
                    last_fired = datetime.fromisoformat(schedule["last_fired_at"])
                    now = datetime.now(timezone.utc)
                    if last_fired < now:
                        backfill_schedule(
                            self._sys_db, schedule["schedule_name"], last_fired, now
                        )
                except Exception:
                    dbos_logger.warning(
                        f"Exception during automatic backfill for "
                        f"schedule '{schedule['schedule_name']}': "
                        f"{traceback.format_exc()}"
                    )

    def _skip_fired(self, entry: _ScheduleEntry, schedule: WorkflowSchedule) -> None:
        """Pass over the ticks this schedule's last_fired_at shows some process enqueued."""
        last_fired_at = schedule.get("last_fired_at")
        if not last_fired_at:
            return
        try:
            last_fired = datetime.fromisoformat(last_fired_at)
        except ValueError:
            return  # Unreadable: the entry keeps its ticks, refired at worst.
        try:
            entry.advance_past(last_fired)
        except Exception:
            self._drop(entry)

    def _arm(self, schedule: WorkflowSchedule) -> Optional[_ScheduleEntry]:
        """Register a new entry for this schedule, or log why it can't run and return None."""
        try:
            entry = _ScheduleEntry(schedule, self._sys_db.serializer)
        except Exception:
            if not croniter.is_valid(schedule["schedule"], second_at_beginning=True):
                dbos_logger.error(
                    f"Cannot run schedule '{schedule['schedule_name']}'. "
                    f'Invalid crontab "{schedule["schedule"]}"'
                )
            else:
                dbos_logger.error(
                    f"Cannot run schedule '{schedule['schedule_name']}': "
                    f"{traceback.format_exc()}"
                )
            return None
        self._entries[entry.schedule_id] = entry
        return entry

    def seconds_until_due(self, lag: float = 0.0) -> float:
        """Seconds until the earliest tick is lag seconds old, inf if none."""
        if not self._heap:
            return float("inf")
        return self._heap[0][0] + lag - time.time()

    def fire_due(self) -> None:
        """Fire every due tick, batch by batch."""
        now = time.time()
        due: List[_ScheduleEntry] = []
        while self._heap and self._heap[0][0] <= now:
            due.append(heapq.heappop(self._heap)[2])
        for i in range(0, len(due), _FIRE_BATCH_SIZE):
            self._fire(due[i : i + _FIRE_BATCH_SIZE])
        for entry in due:
            self._advance(entry)

    def skip_due(self, before: float) -> None:
        """Pass over ticks older than before without firing them."""
        while self._heap and self._heap[0][0] <= before:
            self._advance(heapq.heappop(self._heap)[2])

    def _advance(self, entry: _ScheduleEntry) -> None:
        # A late tick's successor may already be due; it fires on the next pass.
        try:
            entry.advance()
        except Exception:
            self._drop(entry)
            return
        self._push(entry)

    def _drop(self, entry: _ScheduleEntry) -> None:
        # Dropped; the next poll tries to arm it again.
        del self._entries[entry.schedule_id]
        dbos_logger.error(
            f"Cannot run schedule '{entry.definition.schedule_name}': "
            f"{traceback.format_exc()}"
        )

    def _fire(self, batch: List[_ScheduleEntry]) -> None:
        try:
            _enqueue_ticks(
                self._sys_db,
                [(entry.definition, entry.next_time) for entry in batch],
                stamp_last_fired=True,
            )
            return
        except Exception:
            if len(batch) == 1:
                dbos_logger.warning(
                    f"Exception in schedule '{batch[0].definition.schedule_name}': "
                    f"{traceback.format_exc()}"
                )
                return
            # Logged here, as the per-schedule retries may succeed and never show it.
            dbos_logger.warning(
                f"Exception enqueueing {len(batch)} scheduled workflows, "
                f"retrying each schedule alone: {traceback.format_exc()}"
            )
        # One failing schedule must not hold back the rest of its batch.
        for entry in batch:
            self._fire([entry])


def dynamic_scheduler_loop(
    stop_event: threading.Event,
    sys_db: "SystemDatabase",
    polling_interval_sec: float,
    leading: Callable[[], bool],
) -> None:
    """Poll schedules on every process, firing only while leading(). The others keep each
    tick until a poll shows it fired, so a taker fires all its predecessor missed."""
    scheduler = _Scheduler(sys_db)
    buffer_sec = max(_BUFFER_MIN_SEC, 3 * polling_interval_sec)

    startup_deadline = time.monotonic() + _STARTUP_FAST_POLL_DURATION_SEC

    def poll_timeout() -> float:
        if time.monotonic() < startup_deadline:
            return min(_STARTUP_FAST_POLL_INTERVAL_SEC, polling_interval_sec)
        return polling_interval_sec

    next_poll = time.monotonic()
    while not stop_event.is_set():
        if time.monotonic() >= next_poll:
            try:
                scheduler.poll()
            except Exception:
                dbos_logger.warning(
                    f"Exception polling schedules: {traceback.format_exc()}"
                )
            next_poll = time.monotonic() + poll_timeout()
        # Checked once per pass: a holder losing the lock mid-pass still fires all it took.
        firing = leading()
        try:
            if firing:
                scheduler.fire_due()
            else:
                scheduler.skip_due(time.time() - buffer_sec)
        except Exception:
            dbos_logger.warning(f"Exception firing schedules: {traceback.format_exc()}")
        until_poll = next_poll - time.monotonic()
        if firing:
            wait = min(until_poll, scheduler.seconds_until_due())
        else:
            wait = min(
                until_poll,
                scheduler.seconds_until_due(buffer_sec),
                _STANDBY_CHECK_SEC,
            )
        if wait > 0 and stop_event.wait(timeout=wait):
            break
