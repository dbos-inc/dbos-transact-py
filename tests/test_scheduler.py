import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from dbos import DBOS, DBOSClient, DBOSConfig, DBOSConfiguredInstance, Queue
from dbos._error import DBOSException
from dbos._utils import INTERNAL_QUEUE_NAME

from .conftest import default_config, retry_until_success


def test_schedule_crud(dbos: DBOS) -> None:
    @DBOS.workflow()
    def my_workflow(scheduled_at: datetime, ctx: Any) -> None:
        pass

    @DBOS.workflow()
    def other_workflow(scheduled_at: datetime, ctx: Any) -> None:
        pass

    # Create a schedule with context and timezone
    DBOS.create_schedule(
        schedule_name="test-schedule",
        workflow_fn=my_workflow,
        schedule="* * * * *",
        context={"env": "test"},
        cron_timezone="America/New_York",
    )

    # List schedules and verify
    schedules = DBOS.list_schedules()
    assert len(schedules) == 1
    assert schedules[0]["schedule_name"] == "test-schedule"
    assert schedules[0]["workflow_name"] == my_workflow.dbos_function_name  # type: ignore
    assert schedules[0]["schedule"] == "* * * * *"
    assert schedules[0]["context"] == {"env": "test"}
    assert schedules[0]["workflow_class_name"] is None
    assert schedules[0]["cron_timezone"] == "America/New_York"

    # Get schedule by name
    sched = DBOS.get_schedule("test-schedule")
    assert sched is not None
    assert sched["schedule_name"] == "test-schedule"
    assert sched["workflow_name"] == my_workflow.dbos_function_name  # type: ignore
    assert sched["schedule"] == "* * * * *"
    assert sched["schedule_id"] == schedules[0]["schedule_id"]
    assert sched["context"] == {"env": "test"}
    assert sched["workflow_class_name"] is None
    assert sched["cron_timezone"] == "America/New_York"

    # Get nonexistent schedule
    assert DBOS.get_schedule("nonexistent") is None

    # Reject invalid cron expression
    with pytest.raises(DBOSException, match="Invalid cron schedule"):
        DBOS.create_schedule(
            schedule_name="bad-schedule",
            workflow_fn=my_workflow,
            schedule="not a cron",
        )

    # Reject duplicate schedule name
    with pytest.raises(DBOSException, match="already exists"):
        DBOS.create_schedule(
            schedule_name="test-schedule",
            workflow_fn=my_workflow,
            schedule="0 0 * * *",
        )

    # Reject invalid timezone
    with pytest.raises(DBOSException, match="Invalid timezone"):
        DBOS.create_schedule(
            schedule_name="bad-tz",
            workflow_fn=my_workflow,
            schedule="* * * * *",
            cron_timezone="Fake/Zone",
        )

    # --- list_schedules filters ---
    DBOS.create_schedule(
        schedule_name="other-schedule",
        workflow_fn=other_workflow,
        schedule="0 0 * * *",
    )
    DBOS.pause_schedule("other-schedule")

    # Filter by status
    assert len(DBOS.list_schedules(status="ACTIVE")) == 1
    assert DBOS.list_schedules(status="ACTIVE")[0]["schedule_name"] == "test-schedule"
    assert len(DBOS.list_schedules(status="PAUSED")) == 1
    assert len(DBOS.list_schedules(status=["ACTIVE", "PAUSED"])) == 2
    assert len(DBOS.list_schedules(status="NONEXISTENT")) == 0

    # Filter by workflow_name
    assert len(DBOS.list_schedules(workflow_name=my_workflow.dbos_function_name)) == 1  # type: ignore
    assert len(DBOS.list_schedules(workflow_name=other_workflow.dbos_function_name)) == 1  # type: ignore
    assert len(DBOS.list_schedules(workflow_name=[my_workflow.dbos_function_name, other_workflow.dbos_function_name])) == 2  # type: ignore

    # Filter by schedule_name_prefix
    assert len(DBOS.list_schedules(schedule_name_prefix="test-")) == 1
    assert len(DBOS.list_schedules(schedule_name_prefix="other-")) == 1
    assert len(DBOS.list_schedules(schedule_name_prefix=["test-", "other-"])) == 2
    assert len(DBOS.list_schedules(schedule_name_prefix="nonexistent-")) == 0

    # Combine filters
    assert len(DBOS.list_schedules(status="ACTIVE", schedule_name_prefix="test-")) == 1
    assert len(DBOS.list_schedules(status="PAUSED", schedule_name_prefix="test-")) == 0

    # Delete schedules
    DBOS.delete_schedule("other-schedule")
    DBOS.delete_schedule("test-schedule")
    assert DBOS.get_schedule("test-schedule") is None
    assert len(DBOS.list_schedules()) == 0


def test_apply_schedules(dbos: DBOS) -> None:
    @DBOS.workflow()
    def wf_a(scheduled_at: datetime, ctx: Any) -> None:
        pass

    @DBOS.workflow()
    def wf_b(scheduled_at: datetime, ctx: Any) -> None:
        pass

    # Apply two schedules at once (sched-b has automatic_backfill and timezone)
    DBOS.apply_schedules(
        [
            {
                "schedule_name": "sched-a",
                "workflow_fn": wf_a,
                "schedule": "* * * * *",
                "context": {"region": "us"},
            },
            {
                "schedule_name": "sched-b",
                "workflow_fn": wf_b,
                "schedule": "0 0 * * *",
                "context": None,
                "automatic_backfill": True,
                "cron_timezone": "Europe/London",
            },
        ]
    )
    schedules = DBOS.list_schedules()
    assert len(schedules) == 2
    by_name = {s["schedule_name"]: s for s in schedules}
    assert by_name["sched-a"]["schedule"] == "* * * * *"
    assert by_name["sched-a"]["context"] == {"region": "us"}
    assert by_name["sched-a"]["automatic_backfill"] is False
    assert by_name["sched-a"]["cron_timezone"] is None
    assert by_name["sched-b"]["schedule"] == "0 0 * * *"
    assert by_name["sched-b"]["context"] is None
    assert by_name["sched-b"]["automatic_backfill"] is True
    assert by_name["sched-b"]["cron_timezone"] == "Europe/London"

    # Replace sched-a, add sched-c
    DBOS.apply_schedules(
        [
            {
                "schedule_name": "sched-a",
                "workflow_fn": wf_a,
                "schedule": "0 * * * *",
                "context": None,
            },
            {
                "schedule_name": "sched-c",
                "workflow_fn": wf_b,
                "schedule": "*/5 * * * *",
                "context": [1, 2, 3],
            },
        ]
    )
    schedules = DBOS.list_schedules()
    assert len(schedules) == 3
    by_name = {s["schedule_name"]: s for s in schedules}
    assert by_name["sched-a"]["schedule"] == "0 * * * *"
    assert by_name["sched-a"]["context"] is None
    assert by_name["sched-c"]["schedule"] == "*/5 * * * *"
    assert by_name["sched-c"]["context"] == [1, 2, 3]

    # Reject invalid cron
    with pytest.raises(DBOSException, match="Invalid cron schedule"):
        DBOS.apply_schedules(
            [
                {
                    "schedule_name": "bad",
                    "workflow_fn": wf_a,
                    "schedule": "not a cron",
                    "context": None,
                }
            ]
        )

    # Reject missing required fields
    with pytest.raises(DBOSException, match="missing required field 'schedule_name'"):
        DBOS.apply_schedules(
            [{"workflow_fn": wf_a, "schedule": "* * * * *", "context": None}]
        )
    with pytest.raises(DBOSException, match="missing required field 'workflow_fn'"):
        DBOS.apply_schedules(
            [{"schedule_name": "x", "schedule": "* * * * *", "context": None}]
        )
    with pytest.raises(DBOSException, match="missing required field 'schedule'"):
        DBOS.apply_schedules(
            [{"schedule_name": "x", "workflow_fn": wf_a, "context": None}]
        )

    # Reject call from within a workflow
    @DBOS.workflow()
    def bad_workflow() -> None:
        DBOS.apply_schedules(
            [
                {
                    "schedule_name": "x",
                    "workflow_fn": wf_a,
                    "schedule": "* * * * *",
                    "context": None,
                }
            ]
        )

    with pytest.raises(DBOSException, match="cannot be called from within a workflow"):
        DBOS.start_workflow(bad_workflow).get_result()

    # Clean up
    DBOS.delete_schedule("sched-a")
    DBOS.delete_schedule("sched-b")
    DBOS.delete_schedule("sched-c")
    assert len(DBOS.list_schedules()) == 0


def test_schedule_crud_from_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def target_workflow(scheduled_at: datetime, ctx: Any) -> None:
        pass

    @DBOS.workflow()
    def crud_workflow() -> None:
        DBOS.create_schedule(
            schedule_name="wf-schedule",
            workflow_fn=target_workflow,
            schedule="* * * * *",
            context={"from": "workflow"},
        )

        schedules = DBOS.list_schedules()
        assert len(schedules) == 1
        assert schedules[0]["schedule_name"] == "wf-schedule"
        assert schedules[0]["context"] == {"from": "workflow"}
        assert schedules[0]["workflow_class_name"] is None

        sched = DBOS.get_schedule("wf-schedule")
        assert sched is not None
        assert sched["schedule_name"] == "wf-schedule"
        assert sched["context"] == {"from": "workflow"}
        assert sched["workflow_class_name"] is None

        DBOS.delete_schedule("wf-schedule")
        assert DBOS.get_schedule("wf-schedule") is None

    handle = DBOS.start_workflow(crud_workflow)
    handle.get_result()

    steps = DBOS.list_workflow_steps(handle.workflow_id)
    step_names = [s["function_name"] for s in steps]
    assert step_names == [
        "DBOS.createSchedule",
        "DBOS.listSchedules",
        "DBOS.getSchedule",
        "DBOS.deleteSchedule",
        "DBOS.getSchedule",
    ]

    forked_handle = DBOS.fork_workflow(handle.workflow_id, len(steps))
    forked_handle.get_result()
    assert [
        s["function_name"] for s in DBOS.list_workflow_steps(forked_handle.workflow_id)
    ] == step_names


def test_dynamic_scheduler_fires(dbos: DBOS) -> None:
    received_a: list[Any] = []
    received_b: list[Any] = []

    @DBOS.workflow()
    def workflow_a(scheduled_at: datetime, ctx: Any) -> None:
        received_a.append(ctx)

    @DBOS.workflow()
    def workflow_b(scheduled_at: datetime, ctx: Any) -> None:
        received_b.append(ctx)

    DBOS.create_schedule(
        schedule_name="every-second-a",
        workflow_fn=workflow_a,
        schedule="* * * * * *",
        context={"id": "a"},
    )
    DBOS.create_schedule(
        schedule_name="every-second-b",
        workflow_fn=workflow_b,
        schedule="* * * * * *",
        context={"id": "b"},
        cron_timezone="America/New_York",
    )

    def check_both_fired_twice() -> None:
        assert len(received_a) >= 2
        assert all(c == {"id": "a"} for c in received_a)
        assert len(received_b) >= 2
        assert all(c == {"id": "b"} for c in received_b)

    retry_until_success(check_both_fired_twice)

    # Verify last_fired_at is set after firing
    sched_a = DBOS.get_schedule("every-second-a")
    sched_b = DBOS.get_schedule("every-second-b")
    assert sched_a is not None
    assert sched_b is not None
    assert sched_a["last_fired_at"] is not None
    assert sched_b["last_fired_at"] is not None
    # last_fired_at should be a valid ISO datetime
    last_fired_a = datetime.fromisoformat(sched_a["last_fired_at"])
    last_fired_b = datetime.fromisoformat(sched_b["last_fired_at"])
    assert last_fired_a <= datetime.now(timezone.utc)
    assert last_fired_b <= datetime.now(timezone.utc)
    # Schedule A has no timezone — last_fired_at should be in UTC
    assert last_fired_a.utcoffset() == timedelta(0)
    # Schedule B uses America/New_York — last_fired_at should carry that offset
    from zoneinfo import ZoneInfo

    ny_tz = ZoneInfo("America/New_York")
    expected_offset = datetime.now(ny_tz).utcoffset()
    assert last_fired_b.utcoffset() == expected_offset

    DBOS.delete_schedule("every-second-a")
    DBOS.delete_schedule("every-second-b")


def test_dynamic_scheduler_delete_stops_firing(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def scheduled_workflow(scheduled_at: datetime, ctx: Any) -> None:
        nonlocal wf_counter
        wf_counter += 1

    DBOS.create_schedule(
        schedule_name="delete-test",
        workflow_fn=scheduled_workflow,
        schedule="* * * * * *",
        context="delete-ctx",
    )

    def check_fired() -> None:
        assert wf_counter >= 1

    retry_until_success(check_fired)

    DBOS.delete_schedule("delete-test")
    # Wait for the main loop to detect the deletion and stop the thread
    time.sleep(3)
    count_after_delete = wf_counter
    time.sleep(3)
    assert wf_counter == count_after_delete


def test_dynamic_scheduler_add_after_launch(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def scheduled_workflow(scheduled_at: datetime, ctx: Any) -> None:
        nonlocal wf_counter
        wf_counter += 1

    # No schedules yet — wait and confirm nothing fires
    time.sleep(2)
    assert wf_counter == 0

    # Now add a schedule dynamically
    DBOS.create_schedule(
        schedule_name="late-add",
        workflow_fn=scheduled_workflow,
        schedule="* * * * * *",
    )

    def check_fired_twice() -> None:
        assert wf_counter >= 2

    retry_until_success(check_fired_twice)

    DBOS.delete_schedule("late-add")


def test_dynamic_scheduler_replace_schedule(dbos: DBOS) -> None:
    received_contexts: list[Any] = []

    @DBOS.workflow()
    def scheduled_workflow(scheduled_at: datetime, ctx: Any) -> None:
        received_contexts.append(ctx)

    # Create a schedule that runs once a day — should not fire during this test
    DBOS.create_schedule(
        schedule_name="replaceable",
        workflow_fn=scheduled_workflow,
        schedule="0 0 * * *",
        context={"version": 1},
    )
    time.sleep(3)
    assert len(received_contexts) == 0

    # Delete it and replace with one that runs every second
    DBOS.delete_schedule("replaceable")
    DBOS.create_schedule(
        schedule_name="replaceable-fast",
        workflow_fn=scheduled_workflow,
        schedule="* * * * * *",
        context={"version": 2},
    )

    def check_fired_v2() -> None:
        assert len(received_contexts) >= 2
        assert all(c == {"version": 2} for c in received_contexts)

    retry_until_success(check_fired_v2)

    # Replace with a new context and verify the workflow picks it up
    count_before = len(received_contexts)
    DBOS.delete_schedule("replaceable-fast")
    DBOS.create_schedule(
        schedule_name="replaceable-fast",
        workflow_fn=scheduled_workflow,
        schedule="* * * * * *",
        context={"version": 3},
    )

    def check_fired_v3() -> None:
        v3 = [c for c in received_contexts[count_before:] if c == {"version": 3}]
        assert len(v3) >= 2

    retry_until_success(check_fired_v3)

    DBOS.delete_schedule("replaceable-fast")


def test_long_schedule_shutdown(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def scheduled_workflow(scheduled_at: datetime, ctx: Any) -> None:
        nonlocal wf_counter
        wf_counter += 1

    # Create a schedule that runs once a day — should not fire during this test
    DBOS.create_schedule(
        schedule_name="replaceable",
        workflow_fn=scheduled_workflow,
        schedule="0 0 * * *",
    )
    time.sleep(3)
    assert wf_counter == 0

    # If this test doesn't time out, DBOS can properly shut down
    # despite a very long schedule.


def test_backfill_schedule(dbos: DBOS) -> None:
    received: list[tuple[datetime, Any]] = []

    @DBOS.workflow()
    def backfill_workflow(scheduled_at: datetime, ctx: Any) -> None:
        received.append((scheduled_at, ctx))

    DBOS.create_schedule(
        schedule_name="backfill-test",
        workflow_fn=backfill_workflow,
        schedule="0 * * * *",  # every hour
        context={"env": "backfill"},
    )

    # Backfill from 00:30 to 03:30 — yields 01:00, 02:00, 03:00
    start = datetime(2025, 1, 1, 0, 30, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=3)
    handles = DBOS.backfill_schedule("backfill-test", start, end)
    assert len(handles) == 3

    # Wait for the enqueued workflows to execute
    for h in handles:
        h.get_result()

    expected = [datetime(2025, 1, 1, h, 0, 0, tzinfo=timezone.utc) for h in range(1, 4)]
    assert sorted(t for t, _ in received) == expected
    assert all(ctx == {"env": "backfill"} for _, ctx in received)

    # Backfilling again should be idempotent (same workflow IDs)
    handles2 = DBOS.backfill_schedule("backfill-test", start, end)
    assert len(handles2) == 3
    time.sleep(1)
    assert len(received) == 3

    # Nonexistent schedule
    with pytest.raises(DBOSException, match="does not exist"):
        DBOS.backfill_schedule("no-such-schedule", start, end)

    DBOS.delete_schedule("backfill-test")


def test_backfill_with_timezone(dbos: DBOS) -> None:
    received_utc: list[datetime] = []
    received_ny: list[datetime] = []

    @DBOS.workflow()
    def wf_utc(scheduled_at: datetime, ctx: Any) -> None:
        received_utc.append(scheduled_at)

    @DBOS.workflow()
    def wf_ny(scheduled_at: datetime, ctx: Any) -> None:
        received_ny.append(scheduled_at)

    # Same cron (midnight daily), different timezones
    DBOS.create_schedule(
        schedule_name="tz-utc",
        workflow_fn=wf_utc,
        schedule="0 0 * * *",
    )
    DBOS.create_schedule(
        schedule_name="tz-ny",
        workflow_fn=wf_ny,
        schedule="0 0 * * *",
        cron_timezone="America/New_York",
    )

    # Backfill a window that contains two UTC midnights and two NY midnights
    # In winter, America/New_York is UTC-5
    # Start just before midnight UTC Jan 1
    start = datetime(2024, 12, 31, 23, 0, 0, tzinfo=timezone.utc)
    end = datetime(2025, 1, 3, 0, 0, 0, tzinfo=timezone.utc)

    handles_utc = DBOS.backfill_schedule("tz-utc", start, end)
    handles_ny = DBOS.backfill_schedule("tz-ny", start, end)

    for h in handles_utc + handles_ny:
        h.get_result()

    # UTC schedule: midnight UTC on Jan 1 and Jan 2
    utc_times = sorted(received_utc)
    assert len(utc_times) == 2
    assert utc_times[0].day == 1 and utc_times[0].hour == 0
    assert utc_times[1].day == 2 and utc_times[1].hour == 0

    # NY schedule: midnight Eastern = 05:00 UTC, so Jan 1 05:00 and Jan 2 05:00
    ny_times = sorted(received_ny)
    assert len(ny_times) == 2
    for t in ny_times:
        # Midnight in New York
        assert t.hour == 0
        assert t.minute == 0
    # The NY times should be different instants from the UTC times
    # Convert to UTC for comparison: midnight EST = 05:00 UTC
    ny_utc_times = sorted(t.astimezone(timezone.utc) for t in ny_times)
    assert ny_utc_times[0].hour == 5
    assert ny_utc_times[1].hour == 5

    DBOS.delete_schedule("tz-utc")
    DBOS.delete_schedule("tz-ny")


def test_trigger_schedule(dbos: DBOS) -> None:
    received: list[tuple[datetime, Any]] = []

    @DBOS.workflow()
    def trigger_workflow(scheduled_at: datetime, ctx: Any) -> None:
        received.append((scheduled_at, ctx))

    DBOS.create_schedule(
        schedule_name="trigger-test",
        workflow_fn=trigger_workflow,
        schedule="0 0 * * *",  # daily, won't fire during test
        context=[1, 2, 3],
    )

    before = datetime.now(timezone.utc)
    handle = DBOS.trigger_schedule("trigger-test")
    after = datetime.now(timezone.utc)

    assert handle.workflow_id.startswith("sched-trigger-test-trigger-")
    handle.get_result()

    assert len(received) == 1
    assert before <= received[0][0] <= after
    assert received[0][1] == [1, 2, 3]

    # Nonexistent schedule
    with pytest.raises(DBOSException, match="does not exist"):
        DBOS.trigger_schedule("no-such-schedule")

    DBOS.delete_schedule("trigger-test")


def test_client_schedule_crud(client: DBOSClient) -> None:
    # Create a schedule with context and timezone
    client.create_schedule(
        schedule_name="client-schedule",
        workflow_name="some.workflow",
        schedule="* * * * *",
        context={"tenant": "acme"},
        cron_timezone="Asia/Tokyo",
    )

    # List schedules and verify
    schedules = client.list_schedules()
    assert len(schedules) == 1
    assert schedules[0]["schedule_name"] == "client-schedule"
    assert schedules[0]["workflow_name"] == "some.workflow"
    assert schedules[0]["schedule"] == "* * * * *"
    assert schedules[0]["context"] == {"tenant": "acme"}
    assert schedules[0]["workflow_class_name"] is None
    assert schedules[0]["cron_timezone"] == "Asia/Tokyo"

    # Get schedule by name
    sched = client.get_schedule("client-schedule")
    assert sched is not None
    assert sched["schedule_name"] == "client-schedule"
    assert sched["schedule_id"] == schedules[0]["schedule_id"]
    assert sched["context"] == {"tenant": "acme"}
    assert sched["workflow_class_name"] is None
    assert sched["cron_timezone"] == "Asia/Tokyo"

    # Get nonexistent schedule
    assert client.get_schedule("nonexistent") is None

    # Reject invalid cron expression
    with pytest.raises(DBOSException, match="Invalid cron schedule"):
        client.create_schedule(
            schedule_name="bad-schedule",
            workflow_name="some.workflow",
            schedule="not a cron",
        )

    # Reject invalid timezone
    with pytest.raises(DBOSException, match="Invalid timezone"):
        client.create_schedule(
            schedule_name="bad-tz",
            workflow_name="some.workflow",
            schedule="* * * * *",
            cron_timezone="Fake/Zone",
        )

    # --- list_schedules filters ---
    client.create_schedule(
        schedule_name="client-other",
        workflow_name="other.workflow",
        schedule="0 0 * * *",
    )
    client.pause_schedule("client-other")

    # Filter by status
    assert len(client.list_schedules(status="ACTIVE")) == 1
    assert (
        client.list_schedules(status="ACTIVE")[0]["schedule_name"] == "client-schedule"
    )
    assert len(client.list_schedules(status="PAUSED")) == 1
    assert len(client.list_schedules(status=["ACTIVE", "PAUSED"])) == 2

    # Filter by workflow_name
    assert len(client.list_schedules(workflow_name="some.workflow")) == 1
    assert len(client.list_schedules(workflow_name="other.workflow")) == 1
    assert (
        len(client.list_schedules(workflow_name=["some.workflow", "other.workflow"]))
        == 2
    )

    # Filter by schedule_name_prefix
    assert len(client.list_schedules(schedule_name_prefix="client-s")) == 1
    assert len(client.list_schedules(schedule_name_prefix="client-o")) == 1
    assert len(client.list_schedules(schedule_name_prefix="client-")) == 2
    assert (
        len(client.list_schedules(schedule_name_prefix=["client-s", "client-o"])) == 2
    )

    # Combine filters
    assert (
        len(client.list_schedules(status="ACTIVE", workflow_name="some.workflow")) == 1
    )
    assert (
        len(client.list_schedules(status="PAUSED", workflow_name="some.workflow")) == 0
    )

    # Delete schedules
    client.delete_schedule("client-other")
    client.delete_schedule("client-schedule")
    assert client.get_schedule("client-schedule") is None
    assert len(client.list_schedules()) == 0


def test_client_apply_schedules(client: DBOSClient) -> None:
    # Apply two schedules at once (sched-b has automatic_backfill and timezone)
    client.apply_schedules(
        [
            {
                "schedule_name": "sched-a",
                "workflow_name": "wf.a",
                "schedule": "* * * * *",
                "context": {"region": "eu"},
            },
            {
                "schedule_name": "sched-b",
                "workflow_name": "wf.b",
                "schedule": "0 0 * * *",
                "context": None,
                "automatic_backfill": True,
                "cron_timezone": "US/Pacific",
            },
        ]
    )
    schedules = client.list_schedules()
    assert len(schedules) == 2
    by_name = {s["schedule_name"]: s for s in schedules}
    assert by_name["sched-a"]["schedule"] == "* * * * *"
    assert by_name["sched-a"]["context"] == {"region": "eu"}
    assert by_name["sched-a"]["automatic_backfill"] is False
    assert by_name["sched-a"]["cron_timezone"] is None
    assert by_name["sched-b"]["workflow_name"] == "wf.b"
    assert by_name["sched-b"]["context"] is None
    assert by_name["sched-b"]["automatic_backfill"] is True
    assert by_name["sched-b"]["cron_timezone"] == "US/Pacific"

    # Replace sched-a, add sched-c
    client.apply_schedules(
        [
            {
                "schedule_name": "sched-a",
                "workflow_name": "wf.a",
                "schedule": "0 * * * *",
                "context": None,
            },
            {
                "schedule_name": "sched-c",
                "workflow_name": "wf.c",
                "schedule": "*/5 * * * *",
                "context": [1, 2],
            },
        ]
    )
    schedules = client.list_schedules()
    assert len(schedules) == 3
    by_name = {s["schedule_name"]: s for s in schedules}
    assert by_name["sched-a"]["schedule"] == "0 * * * *"
    assert by_name["sched-a"]["context"] is None
    assert by_name["sched-c"]["schedule"] == "*/5 * * * *"
    assert by_name["sched-c"]["context"] == [1, 2]

    # Reject invalid cron
    with pytest.raises(DBOSException, match="Invalid cron schedule"):
        client.apply_schedules(
            [
                {
                    "schedule_name": "bad",
                    "workflow_name": "wf.x",
                    "schedule": "not a cron",
                    "context": None,
                }
            ]
        )

    # Reject missing required fields
    with pytest.raises(DBOSException, match="missing required field 'schedule_name'"):
        client.apply_schedules(
            [{"workflow_name": "wf.x", "schedule": "* * * * *", "context": None}]
        )
    with pytest.raises(DBOSException, match="missing required field 'workflow_name'"):
        client.apply_schedules(
            [{"schedule_name": "x", "schedule": "* * * * *", "context": None}]
        )
    with pytest.raises(DBOSException, match="missing required field 'schedule'"):
        client.apply_schedules(
            [{"schedule_name": "x", "workflow_name": "wf.x", "context": None}]
        )

    # Clean up
    client.delete_schedule("sched-a")
    client.delete_schedule("sched-b")
    client.delete_schedule("sched-c")
    assert len(client.list_schedules()) == 0


def test_client_backfill_schedule(client: DBOSClient) -> None:
    received: list[tuple[datetime, Any]] = []

    @DBOS.workflow()
    def backfill_workflow(scheduled_at: datetime, ctx: Any) -> None:
        received.append((scheduled_at, ctx))

    client.create_schedule(
        schedule_name="client-backfill",
        workflow_name=backfill_workflow.__qualname__,
        schedule="0 * * * *",
        context={"source": "client"},
    )

    start = datetime(2025, 6, 1, 0, 30, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=3)
    handles = client.backfill_schedule("client-backfill", start, end)
    assert len(handles) == 3

    for h in handles:
        h.get_result()

    expected = [datetime(2025, 6, 1, h, 0, 0, tzinfo=timezone.utc) for h in range(1, 4)]
    assert sorted(t for t, _ in received) == expected
    assert all(ctx == {"source": "client"} for _, ctx in received)

    client.delete_schedule("client-backfill")


def test_client_trigger_schedule(client: DBOSClient) -> None:
    received: list[tuple[datetime, Any]] = []

    @DBOS.workflow()
    def trigger_workflow(scheduled_at: datetime, ctx: Any) -> None:
        received.append((scheduled_at, ctx))

    client.create_schedule(
        schedule_name="client-trigger",
        workflow_name=trigger_workflow.__qualname__,
        schedule="0 0 * * *",
        context="trigger-ctx",
    )

    before = datetime.now(timezone.utc)
    handle = client.trigger_schedule("client-trigger")
    after = datetime.now(timezone.utc)

    assert handle.workflow_id.startswith("sched-client-trigger-trigger-")
    handle.get_result()

    assert len(received) == 1
    assert before <= received[0][0] <= after
    assert received[0][1] == "trigger-ctx"

    client.delete_schedule("client-trigger")


def test_pause_resume_schedule(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def scheduled_workflow(scheduled_at: datetime, ctx: Any) -> None:
        nonlocal wf_counter
        wf_counter += 1

    DBOS.create_schedule(
        schedule_name="pause-test",
        workflow_fn=scheduled_workflow,
        schedule="* * * * * *",
    )

    def check_fired() -> None:
        assert wf_counter >= 1

    retry_until_success(check_fired)

    # Pause the schedule
    DBOS.pause_schedule("pause-test")
    sched = DBOS.get_schedule("pause-test")
    assert sched is not None
    assert sched["status"] == "PAUSED"

    # Wait for the scheduler loop to detect the pause and stop the thread
    time.sleep(3)
    count_after_pause = wf_counter
    time.sleep(3)
    assert wf_counter == count_after_pause

    # Resume the schedule
    DBOS.resume_schedule("pause-test")
    sched = DBOS.get_schedule("pause-test")
    assert sched is not None
    assert sched["status"] == "ACTIVE"

    def check_fired_after_resume() -> None:
        assert wf_counter > count_after_pause

    retry_until_success(check_fired_after_resume)

    DBOS.delete_schedule("pause-test")


def test_client_pause_resume_schedule(client: DBOSClient) -> None:
    client.create_schedule(
        schedule_name="client-pause",
        workflow_name="some.workflow",
        schedule="0 0 * * *",
    )

    # Pause
    client.pause_schedule("client-pause")
    sched = client.get_schedule("client-pause")
    assert sched is not None
    assert sched["status"] == "PAUSED"

    # Resume
    client.resume_schedule("client-pause")
    sched = client.get_schedule("client-pause")
    assert sched is not None
    assert sched["status"] == "ACTIVE"

    client.delete_schedule("client-pause")


@pytest.mark.asyncio
async def test_schedule_crud_async(dbos: DBOS) -> None:
    received: list[Any] = []

    @DBOS.workflow()
    async def my_workflow(scheduled_at: datetime, ctx: Any) -> None:
        received.append(ctx)

    @DBOS.workflow()
    async def my_workflow_b(scheduled_at: datetime, ctx: Any) -> None:
        pass

    await DBOS.create_schedule_async(
        schedule_name="async-schedule",
        workflow_fn=my_workflow,
        schedule="* * * * * *",
        context={"async": True},
    )

    schedules = await DBOS.list_schedules_async()
    assert len(schedules) == 1
    assert schedules[0]["schedule_name"] == "async-schedule"
    assert schedules[0]["context"] == {"async": True}

    sched = await DBOS.get_schedule_async("async-schedule")
    assert sched is not None
    assert sched["schedule"] == "* * * * * *"
    assert sched["context"] == {"async": True}

    assert await DBOS.get_schedule_async("nonexistent") is None

    # Filters work through async path
    assert len(await DBOS.list_schedules_async(status="ACTIVE")) == 1
    assert len(await DBOS.list_schedules_async(schedule_name_prefix="async-")) == 1
    assert len(await DBOS.list_schedules_async(schedule_name_prefix="nope-")) == 0

    # Verify the schedule actually fires
    def check_fired() -> None:
        assert len(received) >= 2
        assert all(c == {"async": True} for c in received)

    retry_until_success(check_fired)

    await DBOS.delete_schedule_async("async-schedule")
    assert await DBOS.get_schedule_async("async-schedule") is None
    assert len(await DBOS.list_schedules_async()) == 0

    # Test apply_schedules_async
    await DBOS.apply_schedules_async(
        [
            {
                "schedule_name": "async-sched-a",
                "workflow_fn": my_workflow,
                "schedule": "* * * * *",
                "context": {"region": "us"},
            },
            {
                "schedule_name": "async-sched-b",
                "workflow_fn": my_workflow_b,
                "schedule": "0 0 * * *",
                "context": None,
                "automatic_backfill": True,
                "cron_timezone": "Europe/London",
            },
        ]
    )
    schedules = await DBOS.list_schedules_async()
    assert len(schedules) == 2
    by_name = {s["schedule_name"]: s for s in schedules}
    assert by_name["async-sched-a"]["schedule"] == "* * * * *"
    assert by_name["async-sched-a"]["context"] == {"region": "us"}
    assert by_name["async-sched-a"]["automatic_backfill"] is False
    assert by_name["async-sched-a"]["cron_timezone"] is None
    assert by_name["async-sched-b"]["schedule"] == "0 0 * * *"
    assert by_name["async-sched-b"]["context"] is None
    assert by_name["async-sched-b"]["automatic_backfill"] is True
    assert by_name["async-sched-b"]["cron_timezone"] == "Europe/London"

    # Replace async-sched-a, add async-sched-c
    await DBOS.apply_schedules_async(
        [
            {
                "schedule_name": "async-sched-a",
                "workflow_fn": my_workflow,
                "schedule": "0 * * * *",
                "context": None,
            },
            {
                "schedule_name": "async-sched-c",
                "workflow_fn": my_workflow_b,
                "schedule": "*/5 * * * *",
                "context": [1, 2, 3],
            },
        ]
    )
    schedules = await DBOS.list_schedules_async()
    assert len(schedules) == 3
    by_name = {s["schedule_name"]: s for s in schedules}
    assert by_name["async-sched-a"]["schedule"] == "0 * * * *"
    assert by_name["async-sched-a"]["context"] is None
    assert by_name["async-sched-c"]["schedule"] == "*/5 * * * *"
    assert by_name["async-sched-c"]["context"] == [1, 2, 3]

    # Clean up
    await DBOS.delete_schedule_async("async-sched-a")
    await DBOS.delete_schedule_async("async-sched-b")
    await DBOS.delete_schedule_async("async-sched-c")
    assert len(await DBOS.list_schedules_async()) == 0


@pytest.mark.asyncio
async def test_client_schedule_crud_async(client: DBOSClient) -> None:
    await client.create_schedule_async(
        schedule_name="async-client",
        workflow_name="some.workflow",
        schedule="0 0 * * *",
        context=42,
    )

    schedules = await client.list_schedules_async()
    assert len(schedules) == 1
    assert schedules[0]["schedule_name"] == "async-client"
    assert schedules[0]["context"] == 42

    sched = await client.get_schedule_async("async-client")
    assert sched is not None
    assert sched["workflow_name"] == "some.workflow"
    assert sched["context"] == 42

    assert await client.get_schedule_async("nonexistent") is None

    # Filters work through async path
    assert len(await client.list_schedules_async(status="ACTIVE")) == 1
    assert len(await client.list_schedules_async(workflow_name="some.workflow")) == 1
    assert len(await client.list_schedules_async(workflow_name="other")) == 0

    await client.delete_schedule_async("async-client")
    assert await client.get_schedule_async("async-client") is None
    assert len(await client.list_schedules_async()) == 0

    # Test apply_schedules_async
    await client.apply_schedules_async(
        [
            {
                "schedule_name": "async-client-a",
                "workflow_name": "wf.a",
                "schedule": "* * * * *",
                "context": {"region": "eu"},
            },
            {
                "schedule_name": "async-client-b",
                "workflow_name": "wf.b",
                "schedule": "0 0 * * *",
                "context": None,
                "automatic_backfill": True,
                "cron_timezone": "US/Pacific",
            },
        ]
    )
    schedules = await client.list_schedules_async()
    assert len(schedules) == 2
    by_name = {s["schedule_name"]: s for s in schedules}
    assert by_name["async-client-a"]["schedule"] == "* * * * *"
    assert by_name["async-client-a"]["context"] == {"region": "eu"}
    assert by_name["async-client-a"]["automatic_backfill"] is False
    assert by_name["async-client-a"]["cron_timezone"] is None
    assert by_name["async-client-b"]["schedule"] == "0 0 * * *"
    assert by_name["async-client-b"]["context"] is None
    assert by_name["async-client-b"]["automatic_backfill"] is True
    assert by_name["async-client-b"]["cron_timezone"] == "US/Pacific"

    # Replace async-client-a, add async-client-c
    await client.apply_schedules_async(
        [
            {
                "schedule_name": "async-client-a",
                "workflow_name": "wf.a",
                "schedule": "0 * * * *",
                "context": None,
            },
            {
                "schedule_name": "async-client-c",
                "workflow_name": "wf.c",
                "schedule": "*/5 * * * *",
                "context": [1, 2],
            },
        ]
    )
    schedules = await client.list_schedules_async()
    assert len(schedules) == 3
    by_name = {s["schedule_name"]: s for s in schedules}
    assert by_name["async-client-a"]["schedule"] == "0 * * * *"
    assert by_name["async-client-a"]["context"] is None
    assert by_name["async-client-c"]["schedule"] == "*/5 * * * *"
    assert by_name["async-client-c"]["context"] == [1, 2]

    # Clean up
    await client.delete_schedule_async("async-client-a")
    await client.delete_schedule_async("async-client-b")
    await client.delete_schedule_async("async-client-c")
    assert len(await client.list_schedules_async()) == 0


def test_static_class_method_schedule(dbos: DBOS) -> None:
    received: list[Any] = []

    @DBOS.dbos_class()
    class MyScheduledClass:
        @staticmethod
        @DBOS.workflow()
        def scheduled_wf(scheduled_at: datetime, ctx: Any) -> None:
            received.append(ctx)

    DBOS.create_schedule(
        schedule_name="static-class-schedule",
        workflow_fn=MyScheduledClass.scheduled_wf,
        schedule="* * * * * *",
        context={"class": True},
    )

    sched = DBOS.get_schedule("static-class-schedule")
    assert sched is not None
    # Static methods should not have a class name set
    assert sched["workflow_name"] == MyScheduledClass.scheduled_wf.__qualname__
    assert sched["workflow_class_name"] is None
    assert sched["context"] == {"class": True}

    def check_fired() -> None:
        assert len(received) >= 2
        assert all(c == {"class": True} for c in received)

    retry_until_success(check_fired)

    # Trigger should work for static class methods
    handle = DBOS.trigger_schedule("static-class-schedule")
    handle.get_result()
    assert received[-1] == {"class": True}

    # Backfill should work for static class methods
    start = datetime(2025, 1, 1, 0, 30, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=3)
    DBOS.delete_schedule("static-class-schedule")
    DBOS.create_schedule(
        schedule_name="static-class-backfill",
        workflow_fn=MyScheduledClass.scheduled_wf,
        schedule="0 * * * *",
        context={"backfill": True},
    )
    handles = DBOS.backfill_schedule("static-class-backfill", start, end)
    assert len(handles) == 3
    for h in handles:
        h.get_result()

    DBOS.delete_schedule("static-class-backfill")


def test_classmethod_schedule(dbos: DBOS) -> None:
    received: list[Any] = []

    @DBOS.dbos_class()
    class MyClassMethodSchedule:
        @classmethod
        @DBOS.workflow()
        def scheduled_wf(cls, scheduled_at: datetime, ctx: Any) -> None:
            assert DBOS.workflow_id
            status = DBOS.get_workflow_status(DBOS.workflow_id)
            assert status
            assert status.queue_name == INTERNAL_QUEUE_NAME
            received.append(ctx)

    DBOS.create_schedule(
        schedule_name="classmethod-schedule",
        workflow_fn=MyClassMethodSchedule.scheduled_wf,
        schedule="* * * * * *",
        context={"cls": True},
    )

    sched = DBOS.get_schedule("classmethod-schedule")
    assert sched is not None
    # Class methods should have the class name set
    assert sched["workflow_name"] == MyClassMethodSchedule.scheduled_wf.__qualname__
    assert sched["workflow_class_name"] == MyClassMethodSchedule.__qualname__
    assert sched["context"] == {"cls": True}

    def check_fired() -> None:
        assert len(received) >= 2
        assert all(c == {"cls": True} for c in received)

    retry_until_success(check_fired)

    # Trigger should work for class methods
    handle = DBOS.trigger_schedule("classmethod-schedule")
    handle.get_result()
    assert received[-1] == {"cls": True}

    # Backfill should work for class methods
    start = datetime(2025, 1, 1, 0, 30, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=3)
    DBOS.delete_schedule("classmethod-schedule")
    DBOS.create_schedule(
        schedule_name="classmethod-backfill",
        workflow_fn=MyClassMethodSchedule.scheduled_wf,
        schedule="0 * * * *",
        context={"backfill": True},
    )
    handles = DBOS.backfill_schedule("classmethod-backfill", start, end)
    assert len(handles) == 3
    for h in handles:
        h.get_result()

    DBOS.delete_schedule("classmethod-backfill")


def test_automatic_backfill_on_restart(
    config: DBOSConfig, cleanup_test_databases: None
) -> None:
    """Automatic backfill should enqueue missed executions when DBOS restarts."""
    received: list[datetime] = []

    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def hourly_workflow(scheduled_at: datetime, ctx: Any) -> None:
        received.append(scheduled_at)

    DBOS.launch()

    # Create a schedule with automatic_backfill enabled (hourly — won't fire naturally)
    DBOS.create_schedule(
        schedule_name="backfill-restart",
        workflow_fn=hourly_workflow,
        schedule="0 * * * *",
        context=None,
        automatic_backfill=True,
    )

    # Set last_fired_at to 3 hours ago, simulating that the scheduler was down
    three_hours_ago = datetime.now(timezone.utc) - timedelta(hours=3)
    dbos._sys_db.update_last_fired_at("backfill-restart", three_hours_ago.isoformat())

    # Verify the schedule metadata reflects our changes
    sched = DBOS.get_schedule("backfill-restart")
    assert sched is not None
    assert sched["automatic_backfill"] is True
    assert sched["last_fired_at"] is not None
    assert datetime.fromisoformat(sched["last_fired_at"]) == three_hours_ago

    # Destroy and relaunch DBOS — the scheduler should backfill on startup
    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.launch()

    # Wait for the scheduler loop to pick up the schedule and backfill
    def check_backfilled() -> None:
        assert len(received) >= 3

    retry_until_success(check_backfilled)

    # Verify the backfilled times are the 3 hourly slots between last_fired_at and now
    fired_times = sorted(received)
    for t in fired_times[:3]:
        assert t > three_hours_ago
        assert t <= datetime.now(timezone.utc)
        assert t.minute == 0 and t.second == 0  # hourly cron fires at minute 0

    DBOS.delete_schedule("backfill-restart")
    DBOS.destroy(destroy_registry=True)


def test_instance_method_schedule_rejected(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class MyConfigured(DBOSConfiguredInstance):
        def __init__(self) -> None:
            super().__init__("my-config")

        @DBOS.workflow()
        def scheduled_wf(self, scheduled_at: datetime, ctx: Any) -> None:
            pass

    inst = MyConfigured()

    with pytest.raises(
        DBOSException, match="Configured instance methods cannot be used"
    ):
        DBOS.create_schedule(
            schedule_name="instance-schedule",
            workflow_fn=inst.scheduled_wf,
            schedule="* * * * *",
        )


def test_schedule_with_queue_name(dbos: DBOS) -> None:
    my_queue = Queue("scheduler-test-queue")
    received: list[Any] = []

    @DBOS.workflow()
    def queued_workflow(scheduled_at: datetime, ctx: Any) -> None:
        assert DBOS.workflow_id
        status = DBOS.get_workflow_status(DBOS.workflow_id)
        assert status
        assert status.queue_name == "scheduler-test-queue"
        received.append(ctx)

    # Reject undeclared queue name
    with pytest.raises(DBOSException, match="is not declared"):
        DBOS.create_schedule(
            schedule_name="bad-queue-schedule",
            workflow_fn=queued_workflow,
            schedule="0 0 * * *",
            queue_name="nonexistent-queue",
        )

    # Create a schedule with a valid queue name
    DBOS.create_schedule(
        schedule_name="queued-schedule",
        workflow_fn=queued_workflow,
        schedule="* * * * * *",
        context={"queued": True},
        queue_name="scheduler-test-queue",
    )

    # Verify queue_name is stored via get and list
    sched = DBOS.get_schedule("queued-schedule")
    assert sched is not None
    assert sched["queue_name"] == "scheduler-test-queue"
    schedules = DBOS.list_schedules()
    assert len(schedules) == 1
    assert schedules[0]["queue_name"] == "scheduler-test-queue"

    # Verify the schedule fires and workflows land on the specified queue
    def check_fired() -> None:
        assert len(received) >= 2
        assert all(c == {"queued": True} for c in received)

    retry_until_success(check_fired)

    # Trigger also uses the queue
    count_before = len(received)
    handle = DBOS.trigger_schedule("queued-schedule")
    handle.get_result()
    assert len(received) > count_before

    DBOS.delete_schedule("queued-schedule")

    # Schedule without queue_name should have None
    DBOS.create_schedule(
        schedule_name="no-queue-schedule",
        workflow_fn=queued_workflow,
        schedule="0 0 * * *",
    )
    sched = DBOS.get_schedule("no-queue-schedule")
    assert sched is not None
    assert sched["queue_name"] is None
    DBOS.delete_schedule("no-queue-schedule")
