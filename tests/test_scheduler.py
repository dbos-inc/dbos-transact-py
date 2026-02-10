import time
from datetime import datetime, timedelta, timezone

import pytest

from dbos import DBOS, DBOSClient
from dbos._error import DBOSException

from .conftest import retry_until_success


def test_schedule_crud(dbos: DBOS) -> None:
    @DBOS.workflow()
    def my_workflow(scheduled_at: datetime) -> None:
        pass

    # Create a schedule
    DBOS.create_schedule(
        schedule_name="test-schedule",
        workflow_fn=my_workflow,
        schedule="* * * * *",
    )

    # List schedules and verify
    schedules = DBOS.list_schedules()
    assert len(schedules) == 1
    assert schedules[0]["schedule_name"] == "test-schedule"
    assert schedules[0]["workflow_name"] == my_workflow.dbos_function_name  # type: ignore
    assert schedules[0]["schedule"] == "* * * * *"

    # Get schedule by name
    sched = DBOS.get_schedule("test-schedule")
    assert sched is not None
    assert sched["schedule_name"] == "test-schedule"
    assert sched["workflow_name"] == my_workflow.dbos_function_name  # type: ignore
    assert sched["schedule"] == "* * * * *"
    assert sched["schedule_id"] == schedules[0]["schedule_id"]

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

    # Delete schedule
    DBOS.delete_schedule("test-schedule")
    assert DBOS.get_schedule("test-schedule") is None
    assert len(DBOS.list_schedules()) == 0


def test_apply_schedules(dbos: DBOS) -> None:
    @DBOS.workflow()
    def wf_a(scheduled_at: datetime) -> None:
        pass

    @DBOS.workflow()
    def wf_b(scheduled_at: datetime) -> None:
        pass

    # Apply two schedules at once
    DBOS.apply_schedules(
        {
            "sched-a": (wf_a, "* * * * *"),
            "sched-b": (wf_b, "0 0 * * *"),
        }
    )
    schedules = DBOS.list_schedules()
    assert len(schedules) == 2
    by_name = {s["schedule_name"]: s for s in schedules}
    assert by_name["sched-a"]["schedule"] == "* * * * *"
    assert by_name["sched-b"]["schedule"] == "0 0 * * *"

    # Replace one, delete the other, add a new one
    DBOS.apply_schedules(
        {
            "sched-a": (wf_a, "0 * * * *"),
            "sched-b": None,
            "sched-c": (wf_b, "*/5 * * * *"),
        }
    )
    schedules = DBOS.list_schedules()
    assert len(schedules) == 2
    by_name = {s["schedule_name"]: s for s in schedules}
    assert "sched-b" not in by_name
    assert by_name["sched-a"]["schedule"] == "0 * * * *"
    assert by_name["sched-c"]["schedule"] == "*/5 * * * *"

    # Reject invalid cron
    with pytest.raises(DBOSException, match="Invalid cron schedule"):
        DBOS.apply_schedules({"bad": (wf_a, "not a cron")})

    # Reject call from within a workflow
    @DBOS.workflow()
    def bad_workflow() -> None:
        DBOS.apply_schedules({"x": (wf_a, "* * * * *")})

    with pytest.raises(DBOSException, match="cannot be called from within a workflow"):
        DBOS.start_workflow(bad_workflow).get_result()

    # Clean up
    DBOS.apply_schedules({"sched-a": None, "sched-c": None})
    assert len(DBOS.list_schedules()) == 0


def test_schedule_crud_from_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def target_workflow(scheduled_at: datetime) -> None:
        pass

    @DBOS.workflow()
    def crud_workflow() -> None:
        DBOS.create_schedule(
            schedule_name="wf-schedule",
            workflow_fn=target_workflow,
            schedule="* * * * *",
        )

        schedules = DBOS.list_schedules()
        assert len(schedules) == 1
        assert schedules[0]["schedule_name"] == "wf-schedule"

        sched = DBOS.get_schedule("wf-schedule")
        assert sched is not None
        assert sched["schedule_name"] == "wf-schedule"

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
    counter_a: int = 0
    counter_b: int = 0

    @DBOS.workflow()
    def workflow_a(scheduled_at: datetime) -> None:
        nonlocal counter_a
        counter_a += 1

    @DBOS.workflow()
    def workflow_b(scheduled_at: datetime) -> None:
        nonlocal counter_b
        counter_b += 1

    DBOS.create_schedule(
        schedule_name="every-second-a",
        workflow_fn=workflow_a,
        schedule="* * * * * *",
    )
    DBOS.create_schedule(
        schedule_name="every-second-b",
        workflow_fn=workflow_b,
        schedule="* * * * * *",
    )

    def check_both_fired_twice() -> None:
        assert counter_a >= 2
        assert counter_b >= 2

    retry_until_success(check_both_fired_twice)

    DBOS.delete_schedule("every-second-a")
    DBOS.delete_schedule("every-second-b")


def test_dynamic_scheduler_delete_stops_firing(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def scheduled_workflow(scheduled_at: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    DBOS.create_schedule(
        schedule_name="delete-test",
        workflow_fn=scheduled_workflow,
        schedule="* * * * * *",
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
    def scheduled_workflow(scheduled_at: datetime) -> None:
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
    wf_counter: int = 0

    @DBOS.workflow()
    def scheduled_workflow(scheduled_at: datetime) -> None:
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

    # Delete it and replace with one that runs every second
    DBOS.delete_schedule("replaceable")
    DBOS.create_schedule(
        schedule_name="replaceable-fast",
        workflow_fn=scheduled_workflow,
        schedule="* * * * * *",
    )

    def check_fired_twice() -> None:
        assert wf_counter >= 2

    retry_until_success(check_fired_twice)

    DBOS.delete_schedule("replaceable-fast")


def test_long_schedule_shutdown(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def scheduled_workflow(scheduled_at: datetime) -> None:
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
    received_times: list[datetime] = []

    @DBOS.workflow()
    def backfill_workflow(scheduled_at: datetime) -> None:
        received_times.append(scheduled_at)

    DBOS.create_schedule(
        schedule_name="backfill-test",
        workflow_fn=backfill_workflow,
        schedule="0 * * * *",  # every hour
    )

    # Backfill from 00:30 to 03:30 — yields 01:00, 02:00, 03:00
    start = datetime(2025, 1, 1, 0, 30, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=3)
    count = DBOS.backfill_schedule("backfill-test", start, end)
    assert count == 3

    # Wait for the enqueued workflows to execute
    def check_all_received() -> None:
        assert len(received_times) == 3

    retry_until_success(check_all_received)

    expected = [datetime(2025, 1, 1, h, 0, 0, tzinfo=timezone.utc) for h in range(1, 4)]
    assert sorted(received_times) == expected

    # Backfilling again should be idempotent (same workflow IDs)
    count2 = DBOS.backfill_schedule("backfill-test", start, end)
    assert count2 == 3
    time.sleep(1)
    assert len(received_times) == 3

    # Nonexistent schedule
    with pytest.raises(DBOSException, match="does not exist"):
        DBOS.backfill_schedule("no-such-schedule", start, end)

    DBOS.delete_schedule("backfill-test")


def test_client_schedule_crud(client: DBOSClient) -> None:
    # Create a schedule
    client.create_schedule(
        schedule_name="client-schedule",
        workflow_name="some.workflow",
        schedule="* * * * *",
    )

    # List schedules and verify
    schedules = client.list_schedules()
    assert len(schedules) == 1
    assert schedules[0]["schedule_name"] == "client-schedule"
    assert schedules[0]["workflow_name"] == "some.workflow"
    assert schedules[0]["schedule"] == "* * * * *"

    # Get schedule by name
    sched = client.get_schedule("client-schedule")
    assert sched is not None
    assert sched["schedule_name"] == "client-schedule"
    assert sched["schedule_id"] == schedules[0]["schedule_id"]

    # Get nonexistent schedule
    assert client.get_schedule("nonexistent") is None

    # Reject invalid cron expression
    with pytest.raises(DBOSException, match="Invalid cron schedule"):
        client.create_schedule(
            schedule_name="bad-schedule",
            workflow_name="some.workflow",
            schedule="not a cron",
        )

    # Delete schedule
    client.delete_schedule("client-schedule")
    assert client.get_schedule("client-schedule") is None
    assert len(client.list_schedules()) == 0


def test_client_apply_schedules(client: DBOSClient) -> None:
    # Apply two schedules at once
    client.apply_schedules(
        {
            "sched-a": ("wf.a", "* * * * *"),
            "sched-b": ("wf.b", "0 0 * * *"),
        }
    )
    schedules = client.list_schedules()
    assert len(schedules) == 2
    by_name = {s["schedule_name"]: s for s in schedules}
    assert by_name["sched-a"]["schedule"] == "* * * * *"
    assert by_name["sched-b"]["workflow_name"] == "wf.b"

    # Replace one, delete the other, add a new one
    client.apply_schedules(
        {
            "sched-a": ("wf.a", "0 * * * *"),
            "sched-b": None,
            "sched-c": ("wf.c", "*/5 * * * *"),
        }
    )
    schedules = client.list_schedules()
    assert len(schedules) == 2
    by_name = {s["schedule_name"]: s for s in schedules}
    assert "sched-b" not in by_name
    assert by_name["sched-a"]["schedule"] == "0 * * * *"
    assert by_name["sched-c"]["schedule"] == "*/5 * * * *"

    # Reject invalid cron
    with pytest.raises(DBOSException, match="Invalid cron schedule"):
        client.apply_schedules({"bad": ("wf.x", "not a cron")})

    # Clean up
    client.apply_schedules({"sched-a": None, "sched-c": None})
    assert len(client.list_schedules()) == 0


def test_client_backfill_schedule(client: DBOSClient) -> None:
    received_times: list[datetime] = []

    @DBOS.workflow()
    def backfill_workflow(scheduled_at: datetime) -> None:
        received_times.append(scheduled_at)

    client.create_schedule(
        schedule_name="client-backfill",
        workflow_name=backfill_workflow.dbos_function_name,  # type: ignore
        schedule="0 * * * *",
    )

    start = datetime(2025, 6, 1, 0, 30, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=3)
    count = client.backfill_schedule("client-backfill", start, end)
    assert count == 3

    def check_received() -> None:
        assert len(received_times) == 3

    retry_until_success(check_received)

    expected = [datetime(2025, 6, 1, h, 0, 0, tzinfo=timezone.utc) for h in range(1, 4)]
    assert sorted(received_times) == expected

    client.delete_schedule("client-backfill")
