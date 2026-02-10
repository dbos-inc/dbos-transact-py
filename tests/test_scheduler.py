import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from dbos import DBOS, DBOSClient
from dbos._error import DBOSException

from .conftest import retry_until_success


def test_schedule_crud(dbos: DBOS) -> None:
    @DBOS.workflow()
    def my_workflow(scheduled_at: datetime, ctx: Any) -> None:
        pass

    @DBOS.workflow()
    def other_workflow(scheduled_at: datetime, ctx: Any) -> None:
        pass

    # Create a schedule with context
    DBOS.create_schedule(
        schedule_name="test-schedule",
        workflow_fn=my_workflow,
        schedule="* * * * *",
        context={"env": "test"},
    )

    # List schedules and verify
    schedules = DBOS.list_schedules()
    assert len(schedules) == 1
    assert schedules[0]["schedule_name"] == "test-schedule"
    assert schedules[0]["workflow_name"] == my_workflow.dbos_function_name  # type: ignore
    assert schedules[0]["schedule"] == "* * * * *"
    assert schedules[0]["context"]

    # Get schedule by name
    sched = DBOS.get_schedule("test-schedule")
    assert sched is not None
    assert sched["schedule_name"] == "test-schedule"
    assert sched["workflow_name"] == my_workflow.dbos_function_name  # type: ignore
    assert sched["schedule"] == "* * * * *"
    assert sched["schedule_id"] == schedules[0]["schedule_id"]
    assert sched["context"]

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

    # Apply two schedules at once, one with context
    DBOS.apply_schedules(
        {
            "sched-a": (wf_a, "* * * * *", {"region": "us"}),
            "sched-b": (wf_b, "0 0 * * *"),
        }
    )
    schedules = DBOS.list_schedules()
    assert len(schedules) == 2
    by_name = {s["schedule_name"]: s for s in schedules}
    assert by_name["sched-a"]["schedule"] == "* * * * *"
    assert by_name["sched-a"]["context"]
    assert by_name["sched-b"]["schedule"] == "0 0 * * *"
    assert by_name["sched-b"]["context"]

    # Replace one, delete the other, add a new one
    DBOS.apply_schedules(
        {
            "sched-a": (wf_a, "0 * * * *"),
            "sched-b": None,
            "sched-c": (wf_b, "*/5 * * * *", [1, 2, 3]),
        }
    )
    schedules = DBOS.list_schedules()
    assert len(schedules) == 2
    by_name = {s["schedule_name"]: s for s in schedules}
    assert "sched-b" not in by_name
    assert by_name["sched-a"]["schedule"] == "0 * * * *"
    assert by_name["sched-a"]["context"]
    assert by_name["sched-c"]["schedule"] == "*/5 * * * *"
    assert by_name["sched-c"]["context"]

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
        assert schedules[0]["context"]

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
    )

    def check_both_fired_twice() -> None:
        assert len(received_a) >= 2
        assert all(c == {"id": "a"} for c in received_a)
        assert len(received_b) >= 2
        assert all(c == {"id": "b"} for c in received_b)

    retry_until_success(check_both_fired_twice)

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
    # Create a schedule with context
    client.create_schedule(
        schedule_name="client-schedule",
        workflow_name="some.workflow",
        schedule="* * * * *",
        context={"tenant": "acme"},
    )

    # List schedules and verify
    schedules = client.list_schedules()
    assert len(schedules) == 1
    assert schedules[0]["schedule_name"] == "client-schedule"
    assert schedules[0]["workflow_name"] == "some.workflow"
    assert schedules[0]["schedule"] == "* * * * *"
    assert schedules[0]["context"]

    # Get schedule by name
    sched = client.get_schedule("client-schedule")
    assert sched is not None
    assert sched["schedule_name"] == "client-schedule"
    assert sched["schedule_id"] == schedules[0]["schedule_id"]
    assert sched["context"]

    # Get nonexistent schedule
    assert client.get_schedule("nonexistent") is None

    # Reject invalid cron expression
    with pytest.raises(DBOSException, match="Invalid cron schedule"):
        client.create_schedule(
            schedule_name="bad-schedule",
            workflow_name="some.workflow",
            schedule="not a cron",
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
    # Apply two schedules at once, one with context
    client.apply_schedules(
        {
            "sched-a": ("wf.a", "* * * * *", {"region": "eu"}),
            "sched-b": ("wf.b", "0 0 * * *"),
        }
    )
    schedules = client.list_schedules()
    assert len(schedules) == 2
    by_name = {s["schedule_name"]: s for s in schedules}
    assert by_name["sched-a"]["schedule"] == "* * * * *"
    assert by_name["sched-a"]["context"]
    assert by_name["sched-b"]["workflow_name"] == "wf.b"
    assert by_name["sched-b"]["context"]

    # Replace one, delete the other, add a new one
    client.apply_schedules(
        {
            "sched-a": ("wf.a", "0 * * * *"),
            "sched-b": None,
            "sched-c": ("wf.c", "*/5 * * * *", [1, 2]),
        }
    )
    schedules = client.list_schedules()
    assert len(schedules) == 2
    by_name = {s["schedule_name"]: s for s in schedules}
    assert "sched-b" not in by_name
    assert by_name["sched-a"]["schedule"] == "0 * * * *"
    assert by_name["sched-a"]["context"]
    assert by_name["sched-c"]["schedule"] == "*/5 * * * *"
    assert by_name["sched-c"]["context"]

    # Reject invalid cron
    with pytest.raises(DBOSException, match="Invalid cron schedule"):
        client.apply_schedules({"bad": ("wf.x", "not a cron")})

    # Clean up
    client.apply_schedules({"sched-a": None, "sched-c": None})
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
    @DBOS.workflow()
    def my_workflow(scheduled_at: datetime, ctx: Any) -> None:
        pass

    await DBOS.create_schedule_async(
        schedule_name="async-schedule",
        workflow_fn=my_workflow,
        schedule="* * * * *",
        context={"async": True},
    )

    schedules = await DBOS.list_schedules_async()
    assert len(schedules) == 1
    assert schedules[0]["schedule_name"] == "async-schedule"
    assert schedules[0]["context"]

    sched = await DBOS.get_schedule_async("async-schedule")
    assert sched is not None
    assert sched["schedule"] == "* * * * *"
    assert sched["context"]

    assert await DBOS.get_schedule_async("nonexistent") is None

    # Filters work through async path
    assert len(await DBOS.list_schedules_async(status="ACTIVE")) == 1
    assert len(await DBOS.list_schedules_async(schedule_name_prefix="async-")) == 1
    assert len(await DBOS.list_schedules_async(schedule_name_prefix="nope-")) == 0

    await DBOS.delete_schedule_async("async-schedule")
    assert await DBOS.get_schedule_async("async-schedule") is None
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
    assert schedules[0]["context"]

    sched = await client.get_schedule_async("async-client")
    assert sched is not None
    assert sched["workflow_name"] == "some.workflow"
    assert sched["context"]

    assert await client.get_schedule_async("nonexistent") is None

    # Filters work through async path
    assert len(await client.list_schedules_async(status="ACTIVE")) == 1
    assert len(await client.list_schedules_async(workflow_name="some.workflow")) == 1
    assert len(await client.list_schedules_async(workflow_name="other")) == 0

    await client.delete_schedule_async("async-client")
    assert await client.get_schedule_async("async-client") is None
    assert len(await client.list_schedules_async()) == 0
