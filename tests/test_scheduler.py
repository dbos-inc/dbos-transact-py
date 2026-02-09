import time
from datetime import datetime

import pytest

from dbos import DBOS
from dbos._error import DBOSException


def test_schedule_crud(dbos: DBOS) -> None:
    @DBOS.workflow()
    def my_workflow() -> None:
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

    # Delete schedule
    DBOS.delete_schedule("test-schedule")
    assert DBOS.get_schedule("test-schedule") is None
    assert len(DBOS.list_schedules()) == 0


def test_dynamic_scheduler_fires(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def scheduled_workflow(scheduled_at: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    DBOS.create_schedule(
        schedule_name="every-second",
        workflow_fn=scheduled_workflow,
        schedule="* * * * * *",
    )

    time.sleep(5)
    assert wf_counter > 1 and wf_counter <= 5

    DBOS.delete_schedule("every-second")


def test_dynamic_scheduler_delete_stops_firing(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def scheduled_workflow(scheduled_at: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    DBOS.create_schedule(
        schedule_name="delete-test",
        workflow_fn=scheduled_workflow,
        schedule="* * * * * *",
    )

    time.sleep(3)
    assert wf_counter >= 1

    DBOS.delete_schedule("delete-test")
    # Wait for the main loop to detect the deletion and stop the thread
    time.sleep(2)
    count_after_delete = wf_counter
    time.sleep(3)
    assert wf_counter == count_after_delete


def test_dynamic_scheduler_add_after_launch(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def scheduled_workflow(scheduled_at: datetime, actual: datetime) -> None:
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

    time.sleep(5)
    assert wf_counter > 1 and wf_counter <= 5

    DBOS.delete_schedule("late-add")
