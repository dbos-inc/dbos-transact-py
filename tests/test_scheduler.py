from dbos import DBOS


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

    # Delete schedule
    DBOS.delete_schedule("test-schedule")
    assert DBOS.get_schedule("test-schedule") is None
    assert len(DBOS.list_schedules()) == 0
