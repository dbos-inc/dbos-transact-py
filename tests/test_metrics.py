import datetime

from dbos import DBOS


def test_get_metrics(dbos: DBOS, skip_with_sqlite_imprecise_time: None) -> None:
    """Test the get_metrics method returns correct workflow and step counts."""

    @DBOS.workflow()
    def test_workflow_a() -> str:
        test_step_x()
        test_step_x()
        return "a"

    @DBOS.workflow()
    def test_workflow_b() -> str:
        test_step_y()
        return "b"

    @DBOS.step()
    def test_step_x() -> str:
        return "x"

    @DBOS.step()
    def test_step_y() -> str:
        return "y"

    # Record start time before creating workflows
    start_time = datetime.datetime.now(datetime.timezone.utc)

    # Execute workflows to create metrics data
    test_workflow_a()
    test_workflow_a()
    test_workflow_b()

    # Record end time after creating workflows
    end_time = datetime.datetime.now(datetime.timezone.utc)

    # Query metrics
    metrics = dbos._sys_db.get_metrics(start_time.isoformat(), end_time.isoformat())
    assert len(metrics) == 4

    # Convert to dict for easier assertion
    metrics_dict = {(m["metric_type"], m["metric_name"]): m["value"] for m in metrics}

    # Verify workflow counts
    assert metrics_dict[("workflow_count", test_workflow_a.__qualname__)] == 2
    assert metrics_dict[("workflow_count", test_workflow_b.__qualname__)] == 1

    # Verify step counts
    assert metrics_dict[("step_count", test_step_x.__qualname__)] == 4
    assert metrics_dict[("step_count", test_step_y.__qualname__)] == 1
