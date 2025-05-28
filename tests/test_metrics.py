import time
from math import ceil

from dbos import DBOS, WorkflowStatusString
from dbos._queue import Queue
from dbos._schemas.system_database import SystemSchema


def test_workflow_status_count(dbos: DBOS):
    """
    Test workflow_status_count with global count and filtering.
    """

    # Define a simple workflow
    @dbos.workflow()
    def simple_workflow():
        return "test_result"

    # Run the workflow N times
    N = 100
    workflow_ids = []
    for _ in range(N):
        handler = dbos.start_workflow(simple_workflow)
        workflow_ids.append(handler.get_workflow_id())

    # Manually change the status to PENDING/ENQUEUED/SUCCESS/ERROR
    statuses = [
        WorkflowStatusString.PENDING.value,
        WorkflowStatusString.ENQUEUED.value,
        WorkflowStatusString.SUCCESS.value,
        WorkflowStatusString.ERROR.value,
    ]
    for i, workflow_id in enumerate(workflow_ids):
        new_status = statuses[i % 4]
        with dbos._sys_db.engine.begin() as conn:
            conn.execute(
                SystemSchema.workflow_status.update()
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .values(status=new_status)
            )

    # Check global count
    global_counts = dbos._sys_db.workflow_status_count()
    assert len(global_counts) == 4
    for count in global_counts:
        assert count.workflow_count == N // 4

    # Check filtering by 1 status
    filtered_counts = dbos._sys_db.workflow_status_count(
        status_filter=[WorkflowStatusString.PENDING.value]
    )
    assert len(filtered_counts) == 1
    assert filtered_counts[0].status == WorkflowStatusString.PENDING.value
    assert filtered_counts[0].workflow_count == N // 4


queue_names = ["queue_one", "queue_two", "queue_three"]
queues = [Queue(name) for name in queue_names]


def test_enqueued_count(dbos: DBOS):
    """
    Test enqueued_count with global count and filtering.
    """

    # Define a single workflow
    @dbos.workflow()
    def simple_workflow():
        return "result"

    # Enqueue workflows into the pre-created queues
    for queue in queues:
        for _ in range(10):
            queue.enqueue(simple_workflow)

    # Create non-enqueued workflows
    for _ in range(5):
        dbos.start_workflow(simple_workflow)

    # Check global count
    global_counts = dbos._sys_db.enqueued_count()
    assert len(global_counts) == 3
    for count in global_counts:
        assert count.queue_length == 10

    # Check filtering by specific queue names
    filtered_counts = dbos._sys_db.enqueued_count(
        queue_name_filter=["queue_one", "queue_two"]
    )
    assert len(filtered_counts) == 2
    for count in filtered_counts:
        assert count.queue_length == 10
    assert set([count.queue_name for count in filtered_counts]) == {
        "queue_one",
        "queue_two",
    }


def test_workflow_completion_rate(dbos: DBOS):
    """
    Test completion_rate metric with different time windows.
    """

    # Define a simple workflow
    @dbos.workflow()
    def simple_workflow():
        return "test_result"

    # Run 10 workflows, 1 per second
    rate_per_second = 1
    workflow_ids = []
    num_workflows = 16  # should be an even number of this test
    for _ in range(num_workflows):
        handler = dbos.start_workflow(simple_workflow)
        workflow_ids.append(handler.get_workflow_id())
        time.sleep(1 / rate_per_second)
    duration = num_workflows / rate_per_second

    # Manually update statuses in the database
    for i, workflow_id in enumerate(workflow_ids):
        new_status = (
            WorkflowStatusString.SUCCESS.value
            if i % 2 == 0
            else WorkflowStatusString.ERROR.value
        )
        with dbos._sys_db.engine.begin() as conn:
            conn.execute(
                SystemSchema.workflow_status.update()
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .values(status=new_status)
            )

    bucket_sizes = [60, 1]
    workflows_for_status = num_workflows // 2
    for bucket_size in bucket_sizes:
        if bucket_size > duration:
            # If the bucket size is larger than the total duration, we expect only one bucket
            expected_bucket_count = 1
            expected_rate = workflows_for_status / duration
        else:
            # Calculate expected bucket counts and rates
            expected_bucket_count = (
                ceil(duration / bucket_size) / 2
            )  # Two statuses: SUCCESS and ERROR
            workflows_per_bucket = workflows_for_status / expected_bucket_count
            expected_rate = workflows_per_bucket / bucket_size

            # Query the metric for the current time bucket
            success_rates = dbos._sys_db.workflow_completion_rate(
                status=WorkflowStatusString.SUCCESS.value,
                time_bucket_seconds=bucket_size,
            )
            error_rates = dbos._sys_db.workflow_completion_rate(
                status=WorkflowStatusString.ERROR.value, time_bucket_seconds=bucket_size
            )

            # Assert the number of entries matches the expected bucket size behavior
            assert len(success_rates) == len(error_rates) == expected_bucket_count

            # Assert the rates match the expected values
            for rate in success_rates:
                assert rate.rate == expected_rate
            for rate in error_rates:
                assert rate.rate == expected_rate
