import time
from math import ceil
from threading import Event

from dbos import DBOS, WorkflowStatusString
from dbos._queue import Queue
from dbos._schemas.system_database import SystemSchema
from tests.conftest import queue_entries_are_cleaned_up


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
            assert rate.status == WorkflowStatusString.SUCCESS.value
        for rate in error_rates:
            assert rate.rate == expected_rate
            assert rate.status == WorkflowStatusString.ERROR.value


def test_enqueued_count(dbos: DBOS):
    """
    Test enqueued_count with global count, filtering by queue names, and filtering by status.
    """

    queue_names = ["queue_one", "queue_two", "queue_three"]
    worker_concurrency = 5
    queues = [
        Queue(name, worker_concurrency=worker_concurrency) for name in queue_names
    ]
    wf_per_queue = 10
    start_events = [Event() for _ in range(len(queues) * wf_per_queue)]
    end_events = [Event() for _ in range(len(queues) * wf_per_queue)]

    # Define a single workflow that waits for an event
    @dbos.workflow()
    def event_workflow(i: int):
        start_events[i].set()
        end_events[i].wait()

    # Enqueue workflows into the pre-created queues
    for i, queue in enumerate(queues):
        for j in range(wf_per_queue):
            event_idx = i * wf_per_queue + j
            h = queue.enqueue(event_workflow, i=event_idx)
            # Wait for `worker_concurrency` workflows to start
            if j < worker_concurrency:
                start_events[event_idx].wait()

    # Check counting number of running tasks
    pending_counts = dbos._sys_db.queue_status_count(
        status=WorkflowStatusString.PENDING.value
    )
    assert len(pending_counts) == len(queues)
    for count in pending_counts:
        assert count.tasks_count == worker_concurrency

    # Check filtering by status for ENQUEUED tasks
    enqueued_counts = dbos._sys_db.queue_status_count(
        status=WorkflowStatusString.ENQUEUED.value
    )
    assert len(enqueued_counts) == len(queues)
    for count in enqueued_counts:
        assert count.tasks_count == wf_per_queue - worker_concurrency

    # Check filtering by specific queue names for ENQUEUED status
    filtered_counts = dbos._sys_db.queue_status_count(
        queue_name_filter=["queue_one", "queue_two"],
        status=WorkflowStatusString.ENQUEUED.value,
    )
    assert len(filtered_counts) == 2
    for count in filtered_counts:
        assert count.tasks_count == wf_per_queue - worker_concurrency
    assert set([count.queue_name for count in filtered_counts]) == {
        "queue_one",
        "queue_two",
    }

    # Check counting tasks without filtering by status
    all_counts = dbos._sys_db.queue_status_count()
    assert len(all_counts) == len(queues)
    for count in all_counts:
        assert count.tasks_count == wf_per_queue

    # unblock all workflows
    for end_event in end_events:
        end_event.set()
    assert queue_entries_are_cleaned_up(dbos)


def test_queue_completion_rate(dbos: DBOS):
    """
    Test queue_completion_rate metric with different time windows and queue filtering.
    """

    # Define a simple workflow
    @dbos.workflow()
    def simple_workflow():
        return "test_result"

    # Define queue names
    queue_names = ["queue_one", "queue_two"]

    # Run workflows and assign them to queues
    rate_per_second = 1
    workflow_ids = []
    num_workflows = 10  # should be an even number for this test
    for i in range(num_workflows):
        handler = dbos.start_workflow(simple_workflow)
        workflow_ids.append(handler.get_workflow_id())
        time.sleep(1 / rate_per_second)
    duration = num_workflows / rate_per_second

    # Manually update statuses and assign queues in the database
    for i, workflow_id in enumerate(workflow_ids):
        queue_name = queue_names[i % len(queue_names)]
        with dbos._sys_db.engine.begin() as conn:
            conn.execute(
                SystemSchema.workflow_status.update()
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
                .values(queue_name=queue_name)
            )

    bucket_sizes = [60, 1]
    for bucket_size in bucket_sizes:
        # First test the rate across all queues are computed correctly
        if bucket_size > duration:
            # If the bucket size is larger than the total duration, we expect one bucket
            expected_bucket_count_per_queue = 1
        else:
            # Calculate expected bucket counts and rates
            expected_bucket_count_per_queue = ceil(duration / bucket_size) / len(
                queue_names
            )  # divide by number of queues because we enqueued in all during `duration`
        workflows_per_bucket = num_workflows / expected_bucket_count_per_queue
        expected_rate_per_queue = (workflows_per_bucket / bucket_size) / len(
            queue_names
        )
        # Query the metric for all queues
        all_queue_rates = dbos._sys_db.queue_completion_rate(
            time_bucket_seconds=bucket_size
        )
        # Assert the number of entries matches the expected bucket size behavior
        assert (
            len(all_queue_rates) == len(queue_names) * expected_bucket_count_per_queue
        )
        # Assert the rates match the expected values
        for rate in all_queue_rates:
            assert rate.rate == expected_rate_per_queue
            assert rate.status == WorkflowStatusString.SUCCESS.value

        # Now test the rate for each queue individually
        workflows_per_queue = num_workflows // len(queue_names)
        if bucket_size > duration:
            # If the bucket size is larger than the total duration, we expect only one bucket
            expected_bucket_count = 1
        else:
            # Calculate expected bucket counts and rates for each queue
            expected_bucket_count = ceil(duration / bucket_size) / len(
                queue_names
            )  # divide by number of queues because we enqueued in all during `duration`
        workflows_per_bucket = workflows_per_queue / expected_bucket_count
        expected_rate = workflows_per_bucket / bucket_size

        for queue_name in queue_names:
            # Query the metric for the current time bucket
            queue_rates = dbos._sys_db.queue_completion_rate(
                queue_name=queue_name, time_bucket_seconds=bucket_size
            )
            assert len(queue_rates) == expected_bucket_count
            # Assert the rates match the expected values
            for rate in queue_rates:
                assert rate.rate == expected_rate
                assert rate.queue_name == queue_name
                assert rate.status == WorkflowStatusString.SUCCESS.value
