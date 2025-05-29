import time
from datetime import datetime, timedelta
from math import ceil
from threading import Event

from dbos import DBOS, WorkflowStatusString
from dbos._queue import Queue
from dbos._schemas.system_database import SystemSchema
from tests.conftest import queue_entries_are_cleaned_up


def test_workflow_status_count(dbos: DBOS) -> None:
    """
    Test workflow_status_count with global count and filtering.
    """

    # Define a simple workflow
    @dbos.workflow()
    def simple_workflow() -> str:
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


def test_enqueued_count(dbos: DBOS) -> None:
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
    def event_workflow(ei: int) -> None:
        start_events[ei].set()
        end_events[ei].wait()

    # Enqueue workflows into the pre-created queues
    for i, queue in enumerate(queues):
        for j in range(wf_per_queue):
            event_idx = i * wf_per_queue + j
            queue.enqueue(event_workflow, ei=event_idx)
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
    assert len(all_counts) == len(queues) * 2
    for count in all_counts:
        assert (
            count.tasks_count == wf_per_queue / 2
        )  # because we have two statuses: PENDING and ENQUEUED

    # unblock all workflows
    for end_event in end_events:
        end_event.set()
    assert queue_entries_are_cleaned_up(dbos)
