import time
import uuid

import pytest

from dbos import DBOS, DBOSClient, Queue, SetEnqueueOptions, SetWorkflowID
from dbos._dbos import WorkflowHandle
from dbos._error import DBOSQueueDeduplicatedError
from dbos._sys_db import WorkflowStatusString


def test_delay(dbos: DBOS, client: DBOSClient) -> None:
    queue = Queue("test_delay_queue", polling_interval_sec=0.1)

    @DBOS.workflow()
    def test_workflow() -> None:
        pass

    delay_seconds = 2.0

    # Test via SetEnqueueOptions
    t_before = int(time.time() * 1000)
    with SetEnqueueOptions(delay_seconds=delay_seconds):
        handle = queue.enqueue(test_workflow)
    t_after = int(time.time() * 1000)

    status = handle.get_status()
    assert status.status == WorkflowStatusString.DELAYED.value
    assert status.delay_until_epoch_ms is not None
    assert status.delay_until_epoch_ms >= t_before + int(delay_seconds * 1000)
    assert status.delay_until_epoch_ms <= t_after + int(delay_seconds * 1000)

    handle.get_result()

    final_status = handle.get_status()
    assert final_status.status == WorkflowStatusString.SUCCESS.value
    assert final_status.dequeued_at is not None
    assert final_status.dequeued_at >= status.delay_until_epoch_ms

    # Test via client enqueue
    t_before = int(time.time() * 1000)
    client_handle: WorkflowHandle[None] = client.enqueue(
        {
            "queue_name": queue.name,
            "workflow_name": test_workflow.__qualname__,
            "delay_seconds": delay_seconds,
        }
    )
    t_after = int(time.time() * 1000)

    client_status = client_handle.get_status()
    assert client_status.status == WorkflowStatusString.DELAYED.value
    assert client_status.delay_until_epoch_ms is not None
    assert client_status.delay_until_epoch_ms >= t_before + int(delay_seconds * 1000)
    assert client_status.delay_until_epoch_ms <= t_after + int(delay_seconds * 1000)

    client_handle.get_result()

    final_client_status = client_handle.get_status()
    assert final_client_status.status == WorkflowStatusString.SUCCESS.value
    assert final_client_status.dequeued_at is not None
    assert final_client_status.dequeued_at >= client_status.delay_until_epoch_ms

    # Delayed workflows appear in list_workflows and list_queued_workflows
    with SetEnqueueOptions(delay_seconds=60.0):
        listed_handle = queue.enqueue(test_workflow)
    all_workflows = DBOS.list_workflows(status=WorkflowStatusString.DELAYED.value)
    assert any(w.workflow_id == listed_handle.workflow_id for w in all_workflows)
    queued_workflows = DBOS.list_queued_workflows()
    assert any(w.workflow_id == listed_handle.workflow_id for w in queued_workflows)

    # wait_first treats DELAYED as active and unblocks when it completes
    with SetEnqueueOptions(delay_seconds=1.0):
        wait_handle = queue.enqueue(test_workflow)
    assert wait_handle.get_status().status == WorkflowStatusString.DELAYED.value
    completed = DBOS.wait_first([wait_handle])
    assert completed.workflow_id == wait_handle.workflow_id

    # Deduplication: a second enqueue with the same dedup ID should fail while DELAYED
    dedup_id = str(uuid.uuid4())
    with SetEnqueueOptions(delay_seconds=60.0, deduplication_id=dedup_id):
        dedup_handle = queue.enqueue(test_workflow)
    assert dedup_handle.get_status().status == WorkflowStatusString.DELAYED.value
    with pytest.raises(DBOSQueueDeduplicatedError):
        with SetEnqueueOptions(delay_seconds=60.0, deduplication_id=dedup_id):
            queue.enqueue(test_workflow)


def test_delay_cancel_resume_list(dbos: DBOS) -> None:
    queue = Queue("test_delay_cancel_resume_queue", polling_interval_sec=0.1)

    @DBOS.workflow()
    def test_workflow() -> str:
        return "done"

    # Cancel a DELAYED workflow — it should never run
    with SetEnqueueOptions(delay_seconds=60.0):
        cancel_handle = queue.enqueue(test_workflow)
    assert cancel_handle.get_status().status == WorkflowStatusString.DELAYED.value
    DBOS.cancel_workflow(cancel_handle.workflow_id)
    assert cancel_handle.get_status().status == WorkflowStatusString.CANCELLED.value

    # Verify it never appears in the queue after cancellation
    queued = DBOS.list_queued_workflows()
    assert not any(w.workflow_id == cancel_handle.workflow_id for w in queued)

    # Resume a DELAYED workflow — it should run immediately, bypassing the delay
    with SetEnqueueOptions(delay_seconds=60.0):
        resume_handle = queue.enqueue(test_workflow)
    assert resume_handle.get_status().status == WorkflowStatusString.DELAYED.value
    DBOS.resume_workflow(resume_handle.workflow_id)
    assert resume_handle.get_result() == "done"
    final = resume_handle.get_status()
    assert final.status == WorkflowStatusString.SUCCESS.value
