import time

from dbos import DBOS, DBOSClient, Queue, SetEnqueueOptions
from dbos._dbos import WorkflowHandle
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
