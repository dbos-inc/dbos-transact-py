import uuid

import pytest

from dbos import DBOS, Debouncer, SetWorkflowID
from dbos._context import SetEnqueueOptions, SetWorkflowTimeout
from dbos._queue import Queue
from dbos._utils import GlobalParams


def workflow(x: int) -> int:
    return x


async def workflow_async(x: int) -> int:
    return x


def test_debouncer(dbos: DBOS) -> None:

    DBOS.workflow()(workflow)
    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    @DBOS.step()
    def generate_uuid() -> str:
        return str(uuid.uuid4())

    def debouncer_test() -> None:
        debouncer = Debouncer(debounce_key="key", debounce_period_sec=2)

        first_handle = debouncer.debounce(workflow, first_value)
        second_handle = debouncer.debounce(workflow, second_value)
        assert first_handle.workflow_id == second_handle.workflow_id
        assert first_handle.get_result() == second_value
        assert second_handle.get_result() == second_value

        third_handle = debouncer.debounce(workflow, third_value)
        fourth_handle = debouncer.debounce(workflow, fourth_value)
        assert third_handle.workflow_id != first_handle.workflow_id
        assert third_handle.workflow_id == fourth_handle.workflow_id
        assert third_handle.get_result() == fourth_value
        assert fourth_handle.get_result() == fourth_value

        # Test SetWorkflowID works
        wfid = generate_uuid()
        with SetWorkflowID(wfid):
            handle = debouncer.debounce(workflow, first_value)
        assert handle.workflow_id == wfid
        assert handle.get_result() == first_value

    # First, run the test operations directly
    debouncer_test()

    # Then, run the test operations inside a workflow and verify they work there
    debouncer_test_workflow = DBOS.workflow()(debouncer_test)
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        debouncer_test_workflow()

    # Rerun the workflow, verify it still works
    dbos._sys_db.update_workflow_outcome(wfid, "PENDING")
    with SetWorkflowID(wfid):
        debouncer_test_workflow()


def test_debouncer_timeout(dbos: DBOS) -> None:

    DBOS.workflow()(workflow)
    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    # Set a huge period but small timeout, verify workflows start after the timeout
    debouncer = Debouncer(
        debounce_key="key", debounce_period_sec=10000000, debounce_timeout_sec=2
    )

    first_handle = debouncer.debounce(workflow, first_value)
    second_handle = debouncer.debounce(workflow, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value

    third_handle = debouncer.debounce(workflow, third_value)
    fourth_handle = debouncer.debounce(workflow, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value


def test_debouncer_queue(dbos: DBOS) -> None:

    DBOS.workflow()(workflow)
    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3
    queue = Queue("test-queue")

    debouncer = Debouncer(debounce_key="key", debounce_period_sec=2, queue=queue)

    first_handle = debouncer.debounce(workflow, first_value)
    second_handle = debouncer.debounce(workflow, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value
    assert second_handle.get_status().queue_name == queue.name

    # Test SetWorkflowTimeout works
    with SetWorkflowTimeout(1.0):
        third_handle = debouncer.debounce(workflow, third_value)
        fourth_handle = debouncer.debounce(workflow, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value
    assert fourth_handle.get_status().queue_name == queue.name
    assert fourth_handle.get_status().workflow_timeout_ms == 1000.0
    assert fourth_handle.get_status().workflow_deadline_epoch_ms

    # Test SetWorkflowID works
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = debouncer.debounce(workflow, first_value)
    assert handle.workflow_id == wfid
    assert handle.get_result() == first_value
    assert handle.get_status().queue_name == queue.name

    # Test SetEnqueueOptions works
    test_version = "test_version"
    GlobalParams.app_version = test_version
    with SetEnqueueOptions(
        priority=1, deduplication_id="test", app_version=test_version
    ):
        handle = debouncer.debounce(workflow, first_value)
    assert handle.get_result() == first_value
    assert handle.get_status().queue_name == queue.name
    assert handle.get_status().app_version == test_version


@pytest.mark.asyncio
async def test_debouncer_async(dbos: DBOS) -> None:

    DBOS.workflow()(workflow_async)
    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    debouncer = Debouncer(debounce_key="key", debounce_period_sec=2)

    first_handle = await debouncer.debounce_async(workflow_async, first_value)
    second_handle = await debouncer.debounce_async(workflow_async, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert await first_handle.get_result() == second_value
    assert await second_handle.get_result() == second_value

    third_handle = await debouncer.debounce_async(workflow_async, third_value)
    fourth_handle = await debouncer.debounce_async(workflow_async, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert await third_handle.get_result() == fourth_value
    assert await fourth_handle.get_result() == fourth_value
