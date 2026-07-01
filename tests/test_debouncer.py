import uuid

import pytest

from dbos import (
    DBOS,
    DBOSClient,
    Debouncer,
    DebouncerClient,
    SetWorkflowID,
    WorkflowHandle,
    WorkflowHandleAsync,
)
from dbos._client import EnqueueOptions
from dbos._context import SetEnqueueOptions, SetWorkflowTimeout
from dbos._core import DEBOUNCER_WORKFLOW_NAME
from dbos._queue import Queue
from dbos._utils import GlobalParams


def _debouncer_step_names(user_workflow_id: str) -> list[str]:
    # Return the recorded step names of the debouncer that enqueued the given workflow.
    debouncers = [
        w
        for w in DBOS.list_workflows(name=DEBOUNCER_WORKFLOW_NAME)
        if w.input is not None and w.input["args"][1]["workflow_id"] == user_workflow_id
    ]
    assert len(debouncers) == 1
    return [
        s["function_name"] for s in DBOS.list_workflow_steps(debouncers[0].workflow_id)
    ]


def test_debouncer(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    @DBOS.step()
    def generate_uuid() -> str:
        return str(uuid.uuid4())

    def debouncer_test() -> None:
        debounce_period = 2

        debouncer = Debouncer.create(workflow)
        first_handle = debouncer.debounce("key", debounce_period, first_value)
        debouncer = Debouncer.create(workflow)
        second_handle = debouncer.debounce("key", debounce_period, second_value)
        assert first_handle.workflow_id == second_handle.workflow_id
        assert first_handle.get_result() == second_value
        assert second_handle.get_result() == second_value

        debouncer = Debouncer.create(workflow)
        third_handle = debouncer.debounce("key", debounce_period, third_value)
        debouncer = Debouncer.create(workflow)
        fourth_handle = debouncer.debounce("key", debounce_period, fourth_value)
        assert third_handle.workflow_id != first_handle.workflow_id
        assert third_handle.workflow_id == fourth_handle.workflow_id
        assert third_handle.get_result() == fourth_value
        assert fourth_handle.get_result() == fourth_value

        # Test SetWorkflowID works
        wfid = generate_uuid()
        with SetWorkflowID(wfid):
            handle = debouncer.debounce("key", debounce_period, first_value)
        assert handle.workflow_id == wfid
        assert handle.get_result() == first_value

    # First, run the test operations directly
    debouncer_test()

    # Then, run the test operations inside a workflow and verify they work there
    debouncer_test_workflow = DBOS.workflow()(debouncer_test)
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        debouncer_test_workflow()

    # Rerun the workflow, verify it looks up by name and still works
    dbos._sys_db.update_workflow_outcome(wfid, "PENDING")
    dbos._execute_workflow_id(wfid).get_result()


def test_debouncer_timeout(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    # Set a huge period but small timeout, verify workflows start after the timeout
    debouncer = Debouncer.create(
        workflow,
        debounce_timeout_sec=2,
    )
    long_debounce_period = 10000000

    first_handle = debouncer.debounce("key", long_debounce_period, first_value)
    second_handle = debouncer.debounce("key", long_debounce_period, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value

    third_handle = debouncer.debounce("key", long_debounce_period, third_value)
    fourth_handle = debouncer.debounce("key", long_debounce_period, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value

    # Submit first with a long period then with a short one, verify workflows start on time
    debouncer = Debouncer.create(
        workflow,
    )
    short_debounce_period = 1

    first_handle = debouncer.debounce("key", long_debounce_period, first_value)
    second_handle = debouncer.debounce("key", short_debounce_period, second_value)
    assert fourth_handle.workflow_id != first_handle.workflow_id
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value


def test_multiple_debouncers(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    # Set a huge period but small timeout, verify workflows start after the timeout
    debouncer_one = Debouncer.create(workflow)
    debouncer_two = Debouncer.create(workflow)
    debounce_period = 2

    first_handle = debouncer_one.debounce("key_one", debounce_period, first_value)
    second_handle = debouncer_one.debounce("key_one", debounce_period, second_value)
    third_handle = debouncer_two.debounce("key_two", debounce_period, third_value)
    fourth_handle = debouncer_two.debounce("key_two", debounce_period, fourth_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.workflow_id != third_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value


def test_debouncer_best_effort(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value = 0, 1, 2

    # Best-effort mode skips the send/ack handshake but still coalesces.
    debouncer = Debouncer.create(workflow, best_effort=True)
    debounce_period_sec = 2

    first_handle = debouncer.debounce("key", debounce_period_sec, first_value)
    second_handle = debouncer.debounce("key", debounce_period_sec, second_value)
    third_handle = debouncer.debounce("key", debounce_period_sec, third_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.workflow_id == third_handle.workflow_id
    assert first_handle.get_result() == third_value
    assert third_handle.get_result() == third_value

    # Best-effort means the debouncer records no acks, and without an overall timeout
    # it records no per-iteration clock reads either.
    step_names = _debouncer_step_names(first_handle.workflow_id)
    assert "DBOS.setEvent" not in step_names
    assert "get_time" not in step_names
    assert "get_debounce_deadline_epoch_sec" not in step_names
    assert step_names.count("DBOS.recv") >= 1


def test_debouncer_no_timeout_skips_clock_reads(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    # Default debouncer: acks enabled, no overall timeout.
    debouncer = Debouncer.create(workflow)
    debounce_period_sec = 2

    first_handle = debouncer.debounce("key", debounce_period_sec, 0)
    second_handle = debouncer.debounce("key", debounce_period_sec, 1)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == 1

    # No overall timeout means no clock-read steps are recorded
    step_names = _debouncer_step_names(first_handle.workflow_id)
    assert "get_time" not in step_names
    assert "get_debounce_deadline_epoch_sec" not in step_names
    # But acks are still recorded because best_effort was not set
    assert "DBOS.setEvent" in step_names


def test_debouncer_timeout_records_clock_reads(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    # With an overall timeout, the deadline is tracked, so clock reads are recorded.
    debouncer = Debouncer.create(workflow, debounce_timeout_sec=2)
    long_debounce_period = 10000000

    first_handle = debouncer.debounce("key", long_debounce_period, 0)
    second_handle = debouncer.debounce("key", long_debounce_period, 1)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == 1

    step_names = _debouncer_step_names(first_handle.workflow_id)
    assert "get_debounce_deadline_epoch_sec" in step_names
    assert "get_time" in step_names


def test_debouncer_queue(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3
    queue = DBOS.register_queue("test-queue", priority_enabled=True)

    debouncer = Debouncer.create(workflow, queue=queue)
    debounce_period_sec = 2

    first_handle = debouncer.debounce("key", debounce_period_sec, first_value)
    second_handle = debouncer.debounce("key", debounce_period_sec, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value
    assert second_handle.get_status().queue_name == queue.name

    # Test SetWorkflowTimeout works
    with SetWorkflowTimeout(5.0):
        third_handle = debouncer.debounce("key", debounce_period_sec, third_value)
        fourth_handle = debouncer.debounce("key", debounce_period_sec, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value
    assert fourth_handle.get_status().queue_name == queue.name
    assert fourth_handle.get_status().workflow_timeout_ms == 5000.0
    assert fourth_handle.get_status().workflow_deadline_epoch_ms

    # Test SetWorkflowID works
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = debouncer.debounce("key", debounce_period_sec, first_value)
    assert handle.workflow_id == wfid
    assert handle.get_result() == first_value
    assert handle.get_status().queue_name == queue.name

    # Test SetEnqueueOptions works
    test_version = "test_version"
    GlobalParams.app_version = test_version
    with SetEnqueueOptions(
        priority=1, deduplication_id="test", app_version=test_version
    ):
        handle = debouncer.debounce("key", debounce_period_sec, first_value)
    assert handle.get_result() == first_value
    assert handle.get_status().queue_name == queue.name
    assert handle.get_status().app_version == test_version

    # The queue argument also accepts a queue name as a string.
    debouncer_by_name = Debouncer.create(workflow, queue=queue.name)
    name_handle = debouncer_by_name.debounce(
        "string-key", debounce_period_sec, first_value
    )
    assert name_handle.get_result() == first_value
    assert name_handle.get_status().queue_name == queue.name


@pytest.mark.asyncio
async def test_debouncer_async(dbos: DBOS) -> None:

    @DBOS.workflow()
    async def workflow_async(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    debouncer = Debouncer.create_async(workflow_async)
    debounce_period_sec = 2

    first_handle = await debouncer.debounce_async(
        "key", debounce_period_sec, first_value
    )
    second_handle = await debouncer.debounce_async(
        "key", debounce_period_sec, second_value
    )
    assert first_handle.workflow_id == second_handle.workflow_id
    assert await first_handle.get_result() == second_value
    assert await second_handle.get_result() == second_value

    third_handle = await debouncer.debounce_async(
        "key", debounce_period_sec, third_value
    )
    fourth_handle = await debouncer.debounce_async(
        "key", debounce_period_sec, fourth_value
    )
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert await third_handle.get_result() == fourth_value
    assert await fourth_handle.get_result() == fourth_value


def test_debouncer_client(dbos: DBOS, client: DBOSClient) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3
    DBOS.register_queue("test-queue")

    options: EnqueueOptions = {
        "workflow_name": workflow.__qualname__,
        "queue_name": "test-queue",
    }
    debouncer = DebouncerClient(client, options)
    debounce_period_sec = 2

    first_handle: WorkflowHandle[int] = debouncer.debounce(
        "key", debounce_period_sec, first_value
    )
    second_handle: WorkflowHandle[int] = debouncer.debounce(
        "key", debounce_period_sec, second_value
    )
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value

    third_handle: WorkflowHandle[int] = debouncer.debounce(
        "key", debounce_period_sec, third_value
    )
    fourth_handle: WorkflowHandle[int] = debouncer.debounce(
        "key", debounce_period_sec, fourth_value
    )
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value

    wfid = str(uuid.uuid4())
    options["workflow_id"] = wfid
    handle: WorkflowHandle[int] = debouncer.debounce(
        "key", debounce_period_sec, first_value
    )
    assert handle.workflow_id == wfid
    assert handle.get_result() == first_value


@pytest.mark.asyncio
async def test_debouncer_client_async(dbos: DBOS, client: DBOSClient) -> None:

    @DBOS.workflow()
    async def workflow_async(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3
    await DBOS.register_queue_async("test-queue")

    options: EnqueueOptions = {
        "workflow_name": workflow_async.__qualname__,
        "queue_name": "test-queue",
    }
    debouncer = DebouncerClient(client, options)
    debounce_period_sec = 2

    first_handle: WorkflowHandleAsync[int] = await debouncer.debounce_async(
        "key", debounce_period_sec, first_value
    )
    second_handle: WorkflowHandleAsync[int] = await debouncer.debounce_async(
        "key", debounce_period_sec, second_value
    )
    assert first_handle.workflow_id == second_handle.workflow_id
    assert await first_handle.get_result() == second_value
    assert await second_handle.get_result() == second_value

    third_handle: WorkflowHandleAsync[int] = await debouncer.debounce_async(
        "key", debounce_period_sec, third_value
    )
    fourth_handle: WorkflowHandleAsync[int] = await debouncer.debounce_async(
        "key", debounce_period_sec, fourth_value
    )
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert await third_handle.get_result() == fourth_value
    assert await fourth_handle.get_result() == fourth_value

    wfid = str(uuid.uuid4())
    options["workflow_id"] = wfid
    handle: WorkflowHandleAsync[int] = await debouncer.debounce_async(
        "key", debounce_period_sec, first_value
    )
    assert handle.workflow_id == wfid
    assert await handle.get_result() == first_value
