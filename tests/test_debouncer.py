import time
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
from dbos._sys_db import WorkflowStatus
from dbos._utils import GlobalParams

from .conftest import retry_until_success


def _create_newer_application_version(dbos: DBOS) -> str:
    newer_version = f"newer-{uuid.uuid4()}"
    dbos._sys_db.create_application_version(newer_version)
    dbos._sys_db.update_application_version_timestamp(
        newer_version, int(time.time() * 1000) + 1_000_000
    )
    return newer_version


def _get_debouncer_workflow_for_user_workflow(
    user_workflow_id: str,
) -> WorkflowStatus:
    workflows = DBOS.list_workflows(
        name=DEBOUNCER_WORKFLOW_NAME,
        status="ENQUEUED",
        load_output=False,
    )
    matching_workflows = [
        workflow
        for workflow in workflows
        if workflow.input is not None
        and workflow.input["args"][1]["workflow_id"] == user_workflow_id
    ]
    assert len(matching_workflows) == 1
    return matching_workflows[0]


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


def test_debouncer_uses_latest_application_version(dbos: DBOS) -> None:
    original_version = GlobalParams.app_version
    newer_version = _create_newer_application_version(dbos)

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    debouncer = Debouncer.create(workflow)
    handle = debouncer.debounce(f"key-{uuid.uuid4()}", 0.1, 1)

    try:
        def check_debouncer_enqueued_on_latest_version() -> None:
            debouncer_workflow = _get_debouncer_workflow_for_user_workflow(
                handle.workflow_id
            )
            assert debouncer_workflow.app_version == newer_version
            assert debouncer_workflow.input is not None
            assert debouncer_workflow.input["args"][1]["app_version"] == newer_version

        retry_until_success(
            check_debouncer_enqueued_on_latest_version,
            interval=0.1,
            max_attempts=20,
        )

        GlobalParams.app_version = newer_version
        assert handle.get_result() == 1
        assert handle.get_status().app_version == newer_version
    finally:
        GlobalParams.app_version = original_version


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
    original_version = GlobalParams.app_version
    try:
        GlobalParams.app_version = test_version
        with SetEnqueueOptions(
            priority=1, deduplication_id="test", app_version=test_version
        ):
            handle = debouncer.debounce("key", debounce_period_sec, first_value)
        assert handle.get_result() == first_value
        assert handle.get_status().queue_name == queue.name
        assert handle.get_status().app_version == test_version
    finally:
        GlobalParams.app_version = original_version

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


def test_debouncer_client_uses_latest_application_version(
    dbos: DBOS, client: DBOSClient
) -> None:
    original_version = GlobalParams.app_version

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    DBOS.register_queue("test-queue")
    newer_version = _create_newer_application_version(dbos)

    options: EnqueueOptions = {
        "workflow_name": workflow.__qualname__,
        "queue_name": "test-queue",
    }
    debouncer = DebouncerClient(client, options)
    handle: WorkflowHandle[int] = debouncer.debounce(f"key-{uuid.uuid4()}", 0.1, 1)

    try:
        def check_debouncer_enqueued_on_latest_version() -> None:
            debouncer_workflow = _get_debouncer_workflow_for_user_workflow(
                handle.workflow_id
            )
            assert debouncer_workflow.app_version == newer_version
            assert debouncer_workflow.input is not None
            assert debouncer_workflow.input["args"][1]["app_version"] == newer_version

        retry_until_success(
            check_debouncer_enqueued_on_latest_version,
            interval=0.1,
            max_attempts=20,
        )

        GlobalParams.app_version = newer_version
        assert handle.get_result() == 1
        assert handle.get_status().app_version == newer_version
    finally:
        GlobalParams.app_version = original_version


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
