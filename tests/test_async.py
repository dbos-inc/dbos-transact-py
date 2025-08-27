import asyncio
import time
import uuid
from typing import List, Optional

import pytest
import sqlalchemy as sa

# Public API
from dbos import (
    DBOS,
    DBOSConfig,
    Queue,
    SetWorkflowID,
    SetWorkflowTimeout,
    WorkflowHandleAsync,
)
from dbos._context import assert_current_dbos_context
from dbos._dbos import WorkflowHandle
from dbos._dbos_config import ConfigFile
from dbos._error import DBOSAwaitedWorkflowCancelledError, DBOSException


@pytest.mark.asyncio
async def test_async_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res1 = test_transaction(var1)
        res2 = await test_step(var2)
        DBOS.logger.info("I'm test_workflow")
        return res1 + res2

    @DBOS.step()
    async def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var + f"step{step_counter}"

    @DBOS.transaction(isolation_level="SERIALIZABLE")
    def test_transaction(var: str) -> str:
        rows = (DBOS.sql_session.execute(sa.text("SELECT 1"))).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        DBOS.logger.info("I'm test_transaction")
        return var + f"txn{txn_counter}{rows[0][0]}"

    wfuuid = f"test_async_workflow-{time.time_ns()}"
    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "alicetxn11bobstep1"

    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "alicetxn11bobstep1"

    assert wf_counter == 2
    assert step_counter == 1
    assert txn_counter == 1

    # Test DBOS.start_workflow_async
    handle = await DBOS.start_workflow_async(test_workflow, "alice", "bob")
    assert (await handle.get_result()) == "alicetxn21bobstep2"

    # Test DBOS.start_workflow. Not recommended for async workflows,
    # but needed for backwards compatibility.
    sync_handle = DBOS.start_workflow(test_workflow, "alice", "bob")
    assert sync_handle.get_result() == "alicetxn31bobstep3"  # type: ignore

    # Test DBOS.start_workflow_async on steps
    handle = await DBOS.start_workflow_async(test_step, "alice")
    assert (await handle.get_result()) == "alicestep4"


@pytest.mark.asyncio
async def test_async_step(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res1 = test_transaction(var1)
        res2 = await test_step(var2)
        DBOS.logger.info("I'm test_workflow")
        return res1 + res2

    @DBOS.step()
    async def test_step(var: str) -> str:
        await asyncio.sleep(0.1)
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var + f"step{step_counter}"

    @DBOS.transaction(isolation_level="SERIALIZABLE")
    def test_transaction(var: str) -> str:
        rows = (DBOS.sql_session.execute(sa.text("SELECT 1"))).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        DBOS.logger.info("I'm test_transaction")
        return var + f"txn{txn_counter}{rows[0][0]}"

    wfuuid = f"test_async_step-{time.time_ns()}"
    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "alicetxn11bobstep1"

    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "alicetxn11bobstep1"

    assert wf_counter == 2
    assert step_counter == 1
    assert txn_counter == 1


@pytest.mark.asyncio
async def test_send_recv_async(dbos: DBOS) -> None:
    send_counter: int = 0
    recv_counter: int = 0

    @DBOS.workflow()
    async def test_send_workflow(dest_uuid: str, topic: str) -> str:
        await dbos.send_async(dest_uuid, "test1")
        await dbos.send_async(dest_uuid, "test2", topic=topic)
        await dbos.send_async(dest_uuid, "test3")
        nonlocal send_counter
        send_counter += 1
        return dest_uuid

    @DBOS.workflow()
    async def test_recv_workflow(topic: str) -> str:
        msg1 = await dbos.recv_async(topic, timeout_seconds=10)
        msg2 = await dbos.recv_async(timeout_seconds=10)
        msg3 = await dbos.recv_async(timeout_seconds=10)
        nonlocal recv_counter
        recv_counter += 1
        return "-".join([str(msg1), str(msg2), str(msg3)])

    @DBOS.workflow()
    async def test_recv_timeout(timeout_seconds: float) -> None:
        msg = await dbos.recv_async(timeout_seconds=timeout_seconds)
        assert msg is None

    @DBOS.workflow()
    async def test_send_none(dest_uuid: str) -> None:
        await dbos.send_async(dest_uuid, None)

    dest_uuid = str(uuid.uuid4())

    # Send to non-existent uuid should fail
    with pytest.raises(Exception) as exc_info:
        await test_send_workflow(dest_uuid, "testtopic")
    assert f"Sent to non-existent destination workflow ID: {dest_uuid}" in str(
        exc_info.value
    )

    with SetWorkflowID(dest_uuid):
        handle = await dbos.start_workflow_async(test_recv_workflow, "testtopic")
        assert handle.get_workflow_id() == dest_uuid

    send_uuid = str(uuid.uuid4())
    with SetWorkflowID(send_uuid):
        res = await test_send_workflow(handle.get_workflow_id(), "testtopic")
        assert res == dest_uuid
    begin_time = time.time()
    assert (await handle.get_result()) == "test2-test1-test3"
    duration = time.time() - begin_time
    assert duration < 3.0  # Shouldn't take more than 3 seconds to run

    # Test send 'None'
    none_uuid = str(uuid.uuid4())
    none_handle = None
    with SetWorkflowID(none_uuid):
        none_handle = await dbos.start_workflow_async(test_recv_timeout, 10.0)
    await test_send_none(none_uuid)
    begin_time = time.time()
    result = await none_handle.get_result()  # type: ignore
    assert result is None
    duration = time.time() - begin_time
    assert duration < 1.0  # None is from the received message, not from the timeout.

    timeout_uuid = str(uuid.uuid4())
    with SetWorkflowID(timeout_uuid):
        begin_time = time.time()
        await test_recv_timeout(1.0)
        duration = time.time() - begin_time
        assert duration > 0.7

    # Test OAOO
    with SetWorkflowID(send_uuid):
        res = await test_send_workflow(handle.get_workflow_id(), "testtopic")
        assert res == dest_uuid
        assert send_counter == 2

    with SetWorkflowID(dest_uuid):
        begin_time = time.time()
        res = await test_recv_workflow("testtopic")
        duration = time.time() - begin_time
        assert duration < 3.0
        assert res == "test2-test1-test3"
        assert recv_counter == 2

    with SetWorkflowID(timeout_uuid):
        begin_time = time.time()
        await test_recv_timeout(1.0)
        duration = time.time() - begin_time
        assert duration < 0.3

    # Test recv outside of a workflow
    with pytest.raises(Exception) as exc_info:
        await dbos.recv_async("test1")
    assert "recv() must be called from within a workflow" in str(exc_info.value)


@pytest.mark.asyncio
async def test_set_get_events(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def test_setevent_workflow() -> None:
        await dbos.set_event_async("key1", "value1")
        await dbos.set_event_async("key2", "value2")
        await dbos.set_event_async("key3", None)

    @DBOS.workflow()
    async def test_getevent_workflow(
        target_uuid: str, key: str, timeout_seconds: float = 10
    ) -> Optional[str]:
        msg = await dbos.get_event_async(target_uuid, key, timeout_seconds)
        return str(msg) if msg is not None else None

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        await test_setevent_workflow()
    with SetWorkflowID(wfuuid):
        await test_setevent_workflow()

    value1 = await test_getevent_workflow(wfuuid, "key1")
    assert value1 == "value1"

    value2 = await test_getevent_workflow(wfuuid, "key2")
    assert value2 == "value2"

    # Run getEvent outside of a workflow
    value1 = await dbos.get_event_async(wfuuid, "key1")
    assert value1 == "value1"

    value2 = await dbos.get_event_async(wfuuid, "key2")
    assert value2 == "value2"

    begin_time = time.time()
    value3 = await test_getevent_workflow(wfuuid, "key3")
    assert value3 is None
    duration = time.time() - begin_time
    assert duration < 1  # None is from the event not from the timeout

    # Test OAOO
    timeout_uuid = str(uuid.uuid4())
    with SetWorkflowID(timeout_uuid):
        begin_time = time.time()
        res = await test_getevent_workflow("non-existent-uuid", "key1", 1.0)
        duration = time.time() - begin_time
        assert duration > 0.7
        assert res is None

    with SetWorkflowID(timeout_uuid):
        begin_time = time.time()
        res = await test_getevent_workflow("non-existent-uuid", "key1", 1.0)
        duration = time.time() - begin_time
        assert duration < 0.3
        assert res is None

    # No OAOO for getEvent outside of a workflow
    begin_time = time.time()
    res = await dbos.get_event_async("non-existent-uuid", "key1", 1.0)
    duration = time.time() - begin_time
    assert duration > 0.7
    assert res is None

    begin_time = time.time()
    res = await dbos.get_event_async("non-existent-uuid", "key1", 1.0)
    duration = time.time() - begin_time
    assert duration > 0.7
    assert res is None

    # Test setEvent outside of a workflow
    with pytest.raises(Exception) as exc_info:
        await dbos.set_event_async("key1", "value1")
    assert "set_event() must be called from within a workflow" in str(exc_info.value)


@pytest.mark.asyncio
async def test_sleep(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def test_sleep_workflow(secs: float) -> str:
        await dbos.sleep_async(secs)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    start_time = time.time()
    sleep_uuid = await test_sleep_workflow(1.5)
    assert time.time() - start_time > 1.4

    # Test sleep OAOO, skip sleep
    start_time = time.time()
    with SetWorkflowID(sleep_uuid):
        assert (await test_sleep_workflow(1.5)) == sleep_uuid
        assert time.time() - start_time < 0.3


def test_async_tx_raises(config: ConfigFile) -> None:

    with pytest.raises(DBOSException) as exc_info:

        @DBOS.transaction()
        async def test_async_tx() -> None:
            pass

    # destroy call needed to avoid "functions were registered but DBOS() was not called" warning
    DBOS.destroy(destroy_registry=True)


@pytest.mark.asyncio
async def test_start_workflow_async(dbos: DBOS) -> None:
    wf_counter: int = 0
    step_counter: int = 0
    wfuuid = f"test_start_workflow_async-{time.time_ns()}"
    wf_el_id: int = 0
    step_el_id: int = 0

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        nonlocal wf_el_id
        wf_el_id = id(asyncio.get_running_loop())
        nonlocal wf_counter
        wf_counter += 1
        res2 = test_step(var2)
        DBOS.logger.info("I'm test_workflow")
        return var1 + res2

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_el_id
        step_el_id = id(asyncio.get_running_loop())
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var + f"step{step_counter}"

    with SetWorkflowID(wfuuid):
        handle = await DBOS.start_workflow_async(test_workflow, "alice", "bob")

    assert handle.get_workflow_id() == wfuuid
    result = await handle.get_result()
    assert result == "alicebobstep1"

    with SetWorkflowID(wfuuid):
        handle = await DBOS.start_workflow_async(test_workflow, "alice", "bob")

    assert handle.get_workflow_id() == wfuuid
    result = await handle.get_result()
    assert result == "alicebobstep1"

    assert wf_counter == 1
    assert step_counter == 1
    assert wf_el_id == id(asyncio.get_running_loop())
    assert step_el_id == id(asyncio.get_running_loop())


@pytest.mark.asyncio
async def test_retrieve_workflow_async(dbos: DBOS) -> None:
    wfuuid = f"test_retrieve_workflow_async-{time.time_ns()}"

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        var1 = await test_step(var1)
        await DBOS.sleep_async(5)
        return var1 + var2

    @DBOS.step()
    async def test_step(var: str) -> str:
        return var + "d"

    with SetWorkflowID(wfuuid):
        await DBOS.start_workflow_async(test_workflow, "alice", "bob")

    handle: WorkflowHandleAsync[str] = await DBOS.retrieve_workflow_async(wfuuid)
    assert handle.get_workflow_id() == wfuuid
    result = await handle.get_result()
    assert result == "alicedbob"
    wfstatus = await handle.get_status()
    assert wfstatus.status == "SUCCESS"
    assert wfstatus.workflow_id == wfuuid


def test_unawaited_workflow(dbos: DBOS) -> None:
    input = 5
    child_id = str(uuid.uuid4())
    queue = Queue("test_queue")

    @DBOS.workflow()
    async def child_workflow(x: int) -> int:
        await asyncio.sleep(0.1)
        return x

    @DBOS.workflow()
    async def parent_workflow(x: int) -> None:
        with SetWorkflowID(child_id):
            await DBOS.start_workflow_async(child_workflow, x)

    assert queue.enqueue(parent_workflow, input).get_result() is None
    handle: WorkflowHandle[int] = DBOS.retrieve_workflow(
        child_id, existing_workflow=False
    )
    assert handle.get_result() == 5


def test_unawaited_workflow_exception(dbos: DBOS) -> None:
    child_id = str(uuid.uuid4())
    queue = Queue("test_queue")

    @DBOS.workflow()
    async def child_workflow(s: str) -> int:
        await asyncio.sleep(0.1)
        raise Exception(s)

    @DBOS.workflow()
    async def parent_workflow(s: str) -> None:
        with SetWorkflowID(child_id):
            await DBOS.start_workflow_async(child_workflow, s)

    # Verify the unawaited child properly throws an exception
    input = "alice"
    assert queue.enqueue(parent_workflow, input).get_result() is None
    handle: WorkflowHandle[int] = DBOS.retrieve_workflow(
        child_id, existing_workflow=False
    )
    with pytest.raises(Exception) as exc_info:
        handle.get_result()
    assert input in str(exc_info.value)

    # Verify it works if run again
    input = "bob"
    child_id = str(uuid.uuid4())
    assert queue.enqueue(parent_workflow, input).get_result() is None
    handle = DBOS.retrieve_workflow(child_id, existing_workflow=False)
    with pytest.raises(Exception) as exc_info:
        handle.get_result()
    assert input in str(exc_info.value)


@pytest.mark.asyncio
async def test_workflow_timeout_async(dbos: DBOS) -> None:

    @DBOS.workflow()
    async def blocked_workflow() -> None:
        assert assert_current_dbos_context().workflow_timeout_ms is None
        while True:
            DBOS.sleep(0.1)

    with SetWorkflowTimeout(0.1):
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            await blocked_workflow()
        handle = await DBOS.start_workflow_async(blocked_workflow)
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            await handle.get_result()

    @DBOS.workflow()
    async def parent_workflow_with_timeout() -> None:
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is None
        with SetWorkflowTimeout(0.1):
            with pytest.raises(DBOSAwaitedWorkflowCancelledError):
                await blocked_workflow()
            handle = await DBOS.start_workflow_async(blocked_workflow)
            with pytest.raises(DBOSAwaitedWorkflowCancelledError):
                await handle.get_result()
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is None

    # Verify if a parent calls a blocked workflow with a timeout, the child is cancelled
    await parent_workflow_with_timeout()

    start_child, direct_child = str(uuid.uuid4()), str(uuid.uuid4())

    @DBOS.workflow()
    async def parent_workflow() -> None:
        assert assert_current_dbos_context().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is not None
        with SetWorkflowID(start_child):
            await DBOS.start_workflow_async(blocked_workflow)
        with SetWorkflowID(direct_child):
            await blocked_workflow()

    # Verify if a parent called with a timeout calls a blocked child
    # the deadline propagates and the children are also cancelled.
    with SetWorkflowTimeout(1.0):
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            await parent_workflow()

    with pytest.raises(Exception) as exc_info:
        await (await DBOS.retrieve_workflow_async(start_child)).get_result()
    assert "was cancelled" in str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        await (await DBOS.retrieve_workflow_async(direct_child)).get_result()
    assert "was cancelled" in str(exc_info.value)


@pytest.mark.asyncio
async def test_max_parallel_workflows(dbos: DBOS) -> None:
    queue = Queue("parallel_queue")

    @DBOS.workflow()
    async def test_workflow(i: int) -> int:
        await DBOS.sleep_async(5)
        return i

    begin_time = time.time()

    tasks: List[WorkflowHandleAsync[int]] = []
    for i in range(50):
        tasks.append(await DBOS.start_workflow_async(test_workflow, i))

    # Wait for all tasks to complete
    for i in range(50):
        assert (await tasks[i].get_result()) == i, f"Task {i} should return {i}"

    end_time = time.time()
    assert (
        end_time - begin_time < 10
    ), "All tasks should complete in less than 10 seconds"

    # Test enqueues
    begin_time = time.time()
    tasks = []

    for i in range(50):
        tasks.append(await queue.enqueue_async(test_workflow, i))

    # Wait for all tasks to complete
    for i in range(50):
        assert (await tasks[i].get_result()) == i, f"Task {i} should return {i}"

    end_time = time.time()
    assert (
        end_time - begin_time < 10
    ), "All enqueued tasks should complete in less than 10 seconds"


@pytest.mark.asyncio
async def test_main_loop(dbos: DBOS, config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()

    queue = Queue("queue")

    @DBOS.workflow()
    async def test_workflow() -> int:
        await DBOS.sleep_async(0.1)
        return id(asyncio.get_running_loop())

    # Verify the enqueued task is submitted into the main event loop
    handle = await queue.enqueue_async(test_workflow)
    assert await handle.get_result() == id(asyncio.get_running_loop())


@pytest.mark.asyncio
async def test_workflow_with_task_cancellation(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def test_workflow(duration: float) -> str:
        await DBOS.sleep_async(duration)
        return "completed"

    # Run the workflow in an asyncio task
    wfid = str(uuid.uuid4())
    event = asyncio.Event()

    async def run_workflow_task() -> str:
        with SetWorkflowID(wfid):
            handle = await DBOS.start_workflow_async(test_workflow, 1.0)
        event.set()
        return await handle.get_result()

    task = asyncio.create_task(run_workflow_task())

    # Wait for the workflow to start
    await event.wait()

    # Cancel the task
    task.cancel()

    # Verify the task was cancelled
    with pytest.raises(asyncio.CancelledError):
        await task

    # Verify the workflow completes despite the task cancellation
    handle: WorkflowHandleAsync[str] = await DBOS.retrieve_workflow_async(wfid)
    assert await handle.get_result() == "completed"
