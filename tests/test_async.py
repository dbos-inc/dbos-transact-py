import asyncio
import threading
import time
import uuid
from typing import Any, List, Optional, cast

import pytest
import sqlalchemy as sa

# Public API
from dbos import (
    DBOS,
    DBOSConfig,
    Queue,
    SendMessage,
    SetWorkflowID,
    SetWorkflowTimeout,
    WorkflowHandleAsync,
)
from dbos._context import assert_current_dbos_context, get_local_dbos_context
from dbos._dbos import WorkflowHandle
from dbos._dbos_config import ConfigFile
from dbos._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSException,
    DBOSPatchNondeterminismError,
)
from dbos._event_loop import BackgroundEventLoop
from dbos._schemas.system_database import SystemSchema
from tests.conftest import retry_until_success_async


@pytest.mark.asyncio
async def test_async_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res1 = await asyncio.to_thread(test_transaction, var1)
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
    def fn() -> None:
        sync_handle = DBOS.start_workflow(test_workflow, "alice", "bob")
        assert sync_handle.get_result() == "alicetxn31bobstep3"  # type: ignore

    await asyncio.to_thread(fn)

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
        res1 = await asyncio.to_thread(test_transaction, var1)
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
    assert (
        f"DBOS Error 5: Non-existent `send` destination workflow ID: {dest_uuid}"
        in str(exc_info.value)
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
    assert duration < 9.0

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
    assert duration < 9.0

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
        assert duration < 0.9

    # Test recv outside of a workflow
    with pytest.raises(Exception) as exc_info:
        await dbos.recv_async("test1")
    assert "recv() must be called from within a workflow" in str(exc_info.value)


@pytest.mark.asyncio
async def test_recv_async_cancelled_during_setup(dbos: DBOS) -> None:
    """Cancelling recv_async while its setup phase runs in a worker thread
    must not leave a notifications_map registration behind. Cleanup is
    synchronous: the moment the cancelled call returns, the entry is gone, so
    there is no window in which the next recv on the same workflow and topic
    raises a spurious DBOSWorkflowConflictIDError."""

    @DBOS.workflow()
    async def noop_workflow() -> None:
        return None

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await noop_workflow()

    sys_db = dbos._sys_db
    topic = "cancel_topic"
    payload = f"{wfid}::{topic}"

    in_setup = threading.Event()
    release_setup = threading.Event()
    original_recv_check = sys_db.recv_check

    # recv_setup calls recv_check after registering in notifications_map,
    # so blocking here holds the setup thread mid-registration.
    def blocking_recv_check(*args: Any, **kwargs: Any) -> None:
        in_setup.set()
        assert release_setup.wait(timeout=10)
        original_recv_check(*args, **kwargs)

    sys_db.recv_check = blocking_recv_check  # type: ignore[method-assign]
    try:
        recv_task = asyncio.create_task(
            sys_db.recv_async(wfid, 100, 101, topic, timeout_seconds=10)
        )
        assert await asyncio.to_thread(in_setup.wait, 10)
        # Cancel while the thread is mid-registration, then let it finish so
        # the cancellation's synchronous cleanup can run to completion.
        recv_task.cancel()
        release_setup.set()
        with pytest.raises(asyncio.CancelledError):
            await recv_task
        # Cleanup happened before CancelledError propagated -- no polling
        # window for a concurrent recv to trip over a stale entry.
        assert sys_db.notifications_map.get(payload) is None
    finally:
        sys_db.recv_check = original_recv_check  # type: ignore[method-assign]

    # A later recv on the same workflow and topic must not see a stale entry.
    message = await sys_db.recv_async(wfid, 102, 103, topic, timeout_seconds=0.1)
    assert message is None
    assert sys_db.notifications_map.get(payload) is None


@pytest.mark.asyncio
async def test_get_event_async_cancelled_during_setup(dbos: DBOS) -> None:
    """Cancelling get_event_async while its setup phase runs in a worker
    thread must not leave a workflow_events_map registration behind, and
    cleanup must complete synchronously before CancelledError propagates."""

    @DBOS.workflow()
    async def noop_workflow() -> None:
        return None

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await noop_workflow()

    sys_db = dbos._sys_db
    key = "cancel_key"
    payload = f"{wfid}::{key}"

    in_setup = threading.Event()
    release_setup = threading.Event()
    original_get_event_check = sys_db.get_event_check

    def blocking_get_event_check(*args: Any, **kwargs: Any) -> None:
        in_setup.set()
        assert release_setup.wait(timeout=10)
        original_get_event_check(*args, **kwargs)

    sys_db.get_event_check = blocking_get_event_check  # type: ignore[method-assign]
    try:
        get_event_task = asyncio.create_task(
            sys_db.get_event_async(wfid, key, timeout_seconds=10)
        )
        assert await asyncio.to_thread(in_setup.wait, 10)
        get_event_task.cancel()
        release_setup.set()
        with pytest.raises(asyncio.CancelledError):
            await get_event_task
        assert sys_db.workflow_events_map.get(payload) is None
    finally:
        sys_db.get_event_check = original_get_event_check  # type: ignore[method-assign]


@pytest.mark.asyncio
async def test_send_bulk_async(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def recv_two() -> str:
        msg1 = await dbos.recv_async(timeout_seconds=10)
        msg2 = await dbos.recv_async(timeout_seconds=10)
        return f"{msg1}-{msg2}"

    # Bulk send from outside a workflow.
    dest_uuid = str(uuid.uuid4())
    with SetWorkflowID(dest_uuid):
        handle = await dbos.start_workflow_async(recv_two)

    await DBOS.send_bulk_async(
        [
            SendMessage(dest_uuid, "a"),
            SendMessage(dest_uuid, "b"),
        ]
    )
    assert (await handle.get_result()) == "a-b"

    # Bulk send from within a workflow, recorded as a single step.
    dest_uuid2 = str(uuid.uuid4())
    with SetWorkflowID(dest_uuid2):
        handle2 = await dbos.start_workflow_async(recv_two)

    @DBOS.workflow()
    async def send_bulk_wf(dest: str) -> None:
        await DBOS.send_bulk_async(
            [
                SendMessage(dest, "c"),
                SendMessage(dest, "d"),
            ]
        )

    await send_bulk_wf(dest_uuid2)
    assert (await handle2.get_result()) == "c-d"


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

    assert "is a coroutine function" in str(exc_info.value)
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
        res2 = await test_step(var2)
        DBOS.logger.info("I'm test_workflow")
        return var1 + res2

    @DBOS.step()
    async def test_step(var: str) -> str:
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
    result = cast(str, await DBOS.get_result_async(wfuuid))
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
    DBOS.register_queue("test_queue")

    @DBOS.workflow()
    async def child_workflow(x: int) -> int:
        await asyncio.sleep(0.1)
        return x

    @DBOS.workflow()
    async def parent_workflow(x: int) -> None:
        with SetWorkflowID(child_id):
            await DBOS.start_workflow_async(child_workflow, x)

    assert (
        DBOS.enqueue_workflow("test_queue", parent_workflow, input).get_result() is None
    )
    handle: WorkflowHandle[int] = DBOS.retrieve_workflow(
        child_id, existing_workflow=False
    )
    assert handle.get_result() == 5


def test_unawaited_workflow_exception(dbos: DBOS) -> None:
    child_id = str(uuid.uuid4())
    DBOS.register_queue("test_queue")

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
    assert (
        DBOS.enqueue_workflow("test_queue", parent_workflow, input).get_result() is None
    )
    handle: WorkflowHandle[int] = DBOS.retrieve_workflow(
        child_id, existing_workflow=False
    )
    with pytest.raises(Exception) as exc_info:
        handle.get_result()
    assert input in str(exc_info.value)

    # Verify it works if run again
    input = "bob"
    child_id = str(uuid.uuid4())
    assert (
        DBOS.enqueue_workflow("test_queue", parent_workflow, input).get_result() is None
    )
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
            await DBOS.sleep_async(0.1)

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

    # Verify all timeout tasks complete. A task may still be finishing its
    # (redundant) cancellation of an already-cancelled workflow, so allow it
    # a moment to drain rather than asserting instantaneous emptiness.
    deadline = time.time() + 30
    while dbos._timeout_tasks and time.time() < deadline:
        await asyncio.sleep(0.1)
    assert len(dbos._timeout_tasks) == 0


@pytest.mark.asyncio
async def test_max_parallel_workflows(dbos: DBOS) -> None:
    await DBOS.register_queue_async("parallel_queue")

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

    # The workflows sleep 5s each, so anything well under the 250s serial
    # time proves they ran in parallel; the margin absorbs slow CI runners.
    end_time = time.time()
    assert (
        end_time - begin_time < 30
    ), "All tasks should complete in less than 30 seconds"

    # Test enqueues
    begin_time = time.time()
    tasks = []

    for i in range(50):
        tasks.append(
            await DBOS.enqueue_workflow_async("parallel_queue", test_workflow, i)
        )

    # Wait for all tasks to complete
    for i in range(50):
        assert (await tasks[i].get_result()) == i, f"Task {i} should return {i}"

    end_time = time.time()
    assert (
        end_time - begin_time < 30
    ), "All enqueued tasks should complete in less than 30 seconds"


@pytest.mark.asyncio
async def test_main_loop(dbos: DBOS, config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()

    await DBOS.register_queue_async("queue")

    @DBOS.workflow()
    async def test_workflow() -> int:
        await DBOS.sleep_async(0.1)
        return id(asyncio.get_running_loop())

    # Verify the enqueued task is submitted into the main event loop
    handle = await DBOS.enqueue_workflow_async("queue", test_workflow)
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


@pytest.mark.asyncio
async def test_get_events_async(dbos: DBOS) -> None:
    """Test the async version of get_events function that retrieves all events for a workflow."""

    @DBOS.workflow()
    async def async_events_workflow() -> str:
        # Set multiple events using async methods
        await DBOS.set_event_async("event1", "value1")
        await DBOS.set_event_async("event2", {"nested": "data", "count": 42})
        await DBOS.set_event_async("event3", [1, 2, 3, 4, 5])
        return "completed"

    # Execute the workflow
    handle = await DBOS.start_workflow_async(async_events_workflow)
    result = await handle.get_result()
    assert result == "completed"

    # Get all events for the workflow using async method
    events = await DBOS.get_all_events_async(handle.workflow_id)

    # Verify all events are present with correct values
    assert len(events) == 3
    assert events["event1"] == "value1"
    assert events["event2"] == {"nested": "data", "count": 42}
    assert events["event3"] == [1, 2, 3, 4, 5]

    # Test with a workflow that has no events
    @DBOS.workflow()
    async def no_events_workflow() -> str:
        await DBOS.sleep_async(0.01)
        return "no events"

    handle2 = await DBOS.start_workflow_async(no_events_workflow)
    await handle2.get_result()

    # Should return empty dict for workflow with no events
    events2 = await DBOS.get_all_events_async(handle2.workflow_id)
    assert events2 == {}


@pytest.mark.asyncio
async def test_child_workflow_async(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def child_workflow(var: str) -> str:
        return var + "_child"

    @DBOS.workflow()
    async def parent_workflow() -> tuple[str, str, str]:
        parent_id = DBOS.workflow_id
        assert parent_id is not None
        result = await child_workflow("test")
        child_handle = await dbos.start_workflow_async(child_workflow, "async")
        child_result = await child_handle.get_result()
        return parent_id, child_handle.workflow_id, result + child_result

    # Run the parent workflow and get its ID
    handle = await dbos.start_workflow_async(parent_workflow)
    parent_id, async_child_id, result = await handle.get_result()

    assert result == "test_childasync_child"

    # Verify parent workflow has no parent
    parent_status = await DBOS.get_workflow_status_async(parent_id)
    assert parent_status is not None
    assert parent_status.parent_workflow_id is None

    # The synchronous child workflow ID follows the pattern: parent_id-function_id
    sync_child_id = f"{parent_id}-1"
    sync_child_status = await DBOS.get_workflow_status_async(sync_child_id)
    assert sync_child_status is not None
    assert sync_child_status.parent_workflow_id == parent_id

    # The async child workflow should also have the parent ID set
    async_child_status = await DBOS.get_workflow_status_async(async_child_id)
    assert async_child_status is not None
    assert async_child_status.parent_workflow_id == parent_id


@pytest.mark.asyncio
async def test_asyncio_wait(dbos: DBOS) -> None:
    step_counter: int = 0
    gate = asyncio.Event()

    @DBOS.step()
    async def fast_step(val: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return val + "_done"

    @DBOS.step()
    async def slow_step(val: str) -> str:
        nonlocal step_counter
        step_counter += 1
        await gate.wait()
        return val + "_done"

    @DBOS.workflow()
    async def wait_workflow() -> None:
        done, pending = await DBOS.asyncio_wait(
            [fast_step("fast"), slow_step("slow")],
            return_when=asyncio.FIRST_COMPLETED,
        )

        assert len(done) == 1
        assert len(pending) == 1
        assert [t.result() for t in done] == ["fast_done"]

        # Let the slow step finish and wait for it
        gate.set()
        done2, pending2 = await DBOS.asyncio_wait(list(pending))
        assert len(done2) == 1
        assert len(pending2) == 0
        assert [t.result() for t in done2] == ["slow_done"]

    handle = await DBOS.start_workflow_async(wait_workflow)
    await handle.get_result()
    assert step_counter == 2

    # Verify recorded steps
    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    assert len(steps) == 4
    # Step 1: first asyncio_wait snapshots its context before the tasks run.
    # Recorded done indices [0] means the first future (fast_step) completed.
    assert steps[0]["function_id"] == 1
    assert steps[0]["function_name"] == "DBOS.asyncio_wait"
    assert steps[0]["output"] == [0]
    # Steps 2 & 3: the step coroutines execute inside the asyncio tasks
    assert steps[1]["function_id"] == 2
    assert steps[1]["function_name"] == fast_step.__qualname__
    assert steps[1]["output"] == "fast_done"
    assert steps[2]["function_id"] == 3
    assert steps[2]["function_name"] == slow_step.__qualname__
    assert steps[2]["output"] == "slow_done"
    # Step 4: second asyncio_wait on the pending set
    assert steps[3]["function_id"] == 4
    assert steps[3]["function_name"] == "DBOS.asyncio_wait"
    assert steps[3]["output"] == [0]

    # Fork from a high step to replay everything from DB (OAOO)
    forked = await DBOS.fork_workflow_async(handle.workflow_id, 100)
    await forked.get_result()
    assert step_counter == 2


@pytest.mark.asyncio
async def test_asyncio_wait_all_completed(dbos: DBOS) -> None:
    step_counter: int = 0

    @DBOS.step()
    async def my_step(val: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return val

    @DBOS.workflow()
    async def wait_all_workflow() -> None:
        done, pending = await DBOS.asyncio_wait(
            [my_step("a"), my_step("b")], return_when=asyncio.ALL_COMPLETED
        )

        assert len(done) == 2
        assert len(pending) == 0
        assert sorted([t.result() for t in done]) == ["a", "b"]

    handle = await DBOS.start_workflow_async(wait_all_workflow)
    await handle.get_result()
    assert step_counter == 2

    # Fork from a high step to replay everything from DB (OAOO)
    forked = await DBOS.fork_workflow_async(handle.workflow_id, 100)
    await forked.get_result()
    assert step_counter == 2


@pytest.mark.asyncio
async def test_asyncio_wait_first_exception(dbos: DBOS) -> None:
    step_counter: int = 0
    gate = asyncio.Event()

    @DBOS.step()
    async def error_step() -> str:
        nonlocal step_counter
        step_counter += 1
        raise ValueError("boom")

    @DBOS.step()
    async def slow_step(val: str) -> str:
        nonlocal step_counter
        step_counter += 1
        await gate.wait()
        return val

    @DBOS.workflow()
    async def wait_exception_workflow() -> None:
        done, pending = await DBOS.asyncio_wait(
            [error_step(), slow_step("ok")],
            return_when=asyncio.FIRST_EXCEPTION,
        )

        assert len(done) == 1
        assert len(pending) == 1
        task = next(iter(done))
        assert isinstance(task.exception(), ValueError)
        assert "boom" in str(task.exception())

        # Let the slow step finish and wait for it
        gate.set()
        done2, pending2 = await DBOS.asyncio_wait(list(pending))
        assert len(done2) == 1
        assert len(pending2) == 0
        assert next(iter(done2)).result() == "ok"

    handle = await DBOS.start_workflow_async(wait_exception_workflow)
    await handle.get_result()
    assert step_counter == 2

    # Fork from a high step to replay everything from DB (OAOO)
    forked = await DBOS.fork_workflow_async(handle.workflow_id, 100)
    await forked.get_result()
    assert step_counter == 2


@pytest.mark.asyncio
async def test_asyncio_wait_timeout(dbos: DBOS) -> None:
    step_counter: int = 0
    gate = asyncio.Event()

    @DBOS.step()
    async def blocked_step(val: str) -> str:
        nonlocal step_counter
        step_counter += 1
        await gate.wait()
        return val

    @DBOS.workflow()
    async def wait_timeout_workflow() -> None:
        done, pending = await DBOS.asyncio_wait(
            [blocked_step("a"), blocked_step("b")],
            timeout=0.1,
        )

        # Both should still be pending after timeout
        assert len(done) == 0
        assert len(pending) == 2

        # Unblock and wait for all
        gate.set()
        done2, pending2 = await DBOS.asyncio_wait(list(pending))
        assert len(done2) == 2
        assert len(pending2) == 0
        assert sorted([t.result() for t in done2]) == ["a", "b"]

    handle = await DBOS.start_workflow_async(wait_timeout_workflow)
    await handle.get_result()
    assert step_counter == 2

    # Fork from a high step to replay everything from DB (OAOO)
    forked = await DBOS.fork_workflow_async(handle.workflow_id, 100)
    await forked.get_result()
    assert step_counter == 2


@pytest.mark.asyncio
async def test_workflow_recovery_async(dbos: DBOS, config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()

    step_counter: int = 0
    wf_counter: int = 0
    value = "value"

    wf_event_loop_id: int = 0

    @DBOS.workflow()
    async def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter, wf_event_loop_id
        wf_counter += 1
        wf_event_loop_id = id(asyncio.get_running_loop())
        output = await test_step(var2)
        assert output == var2
        return var

    @DBOS.step()
    async def test_step(var2: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return var2

    test_event_loop_id = id(asyncio.get_running_loop())

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        assert (await test_workflow(value, value)) == value
    # Verify the workflow runs in the test event loop
    assert wf_event_loop_id == test_event_loop_id
    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": test_workflow.__qualname__})
            .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
        )

    # Run recovery (which is fundamentally sync) in a thread
    def recover_in_thread() -> Any:
        handles = DBOS._recover_pending_workflows()
        assert len(handles) == 1
        return handles[0].get_result()

    recovered_result = await asyncio.to_thread(recover_in_thread)
    assert recovered_result
    assert wf_counter == 2
    assert step_counter == 1
    # Verify the recovered workflow runs in the test event loop
    assert wf_event_loop_id == test_event_loop_id

    # Test that there was a recovery attempt of this
    stat = await DBOS.get_workflow_status_async(workflow_id)
    assert stat
    assert stat.recovery_attempts == 2


@pytest.mark.asyncio
async def test_concurrent_patch_async(dbos: DBOS, config: DBOSConfig) -> None:
    # Calling DBOS.patch_async from concurrent tasks inside one workflow is
    # inherently nondeterministic: the position each patch marker lands at
    # depends on task scheduling, so a replay cannot reliably match markers
    # to patch calls. Instead of recording a scheduling-dependent history
    # (or corrupting the checkpoint counter and zombie-polling, see #714),
    # patch_async must detect the interleaving and fail the workflow with a
    # clean error.
    DBOS.destroy(destroy_registry=True)
    config["enable_patching"] = True
    DBOS(config=config)

    @DBOS.step()
    async def small_step(tag: str, i: int) -> str:
        await asyncio.sleep(0)
        return f"{tag}:{i}"

    async def patch_then_steps(tag: str) -> str:
        await DBOS.patch_async(tag)
        for i in range(5):
            await small_step(tag, i)
            await asyncio.sleep(0)
        return tag

    @DBOS.workflow()
    async def parallel_patch_workflow() -> List[str]:
        return list(
            await asyncio.gather(
                patch_then_steps("a"), patch_then_steps("b"), patch_then_steps("c")
            )
        )

    DBOS.launch()

    # The detection is deterministic: the gather schedules all three probes
    # before any of them can reserve a checkpoint position, so every task
    # that loses the race observes another task's reservation and raises.
    # Repeat to exercise many interleavings of which task wins the race.
    for i in range(15):
        wfid = f"concurrent-patch-{i}"
        with SetWorkflowID(wfid):
            with pytest.raises(DBOSPatchNondeterminismError, match="concurrently"):
                # The guard is generous because SQLite busy_timeout stalls can
                # reach 30s; it exists only so a regression cannot hang the test.
                await asyncio.wait_for(parallel_patch_workflow(), timeout=60.0)

        # The workflow fails cleanly rather than hanging or zombie-polling.
        status = await DBOS.get_workflow_status_async(wfid)
        assert status is not None
        assert status.status == "ERROR"


def test_destroy_from_adopted_main_loop_does_not_deadlock(
    config: DBOSConfig, cleanup_test_databases: None
) -> None:
    """Reproduce the DBOS.destroy() self-deadlock.

    When DBOS.launch() runs inside a running event loop, DBOS adopts that loop
    as its main loop. A workflow with a timeout parks a "timeout task" on that
    loop. If destroy() is then called from that same loop's thread while a
    timeout task is still pending, destroy() schedules a cancellation coroutine
    onto the loop and blocks the calling thread on .result() -- but the loop is
    the calling thread, so it can never run the coroutine. Permanent hang.

    We run the whole launch + timeout + destroy scenario on a dedicated thread
    with its own event loop, then join it with a timeout. Before the fix the
    thread deadlocks (join times out); after the fix it completes promptly.
    """
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def wf_with_timeout() -> str:
        return "done"

    scenario_error: List[BaseException] = []

    def run_scenario() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def scenario() -> None:
            # Launch from within the running loop so DBOS adopts it as main loop.
            DBOS.launch()

            # The workflow returns immediately, but its timeout task stays parked
            # in asyncio.sleep() on this loop, so it is still pending in
            # dbos._timeout_tasks when we destroy.
            with SetWorkflowTimeout(30):
                assert wf_with_timeout() == "done"

            # Poll (don't sleep a fixed amount) until the loop has actually
            # created the parked timeout task. The async retry yields to the loop
            # between attempts, so the task gets a chance to be created.
            def timeout_task_is_pending() -> None:
                assert dbos._timeout_tasks, "expected a pending timeout task"

            await retry_until_success_async(
                timeout_task_is_pending, interval=0.2, max_attempts=50
            )

            # Called from the adopted main-loop thread -> deadlocks before the fix.
            DBOS.destroy(destroy_registry=True)

        try:
            loop.run_until_complete(scenario())
        except BaseException as e:
            scenario_error.append(e)
        finally:
            loop.close()

    worker = threading.Thread(
        target=run_scenario, name="dbos-destroy-scenario", daemon=True
    )
    worker.start()
    worker.join(timeout=20)

    assert not worker.is_alive(), (
        "DBOS.destroy() deadlocked: it was called from the adopted main-loop "
        "thread while a timeout task was still pending."
    )
    if scenario_error:
        raise scenario_error[0]


@pytest.mark.asyncio
async def test_submit_coroutine_from_own_loop_raises_instead_of_hanging() -> None:
    """submit_coroutine must fail loudly, not deadlock, when called from its own loop.

    Blocking on .result() from the thread that runs the target loop can never
    complete (the loop can't run the coroutine while blocked waiting for it), so
    the call must raise rather than hang.
    """
    bg = BackgroundEventLoop()
    # start() adopts the currently-running loop (this test's loop) as the main loop.
    bg.start()
    try:

        async def noop() -> int:
            return 42

        assert bg.target_loop() is asyncio.get_running_loop()
        coro = noop()
        with pytest.raises(RuntimeError, match="deadlock"):
            bg.submit_coroutine(coro)
    finally:
        bg.stop()


# Regression: a concurrent end_workflow() (shutdown) could blank an async child's id while its registration ran in a to_thread worker, persisting empty ids.


@pytest.mark.asyncio
async def test_async_child_id_survives_concurrent_context_clear(dbos: DBOS) -> None:
    """Deterministically reproduce the race: while the child's registration runs in
    a worker thread, clear the child context's workflow_id (exactly what shutdown's
    end_workflow() does). The recorded ids must still be correct, not empty."""

    @DBOS.workflow()
    async def child(x: int) -> int:
        return x

    @DBOS.workflow()
    async def parent(n: int) -> list:
        return await asyncio.gather(*[child(i) for i in range(n)])

    real_check = dbos._sys_db.check_operation_execution
    real_record = dbos._sys_db.record_child_workflow
    fired: list = []

    def check_and_clear(*args, **kwargs):  # type: ignore[no-untyped-def]
        # First sys-db call in the child's init_wf (to_thread worker); blanking workflow_id here simulates a concurrent end_workflow() on shutdown.
        result = real_check(*args, **kwargs)
        ctx = get_local_dbos_context()
        if ctx is not None and ctx.has_parent() and ctx.workflow_id:
            ctx._test_saved_wfid = ctx.workflow_id  # type: ignore[attr-defined]
            ctx.workflow_id = ""
            fired.append(1)
        return result

    def record_and_restore(*args, **kwargs):  # type: ignore[no-untyped-def]
        # Last sys-db call in init_wf; restore the id so the child's normal __exit__ stays consistent in this non-cancelled test.
        try:
            return real_record(*args, **kwargs)
        finally:
            ctx = get_local_dbos_context()
            saved = getattr(ctx, "_test_saved_wfid", "") if ctx is not None else ""
            if saved:
                ctx.workflow_id = saved  # type: ignore[union-attr]

    dbos._sys_db.check_operation_execution = check_and_clear  # type: ignore[method-assign]
    dbos._sys_db.record_child_workflow = record_and_restore  # type: ignore[method-assign]
    try:
        fanout = 5
        assert (await parent(fanout)) == list(range(fanout))
    finally:
        dbos._sys_db.check_operation_execution = real_check  # type: ignore[method-assign]
        dbos._sys_db.record_child_workflow = real_record  # type: ignore[method-assign]

    # The injection must have actually fired (otherwise the test proves nothing).
    assert len(fired) >= fanout

    # No corrupt rows anywhere in the system database.
    with dbos._sys_db.engine.connect() as c:
        empty_status = c.execute(
            sa.select(sa.func.count())
            .select_from(SystemSchema.workflow_status)
            .where(sa.func.length(SystemSchema.workflow_status.c.workflow_uuid) == 0)
        ).scalar()
        empty_links = c.execute(
            sa.select(sa.func.count())
            .select_from(SystemSchema.operation_outputs)
            .where(SystemSchema.operation_outputs.c.child_workflow_id == "")
        ).scalar()
        child_links = c.execute(
            sa.select(SystemSchema.operation_outputs.c.child_workflow_id).where(
                SystemSchema.operation_outputs.c.child_workflow_id.isnot(None)
            )
        ).fetchall()

    assert empty_status == 0
    assert empty_links == 0
    # Every recorded child link points at a real child workflow row.
    for (child_id,) in child_links:
        assert child_id
        status = dbos._sys_db.get_workflow_status(child_id)
        assert status is not None


def test_record_child_workflow_rejects_empty_id(dbos: DBOS) -> None:
    """The persistence guard: an empty child id is never valid and must fail loudly
    rather than silently corrupt operation_outputs."""
    with pytest.raises(DBOSException):
        dbos._sys_db.record_child_workflow("some-parent-id", "", 0, "some.func")
