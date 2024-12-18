import asyncio
import time
import uuid
from typing import Optional

import pytest
import sqlalchemy as sa

# Public API
from dbos import DBOS, SetWorkflowID
from dbos._dbos_config import ConfigFile
from dbos._error import DBOSException


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
        res2 = test_step(var2)
        DBOS.logger.info("I'm test_workflow")
        return res1 + res2

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var + f"step{step_counter}"

    @DBOS.transaction(isolation_level="REPEATABLE READ")
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
    dbos._sys_db.wait_for_buffer_flush()

    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "alicetxn11bobstep1"

    assert wf_counter == 2
    assert step_counter == 1
    assert txn_counter == 1


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

    @DBOS.transaction(isolation_level="REPEATABLE READ")
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
    dbos._sys_db.wait_for_buffer_flush()

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
        handle = dbos.start_workflow(test_recv_workflow, "testtopic")
        assert handle.get_workflow_id() == dest_uuid

    send_uuid = str(uuid.uuid4())
    with SetWorkflowID(send_uuid):
        res = await test_send_workflow(handle.get_workflow_id(), "testtopic")
        assert res == dest_uuid
    begin_time = time.time()
    assert handle.get_result() == "test2-test1-test3"
    duration = time.time() - begin_time
    assert duration < 3.0  # Shouldn't take more than 3 seconds to run

    # Test send 'None'
    none_uuid = str(uuid.uuid4())
    none_handle = None
    with SetWorkflowID(none_uuid):
        none_handle = dbos.start_workflow(test_recv_timeout, 10.0)
    await test_send_none(none_uuid)
    begin_time = time.time()
    assert none_handle.get_result() is None
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
        return DBOS.workflow_id

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
    DBOS.destroy()


@pytest.mark.asyncio
async def test_async_step_temp(dbos: DBOS) -> None:
    step_counter: int = 0

    @DBOS.step()
    async def test_step(var: str) -> str:
        await asyncio.sleep(0.1)
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var + f"step{step_counter}"

    wfuuid = f"test_async_step_temp-{time.time_ns()}"
    with SetWorkflowID(wfuuid):
        result = await test_step("alice")
        assert result == "alicestep1"
    dbos._sys_db.wait_for_buffer_flush()

    with SetWorkflowID(wfuuid):
        result = await test_step("alice")
        assert result == "alicestep1"

    assert step_counter == 1
