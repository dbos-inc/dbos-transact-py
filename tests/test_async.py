import uuid

import pytest
import sqlalchemy as sa

# Public API
from dbos import DBOS, SetWorkflowID

# Private API because this is a test


@pytest.mark.asyncio
async def test_async_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res1 = await test_transaction(var1)
        res2 = await test_step(var2)
        DBOS.logger.info("I'm test_workflow")
        return res1 + res2

    @DBOS.step()
    async def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var + f"step{step_counter}"

    @DBOS.transaction(isolation_level="REPEATABLE READ")
    async def test_transaction(var: str) -> str:
        rows = (await DBOS.sql_session_async.execute(sa.text("SELECT 1"))).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        DBOS.logger.info("I'm test_transaction")
        return var + f"txn{txn_counter}{rows[0][0]}"

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "alicetxn11bobstep1"
    await dbos._sys_db.wait_for_buffer_flush()
    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "alicetxn11bobstep1"

    assert wf_counter == 2
    assert step_counter == 1
    assert txn_counter == 1
