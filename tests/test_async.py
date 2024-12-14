import uuid

import pytest
import sqlalchemy as sa

# Public API
from dbos import DBOS, SetWorkflowID
from dbos._context import get_local_dbos_context


@pytest.mark.asyncio
async def test_async_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        ctx = get_local_dbos_context()
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

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "alicetxn11bobstep1"
    # dbos._sys_db.wait_for_buffer_flush()

    # with SetWorkflowID(wfuuid):
    #     result = await test_workflow("alice", "bob")
    #     assert result == "alicetxn11bobstep1"

    # assert wf_counter == 2
    # assert step_counter == 1
    # assert txn_counter == 1
