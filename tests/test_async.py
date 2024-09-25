import uuid

import pytest

# Public API
from dbos import DBOS, SetWorkflowID

# Private API because this is a test


@pytest.mark.asyncio
async def test_async_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    async def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        # res = test_transaction(var2)
        res = await test_step(var)
        DBOS.logger.info("I'm test_workflow")
        return res + var2

    @DBOS.step()
    async def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var + f"step{step_counter}"

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "alicestep1bob"
    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "alicestep1bob"

    assert wf_counter == 2
    assert step_counter == 1
