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
        # res2 = test_step(var)
        DBOS.logger.info("I'm test_workflow")
        return var + var2

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "alicebob"
    with SetWorkflowID(wfuuid):
        result = await test_workflow("alice", "bob")
        assert result == "alicebob"

    assert wf_counter == 2
