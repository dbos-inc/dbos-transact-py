import uuid

import pytest
import sqlalchemy as sa

from dbos_transact.dbos import DBOS
from dbos_transact.transaction import TransactionContext
from dbos_transact.workflows import WorkflowContext


def test_simple_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0

    @dbos.workflow()
    def test_workflow(ctx: WorkflowContext, var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(ctx.txn_ctx(), var2)
        return res + var

    @dbos.transaction()
    def test_transaction(ctx: TransactionContext, var2: str) -> str:
        rows = ctx.session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    assert test_workflow(dbos.wf_ctx(), "bob", "bob") == "bob1bob"

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    assert test_workflow(dbos.wf_ctx(wfuuid), "alice", "alice") == "alice1alice"
    assert test_workflow(dbos.wf_ctx(wfuuid), "alice", "alice") == "alice1alice"
    assert txn_counter == 2  # Only increment once

    # Test we can execute the workflow by uuid
    dbos.execute_workflow_uuid(wfuuid)
    assert wf_counter == 4


def test_exception_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0

    @dbos.transaction()
    def exception_transaction(ctx: TransactionContext, var: str) -> str:
        nonlocal txn_counter
        txn_counter += 1
        raise Exception(var)

    @dbos.workflow()
    def exception_workflow(ctx: WorkflowContext) -> None:
        nonlocal wf_counter
        wf_counter += 1
        try:
            exception_transaction(ctx.txn_ctx(), "test error")
        except Exception as e:
            raise e

    with pytest.raises(Exception) as exc_info:
        exception_workflow(dbos.wf_ctx())

    assert "test error" in str(exc_info.value)

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with pytest.raises(Exception) as exc_info:
        exception_workflow(dbos.wf_ctx(wfuuid))
    assert "test error" in str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        exception_workflow(dbos.wf_ctx(wfuuid))
    assert "test error" in str(exc_info.value)
    assert txn_counter == 2  # Only increment once

    # Test we can execute the workflow by uuid, shouldn't throw errors
    dbos.execute_workflow_uuid(wfuuid)
    assert wf_counter == 4
