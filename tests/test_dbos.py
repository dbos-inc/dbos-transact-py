import pytest
import sqlalchemy as sa

from dbos_transact.dbos import DBOS
from dbos_transact.transaction import TransactionContext
from dbos_transact.workflows import WorkflowContext


def test_simple_workflow(dbos: DBOS) -> None:

    @dbos.workflow()
    def test_workflow(ctx: WorkflowContext, var: str, var2: str) -> str:
        res = test_transaction(ctx.txn_ctx(), var2)
        return res + var

    @dbos.transaction()
    def test_transaction(ctx: TransactionContext, var2: str) -> str:
        rows = ctx.session.execute(sa.text("SELECT 1")).fetchall()
        return var2 + str(rows[0][0])

    assert test_workflow(dbos.wf_ctx(), "bob", "bob") == "bob1bob"


def test_exception_workflow(dbos: DBOS) -> None:

    @dbos.transaction()
    def exception_transaction(ctx: TransactionContext, var: str) -> str:
        raise Exception(var)

    @dbos.workflow()
    def exception_workflow(ctx: WorkflowContext) -> None:
        try:
            exception_transaction(ctx.txn_ctx(), "test error")
        except Exception as e:
            raise e

    with pytest.raises(Exception) as exc_info:
        exception_workflow(dbos.wf_ctx())

    assert "test error" in str(exc_info.value)
