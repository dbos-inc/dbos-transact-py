import sqlalchemy as sa

from dbos_transact import DBOS, WorkflowContext
from dbos_transact.communicator import CommunicatorContext
from dbos_transact.transaction import TransactionContext

dbos = DBOS()


@dbos.workflow()
def example_workflow(ctx: WorkflowContext, var: str) -> str:
    res1 = example_transaction(ctx.txn_ctx(), var)
    res2 = example_communicator(ctx.comm_ctx(), var)
    return res1 + res2


@dbos.transaction()
def example_transaction(ctx: TransactionContext, var: str) -> str:
    rows = ctx.session.execute(sa.text("SELECT 1")).fetchall()
    return var + str(rows[0][0])


@dbos.communicator()
def example_communicator(ctx: CommunicatorContext, var: str) -> str:
    return var + "2"


if __name__ == "__main__":
    assert example_workflow(dbos.wf_ctx(), "mike") == "mike1mike2"
