from dbos_transact import DBOS, WorkflowContext
from dbos_transact.transaction import TransactionContext

dbos = DBOS()


@dbos.workflow()
def example_workflow(ctx: WorkflowContext, var: str) -> str:
    return example_transaction(ctx.txn_ctx(), var)


@dbos.transaction()
def example_transaction(ctx: TransactionContext, var: str) -> str:
    return var


if __name__ == "__main__":
    assert example_workflow(dbos.wf_ctx(), "mike") == "mike"
