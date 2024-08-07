import sqlalchemy as sa
from fastapi import FastAPI

from dbos_transact import DBOS, WorkflowContext
from dbos_transact.transaction import TransactionContext

dbos = DBOS()

app = FastAPI()


@dbos.workflow()
def example_workflow(ctx: WorkflowContext, var: str) -> str:
    return example_transaction(ctx.txn_ctx(), var)


@dbos.transaction()
def example_transaction(ctx: TransactionContext, var: str) -> str:
    rows = ctx.session.execute(
        sa.text(
            "INSERT INTO dbos_hello (name, greet_count) VALUES ('dbos', 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;"
        )
    ).all()
    return var + str(rows[0][0])


@app.get("/greeting/{name}")
def hello_dbos(name: str) -> dict[str, str]:
    output = example_workflow(dbos.wf_ctx(), name)
    return {"name": output}
