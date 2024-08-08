import sqlalchemy as sa
from fastapi import FastAPI

from dbos_transact import DBOS

dbos = DBOS()
app = FastAPI()


@dbos.workflow()
def example_workflow(var: str) -> str:
    return example_transaction(var)


@dbos.transaction()
def example_transaction(var: str) -> str:
    rows = DBOS.sql_session.execute(
        sa.text(
            "INSERT INTO dbos_hello (name, greet_count) VALUES ('dbos', 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;"
        )
    ).all()
    return var + str(rows[0][0])


@app.get("/greeting/{name}")
def hello_dbos(name: str):
    output = example_workflow(name)
    return {"name": output}
