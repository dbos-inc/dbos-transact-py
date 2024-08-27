import sqlalchemy as sa
from fastapi import FastAPI

from dbos import DBOS

app = FastAPI()
dbos = DBOS(app)


@app.get("/greeting/{name}")
@dbos.workflow()
def example_workflow(name: str) -> dict[str, str]:
    DBOS.logger.info("Running workflow!")
    output = example_transaction(name)
    return {"name": output}


@dbos.transaction()
def example_transaction(name: str) -> str:
    rows = DBOS.sql_session.execute(
        sa.text(
            "INSERT INTO dbos_hello (name, greet_count) VALUES ('dbos', 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;"
        )
    ).all()
    return name + str(rows[0][0])
