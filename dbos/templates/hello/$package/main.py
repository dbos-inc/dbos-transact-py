import sqlalchemy as sa
from fastapi import FastAPI
from sqlalchemy.dialects.postgresql import insert

from dbos import DBOS

from .schema import dbos_hello

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
    query = (
        insert(dbos_hello)
        .values(name="dbos", greet_count=1)
        .on_conflict_do_update(
            index_elements=["name"], set_={"greet_count": dbos_hello.c.greet_count + 1}
        )
        .returning(dbos_hello.c.greet_count)
    )

    greet_count = DBOS.sql_session.execute(query).scalar_one()
    DBOS.logger.info(f"{name} greet_count: {greet_count}")
    return name + str(greet_count)
