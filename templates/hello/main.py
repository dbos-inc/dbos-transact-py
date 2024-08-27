import sqlalchemy as sa
from fastapi import FastAPI

from dbos import DBOS, DBOSImpl

app = FastAPI()
dbos = DBOSImpl(app)

# Question is about imports and starting.
# Making them first, it's already serving now, and soon to learn about the code below
# Otherwise, we could do the import of this code first (but what about '@app'?)
# And then start in one place

# Or, we go serverless.  Just put this stuff here, and some other code was
#   responsible for doing the before/after stuff, leaving only this code here.


@app.get("/greeting/{name}")
@DBOS.workflow()
def example_workflow(name: str) -> dict[str, str]:
    DBOS.logger.info("Running workflow!")
    output = example_transaction(name)
    return {"name": output}


@DBOS.transaction()
def example_transaction(name: str) -> str:
    rows = DBOS.sql_session.execute(
        sa.text(
            "INSERT INTO dbos_hello (name, greet_count) VALUES ('dbos', 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;"
        )
    ).all()
    return name + str(rows[0][0])
