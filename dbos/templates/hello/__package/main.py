# Welcome to DBOS!

# This is a sample app built with DBOS and FastAPI.
# It displays greetings to visitors and keeps track of how
# many times each visitor has been greeted.

# First, let's do imports, create a FastAPI app, and initialize DBOS.

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy.dialects.postgresql import insert

from dbos import DBOS

from .schema import dbos_hello

app = FastAPI()
DBOS(app)

# Next, let's use FastAPI to serve a simple HTML readme
# from the root path.


@app.get("/")
def readme() -> HTMLResponse:
    readme = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <title>Welcome to DBOS!</title>
            <script src="https://cdn.tailwindcss.com"></script>
        </head>
        <body class="font-sans text-gray-800 p-6 max-w-2xl mx-auto">
            <h1 class="text-xl font-semibold mb-4">Welcome to DBOS!</h1>
            <p class="mb-4">
                Visit the route <code class="bg-gray-100 px-1 rounded">/greeting/{name}</code> to be greeted!<br>
                For example, visit <code class="bg-gray-100 px-1 rounded"><a href="/greeting/dbos" class="text-blue-600 hover:underline">/greeting/dbos</a></code><br>
                The counter increments with each page visit.<br>
            </p>
            <p>
                To learn more about DBOS, check out the <a href="https://docs.dbos.dev" class="text-blue-600 hover:underline">docs</a>.
            </p>
        </body>
        </html>
        """
    return HTMLResponse(readme)


@DBOS.transaction()
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
