# Welcome to DBOS!

# This is a sample app built with DBOS and FastAPI.
# It displays greetings to visitors and keeps track of how
# many times visitors have been greeted.

# First, let's do imports, create a FastAPI app, and initialize DBOS.

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from dbos import DBOS

from .schema import dbos_hello

app = FastAPI()
DBOS(fastapi=app)

# Next, let's write a function that greets visitors.
# To make it more interesting, we'll keep track of how
# many times visitors have been greeted and store
# the count in the database.

# We annotate this function with @DBOS.transaction to
# access to an automatically-configured database client,
# (DBOS.sql_sesion) then implement the database operations
# using SQLAlchemy. We serve this function from a FastAPI endpoint.


@app.get("/greeting/{name}")
@DBOS.transaction()
def example_transaction(name: str) -> str:
    query = dbos_hello.insert().values(name=name).returning(dbos_hello.c.greet_count)
    greet_count = DBOS.sql_session.execute(query).scalar_one()
    greeting = f"Greetings, {name}! You have been greeted {greet_count} times."
    DBOS.logger.info(greeting)
    return greeting


# Finally, let's use FastAPI to serve a simple HTML readme
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


# To run this app locally:
# - Make sure you have a Postgres database to connect
# - "dbos migrate" to set up your database tables
# - "dbos start" to start the app
# - Visit localhost:8000 to see your app!

# To deploy this app to DBOS Cloud:
# - "npm i -g @dbos-inc/dbos-cloud@latest" to install the Cloud CLI (requires Node)
# - "dbos-cloud app deploy" to deploy your app
# - Deploy outputs a URL--visit it to see your app!
