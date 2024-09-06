import sqlalchemy as sa
from flask import Flask, Response, jsonify

# Public API
from dbos import DBOS


def test_flask_endpoint(dbos: DBOS) -> None:
    app = Flask("test_flask_endpoint")

    @app.route("/endpoint/<var1>/<var2>")
    def hello(var1: str, var2: str) -> Response:
        return test_workflow(var1, var2)

    @app.route("/workflow/<var1>/<var2>")
    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> Response:
        res1 = test_transaction(var1)
        res2 = test_step(var2)
        result = res1 + res2
        return jsonify({"result": result})

    @DBOS.transaction()
    def test_transaction(var: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return var + str(rows[0][0])

    @DBOS.step()
    def test_step(var: str) -> str:
        return var

    app.config["TESTING"] = True
    client = app.test_client()

    response = client.get("/endpoint/a/b")
    assert response.status_code == 200
    assert response.json == {"result": "a1b"}

    response = client.get("/workflow/a/b")
    assert response.status_code == 200
    assert response.json == {"result": "a1b"}
