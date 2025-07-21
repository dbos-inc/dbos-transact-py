import logging
import uuid
from typing import Tuple

import pytest
import sqlalchemy as sa
from flask import Flask, Response, jsonify

from dbos import DBOS
from dbos._context import assert_current_dbos_context


def test_flask_endpoint(
    dbos_flask: Tuple[DBOS, Flask], caplog: pytest.LogCaptureFixture
) -> None:
    _, app = dbos_flask

    @app.route("/endpoint/<var1>/<var2>")
    def test_endpoint(var1: str, var2: str) -> Response:
        ctx = assert_current_dbos_context()
        assert not ctx.is_within_workflow()
        return test_workflow(var1, var2)

    @app.route("/workflow/<var1>/<var2>")
    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> Response:
        res1 = test_transaction(var1)
        res2 = test_step(var2)
        result = res1 + res2
        return jsonify({"result": result})

    @app.route("/transaction/<var>")
    @DBOS.transaction()
    def test_transaction(var: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return var + str(rows[0][0])

    @DBOS.step()
    def test_step(var: str) -> str:
        return var

    app.config["TESTING"] = True
    client = app.test_client()

    original_propagate = logging.getLogger("dbos").propagate
    caplog.set_level(logging.WARNING, "dbos")
    logging.getLogger("dbos").propagate = True

    response = client.get("/endpoint/a/b")
    assert response.status_code == 200
    assert response.json == {"result": "a1b"}
    assert caplog.text == ""

    response = client.get("/workflow/a/b")
    assert response.status_code == 200
    assert response.json == {"result": "a1b"}
    assert caplog.text == ""

    response = client.get("/transaction/bob")
    assert response.status_code == 200
    assert response.text == "bob1"
    assert caplog.text == ""

    # Reset logging
    logging.getLogger("dbos").propagate = original_propagate


def test_endpoint_recovery(dbos_flask: Tuple[DBOS, Flask]) -> None:
    dbos, app = dbos_flask

    wfuuid = str(uuid.uuid4())

    @DBOS.workflow()
    def test_workflow(var1: str) -> tuple[str, str]:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return var1, workflow_id

    @app.route("/<var1>/<var2>")
    def test_endpoint(var1: str, var2: str) -> dict[str, str]:
        res1, id1 = test_workflow(var1)
        res2, id2 = test_workflow(var2)
        return {"res1": res1, "res2": res2, "id1": id1, "id2": id2}

    app.config["TESTING"] = True
    client = app.test_client()

    response = client.get("/a/b", headers={"dbos-idempotency-key": wfuuid})
    assert response.status_code == 200
    assert response.json is not None
    assert response.json.get("res1") == "a"
    assert response.json.get("res2") == "b"
    assert response.json.get("id1") == wfuuid
    assert response.json.get("id2") != wfuuid

    # Change the workflow status to pending
    dbos._sys_db.update_workflow_outcome(wfuuid, "PENDING")

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = DBOS._recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == ("a", wfuuid)
