import uuid
from typing import Tuple

import sqlalchemy as sa
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dbos_transact.context import SetWorkflowUUID
from dbos_transact.dbos import DBOS


def test_simple_endpoint(dbos_fastapi: Tuple[DBOS, FastAPI]) -> None:
    dbos, app = dbos_fastapi
    client = TestClient(app)

    @app.get("/{var1}/{var2}")
    @dbos.workflow()
    def test_workflow(var1: str, var2: str) -> str:
        assert DBOS.request is not None
        res1 = test_transaction(var1)
        res2 = test_communicator(var2)
        return res1 + res2

    @dbos.transaction()
    def test_transaction(var: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return var + str(rows[0][0])

    @dbos.communicator()
    def test_communicator(var: str) -> str:
        return var

    response = client.get("/bob/bob")
    assert response.status_code == 200
    assert response.text == '"bob1bob"'


def test_endpoint_recovery(dbos_fastapi: Tuple[DBOS, FastAPI]) -> None:
    dbos, app = dbos_fastapi
    client = TestClient(app)

    @dbos.workflow()
    def test_workflow(var1: str) -> dict[str, str]:
        assert DBOS.request is not None
        return var1, DBOS.workflow_id

    @app.get("/{var1}/{var2}")
    def test_endpoint(var1: str, var2: str) -> dict[str, str]:
        res1, id1 = test_workflow(var1)
        res2, id2 = test_workflow(var2)
        return {"res1": res1, "res2": res2, "id1": id1, "id2": id2}

    wfuuid = str(uuid.uuid4())
    response = client.get("/a/b", headers={"dbos-idempotency-key": wfuuid})
    assert response.status_code == 200
    assert response.json().get("res1") == "a"
    assert response.json().get("res2") == "b"
    assert response.json().get("id1") == wfuuid
    assert response.json().get("id2") != wfuuid

    # Change the workflow status to pending
    dbos.sys_db.update_workflow_status(
        {
            "workflow_uuid": wfuuid,
            "status": "PENDING",
            "name": test_workflow.__qualname__,
            "output": None,
            "error": None,
            "executor_id": None,
            "app_id": None,
            "app_version": None,
            "request": None,
        }
    )

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = dbos.recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == ("a", wfuuid)