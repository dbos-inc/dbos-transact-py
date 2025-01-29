import logging
import uuid
from typing import Tuple

import pytest
import sqlalchemy as sa
from fastapi import FastAPI
from fastapi.testclient import TestClient

# Public API
from dbos import DBOS

# Private API because this is a unit test
from dbos._context import assert_current_dbos_context


def test_simple_endpoint(
    dbos_fastapi: Tuple[DBOS, FastAPI], caplog: pytest.LogCaptureFixture
) -> None:
    dbos, app = dbos_fastapi
    client = TestClient(app)

    @app.get("/endpoint/{var1}/{var2}")
    def test_endpoint(var1: str, var2: str) -> str:
        result = test_workflow(var1, var2)
        ctx = assert_current_dbos_context()
        assert not ctx.is_within_workflow()
        return result

    @app.get("/workflow/{var1}/{var2}")
    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> str:
        DBOS.span.set_attribute("test_key", "test_value")
        assert DBOS.request is not None
        res1 = test_transaction(var1)
        res2 = test_step(var2)
        return res1 + res2

    @app.get("/transaction/{var}")
    @DBOS.transaction()
    def test_transaction(var: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return var + str(rows[0][0])

    @DBOS.step()
    def test_step(var: str) -> str:
        return var

    original_propagate = logging.getLogger("dbos").propagate
    caplog.set_level(logging.WARNING, "dbos")
    logging.getLogger("dbos").propagate = True

    response = client.get("/workflow/bob/bob")
    assert response.status_code == 200
    assert response.text == '"bob1bob"'
    assert caplog.text == ""

    response = client.get("/endpoint/bob/bob")
    assert response.status_code == 200
    assert response.text == '"bob1bob"'
    assert caplog.text == ""

    response = client.get("/transaction/bob")
    assert response.status_code == 200
    assert response.text == '"bob1"'
    assert caplog.text == ""

    # Reset logging
    logging.getLogger("dbos").propagate = original_propagate


def test_start_workflow(dbos_fastapi: Tuple[DBOS, FastAPI]) -> None:
    dbos, app = dbos_fastapi
    client = TestClient(app)

    @app.get("/{var1}/{var2}")
    def test_endpoint(var1: str, var2: str) -> str:
        handle = dbos.start_workflow(test_workflow, var1, var2)
        context = assert_current_dbos_context()
        assert not context.is_within_workflow()
        return handle.get_result()

    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> str:
        DBOS.span.set_attribute("test_key", "test_value")
        assert DBOS.request is not None
        res1 = test_transaction(var1)
        res2 = test_step(var2)
        return res1 + res2

    @DBOS.transaction()
    def test_transaction(var: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return var + str(rows[0][0])

    @DBOS.step()
    def test_step(var: str) -> str:
        return var

    response = client.get("/bob/bob")
    assert response.status_code == 200
    assert response.text == '"bob1bob"'


def test_endpoint_recovery(dbos_fastapi: Tuple[DBOS, FastAPI]) -> None:
    dbos, app = dbos_fastapi
    client = TestClient(app)

    wfuuid = str(uuid.uuid4())

    @DBOS.workflow()
    def test_workflow(var1: str) -> tuple[str, str]:
        assert DBOS.request is not None
        return var1, DBOS.workflow_id

    @app.get("/{var1}/{var2}")
    def test_endpoint(var1: str, var2: str) -> dict[str, str]:
        assert (
            DBOS.request is not None
            and DBOS.request.headers["dbos-idempotency-key"] == wfuuid
        )
        res1, id1 = test_workflow(var1)
        res2, id2 = test_workflow(var2)
        return {"res1": res1, "res2": res2, "id1": id1, "id2": id2}

    response = client.get("/a/b", headers={"dbos-idempotency-key": wfuuid})
    assert response.status_code == 200
    assert response.json().get("res1") == "a"
    assert response.json().get("res2") == "b"
    assert response.json().get("id1") == wfuuid
    assert response.json().get("id2") != wfuuid

    dbos._sys_db.wait_for_buffer_flush()
    # Change the workflow status to pending
    dbos._sys_db.update_workflow_status(
        {
            "workflow_uuid": wfuuid,
            "status": "PENDING",
            "name": test_workflow.__qualname__,
            "class_name": None,
            "config_name": None,
            "output": None,
            "error": None,
            "executor_id": None,
            "app_id": None,
            "app_version": None,
            "request": None,
            "recovery_attempts": None,
            "authenticated_user": None,
            "authenticated_roles": None,
            "assumed_role": None,
            "queue_name": None,
        }
    )

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = DBOS.recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == ("a", wfuuid)
