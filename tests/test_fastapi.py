import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from typing import Any, Tuple

import httpx
import pytest
import sqlalchemy as sa
import uvicorn
from fastapi import FastAPI
from fastapi.testclient import TestClient

# Public API
from dbos import DBOS, DBOSConfig, Queue

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

    workflow_id = str(uuid.uuid4())

    @DBOS.workflow()
    def test_workflow(var1: str) -> tuple[str, str]:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return var1, workflow_id

    @app.get("/{var1}/{var2}")
    def test_endpoint(var1: str, var2: str) -> dict[str, str]:
        res1, id1 = test_workflow(var1)
        res2, id2 = test_workflow(var2)
        return {"res1": res1, "res2": res2, "id1": id1, "id2": id2}

    response = client.get("/a/b", headers={"dbos-idempotency-key": workflow_id})
    assert response.status_code == 200
    assert response.json().get("res1") == "a"
    assert response.json().get("res2") == "b"
    assert response.json().get("id1") == workflow_id
    assert response.json().get("id2") != workflow_id

    # Change the workflow status to pending
    dbos._sys_db.update_workflow_outcome(workflow_id, "PENDING")

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = DBOS._recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == ("a", workflow_id)


@pytest.mark.asyncio
async def test_custom_lifespan(
    config: DBOSConfig, cleanup_test_databases: None
) -> None:
    resource = None
    port = 8000

    @asynccontextmanager  # pyright: ignore
    async def lifespan(app: FastAPI) -> Any:
        nonlocal resource
        resource = 1
        yield
        resource = None

    app = FastAPI(lifespan=lifespan)

    DBOS.destroy()
    DBOS(fastapi=app, config=config)

    queue = Queue("queue")

    @app.get("/")
    @DBOS.workflow()
    async def resource_workflow() -> Any:
        handle = await queue.enqueue_async(queue_workflow)
        return {
            "resource": resource,
            "loop": id(asyncio.get_event_loop()),
            "queue_loop": await handle.get_result(),
        }

    @DBOS.workflow()
    async def queue_workflow() -> int:
        return id(asyncio.get_event_loop())

    uvicorn_config = uvicorn.Config(
        app=app, host="127.0.0.1", port=port, log_level="error"
    )
    server = uvicorn.Server(config=uvicorn_config)

    # Run server in background task
    server_task = asyncio.create_task(server.serve())
    await asyncio.sleep(0.2)  # Give server time to start

    async with httpx.AsyncClient() as client:
        r = await client.get(f"http://127.0.0.1:{port}")
        assert r.json()["resource"] == 1
        # Verify that both the FastAPI and enqueued workflows run in the main event loop
        assert r.json()["loop"] == id(asyncio.get_event_loop())
        assert r.json()["queue_loop"] == id(asyncio.get_event_loop())

    server.should_exit = True
    await server_task
    assert resource is None


def test_stacked_decorators_wf(dbos_fastapi: Tuple[DBOS, FastAPI]) -> None:
    dbos, app = dbos_fastapi
    client = TestClient(app)

    @app.get("/endpoint/{var1}/{var2}")
    @DBOS.workflow()
    async def test_endpoint(var1: str, var2: str) -> str:
        return f"{var1}, {var2}!"

    response = client.get("/endpoint/plums/deify")
    assert response.status_code == 200
    assert response.text == '"plums, deify!"'


def test_stacked_decorators_step(dbos_fastapi: Tuple[DBOS, FastAPI]) -> None:
    dbos, app = dbos_fastapi
    client = TestClient(app)

    @app.get("/endpoint/{var1}/{var2}")
    @DBOS.step()
    async def test_endpoint(var1: str, var2: str) -> str:
        return f"{var1}, {var2}!"

    response = client.get("/endpoint/plums/deify")
    assert response.status_code == 200
    assert response.text == '"plums, deify!"'
