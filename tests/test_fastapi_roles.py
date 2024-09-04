from typing import Awaitable, Callable, Tuple

import pytest
import sqlalchemy as sa
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware

# Public API
from dbos import DBOS

# Private API because this is a unit test
from dbos.context import DBOSContextEnsure, assert_current_dbos_context
from dbos.error import DBOSException, DBOSNotAuthorizedError


def test_simple_endpoint(dbos_fastapi: Tuple[DBOS, FastAPI]) -> None:
    dbos, app = dbos_fastapi

    class SetRoleMiddleware(BaseHTTPMiddleware):
        async def dispatch(
            self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
        ) -> Response:
            with DBOSContextEnsure() as ctx:
                ctx.authenticated_user = "user1"
                ctx.authenticated_roles = ["user", "engineer"]
                try:
                    response = await call_next(request)
                    return response
                finally:
                    ctx.authenticated_user = None
                    ctx.authenticated_roles = None

    app.add_middleware(SetRoleMiddleware)

    @app.exception_handler(DBOSNotAuthorizedError)
    async def role_error_handler(
        request: Request, exc: DBOSNotAuthorizedError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={"detail": str(exc)},
        )

    @app.get("/dboserror")
    def test_dbos_error() -> None:
        raise DBOSNotAuthorizedError("test")

    @app.get("/open/{var1}")
    @DBOS.required_roles([])
    @DBOS.workflow()
    def test_open_endpoint(var1: str) -> str:
        result = var1
        ctx = assert_current_dbos_context()
        return result

    @app.get("/user/{var1}")
    @DBOS.required_roles(["user"])
    @DBOS.workflow()
    def test_user_endpoint(var1: str) -> str:
        result = var1
        ctx = assert_current_dbos_context()
        assert ctx.assumed_role == "user"
        return result

    @app.get("/engineer/{var1}")
    @DBOS.required_roles(["engineer"])
    @DBOS.workflow()
    def test_engineer_endpoint(var1: str) -> str:
        result = var1
        ctx = assert_current_dbos_context()
        assert ctx.assumed_role == "engineer"
        return result

    @app.get("/admin/{var1}")
    @DBOS.required_roles(["admin"])
    @DBOS.workflow()
    def test_admin_endpoint(var1: str) -> str:
        result = var1
        ctx = assert_current_dbos_context()
        assert ctx.assumed_role == "admin"
        return result

    @DBOS.required_roles(["admin"])
    @DBOS.workflow()
    def test_admin_endpoint_nh(var1: str) -> str:
        result = var1
        ctx = assert_current_dbos_context()
        assert ctx.assumed_role == "admin"
        return result

    @app.get("/adminalt/{var1}")
    def test_admin_handler(var1: str) -> str:
        return test_admin_endpoint_nh(var1)

    @app.get("/error")
    def test_error() -> None:
        raise HTTPException(status_code=401)

    client = TestClient(app)

    response = client.get("/error")
    assert response.status_code == 401

    response = client.get("/dboserror")
    assert response.status_code == 400

    response = client.get("/open/a")
    assert response.status_code == 200
    assert response.text == '"a"'

    response = client.get("/user/b")
    assert response.status_code == 200
    assert response.text == '"b"'

    response = client.get("/engineer/c")
    assert response.status_code == 200
    assert response.text == '"c"'

    # response = client.get("/adminalt/d")
    # assert response.status_code == 400

    with pytest.raises(Exception) as exc_info:
        response = client.get("/admin/d")
    assert exc_info.value.__class__.__qualname__ == DBOSNotAuthorizedError.__qualname__
    assert exc_info.value.__class__.__module__ == DBOSNotAuthorizedError.__module__
    assert id(exc_info.value.__class__) == id(DBOSNotAuthorizedError)

    assert isinstance(exc_info.value, DBOSNotAuthorizedError)
    assert exc_info.errisinstance(DBOSNotAuthorizedError)
    assert str(exc_info.value) == "well no"
