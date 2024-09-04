from typing import Awaitable, Callable, Tuple

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware

# Public API
from dbos import DBOS, DBOSContextEnsure

# Private API because this is a unit test
from dbos.context import assert_current_dbos_context
from dbos.error import DBOSDuplicateWorkflowEventError, DBOSNotAuthorizedError


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

    # app.add_middleware(SetRoleMiddleware)

    @app.middleware("http")
    async def authMiddleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
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

    @app.get("/dboserror")
    def test_dbos_error() -> None:
        raise DBOSNotAuthorizedError("test")

    @app.get("/dbosinternalerror")
    def test_dbos_error_internal() -> None:
        raise DBOSDuplicateWorkflowEventError("nosuchwf", "test")

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
    assert response.status_code == 403

    response = client.get("/dbosinternalerror")
    assert response.status_code == 500

    response = client.get("/open/a")
    assert response.status_code == 200
    assert response.text == '"a"'

    response = client.get("/user/b")
    assert response.status_code == 200
    assert response.text == '"b"'

    response = client.get("/engineer/c")
    assert response.status_code == 200
    assert response.text == '"c"'

    response = client.get("/adminalt/d")
    assert response.status_code == 403

    response = client.get("/admin/d")
    assert response.status_code == 403
    assert (
        response.text
        == '{"message":"Function test_admin_endpoint has required roles, but user is not authenticated for any of them","dbos_error_code":"8","dbos_error":"DBOSNotAuthorizedError"}'
    )
