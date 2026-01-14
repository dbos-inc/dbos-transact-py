from typing import Awaitable, Callable, Tuple

import pytest
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.testclient import TestClient

# For tracing test
from opentelemetry.sdk import trace as tracesdk

# from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

# Public API
from dbos import DBOS, DBOSContextSetAuth

# Private API because this is a unit test
from dbos._context import assert_current_dbos_context
from dbos._error import DBOSInitializationError, DBOSNotAuthorizedError
from tests.conftest import TestOtelType


@pytest.mark.order(1)
def test_simple_endpoint(
    dbos_fastapi: Tuple[DBOS, FastAPI], setup_in_memory_otlp_collector: TestOtelType
) -> None:

    exporter, log_processor, log_exporter = setup_in_memory_otlp_collector
    dbos, app = dbos_fastapi

    @app.middleware("http")
    async def authMiddleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        with DBOSContextSetAuth("user1", ["user", "engineer"]):
            response = await call_next(request)
            return response

    @app.get("/dboserror")
    def test_dbos_error() -> None:
        raise DBOSNotAuthorizedError("test")

    @app.get("/dbosinternalerror")
    def test_dbos_error_internal() -> None:
        raise DBOSInitializationError("oh no")

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
    exporter.clear()

    response = client.get("/user/b")
    assert response.status_code == 200
    assert response.text == '"b"'

    # Test for roles set into span
    # Get the spans that were recorded
    spans = exporter.get_finished_spans()

    # Assert that we have two spans
    #  One is the handler span
    #  The other is the WF span
    assert len(spans) == 2
    assert spans[0].context is not None
    assert spans[1].context is not None

    span = spans[1]
    assert span.name == "/user/b"
    assert span.attributes is not None

    span = spans[0]
    assert span.name == test_user_endpoint.__qualname__
    assert span.parent is not None
    assert span.parent.span_id == spans[1].context.span_id
    assert span.attributes is not None
    assert span.attributes["authenticatedUser"] == "user1"
    assert span.attributes["authenticatedUserAssumedRole"] == "user"
    assert span.attributes["authenticatedUserRoles"] == '["user", "engineer"]'

    # Verify that there is one workflow for this user.
    wfl = DBOS.list_workflows(user="user1")
    assert len(wfl) == 1
    wfs = DBOS.get_workflow_status(wfl[0].workflow_id)
    assert wfs
    assert wfs.assumed_role == "user"
    assert wfs.authenticated_user == "user1"
    assert wfs.authenticated_roles == ["user", "engineer"]

    # Make sure predicate is actually applied
    wfl = DBOS.list_workflows(user="user2")
    assert len(wfl) == 0

    response = client.get("/error")
    assert response.status_code == 401

    response = client.get("/dboserror")
    assert response.status_code == 403

    response = client.get("/dbosinternalerror")
    assert response.status_code == 500

    response = client.get("/open/a")
    assert response.status_code == 200
    assert response.text == '"a"'

    response = client.get("/engineer/c")
    assert response.status_code == 200
    assert response.text == '"c"'

    response = client.get("/adminalt/d")
    assert response.status_code == 403

    response = client.get("/admin/d")
    assert response.status_code == 403
    assert (
        response.text
        == '{"message":"Function test_simple_endpoint.<locals>.test_admin_endpoint has required roles, but user is not authenticated for any of them","dbos_error_code":"8","dbos_error":"DBOSNotAuthorizedError"}'
    )


# This does not test DBOS at all
# (It's just a hard-earned example of how you can unit test your spans)
def test_role_tracing() -> None:
    # Set up a simple in-memory span exporter for testing
    exporter = InMemorySpanExporter()
    span_processor = SimpleSpanProcessor(exporter)
    provider = tracesdk.TracerProvider()
    provider.add_span_processor(span_processor)

    def function_to_trace() -> None:
        with provider.get_tracer(__name__).start_as_current_span(  # pyright: ignore
            "test-span"
        ) as span:
            span.set_attribute("testattribute", "value")

    # Clear any existing spans
    exporter.clear()

    # Run the function that generates the trace spans
    function_to_trace()

    # Get the spans that were recorded
    spans = exporter.get_finished_spans()

    # Assert that we have exactly one span
    assert len(spans) == 1

    # Inspect the span and its attributes
    span = spans[0]
    assert span.name == "test-span"
    assert span.attributes is not None
    assert span.attributes["testattribute"] == "value"
