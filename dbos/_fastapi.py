import uuid
from typing import Any, Callable, cast

from fastapi import FastAPI
from fastapi import Request as FastAPIRequest
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from . import DBOS
from ._context import (
    EnterDBOSHandler,
    OperationType,
    SetWorkflowID,
    TracedAttributes,
    assert_current_dbos_context,
)
from ._error import DBOSException
from ._request import Address, Request, request_id_header


def _get_or_generate_request_id(request: FastAPIRequest) -> str:
    request_id = request.headers.get(request_id_header, None)
    if request_id is not None:
        return request_id
    else:
        return str(uuid.uuid4())


def _make_request(request: FastAPIRequest) -> Request:
    return Request(
        headers=request.headers,
        path_params=request.path_params,
        query_params=request.query_params,
        url=str(request.url),
        base_url=str(request.base_url),
        client=Address(*request.client) if request.client is not None else None,
        cookies=request.cookies,
        method=request.method,
    )


async def _dbos_error_handler(request: FastAPIRequest, gexc: Exception) -> JSONResponse:
    exc: DBOSException = cast(DBOSException, gexc)
    status_code = 500
    if exc.status_code is not None:
        status_code = exc.status_code
    return JSONResponse(
        status_code=status_code,
        content={
            "message": str(exc.message),
            "dbos_error_code": str(exc.dbos_error_code),
            "dbos_error": str(exc.__class__.__name__),
        },
    )


class LifespanMiddleware:
    def __init__(self, app: ASGIApp, dbos: DBOS):
        self.app = app
        self.dbos = dbos

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    self.dbos._launch()
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    self.dbos._destroy()
                    await send({"type": "lifespan.shutdown.complete"})
                    break
        else:
            await self.app(scope, receive, send)


def setup_fastapi_middleware(app: FastAPI, dbos: DBOS) -> None:

    app.add_middleware(LifespanMiddleware, dbos=dbos)
    app.add_exception_handler(DBOSException, _dbos_error_handler)

    @app.middleware("http")
    async def dbos_fastapi_middleware(
        request: FastAPIRequest, call_next: Callable[..., Any]
    ) -> Any:
        attributes: TracedAttributes = {
            "name": str(request.url.path),
            "requestID": _get_or_generate_request_id(request),
            "requestIP": request.client.host if request.client is not None else None,
            "requestURL": str(request.url),
            "requestMethod": request.method,
            "operationType": OperationType.HANDLER.value,
        }
        with EnterDBOSHandler(attributes):
            ctx = assert_current_dbos_context()
            ctx.request = _make_request(request)
            workflow_id = request.headers.get("dbos-idempotency-key")
            if workflow_id is not None:
                # Set the workflow ID for the handler
                with SetWorkflowID(workflow_id):
                    response = await call_next(request)
            else:
                response = await call_next(request)
        return response
