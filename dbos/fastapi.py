import uuid
from typing import Any, Callable

from fastapi import FastAPI
from fastapi import Request as FastAPIRequest

from .context import (
    EnterDBOSHandler,
    OperationType,
    SetWorkflowID,
    TracedAttributes,
    assert_current_dbos_context,
)
from .request import Address, Request

request_id_header = "x-request-id"


def get_or_generate_request_id(request: FastAPIRequest) -> str:
    request_id = request.headers.get(request_id_header, None)
    if request_id is not None:
        return request_id
    else:
        return str(uuid.uuid4())


def make_request(request: FastAPIRequest) -> Request:
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


def setup_fastapi_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def dbos_fastapi_middleware(
        request: FastAPIRequest, call_next: Callable[..., Any]
    ) -> Any:
        attributes: TracedAttributes = {
            "name": str(request.url.path),
            "requestID": get_or_generate_request_id(request),
            "requestIP": request.client.host if request.client is not None else None,
            "requestURL": str(request.url),
            "requestMethod": request.method,
            "operationType": OperationType.HANDLER.value,
        }
        with EnterDBOSHandler(attributes):
            ctx = assert_current_dbos_context()
            ctx.request = make_request(request)
            workflow_id = request.headers.get("dbos-idempotency-key", "")
            with SetWorkflowID(workflow_id):
                response = await call_next(request)
        return response
