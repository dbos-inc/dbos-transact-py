import uuid
from typing import Any
from urllib.parse import urlparse

from flask import Flask
from werkzeug.wrappers import Request as WRequest

from ._context import EnterDBOSHandler, OperationType, SetWorkflowID, TracedAttributes
from ._utils import request_id_header


class FlaskMiddleware:
    def __init__(self, app: Any) -> None:
        self.app = app

    def __call__(self, environ: Any, start_response: Any) -> Any:
        request = WRequest(environ)
        attributes: TracedAttributes = {
            "name": urlparse(request.url).path,
            "requestID": _get_or_generate_request_id(request),
            "requestIP": (
                request.remote_addr if request.remote_addr is not None else None
            ),
            "requestURL": request.url,
            "requestMethod": request.method,
            "operationType": OperationType.HANDLER.value,
        }
        with EnterDBOSHandler(attributes):
            workflow_id = request.headers.get("dbos-idempotency-key")
            if workflow_id is not None:
                # Set the workflow ID for the handler
                with SetWorkflowID(workflow_id):
                    response = self.app(environ, start_response)
            else:
                response = self.app(environ, start_response)
        return response


def _get_or_generate_request_id(request: WRequest) -> str:
    request_id = request.headers.get(request_id_header, None)
    if request_id is not None:
        return request_id
    else:
        return str(uuid.uuid4())


def setup_flask_middleware(app: Flask) -> None:
    app.wsgi_app = FlaskMiddleware(app.wsgi_app)  # type: ignore
