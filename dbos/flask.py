import uuid
from urllib.parse import urlparse

from flask import Flask, request
from werkzeug.wrappers import Request as WRequest

from dbos.context import (
    EnterDBOSHandler,
    OperationType,
    SetWorkflowID,
    TracedAttributes,
    assert_current_dbos_context,
)

from .request import Address, Request, request_id_header


class FlaskMiddleware:
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        request = WRequest(environ)
        attributes: TracedAttributes = {
            "name": request.url,
            "requestID": get_or_generate_request_id(request),
            "requestIP": (
                request.remote_addr if request.remote_addr is not None else None
            ),
            "requestURL": request.url,
            "requestMethod": request.method,
            "operationType": OperationType.HANDLER.value,
        }
        with EnterDBOSHandler(attributes):
            ctx = assert_current_dbos_context()
            ctx.request = make_request(request)
            workflow_id = request.headers.get("dbos-idempotency-key", "")
            with SetWorkflowID(workflow_id):
                response = self.app(environ, start_response)
        return response


def get_or_generate_request_id(request: WRequest) -> str:
    request_id = request.headers.get(request_id_header, None)
    if request_id is not None:
        return request_id
    else:
        return str(uuid.uuid4())


def make_request(request: WRequest) -> Request:
    parsed_url = urlparse(request.url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    # Extract client information
    client = None
    if request.remote_addr:
        hostname = request.remote_addr
        port = request.environ.get("REMOTE_PORT")
        if port:
            client = Address(hostname=hostname, port=int(port))
        else:
            # If port is not available, we'll use 0 as a placeholder
            client = Address(hostname=hostname, port=0)

    return Request(
        headers=dict(request.headers),
        path_params={},
        query_params=dict(request.args),
        url=request.url,
        base_url=base_url,
        client=client,
        cookies=dict(request.cookies),
        method=request.method,
    )


def setup_flask_middleware(app: Flask):
    app.wsgi_app = FlaskMiddleware(app.wsgi_app)
