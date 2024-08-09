from typing import Any, Callable

from fastapi import FastAPI, Request

from .context import DBOSContextEnsure, assert_current_dbos_context
from .logger import dbos_logger


def setup_fastapi_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def dbos_fastapi_middleware(
        request: Request, call_next: Callable[..., Any]
    ) -> Any:
        dbos_logger.info("dbos")
        with DBOSContextEnsure():
            ctx = assert_current_dbos_context()
            ctx.request = request
            response = await call_next(request)
        dbos_logger.info("also dbos")
        return response
