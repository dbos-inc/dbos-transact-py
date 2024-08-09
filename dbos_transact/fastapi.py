from typing import Any, Callable

from fastapi import FastAPI, Request

from .logger import dbos_logger


def setup_fastapi_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def dbos_fastapi_middleware(
        request: Request, call_next: Callable[..., Any]
    ) -> Any:
        dbos_logger.info("dbos")
        response = await call_next(request)
        dbos_logger.info("also dbos")
        return response
