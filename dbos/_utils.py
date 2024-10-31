import asyncio
from typing import Any, Coroutine, TypeVar

from anyio.from_thread import start_blocking_portal

R = TypeVar("R", covariant=True)  # A generic type for workflow return values


def run_coroutine(coro: Coroutine[Any, Any, R]) -> R:

    loop = asyncio.events._get_running_loop()
    if loop is None:
        return asyncio.run(coro)

    async def await_coro() -> R:
        return await coro

    with start_blocking_portal() as portal:
        return portal.call(await_coro)
