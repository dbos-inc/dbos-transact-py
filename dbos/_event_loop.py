import asyncio
import threading
from typing import Any, Coroutine, Optional, TypeVar


class BackgroundEventLoop:
    """
    This is the event loop to which DBOS submits any coroutines that are not started from within an event loop.
    In particular, coroutines submitted to queues (such as from scheduled workflows) run on this event loop.

    If a main event loop is known (whether because an event loop existed in the thread that called DBOS.launch
    or because a FastAPI event loop was detected) then coroutines are submitted there instead.
    """

    def __init__(self) -> None:
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._ready = threading.Event()

    def start(self) -> None:
        if self._running:
            return

        self.set_main_loop()
        self._thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self._thread.start()
        self._ready.wait()  # Wait until the loop is running

    def stop(self) -> None:
        if not self._running or self._loop is None or self._thread is None:
            return

        asyncio.run_coroutine_threadsafe(self._shutdown(), self._loop)
        self._thread.join()
        self._running = False

    def _run_event_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        self._running = True
        self._ready.set()  # Signal that the loop is ready

        try:
            self._loop.run_forever()
        finally:
            self._loop.close()

    async def _shutdown(self) -> None:
        if self._loop is None:
            raise RuntimeError("Event loop not started")
        tasks = [
            task
            for task in asyncio.all_tasks(self._loop)
            if task is not asyncio.current_task(self._loop)
        ]

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        self._loop.stop()

    def set_main_loop(self) -> None:
        """
        Set the main loop to the currently running event loop.
        Should be called from the main thread.
        """
        try:
            self._main_loop = asyncio.get_running_loop()
        except:
            # There's no running event loop to set
            pass

    T = TypeVar("T")

    def submit_coroutine(self, coro: Coroutine[Any, Any, T]) -> T:
        """Submit a coroutine to the background event loop"""
        if self._main_loop is not None and self._main_loop.is_running():
            return asyncio.run_coroutine_threadsafe(coro, self._main_loop).result()
        if self._loop is None:
            raise RuntimeError("Event loop not started")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()
