import asyncio
import importlib.metadata
import os
import sys
import threading
import uuid
from types import TracebackType
from typing import Optional, Set, Tuple, Type

import psycopg
from sqlalchemy.exc import DBAPIError, ResourceClosedError

INTERNAL_QUEUE_NAME = "_dbos_internal_queue"

request_id_header = "x-request-id"


class LoopAwareEvent(threading.Event):
    """A ``threading.Event`` that async callers can also await.

    DBOS signals these events from background threads: the Postgres LISTEN
    listener, the polling listener, or a fallback database re-check. A
    coroutine cannot wait on a plain ``threading.Event`` without either
    holding a thread-pool thread for the whole wait or sub-polling
    ``is_set()``. So each async waiter registers a future on its own event
    loop and ``set()`` wakes every registered waiter via
    ``call_soon_threadsafe``. Sync waiters use the inherited ``wait()``,
    unchanged.

    A single event may be awaited concurrently by any number of waiters,
    across any mix of event loops and threads.
    """

    def __init__(self) -> None:
        super().__init__()
        self._waiters: Set[Tuple[asyncio.AbstractEventLoop, "asyncio.Future[None]"]] = (
            set()
        )
        self._waiters_lock = threading.Lock()

    @staticmethod
    def _resolve(future: "asyncio.Future[None]") -> None:
        """Wake one async waiter. Runs on that waiter's event loop."""
        if not future.done():
            future.set_result(None)

    def set(self) -> None:
        # Set the flag before taking the lock so no wakeup is lost: a concurrent
        # wait_async either sees the flag under the lock and skips registering,
        # or is already in _waiters when the snapshot below drains it.
        super().set()
        with self._waiters_lock:
            waiters, self._waiters = self._waiters, set()
        for loop, waiter_future in waiters:
            try:
                loop.call_soon_threadsafe(self._resolve, waiter_future)
            except RuntimeError:
                pass  # The waiter's loop is closed; it has nothing left to wake.

    async def wait_async(self, timeout: float) -> bool:
        """Await the event, yielding to the loop. Returns whether it is set.

        May return False on a timeout, or spuriously if the event was cleared
        between the wakeup and this check, so callers must re-check their own
        condition rather than treat a return as delivery.
        """
        loop = asyncio.get_running_loop()
        with self._waiters_lock:
            if self.is_set():
                return True
            future: "asyncio.Future[None]" = loop.create_future()
            waiter = (loop, future)
            self._waiters.add(waiter)
        # Time out by resolving the waiter's own future rather than via asyncio.wait_for, which before Python 3.12 returns normally instead of re-raising when a cancellation lands after the future resolved, silently dropping it.
        timer = loop.call_later(max(timeout, 0), self._resolve, future)
        try:
            await future
        finally:
            timer.cancel()
            with self._waiters_lock:
                self._waiters.discard(waiter)
        return self.is_set()


class PollingLimiter:
    """A counting-semaphore-based limiter that caps how many DB-backed polling
    reads (from wait operations such as ``get_result``, ``recv``, ``get_event``,
    and ``read_stream``) may run concurrently against the system database pool.
    This keeps high-fan-out polling from checking out every pool connection and
    starving control-plane operations such as enqueue/dequeue, status writes,
    recovery, and cancellation.

    Use it as a context manager around the polling read::

        with self.poll_limiter:
            ...  # checkout a connection and run the poll query

    A non-positive limit disables the limiter: ``__enter__``/``__exit__`` become
    no-ops, so it adds no overhead when it is turned off. The same instance is
    shared across threads; the underlying ``threading.BoundedSemaphore`` is
    thread-safe and hands permits to waiters in (roughly) FIFO order.
    """

    def __init__(self, limit: int):
        self.limit = limit
        self.enabled = limit > 0
        self._semaphore = threading.BoundedSemaphore(limit) if self.enabled else None

    def __enter__(self) -> "PollingLimiter":
        if self._semaphore is not None:
            self._semaphore.acquire()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if self._semaphore is not None:
            self._semaphore.release()


class GlobalParams:
    app_version: str = os.environ.get("DBOS__APPVERSION", "")
    executor_id: str = os.environ.get("DBOS__VMID", "local")
    dbos_cloud: bool = os.environ.get("DBOS__CLOUD") == "true"
    try:
        # Only works on Python >= 3.8
        dbos_version = importlib.metadata.version("dbos")
    except importlib.metadata.PackageNotFoundError:
        # If package is not installed or during development
        dbos_version = "unknown"


def retriable_postgres_exception(e: Exception) -> bool:
    if not isinstance(e, DBAPIError):
        return False
    if e.connection_invalidated:
        return True
    if isinstance(e.orig, psycopg.OperationalError):
        driver_error: psycopg.OperationalError = e.orig
        pgcode = driver_error.sqlstate or ""
        # Failure to establish connection
        if "connection failed" in str(driver_error):
            return True
        # Error within database transaction
        elif "server closed the connection unexpectedly" in str(driver_error):
            return True
        # Connection timeout
        if isinstance(driver_error, psycopg.errors.ConnectionTimeout):
            return True
        # Insufficient resources
        elif pgcode.startswith("53"):
            return True
        # Connection exception
        elif pgcode.startswith("08"):
            return True
        # Operator intervention
        elif pgcode.startswith("57"):
            return True
        else:
            return False
    else:
        return False


def retriable_sqlite_exception(e: Exception) -> bool:
    if "database is locked" in str(e):
        return True
    # Under concurrent writes, pysqlite can intermittently invalidate the
    # cursor of an "INSERT ... RETURNING" statement before its row is fetched,
    # surfacing as a ResourceClosedError ("does not return rows"). The enclosing
    # transaction has rolled back and the write is idempotent (ON CONFLICT DO
    # UPDATE), so the operation is safe to retry.
    if isinstance(e, ResourceClosedError) and "does not return rows" in str(e):
        return True
    return False


def generate_uuid() -> str:
    if sys.version_info >= (3, 14):
        return str(uuid.uuid7())
    else:
        return str(uuid.uuid4())
