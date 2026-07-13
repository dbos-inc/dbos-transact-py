import importlib.metadata
import os
import sys
import threading
import uuid
from types import TracebackType
from typing import Optional, Type

try:
    import psycopg
except ImportError:  # optional: only the psycopg driver and LISTEN/NOTIFY use it
    psycopg = None  # type: ignore[assignment]
from sqlalchemy.exc import DBAPIError, ResourceClosedError

from dbos._pg_errors import retriable_postgres_exception as _pg_retriable

INTERNAL_QUEUE_NAME = "_dbos_internal_queue"

request_id_header = "x-request-id"


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
    # Driver-neutral (SQLSTATE-based) classification so retry logic works with
    # any PostgreSQL DBAPI driver (psycopg, pg8000, ...).
    return _pg_retriable(e)


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
