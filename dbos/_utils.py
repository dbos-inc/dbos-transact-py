import importlib.metadata
import os
import sys
import time
import uuid
from typing import Optional

import psycopg
from sqlalchemy.exc import DBAPIError

from dbos._error import DBOSException

INTERNAL_QUEUE_NAME = "_dbos_internal_queue"

request_id_header = "x-request-id"


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
    else:
        return False


def _resolve_delay_epoch_ms(
    delay_seconds: Optional[float] = None,
    delay_until_epoch_ms: Optional[int] = None,
) -> int:
    """Resolve delay parameters to an absolute epoch millisecond timestamp."""
    if delay_until_epoch_ms is not None and delay_seconds is not None:
        raise DBOSException(
            "Specify either delay_seconds or delay_until_epoch_ms, not both"
        )
    if delay_until_epoch_ms is not None:
        return delay_until_epoch_ms
    if delay_seconds is not None:
        return int((time.time() + delay_seconds) * 1000)
    raise DBOSException("Must specify either delay_seconds or delay_until_epoch_ms")


def generate_uuid() -> str:
    if sys.version_info >= (3, 14):
        return str(uuid.uuid7())
    else:
        return str(uuid.uuid4())
