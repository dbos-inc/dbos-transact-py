"""Driver-neutral PostgreSQL error classification (SQLSTATE-based).

Lets DBOS's retry/serialization logic work with any PostgreSQL DBAPI driver
(psycopg, pg8000, ...) without importing psycopg. pg8000 surfaces the Postgres
error code in ``args[0]["C"]``; psycopg exposes ``.sqlstate`` — handle both.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy.exc import DBAPIError, OperationalError


def get_sqlstate(orig: Optional[BaseException]) -> Optional[str]:
    if orig is None:
        return None
    sqlstate = getattr(orig, "sqlstate", None)
    if sqlstate:
        return str(sqlstate)
    args = getattr(orig, "args", ())
    if args and isinstance(args[0], dict):
        code = args[0].get("C")
        if code:
            return str(code)
    return None


def is_serialization_error(e: DBAPIError) -> bool:
    return get_sqlstate(getattr(e, "orig", None)) in ("40001", "40P01")


def is_serialization_or_lock_error(e: Exception) -> bool:
    if not isinstance(e, (DBAPIError, OperationalError)):
        return False
    return get_sqlstate(getattr(e, "orig", None)) in ("40001", "40P01", "55P03")


def retriable_postgres_exception(e: Exception) -> bool:
    if not isinstance(e, DBAPIError):
        return False
    if e.connection_invalidated:
        return True
    orig = e.orig
    if orig is None:
        return False
    code = get_sqlstate(orig)
    if code:
        # Classify by SQLSTATE class: 08 connection, 53 resources, 57 intervention.
        # Don't fall through to text checks or deterministic errors retry forever.
        return code.startswith(("08", "53", "57"))
    # No SQLSTATE => a client-side connection failure; match by message, but only for
    # OperationalError so deterministic errors can't match by text.
    if isinstance(e, OperationalError):
        msg = str(orig).lower()
        if (
            "connection failed" in msg
            or "server closed the connection unexpectedly" in msg
        ):
            return True
        if "timeout" in msg and "connection" in msg:
            return True
    return False
