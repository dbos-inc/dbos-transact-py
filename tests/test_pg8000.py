"""Driver-neutral PostgreSQL error handling, exercised with the pg8000 driver.

DBOS defaults to psycopg, but its error-classification layer (``dbos._pg_errors``)
must work with any PostgreSQL DBAPI driver. pg8000 is the interesting case:

* its error objects have no ``.sqlstate`` attribute — the SQLSTATE lives in
  ``args[0]["C"]``; and
* it maps server errors to different DBAPI exception classes than psycopg (e.g.
  a serialization failure surfaces as ``ProgrammingError``, not ``OperationalError``).

These tests pin the code paths that were previously psycopg-specific.
"""

from typing import cast

import pg8000.dbapi
import psycopg
import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, OperationalError, ProgrammingError

from dbos._app_db import PostgresApplicationDatabase
from dbos._pg_errors import (
    get_sqlstate,
    is_serialization_error,
    is_serialization_or_lock_error,
    retriable_postgres_exception,
)
from dbos._sys_db_postgres import PostgresSystemDatabase

from .conftest import default_config


def _wrap(
    orig: Exception,
    base: type[Exception],
    connection_invalidated: bool = False,
) -> DBAPIError:
    """Wrap a raw driver exception the way SQLAlchemy does when a query fails."""
    return cast(
        DBAPIError,
        DBAPIError.instance(
            "SELECT 1", {}, orig, base, connection_invalidated=connection_invalidated
        ),
    )


def _pg8000(
    sqlstate: str,
    message: str = "boom",
    cls: type[Exception] = pg8000.dbapi.DatabaseError,
    connection_invalidated: bool = False,
) -> DBAPIError:
    # pg8000 exposes the server ErrorResponse fields as a dict on args[0]; "C" is the SQLSTATE.
    orig = cls({"S": "ERROR", "C": sqlstate, "M": message})
    return _wrap(
        orig, pg8000.dbapi.Error, connection_invalidated=connection_invalidated
    )


def _psycopg(orig: Exception, connection_invalidated: bool = False) -> DBAPIError:
    return _wrap(orig, psycopg.Error, connection_invalidated=connection_invalidated)


# object.__new__ builds an instance without a live engine; the _is_* checks use only their argument.
def _app_db_probe() -> PostgresApplicationDatabase:
    return object.__new__(PostgresApplicationDatabase)


def _sys_db_probe() -> PostgresSystemDatabase:
    return object.__new__(PostgresSystemDatabase)


def test_get_sqlstate_reads_pg8000_and_psycopg() -> None:
    # pg8000: no .sqlstate attribute; the code is in args[0]["C"]
    pg_err = pg8000.dbapi.DatabaseError({"S": "ERROR", "C": "40001", "M": "serialize"})
    assert not hasattr(pg_err, "sqlstate")
    assert get_sqlstate(pg_err) == "40001"
    # psycopg: exposes .sqlstate
    assert get_sqlstate(psycopg.errors.SerializationFailure("x")) == "40001"
    # shapes that carry no code -> None, never an exception
    assert get_sqlstate(None) is None
    assert get_sqlstate(ValueError("nope")) is None
    assert get_sqlstate(pg8000.dbapi.DatabaseError("string-not-dict")) is None
    assert (
        get_sqlstate(pg8000.dbapi.DatabaseError({"S": "ERROR", "M": "no code"})) is None
    )


def test_serialization_classification_pg8000() -> None:
    for code in ("40001", "40P01"):
        assert is_serialization_error(_pg8000(code)) is True
    for code in ("40001", "40P01", "55P03"):
        assert is_serialization_or_lock_error(_pg8000(code)) is True
    # non-serialization codes must not be misclassified
    assert is_serialization_error(_pg8000("23505")) is False
    assert is_serialization_or_lock_error(_pg8000("23505")) is False
    # non-DBAPI inputs are rejected without raising
    assert is_serialization_or_lock_error(ValueError("x")) is False


def test_pg8000_serialization_error_is_not_operationalerror() -> None:
    """Queue-worker guard: pg8000 does not raise OperationalError for a serialization
    failure, so a bare `except OperationalError` misses it. Classification is by
    SQLSTATE and the queue now catches DBAPIError."""
    err = _pg8000("40001", cls=pg8000.dbapi.ProgrammingError)
    assert not isinstance(err, OperationalError)
    assert isinstance(err, (DBAPIError, ProgrammingError))
    assert is_serialization_or_lock_error(err) is True


def test_unique_and_fk_violation_detected_for_pg8000() -> None:
    """These checks read orig.sqlstate directly before the fix and AttributeError'd on
    pg8000 (which has no .sqlstate)."""
    app = _app_db_probe()
    sys_db = _sys_db_probe()
    unique = _pg8000("23505", cls=pg8000.dbapi.IntegrityError)
    fk = _pg8000("23503", cls=pg8000.dbapi.IntegrityError)
    syntax = _pg8000("42601")
    assert app._is_unique_constraint_violation(unique) is True
    assert sys_db._is_unique_constraint_violation(unique) is True
    assert sys_db._is_foreign_key_violation(fk) is True
    # unrelated errors return False rather than raising
    assert app._is_unique_constraint_violation(syntax) is False
    assert sys_db._is_foreign_key_violation(unique) is False


def test_deterministic_error_is_not_retriable() -> None:
    """H1 regression: a non-transient error whose message text merely mentions
    connection/timeout must NOT be treated as retriable — it carries a real SQLSTATE,
    and misclassifying it would hang the unbounded db_retry loop forever."""
    # pg8000 unique violation whose message echoes user data mentioning connection+timeout
    pg = _pg8000(
        "23505",
        message="duplicate key on connection_idx = 'timeout'",
        cls=pg8000.dbapi.IntegrityError,
    )
    assert "connection" in str(pg.orig).lower() and "timeout" in str(pg.orig).lower()
    assert retriable_postgres_exception(pg) is False
    # psycopg equivalent: message contains "server closed the connection unexpectedly"
    ps = _psycopg(
        psycopg.errors.UniqueViolation(
            "Key (u)=(server closed the connection unexpectedly) already exists"
        )
    )
    assert retriable_postgres_exception(ps) is False


def test_retriable_connection_errors() -> None:
    # connection/resource/intervention SQLSTATE classes are retriable for any driver
    for code in ("08006", "08003", "53300", "57P01"):
        assert retriable_postgres_exception(_pg8000(code)) is True
    # an invalidated connection short-circuits to retriable even for a deterministic code
    assert (
        retriable_postgres_exception(_pg8000("23505", connection_invalidated=True))
        is True
    )
    # psycopg client-side ConnectionTimeout has no SQLSTATE; matched via message heuristic
    assert (
        retriable_postgres_exception(
            _psycopg(psycopg.errors.ConnectionTimeout("connection timeout expired"))
        )
        is True
    )
    # a plain non-connection error is not retriable
    assert retriable_postgres_exception(_pg8000("42601")) is False
    # non-DBAPI inputs are rejected without raising
    assert retriable_postgres_exception(ValueError("x")) is False


def test_pg8000_live_error_classification(skip_with_sqlite: None) -> None:
    """Drive a real pg8000 connection to PostgreSQL and classify genuine server errors,
    so the code runs against pg8000's actual error objects (not constructed ones)."""
    app_url = default_config()["application_database_url"]
    assert app_url is not None
    # pg8000 to the always-present "postgres" maintenance DB (no DBOS engine needed).
    url = sa.make_url(app_url).set(drivername="postgresql+pg8000", database="postgres")
    engine = sa.create_engine(url)
    table = "_dbos_pg8000_error_test"
    app = _app_db_probe()
    sys_db = _sys_db_probe()
    try:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
            conn.execute(
                sa.text(f"CREATE TABLE {table} (id INT PRIMARY KEY, val INT NOT NULL)")
            )
            conn.execute(sa.text(f"INSERT INTO {table} VALUES (1, 0)"))

        # Real unique violation (23505)
        with pytest.raises(DBAPIError) as exc_info:
            with engine.begin() as conn:
                conn.execute(sa.text(f"INSERT INTO {table} VALUES (1, 0)"))
        unique_err = exc_info.value
        assert not hasattr(unique_err.orig, "sqlstate")  # pg8000 shape, unlike psycopg
        assert get_sqlstate(unique_err.orig) == "23505"
        assert app._is_unique_constraint_violation(unique_err) is True
        assert sys_db._is_unique_constraint_violation(unique_err) is True
        assert retriable_postgres_exception(unique_err) is False

        # Real lock-not-available (55P03) via SELECT ... FOR UPDATE NOWAIT
        holder = engine.connect()
        waiter = engine.connect()
        try:
            holder_tx = holder.begin()
            holder.execute(sa.text(f"SELECT * FROM {table} WHERE id = 1 FOR UPDATE"))
            waiter_tx = waiter.begin()
            with pytest.raises(DBAPIError) as exc_info:
                waiter.execute(
                    sa.text(f"SELECT * FROM {table} WHERE id = 1 FOR UPDATE NOWAIT")
                )
            assert get_sqlstate(exc_info.value.orig) == "55P03"
            assert is_serialization_or_lock_error(exc_info.value) is True
            waiter_tx.rollback()
            holder_tx.rollback()
        finally:
            holder.close()
            waiter.close()

        # Real serialization failure (40001) via two SERIALIZABLE transactions with a
        # write-write conflict — deterministic, no sleeps needed.
        a = engine.connect().execution_options(isolation_level="SERIALIZABLE")
        b = engine.connect().execution_options(isolation_level="SERIALIZABLE")
        try:
            a_tx = a.begin()
            b_tx = b.begin()
            a.execute(sa.text(f"SELECT val FROM {table} WHERE id = 1"))
            b.execute(sa.text(f"SELECT val FROM {table} WHERE id = 1"))
            a.execute(sa.text(f"UPDATE {table} SET val = 1 WHERE id = 1"))
            a_tx.commit()
            with pytest.raises(DBAPIError) as exc_info:
                b.execute(sa.text(f"UPDATE {table} SET val = 2 WHERE id = 1"))
                b_tx.commit()
            ser_err = exc_info.value
            assert get_sqlstate(ser_err.orig) == "40001"
            assert is_serialization_error(ser_err) is True
            assert is_serialization_or_lock_error(ser_err) is True
            # pg8000 surfaces this as (Programming)Error, not OperationalError
            assert not isinstance(ser_err, OperationalError)
        finally:
            # close() rolls back any transaction still open (e.g. b's failed one)
            a.close()
            b.close()
    finally:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        engine.dispose()
