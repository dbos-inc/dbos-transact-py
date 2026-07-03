"""Application-error retries for @DBOS.transaction.

A transaction only retried serialization conflicts; an application error raised
by the function was recorded and re-raised on the first attempt. These tests
cover the opt-in application-error retry policy (retries_allowed / max_attempts /
interval_seconds / backoff_rate / should_retry), which mirrors @DBOS.step and the
Go SDK's RunAsTransaction step-retry options. Serialization retries are unchanged
and independent of this policy.
"""

import pytest
import sqlalchemy as sa

from dbos import DBOS


def test_transaction_retries_to_success(dbos: DBOS) -> None:
    runs = 0

    @DBOS.transaction(retries_allowed=True, max_attempts=3, interval_seconds=0.001)
    def flaky(x: str) -> str:
        nonlocal runs
        DBOS.sql_session.execute(sa.text("SELECT 1"))
        runs += 1
        if runs < 3:
            raise Exception("transient")
        return x + "-ok"

    assert flaky("v") == "v-ok"
    assert runs == 3  # one initial attempt plus two retries


def test_transaction_no_retry_by_default(dbos: DBOS) -> None:
    runs = 0

    @DBOS.transaction()
    def boom(x: str) -> str:
        nonlocal runs
        DBOS.sql_session.execute(sa.text("SELECT 1"))
        runs += 1
        raise Exception("boom")

    with pytest.raises(Exception, match="boom"):
        boom("v")
    assert runs == 1  # default retries_allowed=False: runs once, then raises


def test_transaction_exhausts_retries_then_raises(dbos: DBOS) -> None:
    runs = 0

    @DBOS.transaction(retries_allowed=True, max_attempts=3, interval_seconds=0.001)
    def always_fails(x: str) -> str:
        nonlocal runs
        DBOS.sql_session.execute(sa.text("SELECT 1"))
        runs += 1
        raise Exception("nope")

    with pytest.raises(Exception, match="nope"):
        always_fails("v")
    assert runs == 3  # max_attempts total, then the error is recorded and raised


def test_transaction_should_retry_predicate_stops(dbos: DBOS) -> None:
    runs = 0

    @DBOS.transaction(
        retries_allowed=True,
        max_attempts=5,
        interval_seconds=0.001,
        should_retry=lambda e: False,
    )
    def permanent(x: str) -> str:
        nonlocal runs
        DBOS.sql_session.execute(sa.text("SELECT 1"))
        runs += 1
        raise Exception("permanent")

    with pytest.raises(Exception, match="permanent"):
        permanent("v")
    assert runs == 1  # predicate rejected the error: no retries despite max_attempts=5
