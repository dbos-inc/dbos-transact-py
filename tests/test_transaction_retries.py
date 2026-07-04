"""Application-error retries for @DBOS.transaction.

A transaction only retried serialization conflicts; an application error raised
by the function was recorded and re-raised on the first attempt. These tests
cover the opt-in application-error retry policy (retries_allowed / max_attempts /
interval_seconds / backoff_rate / should_retry), which mirrors @DBOS.step and the
Go SDK's RunAsTransaction step-retry options. Serialization retries are unchanged
and independent of this policy.
"""

import time
import uuid

import pytest
import sqlalchemy as sa

from dbos import DBOS, SetWorkflowID
from dbos._schemas.system_database import SystemSchema


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


def test_transaction_recorded_error_replays_without_retrying(dbos: DBOS) -> None:
    """A recorded transaction error, replayed on recovery, must be re-raised
    immediately — not fed back into the retry loop (which would sleep the whole
    backoff again for an already-decided outcome)."""
    runs = {"n": 0}

    @DBOS.transaction(retries_allowed=True, max_attempts=2, interval_seconds=0.5)
    def failing() -> None:
        DBOS.sql_session.execute(sa.text("SELECT 1"))
        runs["n"] += 1
        raise ValueError("permanent")

    @DBOS.workflow()
    def wf() -> None:
        return failing()

    wfid = str(uuid.uuid4())
    # First run: the transaction exhausts its retry budget and records the error
    # (in transaction_outputs, the app DB, and operation_outputs, the system DB).
    with SetWorkflowID(wfid):
        with pytest.raises(ValueError, match="permanent"):
            wf()
    assert runs["n"] == 2  # one initial attempt plus one retry, then recorded

    # Simulate the crash window: drop the system-DB workflow record. The CASCADE
    # wipes operation_outputs, but transaction_outputs (app DB, no such FK) stays.
    # On re-run the top-level replay check misses and the in-loop check finds the
    # recorded error — the path that must not be retried.
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.delete(SystemSchema.workflow_status).where(
                SystemSchema.workflow_status.c.workflow_uuid == wfid
            )
        )

    start = time.time()
    with SetWorkflowID(wfid):
        with pytest.raises(ValueError, match="permanent"):
            wf()
    elapsed = time.time() - start
    assert runs["n"] == 2, "the body is not re-run on replay of a recorded error"
    assert elapsed < 0.3, (
        f"replay of a recorded error must be immediate, not spin the retry "
        f"backoff (took {elapsed:.2f}s)"
    )
