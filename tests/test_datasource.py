"""Tests for SyncDatasource and AsyncDatasource."""

import asyncio
import base64
import inspect
import pickle
import sqlite3
import uuid
from typing import Any, AsyncGenerator, Generator, Optional

import psycopg
import pytest
import pytest_asyncio
import sqlalchemy as sa
from psycopg.errors import SerializationFailure
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from dbos import DBOS, AsyncSQLAlchemyDatasource, SetWorkflowID, SQLAlchemyDatasource
from dbos._app_db import RecordedResult
from dbos._datasource_postgres import PostgresAsyncDatasource, PostgresSyncDatasource
from dbos._datasource_sqlite import SqliteAsyncDatasource, SqliteSyncDatasource
from dbos._error import DBOSException, DBOSWorkflowConflictIDError
from dbos._schemas import SCHEMA_PLACEHOLDER
from dbos._schemas.datasource_database import DatasourceSchema
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import deserialize_value
from dbos._sys_db import WorkflowStatusString
from tests.conftest import (
    ensure_application_database,
    postgres_urls,
    reexecute_workflow_by_id,
    retry_until_success,
    retry_until_success_async,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# A winner that checkpoints its step keeps the workflow, so the loser parks at the lost
# race without ever adopting its result; one that vanishes before checkpointing leaves
# nobody to finish it, so the loser must replay and carry on instead.
_LOST_RACE_CASES = [
    pytest.param(True, 1, 0, id="winner-alive"),
    pytest.param(False, 2, 1, id="winner-gone"),
]


def _count_replays(
    ds: Any, replays: dict[str, int], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Count adoptions of a conflicting row: the fork #818 is about, where replaying
    turns the lost race into a value and the workflow body resumes."""
    real_replay = ds._replay_conflicting_step

    if inspect.iscoroutinefunction(real_replay):

        async def counting_replay_async(workflow_id: str, step_id: int) -> Any:
            replays["n"] += 1
            return await real_replay(workflow_id, step_id)

        monkeypatch.setattr(ds, "_replay_conflicting_step", counting_replay_async)
        return

    def counting_replay(workflow_id: str, step_id: int) -> Any:
        replays["n"] += 1
        return real_replay(workflow_id, step_id)

    monkeypatch.setattr(ds, "_replay_conflicting_step", counting_replay)


def _winner_step_row(conn: Any, wfid: str, step_name: str) -> dict[str, Any]:
    """The winner's checkpoint for one step, to replant mid-race after the rewind."""
    row = conn.execute(
        sa.select(SystemSchema.operation_outputs).where(
            SystemSchema.operation_outputs.c.workflow_uuid == wfid,
            SystemSchema.operation_outputs.c.function_name == step_name,
        )
    ).mappings()
    return dict(row.one())


def _checkpointed_steps(conn: Any, wfid: str) -> list[str]:
    return list(
        conn.execute(
            sa.select(SystemSchema.operation_outputs.c.function_name)
            .where(SystemSchema.operation_outputs.c.workflow_uuid == wfid)
            .order_by(SystemSchema.operation_outputs.c.function_id)
        ).scalars()
    )


# Application table used to prove a losing duplicate execution's writes are rolled back.
_race_side_effects = sa.Table(
    "race_side_effects",
    sa.MetaData(schema=SCHEMA_PLACEHOLDER),
    sa.Column("tag", sa.Text),
)


def _skip_if_pg_unreachable(raw_pg_url: str) -> None:
    try:
        # Probe the maintenance database: the shared application database may have
        # been dropped by a drop_test_databases test, which is not unreachability.
        engine = sa.create_engine(
            sa.make_url(raw_pg_url)
            .set(drivername="postgresql+psycopg")
            .set(database="postgres"),
            connect_args={"connect_timeout": 3},
        )
        with engine.connect():
            pass
        engine.dispose()
    except Exception:
        pytest.skip("PostgreSQL not reachable")


def _check_both_tables(
    ds: SQLAlchemyDatasource, dbos_instance: DBOS, wfid: str
) -> None:
    with ds.engine.connect() as conn:
        ds_row = conn.execute(
            sa.select(
                DatasourceSchema.datasource_outputs.c.step_id,
                DatasourceSchema.datasource_outputs.c.output,
            ).where(DatasourceSchema.datasource_outputs.c.workflow_id == wfid)
        ).first()
    assert ds_row is not None, "datasource_outputs row missing"
    assert ds_row.step_id == 1
    assert ds_row.output is not None

    with dbos_instance._sys_db.engine.connect() as conn:
        sys_row = conn.execute(
            sa.select(
                SystemSchema.operation_outputs.c.function_id,
                SystemSchema.operation_outputs.c.output,
            ).where(SystemSchema.operation_outputs.c.workflow_uuid == wfid)
        ).first()
    assert sys_row is not None, "operation_outputs row missing"
    assert sys_row.function_id == 1
    assert sys_row.output is not None


async def _async_check_both_tables(
    ds: AsyncSQLAlchemyDatasource, dbos_instance: DBOS, wfid: str
) -> None:
    async with ds.engine.connect() as conn:
        ds_row = (
            await conn.execute(
                sa.select(
                    DatasourceSchema.datasource_outputs.c.step_id,
                    DatasourceSchema.datasource_outputs.c.output,
                ).where(DatasourceSchema.datasource_outputs.c.workflow_id == wfid)
            )
        ).first()
    assert ds_row is not None, "datasource_outputs row missing"
    assert ds_row.step_id == 1
    assert ds_row.output is not None

    with dbos_instance._sys_db.engine.connect() as conn:
        sys_row = conn.execute(
            sa.select(
                SystemSchema.operation_outputs.c.function_id,
                SystemSchema.operation_outputs.c.output,
            ).where(SystemSchema.operation_outputs.c.workflow_uuid == wfid)
        ).first()
    assert sys_row is not None, "operation_outputs row missing"
    assert sys_row.function_id == 1
    assert sys_row.output is not None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(params=["sqlite", "pg"])
def sync_ds(
    request: pytest.FixtureRequest, tmp_path: Any
) -> Generator[SQLAlchemyDatasource, None, None]:
    if request.param == "sqlite":
        ds = SQLAlchemyDatasource.create(f"sqlite:///{tmp_path}/ds_test.sqlite")
        yield ds
        if ds.created_engine:
            ds.engine.dispose()
    else:
        url = postgres_urls()[0]
        if not url.startswith("postgresql"):
            pytest.skip("not a PostgreSQL environment")
        _skip_if_pg_unreachable(url)
        ensure_application_database()
        schema = f"ds_test_{uuid.uuid4().hex[:8]}"
        ds = SQLAlchemyDatasource.create(
            url.replace("postgresql://", "postgresql+psycopg://"), schema=schema
        )
        yield ds
        with ds.engine.begin() as conn:
            conn.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        if ds.created_engine:
            ds.engine.dispose()


@pytest_asyncio.fixture(params=["sqlite", "pg"])
async def async_ds(
    request: pytest.FixtureRequest, tmp_path: Any
) -> AsyncGenerator[AsyncSQLAlchemyDatasource, None]:
    if request.param == "sqlite":
        ds = await AsyncSQLAlchemyDatasource.create(
            f"sqlite+aiosqlite:///{tmp_path}/async_ds_test.sqlite"
        )
        yield ds
        await ds.engine.dispose()
    else:
        url = postgres_urls()[0]
        if not url.startswith("postgresql"):
            pytest.skip("not a PostgreSQL environment")
        _skip_if_pg_unreachable(url)
        ensure_application_database()
        schema = f"ds_test_{uuid.uuid4().hex[:8]}"
        ds = await AsyncSQLAlchemyDatasource.create(
            url.replace("postgresql://", "postgresql+psycopg://"), schema=schema
        )
        yield ds
        async with ds.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await ds.engine.dispose()


# ---------------------------------------------------------------------------
# Sync bare-run tests (no DBOS workflow needed)
# ---------------------------------------------------------------------------


def test_sync_ds_bare_run(sync_ds: SQLAlchemyDatasource) -> None:
    """run_tx_step outside a workflow executes the function transactionally."""
    counter = {"n": 0}

    def increment(amount: int) -> int:
        session = sync_ds.sql_session()
        session.execute(text("SELECT 1"))
        counter["n"] += amount
        return counter["n"]

    result = sync_ds.run_tx_step(None, increment, 5)
    assert result == 5
    result = sync_ds.run_tx_step(None, increment, 3)
    assert result == 8


def test_sync_ds_decorator_bare_run(sync_ds: SQLAlchemyDatasource) -> None:
    """@ds.transaction outside a workflow executes the function transactionally."""
    counter = {"n": 0}

    @sync_ds.transaction
    def increment(amount: int) -> int:
        session = sync_ds.sql_session()
        session.execute(text("SELECT 1"))
        counter["n"] += amount
        return counter["n"]

    assert increment(10) == 10
    assert increment(5) == 15


def test_sync_ds_decorator_with_options(sync_ds: SQLAlchemyDatasource) -> None:
    """@ds.transaction accepts isolation_level and name options."""
    counter = {"n": 0}

    @sync_ds.transaction(isolation_level="SERIALIZABLE", name="my_step")
    def increment(amount: int) -> int:
        counter["n"] += amount
        return counter["n"]

    assert increment(7) == 7


def test_sync_ds_sql_session_outside_tx_raises(sync_ds: SQLAlchemyDatasource) -> None:
    """sql_session() outside a datasource transaction must raise."""
    with pytest.raises(AssertionError):
        sync_ds.sql_session()


def test_sync_ds_rejects_coroutine(sync_ds: SQLAlchemyDatasource) -> None:
    """run_tx_step with a coroutine function must raise immediately."""

    async def my_async_func() -> str:
        return "oops"

    with pytest.raises(DBOSException, match="coroutine"):
        sync_ds.run_tx_step(None, my_async_func)  # type: ignore


def test_sync_ds_transaction_decorator_rejects_coroutine(
    sync_ds: SQLAlchemyDatasource,
) -> None:
    """@ds.transaction on a coroutine must raise at decoration time."""
    with pytest.raises(DBOSException, match="coroutine"):

        @sync_ds.transaction
        async def bad() -> str:
            return "nope"


# ---------------------------------------------------------------------------
# Sync OAOO tests (inside a DBOS workflow)
# ---------------------------------------------------------------------------


def test_sync_ds_oaoo(dbos: DBOS, sync_ds: SQLAlchemyDatasource) -> None:
    """run_tx_step inside a workflow records the result and replays on retry."""
    call_count = {"n": 0}

    @DBOS.workflow()
    def my_workflow(value: str) -> str:
        return sync_ds.run_tx_step(None, expensive_step, value)

    def expensive_step(value: str) -> str:
        call_count["n"] += 1
        return f"result:{value}"

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        result = my_workflow("hello")
    assert result == "result:hello"
    assert call_count["n"] == 1

    with SetWorkflowID(wfid):
        result = my_workflow("hello")
    assert result == "result:hello"
    assert call_count["n"] == 1


def test_sync_ds_decorator_oaoo(dbos: DBOS, sync_ds: SQLAlchemyDatasource) -> None:
    """@ds.transaction inside a workflow records and replays the result."""
    call_count = {"n": 0}

    @sync_ds.transaction
    def decorated_step(value: str) -> str:
        call_count["n"] += 1
        return f"decorated:{value}"

    @DBOS.workflow()
    def my_workflow(value: str) -> str:
        return decorated_step(value)

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        result = my_workflow("world")
    assert result == "decorated:world"
    assert call_count["n"] == 1

    with SetWorkflowID(wfid):
        result = my_workflow("world")
    assert result == "decorated:world"
    assert call_count["n"] == 1


def test_sync_ds_error_oaoo(dbos: DBOS, sync_ds: SQLAlchemyDatasource) -> None:
    """When a datasource step raises, the error is recorded and replayed."""
    call_count = {"n": 0}

    def failing_step() -> str:
        call_count["n"] += 1
        raise ValueError("ds step failed")

    @DBOS.workflow()
    def my_workflow() -> str:
        return sync_ds.run_tx_step(None, failing_step)

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        with pytest.raises(ValueError, match="ds step failed"):
            my_workflow()
    assert call_count["n"] == 1

    with SetWorkflowID(wfid):
        with pytest.raises(Exception, match="ds step failed"):
            my_workflow()
    assert call_count["n"] == 1


def test_sync_ds_retries_on_serialization_error(
    dbos: DBOS, sync_ds: SQLAlchemyDatasource
) -> None:
    """A SQLSTATE 40001 raised inside the txn body must be retried, not recorded."""
    if not isinstance(sync_ds, PostgresSyncDatasource):
        pytest.skip("manual serialization error is psycopg-specific")

    call_count = {"n": 0}
    max_retries = 3

    def flaky_step() -> str:
        call_count["n"] += 1
        if call_count["n"] <= max_retries:
            raise OperationalError(
                "Serialization test error", {}, SerializationFailure()
            )
        return "success"

    @DBOS.workflow()
    def my_workflow() -> str:
        return sync_ds.run_tx_step(None, flaky_step)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert my_workflow() == "success"
    # First max_retries calls raised, the (max_retries + 1)-th succeeded.
    assert call_count["n"] == max_retries + 1

    # The successful result, not an error, must be in datasource_outputs.
    with sync_ds.engine.connect() as conn:
        row = conn.execute(
            sa.select(
                DatasourceSchema.datasource_outputs.c.output,
                DatasourceSchema.datasource_outputs.c.error,
            ).where(DatasourceSchema.datasource_outputs.c.workflow_id == wfid)
        ).first()
    assert row is not None
    assert row.error is None
    assert row.output is not None

    # Replay must not re-execute the step.
    with SetWorkflowID(wfid):
        assert my_workflow() == "success"
    assert call_count["n"] == max_retries + 1


def test_sync_ds_retries_locked_precheck(
    dbos: DBOS, sync_ds: SQLAlchemyDatasource, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 'database is locked' error on the OAOO pre-check read must be retried, not terminal (#761)."""
    if not isinstance(sync_ds, SqliteSyncDatasource):
        pytest.skip("SQLite-specific: locked pre-check retry")

    body_calls = {"n": 0}

    def step() -> str:
        body_calls["n"] += 1
        sync_ds.sql_session().execute(sa.text("SELECT 1"))
        return "ok"

    @DBOS.workflow()
    def my_workflow() -> str:
        return sync_ds.run_tx_step({"name": "locked_precheck"}, step)

    # The first pre-check read hits a transient lock; it must be retried, not raised terminally.
    real_check = sync_ds._check_execution
    precheck_calls = {"n": 0}

    def flaky_check(workflow_id: str, step_id: int) -> Any:
        precheck_calls["n"] += 1
        if precheck_calls["n"] == 1:
            raise OperationalError(
                "SELECT ... FROM datasource_outputs ...",
                {},
                sqlite3.OperationalError("database is locked"),
            )
        return real_check(workflow_id, step_id)

    monkeypatch.setattr(sync_ds, "_check_execution", flaky_check)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert my_workflow() == "ok"
    assert precheck_calls["n"] >= 2  # pre-check was retried after the lock
    assert body_calls["n"] == 1  # body ran exactly once

    # The successful result, not an error, must be recorded.
    with sync_ds.engine.connect() as conn:
        row = conn.execute(
            sa.select(
                DatasourceSchema.datasource_outputs.c.output,
                DatasourceSchema.datasource_outputs.c.error,
            ).where(DatasourceSchema.datasource_outputs.c.workflow_id == wfid)
        ).first()
    assert row is not None
    assert row.error is None
    assert row.output is not None


def test_sync_ds_multiple_steps_in_workflow(
    dbos: DBOS, sync_ds: SQLAlchemyDatasource
) -> None:
    """Multiple datasource steps in one workflow each get their own step_id."""
    call_counts = {"a": 0, "b": 0}

    def step_a() -> str:
        call_counts["a"] += 1
        return "A"

    def step_b() -> str:
        call_counts["b"] += 1
        return "B"

    @DBOS.workflow()
    def my_workflow() -> str:
        r1 = sync_ds.run_tx_step(None, step_a)
        r2 = sync_ds.run_tx_step(None, step_b)
        return r1 + r2

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        result = my_workflow()
    assert result == "AB"

    with SetWorkflowID(wfid):
        result = my_workflow()
    assert result == "AB"
    assert call_counts["a"] == 1
    assert call_counts["b"] == 1


def test_sync_ds_writes_both_tables(dbos: DBOS, sync_ds: SQLAlchemyDatasource) -> None:
    """Datasource step writes to both datasource_outputs and operation_outputs."""

    def step_fn() -> str:
        return "hello"

    @DBOS.workflow()
    def my_workflow() -> str:
        return sync_ds.run_tx_step(None, step_fn)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert my_workflow() == "hello"

    _check_both_tables(sync_ds, dbos, wfid)


def test_sync_ds_step_recorded_with_name(
    dbos: DBOS, sync_ds: SQLAlchemyDatasource
) -> None:
    """Datasource step appears in list_workflow_steps with the right name and output."""

    def my_step() -> str:
        return "run_tx_result"

    @sync_ds.transaction
    def unnamed_step() -> str:
        return "unnamed_result"

    @sync_ds.transaction(name="my_named_step")
    def named_step() -> str:
        return "named_result"

    @DBOS.workflow()
    def my_workflow() -> tuple[str, str, str]:
        r1 = sync_ds.run_tx_step(None, my_step)
        r2 = unnamed_step()
        r3 = named_step()
        return r1, r2, r3

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert my_workflow() == ("run_tx_result", "unnamed_result", "named_result")

    steps = DBOS.list_workflow_steps(wfid)
    assert len(steps) == 3
    assert steps[0]["function_id"] == 1
    assert steps[0]["function_name"].endswith("my_step")
    assert steps[0]["output"] == "run_tx_result"
    assert steps[1]["function_id"] == 2
    assert steps[1]["function_name"].endswith("unnamed_step")
    assert steps[1]["output"] == "unnamed_result"
    assert steps[2]["function_id"] == 3
    assert steps[2]["function_name"] == "my_named_step"
    assert steps[2]["output"] == "named_result"

    with sync_ds.engine.connect() as conn:
        ds_rows = conn.execute(
            sa.select(
                DatasourceSchema.datasource_outputs.c.step_id,
                DatasourceSchema.datasource_outputs.c.output,
                DatasourceSchema.datasource_outputs.c.error,
                DatasourceSchema.datasource_outputs.c.serialization,
            )
            .where(DatasourceSchema.datasource_outputs.c.workflow_id == wfid)
            .order_by(DatasourceSchema.datasource_outputs.c.step_id)
        ).fetchall()
    assert len(ds_rows) == 3
    for row in ds_rows:
        assert row.error is None
        assert row.serialization == "py_pickle"
    assert ds_rows[0].step_id == 1
    assert pickle.loads(base64.b64decode(ds_rows[0].output)) == "run_tx_result"
    assert ds_rows[1].step_id == 2
    assert pickle.loads(base64.b64decode(ds_rows[1].output)) == "unnamed_result"
    assert ds_rows[2].step_id == 3
    assert pickle.loads(base64.b64decode(ds_rows[2].output)) == "named_result"


def test_sync_ds_recovers_from_sysdb_loss(
    dbos: DBOS, sync_ds: SQLAlchemyDatasource
) -> None:
    """datasource_outputs is the source of truth when the sysdb step record is lost.

    Simulates the crash window: datasource_outputs was written atomically inside
    the user transaction, but the system crashed before operation_outputs was
    persisted. On re-run, the step result must be recovered from datasource_outputs
    without re-executing the step function.
    """
    call_count = {"n": 0}

    def step_fn() -> str:
        call_count["n"] += 1
        return "recovered"

    @DBOS.workflow()
    def my_workflow() -> str:
        return sync_ds.run_tx_step(None, step_fn)

    wfid = str(uuid.uuid4())

    # First run: writes both tables.
    with SetWorkflowID(wfid):
        assert my_workflow() == "recovered"
    assert call_count["n"] == 1

    # Simulate crash: remove the workflow record from the system DB.
    # The CASCADE on operation_outputs.workflow_uuid wipes the step record too,
    # while datasource_outputs (in the app DB) is unaffected.
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.delete(SystemSchema.workflow_status).where(
                SystemSchema.workflow_status.c.workflow_uuid == wfid
            )
        )

    # Re-run: DBOS treats this as a new workflow (no workflow_status row).
    # run_step finds no operation_outputs entry, so it calls _body().
    # _body() finds the datasource_outputs row and replays — step_fn not called.
    with SetWorkflowID(wfid):
        assert my_workflow() == "recovered"
    assert call_count["n"] == 1


def test_sync_ds_conflicts_when_duplicate_execution_wins(
    dbos: DBOS, sync_ds: SQLAlchemyDatasource, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A duplicate execution that loses the witness-row race stops with a workflow
    conflict instead of surfacing the primary-key IntegrityError (#812) or carrying on
    with the winner's result (#818). It replays only when the conflicting row may be
    its own, from an attempt whose commit was ambiguously lost."""
    _race_side_effects.create(sync_ds.engine, checkfirst=True)
    call_count = {"n": 0}
    should_fail = {"v": False}
    winner_step: dict[str, Any] = {}

    def step_fn() -> str:
        call_count["n"] += 1
        # Populated after the winning run: the live duplicate's step checkpoint, planted
        # mid-transaction because that is what tells a loser someone else owns the workflow.
        if winner_step:
            with dbos._sys_db.engine.begin() as conn:
                conn.execute(
                    sa.insert(SystemSchema.operation_outputs).values(**winner_step)
                )
        sync_ds.sql_session().execute(
            _race_side_effects.insert().values(tag=f"run-{call_count['n']}")
        )
        if should_fail["v"]:
            raise ValueError("loser's own failure")
        return f"result-{call_count['n']}"

    @DBOS.workflow()
    def my_workflow() -> str:
        try:
            return sync_ds.run_tx_step(None, step_fn)
        except DBOSWorkflowConflictIDError:
            # A real duplicate parks here instead; caught to keep the assertion local.
            return "conflicted"

    wfid = str(uuid.uuid4())

    def forget_workflow() -> None:
        # Drop the sysdb checkpoint so run_step calls _body again instead of replaying.
        # A real concurrent duplicate keeps that row and ends in DBOSWorkflowConflictIDError instead.
        with dbos._sys_db.engine.begin() as conn:
            conn.execute(
                sa.delete(SystemSchema.workflow_status).where(
                    SystemSchema.workflow_status.c.workflow_uuid == wfid
                )
            )

    # Blind one pre-check, so a loser misses the winner's row as it does in the real race.
    real_check = sync_ds._check_execution
    blind = {"next": False}

    def blind_next_check(workflow_id: str, step_id: int) -> Optional[RecordedResult]:
        if blind["next"]:
            blind["next"] = False
            return None
        return real_check(workflow_id, step_id)

    monkeypatch.setattr(sync_ds, "_check_execution", blind_next_check)

    # Count error-recording attempts, to pin that a lost result race never tries one.
    real_record_error = sync_ds._record_error
    record_error_calls = {"n": 0}

    def counting_record_error(
        workflow_id: str, step_id: int, error: str, serialization: Optional[str]
    ) -> None:
        record_error_calls["n"] += 1
        real_record_error(workflow_id, step_id, error, serialization)

    monkeypatch.setattr(sync_ds, "_record_error", counting_record_error)

    replays = {"n": 0}
    _count_replays(sync_ds, replays, monkeypatch)

    # The winning execution: commits its app writes and its datasource_outputs row.
    with SetWorkflowID(wfid):
        assert my_workflow() == "result-1"
    assert call_count["n"] == 1
    with dbos._sys_db.engine.connect() as conn:
        winner_step.update(_winner_step_row(conn, wfid, step_fn.__qualname__))

    # A loser whose body succeeds: the collision happens on the result-recording insert.
    forget_workflow()
    blind["next"] = True
    with SetWorkflowID(wfid):
        assert my_workflow() == "conflicted"
    assert call_count["n"] == 2  # the loser did run its body
    assert (
        record_error_calls["n"] == 0
    )  # the recorded result won without an error write
    # Stopped at the lost race, not one statement later at the step checkpoint (#818).
    assert replays["n"] == 0, "the loser adopted the winner's result"

    # A loser whose body fails: the collision moves to the error-recording insert.
    forget_workflow()
    blind["next"] = True
    should_fail["v"] = True
    with SetWorkflowID(wfid):
        assert my_workflow() == "conflicted"
    assert call_count["n"] == 3
    assert record_error_calls["n"] == 1
    assert replays["n"] == 0, "the loser adopted the winner's result"

    # The succeeding loser's writes were discarded and the winner's record still stands.
    with sync_ds.engine.connect() as conn:
        tags = [row.tag for row in conn.execute(sa.select(_race_side_effects.c.tag))]
        ds_row = conn.execute(
            sa.select(
                DatasourceSchema.datasource_outputs.c.output,
                DatasourceSchema.datasource_outputs.c.error,
                DatasourceSchema.datasource_outputs.c.serialization,
            ).where(DatasourceSchema.datasource_outputs.c.workflow_id == wfid)
        ).one()
    assert tags == ["run-1"]
    assert ds_row.error is None  # no loser error was ever recorded
    # The winner's output is still the one on record, unmodified by either loser.
    assert (
        deserialize_value(ds_row.output, ds_row.serialization, sync_ds.serializer)
        == "result-1"
    )


# Whether a lost acknowledgement is retriable decides how the row is met, not whose it is.
_LOST_ACK_ERRORS = [
    pytest.param("server closed the connection unexpectedly", 2, id="retriable"),
    pytest.param("consuming input failed: EOF detected", 1, id="non-retriable"),
]


@pytest.mark.parametrize("lost_ack_message, expected_attempts", _LOST_ACK_ERRORS)
def test_sync_ds_replays_its_own_lost_commit(
    dbos: DBOS,
    sync_ds: SQLAlchemyDatasource,
    monkeypatch: pytest.MonkeyPatch,
    lost_ack_message: str,
    expected_attempts: int,
) -> None:
    """A commit that lands and then loses its acknowledgement leaves a row this very
    execution wrote, so it replays that row instead of parking as a duplicate.

    A retriable error meets the row on the retry's result insert; a non-retriable one
    meets it on the error insert. Neither is a duplicate, so both replay."""
    if sync_ds.engine.dialect.name != "postgresql":
        pytest.skip("only a Postgres-style connection error makes a commit ambiguous")
    _race_side_effects.create(sync_ds.engine, checkfirst=True)
    call_count = {"n": 0}

    def step_fn() -> str:
        call_count["n"] += 1
        sync_ds.sql_session().execute(
            _race_side_effects.insert().values(tag=f"run-{call_count['n']}")
        )
        return f"result-{call_count['n']}"

    lose_ack = {"next": True}
    real_sessionmaker = sync_ds.sessionmaker

    class _LostAck:
        """Commits, then reports the connection dropped, as a lost ack does."""

        def __init__(self, transaction: Any) -> None:
            self._transaction = transaction

        def __enter__(self) -> Any:
            return self._transaction.__enter__()

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
            handled = self._transaction.__exit__(exc_type, exc, tb)
            if exc is None and lose_ack["next"]:
                lose_ack["next"] = False
                raise OperationalError(
                    "COMMIT", {}, psycopg.OperationalError(lost_ack_message)
                )
            return handled

    def flaky_sessionmaker() -> Any:
        session = real_sessionmaker()
        real_begin = session.begin
        # setattr: the proxy only has to satisfy the `with` protocol, not the type.
        setattr(session, "begin", lambda: _LostAck(real_begin()))
        return session

    monkeypatch.setattr(sync_ds, "sessionmaker", flaky_sessionmaker)

    @DBOS.workflow()
    def my_workflow() -> str:
        try:
            return sync_ds.run_tx_step(None, step_fn)
        except DBOSWorkflowConflictIDError:
            # Caught so a spurious park fails the assertion instead of hanging.
            return "conflicted"

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        # The committed attempt's own output, not a later attempt's.
        assert my_workflow() == "result-1"
    assert call_count["n"] == expected_attempts

    with sync_ds.engine.connect() as conn:
        tags = [row.tag for row in conn.execute(sa.select(_race_side_effects.c.tag))]
        ds_row = conn.execute(
            sa.select(
                DatasourceSchema.datasource_outputs.c.output,
                DatasourceSchema.datasource_outputs.c.error,
                DatasourceSchema.datasource_outputs.c.serialization,
            ).where(DatasourceSchema.datasource_outputs.c.workflow_id == wfid)
        ).one()
    assert tags == ["run-1"]  # only the committed attempt's write survives
    assert ds_row.error is None
    assert (
        deserialize_value(ds_row.output, ds_row.serialization, sync_ds.serializer)
        == "result-1"
    )


@pytest.mark.parametrize(
    "winner_checkpoints, expected_emails, expected_replays", _LOST_RACE_CASES
)
def test_sync_ds_duplicate_execution_stops_at_the_lost_race(
    dbos: DBOS,
    sync_ds: SQLAlchemyDatasource,
    monkeypatch: pytest.MonkeyPatch,
    winner_checkpoints: bool,
    expected_emails: int,
    expected_replays: int,
) -> None:
    """A duplicate execution that loses the datasource race parks where an ordinary
    step's loser parks, instead of replaying the winner's result and going on to run
    the workflow's next step with it (#818) -- but only while a winner is still there
    to finish the workflow, which its step checkpoint is the evidence of."""
    reserve_calls = {"n": 0}
    emails_sent = {"n": 0}
    winner_step: dict[str, Any] = {}

    def reserve() -> str:
        reserve_calls["n"] += 1
        # The winner checkpoints while the loser's transaction is open: the instant that
        # hands it the workflow, so recovery reaches the loser's park.
        if reserve_calls["n"] > 1 and winner_checkpoints:
            with dbos._sys_db.engine.begin() as conn:
                conn.execute(
                    sa.insert(SystemSchema.operation_outputs).values(**winner_step)
                )
        sync_ds.sql_session().execute(text("SELECT 1"))
        return "reserved"

    @DBOS.step()
    def send_email() -> None:
        emails_sent["n"] += 1

    @DBOS.workflow()
    def my_workflow() -> str:
        reserved = sync_ds.run_tx_step(None, reserve)
        send_email()
        return reserved

    wfid = str(uuid.uuid4())

    # The winning execution, run to completion.
    with SetWorkflowID(wfid):
        assert my_workflow() == "reserved"
    assert (reserve_calls["n"], emails_sent["n"]) == (1, 1)

    # Rewind to the instant the winner had committed its datasource_outputs row but no
    # step checkpoint yet: nothing to replay, and no outcome for a waiter to adopt.
    with dbos._sys_db.engine.begin() as conn:
        winner_output = conn.execute(
            sa.select(SystemSchema.workflow_status.c.output).where(
                SystemSchema.workflow_status.c.workflow_uuid == wfid
            )
        ).scalar_one()
        winner_step.update(_winner_step_row(conn, wfid, reserve.__qualname__))
        conn.execute(
            sa.delete(SystemSchema.operation_outputs).where(
                SystemSchema.operation_outputs.c.workflow_uuid == wfid
            )
        )
        conn.execute(
            sa.update(SystemSchema.workflow_status)
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
            .values(output=None)
        )

    # Blind one pre-check, so the loser misses the winner's row as it does in the real race.
    real_check = sync_ds._check_execution
    blind = {"next": True}

    def blind_next_check(workflow_id: str, step_id: int) -> Optional[RecordedResult]:
        if blind["next"]:
            blind["next"] = False
            return None
        return real_check(workflow_id, step_id)

    monkeypatch.setattr(sync_ds, "_check_execution", blind_next_check)

    replays = {"n": 0}
    _count_replays(sync_ds, replays, monkeypatch)

    # Dispatch the duplicate off the persisted row, exactly as recovery does.
    handle = reexecute_workflow_by_id(dbos, wfid)

    def loser_transaction_ran() -> None:
        assert reserve_calls["n"] >= 2

    def duplicate_left_the_workflow() -> None:
        # Released either at the park or after the whole body: reached only once the
        # duplicate can no longer run send_email, whichever way it went.
        assert wfid not in dbos._active_workflows_set.activeList()

    try:
        retry_until_success(loser_transaction_ran, interval=0.1, max_attempts=300)
        retry_until_success(duplicate_left_the_workflow, interval=0.1, max_attempts=300)

        assert emails_sent["n"] == expected_emails
        # Parking at the step checkpoint one statement later is not parking at the lost
        # race: with a live winner the duplicate must never adopt its result at all.
        assert replays["n"] == expected_replays, "the duplicate replayed the winner"
        with dbos._sys_db.engine.connect() as conn:
            steps = _checkpointed_steps(conn, wfid)
        if winner_checkpoints:
            # Only the winner's row: the parked duplicate checkpointed nothing.
            assert steps == [reserve.__qualname__], "the parked duplicate checkpointed"
        else:
            # No winner left to park behind, so the duplicate ran the workflow out.
            assert len(steps) == 2, "the duplicate did not finish the workflow"
    finally:
        # Publish the winner's outcome, which is what a parked duplicate waits for.
        # In a finally: an assertion above must not strand a thread polling forever.
        # Unconditional: in the winner-gone case the duplicate has already written this
        # same outcome, and a failed assertion above must not leave a thread parked.
        with dbos._sys_db.engine.begin() as conn:
            conn.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
                .values(status=WorkflowStatusString.SUCCESS.value, output=winner_output)
            )

    assert handle.get_result() == "reserved"


# ---------------------------------------------------------------------------
# Async bare-run tests (no DBOS workflow needed)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_ds_bare_run(async_ds: AsyncSQLAlchemyDatasource) -> None:
    """run_tx_step_async outside a workflow executes the function transactionally."""
    counter = {"n": 0}

    async def increment(amount: int) -> int:
        session = async_ds.sql_session()
        await session.execute(text("SELECT 1"))
        counter["n"] += amount
        return counter["n"]

    result = await async_ds.run_tx_step_async(None, increment, 5)
    assert result == 5
    result = await async_ds.run_tx_step_async(None, increment, 3)
    assert result == 8


@pytest.mark.asyncio
async def test_async_ds_decorator_bare_run(async_ds: AsyncSQLAlchemyDatasource) -> None:
    """@ds.transaction on an async function works outside a workflow."""
    counter = {"n": 0}

    @async_ds.transaction
    async def increment(amount: int) -> int:
        session = async_ds.sql_session()
        await session.execute(text("SELECT 1"))
        counter["n"] += amount
        return counter["n"]

    assert await increment(10) == 10
    assert await increment(5) == 15


@pytest.mark.asyncio
async def test_async_ds_decorator_with_options(
    async_ds: AsyncSQLAlchemyDatasource,
) -> None:
    """@ds.transaction accepts isolation_level and name options."""
    counter = {"n": 0}

    @async_ds.transaction(isolation_level="SERIALIZABLE", name="my_step")
    async def increment(amount: int) -> int:
        counter["n"] += amount
        return counter["n"]

    assert await increment(7) == 7


@pytest.mark.asyncio
async def test_async_ds_sql_session_outside_tx_raises(
    async_ds: AsyncSQLAlchemyDatasource,
) -> None:
    """sql_session() outside an async datasource transaction must raise."""
    with pytest.raises(AssertionError):
        async_ds.sql_session()


@pytest.mark.asyncio
async def test_async_ds_rejects_sync_func(async_ds: AsyncSQLAlchemyDatasource) -> None:
    """run_tx_step_async with a non-coroutine must raise."""

    def sync_func() -> str:
        return "oops"

    with pytest.raises(DBOSException, match="coroutine"):
        await async_ds.run_tx_step_async(None, sync_func)  # type: ignore


@pytest.mark.asyncio
async def test_async_ds_transaction_decorator_rejects_sync(
    async_ds: AsyncSQLAlchemyDatasource,
) -> None:
    """@ds.transaction on a sync function must raise at decoration time."""
    with pytest.raises(DBOSException, match="coroutine"):

        @async_ds.transaction  # type: ignore[arg-type]
        def bad() -> str:
            return "nope"


# ---------------------------------------------------------------------------
# Async OAOO tests (inside a DBOS workflow)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_ds_oaoo(dbos: DBOS, async_ds: AsyncSQLAlchemyDatasource) -> None:
    """run_tx_step_async inside a workflow records the result and replays on retry."""
    call_count = {"n": 0}

    async def expensive_step(value: str) -> str:
        call_count["n"] += 1
        session = async_ds.sql_session()
        await session.execute(text("SELECT 1"))
        return f"async:{value}"

    @DBOS.workflow()
    async def my_workflow(value: str) -> str:
        return await async_ds.run_tx_step_async(None, expensive_step, value)

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        result = await my_workflow("hello")
    assert result == "async:hello"
    assert call_count["n"] == 1

    with SetWorkflowID(wfid):
        result = await my_workflow("hello")
    assert result == "async:hello"
    assert call_count["n"] == 1


@pytest.mark.asyncio
async def test_async_ds_decorator_oaoo(
    dbos: DBOS, async_ds: AsyncSQLAlchemyDatasource
) -> None:
    """@ds.transaction inside a workflow records and replays the result."""
    call_count = {"n": 0}

    @async_ds.transaction
    async def decorated_step(value: str) -> str:
        call_count["n"] += 1
        return f"decorated:{value}"

    @DBOS.workflow()
    async def my_workflow(value: str) -> str:
        return await decorated_step(value)

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        result = await my_workflow("world")
    assert result == "decorated:world"
    assert call_count["n"] == 1

    with SetWorkflowID(wfid):
        result = await my_workflow("world")
    assert result == "decorated:world"
    assert call_count["n"] == 1


@pytest.mark.asyncio
async def test_async_ds_error_oaoo(
    dbos: DBOS, async_ds: AsyncSQLAlchemyDatasource
) -> None:
    """Async datasource step error is recorded and replayed without re-executing."""
    call_count = {"n": 0}

    async def failing_step() -> str:
        call_count["n"] += 1
        raise ValueError("async ds step failed")

    @DBOS.workflow()
    async def my_workflow() -> str:
        return await async_ds.run_tx_step_async(None, failing_step)

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        with pytest.raises(Exception, match="async ds step failed"):
            await my_workflow()
    assert call_count["n"] == 1

    with SetWorkflowID(wfid):
        with pytest.raises(Exception, match="async ds step failed"):
            await my_workflow()
    assert call_count["n"] == 1


@pytest.mark.asyncio
async def test_async_ds_retries_on_serialization_error(
    dbos: DBOS, async_ds: AsyncSQLAlchemyDatasource
) -> None:
    """A SQLSTATE 40001 raised inside the async txn body must be retried, not recorded."""
    if not isinstance(async_ds, PostgresAsyncDatasource):
        pytest.skip("manual serialization error is psycopg-specific")

    call_count = {"n": 0}
    max_retries = 3

    async def flaky_step() -> str:
        call_count["n"] += 1
        if call_count["n"] <= max_retries:
            raise OperationalError(
                "Serialization test error", {}, SerializationFailure()
            )
        return "success"

    @DBOS.workflow()
    async def my_workflow() -> str:
        return await async_ds.run_tx_step_async(None, flaky_step)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert await my_workflow() == "success"
    assert call_count["n"] == max_retries + 1

    async with async_ds.engine.connect() as conn:
        row = (
            await conn.execute(
                sa.select(
                    DatasourceSchema.datasource_outputs.c.output,
                    DatasourceSchema.datasource_outputs.c.error,
                ).where(DatasourceSchema.datasource_outputs.c.workflow_id == wfid)
            )
        ).first()
    assert row is not None
    assert row.error is None
    assert row.output is not None

    with SetWorkflowID(wfid):
        assert await my_workflow() == "success"
    assert call_count["n"] == max_retries + 1


@pytest.mark.asyncio
async def test_async_ds_retries_locked_precheck(
    dbos: DBOS, async_ds: AsyncSQLAlchemyDatasource, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 'database is locked' error on the async OAOO pre-check read must be retried, not terminal (#761)."""
    if not isinstance(async_ds, SqliteAsyncDatasource):
        pytest.skip("SQLite-specific: locked pre-check retry")

    body_calls = {"n": 0}

    async def step() -> str:
        body_calls["n"] += 1
        await async_ds.sql_session().execute(sa.text("SELECT 1"))
        return "ok"

    @DBOS.workflow()
    async def my_workflow() -> str:
        return await async_ds.run_tx_step_async({"name": "locked_precheck"}, step)

    real_check = async_ds._check_execution
    precheck_calls = {"n": 0}

    async def flaky_check(workflow_id: str, step_id: int) -> Any:
        precheck_calls["n"] += 1
        if precheck_calls["n"] == 1:
            raise OperationalError(
                "SELECT ... FROM datasource_outputs ...",
                {},
                sqlite3.OperationalError("database is locked"),
            )
        return await real_check(workflow_id, step_id)

    monkeypatch.setattr(async_ds, "_check_execution", flaky_check)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert await my_workflow() == "ok"
    assert precheck_calls["n"] >= 2  # pre-check was retried after the lock
    assert body_calls["n"] == 1  # body ran exactly once

    async with async_ds.engine.connect() as conn:
        row = (
            await conn.execute(
                sa.select(
                    DatasourceSchema.datasource_outputs.c.output,
                    DatasourceSchema.datasource_outputs.c.error,
                ).where(DatasourceSchema.datasource_outputs.c.workflow_id == wfid)
            )
        ).first()
    assert row is not None
    assert row.error is None
    assert row.output is not None


@pytest.mark.asyncio
async def test_async_ds_non_retryable_error_records_and_replays(
    dbos: DBOS, async_ds: AsyncSQLAlchemyDatasource
) -> None:
    """A DBAPIError that is not a serialization failure must be recorded, not retried."""
    if not isinstance(async_ds, PostgresAsyncDatasource):
        pytest.skip("manual DBAPIError is psycopg-specific")

    call_count = {"n": 0}

    async def failing_step() -> str:
        call_count["n"] += 1
        # Syntax error (42601) — DBAPIError but not retryable.
        await async_ds.sql_session().execute(sa.text("selct abc from c;"))
        return "unreached"

    @DBOS.workflow()
    async def my_workflow() -> str:
        return await async_ds.run_tx_step_async(None, failing_step)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            await my_workflow()
    assert call_count["n"] == 1

    # Replay must not re-execute the step; the recorded error is re-raised.
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            await my_workflow()
    assert call_count["n"] == 1


@pytest.mark.asyncio
async def test_async_ds_multiple_steps_in_workflow(
    dbos: DBOS, async_ds: AsyncSQLAlchemyDatasource
) -> None:
    """Multiple async datasource steps in one workflow each get their own step_id."""
    call_counts = {"a": 0, "b": 0}

    async def step_a() -> str:
        call_counts["a"] += 1
        return "A"

    async def step_b() -> str:
        call_counts["b"] += 1
        return "B"

    @DBOS.workflow()
    async def my_workflow() -> str:
        r1 = await async_ds.run_tx_step_async(None, step_a)
        r2 = await async_ds.run_tx_step_async(None, step_b)
        return r1 + r2

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        result = await my_workflow()
    assert result == "AB"

    with SetWorkflowID(wfid):
        result = await my_workflow()
    assert result == "AB"
    assert call_counts["a"] == 1
    assert call_counts["b"] == 1


@pytest.mark.asyncio
async def test_async_ds_writes_both_tables(
    dbos: DBOS, async_ds: AsyncSQLAlchemyDatasource
) -> None:
    """Async datasource step writes to both datasource_outputs and operation_outputs."""

    async def step_fn() -> str:
        return "world"

    @DBOS.workflow()
    async def my_workflow() -> str:
        return await async_ds.run_tx_step_async(None, step_fn)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert await my_workflow() == "world"

    await _async_check_both_tables(async_ds, dbos, wfid)


@pytest.mark.asyncio
async def test_async_ds_step_recorded_with_name(
    dbos: DBOS, async_ds: AsyncSQLAlchemyDatasource
) -> None:
    """Async datasource step appears in list_workflow_steps with the right name and output."""

    async def my_step() -> str:
        return "run_tx_result"

    @async_ds.transaction
    async def unnamed_step() -> str:
        return "unnamed_result"

    @async_ds.transaction(name="my_named_async_step")
    async def named_step() -> str:
        return "named_result"

    @DBOS.workflow()
    async def my_workflow() -> tuple[str, str, str]:
        r1 = await async_ds.run_tx_step_async(None, my_step)
        r2 = await unnamed_step()
        r3 = await named_step()
        return r1, r2, r3

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert await my_workflow() == (
            "run_tx_result",
            "unnamed_result",
            "named_result",
        )

    steps = await DBOS.list_workflow_steps_async(wfid)
    assert len(steps) == 3
    assert steps[0]["function_id"] == 1
    assert steps[0]["function_name"].endswith("my_step")
    assert steps[0]["output"] == "run_tx_result"
    assert steps[1]["function_id"] == 2
    assert steps[1]["function_name"].endswith("unnamed_step")
    assert steps[1]["output"] == "unnamed_result"
    assert steps[2]["function_id"] == 3
    assert steps[2]["function_name"] == "my_named_async_step"
    assert steps[2]["output"] == "named_result"

    async with async_ds.engine.connect() as conn:
        ds_rows = (
            await conn.execute(
                sa.select(
                    DatasourceSchema.datasource_outputs.c.step_id,
                    DatasourceSchema.datasource_outputs.c.output,
                    DatasourceSchema.datasource_outputs.c.error,
                    DatasourceSchema.datasource_outputs.c.serialization,
                )
                .where(DatasourceSchema.datasource_outputs.c.workflow_id == wfid)
                .order_by(DatasourceSchema.datasource_outputs.c.step_id)
            )
        ).fetchall()
    assert len(ds_rows) == 3
    for row in ds_rows:
        assert row.error is None
        assert row.serialization == "py_pickle"
    assert ds_rows[0].step_id == 1
    assert pickle.loads(base64.b64decode(ds_rows[0].output)) == "run_tx_result"
    assert ds_rows[1].step_id == 2
    assert pickle.loads(base64.b64decode(ds_rows[1].output)) == "unnamed_result"
    assert ds_rows[2].step_id == 3
    assert pickle.loads(base64.b64decode(ds_rows[2].output)) == "named_result"


@pytest.mark.asyncio
async def test_async_ds_recovers_from_sysdb_loss(
    dbos: DBOS, async_ds: AsyncSQLAlchemyDatasource
) -> None:
    """datasource_outputs is the source of truth when the sysdb step record is lost.

    Simulates the crash window: datasource_outputs was written atomically inside
    the user transaction, but the system crashed before operation_outputs was
    persisted. On re-run, the step result must be recovered from datasource_outputs
    without re-executing the step function.
    """
    call_count = {"n": 0}

    async def step_fn() -> str:
        call_count["n"] += 1
        return "recovered"

    @DBOS.workflow()
    async def my_workflow() -> str:
        return await async_ds.run_tx_step_async(None, step_fn)

    wfid = str(uuid.uuid4())

    # First run: writes both tables.
    with SetWorkflowID(wfid):
        assert await my_workflow() == "recovered"
    assert call_count["n"] == 1

    # Simulate crash: remove the workflow record from the system DB.
    # The CASCADE on operation_outputs.workflow_uuid wipes the step record too,
    # while datasource_outputs (in the app DB) is unaffected.
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.delete(SystemSchema.workflow_status).where(
                SystemSchema.workflow_status.c.workflow_uuid == wfid
            )
        )

    # Re-run: DBOS treats this as a new workflow (no workflow_status row).
    # run_step finds no operation_outputs entry, so it calls _body().
    # _body() finds the datasource_outputs row and replays — step_fn not called.
    with SetWorkflowID(wfid):
        assert await my_workflow() == "recovered"
    assert call_count["n"] == 1


@pytest.mark.asyncio
async def test_async_ds_conflicts_when_duplicate_execution_wins(
    dbos: DBOS, async_ds: AsyncSQLAlchemyDatasource, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A duplicate execution that loses the witness-row race stops with a workflow
    conflict instead of surfacing the primary-key IntegrityError (#812) or carrying on
    with the winner's result (#818). It replays only when the conflicting row may be
    its own, from an attempt whose commit was ambiguously lost."""
    async with async_ds.engine.begin() as conn:
        await conn.run_sync(_race_side_effects.create, checkfirst=True)
    call_count = {"n": 0}
    should_fail = {"v": False}
    winner_step: dict[str, Any] = {}

    async def step_fn() -> str:
        call_count["n"] += 1
        # Populated after the winning run: the live duplicate's step checkpoint, planted
        # mid-transaction because that is what tells a loser someone else owns the workflow.
        if winner_step:
            with dbos._sys_db.engine.begin() as conn:
                conn.execute(
                    sa.insert(SystemSchema.operation_outputs).values(**winner_step)
                )
        await async_ds.sql_session().execute(
            _race_side_effects.insert().values(tag=f"run-{call_count['n']}")
        )
        if should_fail["v"]:
            raise ValueError("loser's own failure")
        return f"result-{call_count['n']}"

    @DBOS.workflow()
    async def my_workflow() -> str:
        try:
            return await async_ds.run_tx_step_async(None, step_fn)
        except DBOSWorkflowConflictIDError:
            # A real duplicate parks here instead; caught to keep the assertion local.
            return "conflicted"

    wfid = str(uuid.uuid4())

    def forget_workflow() -> None:
        # Drop the sysdb checkpoint so run_step calls _body again instead of replaying.
        # A real concurrent duplicate keeps that row and ends in DBOSWorkflowConflictIDError instead.
        with dbos._sys_db.engine.begin() as conn:
            conn.execute(
                sa.delete(SystemSchema.workflow_status).where(
                    SystemSchema.workflow_status.c.workflow_uuid == wfid
                )
            )

    # Blind one pre-check, so a loser misses the winner's row as it does in the real race.
    real_check = async_ds._check_execution
    blind = {"next": False}

    async def blind_next_check(
        workflow_id: str, step_id: int
    ) -> Optional[RecordedResult]:
        if blind["next"]:
            blind["next"] = False
            return None
        return await real_check(workflow_id, step_id)

    monkeypatch.setattr(async_ds, "_check_execution", blind_next_check)

    # Count error-recording attempts, to pin that a lost result race never tries one.
    real_record_error = async_ds._record_error
    record_error_calls = {"n": 0}

    async def counting_record_error(
        workflow_id: str, step_id: int, error: str, serialization: Optional[str]
    ) -> None:
        record_error_calls["n"] += 1
        await real_record_error(workflow_id, step_id, error, serialization)

    monkeypatch.setattr(async_ds, "_record_error", counting_record_error)

    replays = {"n": 0}
    _count_replays(async_ds, replays, monkeypatch)

    # The winning execution: commits its app writes and its datasource_outputs row.
    with SetWorkflowID(wfid):
        assert await my_workflow() == "result-1"
    assert call_count["n"] == 1
    with dbos._sys_db.engine.connect() as sys_conn:
        winner_step.update(_winner_step_row(sys_conn, wfid, step_fn.__qualname__))

    # A loser whose body succeeds: the collision happens on the result-recording insert.
    forget_workflow()
    blind["next"] = True
    with SetWorkflowID(wfid):
        assert await my_workflow() == "conflicted"
    assert call_count["n"] == 2  # the loser did run its body
    assert (
        record_error_calls["n"] == 0
    )  # the recorded result won without an error write
    # Stopped at the lost race, not one statement later at the step checkpoint (#818).
    assert replays["n"] == 0, "the loser adopted the winner's result"

    # A loser whose body fails: the collision moves to the error-recording insert.
    forget_workflow()
    blind["next"] = True
    should_fail["v"] = True
    with SetWorkflowID(wfid):
        assert await my_workflow() == "conflicted"
    assert call_count["n"] == 3
    assert record_error_calls["n"] == 1
    assert replays["n"] == 0, "the loser adopted the winner's result"

    # The succeeding loser's writes were discarded and the winner's record still stands.
    async with async_ds.engine.connect() as conn:
        tags = [
            row.tag for row in (await conn.execute(sa.select(_race_side_effects.c.tag)))
        ]
        ds_row = (
            await conn.execute(
                sa.select(
                    DatasourceSchema.datasource_outputs.c.output,
                    DatasourceSchema.datasource_outputs.c.error,
                    DatasourceSchema.datasource_outputs.c.serialization,
                ).where(DatasourceSchema.datasource_outputs.c.workflow_id == wfid)
            )
        ).one()
    assert tags == ["run-1"]
    assert ds_row.error is None  # no loser error was ever recorded
    # The winner's output is still the one on record, unmodified by either loser.
    assert (
        deserialize_value(ds_row.output, ds_row.serialization, async_ds.serializer)
        == "result-1"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "winner_checkpoints, expected_emails, expected_replays", _LOST_RACE_CASES
)
async def test_async_ds_duplicate_execution_stops_at_the_lost_race(
    dbos: DBOS,
    async_ds: AsyncSQLAlchemyDatasource,
    monkeypatch: pytest.MonkeyPatch,
    winner_checkpoints: bool,
    expected_emails: int,
    expected_replays: int,
) -> None:
    """Async sibling of test_sync_ds_duplicate_execution_stops_at_the_lost_race (#818)."""
    reserve_calls = {"n": 0}
    emails_sent = {"n": 0}
    winner_step: dict[str, Any] = {}

    async def reserve() -> str:
        reserve_calls["n"] += 1
        # The winner checkpoints while the loser's transaction is open: the instant that
        # hands it the workflow, so recovery reaches the loser's park.
        if reserve_calls["n"] > 1 and winner_checkpoints:
            with dbos._sys_db.engine.begin() as conn:
                conn.execute(
                    sa.insert(SystemSchema.operation_outputs).values(**winner_step)
                )
        await async_ds.sql_session().execute(text("SELECT 1"))
        return "reserved"

    @DBOS.step()
    async def send_email() -> None:
        emails_sent["n"] += 1

    @DBOS.workflow()
    async def my_workflow() -> str:
        reserved = await async_ds.run_tx_step_async(None, reserve)
        await send_email()
        return reserved

    wfid = str(uuid.uuid4())

    # The winning execution, run to completion.
    with SetWorkflowID(wfid):
        assert await my_workflow() == "reserved"
    assert (reserve_calls["n"], emails_sent["n"]) == (1, 1)

    # Rewind to the instant the winner had committed its datasource_outputs row but no
    # step checkpoint yet: nothing to replay, and no outcome for a waiter to adopt.
    with dbos._sys_db.engine.begin() as conn:
        winner_output = conn.execute(
            sa.select(SystemSchema.workflow_status.c.output).where(
                SystemSchema.workflow_status.c.workflow_uuid == wfid
            )
        ).scalar_one()
        winner_step.update(_winner_step_row(conn, wfid, reserve.__qualname__))
        conn.execute(
            sa.delete(SystemSchema.operation_outputs).where(
                SystemSchema.operation_outputs.c.workflow_uuid == wfid
            )
        )
        conn.execute(
            sa.update(SystemSchema.workflow_status)
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
            .values(output=None)
        )

    # Blind one pre-check, so the loser misses the winner's row as it does in the real race.
    real_check = async_ds._check_execution
    blind = {"next": True}

    async def blind_next_check(
        workflow_id: str, step_id: int
    ) -> Optional[RecordedResult]:
        if blind["next"]:
            blind["next"] = False
            return None
        return await real_check(workflow_id, step_id)

    monkeypatch.setattr(async_ds, "_check_execution", blind_next_check)

    replays = {"n": 0}
    _count_replays(async_ds, replays, monkeypatch)

    # Dispatch the duplicate off the persisted row, exactly as recovery does.
    handle = reexecute_workflow_by_id(dbos, wfid)

    def loser_transaction_ran() -> None:
        assert reserve_calls["n"] >= 2

    def duplicate_left_the_workflow() -> None:
        # An async duplicate is dispatched onto the background loop and handed back a
        # polling handle, so its result says nothing about it: wait on the active set,
        # released either at the park or after the whole body.
        assert wfid not in dbos._active_workflows_set.activeList()

    try:
        await retry_until_success_async(
            loser_transaction_ran, interval=0.1, max_attempts=300
        )
        await retry_until_success_async(
            duplicate_left_the_workflow, interval=0.1, max_attempts=300
        )

        assert emails_sent["n"] == expected_emails
        # Parking at the step checkpoint one statement later is not parking at the lost
        # race: with a live winner the duplicate must never adopt its result at all.
        assert replays["n"] == expected_replays, "the duplicate replayed the winner"
        with dbos._sys_db.engine.connect() as conn:
            steps = _checkpointed_steps(conn, wfid)
        if winner_checkpoints:
            # Only the winner's row: the parked duplicate checkpointed nothing.
            assert steps == [reserve.__qualname__], "the parked duplicate checkpointed"
        else:
            # No winner left to park behind, so the duplicate ran the workflow out.
            assert len(steps) == 2, "the duplicate did not finish the workflow"
    finally:
        # Publish the winner's outcome, which is what a parked duplicate waits for.
        # In a finally: an assertion above must not strand a thread polling forever.
        # Unconditional: in the winner-gone case the duplicate has already written this
        # same outcome, and a failed assertion above must not leave a thread parked.
        with dbos._sys_db.engine.begin() as conn:
            conn.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
                .values(status=WorkflowStatusString.SUCCESS.value, output=winner_output)
            )

    assert await asyncio.to_thread(handle.get_result) == "reserved"


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_ack_message, expected_attempts", _LOST_ACK_ERRORS)
async def test_async_ds_replays_its_own_lost_commit(
    dbos: DBOS,
    async_ds: AsyncSQLAlchemyDatasource,
    monkeypatch: pytest.MonkeyPatch,
    lost_ack_message: str,
    expected_attempts: int,
) -> None:
    """Async sibling of test_sync_ds_replays_its_own_lost_commit."""
    if async_ds.engine.dialect.name != "postgresql":
        pytest.skip("only a Postgres-style connection error makes a commit ambiguous")
    async with async_ds.engine.begin() as conn:
        await conn.run_sync(_race_side_effects.create, checkfirst=True)
    call_count = {"n": 0}

    async def step_fn() -> str:
        call_count["n"] += 1
        await async_ds.sql_session().execute(
            _race_side_effects.insert().values(tag=f"run-{call_count['n']}")
        )
        return f"result-{call_count['n']}"

    lose_ack = {"next": True}
    real_sessionmaker = async_ds.sessionmaker

    class _LostAck:
        """Commits, then reports the connection dropped, as a lost ack does."""

        def __init__(self, transaction: Any) -> None:
            self._transaction = transaction

        async def __aenter__(self) -> Any:
            return await self._transaction.__aenter__()

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
            handled = await self._transaction.__aexit__(exc_type, exc, tb)
            if exc is None and lose_ack["next"]:
                lose_ack["next"] = False
                raise OperationalError(
                    "COMMIT", {}, psycopg.OperationalError(lost_ack_message)
                )
            return handled

    def flaky_sessionmaker() -> Any:
        session = real_sessionmaker()
        real_begin = session.begin
        # setattr: the proxy only has to satisfy the `with` protocol, not the type.
        setattr(session, "begin", lambda: _LostAck(real_begin()))
        return session

    monkeypatch.setattr(async_ds, "sessionmaker", flaky_sessionmaker)

    @DBOS.workflow()
    async def my_workflow() -> str:
        try:
            return await async_ds.run_tx_step_async(None, step_fn)
        except DBOSWorkflowConflictIDError:
            # Caught so a spurious park fails the assertion instead of hanging.
            return "conflicted"

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        # The committed attempt's own output, not a later attempt's.
        assert await my_workflow() == "result-1"
    assert call_count["n"] == expected_attempts

    async with async_ds.engine.connect() as conn:
        tags = [
            row.tag for row in (await conn.execute(sa.select(_race_side_effects.c.tag)))
        ]
        ds_row = (
            await conn.execute(
                sa.select(
                    DatasourceSchema.datasource_outputs.c.output,
                    DatasourceSchema.datasource_outputs.c.error,
                    DatasourceSchema.datasource_outputs.c.serialization,
                ).where(DatasourceSchema.datasource_outputs.c.workflow_id == wfid)
            )
        ).one()
    assert tags == ["run-1"]  # only the committed attempt's write survives
    assert ds_row.error is None
    assert (
        deserialize_value(ds_row.output, ds_row.serialization, async_ds.serializer)
        == "result-1"
    )
