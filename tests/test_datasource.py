"""Tests for SyncDatasource and AsyncDatasource."""

import asyncio
import base64
import inspect
import pickle
import sqlite3
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, AsyncGenerator, Generator, Optional, Union, cast

import psycopg
import pytest
import pytest_asyncio
import sqlalchemy as sa
from psycopg.errors import SerializationFailure
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

import dbos._workflow_commands as workflow_commands
from dbos import (
    DBOS,
    AsyncSQLAlchemyDatasource,
    DBOSConfig,
    SetWorkflowID,
    SQLAlchemyDatasource,
)
from dbos._context import get_local_dbos_context
from dbos._datasource import RecordedResult
from dbos._datasource_migration import (
    DATASOURCE_MIGRATIONS_TABLE,
    get_postgres_datasource_migrations,
)
from dbos._datasource_postgres import PostgresAsyncDatasource, PostgresSyncDatasource
from dbos._datasource_sqlite import SqliteAsyncDatasource, SqliteSyncDatasource
from dbos._error import (
    DBOSException,
    DBOSInitializationError,
    DBOSWorkflowConflictIDError,
)
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import deserialize_value
from dbos._sys_db import WorkflowStatusString
from tests.conftest import (
    ensure_user_database,
    keep_datasource_checkpoints,
    postgres_urls,
    reexecute_workflow_by_id,
    retry_until_success,
    retry_until_success_async,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# A winner that took ownership keeps the workflow, so the loser parks at the lost race
# without ever adopting its result; if the loser still owns it, it replays and carries on.
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


def _set_owner(dbos: DBOS, wfid: str, owner_xid: Optional[str]) -> None:
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.update(SystemSchema.workflow_status)
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
            .values(owner_xid=owner_xid)
        )


def _reclaim_ownership(dbos: DBOS) -> None:
    """Hand the workflow back to the running execution, so its outcome write lands."""
    ctx = get_local_dbos_context()
    assert ctx is not None and ctx.owner_xid is not None
    _set_owner(dbos, ctx.workflow_id, ctx.owner_xid)


def _checkpointed_steps(conn: Any, wfid: str) -> list[str]:
    return list(
        conn.execute(
            sa.select(SystemSchema.operation_outputs.c.function_name)
            .where(SystemSchema.operation_outputs.c.workflow_uuid == wfid)
            .order_by(SystemSchema.operation_outputs.c.function_id)
        ).scalars()
    )


# Application table used to prove a losing duplicate execution's writes are rolled back.
def _race_table(ds: Union[SQLAlchemyDatasource, AsyncSQLAlchemyDatasource]) -> sa.Table:
    return sa.Table(
        "race_side_effects",
        sa.MetaData(schema=ds.schema),
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


def _ds_rows(ds: SQLAlchemyDatasource, wfid: str) -> list[Any]:
    t = ds._outputs_table
    with ds.engine.connect() as conn:
        query = sa.select(t).where(t.c.workflow_id == wfid).order_by(t.c.step_id)
        return list(conn.execute(query).fetchall())


async def _ds_rows_async(ds: AsyncSQLAlchemyDatasource, wfid: str) -> list[Any]:
    t = ds._outputs_table
    async with ds.engine.connect() as conn:
        query = sa.select(t).where(t.c.workflow_id == wfid).order_by(t.c.step_id)
        return list((await conn.execute(query)).fetchall())


def _blind_prechecks(
    ds: Any, monkeypatch: pytest.MonkeyPatch, *, armed: bool
) -> dict[str, bool]:
    """While blind["next"] is set, the next OAOO pre-check misses any recorded row."""
    blind = {"next": armed}
    real_check = ds._check_execution

    if inspect.iscoroutinefunction(real_check):

        async def blind_next_check_async(
            workflow_id: str, step_id: int
        ) -> Optional[RecordedResult]:
            if blind["next"]:
                blind["next"] = False
                return None
            return cast(
                Optional[RecordedResult], await real_check(workflow_id, step_id)
            )

        monkeypatch.setattr(ds, "_check_execution", blind_next_check_async)
        return blind

    def blind_next_check(workflow_id: str, step_id: int) -> Optional[RecordedResult]:
        if blind["next"]:
            blind["next"] = False
            return None
        return cast(Optional[RecordedResult], real_check(workflow_id, step_id))

    monkeypatch.setattr(ds, "_check_execution", blind_next_check)
    return blind


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_real_cleanup = workflow_commands.delete_completed_datasource_checkpoints
_real_cleanup_async = workflow_commands.delete_completed_datasource_checkpoints_async


@pytest.fixture(autouse=True)
def skip_checkpoint_cleanup() -> Generator[None, None, None]:
    """Most tests here read checkpoints after completion."""
    with keep_datasource_checkpoints():
        yield


@pytest.fixture(params=["sqlite", "pg"])
def sync_ds(
    request: pytest.FixtureRequest, tmp_path: Any, cleanup_test_databases: None
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
        ensure_user_database()
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
    request: pytest.FixtureRequest, tmp_path: Any, cleanup_test_databases: None
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
        ensure_user_database()
        schema = f"ds_test_{uuid.uuid4().hex[:8]}"
        ds = await AsyncSQLAlchemyDatasource.create(
            url.replace("postgresql://", "postgresql+psycopg://"), schema=schema
        )
        yield ds
        async with ds.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await ds.engine.dispose()


# ---------------------------------------------------------------------------
# Sync basics
# ---------------------------------------------------------------------------


def test_sync_ds_runs_outside_workflow(sync_ds: SQLAlchemyDatasource) -> None:
    """Outside a workflow, every call style runs transactionally and checkpoints nothing."""
    from dbos._dbos import _get_or_create_dbos_registry

    assert sync_ds in _get_or_create_dbos_registry().datasources
    counter = {"n": 0}

    def increment(amount: int) -> int:
        sync_ds.sql_session().execute(text("SELECT 1"))
        counter["n"] += amount
        return counter["n"]

    decorated = sync_ds.transaction(increment)
    with_options = sync_ds.transaction(isolation_level="SERIALIZABLE", name="my_step")(
        increment
    )
    assert sync_ds.run_tx_step(None, increment, 5) == 5
    assert decorated(3) == 8
    assert with_options(2) == 10
    with sync_ds.engine.connect() as conn:
        count = sa.select(sa.func.count()).select_from(sync_ds._outputs_table)
        assert conn.execute(count).scalar() == 0


def test_sync_ds_rejects_misuse(tmp_path: Any) -> None:
    """Misuse fails fast: wrong or rebinding sessionmaker, coroutine functions, session outside a transaction."""
    with pytest.raises(DBOSException, match="sessionmaker"):
        SQLAlchemyDatasource.create(
            f"sqlite:///{tmp_path}/bad.sqlite",
            sessionmaker=async_sessionmaker(),  # type: ignore[arg-type]
        )
    # Per-mapper binds would route writes away from the checkpoint's engine.
    other = sa.create_engine("sqlite://")
    with pytest.raises(DBOSException, match="binds="):
        SQLAlchemyDatasource.create(
            f"sqlite:///{tmp_path}/bad.sqlite",
            sessionmaker=sessionmaker(binds={_ExpireBase: other}),
        )
    other.dispose()
    # An empty binds map routes nothing, so it is accepted.
    ds = SQLAlchemyDatasource.create(
        f"sqlite:///{tmp_path}/ds.sqlite", sessionmaker=sessionmaker(binds={})
    )
    try:
        with pytest.raises(AssertionError):
            ds.sql_session()

        async def coro() -> str:
            return "nope"

        with pytest.raises(DBOSException, match="coroutine"):
            ds.run_tx_step(None, coro)  # type: ignore
        with pytest.raises(DBOSException, match="coroutine"):
            ds.transaction(coro)
    finally:
        ds.engine.dispose()


def test_sync_ds_records_and_replays(sync_ds: SQLAlchemyDatasource, dbos: DBOS) -> None:
    """Every call style checkpoints to both databases in step order, and the datasource
    checkpoint alone replays the steps once the sysdb records are lost."""
    calls = {"plain": 0, "unnamed": 0, "named": 0}

    def plain(value: str) -> str:
        calls["plain"] += 1
        sync_ds.sql_session().execute(text("SELECT 1"))
        return f"plain:{value}"

    @sync_ds.transaction
    def unnamed(value: str) -> str:
        calls["unnamed"] += 1
        return f"unnamed:{value}"

    @sync_ds.transaction(name="my_named_step", isolation_level="SERIALIZABLE")
    def named(value: str) -> str:
        calls["named"] += 1
        return f"named:{value}"

    @DBOS.workflow()
    def my_workflow(value: str) -> tuple[str, str, str]:
        return sync_ds.run_tx_step(None, plain, value), unnamed(value), named(value)

    expected = ("plain:x", "unnamed:x", "named:x")
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert my_workflow("x") == expected

    steps = DBOS.list_workflow_steps(wfid)
    assert [s["function_id"] for s in steps] == [1, 2, 3]
    assert steps[0]["function_name"].endswith("plain")
    assert steps[1]["function_name"].endswith("unnamed")
    assert steps[2]["function_name"] == "my_named_step"
    assert tuple(s["output"] for s in steps) == expected
    rows = _ds_rows(sync_ds, wfid)
    assert [r.step_id for r in rows] == [1, 2, 3]
    assert all(r.error is None and r.serialization == "py_pickle" for r in rows)
    assert tuple(pickle.loads(base64.b64decode(r.output)) for r in rows) == expected

    # Simulate the crash window: the sysdb records are lost, the datasource rows are not.
    dbos._sys_db.delete_workflows([wfid])
    with SetWorkflowID(wfid):
        assert my_workflow("x") == expected
    assert calls == {"plain": 1, "unnamed": 1, "named": 1}


def test_sync_ds_records_and_replays_errors(
    sync_ds: SQLAlchemyDatasource, dbos: DBOS
) -> None:
    """A failing body's error, from Python or a non-retryable database error, is recorded
    once and replayed from the datasource checkpoint once the sysdb records are lost."""
    calls = {"n": 0}

    def fail(kind: str) -> str:
        calls["n"] += 1
        if kind == "sql":
            sync_ds.sql_session().execute(text("selct abc from c"))
        raise ValueError("ds step failed")

    @DBOS.workflow()
    def my_workflow(kind: str) -> str:
        return sync_ds.run_tx_step(None, fail, kind)

    cases = [("python", ValueError, "ds step failed"), ("sql", Exception, "selct")]
    for kind, exc_type, match in cases:
        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid), pytest.raises(exc_type, match=match):
            my_workflow(kind)
        [row] = _ds_rows(sync_ds, wfid)
        assert row.output is None and row.error is not None
        dbos._sys_db.delete_workflows([wfid])
        with SetWorkflowID(wfid), pytest.raises(exc_type, match=match):
            my_workflow(kind)
    assert calls["n"] == len(cases)


# ---------------------------------------------------------------------------
# Sync checkpointing, retries, and caller configuration
# ---------------------------------------------------------------------------


def test_sync_ds_retries_on_serialization_error(
    sync_ds: SQLAlchemyDatasource, dbos: DBOS
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
    [row] = _ds_rows(sync_ds, wfid)
    assert row.error is None and row.output is not None


def test_sync_ds_retries_locked_precheck(
    sync_ds: SQLAlchemyDatasource, dbos: DBOS, monkeypatch: pytest.MonkeyPatch
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
    [row] = _ds_rows(sync_ds, wfid)
    assert row.error is None and row.output is not None


def test_sync_ds_keeps_caller_engine_schema_translate_map(
    sync_ds: SQLAlchemyDatasource, config: DBOSConfig
) -> None:
    """A caller engine's schema_translate_map applies to the caller's tables but never
    moves DBOS's checkpoints, even when it maps DBOS's own schema elsewhere."""
    app_rows = sa.Table("app_rows", sa.MetaData(schema="app"), sa.Column("v", sa.Text))
    # The second key collides with DBOS's schema and points at one that doesn't exist.
    engine = sync_ds.engine.execution_options(
        schema_translate_map={"app": sync_ds.schema, sync_ds.schema: "missing_schema"}
    )
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    try:
        ds = SQLAlchemyDatasource.create(
            sync_ds.engine.url.render_as_string(hide_password=False),
            engine=engine,
            schema=sync_ds.schema,
        )
        assert ds.engine is engine
        app_rows.create(ds.engine)
        calls = {"n": 0}

        @ds.transaction
        def insert_row(v: str) -> str:
            calls["n"] += 1
            ds.sql_session().execute(app_rows.insert().values(v=v))
            return v

        @DBOS.workflow()
        def wf() -> str:
            return insert_row("x")

        DBOS.launch()
        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid):
            assert wf() == "x"
        # Replay from the checkpoint reads it back through the caller's engine.
        dbos._sys_db.delete_workflows([wfid])
        with SetWorkflowID(wfid):
            assert wf() == "x"
        assert calls["n"] == 1
        with ds.engine.connect() as conn:
            assert conn.execute(sa.select(app_rows.c.v)).scalars().all() == ["x"]
        # Checked on the fixture's untranslated engine: the checkpoint is where migrations put it.
        assert _ds_rows(sync_ds, wfid)[0].step_id == 1
        ds._delete_checkpoints(wfid, 1)
        assert _ds_rows(sync_ds, wfid) == []
    finally:
        DBOS.destroy(destroy_registry=True)


class _TenantSession(Session):
    pass


@event.listens_for(_TenantSession, "after_begin")
def _set_tenant(session: Session, transaction: Any, connection: sa.Connection) -> None:
    session.info["began"] = session.info.get("began", 0) + 1
    if connection.dialect.name == "postgresql":
        connection.execute(
            sa.text("SELECT set_config('app.tenant', :t, true)"),
            {"t": session.info["tenant"]},
        )


def test_sync_ds_custom_sessionmaker(
    sync_ds: SQLAlchemyDatasource, config: DBOSConfig
) -> None:
    """A caller sessionmaker's class, options, and hooks are used; its bind is not."""
    # Bound to an unrelated database: checkpoints only land if DBOS overrides the bind.
    foreign = sa.create_engine("sqlite://")
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    try:
        ds = SQLAlchemyDatasource.create(
            sync_ds.engine.url.render_as_string(hide_password=False),
            engine=sync_ds.engine,
            schema=sync_ds.schema,
            sessionmaker=sessionmaker(
                bind=foreign, class_=_TenantSession, info={"tenant": "t1"}
            ),
        )
        calls = {"n": 0}

        @ds.transaction
        def read_tenant() -> str:
            calls["n"] += 1
            session = ds.sql_session()
            assert isinstance(session, _TenantSession)
            assert session.info["began"] == 1
            if sync_ds.engine.dialect.name == "postgresql":
                return str(
                    session.execute(
                        sa.text("SELECT current_setting('app.tenant', true)")
                    ).scalar()
                )
            return str(session.info["tenant"])

        @DBOS.workflow()
        def wf() -> str:
            return read_tenant()

        DBOS.launch()
        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid):
            assert wf() == "t1"
        # Simulate losing the step's sysdb record: only the datasource checkpoint remains.
        dbos._sys_db.delete_workflows([wfid])
        with SetWorkflowID(wfid):
            assert wf() == "t1"
        assert calls["n"] == 1  # replayed from the checkpoint, not re-run
        with ds.engine.connect() as conn:
            assert conn.execute(
                sa.select(ds._outputs_table.c.step_id).where(
                    ds._outputs_table.c.workflow_id == wfid
                )
            ).scalars().all() == [1]
    finally:
        DBOS.destroy(destroy_registry=True)
        foreign.dispose()


class _ExpireBase(DeclarativeBase):
    pass


# Module-level so the returned object pickles; unqualified, created and dropped per test.
class _ExpireItem(_ExpireBase):
    __tablename__ = "ds_expire_items"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]


def test_sync_ds_returns_loaded_orm_objects(
    sync_ds: SQLAlchemyDatasource, dbos: DBOS
) -> None:
    """ORM objects returned by a transaction stay loaded after commit, and replay with
    their generated key from both the workflow output and the datasource checkpoint."""
    _ExpireBase.metadata.create_all(sync_ds.engine)
    try:

        @sync_ds.transaction
        def add_item(name: str) -> _ExpireItem:
            # No explicit flush: the key is generated only when DBOS flushes.
            item = _ExpireItem(name=name)
            sync_ds.sql_session().add(item)
            return item

        @DBOS.workflow()
        def wf(name: str) -> _ExpireItem:
            item = add_item(name)
            assert item.name == name
            return item

        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid):
            first = wf("a")
        assert first.id is not None and first.name == "a"
        with SetWorkflowID(wfid):
            replayed = wf("a")
        assert (replayed.id, replayed.name) == (first.id, first.name)
        # Lose the sysdb records, so the step replays from the datasource checkpoint.
        dbos._sys_db.delete_workflows([wfid])
        with SetWorkflowID(wfid):
            recovered = wf("a")
        assert (recovered.id, recovered.name) == (first.id, first.name)
    finally:
        _ExpireBase.metadata.drop_all(sync_ds.engine)


def test_sync_ds_conflicts_when_duplicate_execution_wins(
    sync_ds: SQLAlchemyDatasource, dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A duplicate execution that loses the witness-row race stops with a workflow
    conflict instead of surfacing the primary-key IntegrityError (#812) or carrying on
    with the winner's result (#818) once it no longer owns the workflow."""
    race = _race_table(sync_ds)
    race.create(sync_ds.engine, checkfirst=True)
    call_count = {"n": 0}
    should_fail = {"v": False}
    steal = {"v": False}

    def step_fn() -> str:
        call_count["n"] += 1
        # Set after the winning run: another execution takes the workflow mid-transaction.
        if steal["v"]:
            _set_owner(dbos, wfid, "another-execution")
        sync_ds.sql_session().execute(
            race.insert().values(tag=f"run-{call_count['n']}")
        )
        if should_fail["v"]:
            raise ValueError("loser's own failure")
        return f"result-{call_count['n']}"

    @DBOS.workflow()
    def my_workflow() -> str:
        try:
            return sync_ds.run_tx_step(None, step_fn)
        except DBOSWorkflowConflictIDError:
            # A real duplicate parks here instead; caught and reclaimed to keep the assertion local.
            _reclaim_ownership(dbos)
            return "conflicted"

    wfid = str(uuid.uuid4())

    def forget_workflow() -> None:
        # Drop the sysdb checkpoint so run_step calls _body again instead of replaying.
        # Via delete_workflows: operation_outputs has no foreign key to cascade now.
        dbos._sys_db.delete_workflows([wfid])

    # Blind one pre-check, so a loser misses the winner's row as it does in the real race.
    blind = _blind_prechecks(sync_ds, monkeypatch, armed=False)

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
    steal["v"] = True

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
        tags = [row.tag for row in conn.execute(sa.select(race.c.tag))]
        ds_row = conn.execute(
            sa.select(
                sync_ds._outputs_table.c.output,
                sync_ds._outputs_table.c.error,
                sync_ds._outputs_table.c.serialization,
            ).where(sync_ds._outputs_table.c.workflow_id == wfid)
        ).one()
    assert tags == ["run-1"]
    assert ds_row.error is None  # no loser error was ever recorded
    # The winner's output is still the one on record, unmodified by either loser.
    assert (
        deserialize_value(ds_row.output, ds_row.serialization, sync_ds.serializer)
        == "result-1"
    )


def test_sync_ds_rolls_back_once_ownership_moves(
    sync_ds: SQLAlchemyDatasource, dbos: DBOS
) -> None:
    """An execution that loses the workflow mid-transaction rolls back rather than
    commit, so a stale execution cannot apply a step the new owner also runs."""
    race = _race_table(sync_ds)
    race.create(sync_ds.engine, checkfirst=True)

    def step_fn() -> str:
        sync_ds.sql_session().execute(race.insert().values(tag="stale"))
        _set_owner(dbos, wfid, "another-execution")
        return "stale"

    @DBOS.workflow()
    def my_workflow() -> str:
        try:
            return sync_ds.run_tx_step(None, step_fn)
        except DBOSWorkflowConflictIDError:
            # A real duplicate parks here instead; caught and reclaimed to keep the assertion local.
            _reclaim_ownership(dbos)
            return "conflicted"

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert my_workflow() == "conflicted"

    with sync_ds.engine.connect() as conn:
        tags = list(conn.execute(sa.select(race.c.tag)).scalars())
        ds_rows = conn.execute(
            sa.select(sync_ds._outputs_table.c.error).where(
                sync_ds._outputs_table.c.workflow_id == wfid
            )
        ).all()
    assert tags == []
    assert ds_rows == [], "the stale execution left a checkpoint or an error row"
    with dbos._sys_db.engine.connect() as conn:
        assert _checkpointed_steps(conn, wfid) == []


def test_sync_ds_completion_clears_checkpoints(
    sync_ds: SQLAlchemyDatasource, dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        workflow_commands, "delete_completed_datasource_checkpoints", _real_cleanup
    )

    def step_fn() -> str:
        return "done"

    def checkpoints(wfid: str) -> list[int]:
        with sync_ds.engine.connect() as conn:
            return list(
                conn.execute(
                    sa.select(sync_ds._outputs_table.c.step_id).where(
                        sync_ds._outputs_table.c.workflow_id == wfid
                    )
                ).scalars()
            )

    before_cleanup: list[list[int]] = []

    @DBOS.workflow()
    def my_workflow() -> str:
        result = sync_ds.run_tx_step(None, step_fn)
        before_cleanup.append(checkpoints(DBOS.workflow_id or ""))
        return result

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert my_workflow() == "done"
    assert before_cleanup == [[1]]
    assert checkpoints(wfid) == []


# Whether a lost acknowledgement is retriable decides how the row is met, not whose it is.
_LOST_ACK_ERRORS = [
    pytest.param("server closed the connection unexpectedly", 2, id="retriable"),
    pytest.param("consuming input failed: EOF detected", 1, id="non-retriable"),
]


@pytest.mark.parametrize("lost_ack_message, expected_attempts", _LOST_ACK_ERRORS)
def test_sync_ds_replays_its_own_lost_commit(
    sync_ds: SQLAlchemyDatasource,
    dbos: DBOS,
    monkeypatch: pytest.MonkeyPatch,
    lost_ack_message: str,
    expected_attempts: int,
) -> None:
    """A commit that lands and then loses its acknowledgement leaves a row this very
    execution wrote, so it replays that row instead of parking as a duplicate.

    A retriable error meets the row on the retry's result insert; a non-retriable one
    meets it on the error insert. Neither is a duplicate, so both replay."""
    race = _race_table(sync_ds)
    if sync_ds.engine.dialect.name != "postgresql":
        pytest.skip("only a Postgres-style connection error makes a commit ambiguous")
    race.create(sync_ds.engine, checkfirst=True)
    call_count = {"n": 0}

    def step_fn() -> str:
        call_count["n"] += 1
        sync_ds.sql_session().execute(
            race.insert().values(tag=f"run-{call_count['n']}")
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

    def flaky_sessionmaker(**kw: Any) -> Any:
        session = real_sessionmaker(**kw)
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
        tags = [row.tag for row in conn.execute(sa.select(race.c.tag))]
        ds_row = conn.execute(
            sa.select(
                sync_ds._outputs_table.c.output,
                sync_ds._outputs_table.c.error,
                sync_ds._outputs_table.c.serialization,
            ).where(sync_ds._outputs_table.c.workflow_id == wfid)
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
    sync_ds: SQLAlchemyDatasource,
    dbos: DBOS,
    monkeypatch: pytest.MonkeyPatch,
    winner_checkpoints: bool,
    expected_emails: int,
    expected_replays: int,
) -> None:
    """A duplicate execution that loses the datasource race parks where an ordinary
    step's loser parks, instead of replaying the winner's result and going on to run
    the workflow's next step with it (#818) -- but only once the winner owns the workflow.
    """
    reserve_calls = {"n": 0}
    emails_sent = {"n": 0}
    winner_step: dict[str, Any] = {}

    def reserve() -> str:
        reserve_calls["n"] += 1
        # The winner takes the workflow and checkpoints while the loser's transaction is open.
        if reserve_calls["n"] > 1 and winner_checkpoints:
            _set_owner(dbos, wfid, "another-execution")
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
            sa.select(SystemSchema.workflow_output.c.output).where(
                SystemSchema.workflow_output.c.workflow_uuid == wfid
            )
        ).scalar_one()
        winner_step.update(_winner_step_row(conn, wfid, reserve.__qualname__))
        conn.execute(
            sa.delete(SystemSchema.operation_outputs).where(
                SystemSchema.operation_outputs.c.workflow_uuid == wfid
            )
        )
        # Clearing the recorded outcome means both places it can live: the
        # payload table, and the legacy column a dual-writing deployment fills.
        conn.execute(
            sa.delete(SystemSchema.workflow_output).where(
                SystemSchema.workflow_output.c.workflow_uuid == wfid
            )
        )
        conn.execute(
            sa.update(SystemSchema.workflow_status)
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
            .values(output=None)
        )

    # Blind one pre-check, so the loser misses the winner's row as it does in the real race.
    blind = _blind_prechecks(sync_ds, monkeypatch, armed=True)

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
# Async basics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_ds_runs_outside_workflow(
    async_ds: AsyncSQLAlchemyDatasource,
) -> None:
    """Outside a workflow, every call style runs transactionally and checkpoints nothing."""
    counter = {"n": 0}

    async def increment(amount: int) -> int:
        await async_ds.sql_session().execute(text("SELECT 1"))
        counter["n"] += amount
        return counter["n"]

    decorated = async_ds.transaction(increment)
    with_options = async_ds.transaction(isolation_level="SERIALIZABLE", name="my_step")(
        increment
    )
    assert await async_ds.run_tx_step_async(None, increment, 5) == 5
    assert await decorated(3) == 8
    assert await with_options(2) == 10
    async with async_ds.engine.connect() as conn:
        count = sa.select(sa.func.count()).select_from(async_ds._outputs_table)
        assert (await conn.execute(count)).scalar() == 0


@pytest.mark.asyncio
async def test_async_ds_rejects_misuse(tmp_path: Any) -> None:
    """Misuse fails fast: wrong or rebinding sessionmaker, sync functions, session outside a transaction."""
    with pytest.raises(DBOSException, match="sessionmaker"):
        await AsyncSQLAlchemyDatasource.create(
            f"sqlite+aiosqlite:///{tmp_path}/bad.sqlite",
            sessionmaker=sessionmaker(),  # type: ignore[arg-type]
        )
    # Per-mapper binds would route writes away from the checkpoint's engine.
    other = create_async_engine("sqlite+aiosqlite://")
    with pytest.raises(DBOSException, match="binds="):
        await AsyncSQLAlchemyDatasource.create(
            f"sqlite+aiosqlite:///{tmp_path}/bad.sqlite",
            sessionmaker=async_sessionmaker(binds={_ExpireBase: other}),
        )
    await other.dispose()
    # An empty binds map routes nothing, so it is accepted.
    ds = await AsyncSQLAlchemyDatasource.create(
        f"sqlite+aiosqlite:///{tmp_path}/ds.sqlite",
        sessionmaker=async_sessionmaker(binds={}),
    )
    try:
        with pytest.raises(AssertionError):
            ds.sql_session()

        def sync_func() -> str:
            return "nope"

        with pytest.raises(DBOSException, match="coroutine"):
            await ds.run_tx_step_async(None, sync_func)  # type: ignore
        with pytest.raises(DBOSException, match="coroutine"):
            ds.transaction(sync_func)  # type: ignore
    finally:
        await ds.engine.dispose()


@pytest.mark.asyncio
async def test_async_ds_records_and_replays(
    async_ds: AsyncSQLAlchemyDatasource, dbos: DBOS
) -> None:
    """Every call style checkpoints to both databases in step order, and the datasource
    checkpoint alone replays the steps once the sysdb records are lost."""
    calls = {"plain": 0, "unnamed": 0, "named": 0}

    async def plain(value: str) -> str:
        calls["plain"] += 1
        await async_ds.sql_session().execute(text("SELECT 1"))
        return f"plain:{value}"

    @async_ds.transaction
    async def unnamed(value: str) -> str:
        calls["unnamed"] += 1
        return f"unnamed:{value}"

    @async_ds.transaction(name="my_named_step", isolation_level="SERIALIZABLE")
    async def named(value: str) -> str:
        calls["named"] += 1
        return f"named:{value}"

    @DBOS.workflow()
    async def my_workflow(value: str) -> tuple[str, str, str]:
        return (
            await async_ds.run_tx_step_async(None, plain, value),
            await unnamed(value),
            await named(value),
        )

    expected = ("plain:x", "unnamed:x", "named:x")
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert await my_workflow("x") == expected

    steps = await DBOS.list_workflow_steps_async(wfid)
    assert [s["function_id"] for s in steps] == [1, 2, 3]
    assert steps[0]["function_name"].endswith("plain")
    assert steps[1]["function_name"].endswith("unnamed")
    assert steps[2]["function_name"] == "my_named_step"
    assert tuple(s["output"] for s in steps) == expected
    rows = await _ds_rows_async(async_ds, wfid)
    assert [r.step_id for r in rows] == [1, 2, 3]
    assert all(r.error is None and r.serialization == "py_pickle" for r in rows)
    assert tuple(pickle.loads(base64.b64decode(r.output)) for r in rows) == expected

    # Simulate the crash window: the sysdb records are lost, the datasource rows are not.
    dbos._sys_db.delete_workflows([wfid])
    with SetWorkflowID(wfid):
        assert await my_workflow("x") == expected
    assert calls == {"plain": 1, "unnamed": 1, "named": 1}


@pytest.mark.asyncio
async def test_async_ds_records_and_replays_errors(
    async_ds: AsyncSQLAlchemyDatasource, dbos: DBOS
) -> None:
    """A failing body's error, from Python or a non-retryable database error, is recorded
    once and replayed from the datasource checkpoint once the sysdb records are lost."""
    calls = {"n": 0}

    async def fail(kind: str) -> str:
        calls["n"] += 1
        if kind == "sql":
            await async_ds.sql_session().execute(text("selct abc from c"))
        raise ValueError("ds step failed")

    @DBOS.workflow()
    async def my_workflow(kind: str) -> str:
        return await async_ds.run_tx_step_async(None, fail, kind)

    cases = [("python", ValueError, "ds step failed"), ("sql", Exception, "selct")]
    for kind, exc_type, match in cases:
        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid), pytest.raises(exc_type, match=match):
            await my_workflow(kind)
        [row] = await _ds_rows_async(async_ds, wfid)
        assert row.output is None and row.error is not None
        dbos._sys_db.delete_workflows([wfid])
        with SetWorkflowID(wfid), pytest.raises(exc_type, match=match):
            await my_workflow(kind)
    assert calls["n"] == len(cases)


# ---------------------------------------------------------------------------
# Async checkpointing, retries, and caller configuration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_ds_retries_on_serialization_error(
    async_ds: AsyncSQLAlchemyDatasource, dbos: DBOS
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

    [row] = await _ds_rows_async(async_ds, wfid)
    assert row.error is None and row.output is not None


@pytest.mark.asyncio
async def test_async_ds_retries_locked_precheck(
    async_ds: AsyncSQLAlchemyDatasource, dbos: DBOS, monkeypatch: pytest.MonkeyPatch
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

    [row] = await _ds_rows_async(async_ds, wfid)
    assert row.error is None and row.output is not None


class _AsyncTenantSession(AsyncSession):
    sync_session_class = _TenantSession


@pytest.mark.asyncio
async def test_async_ds_custom_sessionmaker(
    async_ds: AsyncSQLAlchemyDatasource, config: DBOSConfig
) -> None:
    """A caller async_sessionmaker's class, options, and hooks are used; its bind is not."""
    # Bound to an unrelated database: checkpoints only land if DBOS overrides the bind.
    foreign = create_async_engine("sqlite+aiosqlite://")
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    try:
        ds = await AsyncSQLAlchemyDatasource.create(
            async_ds.engine.url.render_as_string(hide_password=False),
            engine=async_ds.engine,
            schema=async_ds.schema,
            sessionmaker=async_sessionmaker(
                bind=foreign, class_=_AsyncTenantSession, info={"tenant": "t1"}
            ),
        )
        calls = {"n": 0}

        @ds.transaction
        async def read_tenant() -> str:
            calls["n"] += 1
            session = ds.sql_session()
            assert isinstance(session, _AsyncTenantSession)
            assert session.info["began"] == 1
            if async_ds.engine.dialect.name == "postgresql":
                return str(
                    (
                        await session.execute(
                            sa.text("SELECT current_setting('app.tenant', true)")
                        )
                    ).scalar()
                )
            return str(session.info["tenant"])

        @DBOS.workflow()
        async def wf() -> str:
            return await read_tenant()

        DBOS.launch()
        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid):
            assert await wf() == "t1"
        # Simulate losing the step's sysdb record: only the datasource checkpoint remains.
        dbos._sys_db.delete_workflows([wfid])
        with SetWorkflowID(wfid):
            assert await wf() == "t1"
        assert calls["n"] == 1  # replayed from the checkpoint, not re-run
        async with ds.engine.connect() as conn:
            result = await conn.execute(
                sa.select(ds._outputs_table.c.step_id).where(
                    ds._outputs_table.c.workflow_id == wfid
                )
            )
            assert result.scalars().all() == [1]
    finally:
        DBOS.destroy(destroy_registry=True)
        await foreign.dispose()


@pytest.mark.asyncio
async def test_async_ds_returns_loaded_orm_objects(
    async_ds: AsyncSQLAlchemyDatasource, dbos: DBOS
) -> None:
    """ORM objects returned by a transaction stay loaded after commit, and replay with
    their generated key from both the workflow output and the datasource checkpoint."""
    async with async_ds.engine.begin() as conn:
        await conn.run_sync(_ExpireBase.metadata.create_all)
    try:

        @async_ds.transaction
        async def add_item(name: str) -> _ExpireItem:
            # No explicit flush: the key is generated only when DBOS flushes.
            item = _ExpireItem(name=name)
            async_ds.sql_session().add(item)
            return item

        @DBOS.workflow()
        async def wf(name: str) -> _ExpireItem:
            item = await add_item(name)
            assert item.name == name
            return item

        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid):
            first = await wf("a")
        assert first.id is not None and first.name == "a"
        with SetWorkflowID(wfid):
            replayed = await wf("a")
        assert (replayed.id, replayed.name) == (first.id, first.name)
        # Lose the sysdb records, so the step replays from the datasource checkpoint.
        dbos._sys_db.delete_workflows([wfid])
        with SetWorkflowID(wfid):
            recovered = await wf("a")
        assert (recovered.id, recovered.name) == (first.id, first.name)
    finally:
        async with async_ds.engine.begin() as conn:
            await conn.run_sync(_ExpireBase.metadata.drop_all)


@pytest.mark.asyncio
async def test_async_ds_conflicts_when_duplicate_execution_wins(
    async_ds: AsyncSQLAlchemyDatasource, dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A duplicate execution that loses the witness-row race stops with a workflow
    conflict instead of surfacing the primary-key IntegrityError (#812) or carrying on
    with the winner's result (#818) once it no longer owns the workflow."""
    race = _race_table(async_ds)
    async with async_ds.engine.begin() as conn:
        await conn.run_sync(race.create, checkfirst=True)
    call_count = {"n": 0}
    should_fail = {"v": False}
    steal = {"v": False}

    async def step_fn() -> str:
        call_count["n"] += 1
        # Set after the winning run: another execution takes the workflow mid-transaction.
        if steal["v"]:
            _set_owner(dbos, wfid, "another-execution")
        await async_ds.sql_session().execute(
            race.insert().values(tag=f"run-{call_count['n']}")
        )
        if should_fail["v"]:
            raise ValueError("loser's own failure")
        return f"result-{call_count['n']}"

    @DBOS.workflow()
    async def my_workflow() -> str:
        try:
            return await async_ds.run_tx_step_async(None, step_fn)
        except DBOSWorkflowConflictIDError:
            # A real duplicate parks here instead; caught and reclaimed to keep the assertion local.
            _reclaim_ownership(dbos)
            return "conflicted"

    wfid = str(uuid.uuid4())

    def forget_workflow() -> None:
        # Drop the sysdb checkpoint so run_step calls _body again instead of replaying.
        # Via delete_workflows: operation_outputs has no foreign key to cascade now.
        dbos._sys_db.delete_workflows([wfid])

    # Blind one pre-check, so a loser misses the winner's row as it does in the real race.
    blind = _blind_prechecks(async_ds, monkeypatch, armed=False)

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
    steal["v"] = True

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
        tags = [row.tag for row in (await conn.execute(sa.select(race.c.tag)))]
        ds_row = (
            await conn.execute(
                sa.select(
                    async_ds._outputs_table.c.output,
                    async_ds._outputs_table.c.error,
                    async_ds._outputs_table.c.serialization,
                ).where(async_ds._outputs_table.c.workflow_id == wfid)
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
async def test_async_ds_completion_clears_checkpoints(
    async_ds: AsyncSQLAlchemyDatasource, dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DBOS launched outside this test's loop, so the delete must run on the
    workflow's loop, which holds the datasource's connections, not DBOS's own loop."""
    monkeypatch.setattr(
        workflow_commands,
        "delete_completed_datasource_checkpoints_async",
        _real_cleanup_async,
    )
    delete_loops: list[asyncio.AbstractEventLoop] = []
    real_delete = async_ds._delete_checkpoints_if_owner

    async def recording_delete(workflow_id: str, owner_xid: str) -> bool:
        delete_loops.append(asyncio.get_running_loop())
        return await real_delete(workflow_id, owner_xid)

    monkeypatch.setattr(async_ds, "_delete_checkpoints_if_owner", recording_delete)

    async def step_fn() -> str:
        return "done"

    @DBOS.workflow()
    async def my_workflow() -> str:
        return await async_ds.run_tx_step_async(None, step_fn)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert await my_workflow() == "done"
    assert dbos._background_event_loop.target_loop() is not asyncio.get_running_loop()
    assert delete_loops == [asyncio.get_running_loop()]
    async with async_ds.engine.connect() as conn:
        rows = (
            await conn.execute(
                sa.select(async_ds._outputs_table.c.step_id).where(
                    async_ds._outputs_table.c.workflow_id == wfid
                )
            )
        ).all()
    assert rows == []


@pytest.mark.asyncio
async def test_async_ds_rolls_back_once_ownership_moves(
    async_ds: AsyncSQLAlchemyDatasource, dbos: DBOS
) -> None:
    """An execution that loses the workflow mid-transaction rolls back rather than
    commit, so a stale execution cannot apply a step the new owner also runs."""
    race = _race_table(async_ds)
    async with async_ds.engine.begin() as conn:
        await conn.run_sync(race.create, checkfirst=True)

    async def step_fn() -> str:
        await async_ds.sql_session().execute(race.insert().values(tag="stale"))
        _set_owner(dbos, wfid, "another-execution")
        return "stale"

    @DBOS.workflow()
    async def my_workflow() -> str:
        try:
            return await async_ds.run_tx_step_async(None, step_fn)
        except DBOSWorkflowConflictIDError:
            # A real duplicate parks here instead; caught and reclaimed to keep the assertion local.
            _reclaim_ownership(dbos)
            return "conflicted"

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert await my_workflow() == "conflicted"

    async with async_ds.engine.connect() as conn:
        tags = list((await conn.execute(sa.select(race.c.tag))).scalars())
        ds_rows = (
            await conn.execute(
                sa.select(async_ds._outputs_table.c.error).where(
                    async_ds._outputs_table.c.workflow_id == wfid
                )
            )
        ).all()
    assert tags == []
    assert ds_rows == [], "the stale execution left a checkpoint or an error row"
    with dbos._sys_db.engine.connect() as sys_conn:
        assert _checkpointed_steps(sys_conn, wfid) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "winner_checkpoints, expected_emails, expected_replays", _LOST_RACE_CASES
)
async def test_async_ds_duplicate_execution_stops_at_the_lost_race(
    async_ds: AsyncSQLAlchemyDatasource,
    dbos: DBOS,
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
        # The winner takes the workflow and checkpoints while the loser's transaction is open.
        if reserve_calls["n"] > 1 and winner_checkpoints:
            _set_owner(dbos, wfid, "another-execution")
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
            sa.select(SystemSchema.workflow_output.c.output).where(
                SystemSchema.workflow_output.c.workflow_uuid == wfid
            )
        ).scalar_one()
        winner_step.update(_winner_step_row(conn, wfid, reserve.__qualname__))
        conn.execute(
            sa.delete(SystemSchema.operation_outputs).where(
                SystemSchema.operation_outputs.c.workflow_uuid == wfid
            )
        )
        # Clearing the recorded outcome means both places it can live: the
        # payload table, and the legacy column a dual-writing deployment fills.
        conn.execute(
            sa.delete(SystemSchema.workflow_output).where(
                SystemSchema.workflow_output.c.workflow_uuid == wfid
            )
        )
        conn.execute(
            sa.update(SystemSchema.workflow_status)
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
            .values(output=None)
        )

    # Blind one pre-check, so the loser misses the winner's row as it does in the real race.
    blind = _blind_prechecks(async_ds, monkeypatch, armed=True)

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
    async_ds: AsyncSQLAlchemyDatasource,
    dbos: DBOS,
    monkeypatch: pytest.MonkeyPatch,
    lost_ack_message: str,
    expected_attempts: int,
) -> None:
    """Async sibling of test_sync_ds_replays_its_own_lost_commit."""
    race = _race_table(async_ds)
    if async_ds.engine.dialect.name != "postgresql":
        pytest.skip("only a Postgres-style connection error makes a commit ambiguous")
    async with async_ds.engine.begin() as conn:
        await conn.run_sync(race.create, checkfirst=True)
    call_count = {"n": 0}

    async def step_fn() -> str:
        call_count["n"] += 1
        await async_ds.sql_session().execute(
            race.insert().values(tag=f"run-{call_count['n']}")
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

    def flaky_sessionmaker(**kw: Any) -> Any:
        session = real_sessionmaker(**kw)
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
        tags = [row.tag for row in (await conn.execute(sa.select(race.c.tag)))]
        ds_row = (
            await conn.execute(
                sa.select(
                    async_ds._outputs_table.c.output,
                    async_ds._outputs_table.c.error,
                    async_ds._outputs_table.c.serialization,
                ).where(async_ds._outputs_table.c.workflow_id == wfid)
            )
        ).one()
    assert tags == ["run-1"]  # only the committed attempt's write survives
    assert ds_row.error is None
    assert (
        deserialize_value(ds_row.output, ds_row.serialization, async_ds.serializer)
        == "result-1"
    )


# ---------------------------------------------------------------------------
# Launch ordering and checkpoint deletion
# ---------------------------------------------------------------------------


def _checkpoint_step_ids(ds: SQLAlchemyDatasource, wfid: str) -> list[int]:
    with ds.engine.begin() as conn:
        return sorted(
            conn.execute(
                sa.select(ds._outputs_table.c.step_id).where(
                    ds._outputs_table.c.workflow_id == wfid
                )
            ).scalars()
        )


def test_datasource_must_be_created_before_launch(dbos: DBOS, tmp_path: Any) -> None:
    with pytest.raises(DBOSException, match="before DBOS.launch"):
        SQLAlchemyDatasource.create(f"sqlite:///{tmp_path}/late.sqlite")


def test_sync_ds_delete_checkpoints(sync_ds: SQLAlchemyDatasource, dbos: DBOS) -> None:
    def step(value: str) -> str:
        return value

    @DBOS.workflow()
    def my_workflow() -> None:
        sync_ds.run_tx_step(None, step, "a")
        sync_ds.run_tx_step(None, step, "b")

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        my_workflow()
    other = str(uuid.uuid4())
    with SetWorkflowID(other):
        my_workflow()
    assert _checkpoint_step_ids(sync_ds, wfid) == [1, 2]

    sync_ds._delete_checkpoints(wfid, 2)
    assert _checkpoint_step_ids(sync_ds, wfid) == [1]
    sync_ds._delete_checkpoints(wfid, 1)
    assert _checkpoint_step_ids(sync_ds, wfid) == []
    # Other workflows are untouched.
    assert _checkpoint_step_ids(sync_ds, other) == [1, 2]


@pytest.mark.asyncio
async def test_async_ds_delete_checkpoints(
    async_ds: AsyncSQLAlchemyDatasource, dbos: DBOS
) -> None:
    async def step(value: str) -> str:
        return value

    @DBOS.workflow()
    async def my_workflow() -> None:
        await async_ds.run_tx_step_async(None, step, "a")
        await async_ds.run_tx_step_async(None, step, "b")

    async def step_ids(wfid: str) -> list[int]:
        async with async_ds.engine.begin() as conn:
            result = await conn.execute(
                sa.select(async_ds._outputs_table.c.step_id).where(
                    async_ds._outputs_table.c.workflow_id == wfid
                )
            )
            return sorted(result.scalars())

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await my_workflow()
    assert await step_ids(wfid) == [1, 2]

    await async_ds._delete_checkpoints(wfid, 2)
    assert await step_ids(wfid) == [1]
    await async_ds._delete_checkpoints(wfid, 1)
    assert await step_ids(wfid) == []


# ---------------------------------------------------------------------------
# Migrations
# ---------------------------------------------------------------------------

_LATEST_DS_VERSION = len(get_postgres_datasource_migrations("dbos"))


def _pg_admin_url() -> str:
    url = postgres_urls()[0]
    _skip_if_pg_unreachable(url)
    ensure_user_database()
    return url.replace("postgresql://", "postgresql+psycopg://")


def _pg_admin_exec(admin_url: str, *statements: str) -> None:
    engine = sa.create_engine(admin_url)
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
            for sql in statements:
                c.execute(sa.text(sql))
    finally:
        engine.dispose()


def _pg_schema_exists(admin_url: str, schema: str) -> bool:
    engine = sa.create_engine(admin_url)
    try:
        with engine.connect() as c:
            return (
                c.execute(
                    sa.text("SELECT 1 FROM pg_namespace WHERE nspname = :s"),
                    {"s": schema},
                ).fetchone()
                is not None
            )
    finally:
        engine.dispose()


def _ds_version(ds: SQLAlchemyDatasource) -> int:
    prefix = f'"{ds.schema}".' if ds.schema else ""
    with ds.engine.connect() as c:
        return int(
            c.execute(
                sa.text(f"SELECT version FROM {prefix}{DATASOURCE_MIGRATIONS_TABLE}")
            ).scalar_one()
        )


@pytest.fixture()
def fresh_pg_schema(
    cleanup_test_databases: None,
) -> Generator[tuple[str, str], None, None]:
    """An admin URL and a schema name not yet created in the user database."""
    admin_url = _pg_admin_url()
    schema = f"ds_mig_{uuid.uuid4().hex[:8]}"
    yield admin_url, schema
    _pg_admin_exec(admin_url, f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture()
def least_privilege_ds(
    fresh_pg_schema: tuple[str, str],
) -> Generator[SQLAlchemyDatasource, None, None]:
    """A datasource whose role holds only the grants the migration helper gives it."""
    admin_url, schema = fresh_pg_schema
    role, password = f"ds_app_{uuid.uuid4().hex[:8]}", "ds_app_password"
    _pg_admin_exec(admin_url, f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
    try:
        SQLAlchemyDatasource.migrate(admin_url, schema=schema, application_role=role)
        role_url = (
            sa.make_url(admin_url)
            .set(username=role, password=password)
            .render_as_string(hide_password=False)
        )
        # Verify-only creation needs no more than migrated-schema creation.
        verified = SQLAlchemyDatasource.create(
            role_url, schema=schema, run_migrations=False
        )
        verified.engine.dispose()
        ds = SQLAlchemyDatasource.create(role_url, schema=schema)
        yield ds
        ds.engine.dispose()
    finally:
        _pg_admin_exec(
            admin_url,
            f'DROP SCHEMA IF EXISTS "{schema}" CASCADE',
            f'DROP OWNED BY "{role}"',
            f'DROP ROLE "{role}"',
        )


def test_ds_runs_with_least_privilege_role(
    least_privilege_ds: SQLAlchemyDatasource, dbos: DBOS
) -> None:
    """A role without CREATE records, replays, and deletes checkpoints."""
    ds = least_privilege_ds

    @ds.transaction
    def step(value: str) -> str:
        return value

    @DBOS.workflow()
    def my_workflow() -> str:
        return step("a") + step("b")

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert my_workflow() == "ab"
    assert _checkpoint_step_ids(ds, wfid) == [1, 2]
    with SetWorkflowID(wfid):
        assert my_workflow() == "ab"
    ds._delete_checkpoints(wfid, 1)
    assert _checkpoint_step_ids(ds, wfid) == []

    # The role truly lacks CREATE, so it could not have migrated on its own.
    with pytest.raises(ProgrammingError, match="permission denied"):
        with ds.engine.begin() as conn:
            conn.execute(sa.text(f'CREATE TABLE "{ds.schema}".nope (x INT)'))


def test_ds_run_migrations_false_rejects_unmigrated_pg(
    fresh_pg_schema: tuple[str, str],
) -> None:
    admin_url, schema = fresh_pg_schema
    with pytest.raises(DBOSInitializationError, match="at datasource schema version 0"):
        SQLAlchemyDatasource.create(admin_url, schema=schema, run_migrations=False)
    assert not _pg_schema_exists(admin_url, schema)


def test_ds_run_migrations_false_rejects_unmigrated_sqlite(tmp_path: Any) -> None:
    url = f"sqlite:///{tmp_path}/unmigrated.sqlite"
    with pytest.raises(DBOSInitializationError, match="at datasource schema version 0"):
        SQLAlchemyDatasource.create(url, run_migrations=False)
    engine = sa.create_engine(url)
    with engine.connect() as c:
        assert (
            c.execute(
                sa.text("SELECT name FROM sqlite_master WHERE type='table'")
            ).fetchall()
            == []
        )
    engine.dispose()


def test_ds_static_migrate_sqlite(tmp_path: Any) -> None:
    url = f"sqlite:///{tmp_path}/helper.sqlite"
    SQLAlchemyDatasource.migrate(url)
    ds = SQLAlchemyDatasource.create(url, run_migrations=False)
    assert _ds_version(ds) == _LATEST_DS_VERSION
    ds.engine.dispose()
    with pytest.raises(DBOSException, match="only supported for Postgres"):
        SQLAlchemyDatasource.migrate(url, application_role="app")


@pytest.mark.parametrize("dialect", ["sqlite", "pg"])
def test_ds_adopts_unversioned_table(
    dialect: str, tmp_path: Any, cleanup_test_databases: None
) -> None:
    """A datasource_outputs table from before versioning keeps its rows and gets a version."""
    if dialect == "sqlite":
        url, schema, prefix = f"sqlite:///{tmp_path}/legacy.sqlite", None, ""
        engine = sa.create_engine(url)
    else:
        url, schema = _pg_admin_url(), f"ds_mig_{uuid.uuid4().hex[:8]}"
        prefix = f'"{schema}".'
        engine = sa.create_engine(url)
        with engine.begin() as c:
            c.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
    try:
        with engine.begin() as c:
            c.execute(
                sa.text(
                    f"CREATE TABLE {prefix}datasource_outputs (workflow_id TEXT NOT NULL, "
                    "step_id INT NOT NULL, output TEXT, error TEXT, serialization TEXT, "
                    "created_at BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (workflow_id, step_id))"
                )
            )
            c.execute(
                sa.text(
                    f"INSERT INTO {prefix}datasource_outputs (workflow_id, step_id) VALUES ('w', 1)"
                )
            )
        with pytest.raises(DBOSInitializationError):
            SQLAlchemyDatasource.create(url, schema=schema, run_migrations=False)
        ds = SQLAlchemyDatasource.create(url, schema=schema)
        assert _ds_version(ds) == _LATEST_DS_VERSION
        assert _checkpoint_step_ids(ds, "w") == [1]
        ds.engine.dispose()
    finally:
        if schema is not None:
            with engine.begin() as c:
                c.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        engine.dispose()


def test_ds_concurrent_creation_migrates_once(
    fresh_pg_schema: tuple[str, str],
) -> None:
    admin_url, schema = fresh_pg_schema

    def create() -> SQLAlchemyDatasource:
        return SQLAlchemyDatasource.create(admin_url, schema=schema)

    with ThreadPoolExecutor(max_workers=8) as pool:
        datasources = list(pool.map(lambda _: create(), range(8)))
    assert _ds_version(datasources[0]) == _LATEST_DS_VERSION
    for ds in datasources:
        ds.engine.dispose()


@pytest.mark.asyncio
async def test_async_ds_run_migrations_false(
    async_ds: AsyncSQLAlchemyDatasource, tmp_path: Any
) -> None:
    """Verify-only creation accepts a migrated schema and rejects an unmigrated one."""
    url = async_ds.engine.url.render_as_string(hide_password=False)
    verified = await AsyncSQLAlchemyDatasource.create(
        url, schema=async_ds.schema, run_migrations=False
    )
    await verified.engine.dispose()

    if async_ds.schema is None:
        fresh_url, fresh_schema = f"sqlite+aiosqlite:///{tmp_path}/fresh.sqlite", None
    else:
        fresh_url, fresh_schema = url, f"ds_mig_{uuid.uuid4().hex[:8]}"
    with pytest.raises(DBOSInitializationError, match="at datasource schema version 0"):
        await AsyncSQLAlchemyDatasource.create(
            fresh_url, schema=fresh_schema, run_migrations=False
        )

    # The static migrate brings the same database up to date for verify-only creation.
    try:
        await AsyncSQLAlchemyDatasource.migrate(fresh_url, schema=fresh_schema)
        migrated = await AsyncSQLAlchemyDatasource.create(
            fresh_url, schema=fresh_schema, run_migrations=False
        )
        await migrated.engine.dispose()
    finally:
        if fresh_schema is not None:
            async with async_ds.engine.begin() as conn:
                await conn.execute(
                    text(f'DROP SCHEMA IF EXISTS "{fresh_schema}" CASCADE')
                )
