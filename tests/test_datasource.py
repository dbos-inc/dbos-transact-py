"""Tests for SyncDatasource and AsyncDatasource."""

import base64
import pickle
import uuid
from typing import Any, AsyncGenerator, Generator

import pytest
import pytest_asyncio
import sqlalchemy as sa
from psycopg.errors import SerializationFailure
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from dbos import DBOS, AsyncSQLAlchemyDatasource, SetWorkflowID, SQLAlchemyDatasource
from dbos._datasource_postgres import PostgresAsyncDatasource, PostgresSyncDatasource
from dbos._error import DBOSException
from dbos._schemas.datasource_database import DatasourceSchema
from dbos._schemas.system_database import SystemSchema
from tests.conftest import default_config

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _skip_if_pg_unreachable(raw_pg_url: str) -> None:
    try:
        engine = sa.create_engine(
            sa.make_url(raw_pg_url).set(drivername="postgresql+psycopg"),
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
        cfg = default_config()
        url = cfg.get("application_database_url") or ""
        if not url.startswith("postgresql"):
            pytest.skip("not a PostgreSQL environment")
        _skip_if_pg_unreachable(url)
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
        cfg = default_config()
        url = cfg.get("application_database_url") or ""
        if not url.startswith("postgresql"):
            pytest.skip("not a PostgreSQL environment")
        _skip_if_pg_unreachable(url)
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
