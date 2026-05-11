"""Tests for SyncDatasource and AsyncDatasource."""

import uuid
from typing import Any, AsyncGenerator, Generator

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy import text

from dbos import DBOS, SetWorkflowID
from dbos._datasource import AsyncDatasource, SyncDatasource
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
            sa.make_url(raw_pg_url).set(drivername="postgresql+psycopg2"),
            connect_args={"connect_timeout": 3},
        )
        with engine.connect():
            pass
        engine.dispose()
    except Exception:
        pytest.skip("PostgreSQL not reachable")


def _check_both_tables(ds: SyncDatasource, dbos_instance: DBOS, wfid: str) -> None:
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
    ds: AsyncDatasource, dbos_instance: DBOS, wfid: str
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
) -> Generator[SyncDatasource, None, None]:
    if request.param == "sqlite":
        ds = SyncDatasource.create(f"sqlite:///{tmp_path}/ds_test.sqlite")
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
        ds = SyncDatasource.create(
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
) -> AsyncGenerator[AsyncDatasource, None]:
    if request.param == "sqlite":
        ds = await AsyncDatasource.create(
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
        ds = await AsyncDatasource.create(
            url.replace("postgresql://", "postgresql+psycopg://"), schema=schema
        )
        yield ds
        async with ds.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await ds.engine.dispose()


# ---------------------------------------------------------------------------
# Sync bare-run tests (no DBOS workflow needed)
# ---------------------------------------------------------------------------


def test_sync_ds_bare_run(sync_ds: SyncDatasource) -> None:
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


def test_sync_ds_decorator_bare_run(sync_ds: SyncDatasource) -> None:
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


def test_sync_ds_decorator_with_options(sync_ds: SyncDatasource) -> None:
    """@ds.transaction accepts isolation_level and name options."""
    counter = {"n": 0}

    @sync_ds.transaction(isolation_level="SERIALIZABLE", name="my_step")
    def increment(amount: int) -> int:
        counter["n"] += amount
        return counter["n"]

    assert increment(7) == 7


def test_sync_ds_sql_session_outside_tx_raises(sync_ds: SyncDatasource) -> None:
    """sql_session() outside a datasource transaction must raise."""
    with pytest.raises(AssertionError):
        sync_ds.sql_session()


def test_sync_ds_rejects_coroutine(sync_ds: SyncDatasource) -> None:
    """run_tx_step with a coroutine function must raise immediately."""

    async def my_async_func() -> str:
        return "oops"

    with pytest.raises(DBOSException, match="coroutine"):
        sync_ds.run_tx_step(None, my_async_func)  # type: ignore


def test_sync_ds_transaction_decorator_rejects_coroutine(
    sync_ds: SyncDatasource,
) -> None:
    """@ds.transaction on a coroutine must raise at decoration time."""
    with pytest.raises(DBOSException, match="coroutine"):

        @sync_ds.transaction
        async def bad() -> str:
            return "nope"


# ---------------------------------------------------------------------------
# Sync OAOO tests (inside a DBOS workflow)
# ---------------------------------------------------------------------------


def test_sync_ds_oaoo(dbos: DBOS, sync_ds: SyncDatasource) -> None:
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


def test_sync_ds_decorator_oaoo(dbos: DBOS, sync_ds: SyncDatasource) -> None:
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


def test_sync_ds_error_oaoo(dbos: DBOS, sync_ds: SyncDatasource) -> None:
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


def test_sync_ds_multiple_steps_in_workflow(
    dbos: DBOS, sync_ds: SyncDatasource
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


def test_sync_ds_writes_both_tables(dbos: DBOS, sync_ds: SyncDatasource) -> None:
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


def test_sync_ds_recovers_from_sysdb_loss(dbos: DBOS, sync_ds: SyncDatasource) -> None:
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
async def test_async_ds_bare_run(async_ds: AsyncDatasource) -> None:
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
async def test_async_ds_decorator_bare_run(async_ds: AsyncDatasource) -> None:
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
async def test_async_ds_decorator_with_options(async_ds: AsyncDatasource) -> None:
    """@ds.transaction accepts isolation_level and name options."""
    counter = {"n": 0}

    @async_ds.transaction(isolation_level="SERIALIZABLE", name="my_step")
    async def increment(amount: int) -> int:
        counter["n"] += amount
        return counter["n"]

    assert await increment(7) == 7


@pytest.mark.asyncio
async def test_async_ds_sql_session_outside_tx_raises(
    async_ds: AsyncDatasource,
) -> None:
    """sql_session() outside an async datasource transaction must raise."""
    with pytest.raises(AssertionError):
        async_ds.sql_session()


@pytest.mark.asyncio
async def test_async_ds_rejects_sync_func(async_ds: AsyncDatasource) -> None:
    """run_tx_step_async with a non-coroutine must raise."""

    def sync_func() -> str:
        return "oops"

    with pytest.raises(DBOSException, match="coroutine"):
        await async_ds.run_tx_step_async(None, sync_func)  # type: ignore


@pytest.mark.asyncio
async def test_async_ds_transaction_decorator_rejects_sync(
    async_ds: AsyncDatasource,
) -> None:
    """@ds.transaction on a sync function must raise at decoration time."""
    with pytest.raises(DBOSException, match="coroutine"):

        @async_ds.transaction
        def bad() -> str:
            return "nope"


# ---------------------------------------------------------------------------
# Async OAOO tests (inside a DBOS workflow)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_ds_oaoo(dbos: DBOS, async_ds: AsyncDatasource) -> None:
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
async def test_async_ds_decorator_oaoo(dbos: DBOS, async_ds: AsyncDatasource) -> None:
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
async def test_async_ds_error_oaoo(dbos: DBOS, async_ds: AsyncDatasource) -> None:
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
async def test_async_ds_multiple_steps_in_workflow(
    dbos: DBOS, async_ds: AsyncDatasource
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
    dbos: DBOS, async_ds: AsyncDatasource
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
async def test_async_ds_recovers_from_sysdb_loss(
    dbos: DBOS, async_ds: AsyncDatasource
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
