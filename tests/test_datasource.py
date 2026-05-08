"""Tests for SyncDatasource and AsyncDatasource."""

import uuid
from typing import Any, Generator
from urllib.parse import quote

import pytest
import sqlalchemy as sa
from sqlalchemy import text

from dbos import DBOS, SetWorkflowID
from dbos._datasource import AsyncDatasource, DatasourceOptions, SyncDatasource
from dbos._error import DBOSException
from tests.conftest import default_config, using_sqlite

# ---------------------------------------------------------------------------
# Sync SQLite fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sync_ds(tmp_path: Any) -> Generator[SyncDatasource, None, None]:
    db_url = f"sqlite:///{tmp_path}/ds_test.sqlite"
    ds = SyncDatasource.create(db_url)
    yield ds
    if ds.created_engine:
        ds.engine.dispose()


# ---------------------------------------------------------------------------
# Async PG fixture (skipped when using SQLite)
# ---------------------------------------------------------------------------


def _pg_ds_url() -> str:
    cfg = default_config()
    assert cfg["application_database_url"] is not None
    # Use the same PG host/port/credentials but the same database (schema isolates data)
    return (
        cfg["application_database_url"]
        .replace("postgresql://", "postgresql+psycopg://")
        .replace("sqlite:///", "")  # No-op if already PG
    )


@pytest.fixture()
def require_pg() -> str:
    """Skip the test if SQLite mode or PG is not reachable. Use as first fixture arg
    in tests that need PG so the skip fires before heavier fixtures (e.g. dbos) run."""
    cfg = default_config()
    assert cfg["application_database_url"] is not None
    url = cfg["application_database_url"]
    if not url.startswith("postgresql"):
        pytest.skip("Skipping test when testing SQLite")
    try:
        engine = sa.create_engine(
            sa.make_url(url).set(drivername="postgresql+psycopg2"),
            connect_args={"connect_timeout": 3},
        )
        with engine.connect():
            pass
        engine.dispose()
    except Exception:
        pytest.skip("PostgreSQL not reachable")
    return url


@pytest.fixture()
def pg_ds_url(require_pg: str) -> str:
    """PostgreSQL URL for datasource tests (skips when SQLite or PG unavailable)."""
    return require_pg


@pytest.fixture()
def async_ds_schema() -> str:
    return f"ds_test_{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Sync bare-run tests (no DBOS workflow needed)
# ---------------------------------------------------------------------------


def test_sync_ds_bare_run(sync_ds: SyncDatasource) -> None:
    """run_tx_step outside a workflow executes the function transactionally."""
    counter = {"n": 0}

    def increment(amount: int) -> int:
        session = sync_ds.sql_session()
        session.execute(text("SELECT 1"))  # touch the session
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

    # Second run with same wfid must replay from datasource_outputs
    with SetWorkflowID(wfid):
        result = my_workflow("hello")
    assert result == "result:hello"
    assert call_count["n"] == 1  # Not called again


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

    # Second run: error replayed without calling the function again
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
    # Each step called exactly once
    assert call_counts["a"] == 1
    assert call_counts["b"] == 1


# ---------------------------------------------------------------------------
# Async bare-run tests (PG only — aiosqlite not installed)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_ds_bare_run(pg_ds_url: str, async_ds_schema: str) -> None:
    """run_tx_step_async outside a workflow executes the function transactionally."""
    ds = await AsyncDatasource.create(pg_ds_url, schema=async_ds_schema)

    counter = {"n": 0}

    async def increment(amount: int) -> int:
        session = ds.sql_session()
        await session.execute(text("SELECT 1"))
        counter["n"] += amount
        return counter["n"]

    try:
        result = await ds.run_tx_step_async(None, increment, 5)
        assert result == 5
        result = await ds.run_tx_step_async(None, increment, 3)
        assert result == 8
    finally:
        async with ds.engine.begin() as conn:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{async_ds_schema}" CASCADE')
            )
        await ds.engine.dispose()


@pytest.mark.asyncio
async def test_async_ds_decorator_bare_run(
    pg_ds_url: str, async_ds_schema: str
) -> None:
    """@ds.transaction on an async function works outside a workflow."""
    ds = await AsyncDatasource.create(pg_ds_url, schema=async_ds_schema)

    counter = {"n": 0}

    @ds.transaction
    async def increment(amount: int) -> int:
        session = ds.sql_session()
        await session.execute(text("SELECT 1"))
        counter["n"] += amount
        return counter["n"]

    try:
        assert await increment(10) == 10
        assert await increment(5) == 15
    finally:
        async with ds.engine.begin() as conn:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{async_ds_schema}" CASCADE')
            )
        await ds.engine.dispose()


@pytest.mark.asyncio
async def test_async_ds_decorator_with_options(
    pg_ds_url: str, async_ds_schema: str
) -> None:
    """@ds.transaction accepts isolation_level and name options."""
    ds = await AsyncDatasource.create(pg_ds_url, schema=async_ds_schema)
    counter = {"n": 0}

    @ds.transaction(isolation_level="SERIALIZABLE", name="my_step")
    async def increment(amount: int) -> int:
        counter["n"] += amount
        return counter["n"]

    try:
        assert await increment(7) == 7
    finally:
        async with ds.engine.begin() as conn:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{async_ds_schema}" CASCADE')
            )
        await ds.engine.dispose()


@pytest.mark.asyncio
async def test_async_ds_sql_session_outside_tx_raises(
    pg_ds_url: str, async_ds_schema: str
) -> None:
    """sql_session() outside an async datasource transaction must raise."""
    ds = await AsyncDatasource.create(pg_ds_url, schema=async_ds_schema)
    try:
        with pytest.raises(AssertionError):
            ds.sql_session()
    finally:
        async with ds.engine.begin() as conn:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{async_ds_schema}" CASCADE')
            )
        await ds.engine.dispose()


@pytest.mark.asyncio
async def test_async_ds_rejects_sync_func(pg_ds_url: str, async_ds_schema: str) -> None:
    """run_tx_step_async with a non-coroutine must raise."""
    ds = await AsyncDatasource.create(pg_ds_url, schema=async_ds_schema)

    def sync_func() -> str:
        return "oops"

    try:
        with pytest.raises(DBOSException, match="coroutine"):
            await ds.run_tx_step_async(None, sync_func)  # type: ignore
    finally:
        async with ds.engine.begin() as conn:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{async_ds_schema}" CASCADE')
            )
        await ds.engine.dispose()


@pytest.mark.asyncio
async def test_async_ds_transaction_decorator_rejects_sync(
    pg_ds_url: str, async_ds_schema: str
) -> None:
    """@ds.transaction on a sync function must raise at decoration time."""
    ds = await AsyncDatasource.create(pg_ds_url, schema=async_ds_schema)

    try:
        with pytest.raises(DBOSException, match="coroutine"):

            @ds.transaction
            def bad() -> str:
                return "nope"

    finally:
        async with ds.engine.begin() as conn:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{async_ds_schema}" CASCADE')
            )
        await ds.engine.dispose()


# ---------------------------------------------------------------------------
# Async OAOO tests (PG only, inside a DBOS workflow)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_ds_oaoo(
    require_pg: str, dbos: DBOS, pg_ds_url: str, async_ds_schema: str
) -> None:
    """run_tx_step_async inside a workflow records the result and replays on retry."""
    ds = await AsyncDatasource.create(pg_ds_url, schema=async_ds_schema)
    call_count = {"n": 0}

    async def expensive_step(value: str) -> str:
        call_count["n"] += 1
        session = ds.sql_session()
        await session.execute(text("SELECT 1"))
        return f"async:{value}"

    @DBOS.workflow()
    async def my_workflow(value: str) -> str:
        return await ds.run_tx_step_async(None, expensive_step, value)

    wfid = str(uuid.uuid4())

    try:
        with SetWorkflowID(wfid):
            result = await my_workflow("hello")
        assert result == "async:hello"
        assert call_count["n"] == 1

        with SetWorkflowID(wfid):
            result = await my_workflow("hello")
        assert result == "async:hello"
        assert call_count["n"] == 1  # Not called again
    finally:
        async with ds.engine.begin() as conn:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{async_ds_schema}" CASCADE')
            )
        await ds.engine.dispose()


@pytest.mark.asyncio
async def test_async_ds_decorator_oaoo(
    require_pg: str, dbos: DBOS, pg_ds_url: str, async_ds_schema: str
) -> None:
    """@ds.transaction inside a workflow records and replays the result."""
    ds = await AsyncDatasource.create(pg_ds_url, schema=async_ds_schema)
    call_count = {"n": 0}

    @ds.transaction
    async def decorated_step(value: str) -> str:
        call_count["n"] += 1
        return f"decorated:{value}"

    @DBOS.workflow()
    async def my_workflow(value: str) -> str:
        return await decorated_step(value)

    wfid = str(uuid.uuid4())

    try:
        with SetWorkflowID(wfid):
            result = await my_workflow("world")
        assert result == "decorated:world"
        assert call_count["n"] == 1

        with SetWorkflowID(wfid):
            result = await my_workflow("world")
        assert result == "decorated:world"
        assert call_count["n"] == 1
    finally:
        async with ds.engine.begin() as conn:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{async_ds_schema}" CASCADE')
            )
        await ds.engine.dispose()


@pytest.mark.asyncio
async def test_async_ds_error_oaoo(
    require_pg: str, dbos: DBOS, pg_ds_url: str, async_ds_schema: str
) -> None:
    """Async datasource step error is recorded and replayed without re-executing."""
    ds = await AsyncDatasource.create(pg_ds_url, schema=async_ds_schema)
    call_count = {"n": 0}

    async def failing_step() -> str:
        call_count["n"] += 1
        raise ValueError("async ds step failed")

    @DBOS.workflow()
    async def my_workflow() -> str:
        return await ds.run_tx_step_async(None, failing_step)

    wfid = str(uuid.uuid4())

    try:
        with SetWorkflowID(wfid):
            with pytest.raises(Exception, match="async ds step failed"):
                await my_workflow()
        assert call_count["n"] == 1

        with SetWorkflowID(wfid):
            with pytest.raises(Exception, match="async ds step failed"):
                await my_workflow()
        assert call_count["n"] == 1  # Not called again
    finally:
        async with ds.engine.begin() as conn:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{async_ds_schema}" CASCADE')
            )
        await ds.engine.dispose()


@pytest.mark.asyncio
async def test_async_ds_multiple_steps_in_workflow(
    require_pg: str, dbos: DBOS, pg_ds_url: str, async_ds_schema: str
) -> None:
    """Multiple async datasource steps in one workflow each get their own step_id."""
    ds = await AsyncDatasource.create(pg_ds_url, schema=async_ds_schema)
    call_counts = {"a": 0, "b": 0}

    async def step_a() -> str:
        call_counts["a"] += 1
        return "A"

    async def step_b() -> str:
        call_counts["b"] += 1
        return "B"

    @DBOS.workflow()
    async def my_workflow() -> str:
        r1 = await ds.run_tx_step_async(None, step_a)
        r2 = await ds.run_tx_step_async(None, step_b)
        return r1 + r2

    wfid = str(uuid.uuid4())

    try:
        with SetWorkflowID(wfid):
            result = await my_workflow()
        assert result == "AB"

        with SetWorkflowID(wfid):
            result = await my_workflow()
        assert result == "AB"
        assert call_counts["a"] == 1
        assert call_counts["b"] == 1
    finally:
        async with ds.engine.begin() as conn:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{async_ds_schema}" CASCADE')
            )
        await ds.engine.dispose()
