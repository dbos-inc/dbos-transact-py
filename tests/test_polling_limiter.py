import asyncio
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List

import pytest
import sqlalchemy as sa
from sqlalchemy.pool import QueuePool

from dbos import DBOS, DBOSClient
from dbos._client import DEFAULT_CLIENT_POOL_SIZE
from dbos._serialization import DefaultSerializer
from dbos._sys_db import DEFAULT_SYS_DB_POOL_SIZE, SystemDatabase
from dbos._utils import PollingLimiter


def _run_under_limiter(limit: int, tasks: int) -> int:
    """Run `tasks` workers under a limiter of size `limit`, returning the peak
    number of workers observed running concurrently."""
    limiter = PollingLimiter(limit)
    lock = threading.Lock()
    in_flight = 0
    peak = 0

    def worker() -> None:
        nonlocal in_flight, peak
        with limiter:
            with lock:
                in_flight += 1
                peak = max(peak, in_flight)
            # Hold the permit briefly so overlapping runners would be observed
            # if the limiter permitted them.
            time.sleep(0.02)
            with lock:
                in_flight -= 1

    threads = [threading.Thread(target=worker) for _ in range(tasks)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return peak


def test_limiter_caps_concurrency() -> None:
    assert _run_under_limiter(3, 20) == 3
    assert _run_under_limiter(1, 10) == 1


def test_limiter_is_passthrough_when_non_positive() -> None:
    # A non-positive limit disables the limiter: every worker runs at once.
    assert _run_under_limiter(0, 8) == 8
    assert _run_under_limiter(-5, 8) == 8
    assert PollingLimiter(0).enabled is False
    assert PollingLimiter(-5).enabled is False


def test_limiter_releases_permit_when_body_raises() -> None:
    limiter = PollingLimiter(1)

    class Boom(Exception):
        pass

    try:
        with limiter:
            raise Boom()
    except Boom:
        pass

    # If the permit leaked, this second acquire would block forever.
    acquired = limiter._semaphore.acquire(timeout=1)  # type: ignore[union-attr]
    assert acquired is True


def _make_sysdb(tmp_path: Any, name: str, **kwargs: Any) -> SystemDatabase:
    return SystemDatabase.create(
        system_database_url=f"sqlite:///{tmp_path / name}",
        engine_kwargs={"pool_size": 8},
        engine=None,
        schema=None,
        serializer=DefaultSerializer(),
        executor_id=None,
        **kwargs,
    )


def test_sysdb_defaults_polling_concurrency_to_half_pool(tmp_path: Any) -> None:
    db = _make_sysdb(tmp_path, "default.sqlite")
    try:
        # Half the pool of 8.
        assert db.poll_limiter.limit == 4
        assert db.poll_limiter.enabled is True
    finally:
        db.destroy()


def test_sysdb_explicit_polling_concurrency_overrides_default(tmp_path: Any) -> None:
    db = _make_sysdb(tmp_path, "explicit.sqlite", polling_concurrency=3)
    try:
        # The requested value (3), not the half-the-pool default (4).
        assert db.poll_limiter.limit == 3
    finally:
        db.destroy()


def test_sysdb_polling_concurrency_can_be_disabled(tmp_path: Any) -> None:
    db = _make_sysdb(tmp_path, "disabled.sqlite", polling_concurrency=0)
    try:
        assert db.poll_limiter.enabled is False
    finally:
        db.destroy()


def test_client_defaults_pool_size_and_polling_concurrency(tmp_path: Any) -> None:
    # The client connects lazily, so we can inspect the wiring against a fresh
    # SQLite file without exercising any real workload.
    client = DBOSClient(system_database_url=f"sqlite:///{tmp_path / 'client.sqlite'}")
    try:
        pool = client._sys_db.engine.pool
        assert isinstance(pool, QueuePool)
        assert pool.size() == DEFAULT_CLIENT_POOL_SIZE
        # Polling concurrency defaults to half the pool.
        assert client._sys_db.poll_limiter.limit == DEFAULT_CLIENT_POOL_SIZE // 2
    finally:
        client.destroy()


def test_client_pool_size_and_polling_concurrency_overrides(tmp_path: Any) -> None:
    client = DBOSClient(
        system_database_url=f"sqlite:///{tmp_path / 'client_override.sqlite'}",
        system_database_pool_size=8,
        system_database_polling_concurrency=3,
    )
    try:
        pool = client._sys_db.engine.pool
        assert isinstance(pool, QueuePool)
        assert pool.size() == 8
        # The requested value (3), not the half-the-pool default (4).
        assert client._sys_db.poll_limiter.limit == 3
    finally:
        client.destroy()


def test_sysdb_defaults_polling_concurrency_to_half_custom_pool(tmp_path: Any) -> None:
    # A custom engine with a custom pool size is provided (and no pool_size in
    # engine_kwargs). The limiter should read the custom pool's actual size and
    # default to half of it, rather than falling back to the default pool size.
    engine = sa.create_engine(
        f"sqlite:///{tmp_path / 'custom_pool.sqlite'}",
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=0,
    )
    # Sanity check the custom pool size.
    assert isinstance(engine.pool, QueuePool)
    assert engine.pool.size() == 10
    db = SystemDatabase.create(
        system_database_url=f"sqlite:///{tmp_path / 'custom_pool.sqlite'}",
        engine_kwargs={},
        engine=engine,
        schema=None,
        serializer=DefaultSerializer(),
        executor_id=None,
    )
    try:
        # Half the custom pool of 10, not the default (which would be 10 for a
        # fallback pool size of 20).
        assert db.poll_limiter.limit == 5
        assert db.poll_limiter.enabled is True
    finally:
        db.destroy()
        engine.dispose()


def test_sysdb_defaults_pool_size_when_undeterminable(tmp_path: Any) -> None:
    # A NullPool reports no size and no pool_size is configured, so the limiter
    # falls back to the default pool size to compute its default concurrency.
    engine = sa.create_engine(
        f"sqlite:///{tmp_path / 'nopool.sqlite'}", poolclass=sa.NullPool
    )
    db = SystemDatabase.create(
        system_database_url=f"sqlite:///{tmp_path / 'nopool.sqlite'}",
        engine_kwargs={},
        engine=engine,
        schema=None,
        serializer=DefaultSerializer(),
        executor_id=None,
    )
    try:
        assert db.poll_limiter.limit == max(1, DEFAULT_SYS_DB_POOL_SIZE // 2)
    finally:
        db.destroy()
        engine.dispose()


@pytest.mark.asyncio
async def test_control_plane_responsive_under_polling_storm(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """End-to-end: launch a large fan-out of async pollers calling get_result on
    a still-running workflow with a short polling interval, and verify that
    control-plane operations keep proceeding with minimal latency.

    The pollers run their DB reads through asyncio.to_thread (DBOS's unbounded
    executor), so without the limiter they would check out every connection in
    the system database pool and a control-plane op would queue behind them. The
    limiter caps concurrent poll reads at half the pool, keeping the other half
    free for control-plane work.

    A large fan-out of pollers (far more than the limit) hammers get_result, so
    the limiter is fully exercised: only `limit` of them hold a connection at any
    time and the rest queue on the semaphore, leaving headroom for the control
    plane. This is a latency-sensitive benchmark, so it is skipped on SQLite
    (whose busy_timeout locking makes read latency too noisy) and asserts only a
    generous absolute latency bound, to stay robust across machine speeds while
    still catching a gross regression.
    """
    started = threading.Event()
    release = threading.Event()

    @DBOS.workflow()
    def blocked_workflow() -> str:
        started.set()
        # Block the workflow thread so it stays in-flight (never completes) and
        # the pollers below keep polling instead of resolving.
        release.wait()
        return "done"

    sys_db = dbos._sys_db
    # The limiter must actually be active and leave headroom for control-plane.
    assert sys_db.poll_limiter.enabled
    assert sys_db.poll_limiter.limit < DEFAULT_SYS_DB_POOL_SIZE

    handle = DBOS.start_workflow(blocked_workflow)
    wfid = handle.get_workflow_id()
    assert started.wait(timeout=10)

    # Route asyncio.to_thread through DBOS's executor, exactly as the runtime
    # does before serving concurrent pollers.
    await DBOS._configure_asyncio_thread_pool()

    # Far more pollers than the limit, at a short interval, so the limiter is
    # saturated (excess pollers queue on the semaphore) regardless of machine speed.
    NUM_POLLERS = 350
    POLL_INTERVAL_SEC = 0.001
    NUM_PROBES = 30

    loop = asyncio.get_running_loop()
    # Measure control-plane latency on a dedicated thread so it reflects system
    # database pool availability, not contention for the pollers' thread pool.
    # This models DBOS's background control-plane threads (queue runner,
    # recovery, scheduler), which hit the pool directly rather than via
    # asyncio.to_thread.
    control_plane_executor = ThreadPoolExecutor(
        max_workers=1, thread_name_prefix="control-plane"
    )

    def control_plane_op() -> float:
        # A real control-plane read that checks out a pool connection (bypassing
        # the polling limiter, as enqueue/dequeue/status writes do).
        t0 = time.perf_counter()
        sys_db.list_workflows(limit=1)
        return time.perf_counter() - t0

    async def measure() -> float:
        latencies: List[float] = []
        for _ in range(NUM_PROBES):
            latencies.append(
                await loop.run_in_executor(control_plane_executor, control_plane_op)
            )
            await asyncio.sleep(0.01)
        return statistics.median(latencies)

    async def poll_forever() -> None:
        h: Any = await DBOS.retrieve_workflow_async(wfid, existing_workflow=False)
        while True:
            try:
                # Never returns while the workflow is blocked; reissued only if a
                # poll raises, and cancelled at teardown.
                await h.get_result(polling_interval_sec=POLL_INTERVAL_SEC)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(POLL_INTERVAL_SEC)

    pollers = [asyncio.create_task(poll_forever()) for _ in range(NUM_POLLERS)]
    try:
        # Let the storm ramp up, then measure control-plane latency under it.
        await asyncio.sleep(1.0)
        with_limiter = await measure()
    finally:
        for t in pollers:
            t.cancel()
        await asyncio.gather(*pollers, return_exceptions=True)
        release.set()
        control_plane_executor.shutdown(wait=True)

    # The blocked workflow completes once released.
    assert (await asyncio.to_thread(handle.get_result)) == "done"

    msg = f"control-plane median latency with limiter={with_limiter * 1000:.1f}ms"
    # With the limiter, control-plane work always finds a reserved connection
    # despite the polling storm, so it stays fast in absolute terms -- generously
    # below the 30s pool_timeout even on a slow/loaded CI runner.
    assert with_limiter < 2.0, msg
