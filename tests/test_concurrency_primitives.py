import asyncio
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional

import pytest
import sqlalchemy as sa
from sqlalchemy.pool import QueuePool

from dbos import DBOS, DBOSClient
from dbos._client import DEFAULT_CLIENT_POOL_SIZE
from dbos._serialization import DefaultSerializer
from dbos._sys_db import DEFAULT_SYS_DB_POOL_SIZE, SystemDatabase
from dbos._utils import LoopAwareEvent, PollingLimiter


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
            # Hold the permit briefly so overlapping runners would be observed.
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
    # The client connects lazily, so inspect the wiring against a fresh SQLite file.
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
    # Custom engine with its own pool size and no pool_size kwarg: limiter halves the actual pool, not the fallback.
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
        # Half the custom pool of 10, not the fallback default (which would also be 10).
        assert db.poll_limiter.limit == 5
        assert db.poll_limiter.enabled is True
    finally:
        db.destroy()
        engine.dispose()


def test_sysdb_defaults_pool_size_when_undeterminable(tmp_path: Any) -> None:
    # NullPool reports no size and none is configured, so the limiter uses the fallback pool size.
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
        # Block the workflow thread so it stays in-flight and the pollers keep polling.
        release.wait()
        return "done"

    sys_db = dbos._sys_db
    # The limiter must actually be active and leave headroom for control-plane.
    assert sys_db.poll_limiter.enabled
    assert sys_db.poll_limiter.limit < DEFAULT_SYS_DB_POOL_SIZE

    handle = DBOS.start_workflow(blocked_workflow)
    wfid = handle.get_workflow_id()
    assert started.wait(timeout=10)

    # Route asyncio.to_thread through DBOS's executor, as the runtime does.
    await DBOS._configure_asyncio_thread_pool()

    # Far more pollers than the limit so the limiter saturates regardless of machine speed.
    NUM_POLLERS = 350
    POLL_INTERVAL_SEC = 0.001
    NUM_PROBES = 30

    loop = asyncio.get_running_loop()
    # Dedicated thread so latency reflects pool availability, modelling DBOS's control-plane threads that hit the pool directly.
    control_plane_executor = ThreadPoolExecutor(
        max_workers=1, thread_name_prefix="control-plane"
    )

    def control_plane_op() -> float:
        # A real control-plane read that checks out a connection, bypassing the limiter.
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
                # Never returns while the workflow is blocked; cancelled at teardown.
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
    # With the limiter, control-plane work finds a reserved connection and stays fast, well below the 30s pool_timeout.
    assert with_limiter < 2.0, msg


# ── LoopAwareEvent ────────────────────────────────────────────


async def _await_waiter_count(event: LoopAwareEvent, count: int) -> None:
    """Wait until `count` async waiters have registered on the event."""
    for _ in range(1000):
        with event._waiters_lock:
            if len(event._waiters) >= count:
                return
        await asyncio.sleep(0.005)
    raise AssertionError(f"only {len(event._waiters)} waiters registered, want {count}")


def _wait_on_own_loop(
    event: LoopAwareEvent, results: List[Optional[bool]], idx: int
) -> None:
    """Await the event on a private event loop, as a caller on another loop would."""

    async def run() -> bool:
        return await event.wait_async(timeout=5.0)

    results[idx] = asyncio.run(run())


@pytest.mark.asyncio
async def test_event_wakes_async_waiter_signaled_from_another_thread() -> None:
    """The core contract: the listener thread's set() wakes a coroutine awaiting it."""
    event = LoopAwareEvent()
    threading.Timer(0.05, event.set).start()

    start = time.perf_counter()
    assert await event.wait_async(timeout=5.0) is True
    # Woken by the set, not by the timeout expiring.
    assert time.perf_counter() - start < 1.0
    assert not event._waiters


@pytest.mark.asyncio
async def test_event_already_set_returns_immediately() -> None:
    event = LoopAwareEvent()
    event.set()
    assert await event.wait_async(timeout=5.0) is True
    assert not event._waiters


@pytest.mark.asyncio
async def test_event_returns_false_on_timeout_and_drops_its_waiter() -> None:
    """recv/get_event/stream events are long-lived and shared, so a waiter left
    behind on every timeout would grow _waiters without bound."""
    event = LoopAwareEvent()

    start = time.perf_counter()
    assert await event.wait_async(timeout=0.1) is False
    assert time.perf_counter() - start >= 0.1
    assert not event._waiters


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [0.0, -1.0])
async def test_event_non_positive_timeout_does_not_wait(timeout: float) -> None:
    event = LoopAwareEvent()
    assert await event.wait_async(timeout=timeout) is False
    assert not event._waiters
    event.set()
    assert await event.wait_async(timeout=timeout) is True


@pytest.mark.asyncio
async def test_event_drops_its_waiter_on_cancellation() -> None:
    """Callers cancel recv_async/get_event_async mid-wait; a stranded waiter would
    pin its future and event loop for the life of the event."""
    event = LoopAwareEvent()
    task = asyncio.create_task(event.wait_async(timeout=30.0))
    await _await_waiter_count(event, 1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not event._waiters


@pytest.mark.asyncio
async def test_event_cancellation_is_not_swallowed_by_a_concurrent_set() -> None:
    """A cancellation landing after a set() resolved the waiter's future, but before
    the waiter resumes, must still propagate. asyncio.wait_for swallows it on Python
    <3.12 -- it catches the CancelledError, sees the future is done, and returns
    normally. recv_async would then break out of `while not event.is_set()` and
    consume the message, handing it to a caller that believes it was cancelled.

    The set() below queues the wakeup; one loop pass lets it resolve the future while
    leaving the waiter's resumption still queued behind it -- exactly that window.
    """
    event = LoopAwareEvent()
    task = asyncio.create_task(event.wait_async(timeout=30.0))
    await _await_waiter_count(event, 1)

    event.set()
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not event._waiters


@pytest.mark.asyncio
async def test_event_wakes_every_waiter_on_one_set() -> None:
    """get_event and stream readers share one event object between concurrent
    waiters, so a single signal must resolve every registered waiter."""
    event = LoopAwareEvent()
    waiters = [asyncio.create_task(event.wait_async(timeout=5.0)) for _ in range(10)]
    await _await_waiter_count(event, 10)

    threading.Thread(target=event.set).start()
    assert await asyncio.wait_for(asyncio.gather(*waiters), timeout=5.0) == [True] * 10
    assert not event._waiters


@pytest.mark.asyncio
async def test_event_wakes_waiters_on_distinct_loops() -> None:
    """DBOS runs a background event loop alongside the caller's, so one set() must
    reach waiters on any mix of loops."""
    event = LoopAwareEvent()
    results: List[Optional[bool]] = [None] * 3
    threads = [
        threading.Thread(target=_wait_on_own_loop, args=(event, results, i))
        for i in range(3)
    ]
    for thread in threads:
        thread.start()
    await _await_waiter_count(event, 3)

    event.set()
    for thread in threads:
        thread.join(timeout=5.0)
    assert results == [True, True, True]
    assert not event._waiters


@pytest.mark.asyncio
async def test_event_latches_a_wakeup_against_a_later_clear() -> None:
    """Stream readers share an event and clear() it each iteration, so one reader's
    clear() can land right after a set() destined for another. The wakeup is latched
    in the waiter's future and survives; the old `while not is_set(): sleep()` loop
    was level-triggered and lost it for the whole remaining wait. The waiter still
    wakes promptly here, and reports False because the flag was cleared -- callers
    must re-check their own condition rather than trust the return.
    """
    event = LoopAwareEvent()
    task = asyncio.create_task(event.wait_async(timeout=5.0))
    await _await_waiter_count(event, 1)

    event.set()
    event.clear()
    assert await asyncio.wait_for(task, timeout=1.0) is False
    assert not event._waiters


@pytest.mark.asyncio
async def test_event_set_survives_a_waiter_whose_loop_closed() -> None:
    """The listener thread signals events whose waiters may already be gone; a dead
    loop must neither raise out of set() nor strand the remaining waiters."""
    event = LoopAwareEvent()
    live = asyncio.create_task(event.wait_async(timeout=5.0))
    await _await_waiter_count(event, 1)

    # Register a waiter directly, then close its loop out from under it.
    dead_loop = asyncio.new_event_loop()
    with event._waiters_lock:
        event._waiters.add((dead_loop, dead_loop.create_future()))
    dead_loop.close()

    event.set()
    assert await asyncio.wait_for(live, timeout=1.0) is True


@pytest.mark.asyncio
async def test_event_one_set_wakes_both_sync_and_async_waiters() -> None:
    """set() calls super().set() before draining async waiters, so sync recv and
    get_event -- which still call wait() on these events -- wake from the same signal.
    """
    event = LoopAwareEvent()
    sync_woke = threading.Event()

    def sync_waiter() -> None:
        if event.wait(timeout=5.0):
            sync_woke.set()

    thread = threading.Thread(target=sync_waiter)
    thread.start()
    async_waiter = asyncio.create_task(event.wait_async(timeout=5.0))
    await _await_waiter_count(event, 1)

    threading.Thread(target=event.set).start()
    assert await asyncio.wait_for(async_waiter, timeout=5.0) is True
    thread.join(timeout=5.0)
    assert sync_woke.is_set()


def test_event_sync_wait_times_out() -> None:
    # The subclass must not disturb the inherited timeout path.
    assert LoopAwareEvent().wait(timeout=0.05) is False


@pytest.mark.asyncio
async def test_event_can_be_awaited_again_after_clear() -> None:
    """Stream readers clear() and re-wait on the same event on every iteration."""
    event = LoopAwareEvent()
    event.set()
    assert await event.wait_async(timeout=5.0) is True

    event.clear()
    assert await event.wait_async(timeout=0.05) is False

    threading.Timer(0.05, event.set).start()
    assert await event.wait_async(timeout=5.0) is True


class _LingeringLock:
    """Stand-in for LoopAwareEvent._waiters_lock that holds the window after its
    first release open, letting a waiter register in the gap between set()'s drain
    and its flag write. Widening the window is the only way to exercise that
    ordering reliably: a plain thread race almost never lands inside it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.released = threading.Event()

    def __enter__(self) -> None:
        self._lock.acquire()

    def __exit__(self, *exc: object) -> None:
        self._lock.release()
        if not self.released.is_set():
            self.released.set()
            time.sleep(0.2)


@pytest.mark.asyncio
async def test_event_waiter_registering_inside_set_is_not_lost() -> None:
    """set() must write the flag BEFORE draining waiters, as documented in _utils.py.
    Inverted, a waiter registering between the drain and the flag write falls through
    both paths -- it is absent from the drained snapshot and it saw an unset flag --
    so it parks until its timeout rather than waking. This holds set() open in exactly
    that window after a drain that found no waiters, then registers.

    Note the return value cannot detect this: wait_async ends in `return self.is_set()`,
    so a lost wakeup still reports True once the flag lands. Only the latency shows it.
    """
    event = LoopAwareEvent()
    window = _LingeringLock()
    event._waiters_lock = window  # type: ignore[assignment]

    threading.Thread(target=event.set).start()
    for _ in range(1000):
        if window.released.is_set():
            break
        await asyncio.sleep(0.005)
    else:
        raise AssertionError("set() never reached the window")

    start = time.perf_counter()
    assert await event.wait_async(timeout=5.0) is True
    # Correct ordering: the flag is already set, so this returns without parking.
    assert time.perf_counter() - start < 1.0


@pytest.mark.asyncio
async def test_event_never_loses_a_wakeup_racing_registration() -> None:
    """Stress the same invariant across whatever orderings the scheduler produces:
    a barrier releases set() and a registration together. A lost wakeup parks the
    waiter until its timeout, so latency -- not the return value -- is the signal.
    """
    slowest = 0.0
    for _ in range(200):
        event = LoopAwareEvent()
        barrier = threading.Barrier(2)

        def setter(event: LoopAwareEvent = event) -> None:
            barrier.wait()
            event.set()

        threading.Thread(target=setter).start()
        barrier.wait()
        start = time.perf_counter()
        assert await event.wait_async(timeout=5.0) is True
        slowest = max(slowest, time.perf_counter() - start)
        assert not event._waiters
    assert slowest < 1.0, f"a waiter parked for {slowest:.2f}s -- wakeup lost"
