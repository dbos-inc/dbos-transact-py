import asyncio
import contextvars
import gc
import threading
import time
import uuid
import weakref
from typing import Any, Dict, List, Optional, Tuple

import pytest

from dbos import DBOS, DBOSClient, SetWorkflowID, WorkflowHandleAsync
from dbos._context import get_local_dbos_context
from dbos._error import DBOSAwaitedWorkflowCancelledError, DBOSNonExistentWorkflowError
from dbos._shard_keys import bound_shard_key
from dbos._sys_db import WorkflowStatusString, _StatusWatcher
from tests.conftest import retry_until_success_async

INTERVAL = 0.05


def _status_reads(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> tuple[List[List[str]], List[Any]]:
    """Record each batched status read and the DBOS context it ran in."""
    calls: List[List[str]] = []
    contexts: List[Any] = []
    read = dbos._sys_db.get_workflow_status_values

    def recording_read(workflow_ids: List[str]) -> Dict[str, str]:
        calls.append(list(workflow_ids))
        contexts.append(get_local_dbos_context())
        return read(workflow_ids)

    monkeypatch.setattr(dbos._sys_db, "get_workflow_status_values", recording_read)
    return calls, contexts


async def _all_registered(sys_db: Any, count: int) -> None:
    """Wait until the loop's watcher holds this many waiters."""

    def registered() -> None:
        watchers = sys_db._status_watchers.get(asyncio.get_running_loop(), {})
        watcher = watchers.get(INTERVAL)
        assert watcher is not None and len(watcher._waiters) == count

    await retry_until_success_async(registered, interval=INTERVAL, max_attempts=200)


def _result_checks(dbos: DBOS, monkeypatch: pytest.MonkeyPatch) -> List[str]:
    checks: List[str] = []
    check = dbos._sys_db.check_workflow_result

    def counting_check(workflow_id: str, **kwargs: Any) -> Any:
        checks.append(workflow_id)
        return check(workflow_id, **kwargs)

    monkeypatch.setattr(dbos._sys_db, "check_workflow_result", counting_check)
    return checks


@pytest.mark.asyncio
async def test_result_waits_share_one_status_read(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Async get_result waiters on one loop share one status read per interval, run
    outside any DBOS context, and check their own workflow only once it has finished."""
    release = threading.Event()

    @DBOS.workflow()
    async def blocked(i: int) -> int:
        await asyncio.to_thread(release.wait, 30)
        return i

    queue = await DBOS.register_queue_async("result-wait-batching")
    handles = [await queue.enqueue_async(blocked, i) for i in range(5)]
    calls, contexts = _status_reads(dbos, monkeypatch)
    checks = _result_checks(dbos, monkeypatch)
    waiters = [
        asyncio.create_task(h.get_result(polling_interval_sec=INTERVAL))
        for h in handles
    ]

    await _all_registered(dbos._sys_db, 5)
    # Skip a read that may have started before the last waiter registered.
    start = len(calls) + 1

    def polled() -> None:
        assert len(calls) >= start + 3

    await retry_until_success_async(polled, interval=INTERVAL, max_attempts=200)
    wfids = {h.workflow_id for h in handles}
    # Each waiter checked once before waiting, then left the polling to the watcher.
    assert sorted(checks) == sorted(wfids)
    assert all(set(c) == wfids for c in calls[start : start + 3])
    assert all(c is None for c in contexts)

    release.set()
    assert await asyncio.gather(*waiters) == list(range(5))


@pytest.mark.asyncio
async def test_wait_first_shares_one_status_read(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Async wait_first callers share the same batched read and return the first
    workflow that completes."""
    releases = [threading.Event() for _ in range(3)]

    @DBOS.workflow()
    async def blocked(i: int) -> int:
        await asyncio.to_thread(releases[i].wait, 30)
        return i

    queue = await DBOS.register_queue_async("wait-first-batching")
    handles = [await queue.enqueue_async(blocked, i) for i in range(3)]
    calls, _ = _status_reads(dbos, monkeypatch)
    first = [
        asyncio.create_task(
            DBOS.wait_first_async(handles[:2], polling_interval_sec=INTERVAL)
        ),
        asyncio.create_task(
            DBOS.wait_first_async(handles[1:], polling_interval_sec=INTERVAL)
        ),
    ]

    await _all_registered(dbos._sys_db, 2)
    start = len(calls) + 1

    def polled() -> None:
        assert len(calls) >= start + 3

    await retry_until_success_async(polled, interval=INTERVAL, max_attempts=200)
    assert all(
        set(c) == {h.workflow_id for h in handles} and len(c) == 3
        for c in calls[start : start + 3]
    )

    releases[1].set()
    done = await asyncio.gather(*first)
    assert [h.workflow_id for h in done] == [handles[1].workflow_id] * 2
    for r in releases:
        r.set()
    assert [await h.get_result() for h in handles] == [0, 1, 2]


@pytest.mark.asyncio
async def test_batched_waits_keep_error_and_cancel_semantics(dbos: DBOS) -> None:
    """A woken waiter reads the outcome itself, so errors and cancellations surface
    as before."""
    release = threading.Event()

    @DBOS.workflow()
    async def failing() -> None:
        await asyncio.to_thread(release.wait, 30)
        raise ValueError("boom")

    @DBOS.workflow()
    async def blocked() -> None:
        await asyncio.to_thread(release.wait, 30)

    queue = await DBOS.register_queue_async("result-wait-errors")
    fail_handle = await queue.enqueue_async(failing)
    cancel_handle = await queue.enqueue_async(blocked)
    fail_wait = asyncio.create_task(
        fail_handle.get_result(polling_interval_sec=INTERVAL)
    )
    cancel_wait = asyncio.create_task(
        cancel_handle.get_result(polling_interval_sec=INTERVAL)
    )
    await asyncio.sleep(3 * INTERVAL)
    await DBOS.cancel_workflow_async(cancel_handle.workflow_id)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await cancel_wait
    release.set()
    with pytest.raises(ValueError, match="boom"):
        await fail_wait


@pytest.mark.asyncio
async def test_fail_if_missing_still_raises_when_the_row_goes(dbos: DBOS) -> None:
    """A wait that must not poll a missing row forever still fails once the row is
    deleted while it waits."""
    release = threading.Event()

    @DBOS.workflow()
    async def blocked() -> None:
        await asyncio.to_thread(release.wait, 30)

    queue = await DBOS.register_queue_async("result-wait-missing")
    handle = await queue.enqueue_async(blocked)
    wait = asyncio.create_task(
        dbos._sys_db.await_workflow_result_async(
            handle.workflow_id, INTERVAL, fail_if_missing=True
        )
    )
    await asyncio.sleep(3 * INTERVAL)
    assert not wait.done()
    await DBOS.delete_workflow_async(handle.workflow_id)
    with pytest.raises(DBOSNonExistentWorkflowError):
        await asyncio.wait_for(wait, timeout=10)
    release.set()


@pytest.mark.asyncio
async def test_failed_status_read_falls_back_to_each_waiter(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the shared read fails, each waiter checks for itself, so it raises or
    keeps waiting as an unbatched wait would."""
    release = threading.Event()

    @DBOS.workflow()
    async def blocked(i: int) -> int:
        await asyncio.to_thread(release.wait, 30)
        return i

    queue = await DBOS.register_queue_async("result-wait-read-error")
    handles = [await queue.enqueue_async(blocked, i) for i in range(2)]

    def failing_read(workflow_ids: List[str]) -> Dict[str, str]:
        raise RuntimeError("status read failed")

    monkeypatch.setattr(dbos._sys_db, "get_workflow_status_values", failing_read)
    checks = _result_checks(dbos, monkeypatch)
    waiters = [
        asyncio.create_task(h.get_result(polling_interval_sec=INTERVAL))
        for h in handles
    ]

    def fell_back() -> None:
        assert len(checks) >= 6

    await retry_until_success_async(fell_back, interval=INTERVAL, max_attempts=200)
    assert not any(w.done() for w in waiters)
    release.set()
    assert await asyncio.gather(*waiters) == [0, 1]


@pytest.mark.asyncio
async def test_waiter_keeps_waiting_when_status_flips_back(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A finished status that is gone by the time the waiter checks (a resumed or
    stale row) makes it wait again, not return."""
    release = threading.Event()

    @DBOS.workflow()
    async def blocked() -> str:
        await asyncio.to_thread(release.wait, 30)
        return "done"

    queue = await DBOS.register_queue_async("result-wait-flip")
    handle = await queue.enqueue_async(blocked)
    read = dbos._sys_db.get_workflow_status_values
    reads = 0

    def stale_read(workflow_ids: List[str]) -> Dict[str, str]:
        nonlocal reads
        reads += 1
        if reads == 1:
            return {wfid: WorkflowStatusString.SUCCESS.value for wfid in workflow_ids}
        return read(workflow_ids)

    monkeypatch.setattr(dbos._sys_db, "get_workflow_status_values", stale_read)
    checks = _result_checks(dbos, monkeypatch)
    wait = asyncio.create_task(handle.get_result(polling_interval_sec=INTERVAL))

    def read_again() -> None:
        assert reads >= 3

    await retry_until_success_async(read_again, interval=INTERVAL, max_attempts=200)
    assert not wait.done()
    assert len(checks) == 2
    release.set()
    assert await wait == "done"


@pytest.mark.asyncio
async def test_waiter_checks_itself_when_the_read_misses_its_row(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A row the batched read can't see (say, under a shard key it didn't capture)
    makes the waiter check in its own context instead of waiting forever."""
    release = threading.Event()

    @DBOS.workflow()
    async def blocked() -> str:
        await asyncio.to_thread(release.wait, 30)
        return "done"

    queue = await DBOS.register_queue_async("result-wait-unseen-row")
    handle = await queue.enqueue_async(blocked)

    def blind_read(workflow_ids: List[str]) -> Dict[str, str]:
        return {}

    monkeypatch.setattr(dbos._sys_db, "get_workflow_status_values", blind_read)
    wait = asyncio.create_task(handle.get_result(polling_interval_sec=INTERVAL))
    await asyncio.sleep(3 * INTERVAL)
    assert not wait.done()
    release.set()
    assert await asyncio.wait_for(wait, timeout=10) == "done"


@pytest.mark.asyncio
async def test_dead_poller_wakes_its_waiters(dbos: DBOS) -> None:
    """If the poller dies, its waiters re-check and start a new one, rather than
    waiting forever."""
    release = threading.Event()

    @DBOS.workflow()
    async def blocked() -> str:
        await asyncio.to_thread(release.wait, 30)
        return "done"

    queue = await DBOS.register_queue_async("result-wait-dead-poller")
    handle = await queue.enqueue_async(blocked)
    wait = asyncio.create_task(handle.get_result(polling_interval_sec=INTERVAL))

    def watching() -> _StatusWatcher:
        watchers = dbos._sys_db._status_watchers[asyncio.get_running_loop()]
        watcher = watchers[INTERVAL]
        assert watcher._poller is not None and watcher._waiters
        return watcher

    watcher = await retry_until_success_async(
        watching, interval=INTERVAL, max_attempts=200
    )
    old_poller = watcher._poller
    assert old_poller is not None
    old_poller.cancel()

    def restarted() -> None:
        # The woken waiter left (dropping the idle watcher) and registered again.
        current = watching()
        assert current._poller is not old_poller

    await retry_until_success_async(restarted, interval=INTERVAL, max_attempts=200)
    release.set()
    assert await wait == "done"


@pytest.mark.parametrize("poller_started", [True, False])
def test_status_watcher_releases_closed_loops(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch, poller_started: bool
) -> None:
    """A cancelled caller unregisters, and the last one to leave stops the poller and
    drops its watcher, so the loop can be collected."""
    wfid = "test-closed-loop-" + str(uuid.uuid4())
    monkeypatch.setattr(
        dbos._sys_db,
        "get_workflow_status_values",
        lambda ids: {i: WorkflowStatusString.PENDING.value for i in ids},
    )

    async def wait_and_leave() -> "weakref.ref[asyncio.AbstractEventLoop]":
        wait = asyncio.create_task(dbos._sys_db._await_status(((wfid, None),), 0.01))
        await asyncio.sleep(0)
        watcher = dbos._sys_db._status_watchers[asyncio.get_running_loop()][0.01]
        poller = watcher._poller
        assert poller is not None
        if poller_started:
            await asyncio.sleep(0.05)
            assert not poller.done()
        wait.cancel()
        with pytest.raises(asyncio.CancelledError):
            await wait
        assert not watcher._waiters and watcher._poller is None
        assert asyncio.get_running_loop() not in dbos._sys_db._status_watchers
        with pytest.raises(asyncio.CancelledError):
            await poller
        return weakref.ref(asyncio.get_running_loop())

    loop_ref = asyncio.run(wait_and_leave())
    gc.collect()
    assert loop_ref() is None
    assert len(dbos._sys_db._status_watchers) == 0


def test_status_watcher_releases_a_manually_closed_loop(dbos: DBOS) -> None:
    """A loop run with run_until_complete and closed without cancelling its tasks is
    still released once the wait returns."""
    wfid = "test-manual-loop-" + str(uuid.uuid4())
    loop = asyncio.new_event_loop()
    # No row, so the first read wakes the waiter.
    loop.run_until_complete(dbos._sys_db._await_status(((wfid, None),), 0.01))
    loop.close()
    loop_ref = weakref.ref(loop)
    del loop

    # A read already in its thread holds the loop until it finishes; close doesn't wait.
    deadline = time.monotonic() + 5
    while True:
        gc.collect()
        if loop_ref() is None:
            break
        assert time.monotonic() < deadline, "the closed loop was never released"
        time.sleep(0.05)
    assert len(dbos._sys_db._status_watchers) == 0


@pytest.mark.asyncio
async def test_status_watchers_are_per_interval(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Waits with different intervals get their own watcher, each gone once its
    waiters leave."""
    monkeypatch.setattr(
        dbos._sys_db,
        "get_workflow_status_values",
        lambda ids: {i: WorkflowStatusString.PENDING.value for i in ids},
    )
    wfid = "test-intervals-" + str(uuid.uuid4())
    waits = [
        asyncio.create_task(dbos._sys_db._await_status(((wfid, None),), interval))
        for interval in [INTERVAL, 2 * INTERVAL]
    ]
    await asyncio.sleep(0)
    per_loop = dbos._sys_db._status_watchers[asyncio.get_running_loop()]
    assert set(per_loop) == {INTERVAL, 2 * INTERVAL}
    waits[0].cancel()
    await asyncio.gather(waits[0], return_exceptions=True)
    assert set(per_loop) == {2 * INTERVAL}
    waits[1].cancel()
    await asyncio.gather(waits[1], return_exceptions=True)
    assert asyncio.get_running_loop() not in dbos._sys_db._status_watchers


@pytest.mark.asyncio
async def test_waits_read_under_the_callers_shard_key(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The shard key is resolved in the caller's context when it starts waiting, and
    the shared read runs under it."""
    release = threading.Event()

    @DBOS.workflow()
    async def blocked() -> str:
        await asyncio.to_thread(release.wait, 30)
        return "done"

    queue = await DBOS.register_queue_async("result-wait-shard-key")
    handle = await queue.enqueue_async(blocked)
    # A key resolved from the caller's context, as a sharded _key_hint resolves its
    # bindings; the poller runs in an empty context, where it would be None.
    caller_key: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
        "caller_key", default=None
    )
    monkeypatch.setattr(dbos._sys_db, "_key_hint", lambda wfid: caller_key.get())
    keys: List[Optional[str]] = []
    read = dbos._sys_db.get_workflow_status_values

    def recording_read(workflow_ids: List[str]) -> Dict[str, str]:
        keys.extend(bound_shard_key(w) for w in workflow_ids)
        return read(workflow_ids)

    monkeypatch.setattr(dbos._sys_db, "get_workflow_status_values", recording_read)
    token = caller_key.set("key-x")
    wait = asyncio.create_task(handle.get_result(polling_interval_sec=INTERVAL))
    caller_key.reset(token)

    def read_once() -> None:
        assert keys

    await retry_until_success_async(read_once, interval=INTERVAL, max_attempts=200)
    assert set(keys) == {"key-x"}
    release.set()
    assert await wait == "done"


@pytest.mark.asyncio
async def test_status_watcher_keeps_shard_keys_apart(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A workflow ID is unique only within its shard key: the same ID under several
    keys is read per key, and a failed read wakes only that key's waiters."""
    wfid = "test-twins-" + str(uuid.uuid4())

    def read(workflow_ids: List[str]) -> Dict[str, str]:
        assert workflow_ids == [wfid]
        key = bound_shard_key(wfid)
        if key == "key-c":
            raise RuntimeError("shard unavailable")
        if key == "key-b":
            return {wfid: WorkflowStatusString.SUCCESS.value}
        return {wfid: WorkflowStatusString.PENDING.value}

    monkeypatch.setattr(dbos._sys_db, "get_workflow_status_values", read)

    waits = {
        key: asyncio.create_task(dbos._sys_db._await_status(((wfid, key),), INTERVAL))
        for key in ["key-a", "key-b", "key-c"]
    }
    try:
        await asyncio.wait_for(waits["key-b"], timeout=10)
        await asyncio.wait_for(waits["key-c"], timeout=10)
        await asyncio.sleep(3 * INTERVAL)
        assert not waits["key-a"].done()
    finally:
        for w in waits.values():
            w.cancel()


def test_client_waits_are_batched_without_dbos_loop(
    dbos: DBOS, client: DBOSClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A DBOSClient on a plain asyncio.run loop, with the default executor, uses the
    batched read too."""
    release = threading.Event()

    @DBOS.workflow()
    def blocked(i: int) -> int:
        release.wait(30)
        return i

    wfids = [str(uuid.uuid4()) for _ in range(3)]
    for i, wfid in enumerate(wfids):
        with SetWorkflowID(wfid):
            DBOS.start_workflow(blocked, i)
    client_calls: List[List[str]] = []
    read = client._sys_db.get_workflow_status_values

    def recording_read(workflow_ids: List[str]) -> Dict[str, str]:
        client_calls.append(list(workflow_ids))
        return read(workflow_ids)

    monkeypatch.setattr(client._sys_db, "get_workflow_status_values", recording_read)

    async def wait_all() -> Tuple[List[List[str]], List[int]]:
        handles: List[WorkflowHandleAsync[int]] = [
            await client.retrieve_workflow_async(w) for w in wfids
        ]
        waits = [
            asyncio.create_task(h.get_result(polling_interval_sec=INTERVAL))
            for h in handles
        ]

        await _all_registered(client._sys_db, 3)
        start = len(client_calls) + 1

        def polled() -> None:
            assert len(client_calls) >= start + 3

        await retry_until_success_async(polled, interval=INTERVAL, max_attempts=200)
        # Only reads from before the release: finished waiters leave the batch after it.
        shared = client_calls[start : start + 3]
        release.set()
        return shared, list(await asyncio.gather(*waits))

    shared, results = asyncio.run(wait_all())
    assert results == [0, 1, 2]
    assert all(sorted(c) == sorted(wfids) for c in shared)
