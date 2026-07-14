import time
import uuid
from typing import Any, cast

import pytest
from sqlalchemy import event as sa_event

# Public API
from dbos import DBOS, DBOSConfig, SetWorkflowID
from dbos import _dbos as dbos_module
from dbos._client import DBOSClient
from dbos._sys_db import _no_stream_value
from dbos._sys_db_postgres import PostgresSystemDatabase
from tests.conftest import retry_until_success, set_workflow_status

def test_basic_stream_write_read(dbos: DBOS) -> None:
    """Test basic stream write and read functionality."""
    test_values = ["hello", 42, {"key": "value"}, [1, 2, 3], None]
    stream_key = "test_stream"

    @DBOS.workflow()
    def writer_workflow() -> None:
        for value in test_values:
            DBOS.write_stream(stream_key, value)
        DBOS.close_stream(stream_key)

    # Start the writer workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_workflow()

    # Read the stream
    read_values = []
    for value in DBOS.read_stream(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values

    # Read the stream again, verify no changes
    read_values = []
    for value in DBOS.read_stream(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values


def test_stream_read_offset(dbos: DBOS) -> None:
    """Test reading a stream starting from a non-zero offset."""
    stream_key = "offset_stream"

    @DBOS.workflow()
    def writer_workflow() -> None:
        for i in range(5):
            DBOS.write_stream(stream_key, i)
        DBOS.close_stream(stream_key)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_workflow()

    # The default offset of 0 reads the whole stream
    assert list(DBOS.read_stream(wfid, stream_key)) == [0, 1, 2, 3, 4]
    assert list(DBOS.read_stream(wfid, stream_key, offset=0)) == [0, 1, 2, 3, 4]

    # A non-zero offset skips earlier values
    assert list(DBOS.read_stream(wfid, stream_key, offset=2)) == [2, 3, 4]
    assert list(DBOS.read_stream(wfid, stream_key, offset=4)) == [4]

    # An offset at or past the close sentinel yields nothing
    assert list(DBOS.read_stream(wfid, stream_key, offset=5)) == []
    assert list(DBOS.read_stream(wfid, stream_key, offset=100)) == []


@pytest.mark.asyncio
async def test_stream_read_offset_async(dbos: DBOS) -> None:
    """Test async reading a stream starting from a non-zero offset."""
    stream_key = "offset_stream_async"

    @DBOS.workflow()
    async def writer_workflow() -> None:
        for i in range(5):
            await DBOS.write_stream_async(stream_key, i)
        await DBOS.close_stream_async(stream_key)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await writer_workflow()

    values = [v async for v in DBOS.read_stream_async(wfid, stream_key, offset=3)]
    assert values == [3, 4]


def test_stream_read_value_returns_status_and_value(dbos: DBOS) -> None:
    """read_stream_value answers both questions a reader tick asks -- 'is there a value at this offset?' and 'is the workflow still running?' -- in one round trip, from one snapshot."""
    sys_db = dbos._sys_db

    @DBOS.workflow()
    def writer_workflow() -> None:
        DBOS.write_stream("s", 0)
        DBOS.write_stream("s", None)
        DBOS.close_stream("s")

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        DBOS.start_workflow(writer_workflow).get_result()

    # The value at the offset and the status, together.
    status, value = sys_db.read_stream_value(wfid, "s", 0)
    assert status == "SUCCESS"
    assert value == 0

    # A written None is a value, not an absence -- which is why absence needs its own sentinel.
    status, value = sys_db.read_stream_value(wfid, "s", 1)
    assert status == "SUCCESS"
    assert value is None

    # Past the end: still reports status, so the reader can tell "not yet" from "never".
    status, value = sys_db.read_stream_value(wfid, "s", 99)
    assert status == "SUCCESS"
    assert value is _no_stream_value

    # A non-existent workflow is distinguishable from a workflow with no value at the offset.
    status, value = sys_db.read_stream_value(str(uuid.uuid4()), "s", 0)
    assert status is None
    assert value is _no_stream_value


@pytest.mark.asyncio
async def test_stream_read_async_sub_poll_observes_notifications(
    dbos: DBOS, skip_with_sqlite: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """read_stream_async waits on the notification event by sub-polling it, tightly at first and
    relaxed once a value has been awaited a while. Both branches must observe the event promptly.

    This passes a long polling_interval_sec on purpose. The suite's default (0.01s, conftest.py)
    makes `min(remaining, sub_poll) == remaining` whichever branch is taken, so under it the sub-poll
    constants have no effect and a regression in either is invisible.
    """
    # Relax after 0.2s rather than the real 10s, so the test need not idle for the full window.
    monkeypatch.setattr(dbos_module, "_STREAM_EVENT_FAST_POLL_WINDOW_SEC", 0.2)
    # First gap stays inside the window (fast branch); the rest exceed it (relaxed branch).
    gaps = [0.1, 0.5, 0.5]
    written: dict[int, float] = {}

    @DBOS.workflow()
    def writer_workflow() -> None:
        for i, gap in enumerate(gaps):
            DBOS.sleep(gap)
            written[i] = time.time()
            DBOS.write_stream("s", i)
        DBOS.close_stream("s")

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        DBOS.start_workflow(writer_workflow)

    # A fallback re-read interval far longer than any gap: prompt delivery can only come from the
    # sub-poll observing the notification, never from the fallback.
    latencies = []
    async for value in DBOS.read_stream_async(wfid, "s", polling_interval_sec=30.0):
        latencies.append(time.time() - written[value])

    assert len(latencies) == len(gaps)
    # Fast branch: sub-polls at 0.01s, so the value lands almost immediately.
    assert latencies[0] < 0.15, f"fast-branch latency {latencies[0]:.3f}s"
    # Relaxed branch: sub-polls at 0.1s. Raising it to the 1.0s default fallback interval would make
    # the reader sleep clean past each write and land near 0.7s here.
    assert max(latencies[1:]) < 0.3, f"relaxed-branch latencies {latencies[1:]}"


def test_stream_read_is_one_round_trip_per_value(dbos: DBOS) -> None:
    """Each reader tick issues a single query fetching the value and the workflow status together, rather than reading the stream and then looking the status up separately."""
    n = 25

    @DBOS.workflow()
    def writer_workflow() -> None:
        for i in range(n):
            DBOS.write_stream("s", i)
        DBOS.close_stream("s")

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        DBOS.start_workflow(writer_workflow).get_result()

    # Only the reader joins streams to workflow_status, so background threads sharing the engine cannot match.
    # Matched loosely rather than by table name, which is schema-qualified.
    reads = []

    def count(
        conn: Any, cursor: Any, statement: str, params: Any, context: Any, many: bool
    ) -> None:
        s = " ".join(statement.lower().split())
        if "outer join" in s and "streams" in s and "workflow_status" in s:
            reads.append(statement)

    engine = dbos._sys_db.engine
    sa_event.listen(engine, "before_cursor_execute", count)
    try:
        values = list(DBOS.read_stream(wfid, "s"))
    finally:
        sa_event.remove(engine, "before_cursor_execute", count)

    assert values == list(range(n))
    # Guards against passing vacuously: reading the status separately issues no joined query at all.
    assert reads, "reader did not fetch value and status in one joined query"
    # One query per delivered value, plus the one that finds the close sentinel. Two queries per
    # tick (a read then a status lookup) would double this.
    assert (
        len(reads) == n + 1
    ), f"expected {n + 1} reads for {n} values, got {len(reads)}"


def test_client_read_stream_offset(dbos: DBOS, client: DBOSClient) -> None:
    """Test reading a stream from a client starting from a non-zero offset."""
    stream_key = "client_offset_stream"

    @DBOS.workflow()
    def writer_workflow() -> None:
        for i in range(5):
            DBOS.write_stream(stream_key, i)
        DBOS.close_stream(stream_key)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_workflow()

    assert list(client.read_stream(wfid, stream_key, offset=2)) == [2, 3, 4]


@pytest.mark.asyncio
async def test_client_read_stream_offset_async(dbos: DBOS, client: DBOSClient) -> None:
    """Test async reading a stream from a client starting from a non-zero offset."""
    stream_key = "client_offset_stream_async"

    @DBOS.workflow()
    async def writer_workflow() -> None:
        for i in range(5):
            await DBOS.write_stream_async(stream_key, i)
        await DBOS.close_stream_async(stream_key)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await writer_workflow()

    values = [v async for v in client.read_stream_async(wfid, stream_key, offset=3)]
    assert values == [3, 4]


def test_unclosed_stream(dbos: DBOS) -> None:
    """Test that reading from a stream stops when the workflow terminates."""
    test_values = ["hello", 42, {"key": "value"}, [1, 2, 3], None]
    stream_key = "test_stream"

    @DBOS.workflow()
    def writer_workflow() -> None:
        for value in test_values:
            DBOS.write_stream(stream_key, value)

    @DBOS.workflow()
    def writer_workflow_error() -> None:
        for value in test_values:
            DBOS.write_stream(stream_key, value)
        raise Exception()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_workflow()

    read_values = []
    for value in DBOS.read_stream(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            writer_workflow_error()

    read_values = []
    for value in DBOS.read_stream(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values


def test_stream_termination_while_reader_blocked(dbos: DBOS) -> None:
    """A reader that catches up to an open stream while the writer is still
    running must terminate promptly once the workflow completes, even though no
    value or close marker wakes it. Unlike the other unclosed-stream tests, which
    read only after the workflow finished, this forces the blocking wait path."""
    stream_key = "termination_latency_stream"

    @DBOS.workflow()
    def writer_workflow() -> None:
        # Write once, then stay alive without writing or closing, so the reader
        # catches up and blocks waiting for the workflow to terminate.
        DBOS.write_stream(stream_key, "only_value")
        DBOS.sleep(2.0)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(writer_workflow)

    start = time.time()
    read_values = list(DBOS.read_stream(wfid, stream_key))
    elapsed = time.time() - start

    handle.get_result()
    assert read_values == ["only_value"]
    assert elapsed < 10.0, f"reader took {elapsed:.1f}s to notice termination"


def test_stream_concurrent_write_read(dbos: DBOS) -> None:
    """Test reading from a stream while it's being written to."""
    stream_key = "concurrent_stream"
    num_values = 10

    @DBOS.workflow()
    def writer_workflow() -> None:
        for i in range(num_values):
            DBOS.write_stream(stream_key, f"value_{i}")
            # Small delay to simulate real work
            DBOS.sleep(0.5)
        DBOS.close_stream(stream_key)

    # Start the writer workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(writer_workflow)

    # Start reading immediately (while writing)
    read_values = []
    start_time = time.time()

    for value in DBOS.read_stream(wfid, stream_key):
        read_values.append(value)
        # Safety timeout: generous because SQLite busy_timeout stalls can reach 30s,
        # but below the 120s pytest-timeout hard kill
        assert time.time() - start_time < 100

    # Wait for writer to complete
    handle.get_result()

    # Verify all values were read
    expected_values = [f"value_{i}" for i in range(num_values)]
    assert read_values == expected_values


def test_stream_low_latency_delivery(
    config: DBOSConfig, dbos: DBOS, client: DBOSClient, skip_with_sqlite: None
) -> None:
    """Values should reach a blocked reader promptly via LISTEN/NOTIFY rather
    than after a fixed polling interval. Each value carries the wall-clock time
    it was written; the reader asserts it received the value shortly after.
    Verified for the in-process (DBOS) reader with LISTEN/NOTIFY, the
    out-of-process (client) reader (polling), and an in-process reader with
    LISTEN/NOTIFY disabled (polling).

    Skipped on SQLite: lock contention on slow runners can stall writes for
    several seconds, making latency assertions inherently flaky."""
    stream_key = "latency_stream"
    num_values = 3

    @DBOS.workflow()
    def writer_workflow() -> None:
        for _ in range(num_values):
            # Capture the write time as close to the write as possible, then
            # pause so the reader is genuinely blocked waiting for the next one.
            DBOS.write_stream(stream_key, time.time())
            DBOS.sleep(1.0)
        DBOS.close_stream(stream_key)

    def measure(read_iter: Any) -> tuple[int, float]:
        max_latency = 0.0
        count = 0
        for written_at in read_iter:
            max_latency = max(max_latency, time.time() - written_at)
            count += 1
        return count, max_latency

    # In-process DBOS reader: woken by LISTEN/NOTIFY, so delivery is single-digit
    # milliseconds. The threshold leaves headroom for CI stalls while staying
    # well below what a broken wakeup path would produce.
    # The fallback re-read interval is raised for this phase so that a working notification is the
    # only thing that can deliver within the threshold; at the suite default (0.01s, conftest.py)
    # the fallback delivers every value in ~10ms and this phase passes even with no notifier at all.
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(writer_workflow)
    original_interval = dbos._sys_db._notification_listener_polling_interval_sec
    dbos._sys_db._notification_listener_polling_interval_sec = 10.0
    try:
        count, max_latency = measure(DBOS.read_stream(wfid, stream_key))
    finally:
        dbos._sys_db._notification_listener_polling_interval_sec = original_interval
    handle.get_result()
    assert count == num_values
    assert max_latency < 2.0, f"DBOS delivery latency {max_latency:.3f}s too high"

    # Out-of-process client: no notification listener thread, so its event is
    # never signaled and each read falls back to re-reading the offset once
    # event.wait times out (notification_listener_polling_interval_sec, ~1s by
    # default). Verify it still delivers every value, confirming it actually
    # polls rather than blocking forever on a notification that never arrives.
    client_wfid = str(uuid.uuid4())
    with SetWorkflowID(client_wfid):
        client_handle = DBOS.start_workflow(writer_workflow)
    count, max_latency = measure(client.read_stream(client_wfid, stream_key))
    client_handle.get_result()
    assert count == num_values
    assert max_latency < 5.0, f"client delivery latency {max_latency:.3f}s too high"

    # Recreate the in-process DBOS with LISTEN/NOTIFY disabled: the app-side notifier stays idle and no notifications fire, so the reader is woken by the polling listener thread instead.
    DBOS.destroy(destroy_registry=False)
    config["use_listen_notify"] = False
    DBOS(config=config)
    DBOS.launch()

    poll_wfid = str(uuid.uuid4())
    with SetWorkflowID(poll_wfid):
        poll_handle = DBOS.start_workflow(writer_workflow)
    count, max_latency = measure(DBOS.read_stream(poll_wfid, stream_key))
    poll_handle.get_result()
    assert count == num_values
    assert (
        max_latency < 20.0
    ), f"polling DBOS delivery latency {max_latency:.3f}s too high"


@pytest.mark.asyncio
async def test_stream_low_latency_delivery_async(
    config: DBOSConfig, dbos: DBOS, client: DBOSClient, skip_with_sqlite: None
) -> None:
    """Async counterpart of test_stream_low_latency_delivery, exercising the
    read_stream_async paths for the in-process (DBOS) reader with LISTEN/NOTIFY,
    the out-of-process (client) reader (polling), and an in-process reader with
    LISTEN/NOTIFY disabled (polling).

    Skipped on SQLite: lock contention on slow runners can stall writes for
    several seconds, making latency assertions inherently flaky."""
    stream_key = "latency_stream_async"
    num_values = 3

    @DBOS.workflow()
    async def writer_workflow() -> None:
        for _ in range(num_values):
            # Capture the write time as close to the write as possible, then
            # pause so the reader is genuinely blocked waiting for the next one.
            await DBOS.write_stream_async(stream_key, time.time())
            await DBOS.sleep_async(1.0)
        await DBOS.close_stream_async(stream_key)

    async def measure(read_aiter: Any) -> tuple[int, float]:
        max_latency = 0.0
        count = 0
        async for written_at in read_aiter:
            max_latency = max(max_latency, time.time() - written_at)
            count += 1
        return count, max_latency

    # In-process DBOS reader woken by LISTEN/NOTIFY. Force a long polling
    # interval so low latency can only come from a notification, not the poll.
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.start_workflow_async(writer_workflow)
    count, max_latency = await measure(
        DBOS.read_stream_async(wfid, stream_key, polling_interval_sec=60.0)
    )
    await handle.get_result()
    assert count == num_values
    assert max_latency < 2.0, f"DBOS delivery latency {max_latency:.3f}s too high"

    # Out-of-process client: no notification listener thread, so its event is
    # never signaled and each read falls back to re-reading the offset once
    # event.wait times out (notification_listener_polling_interval_sec, ~1s by
    # default). Verify it still delivers every value, confirming it actually
    # polls rather than blocking forever on a notification that never arrives.
    client_wfid = str(uuid.uuid4())
    with SetWorkflowID(client_wfid):
        client_handle = await DBOS.start_workflow_async(writer_workflow)
    count, max_latency = await measure(
        client.read_stream_async(client_wfid, stream_key)
    )
    await client_handle.get_result()
    assert count == num_values
    assert max_latency < 5.0, f"client delivery latency {max_latency:.3f}s too high"

    # Recreate the in-process DBOS with LISTEN/NOTIFY disabled: the app-side notifier stays idle and no notifications fire, so the reader is woken by the polling listener thread instead.
    DBOS.destroy(destroy_registry=False)
    config["use_listen_notify"] = False
    DBOS(config=config)
    DBOS.launch()

    poll_wfid = str(uuid.uuid4())
    with SetWorkflowID(poll_wfid):
        poll_handle = await DBOS.start_workflow_async(writer_workflow)
    count, max_latency = await measure(DBOS.read_stream_async(poll_wfid, stream_key))
    await poll_handle.get_result()
    assert count == num_values
    assert (
        max_latency < 20.0
    ), f"polling DBOS delivery latency {max_latency:.3f}s too high"


def test_stream_notifier_delivers_without_trigger(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """The per-row NOTIFY trigger is dropped (migration 43) and wakeups come from the coalescing app-side notifier: assert the trigger is gone and a reader on a long poll fallback is still woken by the notifier."""
    import sqlalchemy as sa

    sys_db = dbos._sys_db

    # No per-row trigger may remain on the streams table.
    with sys_db.engine.begin() as c:
        trigger = c.execute(
            sa.text(
                "SELECT 1 FROM pg_trigger t "
                "JOIN pg_class cl ON t.tgrelid = cl.oid "
                "JOIN pg_namespace n ON cl.relnamespace = n.oid "
                "WHERE n.nspname = :schema AND cl.relname = 'streams' "
                "AND t.tgname = 'dbos_streams_trigger'"
            ),
            {"schema": sys_db.schema},
        ).fetchone()
    assert trigger is None, "dbos_streams_trigger should have been dropped"

    stream_key = "notifier_stream"

    @DBOS.workflow()
    def writer_workflow() -> None:
        DBOS.write_stream(stream_key, "first")
        # Keep the stream open so the reader blocks, then write a value it can only get via a notification.
        DBOS.sleep(2.0)
        DBOS.write_stream(stream_key, "second")
        DBOS.close_stream(stream_key)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(writer_workflow)

    # Force a poll interval far longer than the write gap so timely delivery must come from the notifier, not the poll.
    original_interval = sys_db._notification_listener_polling_interval_sec
    sys_db._notification_listener_polling_interval_sec = 30.0
    try:
        received = []
        start = time.time()
        for value in DBOS.read_stream(wfid, stream_key):
            received.append((value, time.time() - start))
        handle.get_result()
    finally:
        sys_db._notification_listener_polling_interval_sec = original_interval

    assert [v for v, _ in received] == ["first", "second"]
    # The second value is written ~2s in; the notifier must wake the reader well before the 30s poll would.
    second_latency = received[1][1]
    assert second_latency < 10.0, f"second value took {second_latency:.3f}s to arrive"


def test_stream_notifier_drops_unsendable_payload(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """A batch pg_notify rejects (e.g. a payload over the 8000-byte limit) is dropped, not requeued, so a poison payload can't permanently stall the notifier (H1); the notifier keeps delivering afterward and polling covers the dropped values."""
    sys_db = cast(PostgresSystemDatabase, dbos._sys_db)
    # A stream key past pg_notify's 8000-byte payload limit makes its batch unsendable.
    poison_payload = f"{uuid.uuid4()}::{'x' * 9000}"

    with sys_db._stream_notifier_lock:
        sys_db._pending_stream_notifications = {poison_payload}
    sys_db._flush_stream_notifications()

    # The poison batch is dropped, not requeued (requeuing would loop forever).
    with sys_db._stream_notifier_lock:
        assert sys_db._pending_stream_notifications == set()

    # The notifier still works afterward: a subsequent good payload is delivered.
    good_wf = str(uuid.uuid4())
    good_key = "deliverable"
    event, payload_key = sys_db.register_stream_listener(good_wf, good_key)
    try:
        event.clear()
        with sys_db._stream_notifier_lock:
            sys_db._pending_stream_notifications = {f"{good_wf}::{good_key}"}
        sys_db._flush_stream_notifications()
        assert event.wait(
            timeout=10
        ), "notifier stopped delivering after a poison batch"
    finally:
        sys_db.unregister_stream_listener(payload_key)


def test_stream_notifier_survives_flush_error(
    dbos: DBOS, skip_with_sqlite: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An exception escaping a flush must not kill the notifier thread (M1): it logs, backs off, and resumes delivering. Without the loop guard the thread would die and stop all stream push notifications for the process."""
    sys_db = cast(PostgresSystemDatabase, dbos._sys_db)
    real_flush = sys_db._flush_stream_notifications
    state = {"raised": False}

    def flaky_flush() -> None:
        # Raise once (even on an empty batch) to simulate an unexpected error; then behave normally.
        if not state["raised"]:
            state["raised"] = True
            raise RuntimeError("simulated flush failure")
        real_flush()

    monkeypatch.setattr(sys_db, "_flush_stream_notifications", flaky_flush)

    # Wait until the running notifier thread has hit the injected failure.
    def failure_injected() -> bool:
        assert state["raised"], "notifier has not called flush yet"
        return True

    retry_until_success(failure_injected)

    # The thread must have survived it and still deliver a subsequently signaled stream.
    good_wf = str(uuid.uuid4())
    good_key = "post_error"
    event, payload_key = sys_db.register_stream_listener(good_wf, good_key)
    try:
        event.clear()
        with sys_db._stream_notifier_lock:
            sys_db._pending_stream_notifications.add(f"{good_wf}::{good_key}")
        assert event.wait(
            timeout=10
        ), "notifier did not resume delivering after a flush error"
    finally:
        sys_db.unregister_stream_listener(payload_key)


def test_stream_multiple_keys(dbos: DBOS) -> None:
    """Test multiple streams with different keys in the same workflow."""

    @DBOS.workflow()
    def multi_stream_workflow() -> None:
        # Write to stream A
        DBOS.write_stream("stream_a", "a1")
        DBOS.write_stream("stream_a", "a2")

        # Write to stream B
        DBOS.write_stream("stream_b", "b1")
        DBOS.write_stream("stream_b", "b2")
        DBOS.write_stream("stream_b", "b3")

        # Close both streams
        DBOS.close_stream("stream_a")
        DBOS.close_stream("stream_b")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        multi_stream_workflow()

    # Read stream A
    stream_a_values = list(DBOS.read_stream(wfid, "stream_a"))
    assert stream_a_values == ["a1", "a2"]

    # Read stream B
    stream_b_values = list(DBOS.read_stream(wfid, "stream_b"))
    assert stream_b_values == ["b1", "b2", "b3"]


def test_stream_empty_stream(dbos: DBOS) -> None:
    """Test reading from an empty stream (only close marker)."""

    @DBOS.workflow()
    def empty_stream_workflow() -> None:
        DBOS.close_stream("empty_stream")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        empty_stream_workflow()

    # Read the empty stream
    values = list(DBOS.read_stream(wfid, "empty_stream"))
    assert values == []


class CustomClass:
    def __init__(self, value: str):
        self.value = value

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, CustomClass) and self.value == other.value


def test_stream_serialization_types(dbos: DBOS) -> None:
    """Test that various data types are properly serialized/deserialized."""

    test_values = [
        "string",
        42,
        3.14,
        True,
        False,
        None,
        [1, 2, 3],
        {"nested": {"dict": "value"}},
        CustomClass("test"),
        (1, 2, 3),  # Tuple
        {1, 2, 3},  # Set
    ]

    @DBOS.workflow()
    def serialization_test_workflow() -> None:
        for value in test_values:
            DBOS.write_stream("serialize_test", value)
        DBOS.close_stream("serialize_test")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        serialization_test_workflow()

    # Read and verify
    read_values = list(DBOS.read_stream(wfid, "serialize_test"))

    # Note: Sets and tuples might be deserialized differently due to JSON serialization
    # So we'll check the values more carefully
    assert len(read_values) == len(test_values)

    for i, (original, read) in enumerate(zip(test_values, read_values)):
        if isinstance(original, CustomClass):
            assert isinstance(read, CustomClass)
            assert read.value == original.value
        elif isinstance(original, (set, tuple)):
            # These might be deserialized as lists
            assert list(original) == read or original == read
        else:
            assert read == original


def test_stream_error_cases(dbos: DBOS) -> None:
    """Test error cases and edge conditions."""

    # Test writing to stream outside of workflow
    with pytest.raises(Exception, match="must be called from within a workflow"):
        DBOS.write_stream("test", "value")

    # Test closing stream outside of workflow
    with pytest.raises(Exception, match="must be called from within a workflow"):
        DBOS.close_stream("test")


def test_stream_workflow_recovery(dbos: DBOS) -> None:
    """Test that stream operations are properly recovered during workflow replay."""

    workflow_call_count = 0
    step_call_count = 0

    @DBOS.step()
    def counting_step() -> int:
        nonlocal step_call_count
        step_call_count += 1
        return step_call_count

    @DBOS.workflow()
    def recovery_test_workflow() -> None:
        nonlocal workflow_call_count
        workflow_call_count += 1
        count1 = counting_step()
        DBOS.write_stream("recovery_stream", f"step_{count1}")

        count2 = counting_step()
        DBOS.write_stream("recovery_stream", f"step_{count2}")

        DBOS.close_stream("recovery_stream")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        recovery_test_workflow()

    # Validate stream contents
    values = list(DBOS.read_stream(wfid, "recovery_stream"))
    assert values == ["step_1", "step_2"]

    # Reset call count and run the same workflow ID again (should replay)
    set_workflow_status(dbos._sys_db, wfid, "PENDING")
    dbos._execute_workflow_id(wfid).get_result()

    # The workflow should have been called again
    assert workflow_call_count == 2
    assert step_call_count == 2

    # Stream should still be readable and contain the same values
    values = list(DBOS.read_stream(wfid, "recovery_stream"))
    assert values == ["step_1", "step_2"]

    steps = DBOS.list_workflow_steps(wfid)
    assert len(steps) == 5
    assert steps[1]["function_name"] == "DBOS.writeStream"
    assert steps[3]["function_name"] == "DBOS.writeStream"
    assert steps[4]["function_name"] == "DBOS.closeStream"


def test_stream_large_data(dbos: DBOS) -> None:
    """Test streaming with larger amounts of data."""

    @DBOS.workflow()
    def large_data_workflow() -> None:
        # Write 100 items
        for i in range(100):
            data = {"id": i, "data": f"item_{i}", "large_field": "x" * 1000}
            DBOS.write_stream("large_stream", data)
        DBOS.close_stream("large_stream")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        large_data_workflow()

    # Read all values
    values = list(DBOS.read_stream(wfid, "large_stream"))

    assert len(values) == 100
    for i, value in enumerate(values):
        assert value["id"] == i
        assert value["data"] == f"item_{i}"
        assert value["large_field"] == "x" * 1000


def test_stream_interleaved_operations(dbos: DBOS) -> None:
    """Test interleaved write operations across multiple streams."""

    @DBOS.workflow()
    def interleaved_workflow() -> None:
        DBOS.write_stream("stream1", "1a")
        DBOS.write_stream("stream2", "2a")
        DBOS.write_stream("stream1", "1b")
        DBOS.write_stream("stream3", "3a")
        DBOS.write_stream("stream2", "2b")
        DBOS.write_stream("stream1", "1c")

        DBOS.close_stream("stream1")
        DBOS.close_stream("stream2")
        DBOS.close_stream("stream3")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        interleaved_workflow()

    # Verify each stream has the correct values in order
    stream1_values = list(DBOS.read_stream(wfid, "stream1"))
    stream2_values = list(DBOS.read_stream(wfid, "stream2"))
    stream3_values = list(DBOS.read_stream(wfid, "stream3"))

    assert stream1_values == ["1a", "1b", "1c"]
    assert stream2_values == ["2a", "2b"]
    assert stream3_values == ["3a"]


def test_stream_write_from_step(dbos: DBOS) -> None:
    """Test writing to a stream from inside a step function that retries and throws exceptions."""

    call_count = 0

    @DBOS.step(retries_allowed=True, max_attempts=4, interval_seconds=0)
    def step_that_writes_and_fails(stream_key: str, value: Any) -> int:
        nonlocal call_count
        call_count += 1

        # Always write to stream first
        DBOS.write_stream(stream_key, f"{value}_attempt_{call_count}")

        # Throw exception to trigger retry (will succeed after 3 attempts)
        if call_count < 4:
            raise RuntimeError(f"Step failed on attempt {call_count}")

        step_id = DBOS.step_id
        assert step_id is not None
        return step_id

    @DBOS.workflow()
    def workflow_with_failing_step() -> None:
        # This step will fail 3 times, then succeed on the 4th attempt
        # But each failure should still write to the stream
        result = step_that_writes_and_fails("retry_stream", "test_value")
        assert result == 1

        # Also write directly from workflow
        DBOS.write_stream("retry_stream", "from_workflow")

        # Close the stream
        DBOS.close_stream("retry_stream")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        workflow_with_failing_step()

    # Read the stream and verify all values are present
    # Should have 4 writes from the step (one per attempt) plus 1 from workflow
    stream_values = list(DBOS.read_stream(wfid, "retry_stream"))

    # Verify we have the expected number of values
    assert len(stream_values) == 5

    # Verify the step writes (one per retry attempt)
    assert stream_values[0] == "test_value_attempt_1"
    assert stream_values[1] == "test_value_attempt_2"
    assert stream_values[2] == "test_value_attempt_3"
    assert stream_values[3] == "test_value_attempt_4"

    # Verify the workflow write
    assert stream_values[4] == "from_workflow"

    # Verify the step was called exactly 4 times (3 failures + 1 success)
    assert call_count == 4


@pytest.mark.asyncio
async def test_async_stream_basic_write_read(dbos: DBOS) -> None:
    """Test basic async stream write and read functionality."""
    test_values = [
        "async_hello",
        123,
        {"async_key": "async_value"},
        [10, 20, 30],
        None,
    ]
    stream_key = "async_test_stream"

    @DBOS.workflow()
    async def async_writer_workflow() -> None:
        for value in test_values:
            await DBOS.write_stream_async(stream_key, value)
        await DBOS.close_stream_async(stream_key)

    # Start the writer workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await async_writer_workflow()

    # Read the stream
    read_values = []
    async for value in DBOS.read_stream_async(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values


@pytest.mark.asyncio
async def test_async_stream_concurrent_write_read(dbos: DBOS) -> None:
    """Test async reading from a stream while it's being written to."""

    stream_key = "async_concurrent_stream"
    num_values = 5

    @DBOS.workflow()
    async def async_writer_workflow() -> None:
        for i in range(num_values):
            await DBOS.write_stream_async(stream_key, f"async_value_{i}")
            # Small delay to simulate real work
            await DBOS.sleep_async(0.5)
        await DBOS.close_stream_async(stream_key)

    # Start the writer workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_handle = await DBOS.start_workflow_async(async_writer_workflow)

    # Start reading immediately (while writing)
    read_values = []
    start_time = time.time()

    async for value in DBOS.read_stream_async(wfid, stream_key):
        read_values.append(value)
        # Safety timeout: generous because SQLite busy_timeout stalls can reach 30s,
        # but below the 120s pytest-timeout hard kill
        assert time.time() - start_time < 100

    # Wait for writer to complete
    await writer_handle.get_result()

    # Verify all values were read
    expected_values = [f"async_value_{i}" for i in range(num_values)]
    assert read_values == expected_values


@pytest.mark.asyncio
async def test_async_stream_empty_stream(dbos: DBOS) -> None:
    """Test async reading from an empty stream (only close marker)."""

    @DBOS.workflow()
    async def async_empty_stream_workflow() -> None:
        await DBOS.close_stream_async("async_empty_stream")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await async_empty_stream_workflow()

    # Read the empty stream
    values = []
    async for value in DBOS.read_stream_async(wfid, "async_empty_stream"):
        values.append(value)
    assert values == []


@pytest.mark.asyncio
async def test_unclosed_stream_async(dbos: DBOS) -> None:
    """Test that reading from a stream stops when the workflow terminates."""
    test_values = ["hello", 42, {"key": "value"}, [1, 2, 3], None]
    stream_key = "test_stream"

    @DBOS.workflow()
    async def writer_workflow() -> None:
        for value in test_values:
            await DBOS.write_stream_async(stream_key, value)

    @DBOS.workflow()
    async def writer_workflow_error() -> None:
        for value in test_values:
            await DBOS.write_stream_async(stream_key, value)
        raise Exception()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await writer_workflow()

    read_values = []
    async for value in DBOS.read_stream_async(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            await writer_workflow_error()

    read_values = []
    async for value in DBOS.read_stream_async(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values


@pytest.mark.asyncio
async def test_stream_termination_while_reader_blocked_async(dbos: DBOS) -> None:
    """Async counterpart of test_stream_termination_while_reader_blocked,
    exercising the read_stream_async termination path."""
    stream_key = "termination_latency_stream_async"

    @DBOS.workflow()
    async def writer_workflow() -> None:
        # Write once, then stay alive without writing or closing, so the reader
        # catches up and blocks waiting for the workflow to terminate.
        await DBOS.write_stream_async(stream_key, "only_value")
        await DBOS.sleep_async(2.0)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.start_workflow_async(writer_workflow)

    start = time.time()
    read_values = []
    async for value in DBOS.read_stream_async(wfid, stream_key):
        read_values.append(value)
    elapsed = time.time() - start

    await handle.get_result()
    assert read_values == ["only_value"]
    # Termination fires no notification, so the reader only notices once its
    # event.wait times out and re-checks the workflow status (one polling
    # interval, ~1s by default); comfortably under the 10s bound.
    assert elapsed < 10.0, f"reader took {elapsed:.1f}s to notice termination"


def test_client_read_stream(dbos: DBOS, client: DBOSClient) -> None:
    """Test reading streams from a DBOS client."""
    test_values = [
        "client_hello",
        99,
        {"client_key": "client_value"},
        [100, 200, 300],
        None,
    ]
    stream_key = "client_test_stream"

    @DBOS.workflow()
    def client_writer_workflow() -> None:
        for value in test_values:
            DBOS.write_stream(stream_key, value)
        DBOS.close_stream(stream_key)

    # Start the writer workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        client_writer_workflow()

    # Create a client and read the stream
    try:
        read_values = []
        for value in client.read_stream(wfid, stream_key):
            read_values.append(value)

        assert read_values == test_values
    finally:
        client.destroy()


@pytest.mark.asyncio
async def test_client_read_stream_async(dbos: DBOS, client: DBOSClient) -> None:
    """Test async reading streams from a DBOS client."""
    test_values = [
        "async_client_hello",
        88,
        {"async_client_key": "async_client_value"},
        [11, 22, 33],
        None,
    ]
    stream_key = "async_client_test_stream"

    @DBOS.workflow()
    async def async_client_writer_workflow() -> None:
        for value in test_values:
            await DBOS.write_stream_async(stream_key, value)
        await DBOS.close_stream_async(stream_key)

    # Start the writer workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await async_client_writer_workflow()

    # Create a client and read the stream asynchronously
    try:
        read_values = []
        async for value in client.read_stream_async(wfid, stream_key):
            read_values.append(value)

        assert read_values == test_values
    finally:
        client.destroy()


def test_client_read_stream_is_one_round_trip_per_value(
    dbos: DBOS, client: DBOSClient
) -> None:
    """The client reader fetches value and status in one joined query per tick, like the in-process one."""
    n = 25

    @DBOS.workflow()
    def writer_workflow() -> None:
        for i in range(n):
            DBOS.write_stream("s", i)
        DBOS.close_stream("s")

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        DBOS.start_workflow(writer_workflow).get_result()

    reads = []

    def count(
        conn: Any, cursor: Any, statement: str, params: Any, context: Any, many: bool
    ) -> None:
        s = " ".join(statement.lower().split())
        if "outer join" in s and "streams" in s and "workflow_status" in s:
            reads.append(statement)

    engine = client._sys_db.engine
    sa_event.listen(engine, "before_cursor_execute", count)
    try:
        values = list(client.read_stream(wfid, "s"))
    finally:
        sa_event.remove(engine, "before_cursor_execute", count)

    assert values == list(range(n))
    # Guards against passing vacuously: reading the status separately issues no joined query at all.
    assert reads, "client did not fetch value and status in one joined query"
    # One query per delivered value, plus the one that finds the close sentinel.
    assert (
        len(reads) == n + 1
    ), f"expected {n + 1} reads for {n} values, got {len(reads)}"


def test_client_read_stream_nonexistent_workflow(
    dbos: DBOS, client: DBOSClient
) -> None:
    """A stream on an unknown workflow ends the client's generator quietly, where the in-process reader raises. Preserved deliberately: the batch read reports a missing workflow the same way as a workflow with nothing buffered."""
    assert list(client.read_stream(str(uuid.uuid4()), "s")) == []


def test_client_read_stream_workflow_termination(
    dbos: DBOS, client: DBOSClient
) -> None:
    """Test that client read_stream stops when workflow terminates without closing stream."""
    test_values = ["terminated_1", "terminated_2", "terminated_3"]
    stream_key = "termination_test_stream"

    @DBOS.workflow()
    def terminating_workflow() -> None:
        for value in test_values:
            DBOS.write_stream(stream_key, value)
        # Intentionally don't close the stream

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        terminating_workflow()

    # Create a client and read the stream - should stop when workflow terminates
    try:
        read_values = []
        for value in client.read_stream(wfid, stream_key):
            read_values.append(value)

        assert read_values == test_values
    finally:
        client.destroy()


@pytest.mark.asyncio
async def test_client_read_stream_async_workflow_termination(
    dbos: DBOS, client: DBOSClient
) -> None:
    """Test that client read_stream_async stops when workflow terminates without closing stream."""
    test_values = ["async_terminated_1", "async_terminated_2", "async_terminated_3"]
    stream_key = "async_termination_test_stream"

    @DBOS.workflow()
    async def async_terminating_workflow() -> None:
        for value in test_values:
            await DBOS.write_stream_async(stream_key, value)
        # Intentionally don't close the stream

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await async_terminating_workflow()

    # Create a client and read the stream asynchronously - should stop when workflow terminates
    try:
        read_values = []
        async for value in client.read_stream_async(wfid, stream_key):
            read_values.append(value)

        assert read_values == test_values
    finally:
        client.destroy()
