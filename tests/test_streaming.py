import asyncio
import threading
import time
import uuid
from typing import Any, cast

import pytest
import sqlalchemy as sa
from sqlalchemy import event as sa_event

# Public API
from dbos import DBOS, DBOSConfig, Queue, SetWorkflowID
from dbos._client import DBOSClient
from dbos._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSNonExistentWorkflowError,
    DBOSStreamTimeoutError,
)
from dbos._serialization import WorkflowSerializationFormat, serialize_value
from dbos._sys_db import _dbos_streams_channel, _no_stream_value
from dbos._sys_db_postgres import PostgresSystemDatabase
from tests.conftest import (
    reexecute_workflow_by_id,
    retry_until_success,
    set_workflow_status,
    wait_for_client_listener,
)


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
async def test_stream_read_async_wakes_on_notification(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """read_stream_async awaits the notification event, so a write wakes it immediately
    rather than at the next fallback re-read.

    This passes a long polling_interval_sec on purpose. The suite's default (0.01s, conftest.py)
    re-reads faster than any notification arrives, so under it a regression in the wakeup path
    would be invisible.
    """
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

    # A fallback re-read interval far longer than any gap: prompt delivery can only come from
    # the notification waking the reader, never from the fallback.
    latencies = []
    async for value in DBOS.read_stream_async(wfid, "s", polling_interval_sec=30.0):
        latencies.append(time.time() - written[value])

    assert len(latencies) == len(gaps)
    # Each value should arrive within a notification cycle; tolerate one CI stall but keep the rest prompt and none near the 30s fallback (a broken wakeup drains all three at ~30s).
    ordered = sorted(latencies)
    assert ordered[1] < 0.15, f"notification latencies {latencies}"
    assert ordered[-1] < 2.0, f"notification latencies {latencies}"


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
    count, max_latency = measure(
        DBOS.read_stream(wfid, stream_key, polling_interval_sec=10.0)
    )
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

    with sys_db._notifier_lock:
        sys_db._pending_notifications = {_dbos_streams_channel: {poison_payload}}
    sys_db._flush_notifications()

    # The poison batch is dropped, not requeued (requeuing would loop forever).
    with sys_db._notifier_lock:
        assert sys_db._pending_notifications.get(_dbos_streams_channel, set()) == set()

    # The notifier still works afterward: a subsequent good payload is delivered.
    good_wf = str(uuid.uuid4())
    good_key = "deliverable"
    event, payload_key = sys_db.register_stream_listener(good_wf, good_key)
    try:
        event.clear()
        with sys_db._notifier_lock:
            sys_db._pending_notifications = {
                _dbos_streams_channel: {f"{good_wf}::{good_key}"}
            }
        sys_db._flush_notifications()
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
    real_flush = sys_db._flush_notifications
    state = {"raised": False}

    def flaky_flush() -> None:
        # Raise once (even on an empty batch) to simulate an unexpected error; then behave normally.
        if not state["raised"]:
            state["raised"] = True
            raise RuntimeError("simulated flush failure")
        real_flush()

    monkeypatch.setattr(sys_db, "_flush_notifications", flaky_flush)

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
        with sys_db._notifier_lock:
            sys_db._pending_notifications.setdefault(_dbos_streams_channel, set()).add(
                f"{good_wf}::{good_key}"
            )
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
    DBOS._recover_pending_workflows()
    DBOS.retrieve_workflow(wfid).get_result()

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
    """Test writing to a stream from inside a step function that retries and throws exceptions.

    The stream is closed from a step too, which is allowed wherever writing is."""

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

    @DBOS.step()
    def step_that_closes(stream_key: str) -> None:
        DBOS.close_stream(stream_key)

    @DBOS.workflow()
    def workflow_with_failing_step() -> None:
        # This step will fail 3 times, then succeed on the 4th attempt
        # But each failure should still write to the stream
        result = step_that_writes_and_fails("retry_stream", "test_value")
        assert result == 1

        # Also write directly from workflow
        DBOS.write_stream("retry_stream", "from_workflow")

        # Close the stream from a step, as a workflow may
        step_that_closes("retry_stream")

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


def test_client_read_stream_listen_notify(
    config: DBOSConfig, dbos: DBOS, skip_with_sqlite: None
) -> None:
    """A client with use_listen_notify=True has the app's notifications wake its
    blocked stream reader, rather than the reader noticing values only on its own
    re-read. Each value carries the time it was written; the reader's re-read
    interval is raised so that a delivered notification is the only thing that can
    meet the latency bound. Without one, every value arrives a re-read interval
    late (~10s) and the assertion fails.

    Skipped on SQLite, which has no LISTEN/NOTIFY."""
    stream_key = "listen_notify_client_stream"
    num_values = 3

    @DBOS.workflow()
    def listen_notify_writer_workflow() -> None:
        for _ in range(num_values):
            # Capture the write time as close to the write as possible, then pause
            # so the reader is genuinely blocked waiting for the next value.
            DBOS.write_stream(stream_key, time.time())
            DBOS.sleep(1.0)
        DBOS.close_stream(stream_key)

    assert config["system_database_url"] is not None
    client = DBOSClient(
        system_database_url=config["system_database_url"],
        use_listen_notify=True,
    )
    try:
        # Subscribe before any value is written, so no notification can be missed.
        wait_for_client_listener(client)
        client._sys_db._notification_listener_polling_interval_sec = 10.0
        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid):
            handle = DBOS.start_workflow(listen_notify_writer_workflow)

        max_latency = 0.0
        count = 0
        for written_at in client.read_stream(wfid, stream_key):
            max_latency = max(max_latency, time.time() - written_at)
            count += 1
        handle.get_result()
    finally:
        client.destroy()

    assert count == num_values
    assert max_latency < 2.0, f"client delivery latency {max_latency:.3f}s too high"


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


def test_read_stream_nonexistent_workflow(dbos: DBOS, client: DBOSClient) -> None:
    """A stream on an unknown workflow raises rather than reading as empty, from a client as well as in-process."""
    with pytest.raises(DBOSNonExistentWorkflowError):
        list(client.read_stream(str(uuid.uuid4()), "s"))
    with pytest.raises(DBOSNonExistentWorkflowError):
        list(DBOS.read_stream(str(uuid.uuid4()), "s"))


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


def test_workflow_read_stream_checkpointing(dbos: DBOS) -> None:
    """A workflow reading a stream records one step per value, so a replay re-yields what it
    read rather than re-reading a stream that has moved on. Reads from a step are not recorded.
    """
    stream_key = "checkpointed_stream"
    # The bytes have no portable JSON form, so they only checkpoint under the app's serializer.
    test_values: list[Any] = ["a", None, {"k": "v"}, b"bytes"]
    reader_calls = 0
    crashing_reader_attempts = 0
    step_calls = 0

    @DBOS.workflow()
    def writer_workflow() -> None:
        for value in test_values:
            DBOS.write_stream(stream_key, value)
        # Left unclosed: a reader stops once the writer goes terminal.

    @DBOS.workflow()
    def reader_workflow(target_id: str) -> list[Any]:
        nonlocal reader_calls
        reader_calls += 1
        return list(DBOS.read_stream(target_id, stream_key))

    @DBOS.workflow()
    def crashing_reader_workflow(target_id: str) -> list[Any]:
        nonlocal crashing_reader_attempts
        crashing_reader_attempts += 1
        seen: list[Any] = []
        for value in DBOS.read_stream(target_id, stream_key):
            seen.append(value)
            if crashing_reader_attempts == 1 and len(seen) == 2:
                raise Exception("reader crashed")
        return seen

    @DBOS.step()
    def counting_step() -> int:
        nonlocal step_calls
        step_calls += 1
        return step_calls

    @DBOS.workflow()
    def partial_reader_workflow(target_id: str) -> list[Any]:
        seen: list[Any] = []
        for value in DBOS.read_stream(target_id, stream_key):
            seen.append(value)
            if len(seen) == 2:
                break
        return [seen, counting_step()]

    @DBOS.workflow(serialization_type=WorkflowSerializationFormat.PORTABLE)
    def portable_reader_workflow(target_id: str) -> int:
        return len(list(DBOS.read_stream(target_id, stream_key)))

    @DBOS.step()
    def read_in_step(target_id: str) -> list[Any]:
        return list(DBOS.read_stream(target_id, stream_key))

    @DBOS.workflow()
    def step_reader_workflow(target_id: str) -> list[Any]:
        return read_in_step(target_id)

    writer_id = str(uuid.uuid4())
    with SetWorkflowID(writer_id):
        writer_workflow()

    # One step per value, including the None, plus one recording that the stream ended.
    reader_id = str(uuid.uuid4())
    with SetWorkflowID(reader_id):
        assert reader_workflow(writer_id) == test_values
    assert [s["function_name"] for s in DBOS.list_workflow_steps(reader_id)] == [
        "DBOS.readStream"
    ] * (len(test_values) + 1)

    # A reader that failed partway records only what it delivered, then reads on from there.
    crashing_reader_id = str(uuid.uuid4())
    with SetWorkflowID(crashing_reader_id):
        with pytest.raises(Exception, match="reader crashed"):
            crashing_reader_workflow(writer_id)
    assert len(DBOS.list_workflow_steps(crashing_reader_id)) == 2
    assert (
        reexecute_workflow_by_id(dbos, crashing_reader_id).get_result() == test_values
    )
    assert crashing_reader_attempts == 2

    # Each value consumes a function id, so a step after a half-read stream still replays.
    partial_reader_id = str(uuid.uuid4())
    with SetWorkflowID(partial_reader_id):
        assert partial_reader_workflow(writer_id) == [test_values[:2], 1]
    assert reexecute_workflow_by_id(dbos, partial_reader_id).get_result() == [
        test_values[:2],
        1,
    ]
    assert step_calls == 1

    # Checkpoints use the app's serializer, not the workflow's declared interop format, so a
    # portable workflow reads a value with no portable form just as any other workflow does.
    portable_reader_id = str(uuid.uuid4())
    with SetWorkflowID(portable_reader_id):
        handle = Queue("portable_reader_queue").enqueue(
            portable_reader_workflow, writer_id
        )
    assert handle.get_result() == len(test_values)

    # A read inside a step is covered by that step's own checkpoint.
    step_reader_id = str(uuid.uuid4())
    with SetWorkflowID(step_reader_id):
        assert step_reader_workflow(writer_id) == test_values
    step_reader_steps = DBOS.list_workflow_steps(step_reader_id)
    assert len(step_reader_steps) == 1
    assert step_reader_steps[0]["function_name"] != "DBOS.readStream"

    # Extend the stream now the readers are done, so a live re-read would see more than they did.
    for value in ["d", "e"]:
        dbos._sys_db.write_stream_from_step(
            writer_id,
            100,
            stream_key,
            value,
            serialization_type=WorkflowSerializationFormat.DEFAULT,
        )
    assert list(DBOS.read_stream(writer_id, stream_key)) == test_values + ["d", "e"]

    assert reexecute_workflow_by_id(dbos, reader_id).get_result() == test_values
    assert reader_calls == 2


@pytest.mark.asyncio
async def test_workflow_read_stream_async_checkpointing(dbos: DBOS) -> None:
    """read_stream_async checkpoints its values too, so a replayed reader resumes where it
    stopped and re-yields what it read rather than what the stream holds now."""
    stream_key = "async_checkpointed_stream"
    test_values: list[Any] = ["a", None, {"k": "v"}]
    reader_calls = 0
    crashing_reader_attempts = 0

    @DBOS.workflow()
    async def async_writer_workflow() -> None:
        for value in test_values:
            await DBOS.write_stream_async(stream_key, value)
        # Left unclosed: a reader stops once the writer goes terminal.

    @DBOS.workflow()
    async def async_reader_workflow(target_id: str) -> list[Any]:
        nonlocal reader_calls
        reader_calls += 1
        return [v async for v in DBOS.read_stream_async(target_id, stream_key)]

    @DBOS.workflow()
    async def async_crashing_reader_workflow(target_id: str) -> list[Any]:
        nonlocal crashing_reader_attempts
        crashing_reader_attempts += 1
        seen: list[Any] = []
        async for value in DBOS.read_stream_async(target_id, stream_key):
            seen.append(value)
            if crashing_reader_attempts == 1 and len(seen) == 2:
                raise Exception("reader crashed")
        return seen

    async def replay(workflow_id: str) -> Any:
        # Recovery is fundamentally sync, so run it off the event loop.
        return await asyncio.to_thread(
            lambda: reexecute_workflow_by_id(dbos, workflow_id).get_result()
        )

    writer_id = str(uuid.uuid4())
    with SetWorkflowID(writer_id):
        await async_writer_workflow()

    # One step per value, including the None, plus one recording that the stream ended.
    reader_id = str(uuid.uuid4())
    with SetWorkflowID(reader_id):
        assert await async_reader_workflow(writer_id) == test_values
    steps = await DBOS.list_workflow_steps_async(reader_id)
    assert [s["function_name"] for s in steps] == ["DBOS.readStream"] * (
        len(test_values) + 1
    )

    # A reader that failed partway records only what it delivered, then reads on from there.
    crashing_reader_id = str(uuid.uuid4())
    with SetWorkflowID(crashing_reader_id):
        with pytest.raises(Exception, match="reader crashed"):
            await async_crashing_reader_workflow(writer_id)
    assert len(await DBOS.list_workflow_steps_async(crashing_reader_id)) == 2
    assert await replay(crashing_reader_id) == test_values
    assert crashing_reader_attempts == 2

    # Extend the stream now the readers are done, so a live re-read would see more than they did.
    for value in ["d", "e"]:
        dbos._sys_db.write_stream_from_step(
            writer_id,
            100,
            stream_key,
            value,
            serialization_type=WorkflowSerializationFormat.DEFAULT,
        )

    assert await replay(reader_id) == test_values
    assert reader_calls == 2


def _insert_stream_value_without_notifying(
    dbos: DBOS, workflow_id: str, key: str, value: str
) -> None:
    """Append to a stream without signaling its channel, so only a re-read can find the value."""
    sys_db = dbos._sys_db
    serialized, serialization = serialize_value(
        value, WorkflowSerializationFormat.DEFAULT, sys_db.serializer
    )
    stmt = sys_db._stream_insert_stmt(workflow_id, 0, key, serialized, serialization)
    with sys_db.engine.begin() as c:
        c.execute(stmt)


def test_read_stream_timeout(dbos: DBOS, client: DBOSClient) -> None:
    """Every reader takes a per-value timeout and a per-call polling interval. The writer stays
    active and silent, so only the timeout can end the wait, and a value delivered in between
    restarts the clock rather than counting against a single overall deadline. Values land
    without a notification and the configured fallback is set far longer than the test can wait,
    so nothing is delivered unless the per-call interval is honored."""
    stream_key = "timeout_stream"
    release = threading.Event()

    @DBOS.workflow()
    def writer_workflow() -> None:
        # Stay active and silent so a reader waits rather than draining to the end.
        release.wait()

    def insert_once_reader_blocks(value: str, delay: float) -> threading.Thread:
        def run() -> None:
            time.sleep(delay)
            _insert_stream_value_without_notifying(dbos, wfid, stream_key, value)

        thread = threading.Thread(target=run)
        thread.start()
        return thread

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(writer_workflow)

    sys_dbs = [dbos._sys_db, client._sys_db]
    originals = [db._notification_listener_polling_interval_sec for db in sys_dbs]
    for db in sys_dbs:
        db._notification_listener_polling_interval_sec = 30.0
    try:
        with pytest.raises(DBOSStreamTimeoutError):
            next(DBOS.read_stream(wfid, stream_key, timeout_seconds=0.3))
        with pytest.raises(DBOSStreamTimeoutError):
            next(client.read_stream(wfid, stream_key, timeout_seconds=0.3))

        # Two values, each arriving within the timeout but together exceeding it: a per-value
        # clock delivers both, a single overall deadline would raise on the second. Every margin
        # here is half the timeout -- the gaps and the timeout are chosen to maximize the smallest.
        threads = [
            insert_once_reader_blocks("v0", 1.0),
            insert_once_reader_blocks("v1", 2.0),
        ]
        reader = DBOS.read_stream(
            wfid, stream_key, timeout_seconds=1.5, polling_interval_sec=0.05
        )
        assert [next(reader), next(reader)] == ["v0", "v1"]
        for thread in threads:
            thread.join()

        # A terminal writer drains to the end of the stream rather than timing out.
        release.set()
        handle.get_result()
        assert list(
            client.read_stream(
                wfid, stream_key, timeout_seconds=0.3, polling_interval_sec=0.05
            )
        ) == ["v0", "v1"]

        with pytest.raises(ValueError, match="must not be negative"):
            next(DBOS.read_stream(wfid, stream_key, timeout_seconds=-1))
        # A zero interval would never wait, so the reader would spin on the database.
        for bad in (0, -1, float("inf")):
            with pytest.raises(ValueError, match="at least 0.001"):
                next(DBOS.read_stream(wfid, stream_key, polling_interval_sec=bad))
            with pytest.raises(ValueError, match="at least 0.001"):
                DBOS.read_stream_offset(wfid, stream_key, 0, polling_interval_sec=bad)
            with pytest.raises(ValueError, match="at least 0.001"):
                next(client.read_stream(wfid, stream_key, polling_interval_sec=bad))
    finally:
        for db, original in zip(sys_dbs, originals):
            db._notification_listener_polling_interval_sec = original
        release.set()
        handle.get_result()


@pytest.mark.asyncio
async def test_read_stream_async_timeout(dbos: DBOS, client: DBOSClient) -> None:
    """Async sibling of test_read_stream_timeout."""
    stream_key = "async_timeout_stream"
    release = asyncio.Event()

    @DBOS.workflow()
    async def async_writer_workflow() -> None:
        # Stay active and silent so a reader waits rather than draining to the end.
        await release.wait()

    async def insert_once_reader_blocks(value: str) -> None:
        await asyncio.sleep(0.2)
        await asyncio.to_thread(
            _insert_stream_value_without_notifying, dbos, wfid, stream_key, value
        )

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.start_workflow_async(async_writer_workflow)

    sys_dbs = [dbos._sys_db, client._sys_db]
    originals = [db._notification_listener_polling_interval_sec for db in sys_dbs]
    for db in sys_dbs:
        db._notification_listener_polling_interval_sec = 30.0
    try:
        with pytest.raises(DBOSStreamTimeoutError):
            await DBOS.read_stream_async(
                wfid, stream_key, timeout_seconds=0.3
            ).__anext__()
        with pytest.raises(DBOSStreamTimeoutError):
            await client.read_stream_async(
                wfid, stream_key, timeout_seconds=0.3
            ).__anext__()

        # Delivered only if the per-call interval is honored: the fallback is 30s.
        inserter = asyncio.create_task(insert_once_reader_blocks("v0"))
        assert (
            await DBOS.read_stream_async(
                wfid, stream_key, timeout_seconds=1.5, polling_interval_sec=0.05
            ).__anext__()
            == "v0"
        )
        await inserter
    finally:
        for db, original in zip(sys_dbs, originals):
            db._notification_listener_polling_interval_sec = original
        release.set()
        await handle.get_result()


def test_workflow_read_stream_timeout_is_checkpointed(dbos: DBOS) -> None:
    """A timeout is an outcome, not a failure of the read, so it is recorded: a replayed reader
    raises it again straight from its checkpoint instead of waiting out the timeout a second time.
    """
    stream_key = "timeout_checkpoint_stream"
    release = threading.Event()
    attempts = 0

    @DBOS.workflow()
    def writer_workflow() -> None:
        DBOS.write_stream(stream_key, "only")
        # Stay active and silent so the reader times out waiting for a second value.
        release.wait()

    @DBOS.workflow()
    def reader_workflow(target_id: str) -> list[Any]:
        nonlocal attempts
        attempts += 1
        seen: list[Any] = []
        try:
            for value in DBOS.read_stream(target_id, stream_key, timeout_seconds=1.0):
                seen.append(value)
        except DBOSStreamTimeoutError:
            seen.append("timed out")
        return seen

    @DBOS.workflow(serialization_type=WorkflowSerializationFormat.PORTABLE)
    def portable_reader_workflow(target_id: str) -> list[Any]:
        seen: list[Any] = []
        try:
            for value in DBOS.read_stream(target_id, stream_key, timeout_seconds=1.0):
                seen.append(value)
        except DBOSStreamTimeoutError:
            seen.append("timed out")
        return seen

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(writer_workflow)
    try:
        reader_id = str(uuid.uuid4())
        with SetWorkflowID(reader_id):
            assert reader_workflow(wfid) == ["only", "timed out"]

        # The delivered value and the timeout that followed it, one step each, the timeout
        # recorded as the step's error rather than as an output.
        steps = DBOS.list_workflow_steps(reader_id)
        assert [s["function_name"] for s in steps] == ["DBOS.readStream"] * 2
        assert steps[0]["error"] is None and steps[0]["output"] == "only"
        assert steps[1]["output"] is None
        assert isinstance(steps[1]["error"], DBOSStreamTimeoutError)

        # Replays from the checkpoint: the writer is still silent, so a live re-read would wait
        # out the full timeout again before raising.
        start = time.time()
        assert reexecute_workflow_by_id(dbos, reader_id).get_result() == [
            "only",
            "timed out",
        ]
        assert attempts == 2
        assert time.time() - start < 0.5

        # A portable workflow records the timeout under the app's serializer too, so its replay
        # rebuilds the exact exception type rather than the PortableWorkflowError that portable
        # serialization would have flattened it into.
        portable_id = str(uuid.uuid4())
        with SetWorkflowID(portable_id):
            assert portable_reader_workflow(wfid) == ["only", "timed out"]
        assert reexecute_workflow_by_id(dbos, portable_id).get_result() == [
            "only",
            "timed out",
        ]
    finally:
        release.set()
        handle.get_result()


def test_read_stream_offset(dbos: DBOS, client: DBOSClient) -> None:
    """read_stream_offset returns the one value at an offset, waiting for it and raising if it
    never arrives -- because the timeout passed or because the stream ended short of it. From a
    workflow it is one checkpointed step, so a replay returns the recorded value or re-raises.
    """
    stream_key = "offset_stream"
    release = threading.Event()
    reader_calls = 0

    @DBOS.workflow()
    def writer_workflow() -> None:
        DBOS.write_stream(stream_key, "v0")
        DBOS.write_stream(stream_key, "v1")
        # Stay active so a read past the end waits rather than giving up immediately.
        release.wait()
        DBOS.close_stream(stream_key)

    @DBOS.workflow()
    def reader_workflow(target_id: str) -> list[Any]:
        nonlocal reader_calls
        reader_calls += 1
        seen: list[Any] = [DBOS.read_stream_offset(target_id, stream_key, 1)]
        try:
            DBOS.read_stream_offset(target_id, stream_key, 5, timeout_seconds=0.3)
        except DBOSStreamTimeoutError:
            seen.append("timed out")
        return seen

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(writer_workflow)
    try:
        # A value already written comes back without waiting, in process and from a client.
        assert DBOS.read_stream_offset(wfid, stream_key, 0) == "v0"
        assert client.read_stream_offset(wfid, stream_key, 1) == "v1"

        # An offset the stream has not reached waits, then raises.
        with pytest.raises(DBOSStreamTimeoutError):
            DBOS.read_stream_offset(wfid, stream_key, 5, timeout_seconds=0.3)

        with pytest.raises(ValueError, match="must not be negative"):
            DBOS.read_stream_offset(wfid, stream_key, 0, timeout_seconds=-1)

        # One step per read, the timed-out one recorded as an error, and a replay repeats both
        # without waiting out the timeout again.
        reader_id = str(uuid.uuid4())
        with SetWorkflowID(reader_id):
            assert reader_workflow(wfid) == ["v1", "timed out"]
        steps = DBOS.list_workflow_steps(reader_id)
        assert [s["function_name"] for s in steps] == ["DBOS.readStreamOffset"] * 2
        assert steps[0]["output"] == "v1" and steps[1]["output"] is None
        assert isinstance(steps[1]["error"], DBOSStreamTimeoutError)

        start = time.time()
        assert reexecute_workflow_by_id(dbos, reader_id).get_result() == [
            "v1",
            "timed out",
        ]
        assert reader_calls == 2
        assert time.time() - start < 0.3
    finally:
        release.set()
        handle.get_result()

    # A closed stream gives up on an offset it never reached, rather than waiting out a timeout.
    start = time.time()
    with pytest.raises(DBOSStreamTimeoutError):
        DBOS.read_stream_offset(wfid, stream_key, 5, timeout_seconds=30.0)
    assert time.time() - start < 1.0


@pytest.mark.asyncio
async def test_read_stream_offset_async(dbos: DBOS, client: DBOSClient) -> None:
    """Async sibling of test_read_stream_offset."""
    stream_key = "async_offset_stream"
    release = asyncio.Event()

    @DBOS.workflow()
    async def async_writer_workflow() -> None:
        await DBOS.write_stream_async(stream_key, "v0")
        # Stay active so a read past the end waits rather than giving up immediately.
        await release.wait()

    @DBOS.workflow()
    async def async_reader_workflow(target_id: str) -> Any:
        return await DBOS.read_stream_offset_async(target_id, stream_key, 0)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.start_workflow_async(async_writer_workflow)
    try:
        assert await DBOS.read_stream_offset_async(wfid, stream_key, 0) == "v0"
        assert await client.read_stream_offset_async(wfid, stream_key, 0) == "v0"

        with pytest.raises(DBOSStreamTimeoutError):
            await DBOS.read_stream_offset_async(
                wfid, stream_key, 5, timeout_seconds=0.3
            )

        reader_id = str(uuid.uuid4())
        with SetWorkflowID(reader_id):
            assert await async_reader_workflow(wfid) == "v0"
        steps = await DBOS.list_workflow_steps_async(reader_id)
        assert [s["function_name"] for s in steps] == ["DBOS.readStreamOffset"]
    finally:
        release.set()
        await handle.get_result()


class AmbiguousTruth:
    """Stands in for a numpy array or pandas DataFrame: comparing one yields a value whose truth
    cannot be tested, so a bare `value == sentinel` raises instead of returning False.
    """

    def __init__(self, tag: str) -> None:
        self.tag = tag

    def __eq__(self, other: Any) -> Any:
        return AmbiguousTruth(f"{self.tag}=={other}")

    def __bool__(self) -> bool:
        raise ValueError("The truth value of AmbiguousTruth is ambiguous")

    __hash__ = None  # type: ignore[assignment]


def test_stream_array_like_values(dbos: DBOS, client: DBOSClient) -> None:
    """Values whose __eq__ returns a non-boolean -- a DataFrame, a string-dtype ndarray -- must
    survive every leg that compares a value to the closed-stream marker: the write, all the read
    paths, and the bulk fetch the conductor uses."""
    stream_key = "array_like_stream"

    @DBOS.workflow()
    def writer_workflow() -> None:
        DBOS.write_stream(stream_key, AmbiguousTruth("a"))
        DBOS.write_stream(stream_key, AmbiguousTruth("b"))
        DBOS.close_stream(stream_key)

    @DBOS.workflow()
    def reader_workflow(target_id: str) -> list[str]:
        return [v.tag for v in DBOS.read_stream(target_id, stream_key)]

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_workflow()

    assert [v.tag for v in DBOS.read_stream(wfid, stream_key)] == ["a", "b"]
    assert [v.tag for v in client.read_stream(wfid, stream_key)] == ["a", "b"]
    assert DBOS.read_stream_offset(wfid, stream_key, 1).tag == "b"
    # Checkpointed reads compare the replayed value to the marker too.
    reader_id = str(uuid.uuid4())
    with SetWorkflowID(reader_id):
        assert reader_workflow(wfid) == ["a", "b"]
    assert reexecute_workflow_by_id(dbos, reader_id).get_result() == ["a", "b"]

    entries = dbos._sys_db.get_all_stream_entries(wfid)
    assert [v.tag for v in entries[stream_key]] == ["a", "b"]

    steps = DBOS.list_workflow_steps(wfid)
    assert [s["function_name"] for s in steps] == [
        "DBOS.writeStream",
        "DBOS.writeStream",
        "DBOS.closeStream",
    ]


@pytest.mark.asyncio
async def test_stream_array_like_values_async(dbos: DBOS, client: DBOSClient) -> None:
    """Async sibling of test_stream_array_like_values."""
    stream_key = "async_array_like_stream"

    @DBOS.workflow()
    async def async_writer_workflow() -> None:
        await DBOS.write_stream_async(stream_key, AmbiguousTruth("a"))
        await DBOS.close_stream_async(stream_key)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await async_writer_workflow()

    assert [v.tag async for v in DBOS.read_stream_async(wfid, stream_key)] == ["a"]
    assert [v.tag async for v in client.read_stream_async(wfid, stream_key)] == ["a"]
    assert (await DBOS.read_stream_offset_async(wfid, stream_key, 0)).tag == "a"


def test_get_all_stream_entries_stops_at_close(dbos: DBOS) -> None:
    """The bulk fetch the conductor uses ends a stream where read_stream ends it, so the two
    never report different contents for the same rows."""
    stream_key = "conductor_view_stream"
    empty_key = "closed_empty_stream"

    @DBOS.workflow()
    def writer_workflow() -> None:
        DBOS.write_stream(stream_key, "v0")
        DBOS.close_stream(stream_key)
        # Writing past a close is still accepted; the two readers must agree on what it holds.
        DBOS.write_stream(stream_key, "after")
        DBOS.close_stream(empty_key)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_workflow()

    assert list(DBOS.read_stream(wfid, stream_key)) == ["v0"]
    entries = dbos._sys_db.get_all_stream_entries(wfid)
    assert entries[stream_key] == ["v0"]
    # A stream that was opened and closed with nothing in it reads as empty, not as absent.
    assert entries[empty_key] == []
    assert list(DBOS.read_stream(wfid, empty_key)) == []


def test_read_stream_notices_cancellation(dbos: DBOS) -> None:
    """A reader waiting on a live producer observes its own cancellation, rather than blocking
    until the producer finishes."""
    stream_key = "cancel_stream"
    release = threading.Event()
    entered = threading.Event()

    @DBOS.workflow()
    def writer_workflow() -> None:
        DBOS.write_stream(stream_key, "v0")
        # Stay active and silent so the reader waits rather than draining to the end.
        release.wait()

    @DBOS.workflow()
    def reader_workflow(target_id: str) -> list[Any]:
        seen: list[Any] = []
        for value in DBOS.read_stream(target_id, stream_key):
            seen.append(value)
            entered.set()
        return seen

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_handle = DBOS.start_workflow(writer_workflow)
    try:
        reader_id = str(uuid.uuid4())
        with SetWorkflowID(reader_id):
            reader_handle = DBOS.start_workflow(reader_workflow, wfid)
        assert entered.wait(10)
        DBOS.cancel_workflow(reader_id)
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            reader_handle.get_result()
    finally:
        release.set()
        writer_handle.get_result()


def test_client_read_stream_from_workflow_is_not_checkpointed(
    dbos: DBOS, client: DBOSClient
) -> None:
    """A client reads through its own system database, which knows nothing of the workflow that
    happens to be calling, so a client read from inside a workflow records no steps."""
    stream_key = "client_in_workflow_stream"

    @DBOS.workflow()
    def writer_workflow() -> None:
        DBOS.write_stream(stream_key, "v0")
        DBOS.close_stream(stream_key)

    @DBOS.workflow()
    def reader_workflow(target_id: str) -> list[Any]:
        return list(client.read_stream(target_id, stream_key))

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_workflow()

    reader_id = str(uuid.uuid4())
    with SetWorkflowID(reader_id):
        assert reader_workflow(wfid) == ["v0"]
    assert DBOS.list_workflow_steps(reader_id) == []


def test_stream_write_to_deleted_workflow_raises(dbos: DBOS) -> None:
    """A write whose workflow row is gone fails instead of retrying forever. The insert's retry
    loop exists for offset conflicts; a foreign key violation never resolves."""
    stream_key = "deleted_workflow_stream"

    @DBOS.workflow()
    def writer_workflow() -> None:
        DBOS.write_stream(stream_key, "v0")

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_workflow()
    DBOS.delete_workflow(wfid)

    with pytest.raises(sa.exc.IntegrityError):
        dbos._sys_db.write_stream_from_step(
            wfid,
            0,
            stream_key,
            "v1",
            serialization_type=WorkflowSerializationFormat.DEFAULT,
        )


@pytest.mark.asyncio
async def test_read_stream_async_close_releases_listener(
    dbos: DBOS, client: DBOSClient
) -> None:
    """Closing an async reader unregisters its listener there and then, rather than leaving it
    for garbage collection to reclaim at some later point."""
    stream_key = "aclose_stream"
    release = asyncio.Event()

    @DBOS.workflow()
    async def async_writer_workflow() -> None:
        await DBOS.write_stream_async(stream_key, "v0")
        # Stay active so a reader is suspended mid-stream rather than exhausted.
        await release.wait()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = await DBOS.start_workflow_async(async_writer_workflow)
    try:
        for sys_db, reader in (
            (dbos._sys_db, DBOS.read_stream_async(wfid, stream_key)),
            (client._sys_db, client.read_stream_async(wfid, stream_key)),
        ):
            assert await reader.__anext__() == "v0"
            assert len(sys_db.streams_map.snapshot()) == 1
            await reader.aclose()
            assert sys_db.streams_map.snapshot() == []
    finally:
        release.set()
        await handle.get_result()
