import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pytest
import sqlalchemy as sa
from psycopg import errors
from sqlalchemy.exc import OperationalError

from dbos import (
    DBOS,
    DBOSClient,
    Debouncer,
    PartitionLimits,
    Queue,
    SetEnqueueOptions,
    SetWorkflowID,
    WorkflowHandle,
    WorkflowStatusString,
)
from dbos._core import prepare_enqueued_workflow
from dbos._error import DBOSException
from dbos._migration import sqlite_migrations
from dbos._queue import _INTERNAL_QUEUE_CONSTRUCTION
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import DefaultSerializer
from dbos._sys_db import SystemDatabase
from dbos._utils import GlobalParams
from tests.conftest import queue_entries_are_cleaned_up, retry_until_success


def _queue_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _key_rows(dbos: DBOS, workflow_id: str) -> Dict[str, str]:
    """The partition keys stored for one workflow, by dimension name."""
    keys = SystemSchema.workflow_partition_keys
    with dbos._sys_db.engine.begin() as c:
        rows = c.execute(
            sa.select(keys.c.partition_name, keys.c.partition_key).where(
                keys.c.workflow_uuid == workflow_id
            )
        ).fetchall()
    return {name: key for name, key in rows}


def test_compound_limits_bind_each_dimension(dbos: DBOS) -> None:
    """Each dimension caps its own partitions at once. With user Bob at one and model
    m1 at two, Bob's second workflow and Carol's m1 workflow wait; releasing Alice
    frees an m1 slot for Carol, but Bob's second still waits behind Bob."""
    lock = threading.Lock()
    labels = ["bob-1", "bob-2", "alice", "carol", "dan"]
    release = {label: threading.Event() for label in labels}
    started: List[str] = []
    running: Dict[str, int] = {}
    peak: Dict[str, int] = {}

    @DBOS.workflow()
    def blocking(label: str, user: str, model: str) -> str:
        scopes = [f"user:{user}", f"model:{model}"]
        with lock:
            started.append(label)
            for scope in scopes:
                running[scope] = running.get(scope, 0) + 1
                peak[scope] = max(peak.get(scope, 0), running[scope])
        release[label].wait()
        with lock:
            for scope in scopes:
                running[scope] -= 1
        return label

    queue = DBOS.register_queue(
        _queue_name("compound_limits"),
        partitions={"user": {"concurrency": 1}, "model": {"concurrency": 2}},
        polling_interval_sec=0.05,
    )
    specs = [
        ("bob-1", "Bob", "m1"),
        ("bob-2", "Bob", "m1"),
        ("alice", "Alice", "m1"),
        ("carol", "Carol", "m1"),
        ("dan", "Dan", "m2"),
    ]
    handles: Dict[str, WorkflowHandle[str]] = {}
    for label, user, model in specs:
        with SetEnqueueOptions(queue_partition_keys={"user": user, "model": model}):
            handles[label] = queue.enqueue(blocking, label, user, model)

    def started_is(expected: Set[str]) -> None:
        with lock:
            assert set(started) == expected

    try:
        first = {"bob-1", "alice", "dan"}
        retry_until_success(lambda: started_is(first), interval=0.1, max_attempts=100)
        time.sleep(0.5)
        started_is(first)

        release["alice"].set()
        second = first | {"carol"}
        retry_until_success(lambda: started_is(second), interval=0.1, max_attempts=100)
        time.sleep(0.5)
        started_is(second)
    finally:
        for event in release.values():
            event.set()

    for label, handle in handles.items():
        assert handle.get_result() == label
    with lock:
        assert peak["user:Bob"] == 1
        assert all(c <= 1 for s, c in peak.items() if s.startswith("user:"))
        assert all(c <= 2 for s, c in peak.items() if s.startswith("model:"))
    assert queue_entries_are_cleaned_up(dbos)


def test_blocked_key_does_not_block_other_keys(dbos: DBOS) -> None:
    """A saturated key waits in line without holding back a later workflow whose key
    still has room."""
    release_bob = threading.Event()
    bob_started = threading.Event()

    @DBOS.workflow()
    def hold(user: str) -> str:
        bob_started.set()
        release_bob.wait()
        return user

    @DBOS.workflow()
    def quick(user: str) -> str:
        return user

    queue = DBOS.register_queue(
        _queue_name("head_of_line"),
        partitions={"user": {"concurrency": 1}},
        polling_interval_sec=0.05,
    )
    try:
        with SetEnqueueOptions(queue_partition_keys={"user": "Bob"}):
            first_bob = queue.enqueue(hold, "Bob")
        assert bob_started.wait(timeout=30)
        with SetEnqueueOptions(queue_partition_keys={"user": "Bob"}):
            second_bob = queue.enqueue(quick, "Bob")
        with SetEnqueueOptions(queue_partition_keys={"user": "Alice"}):
            alice = queue.enqueue(quick, "Alice")

        assert alice.get_result() == "Alice"
        assert first_bob.get_status().status == WorkflowStatusString.PENDING.value
        assert second_bob.get_status().status == WorkflowStatusString.ENQUEUED.value
    finally:
        release_bob.set()
    assert first_bob.get_result() == "Bob"
    assert second_bob.get_result() == "Bob"
    assert queue_entries_are_cleaned_up(dbos)


def test_compound_queue_dequeues_by_priority(dbos: DBOS) -> None:
    """With one workflow running at a time, admissible workflows start in priority
    order: the lower value first."""
    release_blocker = threading.Event()
    blocker_started = threading.Event()
    order: List[str] = []

    @DBOS.workflow()
    def blocker() -> str:
        blocker_started.set()
        release_blocker.wait()
        return "blocker"

    @DBOS.workflow()
    def record(user: str) -> str:
        order.append(user)
        return user

    queue = DBOS.register_queue(
        _queue_name("priority"),
        partitions={"user": {"concurrency": 1}},
        global_concurrency=1,
        polling_interval_sec=0.05,
    )
    handles: List[WorkflowHandle[str]] = []
    try:
        with SetEnqueueOptions(queue_partition_keys={"user": "Blocker"}):
            held = queue.enqueue(blocker)
        assert blocker_started.wait(timeout=30)
        for priority, user in [(5, "Eve"), (1, "Ann"), (3, "Cal")]:
            with SetEnqueueOptions(
                queue_partition_keys={"user": user}, priority=priority
            ):
                handles.append(queue.enqueue(record, user))
    finally:
        release_blocker.set()
    assert held.get_result() == "blocker"
    for handle in handles:
        handle.get_result()
    assert order == ["Ann", "Cal", "Eve"]
    assert queue_entries_are_cleaned_up(dbos)


def test_compound_dequeue_contention(dbos: DBOS, skip_with_sqlite: None) -> None:
    """Executors dequeueing a compound queue at once never admit past a key's limit. Racers
    lock the same head rows with NOWAIT (inside a SERIALIZABLE transaction), so all but one
    lose each round with a lock or serialization error instead of over-admitting."""

    @DBOS.workflow()
    def raced(value: str) -> None:
        pass

    queue_name = _queue_name("compound_race")
    partitions: Dict[str, PartitionLimits] = {
        "user": {"concurrency": 2},
        "model": {"concurrency": 3},
    }
    # No queue row, so no worker thread drains it: the racers below are the only dequeuers.
    queue = Queue(
        queue_name,
        partitions=partitions,
        database_backed_queue=True,
        token=_INTERNAL_QUEUE_CONSTRUCTION,
    )
    statuses = []
    for i in range(24):
        status = prepare_enqueued_workflow(
            dbos,
            raced,
            (str(i),),
            {},
            queue_name=queue_name,
            workflow_id=f"{queue_name}-{i}",
        )
        status["queue_partition_keys"] = {"user": f"u{i % 4}", "model": f"m{i % 3}"}
        statuses.append(status)
    assert len(dbos._sys_db.init_workflows(statuses)) == len(statuses)

    racers, rounds = 4, 5
    barrier = threading.Barrier(racers)
    lost = 0
    lost_lock = threading.Lock()

    def race(slot: int) -> List[str]:
        nonlocal lost
        admitted: List[str] = []
        for _ in range(rounds):
            barrier.wait()
            try:
                ids, _ = dbos._sys_db.start_queued_compound_workflows(
                    queue, f"executor-{slot}", GlobalParams.app_version
                )
                admitted.extend(ids)
            except OperationalError as e:
                # Losing the race is expected; anything else fails the test.
                if not isinstance(
                    e.orig, (errors.LockNotAvailable, errors.SerializationFailure)
                ):
                    raise
                with lost_lock:
                    lost += 1
        return admitted

    with ThreadPoolExecutor(max_workers=racers) as pool:
        futures = [pool.submit(race, slot) for slot in range(racers)]
        admitted = [id for future in futures for id in future.result()]

    ws = SystemSchema.workflow_status
    keys = SystemSchema.workflow_partition_keys
    with dbos._sys_db.engine.begin() as c:
        pending = {
            row[0]
            for row in c.execute(
                sa.select(ws.c.workflow_uuid)
                .where(ws.c.queue_name == queue_name)
                .where(ws.c.status == WorkflowStatusString.PENDING.value)
            )
        }
        counts = c.execute(
            sa.select(keys.c.partition_name, keys.c.partition_key, sa.func.count())
            .select_from(keys.join(ws, ws.c.workflow_uuid == keys.c.workflow_uuid))
            .where(ws.c.queue_name == queue_name)
            .where(ws.c.status == WorkflowStatusString.PENDING.value)
            .group_by(keys.c.partition_name, keys.c.partition_key)
        ).fetchall()
    # Every claim is reported exactly once, and nothing is claimed that was not reported.
    assert sorted(admitted) == sorted(pending)
    assert pending
    for name, key, count in counts:
        assert count <= partitions[name]["concurrency"], (name, key, count)
    # With every user and model saturated at the end, the budgets were contended, not idle.
    assert len(pending) == 8, (len(pending), lost)
    DBOS.delete_workflows([s["workflow_uuid"] for s in statuses])


@pytest.mark.parametrize(
    "partitions, extra, message",
    [
        ({}, {}, "must name at least one partition"),
        ({"": {"concurrency": 1}}, {}, "partition names must be non-empty strings"),
        ({"user": {}}, {}, "must set a concurrency of at least 1"),
        ({"user": {"concurrency": 0}}, {}, "must set a concurrency of at least 1"),
        (
            {"user": {"concurrency": 5}},
            {"global_concurrency": 4},
            "global_concurrency must be greater than or equal to each partition",
        ),
        (
            {"user": {"concurrency": 1}},
            {"partition_concurrency": 1},
            "cannot be combined with partition_concurrency",
        ),
        (
            {"user": {"concurrency": 1}},
            {"partition_worker_concurrency": 1},
            "cannot be combined with partition_concurrency",
        ),
        (
            {"user": {"concurrency": 1}},
            {"partition_limiter": {"limit": 1, "period": 1.0}},
            "cannot be combined with partition_concurrency",
        ),
    ],
)
def test_compound_partitions_validation(
    dbos: DBOS, partitions: Any, extra: Dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        DBOS.register_queue(_queue_name("invalid"), partitions=partitions, **extra)


def test_compound_enqueue_validation(dbos: DBOS) -> None:
    queue = DBOS.register_queue(
        _queue_name("compound_enqueue"),
        partitions={"user": {"concurrency": 1}, "model": {"concurrency": 2}},
        polling_interval_sec=0.05,
    )
    plain = DBOS.register_queue(_queue_name("plain_enqueue"))

    @DBOS.workflow()
    def noop() -> None:
        return None

    cases: List[Tuple[Queue, Dict[str, Any], str]] = [
        (queue, {"queue_partition_keys": {"user": "Bob"}}, "must name exactly"),
        (
            queue,
            {"queue_partition_keys": {"user": "Bob", "model": "m1", "region": "eu"}},
            "must name exactly",
        ),
        (queue, {}, "is partitioned by queue_partition_keys"),
        (
            queue,
            {
                "queue_partition_key": "Bob",
                "queue_partition_keys": {"user": "Bob", "model": "m1"},
            },
            "cannot both be set",
        ),
        (
            queue,
            {
                "queue_partition_keys": {"user": "Bob", "model": "m1"},
                "deduplication_id": "dedup",
            },
            "Deduplication is not supported",
        ),
        (queue, {"queue_partition_keys": {}}, "non-empty dict"),
        (queue, {"queue_partition_keys": {"user": 1, "model": "m1"}}, "non-empty dict"),
        (
            plain,
            {"queue_partition_keys": {"user": "Bob"}},
            "is not partitioned by queue_partition_keys",
        ),
    ]
    for target, options, message in cases:
        with pytest.raises(DBOSException, match=message):
            with SetEnqueueOptions(**options):
                target.enqueue(noop)
    assert queue_entries_are_cleaned_up(dbos)


def test_compound_partitions_round_trip(dbos: DBOS, client: DBOSClient) -> None:
    name = _queue_name("round_trip")
    partitions: Dict[str, PartitionLimits] = {
        "user": {"concurrency": 2},
        "model": {"concurrency": 5},
    }
    DBOS.register_queue(name, partitions=partitions, global_concurrency=10)

    queue = DBOS.retrieve_queue(name)
    assert queue is not None and queue.partitions == partitions
    client_queue = client.retrieve_queue(name)
    assert client_queue is not None and client_queue.partitions == partitions

    # Re-registering without partitions clears them.
    DBOS.register_queue(name, global_concurrency=10, on_conflict="always_update")
    cleared = DBOS.retrieve_queue(name)
    assert cleared is not None and cleared.partitions is None


def test_client_enqueue_runs_under_compound_limits(
    dbos: DBOS, client: DBOSClient
) -> None:
    release = threading.Event()
    lock = threading.Lock()
    running: Dict[str, int] = {}
    peak: Dict[str, int] = {}

    @DBOS.workflow()
    def blocking(user: str) -> str:
        with lock:
            running[user] = running.get(user, 0) + 1
            peak[user] = max(peak.get(user, 0), running[user])
        release.wait()
        with lock:
            running[user] -= 1
        return user

    name = _queue_name("client_compound")
    DBOS.register_queue(
        name, partitions={"user": {"concurrency": 1}}, polling_interval_sec=0.05
    )
    handles: List[WorkflowHandle[str]] = [
        client.enqueue(
            {
                "queue_name": name,
                "workflow_name": blocking.__qualname__,
                "queue_partition_keys": {"user": user},
            },
            user,
        )
        for user in ["Bob", "Bob", "Alice"]
    ]

    def running_is(expected: Dict[str, int]) -> None:
        with lock:
            assert running == expected

    try:
        expected = {"Bob": 1, "Alice": 1}
        retry_until_success(
            lambda: running_is(expected), interval=0.1, max_attempts=100
        )
        time.sleep(0.5)
        running_is(expected)
    finally:
        release.set()
    assert [handle.get_result() for handle in handles] == ["Bob", "Bob", "Alice"]
    with lock:
        assert peak == {"Bob": 1, "Alice": 1}


def test_compound_keys_follow_their_workflow(dbos: DBOS) -> None:
    queue = DBOS.register_queue(
        _queue_name("keys_follow"),
        partitions={"user": {"concurrency": 1}, "model": {"concurrency": 2}},
        polling_interval_sec=0.05,
    )

    @DBOS.workflow()
    def quick() -> str:
        return "done"

    wfid = str(uuid.uuid4())
    with (
        SetWorkflowID(wfid),
        SetEnqueueOptions(queue_partition_keys={"user": "Bob", "model": "m1"}),
    ):
        handle = queue.enqueue(quick)
    assert handle.get_result() == "done"
    assert _key_rows(dbos, wfid) == {"user": "Bob", "model": "m1"}

    # A second enqueue under the same ID attaches to the existing workflow and
    # writes none of its own keys.
    with (
        SetWorkflowID(wfid),
        SetEnqueueOptions(queue_partition_keys={"user": "Alice", "model": "m2"}),
    ):
        again = queue.enqueue(quick)
    assert again.get_workflow_id() == wfid
    assert _key_rows(dbos, wfid) == {"user": "Bob", "model": "m1"}

    # Deleting the workflow cascades to its key rows.
    DBOS.delete_workflow(wfid)
    assert _key_rows(dbos, wfid) == {}


def test_batch_init_writes_partition_keys(dbos: DBOS) -> None:
    @DBOS.workflow()
    def quick() -> str:
        return "done"

    # Not registered as a queue, so nothing dequeues these rows during the test.
    queue_name = _queue_name("batch_keys")
    wfid = str(uuid.uuid4())
    status = prepare_enqueued_workflow(
        dbos, quick, (), {}, queue_name=queue_name, workflow_id=wfid
    )
    status["queue_partition_keys"] = {"user": "Bob", "model": "m1"}
    assert dbos._sys_db.init_workflows([status]) == {wfid}
    assert _key_rows(dbos, wfid) == {"user": "Bob", "model": "m1"}
    # A repeat inserts nothing and leaves the keys as they were.
    assert dbos._sys_db.init_workflows([status]) == set()
    assert _key_rows(dbos, wfid) == {"user": "Bob", "model": "m1"}
    DBOS.delete_workflow(wfid)


def test_by_name_enqueue_checks_compound_keys(dbos: DBOS) -> None:
    """DBOS.enqueue_workflow loads the queue's configuration when keys are supplied, so the
    same checks apply as on a queue handle."""

    @DBOS.workflow()
    def echo(value: str) -> str:
        return value

    compound = _queue_name("by_name_compound")
    DBOS.register_queue(
        compound, partitions={"user": {"concurrency": 1}}, polling_interval_sec=0.05
    )
    plain = _queue_name("by_name_plain")
    DBOS.register_queue(plain)

    with SetEnqueueOptions(queue_partition_keys={"user": "Bob"}):
        assert DBOS.enqueue_workflow(compound, echo, "Bob").get_result() == "Bob"
    with SetEnqueueOptions(queue_partition_keys={"model": "m1"}):
        with pytest.raises(DBOSException, match="must name exactly"):
            DBOS.enqueue_workflow(compound, echo, "x")
    with SetEnqueueOptions(queue_partition_keys={"user": "Bob"}):
        with pytest.raises(
            DBOSException, match="is not partitioned by queue_partition_keys"
        ):
            DBOS.enqueue_workflow(plain, echo, "x")


def test_debouncer_rejects_compound_partition_keys(dbos: DBOS) -> None:
    """A debounce owns a deduplication ID, which compound partitioning does not support. The
    check runs before the bounce, so ambient keys cannot be dropped silently on a bounce.
    """

    @DBOS.workflow()
    def debounced(value: str) -> str:
        return value

    queue = DBOS.register_queue(
        _queue_name("debounce_compound"),
        partitions={"user": {"concurrency": 1}},
        polling_interval_sec=0.05,
    )
    debouncer = Debouncer.create(debounced, queue=queue)
    with SetEnqueueOptions(queue_partition_keys={"user": "Bob"}):
        with pytest.raises(DBOSException, match="queue partition key set"):
            debouncer.debounce("key", 0.5, "x")


def test_compound_partition_schema_migrates_fresh_sqlite(sqlite_path: Path) -> None:
    sys_db = SystemDatabase.create(
        system_database_url=f"sqlite:///{sqlite_path}",
        engine_kwargs={},
        engine=None,
        schema=None,
        executor_id=None,
        serializer=DefaultSerializer(),
    )
    try:
        sys_db.run_migrations()
        with sys_db.engine.connect() as connection:
            key_columns = {
                row[1]
                for row in connection.execute(
                    sa.text("PRAGMA table_info(workflow_partition_keys)")
                )
            }
            assert key_columns == {"workflow_uuid", "partition_name", "partition_key"}
            queue_columns = {
                row[1]
                for row in connection.execute(sa.text("PRAGMA table_info(queues)"))
            }
            assert "partitions" in queue_columns
            foreign_keys = connection.execute(
                sa.text("PRAGMA foreign_key_list(workflow_partition_keys)")
            ).fetchall()
            # Columns: id, seq, table, from, to, on_update, on_delete, match.
            assert any(
                row[2] == "workflow_status" and row[6] == "CASCADE"
                for row in foreign_keys
            )
            assert connection.execute(
                sa.text("SELECT version FROM dbos_migrations")
            ).scalar() == len(sqlite_migrations)
    finally:
        sys_db.destroy()
