import contextlib
import threading
import time
import uuid
from typing import Any, AsyncIterator, Dict, Iterator, List, Optional

import pytest
import sqlalchemy as sa

from dbos import (
    DBOS,
    AsyncSQLAlchemyDatasource,
    DBOSClient,
    DBOSConfig,
    SetEnqueueOptions,
    SetWorkflowID,
    SQLAlchemyDatasource,
)
from dbos._error import DBOSException, DBOSNonExistentWorkflowError
from dbos._schemas.datasource_database import DatasourceSchema
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import deserialize_value
from dbos._sys_db import WorkflowStatusString
from dbos._utils import INTERNAL_QUEUE_NAME
from tests.conftest import set_workflow_status

runs: Dict[str, int] = {}


@contextlib.contextmanager
def paused_queue(name: str) -> Iterator[Any]:
    """A single-slot queue whose only worker is held, so anything enqueued onto it
    stays ENQUEUED until the block exits.

    A rewind re-enqueues, so without this every assertion about the state a rewind
    leaves behind races the queue manager picking the workflow back up and
    overwriting it.
    """
    released = threading.Event()
    started = threading.Event()

    @DBOS.workflow(name=f"{name}_blocker")
    def blocker() -> str:
        started.set()
        released.wait()
        return "held"

    DBOS.register_queue(name, worker_concurrency=1)
    handle = DBOS.enqueue_workflow(name, blocker)
    assert started.wait(timeout=10), f"{name} blocker never started"
    try:
        yield handle
    finally:
        released.set()
        handle.get_result()


def run_count(name: str) -> int:
    runs[name] = runs.get(name, 0) + 1
    return runs[name]


def start(fn: Any, name: str) -> str:
    """Run a workflow to completion under a fresh ID and return that ID."""
    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        DBOS.start_workflow(fn, name).get_result()
    return workflow_id


def step_id_of(workflow_id: str, function_name: str, *, occurrence: int = 0) -> int:
    matches = [
        step["function_id"]
        for step in DBOS.list_workflow_steps(workflow_id)
        if step["function_name"] == function_name
        or step["function_name"].endswith("." + function_name)
    ]
    assert len(matches) > occurrence, (
        f"{workflow_id} has {len(matches)} {function_name} steps, "
        f"wanted index {occurrence}"
    )
    return matches[occurrence]


def status_row(dbos: DBOS, workflow_id: str) -> Any:
    with dbos._sys_db.engine.begin() as c:
        return c.execute(
            sa.select(SystemSchema.workflow_status).where(
                SystemSchema.workflow_status.c.workflow_uuid == workflow_id
            )
        ).one()


def output_row(dbos: DBOS, workflow_id: str) -> Any:
    with dbos._sys_db.engine.begin() as c:
        return c.execute(
            sa.select(SystemSchema.workflow_output).where(
                SystemSchema.workflow_output.c.workflow_uuid == workflow_id
            )
        ).one_or_none()


def step_ids(dbos: DBOS, workflow_id: str) -> List[int]:
    with dbos._sys_db.engine.begin() as c:
        return sorted(
            c.execute(
                sa.select(SystemSchema.operation_outputs.c.function_id).where(
                    SystemSchema.operation_outputs.c.workflow_uuid == workflow_id
                )
            ).scalars()
        )


def events_of(dbos: DBOS, workflow_id: str) -> Dict[str, Any]:
    return dbos._sys_db.get_all_events(workflow_id)


def event_history_ids(dbos: DBOS, workflow_id: str) -> List[int]:
    with dbos._sys_db.engine.begin() as c:
        return sorted(
            c.execute(
                sa.select(SystemSchema.workflow_events_history.c.function_id).where(
                    SystemSchema.workflow_events_history.c.workflow_uuid == workflow_id
                )
            ).scalars()
        )


def stream_rows(dbos: DBOS, workflow_id: str, key: str) -> List[Any]:
    """(offset, value) for a stream, including the close sentinel a reader stops at."""
    with dbos._sys_db.engine.begin() as c:
        rows = c.execute(
            sa.select(
                SystemSchema.streams.c.offset,
                SystemSchema.streams.c.value,
                SystemSchema.streams.c.serialization,
            )
            .where(
                (SystemSchema.streams.c.workflow_uuid == workflow_id)
                & (SystemSchema.streams.c.key == key)
            )
            .order_by(SystemSchema.streams.c.offset)
        ).fetchall()
    return [
        (row[0], deserialize_value(row[1], row[2], dbos._sys_db.serializer))
        for row in rows
    ]


def mailbox(dbos: DBOS, workflow_id: str) -> List[Any]:
    """(message, consumed, consumed_by_function_id) for a workflow, oldest first."""
    with dbos._sys_db.engine.begin() as c:
        rows = c.execute(
            sa.select(
                SystemSchema.notifications.c.message,
                SystemSchema.notifications.c.serialization,
                SystemSchema.notifications.c.consumed,
                SystemSchema.notifications.c.consumed_by_function_id,
            )
            .where(SystemSchema.notifications.c.destination_uuid == workflow_id)
            .order_by(SystemSchema.notifications.c.created_at_epoch_ms)
        ).fetchall()
    return [
        (
            deserialize_value(row[0], row[1], dbos._sys_db.serializer),
            row[2],
            row[3],
        )
        for row in rows
    ]


#######################################
## Notifications
#######################################


def test_rewind_deletes_notifications(dbos: DBOS) -> None:
    @DBOS.workflow()
    def receiver(name: str) -> str:
        run = run_count(name)
        first = DBOS.recv("cmd", timeout_seconds=10)
        second = DBOS.recv("cmd", timeout_seconds=10)
        return f"{first}{second}:{run}"

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(receiver, "partial-delete")
    DBOS.send(workflow_id, "a", "cmd")
    DBOS.send(workflow_id, "b", "cmd")
    assert handle.get_result() == "ab:1"

    # Each recv stamped the row it took with its own step, which is what lets the
    # rewind delete exactly the messages the discarded steps consumed.
    first_recv = step_id_of(workflow_id, "DBOS.recv")
    second_recv = step_id_of(workflow_id, "DBOS.recv", occurrence=1)
    assert first_recv != second_recv
    # A message that arrives once the workflow is done sits unconsumed.
    DBOS.send(workflow_id, "stray", "cmd")
    assert mailbox(dbos, workflow_id) == [
        ("a", True, first_recv),
        ("b", True, second_recv),
        ("stray", False, None),
    ]

    with paused_queue("rewind_delete_gate"):
        dbos._sys_db.rewind_workflow(
            workflow_id, second_recv, queue_name="rewind_delete_gate"
        )
        # The message the discarded step took is gone, and so is the one still
        # waiting. The first recv's message stays consumed: its step survived the cut.
        assert mailbox(dbos, workflow_id) == [("a", True, first_recv)]
        # A message that arrives after the cut is what the replayed recv gets.
        DBOS.send(workflow_id, "c", "cmd")

    assert DBOS.retrieve_workflow(workflow_id).get_result() == "ac:2"
    assert mailbox(dbos, workflow_id) == [
        ("a", True, first_recv),
        ("c", True, second_recv),
    ]


#######################################
## Events
#######################################


def test_rewind_unpublishes_events(dbos: DBOS) -> None:
    @DBOS.workflow()
    def publisher(name: str) -> str:
        run = run_count(name)
        DBOS.set_event("below", "kept")
        DBOS.set_event("both", "old")
        if run == 1:
            DBOS.set_event("both", "new")
            DBOS.set_event("above", "doomed")
            return "first"
        DBOS.set_event("both", "republished")
        return "second"

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        assert DBOS.start_workflow(publisher, "events").get_result() == "first"
    assert events_of(dbos, workflow_id) == {
        "below": "kept",
        "both": "new",
        "above": "doomed",
    }

    # Cut just after the second set_event, so "below" and the first "both" survive.
    cut = step_id_of(workflow_id, "DBOS.setEvent", occurrence=2)

    with paused_queue("rewind_events_gate"):
        dbos._sys_db.rewind_workflow(workflow_id, cut, queue_name="rewind_events_gate")
        assert events_of(dbos, workflow_id) == {
            # Never touched past the cut, so left exactly as it was.
            "below": "kept",
            # Reverted to its last value from below the cut, not deleted.
            "both": "old",
            # "above" was only ever published past the cut, so it is gone entirely.
        }
        assert event_history_ids(dbos, workflow_id) == [1, 2]

        # And that is what a peer reading by key sees, not the discarded values.
        assert DBOS.get_event(workflow_id, "both", 1) == "old"
        assert DBOS.get_event(workflow_id, "above", 1) is None

    assert DBOS.retrieve_workflow(workflow_id).get_result() == "second"

    # The replay republishes over the reverted state with a new value, which is
    # again what peers read. "above" stays gone: nothing sets it again.
    assert events_of(dbos, workflow_id) == {"below": "kept", "both": "republished"}
    assert DBOS.get_event(workflow_id, "both", 2) == "republished"
    assert DBOS.get_event(workflow_id, "below", 2) == "kept"
    assert DBOS.get_event(workflow_id, "above", 1) is None


#######################################
## Streams
#######################################


def test_rewind_keeps_stream_entries(dbos: DBOS) -> None:
    @DBOS.workflow()
    def writer(name: str) -> str:
        run = run_count(name)
        DBOS.write_stream("log", f"a{run}")
        DBOS.write_stream("log", f"b{run}")
        return f"run{run}"

    workflow_id = start(writer, "stream-keep")
    assert list(DBOS.read_stream(workflow_id, "log")) == ["a1", "b1"]

    dbos._sys_db.rewind_workflow(workflow_id, 1)
    assert DBOS.retrieve_workflow(workflow_id).get_result() == "run2"

    # Offsets are addresses peers read by, so the discarded run's entries keep
    # theirs and the replay appends. Deleting would hand offset 0 a new value.
    assert list(DBOS.read_stream(workflow_id, "log")) == ["a1", "b1", "a2", "b2"]
    assert [row[0] for row in stream_rows(dbos, workflow_id, "log")] == [0, 1, 2, 3]


def test_rewind_reopens_a_closed_stream(dbos: DBOS) -> None:
    @DBOS.workflow()
    def writer(name: str) -> str:
        DBOS.write_stream("out", f"v{run_count(name)}")
        DBOS.close_stream("out")
        return f"run{runs[name]}"

    workflow_id = start(writer, "stream-close")
    assert list(DBOS.read_stream(workflow_id, "out")) == ["v1"]

    # The sentinel terminates every reader that reaches it, so one left over from
    # the discarded run would hide the replay's entry with no error anywhere.
    dbos._sys_db.rewind_workflow(workflow_id, 1)
    assert DBOS.retrieve_workflow(workflow_id).get_result() == "run2"
    assert list(DBOS.read_stream(workflow_id, "out")) == ["v1", "v2"]

    # Cut above the close, the sentinel is not the discarded run's to undo: its
    # step survives, so nothing replays it and it has to stay.
    dbos._sys_db.rewind_workflow(workflow_id, 3)
    assert DBOS.retrieve_workflow(workflow_id).get_result() == "run3"
    rows = stream_rows(dbos, workflow_id, "out")
    assert [row[1] for row in rows] == ["v1", "v2", "__DBOS_STREAM_CLOSED__"]


#######################################
## Child workflows
#######################################


def test_rewound_parent_adopts_its_existing_child(dbos: DBOS) -> None:
    child_runs: List[str] = []

    @DBOS.workflow()
    def child(value: int) -> int:
        child_runs.append("ran")
        return value * 2

    @DBOS.workflow()
    def parent(name: str) -> int:
        run = run_count(name)
        handle = DBOS.start_workflow(child, 21)
        return handle.get_result() + run

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        assert DBOS.start_workflow(parent, "adopt").get_result() == 43
    assert len(child_runs) == 1
    child_id = f"{workflow_id}-1"
    assert DBOS.retrieve_workflow(child_id).get_result() == 42

    dbos._sys_db.rewind_workflow(workflow_id, 1)
    assert DBOS.retrieve_workflow(workflow_id).get_result() == 44

    assert len(child_runs) == 1
    assert DBOS.retrieve_workflow(child_id).get_result() == 42


def test_rewind_child_then_parent_to_repair_a_failure(dbos: DBOS) -> None:
    child_runs: List[str] = []

    @DBOS.workflow()
    def child(value: int) -> int:
        child_runs.append("ran")
        if len(child_runs) == 1:
            raise ValueError("child is bogus")
        return value * 2

    @DBOS.workflow()
    def parent(name: str) -> int:
        run_count(name)
        handle = DBOS.start_workflow(child, 21)
        return handle.get_result()

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(parent, "repair")
    with pytest.raises(ValueError, match="child is bogus"):
        handle.get_result()

    child_id = f"{workflow_id}-1"
    assert DBOS.retrieve_workflow(child_id).get_status().status == "ERROR"
    assert DBOS.retrieve_workflow(workflow_id).get_status().status == "ERROR"

    # Repair the child on its own first.
    dbos._sys_db.rewind_workflow(child_id, 1)
    assert DBOS.retrieve_workflow(child_id).get_result() == 42

    # Then rewind the parent to the getResult that failed. The earlier step that
    # started the child survives, and the replay picks up the repaired result.
    get_result_step = step_id_of(workflow_id, "DBOS.getResult")
    dbos._sys_db.rewind_workflow(workflow_id, get_result_step)
    assert DBOS.retrieve_workflow(workflow_id).get_result() == 42
    assert len(child_runs) == 2


#######################################
## Queues, partitions, versions
#######################################


def test_rewind_onto_a_queue_with_a_partition_key(dbos: DBOS) -> None:
    @DBOS.workflow()
    def counter(name: str) -> int:
        return run_count(name)

    DBOS.register_queue("rewind_partitioned", partition_concurrency=1)

    workflow_id = str(uuid.uuid4())
    with SetEnqueueOptions(queue_partition_key="original"):
        with SetWorkflowID(workflow_id):
            handle = DBOS.enqueue_workflow("rewind_partitioned", counter, "partition")
    assert handle.get_result() == 1

    dbos._sys_db.rewind_workflow(
        workflow_id,
        1,
        queue_name="rewind_partitioned",
        queue_partition_key="repaired",
    )
    assert DBOS.retrieve_workflow(workflow_id).get_result() == 2
    status = DBOS.retrieve_workflow(workflow_id).get_status()
    assert status.queue_name == "rewind_partitioned"
    assert status.queue_partition_key == "repaired"

    # Omitting the key clears it, omitting the queue falls back to the internal queue
    dbos._sys_db.rewind_workflow(workflow_id, 1)
    assert DBOS.retrieve_workflow(workflow_id).get_result() == 3
    status = DBOS.retrieve_workflow(workflow_id).get_status()
    assert status.queue_name == INTERNAL_QUEUE_NAME
    assert status.queue_partition_key is None


def test_rewind_onto_a_different_application_version(dbos: DBOS) -> None:
    @DBOS.workflow()
    def counter(name: str) -> int:
        return run_count(name)

    workflow_id = start(counter, "version")
    running_version = status_row(dbos, workflow_id).application_version

    # Dequeueing matches on application version, so a workflow restamped with a
    # version nothing is running on stays enqueued instead of replaying.
    dbos._sys_db.rewind_workflow(
        workflow_id, 1, application_version="not-this-deployment"
    )
    assert status_row(dbos, workflow_id).application_version == "not-this-deployment"
    time.sleep(2.5)  # several queue polls, any of which would pick it up
    assert status_row(dbos, workflow_id).status == WorkflowStatusString.ENQUEUED.value
    assert runs["version"] == 1

    # Getting out of that takes a cancel first: the workflow is ENQUEUED now, and
    # only a terminal workflow can be rewound.
    with pytest.raises(DBOSException, match="only a workflow in a terminal state"):
        dbos._sys_db.rewind_workflow(
            workflow_id, 1, application_version=running_version
        )
    DBOS.cancel_workflow(workflow_id)

    # Restamped with the version this executor is running, it replays
    dbos._sys_db.rewind_workflow(workflow_id, 1, application_version=running_version)
    assert DBOS.retrieve_workflow(workflow_id).get_result() == 2

    # Omitted, the workflow keeps the version it already had
    dbos._sys_db.rewind_workflow(workflow_id, 1)
    assert DBOS.retrieve_workflow(workflow_id).get_result() == 3
    assert status_row(dbos, workflow_id).application_version == running_version


#######################################
## Database state between rewind and replay
#######################################


def test_database_state_between_rewind_and_replay(dbos: DBOS) -> None:
    """Everything a rewind writes, observed before the queue manager picks the
    workflow back up."""

    @DBOS.workflow()
    def subject(name: str) -> str:
        run = run_count(name)
        DBOS.set_event("phase", f"run{run}")
        DBOS.recv("cmd", timeout_seconds=10)
        return f"run{run}"

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(subject, "dbstate")
    DBOS.send(workflow_id, "go", "cmd")
    assert handle.get_result() == "run1"

    before = status_row(dbos, workflow_id)
    assert before.status == WorkflowStatusString.SUCCESS.value
    assert before.completed_at is not None

    with paused_queue("rewind_gate"):
        dbos._sys_db.rewind_workflow(
            workflow_id, 1, queue_name="rewind_gate", queue_partition_key="pk"
        )

        after = status_row(dbos, workflow_id)
        assert after.status == WorkflowStatusString.ENQUEUED.value
        assert after.queue_name == "rewind_gate"
        assert after.queue_partition_key == "pk"
        assert after.recovery_attempts == 0
        assert after.started_at_epoch_ms is None
        assert after.completed_at is None
        assert after.workflow_deadline_epoch_ms is None
        assert after.deduplication_id is None
        # The identity of the workflow is untouched: same name, inputs, created_at.
        assert after.name == before.name
        assert after.created_at == before.created_at

        assert step_ids(dbos, workflow_id) == []
        assert event_history_ids(dbos, workflow_id) == []
        assert events_of(dbos, workflow_id) == {}
        assert mailbox(dbos, workflow_id) == []
        assert output_row(dbos, workflow_id) is None
        assert after.output is None
        assert after.error is None
        DBOS.send(workflow_id, "go", "cmd")

    assert DBOS.retrieve_workflow(workflow_id).get_result() == "run2"
    assert events_of(dbos, workflow_id) == {"phase": "run2"}


#######################################
## Refusals and validation
#######################################


def test_rewind_refuses_a_missing_workflow(dbos: DBOS) -> None:
    with pytest.raises(DBOSNonExistentWorkflowError):
        dbos._sys_db.rewind_workflow(str(uuid.uuid4()), 1)


def test_rewind_input_validation(dbos: DBOS) -> None:
    @DBOS.workflow()
    def counter(name: str) -> int:
        return run_count(name)

    workflow_id = start(counter, "validation")

    with pytest.raises(ValueError, match="must be >= 1"):
        dbos._sys_db.rewind_workflow(workflow_id, 0)

    # That wrote nothing.
    assert DBOS.retrieve_workflow(workflow_id).get_status().status == "SUCCESS"
    assert runs["validation"] == 1


#######################################
## Public API
#######################################


def test_public_rewind_api(dbos: DBOS, client: DBOSClient) -> None:
    @DBOS.workflow()
    def counter(name: str) -> int:
        return run_count(name)

    # start_step defaults to the beginning.
    workflow_id = start(counter, "single")
    assert DBOS.rewind_workflow(workflow_id).get_result() == 2
    assert DBOS.rewind_workflow(workflow_id, start_step=1).get_result() == 3

    workflow_id = start(counter, "clientsingle")
    assert client.rewind_workflow(workflow_id).get_result() == 2
    assert client.rewind_workflow(workflow_id, start_step=1).get_result() == 3


@pytest.mark.asyncio
async def test_public_rewind_api_async(dbos: DBOS, client: DBOSClient) -> None:
    @DBOS.workflow()
    def counter(name: str) -> int:
        return run_count(name)

    workflow_id = start(counter, "asyncsingle")
    handle = await DBOS.rewind_workflow_async(workflow_id, start_step=1)
    assert await handle.get_result() == 2

    workflow_id = start(counter, "asyncclientsingle")
    client_handle = await client.rewind_workflow_async(workflow_id)
    assert await client_handle.get_result() == 2


def test_rewind_from_inside_a_workflow_is_checkpointed(dbos: DBOS) -> None:
    """Rewind is an ordinary step, so a recovered caller replays it off its
    checkpoint instead of rewinding its target a second time.

    This matters because rewind is not idempotent in effect: a second one would
    either run the target again, or be refused because the first rewind left the
    target ENQUEUED rather than terminal.
    """

    @DBOS.workflow()
    def target(name: str) -> int:
        return run_count(name)

    @DBOS.workflow()
    def repairer(workflow_id: str) -> int:
        run_count("repairer")
        result: int = DBOS.rewind_workflow(workflow_id).get_result()
        return result

    target_id = start(target, "repaired")
    assert runs["repaired"] == 1

    repairer_id = str(uuid.uuid4())
    with SetWorkflowID(repairer_id):
        assert DBOS.start_workflow(repairer, target_id).get_result() == 2
    assert runs["repaired"] == 2

    rewind_step = step_id_of(repairer_id, "DBOS.rewindWorkflow")

    # Crash and recover the repairer with its checkpoints intact, which is what
    # a real executor failure looks like.
    set_workflow_status(dbos._sys_db, repairer_id, "PENDING")
    handles = DBOS._recover_pending_workflows()
    assert [h.workflow_id for h in handles] == [repairer_id]
    assert handles[0].get_result() == 2

    # The repairer body ran again, but the rewind did not: the target still has
    # only the two runs from before recovery.
    assert runs["repairer"] == 2
    assert runs["repaired"] == 2


#######################################
## Datasources
#######################################


def datasource_checkpoints(engine: sa.Engine, workflow_id: str) -> List[int]:
    with engine.begin() as c:
        return sorted(
            c.execute(
                sa.select(DatasourceSchema.datasource_outputs.c.step_id).where(
                    DatasourceSchema.datasource_outputs.c.workflow_id == workflow_id
                )
            ).scalars()
        )


async def datasource_checkpoints_async(engine: Any, workflow_id: str) -> List[int]:
    async with engine.begin() as c:
        result = await c.execute(
            sa.select(DatasourceSchema.datasource_outputs.c.step_id).where(
                DatasourceSchema.datasource_outputs.c.workflow_id == workflow_id
            )
        )
        return sorted(result.scalars())


def table_rows(engine: sa.Engine) -> List[str]:
    with engine.begin() as c:
        return sorted(c.execute(sa.text("SELECT v FROM rows")).scalars())


async def table_rows_async(engine: Any) -> List[str]:
    async with engine.begin() as c:
        return sorted((await c.execute(sa.text("SELECT v FROM rows"))).scalars())


@contextlib.contextmanager
def launched_with_datasources(
    config: DBOSConfig, tmp_path: Any, count: int
) -> Iterator[Any]:
    """DBOS launched with `count` sqlite datasources, each holding a `rows` table."""
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    datasources = []
    for i in range(count):
        ds = SQLAlchemyDatasource.create(f"sqlite:///{tmp_path}/ds{i}.sqlite")
        with ds.engine.begin() as c:
            c.execute(sa.text("CREATE TABLE rows (v TEXT)"))
        datasources.append(ds)
    try:
        DBOS.launch()
        yield dbos, datasources
    finally:
        DBOS.destroy(destroy_registry=True)
        for ds in datasources:
            ds.engine.dispose()


@contextlib.asynccontextmanager
async def launched_with_async_datasources(
    config: DBOSConfig, tmp_path: Any, count: int
) -> AsyncIterator[Any]:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    datasources = []
    for i in range(count):
        ds = await AsyncSQLAlchemyDatasource.create(
            f"sqlite+aiosqlite:///{tmp_path}/async_ds{i}.sqlite"
        )
        async with ds.engine.begin() as c:
            await c.execute(sa.text("CREATE TABLE rows (v TEXT)"))
        datasources.append(ds)
    try:
        DBOS.launch()
        yield dbos, datasources
    finally:
        DBOS.destroy(destroy_registry=True)
        for ds in datasources:
            await ds.engine.dispose()


def inserter(ds: SQLAlchemyDatasource) -> Any:
    def insert_row(v: str) -> str:
        ds.sql_session().execute(sa.text("INSERT INTO rows (v) VALUES (:v)"), {"v": v})
        return v

    return insert_row


def test_rewind_drops_datasource_checkpoints(
    config: DBOSConfig, cleanup_test_databases: None, tmp_path: Any
) -> None:
    """Every registered datasource is cleared. A checkpoint left past the cut would
    otherwise be replayed as the transaction's result without running it."""
    with launched_with_datasources(config, tmp_path, 2) as (dbos, (first, second)):
        insert_first, insert_second = inserter(first), inserter(second)

        @DBOS.workflow()
        def writer(name: str) -> int:
            run = run_count(name)
            first.run_tx_step(None, insert_first, "a")
            second.run_tx_step(None, insert_second, "b")
            first.run_tx_step(None, insert_first, "c")
            second.run_tx_step(None, insert_second, "d")
            return run

        workflow_id = start(writer, "datasource")
        assert datasource_checkpoints(first.engine, workflow_id) == [1, 3]
        assert datasource_checkpoints(second.engine, workflow_id) == [2, 4]

        # A start_step the system database would reject must not get as far as the
        # checkpoints, which are deleted before it is ever consulted.
        with pytest.raises(ValueError, match="must be >= 1"):
            DBOS.rewind_workflow(workflow_id, start_step=0)
        assert datasource_checkpoints(first.engine, workflow_id) == [1, 3]
        assert datasource_checkpoints(second.engine, workflow_id) == [2, 4]
        assert runs["datasource"] == 1

        # Cut at the third step: each datasource keeps one checkpoint, loses one.
        with paused_queue("rewind_datasource_gate"):
            DBOS.rewind_workflow(
                workflow_id, start_step=3, queue_name="rewind_datasource_gate"
            )
            assert datasource_checkpoints(first.engine, workflow_id) == [1]
            assert datasource_checkpoints(second.engine, workflow_id) == [2]

        assert DBOS.retrieve_workflow(workflow_id).get_result() == 2
        assert datasource_checkpoints(first.engine, workflow_id) == [1, 3]
        assert datasource_checkpoints(second.engine, workflow_id) == [2, 4]
        # The first transaction on each datasource replayed, the second ran again.
        assert table_rows(first.engine) == ["a", "c", "c"]
        assert table_rows(second.engine) == ["b", "d", "d"]


@pytest.mark.asyncio
async def test_rewind_drops_async_datasource_checkpoints(
    config: DBOSConfig, cleanup_test_databases: None, tmp_path: Any
) -> None:
    async with launched_with_async_datasources(config, tmp_path, 2) as (
        dbos,
        (first, second),
    ):

        async def insert_first(v: str) -> str:
            await first.sql_session().execute(
                sa.text("INSERT INTO rows (v) VALUES (:v)"), {"v": v}
            )
            return v

        async def insert_second(v: str) -> str:
            await second.sql_session().execute(
                sa.text("INSERT INTO rows (v) VALUES (:v)"), {"v": v}
            )
            return v

        @DBOS.workflow()
        async def writer(name: str) -> int:
            run = run_count(name)
            await first.run_tx_step_async(None, insert_first, "a")
            await second.run_tx_step_async(None, insert_second, "b")
            await first.run_tx_step_async(None, insert_first, "c")
            await second.run_tx_step_async(None, insert_second, "d")
            return run

        workflow_id = str(uuid.uuid4())
        with SetWorkflowID(workflow_id):
            assert await writer("async-datasource") == 1
        assert await datasource_checkpoints_async(first.engine, workflow_id) == [1, 3]
        assert await datasource_checkpoints_async(second.engine, workflow_id) == [2, 4]

        handle = await DBOS.rewind_workflow_async(workflow_id)
        assert await handle.get_result() == 2
        assert await datasource_checkpoints_async(first.engine, workflow_id) == [1, 3]
        assert await datasource_checkpoints_async(second.engine, workflow_id) == [2, 4]
        # Everything ran again.
        assert await table_rows_async(first.engine) == ["a", "a", "c", "c"]
        assert await table_rows_async(second.engine) == ["b", "b", "d", "d"]


def test_a_failed_rewind_leaves_the_workflow_untouched_and_can_be_retried(
    config: DBOSConfig,
    cleanup_test_databases: None,
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The datasources' checkpoints are deleted first and the system database is
    rewound last, so whichever part fails the workflow keeps its terminal status
    and nothing stale is ever replayed: a checkpoint delete is idempotent, so
    rewinding again finishes the job."""
    with launched_with_datasources(config, tmp_path, 2) as (dbos, (first, second)):
        insert_first, insert_second = inserter(first), inserter(second)

        @DBOS.workflow()
        def writer(name: str) -> int:
            run = run_count(name)
            first.run_tx_step(None, insert_first, "a")
            second.run_tx_step(None, insert_second, "b")
            first.run_tx_step(None, insert_first, "c")
            second.run_tx_step(None, insert_second, "d")
            return run

        workflow_id = start(writer, "delete-failure")
        assert datasource_checkpoints(first.engine, workflow_id) == [1, 3]
        assert datasource_checkpoints(second.engine, workflow_id) == [2, 4]

        # One datasource's delete fails: the system database is not rewound, so
        # the workflow stays SUCCESS and is not re-enqueued. The datasource
        # deleted before it is already cleared, which a retry tolerates.
        def failing_delete(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("datasource down")

        monkeypatch.setattr(second, "_delete_checkpoints", failing_delete)
        with pytest.raises(RuntimeError, match="datasource down"):
            DBOS.rewind_workflow(workflow_id)
        status = DBOS.retrieve_workflow(workflow_id).get_status()
        assert status.status == "SUCCESS"
        assert datasource_checkpoints(first.engine, workflow_id) == []
        assert datasource_checkpoints(second.engine, workflow_id) == [2, 4]
        assert runs["delete-failure"] == 1

        # The system database rewind fails: every checkpoint is already gone but
        # the workflow stays SUCCESS, so nothing runs against the missing history.
        monkeypatch.undo()

        def failing_rewind(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("system database unavailable")

        monkeypatch.setattr(dbos._sys_db, "rewind_workflow", failing_rewind)
        with pytest.raises(RuntimeError, match="system database unavailable"):
            DBOS.rewind_workflow(workflow_id)
        assert DBOS.retrieve_workflow(workflow_id).get_status().status == "SUCCESS"
        assert datasource_checkpoints(first.engine, workflow_id) == []
        assert datasource_checkpoints(second.engine, workflow_id) == []
        assert runs["delete-failure"] == 1

        # Retrying replays from scratch and repeats every transaction.
        monkeypatch.undo()
        assert DBOS.rewind_workflow(workflow_id).get_result() == 2
        assert datasource_checkpoints(first.engine, workflow_id) == [1, 3]
        assert datasource_checkpoints(second.engine, workflow_id) == [2, 4]
        assert table_rows(first.engine) == ["a", "a", "c", "c"]
        assert table_rows(second.engine) == ["b", "b", "d", "d"]


def test_rewind_with_datasources_refuses_an_active_workflow(
    config: DBOSConfig, cleanup_test_databases: None, tmp_path: Any
) -> None:
    """The checkpoints are deleted before the system database is rewound, so the
    terminal-state check has to run first or a running workflow would lose them."""
    with launched_with_datasources(config, tmp_path, 1) as (dbos, (ds,)):
        insert = inserter(ds)
        checkpointed, release = threading.Event(), threading.Event()

        @DBOS.workflow()
        def blocker() -> None:
            ds.run_tx_step(None, insert, "a")
            checkpointed.set()
            release.wait()

        handle = DBOS.start_workflow(blocker)
        assert checkpointed.wait(10)
        assert datasource_checkpoints(ds.engine, handle.workflow_id) == [1]
        with pytest.raises(DBOSException, match="only a workflow in a terminal state"):
            DBOS.rewind_workflow(handle.workflow_id)
        assert datasource_checkpoints(ds.engine, handle.workflow_id) == [1]
        release.set()
        handle.get_result()
        with pytest.raises(DBOSNonExistentWorkflowError):
            DBOS.rewind_workflow("no-such-workflow")


def test_client_rewind_leaves_datasources_alone(
    config: DBOSConfig, cleanup_test_databases: None, tmp_path: Any
) -> None:
    with launched_with_datasources(config, tmp_path, 2) as (dbos, (first, second)):
        insert_first, insert_second = inserter(first), inserter(second)

        @DBOS.workflow()
        def writer(name: str) -> int:
            run = run_count(name)
            first.run_tx_step(None, insert_first, "a")
            second.run_tx_step(None, insert_second, "b")
            first.run_tx_step(None, insert_first, "c")
            second.run_tx_step(None, insert_second, "d")
            return run

        workflow_id = start(writer, "client-datasource")
        assert config["system_database_url"] is not None
        client = DBOSClient(system_database_url=config["system_database_url"])
        try:
            # The client only rewinds the system database. The datasources'
            # checkpoints stay and the replay takes them as the transactions'
            # results: nothing runs again.
            assert client.rewind_workflow(workflow_id, start_step=3).get_result() == 2
            assert datasource_checkpoints(first.engine, workflow_id) == [1, 3]
            assert datasource_checkpoints(second.engine, workflow_id) == [2, 4]
            assert table_rows(first.engine) == ["a", "c"]
            assert table_rows(second.engine) == ["b", "d"]
        finally:
            client.destroy()


@pytest.mark.asyncio
async def test_async_client_rewind_leaves_datasources_alone(
    config: DBOSConfig, cleanup_test_databases: None, tmp_path: Any
) -> None:
    async with launched_with_async_datasources(config, tmp_path, 2) as (
        dbos,
        (first, second),
    ):

        async def insert_first(v: str) -> str:
            await first.sql_session().execute(
                sa.text("INSERT INTO rows (v) VALUES (:v)"), {"v": v}
            )
            return v

        async def insert_second(v: str) -> str:
            await second.sql_session().execute(
                sa.text("INSERT INTO rows (v) VALUES (:v)"), {"v": v}
            )
            return v

        @DBOS.workflow()
        async def writer(name: str) -> int:
            run = run_count(name)
            await first.run_tx_step_async(None, insert_first, "a")
            await second.run_tx_step_async(None, insert_second, "b")
            await first.run_tx_step_async(None, insert_first, "c")
            await second.run_tx_step_async(None, insert_second, "d")
            return run

        workflow_id = str(uuid.uuid4())
        with SetWorkflowID(workflow_id):
            assert await writer("async-client-datasource") == 1
        assert config["system_database_url"] is not None
        client = DBOSClient(system_database_url=config["system_database_url"])
        try:
            handle = await client.rewind_workflow_async(workflow_id, start_step=3)
            assert await handle.get_result() == 2
            assert await datasource_checkpoints_async(first.engine, workflow_id) == [
                1,
                3,
            ]
            assert await datasource_checkpoints_async(second.engine, workflow_id) == [
                2,
                4,
            ]
            assert await table_rows_async(first.engine) == ["a", "c"]
            assert await table_rows_async(second.engine) == ["b", "d"]
        finally:
            client.destroy()
