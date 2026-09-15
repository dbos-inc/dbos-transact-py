import contextlib
import threading
import uuid
from typing import Any, Dict, Iterator, List, Optional

import pytest
import sqlalchemy as sa

from dbos import DBOS, DBOSClient, SetEnqueueOptions, SetWorkflowID
from dbos._error import DBOSException, DBOSNonExistentWorkflowError
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


def test_rewind_unconsumes_notifications(dbos: DBOS) -> None:
    @DBOS.workflow()
    def receiver(name: str) -> str:
        run = run_count(name)
        first = DBOS.recv("cmd", timeout_seconds=10)
        second = DBOS.recv("cmd", timeout_seconds=10)
        return f"{first}{second}:{run}"

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(receiver, "partial-unconsume")
    DBOS.send(workflow_id, "a", "cmd")
    DBOS.send(workflow_id, "b", "cmd")
    assert handle.get_result() == "ab:1"

    # Each recv stamped the row it took with its own step, which is what lets the
    # rewind put back exactly the messages the discarded steps consumed.
    first_recv = step_id_of(workflow_id, "DBOS.recv")
    second_recv = step_id_of(workflow_id, "DBOS.recv", occurrence=1)
    assert first_recv != second_recv
    assert mailbox(dbos, workflow_id) == [
        ("a", True, first_recv),
        ("b", True, second_recv),
    ]

    with paused_queue("rewind_unconsume_gate"):
        dbos._sys_db.rewind_workflows(
            [workflow_id], [second_recv], queue_name="rewind_unconsume_gate"
        )
        # Only the message the discarded steps took comes back, and its stamp is
        # cleared. The first recv's message stays consumed: its step survived the cut.
        assert mailbox(dbos, workflow_id) == [
            ("a", True, first_recv),
            ("b", False, None),
        ]

    # The replay re-consumes it rather than blocking on an empty mailbox, and
    # re-stamps it with the same step.
    assert DBOS.retrieve_workflow(workflow_id).get_result() == "ab:2"
    assert mailbox(dbos, workflow_id) == [
        ("a", True, first_recv),
        ("b", True, second_recv),
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
        dbos._sys_db.rewind_workflows(
            [workflow_id], [cut], queue_name="rewind_events_gate"
        )
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


def test_rewind_discards_stream_entries(dbos: DBOS) -> None:
    @DBOS.workflow()
    def writer(name: str) -> str:
        run = run_count(name)
        DBOS.write_stream("log", f"a{run}")
        DBOS.write_stream("log", f"b{run}")
        return f"run{run}"

    workflow_id = start(writer, "stream-discard")
    assert list(DBOS.read_stream(workflow_id, "log")) == ["a1", "b1"]

    dbos._sys_db.rewind_workflows([workflow_id], [1])
    assert DBOS.retrieve_workflow(workflow_id).get_result() == "run2"

    # The discarded run's entries must not be spliced together with the replay's.
    assert list(DBOS.read_stream(workflow_id, "log")) == ["a2", "b2"]


def test_rewind_reopens_a_closed_stream(dbos: DBOS) -> None:
    @DBOS.workflow()
    def writer(name: str) -> str:
        run = run_count(name)
        DBOS.write_stream("out", f"v{run}")
        DBOS.close_stream("out")
        return f"run{run}"

    workflow_id = start(writer, "stream-close")
    assert list(DBOS.read_stream(workflow_id, "out")) == ["v1"]

    dbos._sys_db.rewind_workflows([workflow_id], [1])
    assert DBOS.retrieve_workflow(workflow_id).get_result() == "run2"

    assert list(DBOS.read_stream(workflow_id, "out")) == ["v2"]


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

    dbos._sys_db.rewind_workflows([workflow_id], [1])
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
    dbos._sys_db.rewind_workflows([child_id], [1])
    assert DBOS.retrieve_workflow(child_id).get_result() == 42

    # Then rewind the parent to the getResult that failed. The earlier step that
    # started the child survives, and the replay picks up the repaired result.
    get_result_step = step_id_of(workflow_id, "DBOS.getResult")
    dbos._sys_db.rewind_workflows([workflow_id], [get_result_step])
    assert DBOS.retrieve_workflow(workflow_id).get_result() == 42
    assert len(child_runs) == 2


#######################################
## Batching, queues, partitions
#######################################


def test_rewind_batch_with_per_workflow_steps(dbos: DBOS) -> None:
    executed: List[str] = []

    @DBOS.step()
    def marker(name: str, label: str) -> str:
        executed.append(f"{name}{label}")
        return label

    @DBOS.workflow()
    def two_steps(name: str) -> str:
        run = run_count(name)
        return f"{marker(name, 'a')}{marker(name, 'b')}{run}"

    ids = [start(two_steps, f"batch{i}") for i in range(2)]
    assert sorted(executed) == ["batch0a", "batch0b", "batch1a", "batch1b"]
    executed.clear()

    # First workflow keeps its first step, second is rewound to the beginning.
    dbos._sys_db.rewind_workflows(ids, [2, 1])
    for workflow_id in ids:
        assert DBOS.retrieve_workflow(workflow_id).get_result() == "ab2"

    # batch0 re-ran only its second step while batch1 re-ran both.
    assert sorted(executed) == ["batch0b", "batch1a", "batch1b"]
    assert step_ids(dbos, ids[0]) == [1, 2]
    assert step_ids(dbos, ids[1]) == [1, 2]


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

    dbos._sys_db.rewind_workflows(
        [workflow_id],
        [1],
        queue_name="rewind_partitioned",
        queue_partition_key="repaired",
    )
    assert DBOS.retrieve_workflow(workflow_id).get_result() == 2
    status = DBOS.retrieve_workflow(workflow_id).get_status()
    assert status.queue_name == "rewind_partitioned"
    assert status.queue_partition_key == "repaired"

    # Omitting the key clears it, omitting the queue falls back to the internal queue
    dbos._sys_db.rewind_workflows([workflow_id], [1])
    assert DBOS.retrieve_workflow(workflow_id).get_result() == 3
    status = DBOS.retrieve_workflow(workflow_id).get_status()
    assert status.queue_name == INTERNAL_QUEUE_NAME
    assert status.queue_partition_key is None


def test_rewind_batch_is_all_or_nothing(dbos: DBOS) -> None:
    @DBOS.workflow()
    def counter(name: str) -> int:
        return run_count(name)

    good = start(counter, "atomic")
    missing = str(uuid.uuid4())

    with pytest.raises(DBOSNonExistentWorkflowError):
        dbos._sys_db.rewind_workflows([good, missing], [1, 1])

    # The healthy workflow in the batch was not touched.
    assert DBOS.retrieve_workflow(good).get_status().status == "SUCCESS"
    assert DBOS.retrieve_workflow(good).get_result() == 1
    assert runs["atomic"] == 1


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
        dbos._sys_db.rewind_workflows(
            [workflow_id], [1], queue_name="rewind_gate", queue_partition_key="pk"
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
        assert [row[1] for row in mailbox(dbos, workflow_id)] == [False]

    assert DBOS.retrieve_workflow(workflow_id).get_result() == "run2"
    assert events_of(dbos, workflow_id) == {"phase": "run2"}


#######################################
## Refusals and validation
#######################################


def test_rewind_refuses_an_active_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def counter(name: str) -> int:
        return run_count(name)

    with paused_queue("rewind_active") as held:
        # PENDING: the gate's blocker is running on an executor right now, so its
        # history is not ours to delete.
        assert (
            status_row(dbos, held.workflow_id).status
            == WorkflowStatusString.PENDING.value
        )
        with pytest.raises(DBOSException, match="terminal state"):
            dbos._sys_db.rewind_workflows([held.workflow_id], [1])

        queued = str(uuid.uuid4())
        with SetWorkflowID(queued):
            DBOS.enqueue_workflow("rewind_active", counter, "queued")
        assert status_row(dbos, queued).status == WorkflowStatusString.ENQUEUED.value
        with pytest.raises(DBOSException, match="terminal state"):
            dbos._sys_db.rewind_workflows([queued], [1])

    assert DBOS.retrieve_workflow(queued).get_result() == 1


def test_rewind_a_cancelled_workflow(dbos: DBOS) -> None:
    """The refusal above says to cancel first, so that path has to work."""
    released = threading.Event()
    started = threading.Event()

    @DBOS.workflow()
    def blocker(name: str) -> str:
        run = run_count(name)
        if run == 1:
            started.set()
            released.wait()
        return f"run{run}"

    workflow_id = str(uuid.uuid4())
    with SetWorkflowID(workflow_id):
        DBOS.start_workflow(blocker, "cancelled")
    assert started.wait(timeout=10)

    DBOS.cancel_workflow(workflow_id)
    released.set()
    assert status_row(dbos, workflow_id).status == WorkflowStatusString.CANCELLED.value

    dbos._sys_db.rewind_workflows([workflow_id], [1])
    assert DBOS.retrieve_workflow(workflow_id).get_result() == "run2"


def test_rewind_refuses_a_missing_workflow(dbos: DBOS) -> None:
    with pytest.raises(DBOSNonExistentWorkflowError):
        dbos._sys_db.rewind_workflows([str(uuid.uuid4())], [1])


def test_rewind_input_validation(dbos: DBOS) -> None:
    @DBOS.workflow()
    def counter(name: str) -> int:
        return run_count(name)

    workflow_id = start(counter, "validation")

    # An empty batch is a no-op, not an error.
    dbos._sys_db.rewind_workflows([], [])

    with pytest.raises(ValueError, match="same length"):
        dbos._sys_db.rewind_workflows([workflow_id], [1, 2])
    with pytest.raises(ValueError, match="duplicates"):
        dbos._sys_db.rewind_workflows([workflow_id, workflow_id], [1, 1])
    with pytest.raises(ValueError, match="must be >= 1"):
        dbos._sys_db.rewind_workflows([workflow_id], [0])

    # None of the above wrote anything.
    assert DBOS.retrieve_workflow(workflow_id).get_status().status == "SUCCESS"
    assert runs["validation"] == 1


#######################################
## Public API
#######################################


def test_public_rewind_api(dbos: DBOS, client: DBOSClient) -> None:
    @DBOS.workflow()
    def counter(name: str) -> int:
        return run_count(name)

    ids = [start(counter, f"public{i}") for i in range(2)]
    assert [h.get_result() for h in DBOS.rewind_workflows(ids)] == [2, 2]

    # start_steps defaults to the beginning for each workflow, and can be given
    # per workflow.
    ids = [start(counter, f"steps{i}") for i in range(2)]
    handles = DBOS.rewind_workflows(ids, start_steps=[1, 1])
    assert [h.get_result() for h in handles] == [2, 2]

    ids = [start(counter, f"client{i}") for i in range(2)]
    assert [h.get_result() for h in client.rewind_workflows(ids)] == [2, 2]


@pytest.mark.asyncio
async def test_public_rewind_api_async(dbos: DBOS, client: DBOSClient) -> None:
    @DBOS.workflow()
    def counter(name: str) -> int:
        return run_count(name)

    ids = [start(counter, f"async{i}") for i in range(2)]
    handles = await DBOS.rewind_workflows_async(ids)
    assert [await h.get_result() for h in handles] == [2, 2]

    ids = [start(counter, f"asyncclient{i}") for i in range(2)]
    client_handles = await client.rewind_workflows_async(ids)
    assert [await h.get_result() for h in client_handles] == [2, 2]


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
        result: int = DBOS.rewind_workflows([workflow_id])[0].get_result()
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