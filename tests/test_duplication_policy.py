import asyncio
import threading
import uuid
from typing import Any, List, Optional

import pytest

from dbos import (
    DBOS,
    DBOSClient,
    Debouncer,
    DebouncerClient,
    EnqueueOptions,
    SetEnqueueOptions,
    SetWorkflowID,
    WorkflowHandle,
)
from dbos._dbos import WorkflowHandleAsync
from dbos._error import DBOSException, DBOSQueueDeduplicatedError
from dbos._registrations import get_dbos_func_name
from tests.conftest import queue_entries_are_cleaned_up, set_workflow_status

QUEUE_NAME = "test_duplication_policy_queue"


def _register_queue() -> None:
    DBOS.register_queue(QUEUE_NAME, polling_interval_sec=0.1)


def test_return_existing_attaches(dbos: DBOS) -> None:
    _register_queue()
    workflow_event = threading.Event()

    @DBOS.workflow()
    def gated_workflow(input: str) -> str:
        workflow_event.wait()
        return f"{input}-done"

    dedup_id = str(uuid.uuid4())
    with SetEnqueueOptions(
        deduplication_id=dedup_id, duplication_policy="return-existing"
    ):
        handle1 = DBOS.enqueue_workflow(QUEUE_NAME, gated_workflow, "first")
        handle2 = DBOS.enqueue_workflow(QUEUE_NAME, gated_workflow, "second")

    # The second enqueue attaches to the first workflow, discarding its own arguments.
    assert handle2.workflow_id == handle1.workflow_id

    workflow_event.set()
    assert handle1.get_result() == "first-done"
    assert handle2.get_result() == "first-done"
    assert queue_entries_are_cleaned_up(dbos)


def test_return_existing_fresh_after_completion(dbos: DBOS) -> None:
    _register_queue()

    @DBOS.workflow()
    def simple_workflow(input: str) -> str:
        return f"{input}-done"

    dedup_id = str(uuid.uuid4())
    with SetEnqueueOptions(
        deduplication_id=dedup_id, duplication_policy="return-existing"
    ):
        handle1 = DBOS.enqueue_workflow(QUEUE_NAME, simple_workflow, "first")
    assert handle1.get_result() == "first-done"

    # The completed workflow released the deduplication ID, so this starts a new one.
    with SetEnqueueOptions(
        deduplication_id=dedup_id, duplication_policy="return-existing"
    ):
        handle2 = DBOS.enqueue_workflow(QUEUE_NAME, simple_workflow, "second")
    assert handle2.workflow_id != handle1.workflow_id
    assert handle2.get_result() == "second-done"


def test_return_existing_rejects_without_dedup_id(dbos: DBOS) -> None:
    _register_queue()

    @DBOS.workflow()
    def simple_workflow() -> str:
        return "done"

    with pytest.raises(DBOSException) as exc_info:
        with SetEnqueueOptions(duplication_policy="return-existing"):
            DBOS.enqueue_workflow(QUEUE_NAME, simple_workflow)
    assert "requires a deduplication_id" in str(exc_info.value)


def test_return_existing_rejects_without_queue(dbos: DBOS) -> None:
    @DBOS.workflow()
    def simple_workflow() -> str:
        return "done"

    # Not being enqueued: there is no queue to deduplicate on. Set no other enqueue
    # option, so only the policy itself can be what the error rejects.
    wfid = str(uuid.uuid4())
    with pytest.raises(DBOSException) as exc_info:
        with SetEnqueueOptions(duplication_policy="return-existing"):
            with SetWorkflowID(wfid):
                DBOS.start_workflow(simple_workflow)
    assert "duplication_policy" in str(exc_info.value)
    assert "requires a queue" in str(exc_info.value)
    # Rejected before any row is written, leaving no orphaned PENDING workflow.
    assert DBOS.get_workflow_status(wfid) is None

    # "reject" is the default and a no-op without a queue, so it is not rejected.
    with SetEnqueueOptions(duplication_policy="reject"):
        assert DBOS.start_workflow(simple_workflow).get_result() == "done"


def test_return_existing_rejects_invalid_policy(dbos: DBOS) -> None:
    _register_queue()

    @DBOS.workflow()
    def simple_workflow() -> str:
        return "done"

    with pytest.raises(DBOSException) as exc_info:
        with SetEnqueueOptions(
            deduplication_id=str(uuid.uuid4()),
            duplication_policy="return-something-else",  # type: ignore
        ):
            DBOS.enqueue_workflow(QUEUE_NAME, simple_workflow)
    assert "Invalid duplication_policy" in str(exc_info.value)


def test_return_existing_retries_when_slot_released(dbos: DBOS) -> None:
    """The holder can release the deduplication ID between our insert and the lookup.

    Forcing the first lookup to miss makes the retry loop iterate, which must claim
    the slot for a new workflow rather than raise.
    """
    _register_queue()
    workflow_event = threading.Event()

    @DBOS.workflow()
    def gated_workflow(input: str) -> str:
        workflow_event.wait()
        return f"{input}-done"

    dedup_id = str(uuid.uuid4())
    with SetEnqueueOptions(
        deduplication_id=dedup_id, duplication_policy="return-existing"
    ):
        handle1 = DBOS.enqueue_workflow(QUEUE_NAME, gated_workflow, "first")

    original = dbos._sys_db.get_deduplicated_workflow
    calls = 0

    def lookup_misses_once(queue_name: str, deduplication_id: str) -> Optional[str]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return None
        return original(queue_name, deduplication_id)

    setattr(dbos._sys_db, "get_deduplicated_workflow", lookup_misses_once)
    try:
        with SetEnqueueOptions(
            deduplication_id=dedup_id, duplication_policy="return-existing"
        ):
            handle2 = DBOS.enqueue_workflow(QUEUE_NAME, gated_workflow, "second")
        assert calls == 2
        assert handle2.workflow_id == handle1.workflow_id
    finally:
        setattr(dbos._sys_db, "get_deduplicated_workflow", original)

    workflow_event.set()
    assert handle1.get_result() == "first-done"
    assert handle2.get_result() == "first-done"


def test_return_existing_honors_set_workflow_id(dbos: DBOS) -> None:
    """A SetWorkflowID reservation must survive the retry loop.

    Otherwise the first iteration consumes the reserved ID and an iteration that
    goes on to win silently creates a workflow with a generated one.
    """
    _register_queue()

    @DBOS.workflow()
    def simple_workflow(input: str) -> str:
        return f"{input}-done"

    dedup_id = str(uuid.uuid4())
    reserved_id = str(uuid.uuid4())

    original_init = dbos._sys_db.init_workflow
    init_calls = 0

    def init_fails_once(status: Any, **kwargs: Any) -> Any:
        nonlocal init_calls
        init_calls += 1
        if init_calls == 1:
            raise DBOSQueueDeduplicatedError(
                status["workflow_uuid"], QUEUE_NAME, dedup_id
            )
        return original_init(status, **kwargs)

    setattr(dbos._sys_db, "init_workflow", init_fails_once)
    setattr(dbos._sys_db, "get_deduplicated_workflow", lambda *_: None)
    try:
        with SetEnqueueOptions(
            deduplication_id=dedup_id, duplication_policy="return-existing"
        ):
            with SetWorkflowID(reserved_id):
                handle = DBOS.enqueue_workflow(QUEUE_NAME, simple_workflow, "reserved")
        assert init_calls == 2
        assert handle.workflow_id == reserved_id
    finally:
        setattr(dbos._sys_db, "init_workflow", original_init)
        delattr(dbos._sys_db, "get_deduplicated_workflow")

    assert handle.get_result() == "reserved-done"


def test_return_existing_in_parent_workflow(dbos: DBOS) -> None:
    """A parent attaching to an existing workflow consumes exactly one function ID.

    The retry loop must not burn one function ID per iteration: later operations
    would land at function IDs that no longer match what replay expects.
    """
    _register_queue()
    workflow_event = threading.Event()
    # Released by each parent once it has attached, so the gate below only opens
    # after both have: a parent that enqueued after the holder completed would
    # find the deduplication ID free and start a child of its own.
    parent_attached = threading.Semaphore(0)
    # Appended to rather than incremented: two parents run this step concurrently.
    marker_step_runs: List[int] = []
    dedup_id = str(uuid.uuid4())

    @DBOS.workflow()
    def gated_workflow(input: str) -> str:
        workflow_event.wait()
        return f"{input}-done"

    @DBOS.step()
    def marker_step() -> str:
        marker_step_runs.append(1)
        return "after-attach"

    @DBOS.workflow()
    def parent_workflow(child_input: str) -> str:
        with SetEnqueueOptions(
            deduplication_id=dedup_id, duplication_policy="return-existing"
        ):
            handle: WorkflowHandle[str] = DBOS.enqueue_workflow(
                QUEUE_NAME, gated_workflow, child_input
            )
        parent_attached.release()
        result = handle.get_result()
        marker_step()
        return result

    # The first child holds the deduplication ID; both parents attach to it.
    with SetEnqueueOptions(
        deduplication_id=dedup_id, duplication_policy="return-existing"
    ):
        child_handle = DBOS.enqueue_workflow(QUEUE_NAME, gated_workflow, "first")

    # Force the lookup to miss once so the retry loop iterates on the original run.
    original = dbos._sys_db.get_deduplicated_workflow
    calls = 0

    def lookup_misses_once(queue_name: str, deduplication_id: str) -> Optional[str]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return None
        return original(queue_name, deduplication_id)

    # Parent A runs alone under the patch, so it is deterministically the one whose
    # lookup misses and whose function IDs the assertions below cover.
    setattr(dbos._sys_db, "get_deduplicated_workflow", lookup_misses_once)
    try:
        parent_a = DBOS.start_workflow(parent_workflow, "second")
        assert parent_attached.acquire(timeout=30)
    finally:
        setattr(dbos._sys_db, "get_deduplicated_workflow", original)
    assert calls == 2

    parent_b = DBOS.start_workflow(parent_workflow, "third")
    assert parent_attached.acquire(timeout=30)

    # Both parents are attached, so releasing the holder cannot let either start its own child.
    workflow_event.set()
    assert parent_a.get_result() == "first-done"
    assert parent_b.get_result() == "first-done"

    assert child_handle.get_result() == "first-done"
    assert len(marker_step_runs) == 2

    # The attach, the awaited result, and marker_step: three operations at
    # contiguous function IDs, because the retry consumed no extra ID.
    steps = DBOS.list_workflow_steps(parent_a.workflow_id)
    assert len(steps) == 3
    assert [s["function_id"] for s in steps] == [1, 2, 3]
    assert steps[0]["child_workflow_id"] == child_handle.workflow_id
    marker = next(s for s in steps if s["function_name"] == marker_step.__qualname__)
    assert marker["function_id"] == 3

    # Forking past the last step replays every cached operation instead of rerunning it.
    forked: WorkflowHandle[str] = DBOS.fork_workflow(
        parent_a.workflow_id, marker["function_id"] + 1
    )
    assert forked.get_result() == "first-done"
    assert len(marker_step_runs) == 2
    forked_steps = DBOS.list_workflow_steps(forked.workflow_id)
    assert len(forked_steps) == 3
    assert forked_steps[0]["child_workflow_id"] == child_handle.workflow_id
    assert [s["function_id"] for s in forked_steps] == [s["function_id"] for s in steps]


def test_return_existing_recovery(dbos: DBOS) -> None:
    """A recovered parent re-attaches to the same workflow it attached to before."""
    _register_queue()
    workflow_event = threading.Event()
    dedup_id = str(uuid.uuid4())

    @DBOS.workflow()
    def gated_workflow(input: str) -> str:
        workflow_event.wait()
        return f"{input}-done"

    @DBOS.workflow()
    def parent_workflow() -> str:
        with SetEnqueueOptions(
            deduplication_id=dedup_id, duplication_policy="return-existing"
        ):
            handle: WorkflowHandle[str] = DBOS.enqueue_workflow(
                QUEUE_NAME, gated_workflow, "child"
            )
        return handle.workflow_id

    with SetEnqueueOptions(
        deduplication_id=dedup_id, duplication_policy="return-existing"
    ):
        first_handle = DBOS.enqueue_workflow(QUEUE_NAME, gated_workflow, "first")

    parent_id = str(uuid.uuid4())
    with SetWorkflowID(parent_id):
        attached_id = parent_workflow()
    assert attached_id == first_handle.workflow_id

    steps = DBOS.list_workflow_steps(parent_id)
    assert len(steps) == 1
    assert steps[0]["child_workflow_id"] == first_handle.workflow_id
    # The attach is checkpointed as a child, not as a deduplication error.
    assert steps[0]["error"] is None

    # Let the holder finish, which releases the deduplication ID: a recovered parent
    # that ignored its checkpoint would now win the ID and start a child of its own.
    workflow_event.set()
    assert first_handle.get_result() == "first-done"

    set_workflow_status(dbos._sys_db, parent_id, "PENDING")
    DBOS._recover_pending_workflows()
    recovered: WorkflowHandle[str] = DBOS.retrieve_workflow(parent_id)
    assert recovered.get_result() == first_handle.workflow_id
    # Still one child, recorded once, pointing at the workflow it originally attached to.
    recovered_steps = DBOS.list_workflow_steps(parent_id)
    assert len(recovered_steps) == 1
    assert recovered_steps[0]["child_workflow_id"] == first_handle.workflow_id


@pytest.mark.asyncio
async def test_return_existing_async(dbos: DBOS) -> None:
    await DBOS.register_queue_async(QUEUE_NAME, polling_interval_sec=0.1)
    workflow_event = asyncio.Event()

    @DBOS.workflow()
    async def gated_workflow(input: str) -> str:
        await workflow_event.wait()
        return f"{input}-done"

    dedup_id = str(uuid.uuid4())
    with SetEnqueueOptions(
        deduplication_id=dedup_id, duplication_policy="return-existing"
    ):
        handle1: WorkflowHandleAsync[str] = await DBOS.enqueue_workflow_async(
            QUEUE_NAME, gated_workflow, "first"
        )
        handle2: WorkflowHandleAsync[str] = await DBOS.enqueue_workflow_async(
            QUEUE_NAME, gated_workflow, "second"
        )
    assert handle2.workflow_id == handle1.workflow_id

    workflow_event.set()
    assert await handle1.get_result() == "first-done"
    assert await handle2.get_result() == "first-done"


def test_return_existing_enqueue_with_options(dbos: DBOS) -> None:
    _register_queue()
    workflow_event = threading.Event()

    @DBOS.workflow()
    def gated_workflow(input: str) -> str:
        workflow_event.wait()
        return f"{input}-done"

    dedup_id = str(uuid.uuid4())
    options: EnqueueOptions = {
        "queue_name": QUEUE_NAME,
        "workflow_name": get_dbos_func_name(gated_workflow),
        "deduplication_id": dedup_id,
        "duplication_policy": "return-existing",
    }
    handle1: WorkflowHandle[str] = DBOS.enqueue_workflow_with_options(options, "first")
    handle2: WorkflowHandle[str] = DBOS.enqueue_workflow_with_options(options, "second")
    assert handle2.workflow_id == handle1.workflow_id

    workflow_event.set()
    assert handle1.get_result() == "first-done"
    assert handle2.get_result() == "first-done"


def test_client_return_existing(dbos: DBOS, client: DBOSClient) -> None:
    _register_queue()
    workflow_event = threading.Event()

    @DBOS.workflow()
    def gated_workflow(input: str) -> str:
        workflow_event.wait()
        return f"{input}-done"

    dedup_id = str(uuid.uuid4())
    options: EnqueueOptions = {
        "queue_name": QUEUE_NAME,
        "workflow_name": get_dbos_func_name(gated_workflow),
        "deduplication_id": dedup_id,
        "duplication_policy": "return-existing",
    }
    handle1: WorkflowHandle[str] = client.enqueue(options, "first")
    handle2: WorkflowHandle[str] = client.enqueue(options, "second")
    assert handle2.workflow_id == handle1.workflow_id

    workflow_event.set()
    assert handle1.get_result() == "first-done"
    assert handle2.get_result() == "first-done"


def test_client_return_existing_requires_dedup_id(client: DBOSClient) -> None:
    options: EnqueueOptions = {
        "queue_name": QUEUE_NAME,
        "workflow_name": "gated_workflow",
        "duplication_policy": "return-existing",
    }
    with pytest.raises(DBOSException) as exc_info:
        client.enqueue(options, "first")
    assert "requires a deduplication_id" in str(exc_info.value)


def test_client_return_existing_rejected_in_transaction(client: DBOSClient) -> None:
    options: EnqueueOptions = {
        "queue_name": QUEUE_NAME,
        "workflow_name": "gated_workflow",
        "deduplication_id": str(uuid.uuid4()),
        "duplication_policy": "return-existing",
    }
    with client._sys_db.engine.connect() as conn:
        with conn.begin():
            with pytest.raises(DBOSException) as exc_info:
                client.enqueue_in_transaction(conn, options, "first")
    assert "not supported by enqueue_in_transaction" in str(exc_info.value)


def test_debouncer_rejects_return_existing(dbos: DBOS, client: DBOSClient) -> None:
    _register_queue()

    @DBOS.workflow()
    def simple_workflow(input: str) -> str:
        return f"{input}-done"

    debouncer = Debouncer.create(simple_workflow, queue=QUEUE_NAME)
    with pytest.raises(DBOSException) as exc_info:
        with SetEnqueueOptions(duplication_policy="return-existing"):
            debouncer.debounce("key", 0.1, "first")
    assert "Cannot debounce" in str(exc_info.value)

    client_debouncer = DebouncerClient(
        client,
        workflow_options={
            "queue_name": QUEUE_NAME,
            "workflow_name": "simple_workflow",
            "duplication_policy": "return-existing",
        },
    )
    with pytest.raises(DBOSException) as exc_info:
        client_debouncer.debounce("key", 0.1, "first")
    assert "Cannot debounce" in str(exc_info.value)
