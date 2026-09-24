import asyncio
import threading
import time
import uuid
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest
from sqlalchemy.exc import OperationalError

from dbos import (
    DBOS,
    DBOSClient,
    DBOSContextEnsure,
    Debouncer,
    DebouncerClient,
    EnqueueOptions,
    PortableWorkflowError,
    SetEnqueueOptions,
    SetWorkflowID,
    WorkflowHandle,
    WorkflowSerializationFormat,
)
from dbos._dbos import WorkflowHandleAsync
from dbos._debug_trigger import DebugAction, DebugTriggers
from dbos._error import (
    DBOSErrorCode,
    DBOSException,
    DBOSQueueDeduplicatedError,
    DBOSStepNondeterminismError,
    DBOSWorkflowIDInUseError,
    is_workflow_id_in_use_error,
)
from dbos._registrations import get_dbos_func_name
from tests.conftest import (
    queue_entries_are_cleaned_up,
    reexecute_workflow_by_id,
    set_workflow_status,
)

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
    def parent_workflow(child_input: str, with_options: bool) -> str:
        handle: WorkflowHandle[str]
        if with_options:
            options: EnqueueOptions = {
                "queue_name": QUEUE_NAME,
                "workflow_name": get_dbos_func_name(gated_workflow),
                "deduplication_id": dedup_id,
                "duplication_policy": "return-existing",
            }
            handle = DBOS.enqueue_workflow_with_options(options, child_input)
        else:
            with SetEnqueueOptions(
                deduplication_id=dedup_id, duplication_policy="return-existing"
            ):
                handle = DBOS.enqueue_workflow(QUEUE_NAME, gated_workflow, child_input)
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
        parent_a = DBOS.start_workflow(parent_workflow, "second", False)
        assert parent_attached.acquire(timeout=30)
    finally:
        setattr(dbos._sys_db, "get_deduplicated_workflow", original)
    assert calls == 2

    # Parent B attaches through enqueue_workflow_with_options, which records the attach itself.
    parent_b = DBOS.start_workflow(parent_workflow, "third", True)
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

    # Parent B recorded the same child, and replays it the same way.
    steps_b = DBOS.list_workflow_steps(parent_b.workflow_id)
    assert [s["function_id"] for s in steps_b] == [1, 2, 3]
    assert steps_b[0]["child_workflow_id"] == child_handle.workflow_id
    forked_b: WorkflowHandle[str] = DBOS.fork_workflow(parent_b.workflow_id, 4)
    assert forked_b.get_result() == "first-done"
    assert len(marker_step_runs) == 2
    assert (
        DBOS.list_workflow_steps(forked_b.workflow_id)[0]["child_workflow_id"]
        == child_handle.workflow_id
    )


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


# Ways a parent can start a child workflow.
REUSE_VIAS = ["start", "enqueue", "enqueue_options", "call"]


def _register_reuse_workflows() -> SimpleNamespace:
    _register_queue()
    gate = threading.Event()

    @DBOS.workflow()
    def echo(input: str) -> str:
        if input == "fail":
            raise Exception("echo failed")
        return input

    @DBOS.workflow()
    def other_workflow(input: str) -> str:
        return input

    @DBOS.workflow()
    def gated(input: str) -> str:
        gate.wait()
        return input

    def run_echo(child_id: str, via: str, input: str) -> str:
        """Run echo as child_id under the reject policy."""
        with SetWorkflowID(child_id, workflow_id_reuse_policy="reject"):
            if via == "start":
                return DBOS.start_workflow(echo, input).get_result()
            if via == "enqueue":
                return DBOS.enqueue_workflow(QUEUE_NAME, echo, input).get_result()
            if via == "enqueue_options":
                # The ID and policy come from the ambient SetWorkflowID.
                options: EnqueueOptions = {
                    "queue_name": QUEUE_NAME,
                    "workflow_name": get_dbos_func_name(echo),
                }
                handle = DBOS.enqueue_workflow_with_options(options, input)
                return str(handle.get_result())
            assert via == "call"
            return echo(input)

    @DBOS.workflow()
    def start_child(child_id: str, via: str) -> str:
        try:
            return run_echo(child_id, via, "from-parent")
        except Exception as e:
            return f"rejected:{type(e).__name__}"

    @DBOS.workflow()
    def start_same_child_twice(child_id: str, via: str) -> str:
        run_echo(child_id, via, "from-parent")
        try:
            run_echo(child_id, via, "from-parent")
            return "second-attached"
        except Exception as e:
            if is_workflow_id_in_use_error(e):
                return "second-rejected"
            return f"unexpected:{e!r}"

    return SimpleNamespace(
        gate=gate,
        echo=echo,
        other_workflow=other_workflow,
        gated=gated,
        run_echo=run_echo,
        start_child=start_child,
        start_same_child_twice=start_same_child_twice,
    )


def _snapshot(workflow_id: str) -> Dict[str, Any]:
    """Every status field, comparable across reads (a deserialized error compares by identity)."""
    status = DBOS.get_workflow_status(workflow_id)
    assert status is not None
    fields = dict(vars(status))
    fields["error"] = repr(fields["error"])
    return fields


def _fail_next_init_commit() -> DebugAction:
    """Make the next workflow-init commit land but report a lost connection, so db_retry reruns it."""
    action = DebugAction().set_exception_to_throw(
        OperationalError(
            statement=None,
            params=None,
            orig=BaseException("Connection lost"),
            connection_invalidated=True,
        )
    )
    DebugTriggers.set_debug_trigger(DebugTriggers.DEBUG_TRIGGER_INITWF_COMMIT, action)
    return action


@pytest.mark.parametrize(
    "status", ["SUCCESS", "ERROR", "PENDING", "CANCELLED", "ENQUEUED", "DELAYED"]
)
def test_id_reuse_reject_existing(dbos: DBOS, status: str) -> None:
    wf = _register_reuse_workflows()
    workflow_id = f"reuse-{status}-{uuid.uuid4()}"
    func = wf.gated if status in ("PENDING", "CANCELLED") else wf.echo
    try:
        with SetWorkflowID(workflow_id):
            if status == "SUCCESS":
                DBOS.start_workflow(func, "original").get_result()
            elif status == "ERROR":
                handle = DBOS.start_workflow(func, "fail")
                with pytest.raises(Exception, match="echo failed"):
                    handle.get_result()
            elif status in ("PENDING", "CANCELLED"):
                DBOS.start_workflow(func, "original")
            elif status == "ENQUEUED":
                with SetEnqueueOptions(app_version="no-executor-runs-this"):
                    DBOS.enqueue_workflow(QUEUE_NAME, func, "original")
            else:
                with SetEnqueueOptions(delay_seconds=3600):
                    DBOS.enqueue_workflow(QUEUE_NAME, func, "original")
        if status == "CANCELLED":
            DBOS.cancel_workflow(workflow_id)
        before = _snapshot(workflow_id)
        assert before["status"] == status

        with pytest.raises(DBOSWorkflowIDInUseError) as exc_info:
            with SetWorkflowID(workflow_id, workflow_id_reuse_policy="reject"):
                DBOS.start_workflow(func, "new")
        assert exc_info.value.workflow_id == workflow_id
        assert exc_info.value.workflow_status == status
        assert exc_info.value.workflow_name == get_dbos_func_name(func)
        # The rejected start leaves every field of the existing workflow as it was, including its inputs.
        assert _snapshot(workflow_id) == before

        # A default-policy start attaches, and also leaves the row untouched.
        with SetWorkflowID(workflow_id):
            handle = DBOS.start_workflow(func, "new")
        assert handle.workflow_id == workflow_id
        assert _snapshot(workflow_id) == before
    finally:
        wf.gate.set()


def test_id_reuse_reject_fresh_id_runs_and_default_attaches(dbos: DBOS) -> None:
    wf = _register_reuse_workflows()
    workflow_id = f"reuse-fresh-{uuid.uuid4()}"
    with SetWorkflowID(workflow_id, workflow_id_reuse_policy="reject"):
        assert DBOS.start_workflow(wf.echo, "first").get_result() == "first"

    with SetWorkflowID(workflow_id):
        assert DBOS.start_workflow(wf.echo, "second").get_result() == "first"
    with SetWorkflowID(workflow_id):
        assert wf.echo("third") == "first"
    with pytest.raises(DBOSWorkflowIDInUseError):
        with SetWorkflowID(workflow_id, workflow_id_reuse_policy="reject"):
            wf.echo("fourth")


def test_id_reuse_reject_name_mismatch_raises_in_use(dbos: DBOS) -> None:
    wf = _register_reuse_workflows()
    workflow_id = f"reuse-mismatch-{uuid.uuid4()}"
    with SetWorkflowID(workflow_id):
        DBOS.start_workflow(wf.echo, "original").get_result()

    with pytest.raises(DBOSWorkflowIDInUseError) as exc_info:
        with SetWorkflowID(workflow_id, workflow_id_reuse_policy="reject"):
            DBOS.start_workflow(wf.other_workflow, "new")
    assert exc_info.value.workflow_name == get_dbos_func_name(wf.echo)


def test_id_reuse_invalid_policy(dbos: DBOS, client: DBOSClient) -> None:
    wf = _register_reuse_workflows()
    with pytest.raises(DBOSException, match="Invalid workflow_id_reuse_policy"):
        SetWorkflowID("some-id", workflow_id_reuse_policy="bogus")  # type: ignore[arg-type]
    options: EnqueueOptions = {
        "queue_name": QUEUE_NAME,
        "workflow_name": get_dbos_func_name(wf.echo),
        "workflow_id_reuse_policy": "bogus",  # type: ignore[typeddict-item]
    }
    with pytest.raises(DBOSException, match="Invalid workflow_id_reuse_policy"):
        client.enqueue(options, "x")


@pytest.mark.parametrize(
    "via",
    ["start", "client_enqueue", "enqueue_options"]
    + [f"child_{via}" for via in REUSE_VIAS],
)
def test_id_reuse_reject_survives_retried_init_commit(
    dbos: DBOS, client: DBOSClient, via: str
) -> None:
    wf = _register_reuse_workflows()
    armed: List[DebugAction] = []

    @DBOS.workflow()
    def parent_with_glitch(child_id: str, child_via: str) -> str:
        # Armed here, so the glitch lands on the child's combined insert.
        armed.append(_fail_next_init_commit())
        return str(wf.run_echo(child_id, child_via, "from-parent"))

    workflow_id = f"reuse-retry-{via}-{uuid.uuid4()}"
    options: EnqueueOptions = {
        "queue_name": QUEUE_NAME,
        "workflow_name": get_dbos_func_name(wf.echo),
        "workflow_id": workflow_id,
        "workflow_id_reuse_policy": "reject",
    }
    try:
        if via.startswith("child_"):
            parent_id = f"reuse-retry-parent-{via}-{uuid.uuid4()}"
            with SetWorkflowID(parent_id):
                parent = DBOS.start_workflow(
                    parent_with_glitch, workflow_id, via.removeprefix("child_")
                )
            assert parent.get_result() == "from-parent"
            steps = DBOS.list_workflow_steps(parent_id)
            assert steps[0]["child_workflow_id"] == workflow_id
            assert steps[0]["error"] is None
        else:
            armed.append(_fail_next_init_commit())
            handle: WorkflowHandle[str]
            if via == "start":
                with SetWorkflowID(workflow_id, workflow_id_reuse_policy="reject"):
                    handle = DBOS.start_workflow(wf.echo, "retried")
            elif via == "client_enqueue":
                handle = client.enqueue(options, "retried")
            else:
                handle = DBOS.enqueue_workflow_with_options(options, "retried")
            assert handle.get_result() == "retried"
        # The glitch fired, so the insert was retried and recognized its own row.
        assert len(armed) == 1
        assert armed[0].exception_to_throw is None
    finally:
        DebugTriggers.clear_debug_triggers()


def test_id_reuse_reject_dedup_interplay(dbos: DBOS) -> None:
    wf = _register_reuse_workflows()
    workflow_id = f"reuse-dedup-{uuid.uuid4()}"
    dedup_id = f"dedup-{uuid.uuid4()}"

    def enqueue(id: str) -> None:
        with SetWorkflowID(id, workflow_id_reuse_policy="reject"):
            with SetEnqueueOptions(deduplication_id=dedup_id, delay_seconds=3600):
                DBOS.enqueue_workflow(QUEUE_NAME, wf.echo, "x")

    enqueue(workflow_id)
    # The same ID collides on the workflow ID before the deduplication ID.
    with pytest.raises(DBOSWorkflowIDInUseError):
        enqueue(workflow_id)
    with pytest.raises(DBOSQueueDeduplicatedError):
        enqueue(f"reuse-dedup-other-{uuid.uuid4()}")


def test_id_reuse_reject_enqueue_with_options(dbos: DBOS) -> None:
    wf = _register_reuse_workflows()
    workflow_id = f"reuse-ewo-{uuid.uuid4()}"
    with SetWorkflowID(workflow_id):
        DBOS.start_workflow(wf.echo, "original").get_result()

    def options(id: str) -> EnqueueOptions:
        return {
            "queue_name": QUEUE_NAME,
            "workflow_name": get_dbos_func_name(wf.echo),
            "workflow_id": id,
            "workflow_id_reuse_policy": "reject",
        }

    with pytest.raises(DBOSWorkflowIDInUseError):
        DBOS.enqueue_workflow_with_options(options(workflow_id), "new")

    fresh = DBOS.enqueue_workflow_with_options(
        options(f"reuse-ewo-fresh-{uuid.uuid4()}"), "fresh"
    )
    assert fresh.get_result() == "fresh"

    # An explicit option wins over the ambient policy, with the ambient ID supplying the ID.
    attach = options(workflow_id)
    del attach["workflow_id"]
    attach["workflow_id_reuse_policy"] = "return-existing"
    with SetWorkflowID(workflow_id, workflow_id_reuse_policy="reject"):
        handle = DBOS.enqueue_workflow_with_options(attach, "new")
    assert handle.get_result() == "original"

    # The ambient policy belongs to the ambient ID, so an explicit options ID attaches.
    unset = options(workflow_id)
    del unset["workflow_id_reuse_policy"]
    with SetWorkflowID(
        f"reuse-ewo-ambient-{uuid.uuid4()}", workflow_id_reuse_policy="reject"
    ):
        handle = DBOS.enqueue_workflow_with_options(unset, "new")
    assert handle.get_result() == "original"


def test_id_reuse_reject_client_enqueue(dbos: DBOS, client: DBOSClient) -> None:
    wf = _register_reuse_workflows()
    workflow_id = f"reuse-client-{uuid.uuid4()}"
    with SetWorkflowID(workflow_id):
        DBOS.start_workflow(wf.echo, "original").get_result()
    before = _snapshot(workflow_id)

    def options(id: str, *, singleton: bool = False) -> EnqueueOptions:
        opts: EnqueueOptions = {
            "queue_name": QUEUE_NAME,
            "workflow_name": get_dbos_func_name(wf.echo),
            "workflow_id": id,
            "workflow_id_reuse_policy": "reject",
        }
        if singleton:
            opts["deduplication_id"] = f"dedup-{uuid.uuid4()}"
            opts["duplication_policy"] = "return-existing"
        return opts

    def fresh_id() -> str:
        return f"reuse-client-fresh-{uuid.uuid4()}"

    with pytest.raises(DBOSWorkflowIDInUseError):
        client.enqueue(options(workflow_id), "new")
    with pytest.raises(DBOSWorkflowIDInUseError):
        client.enqueue(options(workflow_id, singleton=True), "new")

    fresh: WorkflowHandle[str] = client.enqueue(options(fresh_id()), "fresh")
    assert fresh.get_result() == "fresh"
    singleton: WorkflowHandle[str] = client.enqueue(
        options(fresh_id(), singleton=True), "singleton"
    )
    assert singleton.get_result() == "singleton"

    with client._sys_db.engine.connect() as conn:
        with conn.begin():
            with pytest.raises(DBOSWorkflowIDInUseError):
                client.enqueue_in_transaction(conn, options(workflow_id), "new")
            # The rejection leaves the caller's transaction usable; committing it changes nothing.
    assert _snapshot(workflow_id) == before

    with client._sys_db.engine.connect() as conn:
        with conn.begin():
            in_tx: WorkflowHandle[str] = client.enqueue_in_transaction(
                conn, options(fresh_id()), "in-tx"
            )
    assert in_tx.get_result() == "in-tx"


@pytest.mark.parametrize("via", REUSE_VIAS)
def test_id_reuse_reject_child_is_recorded_for_replay(dbos: DBOS, via: str) -> None:
    wf = _register_reuse_workflows()
    child_id = f"reuse-child-{via}-{uuid.uuid4()}"
    with SetWorkflowID(child_id):
        DBOS.start_workflow(wf.echo, "original").get_result()

    parent_id = f"reuse-parent-{via}-{uuid.uuid4()}"
    with SetWorkflowID(parent_id):
        parent = DBOS.start_workflow(wf.start_child, child_id, via)
    assert parent.get_result() == "rejected:DBOSWorkflowIDInUseError"

    steps = DBOS.list_workflow_steps(parent_id)
    assert steps[0]["function_id"] == 1
    assert isinstance(steps[0]["error"], DBOSWorkflowIDInUseError)

    # With the child gone a fresh start would succeed, so the fork's rejection must come from the checkpoint.
    DBOS.delete_workflow(child_id)
    assert DBOS.get_workflow_status(child_id) is None
    forked = DBOS.fork_workflow(parent_id, 2)
    assert forked.get_result() == "rejected:DBOSWorkflowIDInUseError"
    assert DBOS.get_workflow_status(child_id) is None


@pytest.mark.parametrize("via", REUSE_VIAS)
def test_id_reuse_reject_same_parent_reuse(dbos: DBOS, via: str) -> None:
    wf = _register_reuse_workflows()
    child_id = f"reuse-same-parent-{via}-{uuid.uuid4()}"
    parent_id = f"reuse-same-parent-p-{via}-{uuid.uuid4()}"
    with SetWorkflowID(parent_id):
        parent = DBOS.start_workflow(wf.start_same_child_twice, child_id, via)
    assert parent.get_result() == "second-rejected"

    steps = {s["function_id"]: s for s in DBOS.list_workflow_steps(parent_id)}
    assert steps[1]["child_workflow_id"] == child_id
    assert isinstance(steps[3]["error"], DBOSWorkflowIDInUseError)

    # Replay serves the recorded child, then the recorded rejection.
    assert reexecute_workflow_by_id(dbos, parent_id).get_result() == "second-rejected"


def test_init_child_workflow_is_atomic(dbos: DBOS) -> None:
    wf = _register_reuse_workflows()
    child_id = f"reuse-atomic-child-{uuid.uuid4()}"
    parent_id = f"reuse-atomic-parent-{uuid.uuid4()}"
    with SetWorkflowID(parent_id):
        assert (
            DBOS.start_workflow(wf.start_child, child_id, "start").get_result()
            == "from-parent"
        )
    status = dbos._sys_db.get_workflow_status(child_id)
    assert status is not None

    def init_child(workflow_id: str) -> None:
        child_status = status.copy()
        child_status["workflow_uuid"] = workflow_id
        dbos._sys_db.init_child_workflow(
            child_status,
            creator_xid=str(uuid.uuid4()),
            parent_workflow_id=parent_id,
            parent_function_id=1,
            function_name=child_status["name"],
            started_at_epoch_ms=int(time.time() * 1000),
        )

    # Re-recording the same child, as a db_retry after a landed commit does, is idempotent.
    init_child(child_id)
    # A different child at the same step conflicts, and its status row rolls back with the step.
    other_id = f"reuse-atomic-other-{uuid.uuid4()}"
    with pytest.raises(DBOSStepNondeterminismError):
        init_child(other_id)
    assert DBOS.get_workflow_status(other_id) is None


@pytest.mark.parametrize("via", ["start", "enqueue_options"])
def test_id_reuse_reject_portable_replay(dbos: DBOS, via: str) -> None:
    _register_queue()

    @DBOS.workflow(serialization_type=WorkflowSerializationFormat.PORTABLE)
    def portable_echo(input: str) -> str:
        return input

    def start_portable(child_id: str, input: str) -> None:
        with SetWorkflowID(child_id, workflow_id_reuse_policy="reject"):
            if via == "start":
                DBOS.start_workflow(portable_echo, input)
            else:
                options: EnqueueOptions = {
                    "queue_name": QUEUE_NAME,
                    "workflow_name": get_dbos_func_name(portable_echo),
                    "serialization_type": WorkflowSerializationFormat.PORTABLE,
                }
                DBOS.enqueue_workflow_with_options(options, input)

    @DBOS.workflow()
    def parent(child_id: str) -> str:
        start_portable(child_id, "first")
        DBOS.retrieve_workflow(child_id).get_result()
        try:
            start_portable(child_id, "second")
            return "attached"
        except DBOSWorkflowIDInUseError as e:
            return f"rejected:{e.workflow_status}"

    child_id = f"reuse-portable-child-{via}-{uuid.uuid4()}"
    parent_id = f"reuse-portable-parent-{via}-{uuid.uuid4()}"
    with SetWorkflowID(parent_id):
        assert DBOS.start_workflow(parent, child_id).get_result() == "rejected:SUCCESS"
    # The rejection is saved in the default format, so a plain except catches its replay too.
    assert reexecute_workflow_by_id(dbos, parent_id).get_result() == "rejected:SUCCESS"
    rejected = [s for s in DBOS.list_workflow_steps(parent_id) if s["error"]]
    assert len(rejected) == 1
    assert isinstance(rejected[0]["error"], DBOSWorkflowIDInUseError)

    # A checkpoint stored in portable form replays as PortableWorkflowError, which the helper matches.
    assert is_workflow_id_in_use_error(
        PortableWorkflowError("in use", DBOSWorkflowIDInUseError.__name__)
    )
    # A custom serializer may rebuild a generic error that keeps only the code.
    assert is_workflow_id_in_use_error(
        DBOSException("in use", dbos_error_code=DBOSErrorCode.WorkflowIDInUse.value)
    )
    assert not is_workflow_id_in_use_error(DBOSException("other"))


def test_debouncer_rejects_reject_reuse_policy(dbos: DBOS, client: DBOSClient) -> None:
    wf = _register_reuse_workflows()

    debouncer = Debouncer.create(wf.echo, queue=QUEUE_NAME)
    # An outer context survives the SetWorkflowID block, as a workflow's does.
    with DBOSContextEnsure() as ctx:
        with pytest.raises(DBOSException, match="workflow_id_reuse_policy 'reject'"):
            with SetWorkflowID(str(uuid.uuid4()), workflow_id_reuse_policy="reject"):
                debouncer.debounce("key", 0.1, "first")
        # The rejected debounce consumed the pinned ID, so the next start cannot inherit it.
        assert ctx.id_assigned_for_next_workflow == ""
        assert ctx.workflow_id_reuse_policy is None

    client_debouncer = DebouncerClient(
        client,
        workflow_options={
            "queue_name": QUEUE_NAME,
            "workflow_name": get_dbos_func_name(wf.echo),
            "workflow_id_reuse_policy": "reject",
        },
    )
    with pytest.raises(DBOSException, match="workflow_id_reuse_policy 'reject'"):
        client_debouncer.debounce("key", 0.1, "first")


@pytest.mark.asyncio
async def test_id_reuse_reject_async(dbos: DBOS, client: DBOSClient) -> None:
    await DBOS.register_queue_async(QUEUE_NAME, polling_interval_sec=0.1)

    @DBOS.workflow()
    async def echo_async(input: str) -> str:
        return input

    @DBOS.workflow()
    async def start_child_async(child_id: str, direct: bool) -> str:
        try:
            with SetWorkflowID(child_id, workflow_id_reuse_policy="reject"):
                if direct:
                    return await echo_async("from-parent")
                handle = await DBOS.start_workflow_async(echo_async, "from-parent")
                return await handle.get_result()
        except Exception as e:
            return f"rejected:{type(e).__name__}"

    workflow_id = f"reuse-async-{uuid.uuid4()}"
    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(echo_async, "original")
    assert await handle.get_result() == "original"

    with pytest.raises(DBOSWorkflowIDInUseError):
        with SetWorkflowID(workflow_id, workflow_id_reuse_policy="reject"):
            await DBOS.start_workflow_async(echo_async, "new")
    with pytest.raises(DBOSWorkflowIDInUseError):
        with SetWorkflowID(workflow_id, workflow_id_reuse_policy="reject"):
            await echo_async("new")

    options: EnqueueOptions = {
        "queue_name": QUEUE_NAME,
        "workflow_name": get_dbos_func_name(echo_async),
        "workflow_id": workflow_id,
        "workflow_id_reuse_policy": "reject",
    }
    with pytest.raises(DBOSWorkflowIDInUseError):
        await DBOS.enqueue_workflow_with_options_async(options, "new")
    with pytest.raises(DBOSWorkflowIDInUseError):
        await client.enqueue_async(options, "new")

    for direct in (False, True):
        parent_id = f"reuse-async-parent-{direct}-{uuid.uuid4()}"
        with SetWorkflowID(parent_id):
            parent = await DBOS.start_workflow_async(
                start_child_async, workflow_id, direct
            )
        assert await parent.get_result() == "rejected:DBOSWorkflowIDInUseError"
        steps = await DBOS.list_workflow_steps_async(parent_id)
        assert isinstance(steps[0]["error"], DBOSWorkflowIDInUseError)
