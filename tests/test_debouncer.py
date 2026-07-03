import uuid
from typing import Any

import pytest

from dbos import (
    DBOS,
    DBOSClient,
    Debouncer,
    DebouncerClient,
    SetWorkflowID,
    WorkflowHandle,
    WorkflowHandleAsync,
)
from dbos._client import EnqueueOptions
from dbos._context import SetEnqueueOptions, SetWorkflowTimeout
from dbos._error import DBOSException, DBOSQueueDeduplicatedError
from dbos._queue import Queue
from dbos._registrations import get_dbos_func_name
from dbos._utils import GlobalParams


def test_debouncer(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    @DBOS.step()
    def generate_uuid() -> str:
        return str(uuid.uuid4())

    def debouncer_test() -> None:
        debounce_period = 2

        debouncer = Debouncer.create(workflow)
        first_handle = debouncer.debounce("key", debounce_period, first_value)
        debouncer = Debouncer.create(workflow)
        second_handle = debouncer.debounce("key", debounce_period, second_value)
        assert first_handle.workflow_id == second_handle.workflow_id
        assert first_handle.get_result() == second_value
        assert second_handle.get_result() == second_value

        debouncer = Debouncer.create(workflow)
        third_handle = debouncer.debounce("key", debounce_period, third_value)
        debouncer = Debouncer.create(workflow)
        fourth_handle = debouncer.debounce("key", debounce_period, fourth_value)
        assert third_handle.workflow_id != first_handle.workflow_id
        assert third_handle.workflow_id == fourth_handle.workflow_id
        assert third_handle.get_result() == fourth_value
        assert fourth_handle.get_result() == fourth_value

        # Test SetWorkflowID works
        wfid = generate_uuid()
        with SetWorkflowID(wfid):
            handle = debouncer.debounce("key", debounce_period, first_value)
        assert handle.workflow_id == wfid
        assert handle.get_result() == first_value

    # First, run the test operations directly
    debouncer_test()

    # Then, run the test operations inside a workflow and verify they work there
    debouncer_test_workflow = DBOS.workflow()(debouncer_test)
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        debouncer_test_workflow()

    # Rerun the workflow, verify it looks up by name and still works
    dbos._sys_db.update_workflow_outcome(wfid, "PENDING")
    dbos._execute_workflow_id(wfid).get_result()


def test_debouncer_timeout(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    # Set a huge period but small timeout, verify workflows start after the timeout
    debouncer = Debouncer.create(
        workflow,
        debounce_timeout_sec=2,
    )
    long_debounce_period = 10000000

    first_handle = debouncer.debounce("key", long_debounce_period, first_value)
    second_handle = debouncer.debounce("key", long_debounce_period, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value

    third_handle = debouncer.debounce("key", long_debounce_period, third_value)
    fourth_handle = debouncer.debounce("key", long_debounce_period, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value

    # Submit first with a long period then with a short one, verify workflows start on time
    debouncer = Debouncer.create(
        workflow,
    )
    short_debounce_period = 1

    first_handle = debouncer.debounce("key", long_debounce_period, first_value)
    second_handle = debouncer.debounce("key", short_debounce_period, second_value)
    assert fourth_handle.workflow_id != first_handle.workflow_id
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value


def test_multiple_debouncers(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    # Set a huge period but small timeout, verify workflows start after the timeout
    debouncer_one = Debouncer.create(workflow)
    debouncer_two = Debouncer.create(workflow)
    debounce_period = 2

    first_handle = debouncer_one.debounce("key_one", debounce_period, first_value)
    second_handle = debouncer_one.debounce("key_one", debounce_period, second_value)
    third_handle = debouncer_two.debounce("key_two", debounce_period, third_value)
    fourth_handle = debouncer_two.debounce("key_two", debounce_period, fourth_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.workflow_id != third_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value


def test_debouncer_queue(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3
    queue = DBOS.register_queue("test-queue", priority_enabled=True)

    debouncer = Debouncer.create(workflow, queue=queue)
    debounce_period_sec = 2

    first_handle = debouncer.debounce("key", debounce_period_sec, first_value)
    second_handle = debouncer.debounce("key", debounce_period_sec, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value
    assert second_handle.get_status().queue_name == queue.name

    # Test SetWorkflowTimeout works
    with SetWorkflowTimeout(5.0):
        third_handle = debouncer.debounce("key", debounce_period_sec, third_value)
        fourth_handle = debouncer.debounce("key", debounce_period_sec, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value
    assert fourth_handle.get_status().queue_name == queue.name
    assert fourth_handle.get_status().workflow_timeout_ms == 5000.0
    assert fourth_handle.get_status().workflow_deadline_epoch_ms

    # Test SetWorkflowID works
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = debouncer.debounce("key", debounce_period_sec, first_value)
    assert handle.workflow_id == wfid
    assert handle.get_result() == first_value
    assert handle.get_status().queue_name == queue.name

    # SetEnqueueOptions app_version passes through (deduplication_id/delay/priority/partition key are rejected, see test_debounce_rejects_*).
    test_version = "test_version"
    GlobalParams.app_version = test_version
    with SetEnqueueOptions(app_version=test_version):
        handle = debouncer.debounce("key", debounce_period_sec, first_value)
    assert handle.get_result() == first_value
    assert handle.get_status().queue_name == queue.name
    assert handle.get_status().app_version == test_version

    # The queue argument also accepts a queue name as a string.
    debouncer_by_name = Debouncer.create(workflow, queue=queue.name)
    name_handle = debouncer_by_name.debounce(
        "string-key", debounce_period_sec, first_value
    )
    assert name_handle.get_result() == first_value
    assert name_handle.get_status().queue_name == queue.name


@pytest.mark.asyncio
async def test_debouncer_async(dbos: DBOS) -> None:

    @DBOS.workflow()
    async def workflow_async(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    debouncer = Debouncer.create_async(workflow_async)
    debounce_period_sec = 2

    first_handle = await debouncer.debounce_async(
        "key", debounce_period_sec, first_value
    )
    second_handle = await debouncer.debounce_async(
        "key", debounce_period_sec, second_value
    )
    assert first_handle.workflow_id == second_handle.workflow_id
    assert await first_handle.get_result() == second_value
    assert await second_handle.get_result() == second_value

    third_handle = await debouncer.debounce_async(
        "key", debounce_period_sec, third_value
    )
    fourth_handle = await debouncer.debounce_async(
        "key", debounce_period_sec, fourth_value
    )
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert await third_handle.get_result() == fourth_value
    assert await fourth_handle.get_result() == fourth_value


def test_debouncer_client(dbos: DBOS, client: DBOSClient) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3
    DBOS.register_queue("test-queue")

    options: EnqueueOptions = {
        "workflow_name": workflow.__qualname__,
        "queue_name": "test-queue",
    }
    debouncer = DebouncerClient(client, options)
    debounce_period_sec = 2

    first_handle: WorkflowHandle[int] = debouncer.debounce(
        "key", debounce_period_sec, first_value
    )
    second_handle: WorkflowHandle[int] = debouncer.debounce(
        "key", debounce_period_sec, second_value
    )
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value

    third_handle: WorkflowHandle[int] = debouncer.debounce(
        "key", debounce_period_sec, third_value
    )
    fourth_handle: WorkflowHandle[int] = debouncer.debounce(
        "key", debounce_period_sec, fourth_value
    )
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value

    wfid = str(uuid.uuid4())
    options["workflow_id"] = wfid
    handle: WorkflowHandle[int] = debouncer.debounce(
        "key", debounce_period_sec, first_value
    )
    assert handle.workflow_id == wfid
    assert handle.get_result() == first_value


@pytest.mark.asyncio
async def test_debouncer_client_async(dbos: DBOS, client: DBOSClient) -> None:

    @DBOS.workflow()
    async def workflow_async(x: int) -> int:
        return x

    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3
    await DBOS.register_queue_async("test-queue")

    options: EnqueueOptions = {
        "workflow_name": workflow_async.__qualname__,
        "queue_name": "test-queue",
    }
    debouncer = DebouncerClient(client, options)
    debounce_period_sec = 2

    first_handle: WorkflowHandleAsync[int] = await debouncer.debounce_async(
        "key", debounce_period_sec, first_value
    )
    second_handle: WorkflowHandleAsync[int] = await debouncer.debounce_async(
        "key", debounce_period_sec, second_value
    )
    assert first_handle.workflow_id == second_handle.workflow_id
    assert await first_handle.get_result() == second_value
    assert await second_handle.get_result() == second_value

    third_handle: WorkflowHandleAsync[int] = await debouncer.debounce_async(
        "key", debounce_period_sec, third_value
    )
    fourth_handle: WorkflowHandleAsync[int] = await debouncer.debounce_async(
        "key", debounce_period_sec, fourth_value
    )
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert await third_handle.get_result() == fourth_value
    assert await fourth_handle.get_result() == fourth_value

    wfid = str(uuid.uuid4())
    options["workflow_id"] = wfid
    handle: WorkflowHandleAsync[int] = await debouncer.debounce_async(
        "key", debounce_period_sec, first_value
    )
    assert handle.workflow_id == wfid
    assert await handle.get_result() == first_value


def test_debounce_flip_clears_dedup(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    debouncer = Debouncer.create(workflow)
    # A huge period keeps the workflow DELAYED until a later bounce shortens it.
    handle = debouncer.debounce("key", 1000000, 1)

    # While delayed, the workflow is marked debounced and holds the debounce key.
    status = dbos._sys_db.get_workflow_status(handle.workflow_id)
    assert status is not None
    assert status["status"] == "DELAYED"
    assert status["is_debounced"] is True
    assert status["deduplication_id"] == f"{get_dbos_func_name(workflow)}-key"

    # A bounce extends the delay and replaces the input; the same workflow runs.
    handle2 = debouncer.debounce("key", 1, 2)
    assert handle2.workflow_id == handle.workflow_id
    assert handle2.get_result() == 2

    # On the DELAYED->ENQUEUED transition its debounce key was cleared, so a later debounce with the same key starts fresh.
    status = dbos._sys_db.get_workflow_status(handle.workflow_id)
    assert status is not None
    assert status["deduplication_id"] is None

    handle3 = debouncer.debounce("key", 1, 3)
    assert handle3.workflow_id != handle.workflow_id
    assert handle3.get_result() == 3


def test_debounce_cancel_then_redebounce(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    debouncer = Debouncer.create(workflow)
    handle = debouncer.debounce("key", 1000000, 1)
    status = dbos._sys_db.get_workflow_status(handle.workflow_id)
    assert status is not None and status["status"] == "DELAYED"

    # Cancelling a delayed debounced workflow clears its debounce key.
    DBOS.cancel_workflow(handle.workflow_id)

    # A new debounce with the same key therefore starts a fresh workflow.
    handle2 = debouncer.debounce("key", 1, 2)
    assert handle2.workflow_id != handle.workflow_id
    assert handle2.get_result() == 2


def test_debounce_deadline_caps_initial_delay(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    # A huge period with a finite timeout: the delay is capped at the deadline.
    debouncer = Debouncer.create(workflow, debounce_timeout_sec=1000)
    handle = debouncer.debounce("key", 1000000, 1)

    status = dbos._sys_db.get_workflow_status(handle.workflow_id)
    assert status is not None
    assert status["status"] == "DELAYED"
    assert status["is_debounced"] is True
    assert status["debounce_deadline_epoch_ms"] is not None
    assert status["delay_until_epoch_ms"] is not None
    assert status["delay_until_epoch_ms"] <= status["debounce_deadline_epoch_ms"]

    DBOS.cancel_workflow(handle.workflow_id)


def test_debounce_user_dedup_conflict_raises(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        DBOS.sleep(1000000)
        return x

    # A non-debounced workflow occupies the debounce key on the internal queue.
    dedup_id = f"{get_dbos_func_name(workflow)}-key"
    internal_queue = dbos._registry.get_internal_queue()
    with SetEnqueueOptions(deduplication_id=dedup_id):
        blocker = internal_queue.enqueue(workflow, 1)

    # The key is held by a non-debounced workflow, so debouncing it surfaces the dedup conflict rather than hijacking it.
    debouncer = Debouncer.create(workflow)
    with pytest.raises(DBOSQueueDeduplicatedError):
        debouncer.debounce("key", 1, 2)

    DBOS.cancel_workflow(blocker.workflow_id)


def test_debounce_fixed_workflow_id_reuse(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    debouncer = Debouncer.create(workflow)

    # Pinning the same workflow ID across debounces of one key must still bounce the existing workflow (last input wins), not drop it on a workflow_uuid collision.
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        first_handle = debouncer.debounce("key", 2, 1)
    with SetWorkflowID(wfid):
        second_handle = debouncer.debounce("key", 2, 2)
    assert first_handle.workflow_id == wfid
    assert second_handle.workflow_id == wfid
    assert first_handle.get_result() == 2
    assert second_handle.get_result() == 2

    # A huge initial period then a short one under the same pinned ID must still shorten the delay so the workflow runs.
    wfid2 = str(uuid.uuid4())
    with SetWorkflowID(wfid2):
        third_handle = debouncer.debounce("key2", 1000000, 3)
    with SetWorkflowID(wfid2):
        fourth_handle = debouncer.debounce("key2", 1, 4)
    assert third_handle.workflow_id == wfid2
    assert fourth_handle.workflow_id == wfid2
    assert fourth_handle.get_result() == 4


def test_debounce_inworkflow_does_not_inherit_parent_deadline(dbos: DBOS) -> None:

    ran = {"child": False}

    @DBOS.workflow()
    def child(x: int) -> int:
        ran["child"] = True
        return x

    debouncer = Debouncer.create(child)

    # Debounce inside a workflow whose timeout (1s) is shorter than the debounce delay (3s); the debounced workflow must not inherit the parent's deadline or it would be cancelled before running.
    @DBOS.workflow()
    def parent() -> str:
        handle = debouncer.debounce("deadline-key", 3.0, 5)
        return handle.workflow_id

    with SetWorkflowTimeout(1.0):
        parent_handle = DBOS.start_workflow(parent)
    child_wfid = parent_handle.get_result()

    # The debounced workflow carries no inherited deadline while delayed.
    status = dbos._sys_db.get_workflow_status(child_wfid)
    assert status is not None
    assert status["status"] == "DELAYED"
    assert status["workflow_deadline_epoch_ms"] is None

    # It runs to completion after the delay rather than being cancelled.
    child_handle: WorkflowHandle[int] = DBOS.retrieve_workflow(child_wfid)
    assert child_handle.get_result() == 5
    assert ran["child"] is True
    assert child_handle.get_status().status == "SUCCESS"


def test_debounce_inworkflow_replay_is_deterministic(dbos: DBOS) -> None:

    child_runs = {"count": 0}

    @DBOS.workflow()
    def child(x: int) -> int:
        child_runs["count"] += 1
        return x

    debouncer = Debouncer.create(child)

    parent_body_runs = {"count": 0}

    @DBOS.workflow()
    def parent() -> str:
        parent_body_runs["count"] += 1
        handle = debouncer.debounce("replay-key", 1.0, 7)
        return handle.workflow_id

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        parent_handle = DBOS.start_workflow(parent)
    child_wfid = parent_handle.get_result()
    assert parent_body_runs["count"] == 1

    # Force the parent to replay from its checkpoint: the bounce step and enqueue are checkpointed, so replay must re-run the body, return the same child id, and not enqueue a second child.
    dbos._sys_db.update_workflow_outcome(wfid, "PENDING")
    recovery_handles = DBOS._recover_pending_workflows()
    replayed = [h for h in recovery_handles if h.workflow_id == wfid]
    assert len(replayed) == 1
    assert replayed[0].get_result() == child_wfid
    assert parent_body_runs["count"] == 2

    # Exactly one child workflow exists for this key: replay did not double-enqueue.
    child_name = get_dbos_func_name(child)
    assert len(DBOS.list_workflows(name=child_name)) == 1

    # The single debounced child runs to completion exactly once.
    child_handle: WorkflowHandle[int] = DBOS.retrieve_workflow(child_wfid)
    assert child_handle.get_result() == 7
    assert child_runs["count"] == 1


def test_debounce_retries_replayed_portable_dedup_error(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Regression test: when an in-workflow debounce's fresh enqueue loses the dedup race, the
    # DBOSQueueDeduplicatedError is checkpointed at the parent's function ID. On replay it is
    # re-raised from that checkpoint, but a workflow using portable (cross-language JSON)
    # serialization deserializes it to a PortableWorkflowError carrying only the original type
    # name -- not the original type. The retry loop must still recognize that form and bounce the
    # existing workflow; otherwise the replayed workflow errors instead of retrying.
    from dbos._serialization import PortableWorkflowError

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    debouncer = Debouncer.create(workflow)

    original_enqueue = Queue.enqueue
    enqueue_calls = {"count": 0}

    def enqueue_raising_portable_dedup_once(
        self: Queue, func: Any, *args: Any, **kwargs: Any
    ) -> Any:
        # The first enqueue raises the exact object a replay produces from a checkpointed,
        # portable-serialized dedup error; subsequent enqueues behave normally.
        enqueue_calls["count"] += 1
        if enqueue_calls["count"] == 1:
            raise PortableWorkflowError(
                "Workflow was deduplicated",
                DBOSQueueDeduplicatedError.__name__,
                None,
                None,
            )
        return original_enqueue(self, func, *args, **kwargs)

    monkeypatch.setattr(Queue, "enqueue", enqueue_raising_portable_dedup_once)

    handle = debouncer.debounce("portable-key", 0.5, 5)

    # The loop recognized the replay-form dedup error and retried instead of propagating it.
    assert enqueue_calls["count"] == 2
    assert handle.get_result() == 5


def test_debounce_rejects_caller_dedup_and_delay(
    dbos: DBOS, client: DBOSClient
) -> None:
    # A debounce owns the workflow's deduplication ID and delay, and priority/partition keys cannot apply to a debounced enqueue, so a caller that sets any of them must fail loudly rather than have it silently ignored or break later.

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    debouncer = Debouncer.create(workflow)

    # Local: a caller-set deduplication_id is rejected.
    with pytest.raises(DBOSException, match="deduplication_id"):
        with SetEnqueueOptions(deduplication_id="caller-dedup"):
            debouncer.debounce("k", 1.0, 1)

    # Local: a caller-set delay is rejected.
    with pytest.raises(DBOSException, match="delay"):
        with SetEnqueueOptions(delay_seconds=100.0):
            debouncer.debounce("k", 1.0, 1)

    # Local: a caller-set priority is rejected.
    with pytest.raises(DBOSException, match="priority"):
        with SetEnqueueOptions(priority=1):
            debouncer.debounce("k", 1.0, 1)

    # Local: a caller-set partition key is rejected.
    with pytest.raises(DBOSException, match="partition key"):
        with SetEnqueueOptions(queue_partition_key="caller-partition"):
            debouncer.debounce("k", 1.0, 1)

    # No conflicting option left a workflow behind.
    assert len(DBOS.list_workflows(name=get_dbos_func_name(workflow))) == 0

    DBOS.register_queue("reject-queue")

    # Client: a deduplication_id in the workflow options is rejected.
    dedup_client = DebouncerClient(
        client,
        {
            "workflow_name": workflow.__qualname__,
            "queue_name": "reject-queue",
            "deduplication_id": "caller-dedup",
        },
    )
    with pytest.raises(DBOSException, match="deduplication_id"):
        dedup_client.debounce("k", 1.0, 1)

    # Client: a delay in the workflow options is rejected.
    delay_client = DebouncerClient(
        client,
        {
            "workflow_name": workflow.__qualname__,
            "queue_name": "reject-queue",
            "delay_seconds": 100.0,
        },
    )
    with pytest.raises(DBOSException, match="delay"):
        delay_client.debounce("k", 1.0, 1)

    # Client: a priority in the workflow options is rejected.
    priority_client = DebouncerClient(
        client,
        {
            "workflow_name": workflow.__qualname__,
            "queue_name": "reject-queue",
            "priority": 1,
        },
    )
    with pytest.raises(DBOSException, match="priority"):
        priority_client.debounce("k", 1.0, 1)

    # Client: a partition key in the workflow options is rejected.
    partition_client = DebouncerClient(
        client,
        {
            "workflow_name": workflow.__qualname__,
            "queue_name": "reject-queue",
            "queue_partition_key": "caller-partition",
        },
    )
    with pytest.raises(DBOSException, match="partition key"):
        partition_client.debounce("k", 1.0, 1)


def test_debounce_bounce_path_does_not_leak_pinned_id(dbos: DBOS) -> None:
    # Regression test: a SetWorkflowID pinned around a debounce that coalesces (bounce path) is captured by the debounce and must not stay armed on the workflow's context, or the next child workflow the parent starts would silently run under the pinned ID.

    @DBOS.workflow()
    def child(x: int) -> int:
        return x

    @DBOS.workflow()
    def other(x: int) -> int:
        return x

    debouncer = Debouncer.create(child)

    @DBOS.workflow()
    def parent(pinned_id: str) -> tuple[str, str]:
        # The first call creates the delayed workflow; the pinned second call bounces it, so the pinned ID goes unused.
        first = debouncer.debounce("leak-key", 1000000, 1)
        with SetWorkflowID(pinned_id):
            bounced = debouncer.debounce("leak-key", 1000000, 2)
        assert bounced.workflow_id == first.workflow_id
        next_handle = DBOS.start_workflow(other, 3)
        return first.workflow_id, next_handle.workflow_id

    pinned_id = str(uuid.uuid4())
    delayed_wfid, next_wfid = DBOS.start_workflow(parent, pinned_id).get_result()

    # The next child workflow did not inherit the unused pinned ID.
    assert next_wfid != pinned_id
    assert DBOS.retrieve_workflow(next_wfid).get_result() == 3

    DBOS.cancel_workflow(delayed_wfid)


def test_debounce_pinned_id_survives_enqueue_dedup_race(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Regression test: a fresh-enqueue attempt consumes a SetWorkflowID-pinned ID from the context before its INSERT can lose the dedup race. The retry loop must re-apply the pinned ID so the workflow is still created under it, not a generated UUID.

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    debouncer = Debouncer.create(workflow)

    original_init = dbos._sys_db.init_workflow
    init_calls = {"count": 0}

    def init_raising_dedup_once(*args: Any, **kwargs: Any) -> Any:
        # The first enqueue's insert loses the dedup race after the pinned ID was already consumed; later calls behave normally.
        init_calls["count"] += 1
        if init_calls["count"] == 1:
            raise DBOSQueueDeduplicatedError("racer", "queue", "dedup")
        return original_init(*args, **kwargs)

    monkeypatch.setattr(dbos._sys_db, "init_workflow", init_raising_dedup_once)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = debouncer.debounce("pin-race-key", 0.5, 7)

    # The retried enqueue reused the pinned ID rather than minting a random one.
    assert init_calls["count"] >= 2
    assert handle.workflow_id == wfid
    assert handle.get_result() == 7
