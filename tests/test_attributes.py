import threading
import uuid
from typing import Any

import pytest

from dbos import (
    DBOS,
    DBOSClient,
    Debouncer,
    EnqueueOptions,
    Queue,
    SetWorkflowAttributes,
    SetWorkflowID,
    WorkflowHandle,
)
from dbos._error import DBOSException
from tests.conftest import using_sqlite


def test_attributes_direct_invocation(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child_workflow() -> str:
        assert DBOS.workflow_id is not None
        return DBOS.workflow_id

    @DBOS.workflow()
    def parent_workflow() -> str:
        return child_workflow()

    wfid = str(uuid.uuid4())
    attributes = {"customer": "acme", "tier": 3}
    with SetWorkflowAttributes(attributes):
        with SetWorkflowID(wfid):
            child_id = parent_workflow()

    status = DBOS.list_workflows(workflow_ids=[wfid])[0]
    assert status.attributes == attributes

    # Child workflows do not inherit their parent's attributes
    child_status = DBOS.list_workflows(workflow_ids=[child_id])[0]
    assert child_status.attributes is None

    # Workflows started outside the block have no attributes
    wfid_no_attrs = str(uuid.uuid4())
    with SetWorkflowID(wfid_no_attrs):
        parent_workflow()
    assert DBOS.list_workflows(workflow_ids=[wfid_no_attrs])[0].attributes is None


def test_attributes_start_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def noop_workflow() -> None:
        return None

    # Nested blocks override and restore attributes
    with SetWorkflowAttributes({"region": "us-east-1"}):
        with SetWorkflowAttributes({"region": "eu-west-1"}):
            inner_handle = DBOS.start_workflow(noop_workflow)
        outer_handle = DBOS.start_workflow(noop_workflow)

    inner_handle.get_result()
    outer_handle.get_result()
    assert inner_handle.get_status().attributes == {"region": "eu-west-1"}
    assert outer_handle.get_status().attributes == {"region": "us-east-1"}


def test_attributes_enqueue(dbos: DBOS) -> None:
    queue = Queue("test_attributes_queue")

    @DBOS.workflow()
    def queued_workflow(x: int) -> int:
        return x

    with SetWorkflowAttributes({"source": "queue"}):
        handle = queue.enqueue(queued_workflow, 5)
    assert handle.get_result() == 5
    assert handle.get_status().attributes == {"source": "queue"}


def test_attributes_fork(dbos: DBOS) -> None:
    @DBOS.workflow()
    def forkable_workflow() -> None:
        return None

    wfid = str(uuid.uuid4())
    attributes = {"customer": "acme"}
    with SetWorkflowAttributes(attributes):
        with SetWorkflowID(wfid):
            forkable_workflow()

    forked_handle = DBOS.fork_workflow(wfid, 1)
    forked_handle.get_result()
    assert forked_handle.get_status().attributes == attributes


def test_attributes_client(client: DBOSClient, dbos: DBOS) -> None:
    # Enqueue to a queue nothing consumes; the workflow stays ENQUEUED, which
    # is enough to check the attributes recorded at creation.
    options: EnqueueOptions = {
        "queue_name": "unconsumed_queue",
        "workflow_name": "client_workflow",
        "attributes": {"source": "client"},
    }
    handle: WorkflowHandle[Any] = client.enqueue(options, 1)
    assert handle.get_status().attributes == {"source": "client"}


def test_attributes_list_filter(dbos: DBOS, skip_with_sqlite: None) -> None:
    @DBOS.workflow()
    def attr_workflow() -> None:
        return None

    with SetWorkflowAttributes(
        {"customer": "acme", "tier": 1, "beta": True, "note": None}
    ):
        h1 = DBOS.start_workflow(attr_workflow)
    with SetWorkflowAttributes(
        {"customer": "bigco", "tier": 2, "meta": {"region": "us-east-1"}}
    ):
        h2 = DBOS.start_workflow(attr_workflow)
    h1.get_result()
    h2.get_result()

    def matched_ids(**kwargs: Any) -> set[str]:
        return {s.workflow_id for s in DBOS.list_workflows(**kwargs)}

    # Single key
    assert matched_ids(attributes={"customer": "acme"}) == {h1.workflow_id}
    # Multiple keys AND together
    assert matched_ids(attributes={"customer": "bigco", "tier": 2}) == {h2.workflow_id}
    # Value mismatch on one key matches nothing
    assert matched_ids(attributes={"customer": "acme", "tier": 2}) == set()
    # Non-string value types
    assert matched_ids(attributes={"tier": 1}) == {h1.workflow_id}
    assert matched_ids(attributes={"beta": True}) == {h1.workflow_id}
    assert matched_ids(attributes={"note": None}) == {h1.workflow_id}
    assert matched_ids(attributes={"meta": {"region": "us-east-1"}}) == {h2.workflow_id}
    # Composes with other filters
    assert matched_ids(attributes={"tier": 1}, workflow_ids=[h2.workflow_id]) == set()
    # Workflows without attributes never match
    assert matched_ids(attributes={"missing": "key"}) == set()


def test_attributes_list_queued(dbos: DBOS, skip_with_sqlite: None) -> None:
    start_event = threading.Event()
    blocking_event = threading.Event()

    queue = Queue("attr_filter_queue")

    @DBOS.workflow()
    def blocking_workflow() -> None:
        start_event.set()
        blocking_event.wait()

    with SetWorkflowAttributes({"side": "queued"}):
        handle = queue.enqueue(blocking_workflow)
    start_event.wait()

    statuses = DBOS.list_queued_workflows(attributes={"side": "queued"})
    assert [s.workflow_id for s in statuses] == [handle.workflow_id]
    assert DBOS.list_queued_workflows(attributes={"side": "other"}) == []

    blocking_event.set()
    handle.get_result()


@pytest.mark.asyncio
async def test_attributes_list_async(dbos: DBOS, skip_with_sqlite: None) -> None:
    start_event = threading.Event()
    blocking_event = threading.Event()

    queue = Queue("attr_filter_queue_async")

    @DBOS.workflow()
    def blocking_workflow() -> None:
        start_event.set()
        blocking_event.wait()

    with SetWorkflowAttributes({"side": "async"}):
        handle = queue.enqueue(blocking_workflow)
    start_event.wait()

    statuses = await DBOS.list_workflows_async(attributes={"side": "async"})
    assert [s.workflow_id for s in statuses] == [handle.workflow_id]
    queued = await DBOS.list_queued_workflows_async(attributes={"side": "async"})
    assert [s.workflow_id for s in queued] == [handle.workflow_id]
    assert await DBOS.list_workflows_async(attributes={"side": "other"}) == []

    blocking_event.set()
    handle.get_result()


def test_attributes_client_list(
    client: DBOSClient, dbos: DBOS, skip_with_sqlite: None
) -> None:
    options: EnqueueOptions = {
        "queue_name": "unconsumed_queue",
        "workflow_name": "client_workflow",
        "attributes": {"source": "client", "n": 1},
    }
    h1: WorkflowHandle[Any] = client.enqueue(options, 1)
    options["attributes"] = {"source": "client", "n": 2}
    h2: WorkflowHandle[Any] = client.enqueue(options, 2)

    statuses = client.list_workflows(attributes={"source": "client"})
    assert {s.workflow_id for s in statuses} == {h1.workflow_id, h2.workflow_id}
    queued = client.list_queued_workflows(attributes={"n": 2})
    assert [s.workflow_id for s in queued] == [h2.workflow_id]


@pytest.mark.asyncio
async def test_attributes_client_list_async(
    client: DBOSClient, dbos: DBOS, skip_with_sqlite: None
) -> None:
    options: EnqueueOptions = {
        "queue_name": "unconsumed_queue",
        "workflow_name": "client_workflow",
        "attributes": {"source": "client_async", "n": 1},
    }
    h1: WorkflowHandle[Any] = client.enqueue(options, 1)
    options["attributes"] = {"source": "client_async", "n": 2}
    h2: WorkflowHandle[Any] = client.enqueue(options, 2)

    statuses = await client.list_workflows_async(attributes={"source": "client_async"})
    assert {s.workflow_id for s in statuses} == {h1.workflow_id, h2.workflow_id}
    queued = await client.list_queued_workflows_async(attributes={"n": 1})
    assert [s.workflow_id for s in queued] == [h1.workflow_id]


def test_attributes_filter_unsupported_sqlite(dbos: DBOS) -> None:
    if not using_sqlite():
        pytest.skip("Tests the SQLite-only error path")
    with pytest.raises(DBOSException, match="not supported on SQLite"):
        DBOS.list_workflows(attributes={"customer": "acme"})


def test_attributes_debouncer(dbos: DBOS) -> None:
    @DBOS.workflow()
    def debounced_workflow(x: int) -> int:
        return x

    debouncer = Debouncer.create(debounced_workflow)
    with SetWorkflowAttributes({"source": "debouncer"}):
        handle = debouncer.debounce("key", 1.0, 5)
    assert handle.get_result() == 5
    assert handle.get_status().attributes == {"source": "debouncer"}

    # The internal debouncer workflow itself does not get the user's attributes
    internal_statuses = DBOS.list_workflows(name="_dbos_debouncer_workflow")
    for status in internal_statuses:
        assert status.attributes is None
