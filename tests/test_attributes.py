import uuid
from typing import Any

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
