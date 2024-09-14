import uuid

from dbos import DBOS, Queue, SetWorkflowID


def test_simple_workflow(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        return var1 + var2

    assert test_workflow("abc", "123") == "abc123"
    assert wf_counter == 1

    queue = Queue("test_queue")

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        queue.enqueue(test_workflow, "abc", "123")

    handle = DBOS.execute_workflow_id(wfid)
    assert handle.get_result() == "abc123"
