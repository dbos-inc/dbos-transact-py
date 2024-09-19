import threading
import time
import uuid

from dbos import DBOS, Queue, SetWorkflowID


def test_simple_queue(dbos: DBOS) -> None:
    wf_counter: int = 0
    step_counter: int = 0

    wfid = str(uuid.uuid4())

    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> str:
        assert DBOS.workflow_id == wfid
        nonlocal wf_counter
        wf_counter += 1
        var1 = test_step(var1)
        return var1 + var2

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return var + "d"

    queue = Queue("test_queue")

    with SetWorkflowID(wfid):
        handle = queue.enqueue(test_workflow, "abc", "123")
    assert handle.get_result() == "abcd123"
    with SetWorkflowID(wfid):
        assert test_workflow("abc", "123") == "abcd123"
    assert wf_counter == 2
    assert step_counter == 1


def test_one_at_a_time(dbos: DBOS) -> None:
    wf_counter = 0
    flag = False
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.workflow()
    def workflow_one() -> None:
        nonlocal wf_counter
        wf_counter += 1
        main_thread_event.set()
        workflow_event.wait()

    @DBOS.workflow()
    def workflow_two() -> None:
        nonlocal flag
        flag = True

    queue = Queue("test_queue", 1)
    handle1 = queue.enqueue(workflow_one)
    handle2 = queue.enqueue(workflow_two)

    main_thread_event.wait()
    time.sleep(2)  # Verify the other task isn't scheduled on subsequent poller ticks.
    assert not flag
    workflow_event.set()
    assert handle1.get_result() == None
    assert handle2.get_result() == None
    assert flag
    assert wf_counter == 1


def test_queue_step(dbos: DBOS) -> None:
    step_counter: int = 0
    wfid = str(uuid.uuid4())

    @DBOS.step()
    def test_step(var: str) -> str:
        assert DBOS.workflow_id == wfid
        nonlocal step_counter
        step_counter += 1
        return var + "1"

    queue = Queue("test_queue")

    with SetWorkflowID(wfid):
        handle = queue.enqueue(test_step, "abc")
    assert handle.get_result() == "abc1"
    with SetWorkflowID(wfid):
        assert test_step("abc") == "abc1"
    assert step_counter == 1


def test_queue_transaction(dbos: DBOS) -> None:
    step_counter: int = 0
    wfid = str(uuid.uuid4())

    @DBOS.transaction()
    def test_transaction(var: str) -> str:
        assert DBOS.workflow_id == wfid
        nonlocal step_counter
        step_counter += 1
        return var + "1"

    queue = Queue("test_queue")

    with SetWorkflowID(wfid):
        handle = queue.enqueue(test_transaction, "abc")
    assert handle.get_result() == "abc1"
    with SetWorkflowID(wfid):
        assert test_transaction("abc") == "abc1"
    assert step_counter == 1
