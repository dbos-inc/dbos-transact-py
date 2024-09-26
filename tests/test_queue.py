import threading
import time
import uuid

import sqlalchemy as sa

from dbos import DBOS, Queue, SetWorkflowID
from dbos.dbos import WorkflowHandle
from dbos.schemas.system_database import SystemSchema
from dbos.system_database import WorkflowStatusString


def queue_entries_are_cleaned_up(dbos: DBOS) -> bool:
    max_tries = 10
    success = False
    for i in range(max_tries):
        with dbos._sys_db.engine.begin() as c:
            query = sa.select(sa.func.count()).select_from(SystemSchema.job_queue)
            row = c.execute(query).fetchone()
            assert row is not None
            count = row[0]
            if count == 0:
                success = True
                break
        time.sleep(1)
    return success


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
    assert queue_entries_are_cleaned_up(dbos)


def test_one_at_a_time_with_limiter(dbos: DBOS) -> None:
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

    queue = Queue("test_queue", concurrency=1, limiter={"limit": 10, "period": 1})
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


def test_limiter(dbos: DBOS) -> None:

    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> float:
        assert var1 == "abc" and var2 == "123"
        return time.time()

    limit = 5
    period = 2
    queue = Queue("test_queue", limiter={"limit": limit, "period": period})

    handles: list[WorkflowHandle[float]] = []
    times: list[float] = []

    # Launch a number of tasks equal to three times the max.
    # This should lead to three "waves" of the max tasks being
    # executed simultaneously, followed by a wait of the duration,
    # followed by the next wave.
    num_waves = 3
    for _ in range(limit * num_waves):
        h = queue.enqueue(test_workflow, "abc", "123")
        handles.append(h)
    for h in handles:
        times.append(h.get_result())

    # Verify that each "wave" of tasks started at the ~same time.
    for wave in range(num_waves):
        for i in range(wave * limit, (wave + 1) * limit - 1):
            assert times[i + 1] - times[i] < 0.1

    # Verify that the gap between "waves" is ~equal to the duration
    for wave in range(num_waves - 1):
        assert times[limit * wave] - times[limit * wave - 1] < period + 0.1

    # Verify all workflows get the SUCCESS status eventually
    dbos._sys_db.wait_for_buffer_flush()
    for h in handles:
        assert h.get_status().status == WorkflowStatusString.SUCCESS.value

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)
