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
            query = sa.select(sa.func.count()).select_from(SystemSchema.workflow_queue)
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
    assert handle1.get_status().queue_name == "test_queue"
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
    assert queue_entries_are_cleaned_up(dbos)


def test_queue_childwf(dbos: DBOS) -> None:
    queue = Queue("child_queue", 3)

    @DBOS.workflow()
    def test_child_wf(val: str) -> str:
        DBOS.recv("release", 30)
        return val + "d"

    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> str:
        wfh1 = queue.enqueue(test_child_wf, var1)
        wfh2 = queue.enqueue(test_child_wf, var2)
        wfh3 = queue.enqueue(test_child_wf, var1)
        wfh4 = queue.enqueue(test_child_wf, var2)

        DBOS.sleep(1)
        assert wfh4.get_status().status == "ENQUEUED"

        DBOS.send(wfh1.get_workflow_id(), "go", "release")
        DBOS.send(wfh2.get_workflow_id(), "go", "release")
        DBOS.send(wfh3.get_workflow_id(), "go", "release")
        DBOS.send(wfh4.get_workflow_id(), "go", "release")

        return (
            wfh1.get_result()
            + wfh2.get_result()
            + wfh3.get_result()
            + wfh4.get_result()
        )

    assert test_workflow("a", "b") == "adbdadbd"


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

    # Launch a number of tasks equal to three times the limit.
    # This should lead to three "waves" of the limit tasks being
    # executed simultaneously, followed by a wait of the period,
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
            assert times[i + 1] - times[i] < 0.2

    # Verify that the gap between "waves" is ~equal to the period
    for wave in range(num_waves - 1):
        assert times[limit * (wave + 1)] - times[limit * wave] > period - 0.2
        assert times[limit * (wave + 1)] - times[limit * wave] < period + 0.2

    # Verify all workflows get the SUCCESS status eventually
    dbos._sys_db.wait_for_buffer_flush()
    for h in handles:
        assert h.get_status().status == WorkflowStatusString.SUCCESS.value

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_multiple_queues(dbos: DBOS) -> None:

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

    concurrency_queue = Queue("test_concurrency_queue", 1)
    handle1 = concurrency_queue.enqueue(workflow_one)
    assert handle1.get_status().queue_name == "test_concurrency_queue"
    handle2 = concurrency_queue.enqueue(workflow_two)

    @DBOS.workflow()
    def limited_workflow(var1: str, var2: str) -> float:
        assert var1 == "abc" and var2 == "123"
        return time.time()

    limit = 5
    period = 2
    limiter_queue = Queue(
        "test_limit_queue", limiter={"limit": limit, "period": period}
    )

    handles: list[WorkflowHandle[float]] = []
    times: list[float] = []

    # Launch a number of tasks equal to three times the limit.
    # This should lead to three "waves" of the limit tasks being
    # executed simultaneously, followed by a wait of the period,
    # followed by the next wave.
    num_waves = 3
    for _ in range(limit * num_waves):
        h = limiter_queue.enqueue(limited_workflow, "abc", "123")
        handles.append(h)
    for h in handles:
        times.append(h.get_result())

    # Verify that each "wave" of tasks started at the ~same time.
    for wave in range(num_waves):
        for i in range(wave * limit, (wave + 1) * limit - 1):
            assert times[i + 1] - times[i] < 0.2

    # Verify that the gap between "waves" is ~equal to the period
    for wave in range(num_waves - 1):
        assert times[limit * (wave + 1)] - times[limit * wave] > period - 0.2
        assert times[limit * (wave + 1)] - times[limit * wave] < period + 0.2

    # Verify all workflows get the SUCCESS status eventually
    dbos._sys_db.wait_for_buffer_flush()
    for h in handles:
        assert h.get_status().status == WorkflowStatusString.SUCCESS.value

    # Verify that during all this time, the second task
    # was not launched on the concurrency-limited queue.
    # Then, finish the first task and verify the second
    # task runs on schedule.
    assert not flag
    workflow_event.set()
    assert handle1.get_result() == None
    assert handle2.get_result() == None
    assert flag
    assert wf_counter == 1

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)
