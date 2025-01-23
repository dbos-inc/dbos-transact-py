import logging
import os
import subprocess
import threading
import time
import uuid
from multiprocessing import Process

import pytest
import sqlalchemy as sa

from dbos import (
    DBOS,
    ConfigFile,
    DBOSConfiguredInstance,
    Queue,
    SetWorkflowID,
    WorkflowHandle,
)
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import WorkflowStatusString
from tests.conftest import default_config


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


def test_queue_workflow_in_recovered_workflow(dbos: DBOS) -> None:
    # We don't want to be taking queued jobs while subprocess runs
    DBOS.destroy()

    # Set up environment variables to trigger the crash in subprocess
    env = os.environ.copy()
    env["DIE_ON_PURPOSE"] = "true"

    # Run the script as a subprocess to get a workflow stuck
    process = subprocess.run(
        ["python", "tests/queuedworkflow.py"],
        cwd=os.getcwd(),
        env=env,
        capture_output=True,
        text=True,
    )
    # print ("Process Return: ")
    # print (process.stdout)
    # print (process.stderr)
    assert process.returncode != 0  # Crashed

    # Run script again without crash
    process = subprocess.run(
        ["python", "tests/queuedworkflow.py"],
        cwd=os.getcwd(),
        env=os.environ,
        capture_output=True,
        text=True,
    )
    # print ("Process Return: ")
    # print (process.stdout)
    # print (process.stderr)
    assert process.returncode == 0  # Ran to completion

    # Launch DBOS to check answer
    dbos = DBOS(config=default_config())
    DBOS.launch()
    wfh: WorkflowHandle[int] = DBOS.retrieve_workflow("testqueuedwfcrash")
    assert wfh.get_result() == 5
    assert wfh.get_status().status == "SUCCESS"
    assert queue_entries_are_cleaned_up(dbos)
    return


###########################
# TEST WORKER CONCURRENCY #
###########################


def test_one_at_a_time_with_worker_concurrency(dbos: DBOS) -> None:
    wf_counter = 0
    flag = False
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.workflow()
    def workflow_one() -> None:
        nonlocal wf_counter
        wf_counter += 1
        main_thread_event.set()  # Signal main thread we got running
        workflow_event.wait()  # Wait to complete

    @DBOS.workflow()
    def workflow_two() -> None:
        nonlocal flag
        flag = True

    queue = Queue("test_queue", worker_concurrency=1)
    handle1 = queue.enqueue(workflow_one)
    handle2 = queue.enqueue(workflow_two)

    # Wait until the first task is dequeued
    main_thread_event.wait()
    # Let pass a few dequeuing intervals
    time.sleep(2)
    # 2nd task should not have been dequeued
    assert not flag
    # Unlock the first task
    workflow_event.set()
    # Both tasks should have completed
    assert handle1.get_result() == None
    assert handle2.get_result() == None
    assert flag
    assert wf_counter == 1, f"wf_counter={wf_counter}"
    assert queue_entries_are_cleaned_up(dbos)


# Declare a workflow globally (we need it to be registered across process under a known name)
@DBOS.workflow()
def worker_concurrency_test_workflow() -> None:
    pass


def run_dbos_test_in_process(i: int) -> None:
    dbos_config: ConfigFile = {
        "name": "test-app",
        "language": "python",
        "database": {
            "hostname": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": os.environ["PGPASSWORD"],
            "app_db_name": "dbostestpy",
        },
        "runtimeConfig": {
            "start": ["python3 main.py"],
            "admin_port": 8001 + i,
        },
        "telemetry": {},
        "env": {},
    }
    dbos = DBOS(config=dbos_config)
    DBOS.launch()

    Queue("test_queue", worker_concurrency=1)
    time.sleep(
        2
    )  # Give some time for the parent worker to enqueue and for this worker to dequeue

    queue_entries_are_cleaned_up(dbos)

    DBOS.destroy()


def test_worker_concurrency_with_n_dbos_instances(dbos: DBOS) -> None:

    # Start N proccesses to dequeue
    processes = []
    for i in range(0, 10):
        os.environ["DBOS__VMID"] = f"test-executor-{i}"
        process = Process(target=run_dbos_test_in_process, args=(i,))
        process.start()
        processes.append(process)

    # Enqueue N tasks but ensure this worker cannot dequeue

    queue = Queue("test_queue", limiter={"limit": 0, "period": 1})
    for i in range(0, 10):
        queue.enqueue(worker_concurrency_test_workflow)

    for process in processes:
        process.join()


# Test error cases where we have duplicated workflows starting with the same workflow ID.
def test_duplicate_workflow_id(dbos: DBOS, caplog: pytest.LogCaptureFixture) -> None:
    wfid = str(uuid.uuid4())

    @DBOS.workflow()
    def test_workflow(var1: str) -> str:
        DBOS.sleep(1)
        return var1

    @DBOS.workflow()
    def test_dup_workflow() -> None:
        DBOS.sleep(0.1)
        return

    @DBOS.dbos_class()
    class TestDup:
        @classmethod
        @DBOS.workflow()
        def test_workflow(cls, var1: str) -> str:
            DBOS.sleep(0.1)
            return var1

    @DBOS.dbos_class()
    class TestDupInst(DBOSConfiguredInstance):
        def __init__(self, config_name: str):
            self.config_name = config_name
            super().__init__(config_name)

        @DBOS.workflow()
        def test_workflow(self, var1: str) -> str:
            DBOS.sleep(0.1)
            return self.config_name + ":" + var1

    original_propagate = logging.getLogger("dbos").propagate
    caplog.set_level(logging.WARNING, "dbos")
    logging.getLogger("dbos").propagate = True

    with SetWorkflowID(wfid):
        origHandle = DBOS.start_workflow(test_workflow, "abc")
        # The second one will generate a warning message but no error.
        test_dup_workflow()

    assert "Multiple workflows started in the same SetWorkflowID block." in caplog.text

    # It's okay to call the same workflow with the same ID again.
    with SetWorkflowID(wfid):
        same_handle = DBOS.start_workflow(test_workflow, "abc")

    # Call with a different function name is not allowed.
    with SetWorkflowID(wfid):
        with pytest.raises(Exception) as exc_info:
            DBOS.start_workflow(test_dup_workflow)
        assert "Workflow already exists with a different function name" in str(
            exc_info.value
        )

    # Call the same function name in a different class is not allowed.
    with SetWorkflowID(wfid):
        with pytest.raises(Exception) as exc_info:
            DBOS.start_workflow(TestDup.test_workflow, "abc")
        assert "Workflow already exists with a different function name" in str(
            exc_info.value
        )
    # Normal invocation is fine.
    res = TestDup.test_workflow("abc")
    assert res == "abc"

    # Call the same function name from a different instance is not allowed.
    wfid2 = str(uuid.uuid4())
    inst = TestDupInst("myconfig")
    with SetWorkflowID(wfid2):
        # Normal invocation is fine.
        res = inst.test_workflow("abc")
        assert res == "myconfig:abc"

    inst2 = TestDupInst("myconfig2")
    with SetWorkflowID(wfid2):
        with pytest.raises(Exception) as exc_info:
            inst2.test_workflow("abc")
        assert "Workflow already exists with a different config name" in str(
            exc_info.value
        )

    # Call the same function in a different queue would generate a warning, but is allowed.
    queue = Queue("test_queue")
    with SetWorkflowID(wfid):
        handle = queue.enqueue(test_workflow, "abc")
    assert handle.get_result() == "abc"
    assert "Workflow already exists in queue" in caplog.text

    # Call with a different input would generate a warning, but still use the recorded input.
    with SetWorkflowID(wfid):
        res = test_workflow("def")
        # We want to see the warning message, but the result is non-deterministic
        # TODO: in the future, we may want to always use the recorded inputs.
        assert res == "abc" or res == "def"
    assert f"Workflow inputs for {wfid} changed since the first call" in caplog.text

    assert origHandle.get_result() == "abc"
    assert same_handle.get_result() == "abc"

    # Reset logging
    logging.getLogger("dbos").propagate = original_propagate
