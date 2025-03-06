import logging
import multiprocessing
import multiprocessing.synchronize
import os
import subprocess
import threading
import time
import uuid

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
from dbos._sys_db import WorkflowStatusString, _buffer_flush_interval_secs
from tests.conftest import default_config, queue_entries_are_cleaned_up


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
start_event = threading.Event()
end_event = threading.Event()


@DBOS.workflow()
def worker_concurrency_test_workflow() -> None:
    start_event.set()
    end_event.wait()


local_concurrency_limit: int = 5
global_concurrency_limit: int = local_concurrency_limit * 2


def run_dbos_test_in_process(
    i: int,
    start_signal: multiprocessing.synchronize.Event,
    end_signal: multiprocessing.synchronize.Event,
) -> None:
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
        "application": {},
    }
    dbos = DBOS(config=dbos_config)
    DBOS.launch()

    Queue(
        "test_queue",
        worker_concurrency=local_concurrency_limit,
        concurrency=global_concurrency_limit,
    )
    # Wait to dequeue as many tasks as we can locally
    for _ in range(0, local_concurrency_limit):
        start_event.wait()
        start_event.clear()
    # Signal the parent process we've dequeued
    start_signal.set()
    # Wait for the parent process to signal we can move on
    end_signal.wait()
    # Complete the task. 1 set should unblock them all
    end_event.set()

    # Now whatever is in the queue should be cleared up fast (start/end events are already set)
    queue_entries_are_cleaned_up(dbos)


# Test global concurrency and worker utilization by carefully filling the queue up to 1) the local limit 2) the global limit
# For the global limit, we fill the queue in 2 steps, ensuring that the 2nd worker is able to cap its local utilization even
# after having dequeued some tasks already
def test_worker_concurrency_with_n_dbos_instances(dbos: DBOS) -> None:
    # Ensure children processes do not share global variables (including DBOS instance) with the parent
    multiprocessing.set_start_method("spawn")

    queue = Queue(
        "test_queue", limiter={"limit": 0, "period": 1}
    )  # This process cannot dequeue tasks

    # First, start local concurrency limit tasks
    handles = []
    for _ in range(0, local_concurrency_limit):
        handles.append(queue.enqueue(worker_concurrency_test_workflow))

    # Start 2 workers
    processes = []
    start_signals = []
    end_signals = []
    manager = multiprocessing.Manager()
    for i in range(0, 2):
        os.environ["DBOS__VMID"] = f"test-executor-{i}"
        start_signal = manager.Event()
        start_signals.append(start_signal)
        end_signal = manager.Event()
        end_signals.append(end_signal)
        process = multiprocessing.Process(
            target=run_dbos_test_in_process, args=(i, start_signal, end_signal)
        )
        process.start()
        processes.append(process)
    del os.environ["DBOS__VMID"]

    # Check that a single worker was able to acquire all the tasks
    loop = True
    while loop:
        for signal in start_signals:
            signal.wait(timeout=1)
            if signal.is_set():
                loop = False
    executors = []
    for handle in handles:
        status = handle.get_status()
        assert status.status == WorkflowStatusString.PENDING.value
        executors.append(status.executor_id)
    assert len(set(executors)) == 1

    # Now enqueue less than the local concurrency limit. Check that the 2nd worker acquired them. We won't have a signal set from the worker so we need to sleep a little.
    handles = []
    for _ in range(0, local_concurrency_limit - 1):
        handles.append(queue.enqueue(worker_concurrency_test_workflow))
    time.sleep(2)
    executors = []
    for handle in handles:
        status = handle.get_status()
        assert status.status == WorkflowStatusString.PENDING.value
        executors.append(status.executor_id)
    assert len(set(executors)) == 1

    # Now, enqueue two more tasks. This means qlen > local concurrency limit * 2 and qlen > global concurrency limit
    # We should have 1 tasks PENDING and 1 ENQUEUED, thus meeting both local and global concurrency limits
    handles = []
    for _ in range(0, 2):
        handles.append(queue.enqueue(worker_concurrency_test_workflow))
    # we can check the signal because the 2nd executor will set it
    num_dequeued = 0
    while num_dequeued < 2:
        for signal in start_signals:
            signal.wait(timeout=1)
            if signal.is_set():
                num_dequeued += 1
    executors = []
    statuses = []
    for handle in handles:
        status = handle.get_status()
        statuses.append(status.status)
        executors.append(status.executor_id)
    assert set(statuses) == {
        WorkflowStatusString.PENDING.value,
        WorkflowStatusString.ENQUEUED.value,
    }
    assert len(set(executors)) == 2
    assert "local" in executors

    # Now check in the DB that global concurrency is met
    with dbos._sys_db.engine.begin() as conn:
        query = (
            sa.select(sa.func.count())
            .select_from(SystemSchema.workflow_status)
            .where(
                SystemSchema.workflow_status.c.status
                == WorkflowStatusString.PENDING.value
            )
        )
        row = conn.execute(query).fetchone()

        assert row is not None, "Query returned no results"
        count = row[0]
        assert (
            count == global_concurrency_limit
        ), f"Expected {global_concurrency_limit} workflows, found {count}"

    # Signal the workers they can move on
    for signal in end_signals:
        signal.set()

    for process in processes:
        process.join()

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


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


def test_queue_recovery(dbos: DBOS) -> None:
    step_counter: int = 0
    queued_steps = 5

    wfid = str(uuid.uuid4())
    queue = Queue("test_queue")
    step_events = [threading.Event() for _ in range(queued_steps)]
    event = threading.Event()

    @DBOS.workflow()
    def test_workflow() -> list[int]:
        assert DBOS.workflow_id == wfid
        handles = []
        for i in range(queued_steps):
            h = queue.enqueue(test_step, i)
            handles.append(h)
        return [h.get_result() for h in handles]

    @DBOS.step()
    def test_step(i: int) -> int:
        nonlocal step_counter
        step_counter += 1
        step_events[i].set()
        event.wait()
        return i

    # Start the workflow. Wait for all five steps to start. Verify that they started.
    with SetWorkflowID(wfid):
        original_handle = DBOS.start_workflow(test_workflow)
    for e in step_events:
        e.wait()
        e.clear()

    assert step_counter == 5

    # Recover the workflow, then resume it.
    recovery_handles = DBOS.recover_pending_workflows()
    # Wait until the 2nd invocation of the workflows are dequeued and executed
    for e in step_events:
        e.wait()
    event.set()

    # There should be one handle for the workflow and another for each queued step.
    assert len(recovery_handles) == queued_steps + 1
    # Verify that both the recovered and original workflows complete correctly.
    for h in recovery_handles:
        if h.get_workflow_id() == wfid:
            assert h.get_result() == [0, 1, 2, 3, 4]
    assert original_handle.get_result() == [0, 1, 2, 3, 4]
    # Each step should start twice, once originally and once in recovery.
    assert step_counter == 10

    # Rerun the workflow. Because each step is complete, none should start again.
    with SetWorkflowID(wfid):
        assert test_workflow() == [0, 1, 2, 3, 4]
    assert step_counter == 10

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_queue_concurrency_under_recovery(dbos: DBOS) -> None:
    event = threading.Event()
    wf_events = [threading.Event() for _ in range(2)]
    counter = 0

    @DBOS.workflow()
    def blocked_workflow(i: int) -> None:
        wf_events[i].set()
        nonlocal counter
        counter += 1
        event.wait()

    @DBOS.workflow()
    def noop() -> None:
        pass

    queue = Queue(
        "test_queue", worker_concurrency=2
    )  # covers global concurrency limit because we have a single process
    handle1 = queue.enqueue(blocked_workflow, 0)
    handle2 = queue.enqueue(blocked_workflow, 1)
    handle3 = queue.enqueue(noop)

    # Wait for the two first workflows to be dequeued
    for e in wf_events:
        e.wait()
        e.clear()

    assert counter == 2
    assert handle1.get_status().status == WorkflowStatusString.PENDING.value
    assert handle2.get_status().status == WorkflowStatusString.PENDING.value
    assert handle3.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Manually update the database to pretend the 3rd workflow is PENDING and comes from another executor
    with dbos._sys_db.engine.begin() as c:
        query = (
            sa.update(SystemSchema.workflow_status)
            .values(status=WorkflowStatusString.PENDING.value, executor_id="other")
            .where(
                SystemSchema.workflow_status.c.workflow_uuid
                == handle3.get_workflow_id()
            )
        )
        c.execute(query)

    # Trigger workflow recovery. The two first workflows should still be blocked but the 3rd one enqueued
    recovered_other_handles = DBOS.recover_pending_workflows(["other"])
    assert handle1.get_status().status == WorkflowStatusString.PENDING.value
    assert handle2.get_status().status == WorkflowStatusString.PENDING.value
    assert len(recovered_other_handles) == 1
    assert recovered_other_handles[0].get_workflow_id() == handle3.get_workflow_id()
    assert handle3.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Trigger workflow recovery for "local". The two first workflows should be re-enqueued then dequeued again
    recovered_local_handles = DBOS.recover_pending_workflows(["local"])
    assert len(recovered_local_handles) == 2
    for h in recovered_local_handles:
        assert h.get_workflow_id() in [
            handle1.get_workflow_id(),
            handle2.get_workflow_id(),
        ]
    for e in wf_events:
        e.wait()
    assert counter == 4
    assert handle1.get_status().status == WorkflowStatusString.PENDING.value
    assert handle2.get_status().status == WorkflowStatusString.PENDING.value
    # Because tasks are re-enqueued in order, the 3rd task is head of line blocked
    assert handle3.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Unblock the first two workflows
    event.set()

    # Verify all queue entries eventually get cleaned up.
    assert handle1.get_result() == None
    assert handle2.get_result() == None
    assert handle3.get_result() == None
    assert handle3.get_status().executor_id == "local"
    assert queue_entries_are_cleaned_up(dbos)


def test_cancelling_queued_workflows(dbos: DBOS) -> None:
    start_event = threading.Event()
    blocking_event = threading.Event()

    @DBOS.workflow()
    def stuck_workflow() -> None:
        start_event.set()
        blocking_event.wait()

    @DBOS.workflow()
    def regular_workflow() -> None:
        return

    # Enqueue both the blocked workflow and a regular workflow on a queue with concurrency 1
    queue = Queue("test_queue", concurrency=1)
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        blocked_handle = queue.enqueue(stuck_workflow)
    regular_handle = queue.enqueue(regular_workflow)

    # Verify that the blocked workflow starts and is PENDING while the regular workflow remains ENQUEUED.
    start_event.wait()
    assert blocked_handle.get_status().status == WorkflowStatusString.PENDING.value
    assert regular_handle.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Cancel the blocked workflow. Verify this lets the regular workflow run.
    dbos.cancel_workflow(wfid)
    assert blocked_handle.get_status().status == WorkflowStatusString.CANCELLED.value
    assert regular_handle.get_result() == None

    # Complete the blocked workflow
    blocking_event.set()
    assert blocked_handle.get_result() == None

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_resuming_queued_workflows(dbos: DBOS) -> None:
    start_event = threading.Event()
    blocking_event = threading.Event()

    @DBOS.workflow()
    def stuck_workflow() -> None:
        start_event.set()
        blocking_event.wait()

    @DBOS.workflow()
    def regular_workflow() -> None:
        return

    # Enqueue a blocked workflow and two regular workflows on a queue with concurrency 1
    queue = Queue("test_queue", concurrency=1)
    wfid = str(uuid.uuid4())
    blocked_handle = queue.enqueue(stuck_workflow)
    with SetWorkflowID(wfid):
        regular_handle_1 = queue.enqueue(regular_workflow)
    regular_handle_2 = queue.enqueue(regular_workflow)

    # Verify that the blocked workflow starts and is PENDING while the regular workflows remain ENQUEUED.
    start_event.wait()
    assert blocked_handle.get_status().status == WorkflowStatusString.PENDING.value
    assert regular_handle_1.get_status().status == WorkflowStatusString.ENQUEUED.value
    assert regular_handle_2.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Resume a regular workflow. Verify it completes.
    dbos.resume_workflow(wfid)
    assert regular_handle_1.get_result() == None

    # Complete the blocked workflow. Verify the second regular workflow also completes.
    blocking_event.set()
    assert blocked_handle.get_result() == None
    assert regular_handle_2.get_result() == None

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


# Test a race condition between removing a task from the queue and flushing the status buffer
def test_resuming_already_completed_queue_workflow(dbos: DBOS) -> None:
    dbos._sys_db._run_background_processes = False # Disable buffer flush

    start_event = threading.Event()
    counter = 0
    @DBOS.workflow()
    def test_step() -> None:
        start_event.set()
        nonlocal counter
        counter += 1

    queue = Queue("test_queue")
    handle = queue.enqueue(test_step)
    start_event.wait()
    start_event.clear()
    time.sleep(_buffer_flush_interval_secs)
    assert handle.get_status().status == WorkflowStatusString.PENDING.value # Not flushed
    assert counter == 1 # But, really, it's completed
    dbos._sys_db._workflow_status_buffer = {} # Clear buffer (simulates a process restart)

    # Recovery picks up on the workflow and recovers it
    recovered_ids = DBOS.recover_pending_workflows()
    assert len(recovered_ids) == 1
    assert recovered_ids[0].get_workflow_id() == handle.get_workflow_id()
    start_event.wait()
    assert counter == 2 # The workflow ran again
    time.sleep(_buffer_flush_interval_secs) # This is actually to wait that _get_wf_invoke_func buffers the status
    dbos._sys_db._flush_workflow_status_buffer() # Manually flush
    assert handle.get_status().status == WorkflowStatusString.SUCCESS.value # Is recovered
    assert handle.get_status().executor_id == "local"
    assert handle.get_status().recovery_attempts == 2


def test_dlq_enqueued_workflows(dbos: DBOS) -> None:
    start_event = threading.Event()
    blocking_event = threading.Event()
    max_recovery_attempts = 10
    recovery_count = 0

    @DBOS.workflow(max_recovery_attempts=max_recovery_attempts)
    def blocked_workflow() -> None:
        start_event.set()
        nonlocal recovery_count
        recovery_count += 1
        blocking_event.wait()

    @DBOS.workflow()
    def regular_workflow() -> None:
        return

    # Enqueue both the blocked workflow and a regular workflow on a queue with concurrency 1
    queue = Queue("test_queue", concurrency=1)
    blocked_handle = queue.enqueue(blocked_workflow)
    regular_handle = queue.enqueue(regular_workflow)

    # Verify that the blocked workflow starts and is PENDING while the regular workflow remains ENQUEUED.
    start_event.wait()
    assert blocked_handle.get_status().status == WorkflowStatusString.PENDING.value
    assert regular_handle.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Attempt to recover the blocked workflow the maximum number of times
    for i in range(max_recovery_attempts):
        start_event.clear()
        DBOS.recover_pending_workflows()
        start_event.wait()
        assert recovery_count == i + 2

    # Verify an additional recovery throws puts the workflow in the DLQ status.
    DBOS.recover_pending_workflows()
    # we can't start_event.wait() here because the workflow will never execute
    time.sleep(2)
    assert (
        blocked_handle.get_status().status
        == WorkflowStatusString.RETRIES_EXCEEDED.value
    )
    with dbos._sys_db.engine.begin() as c:
        query = sa.select(SystemSchema.workflow_status.c.recovery_attempts).where(
            SystemSchema.workflow_status.c.workflow_uuid
            == blocked_handle.get_workflow_id()
        )
        result = c.execute(query)
        row = result.fetchone()
        assert row is not None
        assert row[0] == max_recovery_attempts + 2

    # Verify the blocked workflow entering the DLQ lets the regular workflow run
    assert regular_handle.get_result() == None

    # Complete the blocked workflow
    blocking_event.set()
    assert blocked_handle.get_result() == None
    dbos._sys_db.wait_for_buffer_flush()
    assert blocked_handle.get_status().status == WorkflowStatusString.SUCCESS.value
    with dbos._sys_db.engine.begin() as c:
        query = sa.select(SystemSchema.workflow_status.c.recovery_attempts).where(
            SystemSchema.workflow_status.c.workflow_uuid
            == blocked_handle.get_workflow_id()
        )
        result = c.execute(query)
        row = result.fetchone()
        assert row is not None
        assert row[0] == max_recovery_attempts + 2

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)
