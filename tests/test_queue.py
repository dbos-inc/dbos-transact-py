import asyncio
import multiprocessing
import multiprocessing.synchronize
import os
import subprocess
import threading
import time
import uuid
from typing import Any, List
from urllib.parse import quote

import pytest
import sqlalchemy as sa
from pydantic import BaseModel

from dbos import (
    DBOS,
    DBOSClient,
    DBOSConfig,
    DBOSConfiguredInstance,
    Queue,
    SetEnqueueOptions,
    SetWorkflowID,
    SetWorkflowTimeout,
    WorkflowHandle,
)
from dbos._context import assert_current_dbos_context
from dbos._dbos import WorkflowHandleAsync
from dbos._error import DBOSAwaitedWorkflowCancelledError
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import WorkflowStatusString
from dbos._utils import GlobalParams
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
    assert wf_counter == 1
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
        handle = queue.enqueue(test_step, "abc")
    assert handle.get_result() == "abc1"
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
    period = 1.8
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
            assert times[i + 1] - times[i] < 0.5

    # Verify that the gap between "waves" is ~equal to the period
    for wave in range(num_waves - 1):
        assert times[limit * (wave + 1)] - times[limit * wave] > period - 0.5
        assert times[limit * (wave + 1)] - times[limit * wave] < period + 0.5

    # Verify all workflows get the SUCCESS status eventually
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
    period = 1.8
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
            assert times[i + 1] - times[i] < 0.5

    # Verify that the gap between "waves" is ~equal to the period
    for wave in range(num_waves - 1):
        assert times[limit * (wave + 1)] - times[limit * wave] > period - 0.5
        assert times[limit * (wave + 1)] - times[limit * wave] < period + 0.5

    # Verify all workflows get the SUCCESS status eventually
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
    dbos_config: DBOSConfig = {
        "name": "test-app",
        "system_database_url": default_config()["system_database_url"],
        "application_database_url": default_config()["application_database_url"],
        "admin_port": 8001 + i,
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
def test_worker_concurrency_with_n_dbos_instances(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
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
        os.environ["DBOS__APPVERSION"] = GlobalParams.app_version
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
    del os.environ["DBOS__APPVERSION"]

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
def test_duplicate_workflow_id(dbos: DBOS) -> None:
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

    with SetWorkflowID(wfid):
        origHandle = DBOS.start_workflow(test_workflow, "abc")
        # The second one will generate a warning message but no error.
        test_dup_workflow()

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

    # Call with a different input still uses the recorded input.
    with SetWorkflowID(wfid):
        res = test_workflow("def")
        # We want to see the warning message, but the result is non-deterministic
        # TODO: in the future, we may want to always use the recorded inputs.
        assert res == "abc" or res == "def"

    assert origHandle.get_result() == "abc"
    assert same_handle.get_result() == "abc"


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
    recovery_handles = DBOS._recover_pending_workflows()
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
    recovered_other_handles = DBOS._recover_pending_workflows(["other"])
    assert handle1.get_status().status == WorkflowStatusString.PENDING.value
    assert handle2.get_status().status == WorkflowStatusString.PENDING.value
    assert len(recovered_other_handles) == 1
    assert recovered_other_handles[0].get_workflow_id() == handle3.get_workflow_id()
    assert handle3.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Trigger workflow recovery for "local". The two first workflows should be re-enqueued then dequeued again
    recovered_local_handles = DBOS._recover_pending_workflows(["local"])
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
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        blocked_handle.get_result()

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_timeout_queue(dbos: DBOS) -> None:
    @DBOS.workflow()
    def blocking_workflow() -> None:
        assert assert_current_dbos_context().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is not None
        while True:
            DBOS.sleep(0.1)

    @DBOS.workflow()
    def normal_workflow() -> None:
        assert assert_current_dbos_context().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is not None
        return

    queue = Queue("test_queue", concurrency=1)

    # Enqueue a few blocked workflow
    num_workflows = 3
    handles: list[WorkflowHandle[None]] = []
    for _ in range(num_workflows):
        with SetWorkflowTimeout(0.1):
            handle = queue.enqueue(blocking_workflow)
            handles.append(handle)

    # Also enqueue a normal workflow
    with SetWorkflowTimeout(1.0):
        normal_handle = queue.enqueue(normal_workflow)

    # Verify the blocked workflows are cancelled
    for handle in handles:
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            handle.get_result()

    # Verify the normal workflow succeeds
    normal_handle.get_result()

    # Verify if a parent called with a timeout enqueues a blocked child
    # the deadline propagates and the child is also cancelled.
    child_id = str(uuid.uuid4())
    queue = Queue("regular_queue")

    @DBOS.workflow()
    def parent_workflow() -> None:
        with SetWorkflowID(child_id):
            handle = queue.enqueue(blocking_workflow)
        handle.get_result()

    with SetWorkflowTimeout(1.0):
        handle = queue.enqueue(parent_workflow)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        DBOS.retrieve_workflow(child_id).get_result()

    # Verify if a parent called with a timeout enqueues a blocked child
    # then exits the deadline propagates and the child is cancelled.
    queue = Queue("regular_queue")

    @DBOS.workflow()
    def exiting_parent_workflow() -> str:
        handle = queue.enqueue(blocking_workflow)
        return handle.get_workflow_id()

    with SetWorkflowTimeout(1.0):
        child_id = exiting_parent_workflow()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        DBOS.retrieve_workflow(child_id).get_result()

    # Verify if a parent called with a timeout enqueues a child that
    # never starts because the queue is blocked, the deadline propagates
    # and both parent and child are cancelled.
    child_id = str(uuid.uuid4())
    queue = Queue("stuck_queue", concurrency=1)

    start_event = threading.Event()
    blocking_event = threading.Event()

    @DBOS.workflow()
    def stuck_workflow() -> None:
        start_event.set()
        blocking_event.wait()

    stuck_handle = queue.enqueue(stuck_workflow)
    start_event.wait()

    @DBOS.workflow()
    def blocked_parent_workflow() -> None:
        with SetWorkflowID(child_id):
            queue.enqueue(blocking_workflow)
        while True:
            DBOS.sleep(0.1)

    with SetWorkflowTimeout(1.0):
        handle = DBOS.start_workflow(blocked_parent_workflow)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        DBOS.retrieve_workflow(child_id).get_result()
    blocking_event.set()
    stuck_handle.get_result()

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

    # Enqueue the blocked workflow repeatedly, verify recovery attempts is not increased
    for _ in range(max_recovery_attempts):
        with SetWorkflowID(blocked_handle.workflow_id):
            queue.enqueue(blocked_workflow)
    recovery_attempts = blocked_handle.get_status().recovery_attempts
    assert recovery_attempts is not None and recovery_attempts <= 1

    # Verify that the blocked workflow starts and is PENDING while the regular workflow remains ENQUEUED.
    start_event.wait()
    assert blocked_handle.get_status().status == WorkflowStatusString.PENDING.value
    assert regular_handle.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Attempt to recover the blocked workflow the maximum number of times
    for i in range(max_recovery_attempts):
        start_event.clear()
        DBOS._recover_pending_workflows()
        start_event.wait()
        assert recovery_count == i + 2

    # Verify an additional recovery throws puts the workflow in the DLQ status.
    DBOS._recover_pending_workflows()
    # we can't start_event.wait() here because the workflow will never execute
    time.sleep(2)
    assert (
        blocked_handle.get_status().status
        == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value
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


@pytest.mark.asyncio
async def test_simple_queue_async(dbos: DBOS) -> None:
    wf_counter: int = 0
    step_counter: int = 0

    wfid = str(uuid.uuid4())

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        assert DBOS.workflow_id == wfid
        nonlocal wf_counter
        wf_counter += 1
        var1 = await test_step(var1)
        return var1 + var2

    @DBOS.step()
    async def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return var + "d"

    queue = Queue("test_queue")

    with SetWorkflowID(wfid):
        handle = await queue.enqueue_async(test_workflow, "abc", "123")
    assert (await handle.get_result()) == "abcd123"
    with SetWorkflowID(wfid):
        assert (await test_workflow("abc", "123")) == "abcd123"
    assert wf_counter == 2
    assert step_counter == 1


def test_queue_deduplication(dbos: DBOS) -> None:
    queue_name = "test_dedup_queue"
    queue = Queue(queue_name)
    workflow_event = threading.Event()

    @DBOS.workflow()
    def child_workflow(var1: str) -> str:
        workflow_event.wait()
        return var1 + "-c"

    @DBOS.workflow()
    def test_workflow(var1: str) -> str:
        # Make sure the child workflow is not blocked by the same deduplication ID
        child_handle = queue.enqueue(child_workflow, var1)
        workflow_event.wait()
        return child_handle.get_result() + "-p"

    # Make sure only one workflow is running at a time
    wfid = str(uuid.uuid4())
    dedup_id = "my_dedup_id"
    with SetEnqueueOptions(deduplication_id=dedup_id):
        with SetWorkflowID(wfid):
            handle1 = queue.enqueue(test_workflow, "abc")

    # Enqueue the same workflow with a different deduplication ID should be fine.
    with SetEnqueueOptions(deduplication_id="my_other_dedup_id"):
        another_handle = queue.enqueue(test_workflow, "ghi")

    # Enqueue a workflow without deduplication ID should be fine.
    nodedup_handle1 = queue.enqueue(test_workflow, "jkl")

    # Enqueued multiple times without deduplication ID but with different inputs should be fine, but get the result of the first one.
    with SetWorkflowID(wfid):
        nodedup_handle2 = queue.enqueue(test_workflow, "mno")

    # Enqueue the same workflow with the same deduplication ID should raise an exception.
    wfid2 = str(uuid.uuid4())
    with SetEnqueueOptions(deduplication_id=dedup_id):
        with SetWorkflowID(wfid2):
            with pytest.raises(Exception) as exc_info:
                queue.enqueue(test_workflow, "def")
        assert (
            f"Workflow {wfid2} was deduplicated due to an existing workflow in queue {queue_name} with deduplication ID {dedup_id}."
            in str(exc_info.value)
        )

    # Now unblock the first two workflows and wait for them to finish.
    workflow_event.set()
    assert handle1.get_result() == "abc-c-p"
    assert another_handle.get_result() == "ghi-c-p"
    assert nodedup_handle1.get_result() == "jkl-c-p"
    assert nodedup_handle2.get_result() == "abc-c-p"

    # Invoke the workflow again with the same deduplication ID now should be fine because it's no longer in the queue.
    with SetEnqueueOptions(deduplication_id=dedup_id):
        with SetWorkflowID(wfid2):
            handle2 = queue.enqueue(test_workflow, "def")
    assert handle2.get_result() == "def-c-p"

    assert queue_entries_are_cleaned_up(dbos)


@pytest.mark.asyncio
async def test_queue_deduplication_async(dbos: DBOS) -> None:
    queue_name = "test_dedup_queue_async"
    queue = Queue(queue_name)
    workflow_event = asyncio.Event()

    @DBOS.workflow()
    async def child_workflow(var1: str) -> str:
        await workflow_event.wait()
        return var1 + "-c"

    @DBOS.workflow()
    async def test_workflow(var1: str) -> str:
        # Make sure the child workflow is not blocked by the same deduplication ID
        child_handle = await queue.enqueue_async(child_workflow, var1)
        await workflow_event.wait()
        return (await child_handle.get_result()) + "-p"

    # Make sure only one workflow is running at a time
    wfid = str(uuid.uuid4())
    dedup_id = "my_dedup_id"
    with SetEnqueueOptions(deduplication_id=dedup_id):
        with SetWorkflowID(wfid):
            handle1 = await queue.enqueue_async(test_workflow, "abc")

    # Enqueue the same workflow with a different deduplication ID should be fine.
    with SetEnqueueOptions(deduplication_id="my_other_dedup_id"):
        another_handle = await queue.enqueue_async(test_workflow, "ghi")

    # Enqueue a workflow without deduplication ID should be fine.
    nodedup_handle1 = await queue.enqueue_async(test_workflow, "jkl")

    # Enqueued multiple times without deduplication ID but with different inputs should be fine, but get the result of the first one.
    with SetWorkflowID(wfid):
        nodedup_handle2 = await queue.enqueue_async(test_workflow, "mno")

    # Enqueue the same workflow with the same deduplication ID should raise an exception.
    wfid2 = str(uuid.uuid4())
    with SetEnqueueOptions(deduplication_id=dedup_id):
        with SetWorkflowID(wfid2):
            with pytest.raises(Exception) as exc_info:
                await queue.enqueue_async(test_workflow, "def")
        assert (
            f"Workflow {wfid2} was deduplicated due to an existing workflow in queue {queue_name} with deduplication ID {dedup_id}."
            in str(exc_info.value)
        )

    # Now unblock the first two workflows and wait for them to finish.
    workflow_event.set()
    assert (await handle1.get_result()) == "abc-c-p"
    assert (await another_handle.get_result()) == "ghi-c-p"
    assert (await nodedup_handle1.get_result()) == "jkl-c-p"
    assert (await nodedup_handle2.get_result()) == "abc-c-p"

    # Invoke the workflow again with the same deduplication ID now should be fine because it's no longer in the queue.
    with SetEnqueueOptions(deduplication_id=dedup_id):
        with SetWorkflowID(wfid2):
            handle2 = await queue.enqueue_async(test_workflow, "def")
    assert (await handle2.get_result()) == "def-c-p"

    assert queue_entries_are_cleaned_up(dbos)


def test_priority_queue(dbos: DBOS) -> None:
    # Make sure that we can enqueue workflows with different priorities correctly
    queue = Queue("test_queue_priority", 1, priority_enabled=True)
    child_queue = Queue("test_queue_child")

    workflow_event = threading.Event()
    wf_priority_list = []

    @DBOS.workflow()
    def child_workflow(p: int) -> int:
        workflow_event.wait()
        return p

    @DBOS.workflow()
    def test_workflow(priority: int) -> int:
        wf_priority_list.append(priority)
        # Make sure the priority is not propagated
        assert assert_current_dbos_context().priority == None
        child_handle = child_queue.enqueue(child_workflow, priority)
        workflow_event.wait()
        return child_handle.get_result() + priority

    # Enqueue an invalid priority
    with pytest.raises(Exception) as exc_info:
        with SetEnqueueOptions(priority=-100):
            queue.enqueue(test_workflow, -100)
    assert "Invalid priority" in str(exc_info.value)

    wf_handles = []
    # First, enqueue a workflow without priority
    handle = queue.enqueue(test_workflow, 0)
    wf_handles.append(handle)

    # Then, enqueue a workflow with priority 1 to 5
    for i in range(1, 6):
        with SetEnqueueOptions(priority=i):
            handle = queue.enqueue(test_workflow, i)
        wf_handles.append(handle)

    # Finally, enqueue two workflows without priority again
    wf_handles.append(queue.enqueue(test_workflow, 6))
    wf_handles.append(queue.enqueue(test_workflow, 7))

    # The finish sequence should be 0, 6, 7, 1, 2, 3, 4, 5
    workflow_event.set()
    for i in range(len(wf_handles)):
        res = wf_handles[i].get_result()
        assert res == i * 2

    assert wf_priority_list == [0, 6, 7, 1, 2, 3, 4, 5]
    assert queue_entries_are_cleaned_up(dbos)


@pytest.mark.asyncio
async def test_priority_queue_async(dbos: DBOS) -> None:
    # Make sure that we can enqueue workflows with different priorities correctly
    queue = Queue("test_queue_priority_async", 1, priority_enabled=True)
    child_queue = Queue("test_queue_child_async")

    workflow_event = asyncio.Event()
    wf_priority_list = []

    @DBOS.workflow()
    async def child_workflow(p: int) -> int:
        await workflow_event.wait()
        return p

    @DBOS.workflow()
    async def test_workflow(priority: int) -> int:
        wf_priority_list.append(priority)
        # Make sure the priority is not propagated
        assert assert_current_dbos_context().priority == None
        child_handle = await child_queue.enqueue_async(child_workflow, priority)
        await workflow_event.wait()
        return (await child_handle.get_result()) + priority

    # Enqueue an invalid priority
    with pytest.raises(Exception) as exc_info:
        with SetEnqueueOptions(priority=-100):
            await queue.enqueue_async(test_workflow, -100)
    assert "Invalid priority" in str(exc_info.value)

    wf_handles: List[WorkflowHandleAsync[int]] = []
    # First, enqueue a workflow without priority
    handle = await queue.enqueue_async(test_workflow, 0)
    wf_handles.append(handle)

    # Then, enqueue a workflow with priority 1 to 5
    for i in range(1, 6):
        with SetEnqueueOptions(priority=i):
            handle = await queue.enqueue_async(test_workflow, i)
        wf_handles.append(handle)

    # Finally, enqueue two workflows without priority again
    wf_handles.append(await queue.enqueue_async(test_workflow, 6))
    wf_handles.append(await queue.enqueue_async(test_workflow, 7))

    # The finish sequence should be 0, 6, 7, 1, 2, 3, 4, 5
    workflow_event.set()
    for i in range(len(wf_handles)):
        res = await wf_handles[i].get_result()
        assert res == i * 2

    assert wf_priority_list == [0, 6, 7, 1, 2, 3, 4, 5]
    assert queue_entries_are_cleaned_up(dbos)


def test_worker_concurrency_across_versions(dbos: DBOS, client: DBOSClient) -> None:
    queue = Queue("test_worker_concurrency_across_versions", worker_concurrency=1)

    @DBOS.workflow()
    def test_workflow() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    # First enqueue a workflow on the other version, then on the current version
    other_version = "other_version"
    other_version_handle: WorkflowHandle[None] = client.enqueue(
        {
            "queue_name": "test_worker_concurrency_across_versions",
            "workflow_name": test_workflow.__qualname__,
            "app_version": other_version,
        }
    )
    handle = queue.enqueue(test_workflow)

    # Verify the workflow on the current version completes, but the other version is still ENQUEUED
    assert handle.get_result()
    assert other_version_handle.get_status().status == "ENQUEUED"

    # Change the version, verify the other version complets
    GlobalParams.app_version = other_version
    assert other_version_handle.get_result()


def test_timeout_queue_recovery(dbos: DBOS) -> None:
    queue = Queue("test_queue")
    evt = threading.Event()

    @DBOS.workflow()
    def blocking_workflow() -> None:
        evt.set()
        while True:
            DBOS.sleep(0.1)

    timeout = 3.0
    enqueue_time = time.time()
    with SetWorkflowTimeout(timeout):
        original_handle = queue.enqueue(blocking_workflow)

    # Verify the workflow's timeout is properly configured
    evt.wait()
    original_status = original_handle.get_status()
    assert original_status.workflow_timeout_ms == timeout * 1000
    assert (
        original_status.workflow_deadline_epoch_ms is not None
        and original_status.workflow_deadline_epoch_ms > enqueue_time * 1000
    )

    # Recover the workflow. Verify its deadline remains the same
    evt.clear()
    handles = DBOS._recover_pending_workflows()
    assert len(handles) == 1
    evt.wait()
    recovered_handle = handles[0]
    recovered_status = recovered_handle.get_status()
    assert recovered_status.workflow_timeout_ms == timeout * 1000
    assert (
        recovered_status.workflow_deadline_epoch_ms
        == original_status.workflow_deadline_epoch_ms
    )

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        original_handle.get_result()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        recovered_handle.get_result()


def test_unsetting_timeout(dbos: DBOS) -> None:

    queue = Queue("test_queue")

    @DBOS.workflow()
    def child() -> str:
        for _ in range(5):
            DBOS.sleep(1)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    @DBOS.workflow()
    def parent(child_one: str, child_two: str) -> None:
        with SetWorkflowID(child_two):
            with SetWorkflowTimeout(None):
                queue.enqueue(child)

        with SetWorkflowID(child_one):
            queue.enqueue(child)

    child_one, child_two = str(uuid.uuid4()), str(uuid.uuid4())
    with SetWorkflowTimeout(1.0):
        queue.enqueue(parent, child_one, child_two).get_result()

    # Verify child one, which has a propagated timeout, is cancelled
    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(child_one)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()

    # Verify child two, which doesn't have a timeout, succeeds
    handle = DBOS.retrieve_workflow(child_two)
    assert handle.get_result() == child_two


def test_queue_executor_id(dbos: DBOS) -> None:

    queue = Queue("test-queue")

    @DBOS.workflow()
    def example_workflow() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    # Set an executor ID
    original_executor_id = str(uuid.uuid4())
    GlobalParams.executor_id = original_executor_id

    # Enqueue the workflow, validate its executor ID
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = queue.enqueue(example_workflow)
    assert handle.get_result() == wfid
    assert handle.get_status().executor_id == original_executor_id

    # Set a new executor ID
    new_executor_id = str(uuid.uuid4())
    GlobalParams.executor_id = new_executor_id

    # Re-enqueue the workflow, verify its executor ID does not change.
    with SetWorkflowID(wfid):
        handle = queue.enqueue(example_workflow)
        assert handle.get_result() == wfid
    assert handle.get_status().executor_id == original_executor_id


# Non-basic types must be declared in an importable scope (so not inside a function)
# to be serializable and deserializable
class InnerType(BaseModel):
    one: str
    two: int


class OuterType(BaseModel):
    inner: InnerType


def test_complex_type(dbos: DBOS) -> None:
    queue = Queue("test_queue")

    @DBOS.workflow()
    def workflow(input: OuterType) -> OuterType:
        return input

    # Verify a workflow with non-basic inputs and outputs can be enqueued
    inner = InnerType(one="one", two=2)
    outer = OuterType(inner=inner)

    handle = queue.enqueue(workflow, outer)
    result = handle.get_result()

    def check(result: Any) -> None:
        assert isinstance(result, OuterType)
        assert isinstance(result.inner, InnerType)
        assert result.inner.one == outer.inner.one
        assert result.inner.two == outer.inner.two

    check(result)

    # Verify a workflow with non-basic inputs and outputs can be recovered
    start_event = threading.Event()
    event = threading.Event()

    @DBOS.workflow()
    def blocked_workflow(input: OuterType) -> OuterType:
        start_event.set()
        event.wait()
        return input

    handle = queue.enqueue(blocked_workflow, outer)

    start_event.wait()
    recovery_handle = DBOS._recover_pending_workflows()[0]
    event.set()
    check(handle.get_result())
    check(recovery_handle.get_result())


def test_enqueue_version(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    queue = Queue("queue")
    input = 5

    # Enqueue the app on a different version, verify it has that version
    future_version = str(uuid.uuid4())
    with SetEnqueueOptions(app_version=future_version):
        handle = queue.enqueue(workflow, input)
    assert handle.get_status().app_version == future_version

    # Change the global version, verify it works
    GlobalParams.app_version = future_version
    assert handle.get_result() == input


@pytest.mark.asyncio
async def test_enqueue_version_async(dbos: DBOS) -> None:

    @DBOS.workflow()
    async def workflow(x: int) -> int:
        return x

    queue = Queue("queue")
    input = 5

    # Enqueue the app on a different version, verify it has that version
    future_version = str(uuid.uuid4())
    with SetEnqueueOptions(app_version=future_version):
        handle = await queue.enqueue_async(workflow, input)
    assert (await handle.get_status()).app_version == future_version

    # Change the global version, verify it works
    GlobalParams.app_version = future_version
    assert await handle.get_result() == input
