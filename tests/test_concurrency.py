import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Tuple

from sqlalchemy import text

# Public API
from dbos import DBOS, SetWorkflowID


def test_concurrent_workflows(dbos: DBOS) -> None:
    @DBOS.workflow()
    def test_workflow() -> str:
        time.sleep(1)
        return DBOS.workflow_id

    def test_thread(id: str) -> str:
        with SetWorkflowID(id):
            return test_workflow()

    num_threads = 10
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures: list[Tuple[str, Future[str]]] = []
        for _ in range(num_threads):
            id = str(uuid.uuid4())
            futures.append((id, executor.submit(test_thread, id)))
        for id, future in futures:
            assert id == future.result()


def test_concurrent_conflict_uuid(dbos: DBOS) -> None:
    condition = threading.Condition()
    step_count = 0
    txn_count = 0

    @DBOS.step()
    def test_step() -> str:
        nonlocal step_count
        condition.acquire()
        step_count += 1
        if step_count % 2 == 1:
            # Wait for the other one to notify
            condition.wait()
        else:
            # Notify the other one
            condition.notify()
        condition.release()

        return DBOS.workflow_id

    @DBOS.workflow()
    def test_workflow() -> str:
        res = test_step()
        return res

    def test_comm_thread(id: str) -> str:
        with SetWorkflowID(id):
            return test_step()

    # Need to set isolation level to a lower one, otherwise it gets serialization error instead (we already handle it correctly by automatic retries).
    @DBOS.transaction(isolation_level="REPEATABLE READ")
    def test_transaction() -> str:
        DBOS.sql_session.execute(text("SELECT 1")).fetchall()
        nonlocal txn_count
        condition.acquire()
        txn_count += 1
        if txn_count % 2 == 1:
            # Wait for the other one to notify
            condition.wait()
        else:
            # Notify the other one
            condition.notify()
        condition.release()

        return DBOS.workflow_id

    def test_txn_thread(id: str) -> str:
        with SetWorkflowID(id):
            return test_transaction()

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        wf_handle1 = dbos.start_workflow(test_workflow)

    with SetWorkflowID(wfuuid):
        wf_handle2 = dbos.start_workflow(test_workflow)

    # These two workflows should run concurrently, but both should succeed.
    assert wf_handle1.get_result() == wfuuid
    assert wf_handle2.get_result() == wfuuid

    # Make sure temp workflows can handle conflicts as well.
    wfuuid = str(uuid.uuid4())
    with ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(test_comm_thread, wfuuid)
        future2 = executor.submit(test_comm_thread, wfuuid)

    assert future1.result() == wfuuid
    assert future2.result() == wfuuid

    # Make sure temp transactions can handle conflicts as well.
    wfuuid = str(uuid.uuid4())
    with ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(test_txn_thread, wfuuid)
        future2 = executor.submit(test_txn_thread, wfuuid)

    assert future1.result() == wfuuid
    assert future2.result() == wfuuid
