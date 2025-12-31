import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Tuple, cast

from sqlalchemy import text

# Public API
from dbos import DBOS, SetWorkflowID
from tests.conftest import using_sqlite


def test_concurrent_workflows(dbos: DBOS) -> None:
    @DBOS.workflow()
    def test_workflow() -> str:
        time.sleep(1)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

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


def test_concurrent_getevent(dbos: DBOS) -> None:
    @DBOS.workflow()
    def test_workflow(event_name: str, value: str) -> str:
        DBOS.set_event(event_name, value)
        return value

    def test_thread(id: str, event_name: str) -> str:
        return cast(str, DBOS.get_event(id, event_name, 5))

    wfuuid = str(uuid.uuid4())
    event_name = "test_event"
    with ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(test_thread, wfuuid, event_name)
        future2 = executor.submit(test_thread, wfuuid, event_name)

        expected_message = "test message"
        with SetWorkflowID(wfuuid):
            test_workflow(event_name, expected_message)

        # Both should return the same message (shared handle, NOT concurrent)
        assert future1.result() == future2.result()
        assert future1.result() == expected_message
        # Make sure the event map is empty
        assert not dbos._sys_db.workflow_events_map._dict
