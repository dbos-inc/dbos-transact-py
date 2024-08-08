import random
import string
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Tuple

from dbos_transact import DBOS, SetWorkflowUUID


def test_concurrent_workflows(dbos: DBOS) -> None:

    @dbos.workflow()
    def test_workflow() -> str:
        time.sleep(1)
        return DBOS.workflow_id

    def test_thread(id: str) -> str:
        with SetWorkflowUUID(id):
            return test_workflow()

    num_threads = 10
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures: list[Tuple[str, Future[str]]] = []
        for _ in range(num_threads):
            id = "".join(random.choices(string.ascii_letters + string.digits, k=10))
            futures.append((id, executor.submit(test_thread, id)))
        for id, future in futures:
            assert id == future.result()
