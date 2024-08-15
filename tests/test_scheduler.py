import time
from datetime import datetime

from dbos_transact import DBOS


def test_simple_workflow(dbos: DBOS) -> None:
    wf_counter: int = 0

    @dbos.scheduled(1)
    @dbos.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    time.sleep(2)
    assert wf_counter >= 1 and wf_counter <= 3
