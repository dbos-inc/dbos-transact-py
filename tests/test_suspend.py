"""recv(suspend=True) and sleep(suspend=True) are Transact Enterprise features: without
suspend mode the calls are refused, and nothing in the send path or the background jobs
changes. The operations themselves are tested in dbos-enterprise."""

import uuid

import pytest

from dbos import DBOS, SetWorkflowID
from dbos._error import DBOSException


def test_recv_suspend_is_refused_without_suspend_mode(dbos: DBOS) -> None:
    assert dbos._sys_db.suspend is None

    @DBOS.workflow()
    def waiter() -> str:
        return str(DBOS.recv("topic", timeout_seconds=1, suspend=True))

    with SetWorkflowID(str(uuid.uuid4())):
        handle = DBOS.start_workflow(waiter)
    with pytest.raises(DBOSException, match="requires suspend mode"):
        handle.get_result()


def test_sleep_suspend_is_refused_without_suspend_mode(dbos: DBOS) -> None:
    assert dbos._sys_db.suspend is None

    @DBOS.workflow()
    def sleeper() -> None:
        DBOS.sleep(1, suspend=True)

    with SetWorkflowID(str(uuid.uuid4())):
        handle = DBOS.start_workflow(sleeper)
    with pytest.raises(DBOSException, match="requires suspend mode"):
        handle.get_result()
