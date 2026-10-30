"""recv(suspend=True) and sleep(suspend=True) are Transact Enterprise features: without
suspend mode the calls are refused. Waking a suspended recv is core: every send does it,
so a sender without suspend mode still wakes a workflow another process parked. The
operations themselves are tested in dbos-enterprise."""

import uuid

import pytest
import sqlalchemy as sa

from dbos import DBOS, DBOSClient, SetWorkflowID
from dbos._error import DBOSException, DBOSWorkflowSuspendedError
from dbos._outcome import NoResult
from dbos._sys_db import WorkflowStatusString


def test_suspended_error_names_what_the_workflow_waits_on() -> None:
    assert (
        DBOSWorkflowSuspendedError("w", "approve").message
        == "Workflow w suspended waiting on topic approve"
    )
    assert (
        DBOSWorkflowSuspendedError("w", "__null__topic__").message
        == "Workflow w suspended waiting on a message with no topic"
    )
    assert (
        DBOSWorkflowSuspendedError("w", None).message == "Workflow w suspended sleeping"
    )


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


@pytest.mark.asyncio
async def test_async_suspend_is_refused_without_suspend_mode(dbos: DBOS) -> None:
    assert dbos._sys_db.suspend is None

    @DBOS.workflow()
    async def waiter() -> str:
        return str(await DBOS.recv_async("topic", timeout_seconds=1, suspend=True))

    @DBOS.workflow()
    async def sleeper() -> None:
        await DBOS.sleep_async(1, suspend=True)

    for wf in (waiter, sleeper):
        handle = await DBOS.start_workflow_async(wf)
        with pytest.raises(DBOSException, match="requires suspend mode"):
            await handle.get_result()


def test_send_wakes_suspended_receivers_without_suspend_mode(
    dbos: DBOS, client: DBOSClient, skip_with_sharding: None
) -> None:
    """Parks workflows by hand, as dbos-enterprise's recv would: a send on the awaited
    topic, from the application or a client, makes the receiver due (delay 0), and a
    send on another topic leaves it parked."""
    assert dbos._sys_db.suspend is None
    sys_db = dbos._sys_db
    ws = sys_db.tables.workflow_status
    deadline_ms = 2**62

    @DBOS.workflow()
    def parked() -> None:
        pass

    def park(topic: str) -> str:
        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid):
            parked()
        with sys_db.engine.begin() as c:
            c.execute(
                sa.update(ws)
                .where(ws.c.workflow_uuid == wfid)
                .values(
                    status=WorkflowStatusString.SUSPENDED.value,
                    suspend_key=topic,
                    delay_until_epoch_ms=deadline_ms,
                )
            )
        return wfid

    def delay(wfid: str) -> int:
        with sys_db.engine.connect() as c:
            return int(
                c.execute(
                    sa.select(ws.c.delay_until_epoch_ms).where(
                        ws.c.workflow_uuid == wfid
                    )
                ).scalar_one()
            )

    by_app = park("approve")
    by_client = park("approve")
    DBOS.send(by_app, "no", topic="other")
    assert delay(by_app) == deadline_ms
    DBOS.send(by_app, "yes", topic="approve")
    assert delay(by_app) == 0
    client.send(by_client, "yes", topic="approve")
    assert delay(by_client) == 0


def test_parked_workflow_is_still_active(dbos: DBOS, skip_with_sharding: None) -> None:
    """Parks a workflow by hand on a queue: wait_first does not take it for complete, and
    queue listings still show it."""
    sys_db = dbos._sys_db
    ws = sys_db.tables.workflow_status

    @DBOS.workflow()
    def parked() -> None:
        pass

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        parked()
    with sys_db.engine.begin() as c:
        c.execute(
            sa.update(ws)
            .where(ws.c.workflow_uuid == wfid)
            .values(
                status=WorkflowStatusString.SUSPENDED.value,
                suspend_key="approve",
                delay_until_epoch_ms=2**62,
                queue_name="suspend-q",
            )
        )

    assert isinstance(sys_db.check_first_workflow_id([wfid]), NoResult)
    queued = DBOS.list_queued_workflows(queue_name="suspend-q")
    assert [(w.workflow_id, w.status) for w in queued] == [
        (wfid, WorkflowStatusString.SUSPENDED.value)
    ]
