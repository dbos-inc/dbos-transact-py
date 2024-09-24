import datetime
import threading
import time

import pytest
import sqlalchemy as sa
from psycopg.errors import SerializationFailure
from sqlalchemy.exc import OperationalError

# Public API
from dbos import DBOS, GetWorkflowsInput, SetWorkflowID
from dbos.error import DBOSDeadLetterQueueError, DBOSErrorCode, DBOSException
from dbos.system_database import WorkflowStatusString


def test_transaction_errors(dbos: DBOS) -> None:
    retry_counter: int = 0

    @DBOS.transaction()
    def test_retry_transaction(max_retry: int) -> int:
        nonlocal retry_counter
        if retry_counter < max_retry:
            retry_counter += 1
            raise OperationalError(
                "Serialization test error", {}, SerializationFailure()
            )
        return max_retry

    @DBOS.transaction()
    def test_noretry_transaction() -> None:
        nonlocal retry_counter
        retry_counter += 1
        DBOS.sql_session.execute(sa.text("selct abc from c;")).fetchall()

    res = test_retry_transaction(10)
    assert res == 10
    assert retry_counter == 10

    with pytest.raises(Exception) as exc_info:
        test_noretry_transaction()
    assert exc_info.value.orig.sqlstate == "42601"  # type: ignore
    assert retry_counter == 11


def test_notification_errors(dbos: DBOS) -> None:
    @DBOS.workflow()
    def test_send_workflow(dest_uuid: str, topic: str) -> str:
        DBOS.send(dest_uuid, "test1")
        DBOS.send(dest_uuid, "test2", topic=topic)
        DBOS.send(dest_uuid, "test3")
        return dest_uuid

    @DBOS.workflow()
    def test_recv_workflow(topic: str) -> str:
        msg1 = DBOS.recv(topic, timeout_seconds=10)
        msg2 = DBOS.recv(timeout_seconds=10)
        msg3 = DBOS.recv(timeout_seconds=10)
        return "-".join([str(msg1), str(msg2), str(msg3)])

    # Crash the notification connection and make sure send/recv works on time.
    while dbos._sys_db.notification_conn is None:
        time.sleep(1)
    dbos._sys_db.notification_conn.close()
    assert dbos._sys_db.notification_conn.closed == 1

    # Wait for the connection to be re-established
    while dbos._sys_db.notification_conn.closed != 0:
        time.sleep(1)

    dest_uuid = str("sruuid1")
    with SetWorkflowID(dest_uuid):
        handle = dbos.start_workflow(test_recv_workflow, "testtopic")
        assert handle.get_workflow_id() == dest_uuid

    send_uuid = str("sruuid2")
    with SetWorkflowID(send_uuid):
        res = test_send_workflow(handle.get_workflow_id(), "testtopic")
        assert res == dest_uuid

    begin_time = time.time()
    assert handle.get_result() == "test2-test1-test3"
    duration = time.time() - begin_time
    assert duration < 3.0


def test_buffer_flush_errors(dbos: DBOS) -> None:
    @DBOS.transaction()
    def test_transaction(var: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return var + str(rows[0][0])

    cur_time: str = datetime.datetime.now().isoformat()
    gwi: GetWorkflowsInput = GetWorkflowsInput()
    gwi.start_time = cur_time

    res = test_transaction("bob")
    assert res == "bob1"

    dbos._sys_db.wait_for_buffer_flush()
    wfs = dbos._sys_db.get_workflows(gwi)
    assert len(wfs.workflow_uuids) == 1

    # Crash the system database connection and make sure the buffer flush works on time.
    backup_engine = dbos._sys_db.engine
    dbos._sys_db.engine = sa.create_engine(
        "postgresql+psycopg://fake:database@localhost/fake_db"
    )

    res = test_transaction("bob")
    assert res == "bob1"

    # Should see some errors in the logs
    time.sleep(2)

    # Switch back to the original good engine.
    dbos._sys_db.engine = backup_engine

    dbos._sys_db.wait_for_buffer_flush()
    wfs = dbos._sys_db.get_workflows(gwi)
    assert len(wfs.workflow_uuids) == 2


def test_dead_letter_queue(dbos: DBOS) -> None:
    event = threading.Event()
    max_recovery_attempts = 20
    recovery_count = 0

    @DBOS.workflow(max_recovery_attempts=max_recovery_attempts)
    def dead_letter_workflow() -> None:
        nonlocal recovery_count
        recovery_count += 1
        event.wait()

    handle = DBOS.start_workflow(dead_letter_workflow)

    for i in range(max_recovery_attempts):
        DBOS.recover_pending_workflows()
        assert recovery_count == i + 2

    with pytest.raises(Exception) as exc_info:
        DBOS.recover_pending_workflows()
    assert exc_info.errisinstance(DBOSDeadLetterQueueError)
    assert handle.get_status().status == WorkflowStatusString.RETRIES_EXCEEDED.value

    with SetWorkflowID(handle.get_workflow_id()):
        DBOS.start_workflow(dead_letter_workflow)
    assert recovery_count == max_recovery_attempts + 2

    event.set()
    assert handle.get_result() == None
    dbos._sys_db.wait_for_buffer_flush()
    assert handle.get_status().status == WorkflowStatusString.SUCCESS.value
