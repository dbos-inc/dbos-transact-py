import datetime
import importlib
import sys
import time
import uuid
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec
from typing import Any, Optional

import pytest
import sqlalchemy as sa

# Public API
from dbos import DBOS, ConfigFile, SetWorkflowUUID, WorkflowHandle, WorkflowStatusString

# Private API because this is a test
from dbos.context import assert_current_dbos_context, get_local_dbos_context
from dbos.error import DBOSCommunicatorMaxRetriesExceededError
from dbos.system_database import GetWorkflowsInput


def test_simple_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    comm_counter: int = 0

    @dbos.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(var2)
        res2 = test_communicator(var)
        DBOS.logger.info("I'm test_workflow")
        return res + res2

    @dbos.transaction(isolation_level="REPEATABLE READ")
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        DBOS.logger.info("I'm test_transaction")
        return var2 + str(rows[0][0])

    @dbos.communicator()
    def test_communicator(var: str) -> str:
        nonlocal comm_counter
        comm_counter += 1
        DBOS.logger.info("I'm test_communicator")
        return var

    assert test_workflow("bob", "bob") == "bob1bob"

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with SetWorkflowUUID(wfuuid):
        assert test_workflow("alice", "alice") == "alice1alice"
    with SetWorkflowUUID(wfuuid):
        assert test_workflow("alice", "alice") == "alice1alice"
    assert txn_counter == 2  # Only increment once
    assert comm_counter == 2  # Only increment once

    # Test we can execute the workflow by uuid
    handle = dbos.execute_workflow_uuid(wfuuid)
    assert handle.get_result() == "alice1alice"
    assert wf_counter == 4


def test_child_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    comm_counter: int = 0

    @dbos.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        DBOS.logger.info("I'm test_transaction")
        return var2 + str(rows[0][0])

    @dbos.communicator()
    def test_communicator(var: str) -> str:
        nonlocal comm_counter
        comm_counter += 1
        DBOS.logger.info("I'm test_communicator")
        return var

    @dbos.workflow()
    def test_workflow(var: str, var2: str) -> str:
        DBOS.logger.info("I'm test_workflow")
        if len(DBOS.parent_workflow_id):
            DBOS.logger.info("  This is a child test_workflow")
            # Note this assertion is only true if child wasn't assigned an ID explicitly
            assert DBOS.workflow_id.startswith(DBOS.parent_workflow_id)
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(var2)
        res2 = test_communicator(var)
        return res + res2

    @dbos.workflow()
    def test_workflow_child() -> str:
        nonlocal wf_counter
        wf_counter += 1
        res1 = test_workflow("child1", "child1")
        return res1

    wf_ac_counter: int = 0
    txn_ac_counter: int = 0

    @dbos.workflow()
    def test_workflow_children() -> str:
        nonlocal wf_counter
        wf_counter += 1
        res1 = test_workflow("child1", "child1")
        wfh1 = dbos.start_workflow(test_workflow, "child2a", "child2a")
        wfh2 = dbos.start_workflow(test_workflow, "child2b", "child2b")
        res2 = wfh1.get_result()
        res3 = wfh2.get_result()
        return res1 + res2 + res3

    @dbos.transaction()
    def test_transaction_ac(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_ac_counter
        txn_ac_counter += 1
        return var2 + str(rows[0][0])

    @dbos.workflow()
    def test_workflow_ac(var: str, var2: str) -> str:
        DBOS.logger.info("I'm test_workflow assigned child id")
        assert DBOS.workflow_id == "run_me_just_once"
        res = test_transaction_ac(var2)
        return var + res

    @dbos.workflow()
    def test_workflow_assignchild() -> str:
        nonlocal wf_ac_counter
        wf_ac_counter += 1
        with SetWorkflowUUID("run_me_just_once"):
            res1 = test_workflow_ac("child1", "child1")
        with SetWorkflowUUID("run_me_just_once"):
            wfh = dbos.start_workflow(test_workflow_ac, "child1", "child1")
            res2 = wfh.get_result()
        return res1 + res2

    # Test child wf
    assert test_workflow_child() == "child11child1"
    assert test_workflow_children() == "child11child1child2a1child2achild2b1child2b"

    # Test child wf with assigned ID
    assert test_workflow_assignchild() == "child1child11child1child11"
    assert test_workflow_assignchild() == "child1child11child1child11"
    assert wf_ac_counter == 2
    assert txn_ac_counter == 1  # Only ran tx once


def test_exception_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    comm_counter: int = 0

    @dbos.transaction()
    def exception_transaction(var: str) -> str:
        nonlocal txn_counter
        txn_counter += 1
        raise Exception(var)

    @dbos.communicator()
    def exception_communicator(var: str) -> str:
        nonlocal comm_counter
        comm_counter += 1
        raise Exception(var)

    @dbos.workflow()
    def exception_workflow() -> None:
        nonlocal wf_counter
        wf_counter += 1
        err1 = None
        err2 = None
        try:
            exception_transaction("test error")
        except Exception as e:
            err1 = e

        try:
            exception_communicator("test error")
        except Exception as e:
            err2 = e
        assert err1 is not None and err2 is not None
        assert str(err1) == str(err2)
        raise err1

    with pytest.raises(Exception) as exc_info:
        exception_workflow()

    assert "test error" in str(exc_info.value)

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with pytest.raises(Exception) as exc_info:
        with SetWorkflowUUID(wfuuid):
            exception_workflow()
    assert "test error" == str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        with SetWorkflowUUID(wfuuid):
            exception_workflow()
    assert "test error" == str(exc_info.value)
    assert txn_counter == 2  # Only increment once
    assert comm_counter == 2  # Only increment once

    # Test we can execute the workflow by uuid, shouldn't throw errors
    handle = dbos.execute_workflow_uuid(wfuuid)
    with pytest.raises(Exception) as exc_info:
        handle.get_result()
    assert "test error" == str(exc_info.value)
    assert wf_counter == 4


def test_temp_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    comm_counter: int = 0

    cur_time: str = datetime.datetime.now().isoformat()
    gwi: GetWorkflowsInput = GetWorkflowsInput()
    gwi.start_time = cur_time

    @dbos.transaction(isolation_level="READ COMMITTED")
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    @dbos.communicator()
    def test_communicator(var: str) -> str:
        nonlocal comm_counter
        comm_counter += 1
        return var

    @dbos.communicator()
    def call_communicator(var: str) -> str:
        return test_communicator(var)

    assert get_local_dbos_context() is None
    res = test_transaction("var2")
    assert res == "var21"
    assert get_local_dbos_context() is None
    res = test_communicator("var")
    assert res == "var"

    wfs = dbos.sys_db.get_workflows(gwi)
    assert len(wfs.workflow_uuids) == 2

    wfi1 = dbos.sys_db.get_workflow_info(wfs.workflow_uuids[0], False)
    assert wfi1
    assert wfi1["name"].startswith("<temp>")

    wfi2 = dbos.sys_db.get_workflow_info(wfs.workflow_uuids[1], False)
    assert wfi2
    assert wfi2["name"].startswith("<temp>")

    assert txn_counter == 1
    assert comm_counter == 1

    res = call_communicator("var2")
    assert res == "var2"
    assert comm_counter == 2


def test_temp_workflow_errors(dbos: DBOS) -> None:
    txn_counter: int = 0
    comm_counter: int = 0
    retried_comm_counter: int = 0

    cur_time: str = datetime.datetime.now().isoformat()
    gwi: GetWorkflowsInput = GetWorkflowsInput()
    gwi.start_time = cur_time

    @dbos.transaction()
    def test_transaction(var2: str) -> str:
        nonlocal txn_counter
        txn_counter += 1
        raise Exception(var2)

    @dbos.communicator()
    def test_communicator(var: str) -> str:
        nonlocal comm_counter
        comm_counter += 1
        raise Exception(var)

    @dbos.communicator(retries_allowed=True)
    def test_retried_communicator(var: str) -> str:
        nonlocal retried_comm_counter
        retried_comm_counter += 1
        raise Exception(var)

    with pytest.raises(Exception) as exc_info:
        test_transaction("tval")
    assert "tval" == str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        test_communicator("cval")
    assert "cval" == str(exc_info.value)

    with pytest.raises(DBOSCommunicatorMaxRetriesExceededError) as exc_info:
        test_retried_communicator("rval")

    assert txn_counter == 1
    assert comm_counter == 1
    assert retried_comm_counter == 3


def test_recovery_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0

    @dbos.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(var2)
        return res + var

    @dbos.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    wfuuid = str(uuid.uuid4())
    with SetWorkflowUUID(wfuuid):
        assert test_workflow("bob", "bob") == "bob1bob"

    # Change the workflow status to pending
    dbos.sys_db.update_workflow_status(
        {
            "workflow_uuid": wfuuid,
            "status": "PENDING",
            "name": test_workflow.__qualname__,
            "class_name": None,
            "config_name": None,
            "output": None,
            "error": None,
            "executor_id": None,
            "app_id": None,
            "app_version": None,
            "request": None,
            "recovery_attempts": None,
        }
    )

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = dbos.recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == "bob1bob"
    assert wf_counter == 2
    assert txn_counter == 1

    # Test that there was a recovery attempt of this
    stat = workflow_handles[0].get_status()
    assert stat
    assert stat.recovery_attempts == 1


def test_recovery_temp_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0

    @dbos.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    cur_time: str = datetime.datetime.now().isoformat()
    gwi: GetWorkflowsInput = GetWorkflowsInput()
    gwi.start_time = cur_time

    wfuuid = str(uuid.uuid4())
    with SetWorkflowUUID(wfuuid):
        res = test_transaction("bob")
        assert res == "bob1"

    wfs = dbos.sys_db.get_workflows(gwi)
    assert len(wfs.workflow_uuids) == 1
    assert wfs.workflow_uuids[0] == wfuuid

    wfi = dbos.sys_db.get_workflow_info(wfs.workflow_uuids[0], False)
    assert wfi
    assert wfi["name"].startswith("<temp>")

    # Change the workflow status to pending
    dbos.sys_db.update_workflow_status(
        {
            "workflow_uuid": wfuuid,
            "status": "PENDING",
            "name": wfi["name"],
            "class_name": None,
            "config_name": None,
            "output": None,
            "error": None,
            "executor_id": None,
            "app_id": None,
            "app_version": None,
            "request": None,
            "recovery_attempts": None,
        }
    )

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = dbos.recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == "bob1"

    wfs = dbos.sys_db.get_workflows(gwi)
    assert len(wfs.workflow_uuids) == 1
    assert wfs.workflow_uuids[0] == wfuuid

    wfi = dbos.sys_db.get_workflow_info(wfs.workflow_uuids[0], False)
    assert wfi
    assert wfi["name"].startswith("<temp>")
    assert wfi["status"] == "SUCCESS"

    assert txn_counter == 1


def test_recovery_thread(config: ConfigFile, dbos: DBOS) -> None:
    wf_counter: int = 0
    test_var = "dbos"

    @dbos.workflow()
    def test_workflow(var: str) -> str:
        nonlocal wf_counter
        if var == test_var:
            wf_counter += 1
        return var

    wfuuid = str(uuid.uuid4())
    with SetWorkflowUUID(wfuuid):
        assert test_workflow(test_var) == test_var

    # Change the workflow status to pending
    dbos.sys_db.update_workflow_status(
        {
            "workflow_uuid": wfuuid,
            "status": "PENDING",
            "name": test_workflow.__qualname__,
            "class_name": None,
            "config_name": None,
            "output": None,
            "error": None,
            "executor_id": None,
            "app_id": None,
            "app_version": None,
            "request": None,
            "recovery_attempts": None,
        }
    )

    dbos.destroy()
    dbos.__init__(config=config)  # type: ignore

    @dbos.workflow()  # type: ignore
    def test_workflow(var: str) -> str:
        nonlocal wf_counter
        if var == test_var:
            wf_counter += 1
        return var

    # Upon re-initialization, the background thread should recover the workflow safely.
    max_retries = 5
    success = False
    for i in range(max_retries):
        try:
            assert wf_counter == 2
            success = True
        except AssertionError:
            time.sleep(1)
    assert success


def test_start_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0

    @dbos.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(var2)
        return res + var

    @dbos.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    wfuuid = str(uuid.uuid4())
    with SetWorkflowUUID(wfuuid):
        handle = dbos.start_workflow(test_workflow, "bob", "bob")
        context = assert_current_dbos_context()
        assert not context.is_within_workflow()
        assert handle.get_result() == "bob1bob"
    with SetWorkflowUUID(wfuuid):
        handle = dbos.start_workflow(test_workflow, "bob", "bob")
        context = assert_current_dbos_context()
        assert not context.is_within_workflow()
        assert handle.get_result() == "bob1bob"
    with SetWorkflowUUID(wfuuid):
        assert test_workflow("bob", "bob") == "bob1bob"
        context = assert_current_dbos_context()
        assert not context.is_within_workflow()
    assert txn_counter == 1
    assert wf_counter == 3


def test_retrieve_workflow(dbos: DBOS) -> None:
    @dbos.workflow()
    def test_sleep_workflow(secs: float) -> str:
        dbos.sleep(secs)
        return DBOS.workflow_id

    @dbos.workflow()
    def test_sleep_workthrow(secs: float) -> str:
        dbos.sleep(secs)
        raise Exception("Wake Up!")

    dest_uuid = "aaaa"
    with pytest.raises(Exception) as exc_info:
        dbos.retrieve_workflow(dest_uuid)
    pattern = f"Sent to non-existent destination workflow UUID: {dest_uuid}"
    assert pattern in str(exc_info.value)

    # These return
    sleep_wfh = dbos.start_workflow(test_sleep_workflow, 1.5)
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.PENDING.value)

    sleep_pwfh: WorkflowHandle[str] = dbos.retrieve_workflow(sleep_wfh.workflow_uuid)
    assert sleep_wfh.workflow_uuid == sleep_pwfh.workflow_uuid
    dbos.logger.info(f"UUID: {sleep_pwfh.get_workflow_uuid()}")
    hres = sleep_pwfh.get_result()
    assert hres == sleep_pwfh.get_workflow_uuid()
    dbos.logger.info(f"RES: {hres}")
    istat = sleep_pwfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.SUCCESS.value)

    assert sleep_wfh.get_result() == sleep_wfh.get_workflow_uuid()
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.SUCCESS.value)

    # These throw
    sleep_wfh = dbos.start_workflow(test_sleep_workthrow, 1.5)
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.PENDING.value)
    sleep_pwfh = dbos.retrieve_workflow(sleep_wfh.workflow_uuid)
    assert sleep_wfh.workflow_uuid == sleep_pwfh.workflow_uuid

    with pytest.raises(Exception) as exc_info:
        sleep_pwfh.get_result()
    assert str(exc_info.value) == "Wake Up!"
    istat = sleep_pwfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.ERROR.value)

    with pytest.raises(Exception) as exc_info:
        sleep_wfh.get_result()
    assert str(exc_info.value) == "Wake Up!"
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.ERROR.value)


def test_retrieve_workflow_in_workflow(dbos: DBOS) -> None:
    @dbos.workflow()
    def test_sleep_workflow(secs: float) -> str:
        dbos.sleep(secs)
        return DBOS.workflow_id

    @dbos.workflow()
    def test_workflow_status_a() -> str:
        with SetWorkflowUUID("run_this_once_a"):
            dbos.start_workflow(test_sleep_workflow, 1.5)

        fstat1 = dbos.get_workflow_status("run_this_once_a")
        assert fstat1
        fres: str = dbos.retrieve_workflow("run_this_once_a").get_result()
        fstat2 = dbos.get_workflow_status("run_this_once_a")
        assert fstat2
        return fstat1.status + fres + fstat2.status

    @dbos.workflow()
    def test_workflow_status_b() -> str:
        assert DBOS.workflow_id == "parent_b"
        with SetWorkflowUUID("run_this_once_b"):
            wfh = dbos.start_workflow(test_sleep_workflow, 1.5)
        assert DBOS.workflow_id == "parent_b"

        fstat1 = wfh.get_status()
        assert fstat1
        fres = wfh.get_result()
        fstat2 = wfh.get_status()
        assert fstat2
        return fstat1.status + fres + fstat2.status

    with SetWorkflowUUID("parent_a"):
        assert test_workflow_status_a() == "PENDINGrun_this_once_aSUCCESS"
    with SetWorkflowUUID("parent_a"):
        assert test_workflow_status_a() == "PENDINGrun_this_once_aSUCCESS"

    with SetWorkflowUUID("parent_b"):
        assert test_workflow_status_b() == "PENDINGrun_this_once_bSUCCESS"
    with SetWorkflowUUID("parent_b"):
        assert test_workflow_status_b() == "PENDINGrun_this_once_bSUCCESS"

    # Test that there were no recovery attempts of this
    stat = dbos.get_workflow_status("parent_a")
    assert stat
    assert stat.recovery_attempts == 0
    stat = dbos.get_workflow_status("parent_b")
    assert stat
    assert stat.recovery_attempts == 0
    stat = dbos.get_workflow_status("run_this_once_a")
    assert stat
    assert stat.recovery_attempts == 0
    stat = dbos.get_workflow_status("run_this_once_b")
    assert stat
    assert stat.recovery_attempts == 0


def test_without_fastapi(dbos: DBOS) -> None:
    """
    Since DBOS does not depend on FastAPI directly, verify DBOS works in an environment without FastAPI.
    """
    # Unimport FastAPI
    for module_name in list(sys.modules.keys()):
        if module_name == "fastapi" or module_name.startswith("fastapi."):
            del sys.modules[module_name]

    # Throw an error if FastAPI is imported
    class FastAPIBlocker(MetaPathFinder):
        def find_spec(
            self, fullname: str, path: Any = None, target: Any = None
        ) -> Optional[ModuleSpec]:
            if fullname == "fastapi" or fullname.startswith("fastapi."):
                raise ImportError(f"Illegal FastAPI import detected: {fullname}")
            return None

    blocker = FastAPIBlocker()
    sys.meta_path.insert(0, blocker)

    # Reload all DBOS modules, verifying none import FastAPI
    try:
        for module_name in dict(sys.modules.items()):
            module = sys.modules[module_name]
            if module_name == "dbos" or module_name.startswith("dbos."):
                importlib.reload(module)
    finally:
        sys.meta_path.remove(blocker)

    @dbos.workflow()
    def test_workflow(var: str) -> str:
        return var

    assert test_workflow("bob") == "bob"


def test_sleep(dbos: DBOS) -> None:
    @dbos.workflow()
    def test_sleep_workflow(secs: float) -> str:
        dbos.sleep(secs)
        return DBOS.workflow_id

    start_time = time.time()
    sleep_uuid = test_sleep_workflow(1.5)
    assert time.time() - start_time > 1.4

    # Test sleep OAOO, skip sleep
    start_time = time.time()
    with SetWorkflowUUID(sleep_uuid):
        assert test_sleep_workflow(1.5) == sleep_uuid
        assert time.time() - start_time < 0.3


def test_send_recv(dbos: DBOS) -> None:
    send_counter: int = 0
    recv_counter: int = 0

    @dbos.workflow()
    def test_send_workflow(dest_uuid: str, topic: str) -> str:
        dbos.send(dest_uuid, "test1")
        dbos.send(dest_uuid, "test2", topic=topic)
        dbos.send(dest_uuid, "test3")
        nonlocal send_counter
        send_counter += 1
        return dest_uuid

    @dbos.workflow()
    def test_recv_workflow(topic: str) -> str:
        msg1 = dbos.recv(topic, timeout_seconds=10)
        msg2 = dbos.recv(timeout_seconds=10)
        msg3 = dbos.recv(timeout_seconds=10)
        nonlocal recv_counter
        recv_counter += 1
        return "-".join([str(msg1), str(msg2), str(msg3)])

    @dbos.workflow()
    def test_recv_timeout(timeout_seconds: float) -> None:
        msg = dbos.recv(timeout_seconds=timeout_seconds)
        assert msg is None

    @dbos.workflow()
    def test_send_none(dest_uuid: str) -> None:
        dbos.send(dest_uuid, None)

    dest_uuid = str(uuid.uuid4())

    # Send to non-existent uuid should fail
    with pytest.raises(Exception) as exc_info:
        test_send_workflow(dest_uuid, "testtopic")
    assert f"Sent to non-existent destination workflow UUID: {dest_uuid}" in str(
        exc_info.value
    )

    with SetWorkflowUUID(dest_uuid):
        handle = dbos.start_workflow(test_recv_workflow, "testtopic")
        assert handle.get_workflow_uuid() == dest_uuid

    send_uuid = str(uuid.uuid4())
    with SetWorkflowUUID(send_uuid):
        res = test_send_workflow(handle.get_workflow_uuid(), "testtopic")
        assert res == dest_uuid
    begin_time = time.time()
    assert handle.get_result() == "test2-test1-test3"
    duration = time.time() - begin_time
    assert duration < 3.0  # Shouldn't take more than 3 seconds to run

    # Test send 'None'
    none_uuid = str(uuid.uuid4())
    none_handle = None
    with SetWorkflowUUID(none_uuid):
        none_handle = dbos.start_workflow(test_recv_timeout, 10.0)
    test_send_none(none_uuid)
    begin_time = time.time()
    assert none_handle.get_result() is None
    duration = time.time() - begin_time
    assert duration < 1.0  # None is from the received message, not from the timeout.

    timeout_uuid = str(uuid.uuid4())
    with SetWorkflowUUID(timeout_uuid):
        begin_time = time.time()
        timeoutres = test_recv_timeout(1.0)
        duration = time.time() - begin_time
        assert duration > 0.7
        assert timeoutres is None

    # Test OAOO
    with SetWorkflowUUID(send_uuid):
        res = test_send_workflow(handle.get_workflow_uuid(), "testtopic")
        assert res == dest_uuid
        assert send_counter == 2

    with SetWorkflowUUID(dest_uuid):
        begin_time = time.time()
        res = test_recv_workflow("testtopic")
        duration = time.time() - begin_time
        assert duration < 3.0
        assert res == "test2-test1-test3"
        assert recv_counter == 2

    with SetWorkflowUUID(timeout_uuid):
        begin_time = time.time()
        timeoutres = test_recv_timeout(1.0)
        duration = time.time() - begin_time
        assert duration < 0.3
        assert timeoutres is None

    # Test recv outside of a workflow
    with pytest.raises(Exception) as exc_info:
        dbos.recv("test1")
    assert "recv() must be called from within a workflow" in str(exc_info.value)


def test_send_recv_temp_wf(dbos: DBOS) -> None:
    recv_counter: int = 0
    cur_time: str = datetime.datetime.now().isoformat()
    gwi: GetWorkflowsInput = GetWorkflowsInput()
    gwi.start_time = cur_time

    @dbos.workflow()
    def test_send_recv_workflow(topic: str) -> str:
        msg1 = dbos.recv(topic, timeout_seconds=10)
        nonlocal recv_counter
        recv_counter += 1
        # TODO Set event back
        return "-".join([str(msg1)])

    dest_uuid = str(uuid.uuid4())

    with SetWorkflowUUID(dest_uuid):
        handle = dbos.start_workflow(test_send_recv_workflow, "testtopic")
        assert handle.get_workflow_uuid() == dest_uuid

    dbos.send(dest_uuid, "testsend1", "testtopic")
    assert handle.get_result() == "testsend1"

    wfs = dbos.sys_db.get_workflows(gwi)
    assert len(wfs.workflow_uuids) == 2
    assert wfs.workflow_uuids[1] == dest_uuid
    assert wfs.workflow_uuids[0] != dest_uuid

    wfi = dbos.sys_db.get_workflow_info(wfs.workflow_uuids[0], False)
    assert wfi
    assert wfi["name"] == "<temp>.temp_send_workflow"

    assert recv_counter == 1


def test_set_get_events(dbos: DBOS) -> None:
    @dbos.workflow()
    def test_setevent_workflow() -> None:
        dbos.set_event("key1", "value1")
        dbos.set_event("key2", "value2")
        dbos.set_event("key3", None)

    @dbos.workflow()
    def test_getevent_workflow(
        target_uuid: str, key: str, timeout_seconds: float = 10
    ) -> Optional[str]:
        msg = dbos.get_event(target_uuid, key, timeout_seconds)
        return str(msg) if msg is not None else None

    wfuuid = str(uuid.uuid4())
    with SetWorkflowUUID(wfuuid):
        test_setevent_workflow()
    with SetWorkflowUUID(wfuuid):
        test_setevent_workflow()

    value1 = test_getevent_workflow(wfuuid, "key1")
    assert value1 == "value1"

    value2 = test_getevent_workflow(wfuuid, "key2")
    assert value2 == "value2"

    # Run getEvent outside of a workflow
    value1 = dbos.get_event(wfuuid, "key1")
    assert value1 == "value1"

    value2 = dbos.get_event(wfuuid, "key2")
    assert value2 == "value2"

    begin_time = time.time()
    value3 = test_getevent_workflow(wfuuid, "key3")
    assert value3 is None
    duration = time.time() - begin_time
    assert duration < 1  # None is from the event not from the timeout

    # Test OAOO
    timeout_uuid = str(uuid.uuid4())
    with SetWorkflowUUID(timeout_uuid):
        begin_time = time.time()
        res = test_getevent_workflow("non-existent-uuid", "key1", 1.0)
        duration = time.time() - begin_time
        assert duration > 0.7
        assert res is None

    with SetWorkflowUUID(timeout_uuid):
        begin_time = time.time()
        res = test_getevent_workflow("non-existent-uuid", "key1", 1.0)
        duration = time.time() - begin_time
        assert duration < 0.3
        assert res is None

    # No OAOO for getEvent outside of a workflow
    begin_time = time.time()
    res = dbos.get_event("non-existent-uuid", "key1", 1.0)
    duration = time.time() - begin_time
    assert duration > 0.7
    assert res is None

    begin_time = time.time()
    res = dbos.get_event("non-existent-uuid", "key1", 1.0)
    duration = time.time() - begin_time
    assert duration > 0.7
    assert res is None

    # Test setEvent outside of a workflow
    with pytest.raises(Exception) as exc_info:
        dbos.set_event("key1", "value1")
    assert "set_event() must be called from within a workflow" in str(exc_info.value)
