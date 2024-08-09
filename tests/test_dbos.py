import time
import uuid

import pytest
import sqlalchemy as sa

from dbos_transact import DBOS, ConfigFile, SetWorkflowUUID


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
        assert err1 == err2 and err1 is not None
        raise err1

    with pytest.raises(Exception) as exc_info:
        exception_workflow()

    assert "test error" in str(exc_info.value)

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with pytest.raises(Exception) as exc_info:
        with SetWorkflowUUID(wfuuid):
            exception_workflow()
    assert "test error" in str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        with SetWorkflowUUID(wfuuid):
            exception_workflow()
    assert "test error" in str(exc_info.value)
    assert txn_counter == 2  # Only increment once
    assert comm_counter == 2  # Only increment once

    # Test we can execute the workflow by uuid, shouldn't throw errors
    handle = dbos.execute_workflow_uuid(wfuuid)
    with pytest.raises(Exception) as exc_info:
        handle.get_result()
    assert wf_counter == 4


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
            "output": None,
            "error": None,
            "executor_id": None,
            "app_id": None,
            "app_version": None,
        }
    )

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = dbos.recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == "bob1bob"
    assert wf_counter == 2
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
            "output": None,
            "error": None,
            "executor_id": None,
            "app_id": None,
            "app_version": None,
        }
    )

    dbos.destroy()
    dbos.__init__(config)  # type: ignore

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
        assert handle.get_result() == "bob1bob"
    with SetWorkflowUUID(wfuuid):
        handle = dbos.start_workflow(test_workflow, "bob", "bob")
        assert handle.get_result() == "bob1bob"
    with SetWorkflowUUID(wfuuid):
        assert test_workflow("bob", "bob") == "bob1bob"
    assert txn_counter == 1
    assert wf_counter == 3
