# mypy: disable-error-code="no-redef"

import datetime
import logging
import os
import threading
import time
import uuid
from typing import Optional

import pytest
import sqlalchemy as sa

# Public API
from dbos import (
    DBOS,
    DBOSConfig,
    Queue,
    SetWorkflowID,
    SetWorkflowTimeout,
    WorkflowHandle,
    WorkflowStatusString,
)

# Private API because this is a test
from dbos._context import assert_current_dbos_context, get_local_dbos_context
from dbos._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSConflictingRegistrationError,
    DBOSException,
)
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import GetWorkflowsInput
from dbos._utils import GlobalParams


def test_simple_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(var2)
        res2 = test_step(var)
        DBOS.logger.info("I'm test_workflow " + var + var2)
        return res + res2

    @DBOS.transaction(isolation_level="SERIALIZABLE")
    def test_transaction(var2: str) -> str:
        assert DBOS.step_id == 1
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        DBOS.logger.info("I'm test_transaction " + var2)
        return var2 + str(rows[0][0])

    @DBOS.step()
    def test_step(var: str) -> str:
        assert DBOS.step_id == 2
        step_status = DBOS.step_status
        assert step_status is not None
        assert step_status.step_id == 2
        assert step_status.current_attempt is None
        assert step_status.max_attempts is None
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step " + var)
        return var

    assert test_workflow("bob", "bob") == "bob1bob"

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow("alice", "alice") == "alice1alice"
    assert wf_counter == 2
    with SetWorkflowID(wfuuid):
        assert test_workflow("alice", "alice") == "alice1alice"
    assert txn_counter == 2  # Only increment once
    assert step_counter == 2  # Only increment once
    assert wf_counter == 2  # Only increment once

    # Test we can execute the workflow by uuid
    handle = DBOS._execute_workflow_id(wfuuid)
    assert handle.get_result() == "alice1alice"
    assert wf_counter == 2


def test_simple_workflow_attempts_counter(dbos: DBOS) -> None:
    @DBOS.workflow()
    def noop() -> None:
        time.sleep(2)

    wfuuid = str(uuid.uuid4())
    with dbos._sys_db.engine.connect() as c:
        stmt = sa.select(
            SystemSchema.workflow_status.c.recovery_attempts,
            SystemSchema.workflow_status.c.created_at,
            SystemSchema.workflow_status.c.updated_at,
        ).where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        for i in range(10):
            with SetWorkflowID(wfuuid):
                noop()
            result = c.execute(stmt).fetchone()
            assert result is not None
            recovery_attempts, created_at, updated_at = result
            assert recovery_attempts == i + 1
            assert updated_at >= created_at


def test_child_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        DBOS.logger.info("I'm test_transaction")
        return var2 + str(rows[0][0])

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        DBOS.logger.info("I'm test_workflow")
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(var2)
        res2 = test_step(var)
        return res + res2

    @DBOS.workflow()
    def test_workflow_child() -> str:
        nonlocal wf_counter
        wf_counter += 1
        res1 = test_workflow("child1", "child1")
        return res1

    wf_ac_counter: int = 0
    txn_ac_counter: int = 0

    @DBOS.workflow()
    def test_workflow_children() -> str:
        nonlocal wf_counter
        wf_counter += 1
        res1 = test_workflow("child1", "child1")
        wfh1 = dbos.start_workflow(test_workflow, "child2a", "child2a")
        wfh2 = dbos.start_workflow(test_workflow, "child2b", "child2b")
        res2 = wfh1.get_result()
        res3 = wfh2.get_result()
        return res1 + res2 + res3

    @DBOS.transaction()
    def test_transaction_ac(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_ac_counter
        txn_ac_counter += 1
        return var2 + str(rows[0][0])

    @DBOS.workflow()
    def test_workflow_ac(var: str, var2: str) -> str:
        DBOS.logger.info("I'm test_workflow assigned child id")
        assert DBOS.workflow_id == "run_me_just_once"
        res = test_transaction_ac(var2)
        return var + res

    @DBOS.workflow()
    def test_workflow_assignchild() -> str:
        nonlocal wf_ac_counter
        wf_ac_counter += 1
        with SetWorkflowID("run_me_just_once"):
            res1 = test_workflow_ac("child1", "child1")
        with SetWorkflowID("run_me_just_once"):
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
    step_counter: int = 0
    bad_txn_counter: int = 0

    @DBOS.transaction()
    def exception_transaction(var: str) -> str:
        nonlocal txn_counter
        txn_counter += 1
        raise Exception(var)

    @DBOS.transaction()
    def bad_transaction() -> None:
        nonlocal bad_txn_counter
        bad_txn_counter += 1
        # Make sure we record this error in the database
        DBOS.sql_session.execute(sa.text("selct abc from c;")).fetchall()

    @DBOS.step()
    def exception_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        raise Exception(var)

    @DBOS.workflow()
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
            exception_step("test error")
        except Exception as e:
            err2 = e
        assert err1 is not None and err2 is not None
        assert str(err1) == str(err2)

        try:
            bad_transaction()
        except Exception as e:
            assert "syntax error" in str(e)
        raise err1

    with pytest.raises(Exception) as exc_info:
        exception_workflow()

    assert "test error" in str(exc_info.value)

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with pytest.raises(Exception) as exc_info:
        with SetWorkflowID(wfuuid):
            exception_workflow()
    assert "test error" == str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        with SetWorkflowID(wfuuid):
            exception_workflow()
    assert "test error" == str(exc_info.value)
    assert txn_counter == 2  # Only increment once
    assert step_counter == 2  # Only increment once
    assert bad_txn_counter == 2  # Only increment once

    # Test we can execute the workflow by uuid, shouldn't throw errors
    handle = DBOS._execute_workflow_id(wfuuid)
    with pytest.raises(Exception) as exc_info:
        handle.get_result()
    assert "test error" == str(exc_info.value)
    assert wf_counter == 2  # The workflow error is directly returned without running


def test_temp_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    step_counter: int = 0

    @DBOS.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return var

    @DBOS.step()
    def call_step(var: str) -> str:
        return test_step(var)

    assert get_local_dbos_context() is None
    res = test_transaction("var2")
    assert res == "var21"
    assert get_local_dbos_context() is None
    res = test_step("var")
    assert res == "var"

    wfs = DBOS.list_workflows()
    assert len(wfs) == 1

    wfi1 = dbos._sys_db.get_workflow_status(wfs[0].workflow_id)
    assert wfi1
    assert wfi1["name"].startswith("<temp>")

    assert txn_counter == 1
    assert step_counter == 1

    res = call_step("var2")
    assert res == "var2"
    assert step_counter == 2


def test_temp_workflow_errors(dbos: DBOS) -> None:
    txn_counter: int = 0
    step_counter: int = 0
    retried_step_counter: int = 0

    cur_time: str = datetime.datetime.now().isoformat()
    gwi: GetWorkflowsInput = GetWorkflowsInput()
    gwi.start_time = cur_time

    @DBOS.transaction()
    def test_transaction(var2: str) -> str:
        nonlocal txn_counter
        txn_counter += 1
        raise Exception(var2)

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        raise Exception(var)

    @DBOS.step(retries_allowed=True)
    def test_retried_step(var: str) -> str:
        nonlocal retried_step_counter
        retried_step_counter += 1
        raise ValueError(var)

    with pytest.raises(Exception) as exc_info:
        test_transaction("tval")
    assert "tval" == str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        test_step("cval")
    assert "cval" == str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        test_retried_step("rval")

    assert txn_counter == 1
    assert step_counter == 1
    assert retried_step_counter == 1


def test_recovery_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    txn_return_none_counter: int = 0
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(var2)
        should_be_none = test_transaction_return_none()
        assert should_be_none is None
        return res + var

    @DBOS.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    @DBOS.transaction()
    def test_transaction_return_none() -> None:
        nonlocal txn_return_none_counter
        DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        txn_return_none_counter += 1
        return

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") == "bob1bob"

    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": test_workflow.__qualname__})
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        )

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = DBOS._recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == "bob1bob"
    assert wf_counter == 2
    assert txn_counter == 1
    assert txn_return_none_counter == 1

    # Test that there was a recovery attempt of this
    stat = workflow_handles[0].get_status()
    assert stat
    assert stat.recovery_attempts == 2  # original attempt + recovery attempt


def test_recovery_workflow_step(dbos: DBOS) -> None:
    step_counter: int = 0
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        should_be_none = test_step(var2)
        assert should_be_none is None
        return var

    @DBOS.step()
    def test_step(var2: str) -> None:
        nonlocal step_counter
        step_counter += 1
        print(f"I'm a test_step {var2}!")
        return

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") == "bob"

    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": test_workflow.__qualname__})
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        )

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = DBOS._recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == "bob"
    assert wf_counter == 2
    assert step_counter == 1

    # Test that there was a recovery attempt of this
    stat = workflow_handles[0].get_status()
    assert stat
    assert stat.recovery_attempts == 2


def test_workflow_returns_none(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> None:
        nonlocal wf_counter
        wf_counter += 1
        assert var == var2 == "bob"
        return

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") is None
    assert wf_counter == 1

    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") is None
    assert wf_counter == 1

    handle: WorkflowHandle[None] = DBOS.retrieve_workflow(wfuuid)
    assert handle.get_result() == None
    assert wf_counter == 1

    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": test_workflow.__qualname__})
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        )

    workflow_handles = DBOS._recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() is None
    assert wf_counter == 2

    # Test that there was a recovery attempt of this
    stat = workflow_handles[0].get_status()
    assert stat
    assert stat.recovery_attempts == 3  # 2 calls to test_workflow + 1 recovery attempt


def test_recovery_temp_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0

    @DBOS.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        res = test_transaction("bob")
        assert res == "bob1"

    wfs = DBOS.list_workflows()
    assert len(wfs) == 1
    assert wfs[0].workflow_id == wfuuid

    wfi = dbos._sys_db.get_workflow_status(wfs[0].workflow_id)
    assert wfi
    assert wfi["name"].startswith("<temp>")

    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": wfi["name"]})
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        )

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = DBOS._recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == "bob1"

    wfs = DBOS.list_workflows()
    assert len(wfs) == 1
    assert wfs[0].workflow_id == wfuuid

    wfi = dbos._sys_db.get_workflow_status(wfs[0].workflow_id)
    assert wfi
    assert wfi["name"].startswith("<temp>")
    assert wfi["status"] == "SUCCESS"

    assert txn_counter == 1


def test_recovery_thread(config: DBOSConfig) -> None:
    wf_counter: int = 0
    test_var = "dbos"

    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def test_workflow(var: str) -> str:
        nonlocal wf_counter
        if var == test_var:
            wf_counter += 1
        return var

    DBOS.launch()

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow(test_var) == test_var

    # Change the workflow status to pending
    dbos._sys_db.update_workflow_outcome(wfuuid, "PENDING")

    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    @DBOS.workflow()
    def test_workflow(var: str) -> str:
        nonlocal wf_counter
        if var == test_var:
            wf_counter += 1
        return var

    DBOS.launch()

    # Upon re-launch, the background thread should recover the workflow safely.
    max_retries = 10
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

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(var2)
        return res + var

    @DBOS.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        handle = dbos.start_workflow(test_workflow, "bob", "bob")
        context = assert_current_dbos_context()
        assert not context.is_within_workflow()
        assert handle.get_result() == "bob1bob"
    with SetWorkflowID(wfuuid):
        handle = dbos.start_workflow(test_workflow, "bob", "bob")
        context = assert_current_dbos_context()
        assert not context.is_within_workflow()
        assert handle.get_result() == "bob1bob"
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") == "bob1bob"
        context = assert_current_dbos_context()
        assert not context.is_within_workflow()
    assert txn_counter == 1
    assert wf_counter == 1


def test_retrieve_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def test_sleep_workflow(secs: float) -> str:
        dbos.sleep(secs)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    @DBOS.workflow()
    def test_sleep_workthrow(secs: float) -> str:
        dbos.sleep(secs)
        raise Exception("Wake Up!")

    @DBOS.workflow()
    def test_workflow(x: int) -> int:
        return x

    dest_uuid = "aaaa"
    with pytest.raises(Exception) as exc_info:
        dbos.retrieve_workflow(dest_uuid)
    pattern = f"Sent to non-existent destination workflow ID: {dest_uuid}"
    assert pattern in str(exc_info.value)

    # These return
    sleep_wfh = dbos.start_workflow(test_sleep_workflow, 1.5)
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.PENDING.value)

    sleep_pwfh: WorkflowHandle[str] = dbos.retrieve_workflow(sleep_wfh.workflow_id)
    assert sleep_wfh.workflow_id == sleep_pwfh.workflow_id
    dbos.logger.info(f"UUID: {sleep_pwfh.get_workflow_id()}")
    hres = sleep_pwfh.get_result()
    assert hres == sleep_pwfh.get_workflow_id()
    dbos.logger.info(f"RES: {hres}")
    istat = sleep_pwfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.SUCCESS.value)

    assert sleep_wfh.get_result() == sleep_wfh.get_workflow_id()
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.SUCCESS.value)
    assert istat.output == sleep_wfh.get_workflow_id()

    # These throw
    sleep_wfh = dbos.start_workflow(test_sleep_workthrow, 1.5)
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.PENDING.value)
    sleep_pwfh = dbos.retrieve_workflow(sleep_wfh.workflow_id)
    assert sleep_wfh.workflow_id == sleep_pwfh.workflow_id

    with pytest.raises(Exception) as exc_info:
        sleep_pwfh.get_result()
    assert str(exc_info.value) == "Wake Up!"
    istat = sleep_pwfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.ERROR.value)
    assert istat.output is None
    assert istat.error is not None
    assert isinstance(istat.error, Exception)
    assert str(istat.error) == "Wake Up!"

    with pytest.raises(Exception) as exc_info:
        sleep_wfh.get_result()
    assert str(exc_info.value) == "Wake Up!"
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.ERROR.value)

    # Validate the status properly stores the output of the workflow
    x = 5
    handle = DBOS.start_workflow(test_workflow, x)
    assert handle.get_result() == x
    retrieved_handle: WorkflowHandle[int] = DBOS.retrieve_workflow(
        handle.get_workflow_id()
    )
    assert retrieved_handle.get_result() == x
    assert retrieved_handle.get_status().output == x
    assert retrieved_handle.get_status().error is None


def test_retrieve_workflow_in_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def test_sleep_workflow(secs: float) -> str:
        dbos.sleep(secs)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    @DBOS.workflow()
    def test_workflow_status_a() -> str:
        with SetWorkflowID("run_this_once_a"):
            dbos.start_workflow(test_sleep_workflow, 1.5)

        fstat1 = dbos.get_workflow_status("run_this_once_a")
        assert fstat1
        fres: str = dbos.retrieve_workflow("run_this_once_a").get_result()
        fstat2 = dbos.get_workflow_status("run_this_once_a")
        assert fstat2
        return fstat1.status + fres + fstat2.status

    @DBOS.workflow()
    def test_workflow_status_b() -> str:
        assert DBOS.workflow_id == "parent_b"
        with SetWorkflowID("run_this_once_b"):
            wfh = dbos.start_workflow(test_sleep_workflow, 1.5)
        assert DBOS.workflow_id == "parent_b"

        fstat1 = wfh.get_status()
        assert fstat1
        fres = wfh.get_result()
        fstat2 = wfh.get_status()
        assert fstat2
        return fstat1.status + fres + fstat2.status

    with SetWorkflowID("parent_a"):
        assert test_workflow_status_a() == "PENDINGrun_this_once_aSUCCESS"
    with SetWorkflowID("parent_a"):
        assert test_workflow_status_a() == "PENDINGrun_this_once_aSUCCESS"

    with SetWorkflowID("parent_b"):
        assert test_workflow_status_b() == "PENDINGrun_this_once_bSUCCESS"
    with SetWorkflowID("parent_b"):
        assert test_workflow_status_b() == "PENDINGrun_this_once_bSUCCESS"

    # Test that the number of attempts matches the number of calls
    stat = dbos.get_workflow_status("parent_a")
    assert stat
    assert stat.recovery_attempts == 2
    stat = dbos.get_workflow_status("parent_b")
    assert stat
    assert stat.recovery_attempts == 2
    stat = dbos.get_workflow_status("run_this_once_a")
    assert stat
    assert (
        stat.recovery_attempts == 1
    )  # because we now return based on parent child reln
    stat = dbos.get_workflow_status("run_this_once_b")
    assert stat
    assert (
        stat.recovery_attempts == 1
    )  # because we now return based on parent child reln


def test_sleep(dbos: DBOS) -> None:
    @DBOS.workflow()
    def test_sleep_workflow(secs: float) -> str:
        dbos.sleep(secs)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    start_time = time.time()
    sleep_uuid = test_sleep_workflow(1.5)
    assert time.time() - start_time > 1.4

    # Test sleep OAOO, skip sleep
    start_time = time.time()
    with SetWorkflowID(sleep_uuid):
        assert test_sleep_workflow(1.5) == sleep_uuid
        assert time.time() - start_time < 0.3


def test_send_recv(dbos: DBOS) -> None:
    send_counter: int = 0
    recv_counter: int = 0

    @DBOS.workflow()
    def test_send_workflow(dest_uuid: str, topic: str) -> str:
        dbos.send(dest_uuid, "test1")
        dbos.send(dest_uuid, "test2", topic=topic)
        dbos.send(dest_uuid, "test3")
        nonlocal send_counter
        send_counter += 1
        return dest_uuid

    @DBOS.workflow()
    def test_recv_workflow(topic: str) -> str:
        msg1 = dbos.recv(topic, timeout_seconds=10)
        msg2 = dbos.recv(timeout_seconds=10)
        msg3 = dbos.recv(timeout_seconds=10)
        nonlocal recv_counter
        recv_counter += 1
        return "-".join([str(msg1), str(msg2), str(msg3)])

    @DBOS.workflow()
    def test_recv_timeout(timeout_seconds: float) -> None:
        msg = dbos.recv(timeout_seconds=timeout_seconds)
        assert msg is None

    @DBOS.workflow()
    def test_send_none(dest_uuid: str) -> None:
        dbos.send(dest_uuid, None)

    dest_uuid = str(uuid.uuid4())

    # Send to non-existent uuid should fail
    with pytest.raises(Exception) as exc_info:
        test_send_workflow(dest_uuid, "testtopic")
    assert f"Sent to non-existent destination workflow ID: {dest_uuid}" in str(
        exc_info.value
    )

    with SetWorkflowID(dest_uuid):
        handle = dbos.start_workflow(test_recv_workflow, "testtopic")
        assert handle.get_workflow_id() == dest_uuid

    send_uuid = str(uuid.uuid4())
    with SetWorkflowID(send_uuid):
        res = test_send_workflow(handle.get_workflow_id(), "testtopic")
        assert res == dest_uuid
    begin_time = time.time()
    assert handle.get_result() == "test2-test1-test3"
    duration = time.time() - begin_time
    assert duration < 3.0  # Shouldn't take more than 3 seconds to run

    # Test send 'None'
    none_uuid = str(uuid.uuid4())
    none_handle = None
    with SetWorkflowID(none_uuid):
        none_handle = dbos.start_workflow(test_recv_timeout, 10.0)
    test_send_none(none_uuid)
    begin_time = time.time()
    assert none_handle.get_result() is None
    duration = time.time() - begin_time
    assert duration < 1.0  # None is from the received message, not from the timeout.

    timeout_uuid = str(uuid.uuid4())
    with SetWorkflowID(timeout_uuid):
        begin_time = time.time()
        timeoutres = test_recv_timeout(1.0)
        duration = time.time() - begin_time
        assert duration > 0.7
        assert timeoutres is None

    # Test OAOO
    with SetWorkflowID(send_uuid):
        res = test_send_workflow(handle.get_workflow_id(), "testtopic")
        assert res == dest_uuid
        assert send_counter == 1

    with SetWorkflowID(dest_uuid):
        begin_time = time.time()
        res = test_recv_workflow("testtopic")
        duration = time.time() - begin_time
        assert duration < 3.0
        assert res == "test2-test1-test3"
        assert recv_counter == 1

    with SetWorkflowID(timeout_uuid):
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
    gwi: GetWorkflowsInput = GetWorkflowsInput()

    @DBOS.workflow()
    def test_send_recv_workflow(topic: str) -> str:
        msg1 = dbos.recv(topic, timeout_seconds=10)
        nonlocal recv_counter
        recv_counter += 1
        # TODO Set event back
        return "-".join([str(msg1)])

    dest_uuid = str(uuid.uuid4())

    with SetWorkflowID(dest_uuid):
        handle = dbos.start_workflow(test_send_recv_workflow, "testtopic")
        assert handle.get_workflow_id() == dest_uuid

    dbos.send(dest_uuid, "testsend1", "testtopic")
    assert handle.get_result() == "testsend1"

    wfs = dbos._sys_db.get_workflows(gwi)
    assert len(wfs) == 2
    assert wfs[0].workflow_id == dest_uuid
    assert wfs[1].workflow_id != dest_uuid

    wfi = dbos._sys_db.get_workflow_status(wfs[1].workflow_id)
    assert wfi
    assert wfi["name"] == "<temp>.temp_send_workflow"

    assert recv_counter == 1

    # Test substring search
    gwi.start_time = None
    gwi.workflow_id_prefix = dest_uuid
    wfs = dbos._sys_db.get_workflows(gwi)
    assert wfs[0].workflow_id == dest_uuid

    gwi.workflow_id_prefix = dest_uuid[0:10]
    wfs = dbos._sys_db.get_workflows(gwi)
    assert dest_uuid in [w.workflow_id for w in wfs]

    gwi.workflow_id_prefix = dest_uuid[0:10]
    wfs = dbos._sys_db.get_workflows(gwi)
    assert dest_uuid in [w.workflow_id for w in wfs]

    x = dbos.list_workflows(
        start_time=datetime.datetime.now().isoformat(),
        workflow_id_prefix=dest_uuid[0:10],
    )
    assert len(x) == 0

    x = dbos.list_workflows(workflow_id_prefix=dest_uuid[0:10])
    assert len(x) >= 1
    assert dest_uuid in [w.workflow_id for w in x]

    x = dbos.list_workflows(workflow_id_prefix=dest_uuid + "thisdoesnotexist")
    assert len(x) == 0

    x = dbos.list_workflows(workflow_id_prefix="1" + dest_uuid)
    assert len(x) == 0


def test_set_get_events(dbos: DBOS) -> None:
    @DBOS.workflow()
    def test_setevent_workflow() -> None:
        dbos.set_event("key1", "value1")
        dbos.set_event("key2", "value2")
        dbos.set_event("key3", None)

    @DBOS.workflow()
    def test_getevent_workflow(
        target_uuid: str, key: str, timeout_seconds: float = 10
    ) -> Optional[str]:
        msg = dbos.get_event(target_uuid, key, timeout_seconds)
        return str(msg) if msg is not None else None

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        test_setevent_workflow()
    with SetWorkflowID(wfuuid):
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
    with SetWorkflowID(timeout_uuid):
        begin_time = time.time()
        res = test_getevent_workflow("non-existent-uuid", "key1", 1.0)
        duration = time.time() - begin_time
        assert duration > 0.7
        assert res is None

    with SetWorkflowID(timeout_uuid):
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


def test_nonserializable_values(dbos: DBOS) -> None:
    def invalid_return() -> str:
        return "literal"

    @DBOS.transaction()
    def test_ns_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return invalid_return  #  type: ignore

    @DBOS.step()
    def test_ns_step(var: str) -> str:
        return invalid_return  #  type: ignore

    @DBOS.workflow()
    def test_ns_wf(var: str) -> str:
        return invalid_return  #  type: ignore

    @DBOS.transaction()
    def test_reg_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return var2

    @DBOS.step()
    def test_reg_step(var: str) -> str:
        return var

    @DBOS.workflow()
    def test_reg_wf(var: str) -> str:
        return test_reg_step(var) + test_reg_transaction(var)

    @DBOS.workflow()
    def test_ns_event(var: str) -> str:
        DBOS.set_event("aaa", invalid_return)
        return test_reg_step(var) + test_reg_transaction(var)

    @DBOS.workflow()
    def test_bad_wf1(var: str) -> str:
        return test_reg_step(invalid_return) + test_reg_transaction(var)  # type: ignore

    @DBOS.workflow()
    def test_bad_wf2(var: str) -> str:
        return test_reg_step(var) + test_reg_transaction(invalid_return)  # type: ignore

    @DBOS.workflow()
    def test_bad_wf3(var: str) -> str:
        return test_ns_transaction(var)

    @DBOS.workflow()
    def test_bad_wf4(var: str) -> str:
        return test_ns_step(var)

    with pytest.raises(Exception) as exc_info:
        test_ns_transaction("h")
    assert "data item should not be a function" in str(exc_info.value)
    with pytest.raises(Exception) as exc_info:
        test_ns_wf("g")
    assert "data item should not be a function" in str(exc_info.value)

    wfh = DBOS.start_workflow(test_reg_wf, "a")
    with pytest.raises(Exception) as exc_info:
        DBOS.send(wfh.workflow_id, invalid_return, "sss")
    assert "data item should not be a function" in str(exc_info.value)
    wfh.get_result()

    with pytest.raises(Exception) as exc_info:
        test_ns_event("e")
    assert "data item should not be a function" in str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        test_bad_wf1("a")
    assert "data item should not be a function" in str(exc_info.value)
    with pytest.raises(Exception) as exc_info:
        test_bad_wf2("b")
    assert "data item should not be a function" in str(exc_info.value)
    with pytest.raises(Exception) as exc_info:
        test_bad_wf3("c")
    assert "data item should not be a function" in str(exc_info.value)
    with pytest.raises(Exception) as exc_info:
        test_bad_wf4("d")
    assert "data item should not be a function" in str(exc_info.value)


def test_multi_set_event(dbos: DBOS) -> None:
    event = threading.Event()

    wfid = str(uuid.uuid4())

    @DBOS.workflow()
    def test_setevent_workflow() -> None:
        assert DBOS.workflow_id == wfid
        DBOS.set_event("key", "value1")
        event.wait()
        DBOS.set_event("key", "value2")

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(test_setevent_workflow)
    assert DBOS.get_event(wfid, "key") == "value1"
    event.set()
    assert handle.get_result() == None
    assert DBOS.get_event(wfid, "key") == "value2"


def test_debug_logging(
    dbos: DBOS, caplog: pytest.LogCaptureFixture, config: DBOSConfig
) -> None:
    wfid = str(uuid.uuid4())
    dest_wfid = str(uuid.uuid4())

    @DBOS.step()
    def step_function(message: str) -> str:
        return f"Step: {message}"

    @DBOS.transaction()
    def transaction_function(message: str) -> str:
        return f"Transaction: {message}"

    @DBOS.workflow()
    def test_workflow() -> str:
        dbos.set_event("test_event", "event_value")
        step_result = step_function("Hello")
        transaction_result = transaction_function("World")
        dbos.send(dest_wfid, "test_message", topic="test_topic")
        dbos.sleep(1)
        return ", ".join([step_result, transaction_result])

    @DBOS.workflow()
    def test_workflow_dest() -> str:
        event_value = dbos.get_event(wfid, "test_event")
        msg_value = dbos.recv(topic="test_topic")
        return ", ".join([event_value, msg_value])

    original_propagate = logging.getLogger("dbos").propagate
    caplog.set_level(logging.DEBUG, "dbos")
    logging.getLogger("dbos").propagate = True

    # First run
    with SetWorkflowID(dest_wfid):
        dest_handle = dbos.start_workflow(test_workflow_dest)

    with SetWorkflowID(wfid):
        result1 = test_workflow()

    assert result1 == "Step: Hello, Transaction: World"
    assert (
        "Running step" in caplog.text
        and f"name: {step_function.__qualname__}" in caplog.text
    )
    assert (
        "Running transaction" in caplog.text
        and f"name: {transaction_function.__qualname__}" in caplog.text
    )
    assert "Running sleep" in caplog.text
    assert "Running set_event" in caplog.text
    assert "Running send" in caplog.text

    result2 = dest_handle.get_result()
    assert result2 == "event_value, test_message"
    assert "Running get_event" in caplog.text
    assert "Running recv" in caplog.text
    caplog.clear()

    # Second run
    with SetWorkflowID(dest_wfid):
        dest_handle_2 = dbos.start_workflow(test_workflow_dest)

    with SetWorkflowID(wfid):
        result3 = test_workflow()

    assert result3 == result1
    assert "is already completed with status SUCCESS" in caplog.text

    result4 = dest_handle_2.get_result()
    assert result4 == result2
    caplog.clear()

    # Debug mode run
    DBOS.destroy()
    DBOS(config=config)
    logging.getLogger("dbos").propagate = True
    caplog.set_level(logging.DEBUG, "dbos")
    DBOS.launch(debug_mode=True)

    with SetWorkflowID(dest_wfid):
        dest_handle_2 = dbos.start_workflow(test_workflow_dest)

    with SetWorkflowID(wfid):
        result3 = test_workflow()

    assert result3 == result1
    assert (
        "Replaying step" in caplog.text
        and f"name: {step_function.__qualname__}" in caplog.text
    )
    assert (
        "Replaying transaction" in caplog.text
        and f"name: {transaction_function.__qualname__}" in caplog.text in caplog.text
    )
    assert "Replaying sleep" in caplog.text
    assert "Replaying set_event" in caplog.text
    assert "Replaying send" in caplog.text

    result4 = dest_handle_2.get_result()
    assert result4 == result2

    # Reset logging
    logging.getLogger("dbos").propagate = original_propagate


def test_destroy_semantics(dbos: DBOS, config: DBOSConfig) -> None:

    @DBOS.workflow()
    def test_workflow(var: str) -> str:
        return var

    var = "test"
    assert test_workflow(var) == var

    DBOS.destroy()
    DBOS(config=config)
    DBOS.launch()

    assert test_workflow(var) == var


def test_double_decoration(dbos: DBOS) -> None:
    with pytest.raises(
        DBOSConflictingRegistrationError,
        match="is already registered with a conflicting function type",
    ):

        @DBOS.step()
        @DBOS.transaction()
        def my_function() -> None:
            pass

        my_function()


def test_duplicate_registration(
    dbos: DBOS, caplog: pytest.LogCaptureFixture, config: DBOSConfig
) -> None:
    original_propagate = logging.getLogger("dbos").propagate
    caplog.set_level(logging.WARNING, "dbos")
    logging.getLogger("dbos").propagate = True

    @DBOS.transaction()
    def my_transaction() -> None:
        pass

    @DBOS.transaction()
    def my_transaction() -> None:
        pass

    assert (
        "Duplicate registration of function 'test_duplicate_registration.<locals>.my_transaction'"
        in caplog.text
    )

    @DBOS.step()
    def my_step() -> None:
        pass

    @DBOS.step()
    def my_step() -> None:
        pass

    assert (
        "Duplicate registration of function 'test_duplicate_registration.<locals>.my_step'"
        in caplog.text
    )

    @DBOS.workflow()
    def my_workflow() -> None:
        my_step()
        my_transaction()

    @DBOS.workflow()
    def my_workflow() -> None:
        my_step()
        my_transaction()

    assert (
        "Duplicate registration of function 'test_duplicate_registration.<locals>.my_workflow'"
        in caplog.text
    )

    DBOS.destroy()
    DBOS(config=config)
    DBOS.launch()
    assert "Duplicate registration of function 'temp_send_workflow'" not in caplog.text

    # Reset logging
    logging.getLogger("dbos").propagate = original_propagate


def test_app_version(config: DBOSConfig) -> None:
    def is_hex(s: str) -> bool:
        return all(c in "0123456789abcdefABCDEF" for c in s)

    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def workflow_one(x: int) -> int:
        return x

    @DBOS.workflow()
    def workflow_two(y: int) -> int:
        return y

    DBOS.launch()

    # Verify that app version is correctly set to a hex string
    app_version = GlobalParams.app_version
    assert len(app_version) > 0
    assert is_hex(app_version)

    DBOS.destroy(destroy_registry=True)
    assert GlobalParams.app_version == ""
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def workflow_one(x: int) -> int:
        return x

    @DBOS.workflow()
    def workflow_two(y: int) -> int:
        return y

    DBOS.launch()

    # Verify stability--the same workflow source produces the same app version.
    assert GlobalParams.app_version == app_version

    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def workflow_one(x: int) -> int:
        return x

    # Verify that changing the workflow source changes the workflow version
    DBOS.launch()
    assert GlobalParams.app_version != app_version

    # Verify that version can be overriden with an environment variable
    app_version = str(uuid.uuid4())
    os.environ["DBOS__APPVERSION"] = app_version

    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def workflow_one(x: int) -> int:
        return x

    DBOS.launch()
    assert GlobalParams.app_version == app_version

    del os.environ["DBOS__APPVERSION"]

    # Verify that version and executor ID can be overriden with a config parameter
    app_version = str(uuid.uuid4())
    executor_id = str(uuid.uuid4())

    DBOS.destroy(destroy_registry=True)
    config["application_version"] = app_version
    config["executor_id"] = executor_id
    DBOS(config=config)

    @DBOS.workflow()
    def test_workflow() -> str:
        assert DBOS.workflow_id
        return DBOS.workflow_id

    DBOS.launch()
    assert GlobalParams.app_version == app_version
    assert GlobalParams.executor_id == executor_id
    wfid = test_workflow()
    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    assert handle.get_status().app_version == app_version
    assert handle.get_status().executor_id == executor_id


def test_recovery_appversion(config: DBOSConfig) -> None:
    input = 5
    os.environ["DBOS__VMID"] = "testexecutor"

    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def test_workflow(x: int) -> int:
        return x

    DBOS.launch()

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow(input) == input

    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": test_workflow.__qualname__})
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        )

    # Reconstruct an identical environment to simulate a restart
    os.environ["DBOS__VMID"] = "testexecutor_another"
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def test_workflow(x: int) -> int:
        return x

    DBOS.launch()

    # The workflow should successfully recover
    workflow_handles = DBOS._recover_pending_workflows(["testexecutor"])
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == input

    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": test_workflow.__qualname__})
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        )

    # Now reconstruct a "modified application" with a different application version
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def test_workflow(x: int) -> int:
        return x + 1

    DBOS.launch()

    # The workflow should not recover
    workflow_handles = DBOS._recover_pending_workflows(["testexecutor"])
    assert len(workflow_handles) == 0

    # Clean up the environment variable
    del os.environ["DBOS__VMID"]


def test_workflow_timeout(dbos: DBOS) -> None:

    @DBOS.workflow()
    def blocked_workflow() -> None:
        assert assert_current_dbos_context().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is not None
        while True:
            DBOS.sleep(0.1)

    # Verify a blocked workflow called with a timeout is cancelled
    wfid = str(uuid.uuid4())
    with SetWorkflowTimeout(0.1):
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            with SetWorkflowID(wfid):
                blocked_workflow()
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is None
        start_time = time.time() * 1000
        handle = DBOS.start_workflow(blocked_workflow)
        status = handle.get_status()
        assert status.workflow_timeout_ms == 100
        assert (
            status.workflow_deadline_epoch_ms is not None
            and status.workflow_deadline_epoch_ms > start_time
        )
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            handle.get_result()

    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING"})
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
        )
    # Recover the workflow, verify it still times out
    handles = DBOS._recover_pending_workflows()
    assert len(handles) == 1
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handles[0].get_result()

    @DBOS.workflow()
    def parent_workflow_with_timeout() -> None:
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is None
        with SetWorkflowTimeout(0.1):
            with pytest.raises(DBOSAwaitedWorkflowCancelledError):
                blocked_workflow()
            handle = DBOS.start_workflow(blocked_workflow)
            with pytest.raises(DBOSAwaitedWorkflowCancelledError):
                handle.get_result()
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is None

    # Verify if a parent calls a blocked workflow with a timeout, the child is cancelled
    assert parent_workflow_with_timeout() == None

    start_child, direct_child = str(uuid.uuid4()), str(uuid.uuid4())

    @DBOS.workflow()
    def parent_workflow() -> None:
        assert assert_current_dbos_context().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is not None
        with SetWorkflowID(start_child):
            DBOS.start_workflow(blocked_workflow)
        with SetWorkflowID(direct_child):
            blocked_workflow()

    # Verify if a parent called with a timeout calls a blocked child
    # the deadline propagates and the children are also cancelled.
    with SetWorkflowTimeout(1.0):
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            parent_workflow()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        DBOS.retrieve_workflow(start_child).get_result()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        DBOS.retrieve_workflow(direct_child).get_result()

    # Verify the context variables are set correctly
    with SetWorkflowTimeout(1.0):
        assert assert_current_dbos_context().workflow_timeout_ms == 1000
        with SetWorkflowTimeout(2.0):
            assert assert_current_dbos_context().workflow_timeout_ms == 2000
        with SetWorkflowTimeout(None):
            assert assert_current_dbos_context().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_timeout_ms == 1000
    assert get_local_dbos_context() is None


def test_custom_names(dbos: DBOS) -> None:
    workflow_name = "workflow_name"
    step_name = "step_name"
    txn_name = "txn_name"
    queue = Queue("test-queue")

    @DBOS.workflow(name=workflow_name)
    def workflow() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    handle = queue.enqueue(workflow)
    assert handle.get_status().name == workflow_name
    assert handle.get_result() == handle.workflow_id

    @DBOS.step(name=step_name)
    def step() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    handle = queue.enqueue(step)
    assert handle.get_status().name == f"<temp>.{step_name}"
    assert handle.get_result() == handle.workflow_id

    @DBOS.transaction(name=txn_name)
    def txn() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    handle = queue.enqueue(txn)
    assert handle.get_status().name == f"<temp>.{txn_name}"
    assert handle.get_result() == handle.workflow_id

    # Verify we can declare another workflow with the same function name
    # but a different custom name

    another_workflow = "another_workflow"

    @DBOS.workflow(name=another_workflow)
    def workflow(x: int) -> int:
        return x

    value = 5
    handle = DBOS.start_workflow(workflow, value)  # type: ignore
    assert handle.get_status().name == another_workflow
    assert handle.get_result() == value  # type: ignore


@pytest.mark.asyncio
async def test_step_without_dbos(dbos: DBOS, config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)

    @DBOS.step()
    def step(x: int) -> int:
        assert DBOS.workflow_id is None
        return x

    @DBOS.step()
    async def async_step(x: int) -> int:
        assert DBOS.workflow_id is None
        return x

    assert step(5) == 5
    assert await async_step(5) == 5

    DBOS(config=config)

    assert step(5) == 5
    assert await async_step(5) == 5

    DBOS.launch()

    assert step(5) == 5
    assert await async_step(5) == 5

    assert len(DBOS.list_workflows()) == 0


def test_nested_steps(dbos: DBOS) -> None:

    @DBOS.step()
    def outer_step() -> str:
        return inner_step()

    @DBOS.step()
    def inner_step() -> str:
        id = DBOS.workflow_id
        assert id is not None
        return id

    @DBOS.workflow()
    def workflow() -> str:
        return outer_step()

    id = workflow()
    steps = DBOS.list_workflow_steps(id)
    assert len(steps) == 1
    assert steps[0]["function_name"] == outer_step.__qualname__


def test_destroy(dbos: DBOS, config: DBOSConfig) -> None:

    @DBOS.workflow()
    def unblocked_workflow() -> None:
        return

    blocking_event = threading.Event()

    @DBOS.workflow()
    def blocked_workflow() -> None:
        blocking_event.wait()

    unblocked_workflow()

    # Destroy DBOS with no active workflows, verify it is destroyed immediately
    start = time.time()
    DBOS.destroy(workflow_completion_timeout_sec=60)
    assert time.time() - start < 5

    DBOS(config=config)
    DBOS.launch()

    handle = DBOS.start_workflow(blocked_workflow)

    # Destroy DBOS with an active workflow, verify it waits out the timeout
    start = time.time()
    DBOS.destroy(workflow_completion_timeout_sec=3)
    assert time.time() - start > 3
    blocking_event.set()
    with pytest.raises(DBOSException):
        handle.get_result()
