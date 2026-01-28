# mypy: disable-error-code="no-redef"

import asyncio
import datetime
import json
import logging
import os
import threading
import time
import uuid
from typing import Any, Optional, cast

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

# Public API
from dbos import (
    DBOS,
    DBOSConfig,
    Queue,
    Serializer,
    SetWorkflowID,
    SetWorkflowTimeout,
    WorkflowHandle,
    WorkflowStatusString,
)

# Private API because this is a test
from dbos._client import DBOSClient
from dbos._context import assert_current_dbos_context, get_local_dbos_context
from dbos._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSConflictingRegistrationError,
    DBOSException,
)
from dbos._schemas.system_database import SystemSchema
from dbos._utils import GlobalParams
from tests.conftest import using_sqlite


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
            assert (
                recovery_attempts == 1
            )  # This is just runs, and does not imply recovery retries
            assert updated_at >= created_at


def test_eid_reset(dbos: DBOS) -> None:
    @DBOS.step()
    def test_step() -> str:
        return "hello"

    @DBOS.workflow()
    def test_workflow() -> str:
        DBOS.set_event("started", 1)
        DBOS.recv("run_step")
        return test_step()

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        wfh = dbos.start_workflow(test_workflow)
        DBOS.get_event(wfuuid, "started")
        with dbos._sys_db.engine.connect() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .values(executor_id="some_other_executor")
                .where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
            )
            c.commit()
        DBOS.send(wfuuid, 1, "run_step")
        wfh.get_result()
        with dbos._sys_db.engine.connect() as c:
            x = c.execute(
                sa.select(SystemSchema.workflow_status.c.executor_id).where(
                    SystemSchema.workflow_status.c.workflow_uuid == wfuuid
                )
            ).fetchone()
            assert x is not None
            assert x[0] == "local"


def test_child_workflow(dbos: DBOS) -> None:
    @DBOS.workflow()
    def child_workflow(var: str) -> str:
        return var + "_child"

    @DBOS.workflow()
    def parent_workflow() -> tuple[str, str, str]:
        parent_id = DBOS.workflow_id
        assert parent_id is not None
        result = child_workflow("test")
        child_handle = dbos.start_workflow(child_workflow, "async")
        child_result = child_handle.get_result()
        return parent_id, child_handle.workflow_id, result + child_result

    # Run the parent workflow and get its ID
    handle = dbos.start_workflow(parent_workflow)
    parent_id, async_child_id, result = handle.get_result()

    assert result == "test_childasync_child"

    # Verify parent workflow has no parent
    parent_status = DBOS.get_workflow_status(parent_id)
    assert parent_status is not None
    assert parent_status.parent_workflow_id is None

    # The synchronous child workflow ID follows the pattern: parent_id-function_id
    sync_child_id = f"{parent_id}-1"
    sync_child_status = DBOS.get_workflow_status(sync_child_id)
    assert sync_child_status is not None
    assert sync_child_status.parent_workflow_id == parent_id

    # The async child workflow should also have the parent ID set
    async_child_status = DBOS.get_workflow_status(async_child_id)
    assert async_child_status is not None
    assert async_child_status.parent_workflow_id == parent_id


def test_child_workflow_assigned_id(dbos: DBOS) -> None:
    txn_counter: int = 0

    @DBOS.transaction()
    def test_transaction(var: str) -> str:
        nonlocal txn_counter
        txn_counter += 1
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return var + str(rows[0][0])

    @DBOS.workflow()
    def child_with_assigned_id(var: str) -> str:
        assert DBOS.workflow_id == "assigned_child_id"
        return test_transaction(var)

    @DBOS.workflow()
    def parent_with_assigned_child() -> str:
        with SetWorkflowID("assigned_child_id"):
            res1 = child_with_assigned_id("first")
        # Call again with same ID - should return cached result
        with SetWorkflowID("assigned_child_id"):
            handle = dbos.start_workflow(child_with_assigned_id, "second")
            res2 = handle.get_result()
        return res1 + res2

    # Run twice - transaction should only execute once due to idempotency
    original_parent_id = str(uuid.uuid4())
    with SetWorkflowID(original_parent_id):
        assert parent_with_assigned_child() == "first1first1"
    assert parent_with_assigned_child() == "first1first1"
    assert txn_counter == 1  # Transaction only ran once

    # Verify the assigned child has parent_workflow_id set
    child_status = DBOS.get_workflow_status("assigned_child_id")
    assert child_status is not None
    assert child_status.parent_workflow_id == original_parent_id


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
    assert (
        stat.recovery_attempts == 2
    )  # 1 actual call to test_workflow + 1 recovery attempt


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
    pattern = f"Non-existent target workflow ID: {dest_uuid}"
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

    # Test that the number of attempts matches the number of calls requiring work (1)
    stat = dbos.get_workflow_status("parent_a")
    assert stat
    assert stat.recovery_attempts == 1
    stat = dbos.get_workflow_status("parent_b")
    assert stat
    assert stat.recovery_attempts == 1
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


def test_send_recv(dbos: DBOS, config: DBOSConfig) -> None:
    for use_listen_notify in [True, False]:
        # Test using both LISTEN/NOTIFY and polling
        DBOS.destroy(destroy_registry=True)
        config["use_listen_notify"] = use_listen_notify
        dbos = DBOS(config=config)
        DBOS.launch()

        send_counter: int = 0
        recv_counter: int = 0

        @DBOS.workflow()
        def test_send_workflow(dest_uuid: str, topic: str) -> str:
            DBOS.send(dest_uuid, "test1")
            DBOS.send(dest_uuid, "test2", topic=topic)
            DBOS.send(dest_uuid, "test3")
            nonlocal send_counter
            send_counter += 1
            return dest_uuid

        @DBOS.workflow()
        def test_recv_workflow(topic: str) -> str:
            msg1 = DBOS.recv(topic, timeout_seconds=10)
            msg2 = DBOS.recv(timeout_seconds=10)
            msg3 = DBOS.recv(timeout_seconds=10)
            nonlocal recv_counter
            recv_counter += 1
            return "-".join([str(msg1), str(msg2), str(msg3)])

        @DBOS.workflow()
        def test_recv_timeout(timeout_seconds: float) -> None:
            msg = DBOS.recv(timeout_seconds=timeout_seconds)
            assert msg is None

        @DBOS.workflow()
        def test_send_none(dest_uuid: str) -> None:
            DBOS.send(dest_uuid, None)

        dest_uuid = str(uuid.uuid4())

        # Send to non-existent uuid should fail
        with pytest.raises(Exception) as exc_info:
            test_send_workflow(dest_uuid, "testtopic")
        assert f"Non-existent `send` destination workflow ID: {dest_uuid}" in str(
            exc_info.value
        )

        with SetWorkflowID(dest_uuid):
            handle = DBOS.start_workflow(test_recv_workflow, "testtopic")
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
        assert (
            duration < 1.0
        )  # None is from the received message, not from the timeout.

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
            DBOS.recv("test1")
        assert "recv() must be called from within a workflow" in str(exc_info.value)


def test_send_recv_temp_wf(dbos: DBOS) -> None:
    recv_counter: int = 0

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

    wfs = DBOS.list_workflows()
    assert len(wfs) == 2
    assert wfs[0].workflow_id == dest_uuid
    assert wfs[1].workflow_id != dest_uuid

    wfi = DBOS.get_workflow_status(wfs[1].workflow_id)
    assert wfi
    assert wfi.name == "<temp>.temp_send_workflow"

    assert recv_counter == 1

    # Test substring search
    wfs = DBOS.list_workflows(workflow_id_prefix=dest_uuid)
    assert wfs[0].workflow_id == dest_uuid

    wfs = DBOS.list_workflows(workflow_id_prefix=dest_uuid[0:10])
    assert dest_uuid in [w.workflow_id for w in wfs]

    wfs = DBOS.list_workflows(workflow_id_prefix=dest_uuid[0:10])
    assert dest_uuid in [w.workflow_id for w in wfs]

    x = DBOS.list_workflows(
        start_time=datetime.datetime.now().isoformat(),
        workflow_id_prefix=dest_uuid[0:10],
    )
    assert len(x) == 0

    x = DBOS.list_workflows(workflow_id_prefix=dest_uuid[0:10])
    assert len(x) >= 1
    assert dest_uuid in [w.workflow_id for w in x]

    x = DBOS.list_workflows(workflow_id_prefix=dest_uuid + "thisdoesnotexist")
    assert len(x) == 0

    x = DBOS.list_workflows(workflow_id_prefix="1" + dest_uuid)
    assert len(x) == 0


def test_set_get_events(dbos: DBOS, config: DBOSConfig) -> None:
    for use_listen_notify in [True, False]:
        # Test using both LISTEN/NOTIFY and polling
        DBOS.destroy(destroy_registry=True)
        config["use_listen_notify"] = use_listen_notify
        dbos = DBOS(config=config)
        DBOS.launch()

        @DBOS.workflow()
        def test_setevent_workflow() -> None:
            DBOS.set_event("key1", "value1")
            DBOS.set_event("key2", "value2")
            DBOS.set_event("key3", None)
            set_event_step()

        @DBOS.step()
        def set_event_step() -> None:
            DBOS.set_event("key4", "badvalue")
            DBOS.set_event("key4", "value4")

        @DBOS.workflow()
        def test_getevent_workflow(
            target_uuid: str, key: str, timeout: float = 0.0
        ) -> Optional[str]:
            msg = dbos.get_event(target_uuid, key, timeout)
            return str(msg) if msg is not None else None

        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid):
            test_setevent_workflow()
        with SetWorkflowID(wfid):
            test_setevent_workflow()

        value1 = test_getevent_workflow(wfid, "key1")
        assert value1 == "value1"

        value2 = test_getevent_workflow(wfid, "key2")
        assert value2 == "value2"

        # Run getEvent outside of a workflow
        value1 = DBOS.get_event(wfid, "key1", 0)
        assert value1 == "value1"

        value2 = DBOS.get_event(wfid, "key2", 0)
        assert value2 == "value2"

        begin_time = time.time()
        value3 = test_getevent_workflow(wfid, "key3")
        assert value3 is None

        value4 = DBOS.get_event(wfid, "key4", 0)
        assert value4 == "value4"

        steps = DBOS.list_workflow_steps(wfid)
        assert len(steps) == 4
        assert (
            steps[0]["function_name"]
            == steps[1]["function_name"]
            == steps[2]["function_name"]
            == "DBOS.setEvent"
        )
        assert steps[3]["function_name"] == set_event_step.__qualname__

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
        res = DBOS.get_event("non-existent-uuid", "key1", 1.0)
        duration = time.time() - begin_time
        assert duration > 0.7
        assert res is None

        begin_time = time.time()
        res = DBOS.get_event("non-existent-uuid", "key1", 1.0)
        duration = time.time() - begin_time
        assert duration > 0.7
        assert res is None

        # Test setEvent outside of a workflow
        with pytest.raises(Exception) as exc_info:
            DBOS.set_event("key1", "value1")
        assert "set_event() must be called from within a workflow" in str(
            exc_info.value
        )

        # Test timing when listening for an event that has not yet been set
        event = threading.Event()

        @DBOS.workflow()
        def set_event_workflow() -> None:
            event.wait()
            DBOS.set_event("key", "value")

        @DBOS.workflow()
        def get_event_workflow(id: str) -> Any:
            return DBOS.get_event(id, "key", timeout_seconds=60.0)

        start_time = time.time()
        set_event_handle = DBOS.start_workflow(set_event_workflow)
        get_event_handle = DBOS.start_workflow(
            get_event_workflow, set_event_handle.workflow_id
        )
        time.sleep(1)
        event.set()
        set_event_handle.get_result()
        assert get_event_handle.get_result() == "value"
        assert time.time() - start_time < 5


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

    with pytest.raises(Exception):
        test_ns_transaction("h")
    with pytest.raises(Exception):
        test_ns_wf("g")

    wfh = DBOS.start_workflow(test_reg_wf, "a")
    with pytest.raises(Exception):
        DBOS.send(wfh.workflow_id, invalid_return, "sss")
    wfh.get_result()

    with pytest.raises(Exception):
        test_ns_event("e")

    with pytest.raises(Exception):
        test_bad_wf1("a")
    with pytest.raises(Exception):
        test_bad_wf2("b")
    with pytest.raises(Exception):
        test_bad_wf3("c")
    with pytest.raises(Exception):
        test_bad_wf4("d")


def test_multi_set_event(dbos: DBOS) -> None:
    event = threading.Event()

    wfid = str(uuid.uuid4())

    workflow_key = "workflow_key"
    step_key = "step_key"
    value1 = "value1"
    value2 = "value2"

    @DBOS.step()
    def set_event_step(value: str) -> None:
        DBOS.set_event(step_key, value)

    @DBOS.workflow()
    def set_event_workflow() -> None:
        assert DBOS.workflow_id == wfid
        DBOS.set_event(workflow_key, value1)
        set_event_step(value1)
        event.wait()
        DBOS.set_event(workflow_key, value2)
        set_event_step(value2)

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(set_event_workflow)
    assert DBOS.get_event(wfid, workflow_key) == value1
    assert DBOS.get_event(wfid, step_key) == value1
    event.set()
    assert handle.get_result() == None
    assert DBOS.get_event(wfid, workflow_key) == value2
    assert DBOS.get_event(wfid, step_key) == value2
    assert DBOS.get_all_events(wfid) == {workflow_key: value2, step_key: value2}


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
    assert "already run with status SUCCESS" in caplog.text

    result4 = dest_handle_2.get_result()
    assert result4 == result2
    caplog.clear()

    # Reset logging
    logging.getLogger("dbos").propagate = original_propagate


def test_destroy_semantics(dbos: DBOS, config: DBOSConfig) -> None:

    @DBOS.workflow()
    def test_workflow(var: str) -> str:
        return var

    var = "test"
    assert test_workflow(var) == var

    # Start the workflow asynchornously
    wf = dbos.start_workflow(test_workflow, var)
    assert wf.get_result() == var

    DBOS.destroy()
    DBOS(config=config)
    DBOS.launch()

    assert test_workflow(var) == var

    wf = dbos.start_workflow(test_workflow, var)
    assert wf.get_result() == var


@pytest.mark.asyncio
async def test_destroy_semantics_async(dbos: DBOS, config: DBOSConfig) -> None:

    @DBOS.workflow()
    async def test_workflow(var: str) -> str:
        return var

    var = "test"
    assert await test_workflow(var) == var

    # Start the workflow asynchornously
    wf = await dbos.start_workflow_async(test_workflow, var)
    assert await wf.get_result() == var

    DBOS.destroy()
    DBOS(config=config)
    DBOS.launch()

    assert await test_workflow(var) == var

    wf = await dbos.start_workflow_async(test_workflow, var)
    assert await wf.get_result() == var


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
    app_version = DBOS.application_version
    assert len(app_version) > 0
    assert is_hex(app_version)

    DBOS.destroy(destroy_registry=True)
    assert DBOS.application_version == ""
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def workflow_one(x: int) -> int:
        return x

    @DBOS.workflow()
    def workflow_two(y: int) -> int:
        return y

    DBOS.launch()

    # Verify stability--the same workflow source produces the same app version.
    assert DBOS.application_version == app_version

    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def workflow_one(x: int) -> int:
        return x

    # Verify that changing the workflow source changes the workflow version
    DBOS.launch()
    assert DBOS.application_version != app_version

    # Verify that version can be overriden with an environment variable
    app_version = str(uuid.uuid4())
    os.environ["DBOS__APPVERSION"] = app_version

    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def workflow_one(x: int) -> int:
        return x

    DBOS.launch()
    assert DBOS.application_version == app_version

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
    assert DBOS.application_version == app_version
    assert GlobalParams.executor_id == executor_id == DBOS.executor_id
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
    assert DBOS.get_result(handle.workflow_id) == handle.workflow_id

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

    assert await asyncio.to_thread(step, 5) == 5
    assert await async_step(5) == 5

    DBOS(config=config)

    assert await asyncio.to_thread(step, 5) == 5
    assert await async_step(5) == 5

    DBOS.launch()

    assert await asyncio.to_thread(step, 5) == 5
    assert await async_step(5) == 5

    assert len(await DBOS.list_workflows_async()) == 0


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


def test_without_appdb(config: DBOSConfig, cleanup_test_databases: None) -> None:
    DBOS.destroy(destroy_registry=True)
    config["application_database_url"] = None
    dbos = DBOS(config=config)
    DBOS.launch()
    assert dbos._app_db is None

    @DBOS.step()
    def step() -> None:
        return

    @DBOS.workflow()
    def workflow() -> str:
        step()
        step()
        step()
        assert DBOS.workflow_id
        return DBOS.workflow_id

    wfid = workflow()
    assert wfid
    steps = DBOS.list_workflow_steps(wfid)
    assert len(steps) == 3
    for s in steps:
        assert s["function_name"] == step.__qualname__
    forked_handle = DBOS.fork_workflow(wfid, start_step=1)
    assert forked_handle.get_result() == forked_handle.workflow_id

    @DBOS.transaction()
    def transaction() -> None:
        return

    with pytest.raises(AssertionError):
        transaction()

    DBOS.destroy(destroy_registry=True)

    client = DBOSClient(system_database_url=config["system_database_url"])
    steps = client.list_workflow_steps(wfid)
    assert len(steps) == 3
    for s in steps:
        assert s["function_name"] == step.__qualname__


def test_custom_database(
    config: DBOSConfig, db_engine: sa.Engine, cleanup_test_databases: None
) -> None:
    DBOS.destroy(destroy_registry=True)
    assert config["system_database_url"]
    custom_database = "F8nny_dAtaB@s3@-n@m3.sqlite"
    url = sa.make_url(config["system_database_url"])
    url = url.set(database=custom_database)
    config["system_database_url"] = url.render_as_string(hide_password=False)
    # Destroy the database if it exists
    if using_sqlite():
        parsed_url = sa.make_url(config["system_database_url"])
        db_path = parsed_url.database
        assert db_path is not None
        if os.path.exists(db_path):
            os.remove(db_path)
    else:
        with db_engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(
                sa.text(f'DROP DATABASE IF EXISTS "{custom_database}" WITH (FORCE)')
            )
    DBOS(config=config)
    DBOS.launch()

    key = "key"
    val = "val"

    @DBOS.transaction()
    def transaction() -> None:
        return

    @DBOS.workflow()
    def recv_workflow() -> Any:
        transaction()
        DBOS.set_event(key, val)
        return DBOS.recv()

    handle = DBOS.start_workflow(recv_workflow)
    assert DBOS.get_event(handle.workflow_id, key) == val
    DBOS.send(handle.workflow_id, val)
    assert handle.get_result() == val
    assert len(DBOS.list_workflows()) == 2
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(steps) == 4
    assert "transaction" in steps[0]["function_name"]
    DBOS.destroy(destroy_registry=True)

    # Test custom database with client
    client = DBOSClient(
        system_database_url=config["system_database_url"],
        application_database_url=config["application_database_url"],
    )
    assert len(client.list_workflows()) == 2
    steps = client.list_workflow_steps(handle.workflow_id)
    assert len(steps) == 4
    assert "transaction" in steps[0]["function_name"]


def test_custom_schema(
    config: DBOSConfig, cleanup_test_databases: None, skip_with_sqlite: None
) -> None:
    DBOS.destroy(destroy_registry=True)
    config["dbos_system_schema"] = "F8nny_sCHem@-n@m3"
    dbos = DBOS(config=config)
    DBOS.launch()
    with dbos._sys_db.engine.connect() as connection:
        # Check that the 'dbos' schema does not exist
        result = connection.execute(
            sa.text(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'dbos'"
            )
        )
        rows = result.fetchall()
        assert len(rows) == 0

    key = "key"
    val = "val"

    @DBOS.transaction()
    def transaction() -> None:
        return

    @DBOS.workflow()
    def recv_workflow() -> Any:
        transaction()
        DBOS.set_event(key, val)
        return DBOS.recv()

    handle = DBOS.start_workflow(recv_workflow)
    assert DBOS.get_event(handle.workflow_id, key) == val
    DBOS.send(handle.workflow_id, val)
    assert handle.get_result() == val
    assert len(DBOS.list_workflows()) == 2
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(steps) == 4
    assert "transaction" in steps[0]["function_name"]
    DBOS.destroy(destroy_registry=True)

    # Test custom schema with client
    client = DBOSClient(
        system_database_url=config["system_database_url"],
        application_database_url=config["application_database_url"],
        dbos_system_schema=config["dbos_system_schema"],
    )
    assert len(client.list_workflows()) == 2
    steps = client.list_workflow_steps(handle.workflow_id)
    assert len(steps) == 4
    assert "transaction" in steps[0]["function_name"]


def test_custom_engine(
    config: DBOSConfig,
    cleanup_test_databases: None,
    db_engine: sa.Engine,
    skip_with_sqlite: None,
) -> None:
    DBOS.destroy(destroy_registry=True)
    assert config["system_database_url"]
    config["application_database_url"] = None
    system_database_url = config["system_database_url"]

    # Create a custom engine
    engine = sa.create_engine(system_database_url)
    config["system_database_engine"] = engine

    # Launch DBOS with the engine. It should fail because the database does not exist.
    dbos = DBOS(config=config)
    with pytest.raises(OperationalError):
        DBOS.launch()
    DBOS.destroy(destroy_registry=True)

    # Create the database
    with db_engine.connect() as c:
        c.execution_options(isolation_level="AUTOCOMMIT")
        sysdb_name = sa.make_url(config["system_database_url"]).database
        c.execute(sa.text(f"CREATE DATABASE {sysdb_name}"))

    # Launch DBOS again using the custom pool. It should succeed despite the bogus URL.
    config["system_database_url"] = "postgresql://bogus:url@not:42/fake"
    dbos = DBOS(config=config)
    DBOS.launch()

    key = "key"
    val = "val"

    @DBOS.workflow()
    def recv_workflow() -> Any:
        DBOS.set_event(key, val)
        return DBOS.recv()

    assert dbos._sys_db.engine == engine
    handle = DBOS.start_workflow(recv_workflow)
    assert DBOS.get_event(handle.workflow_id, key) == val
    DBOS.send(handle.workflow_id, val)
    assert handle.get_result() == val
    assert len(DBOS.list_workflows()) == 2
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(steps) == 3
    assert "setEvent" in steps[0]["function_name"]
    DBOS.destroy(destroy_registry=True)

    # Also verify a custom engine works with no URL
    config["system_database_url"] = None
    dbos = DBOS(config=config)
    DBOS.launch()
    DBOS.destroy()

    # Test custom engine with client and a bogus URL
    client = DBOSClient(
        system_database_url="postgresql://bogus:url@not:42/fake",
        system_database_engine=config["system_database_engine"],
    )
    assert len(client.list_workflows()) == 2
    steps = client.list_workflow_steps(handle.workflow_id)
    assert len(steps) == 3
    assert "setEvent" in steps[0]["function_name"]

    # Test custom engine with client and no URL
    client = DBOSClient(
        system_database_engine=config["system_database_engine"],
    )
    assert len(client.list_workflows()) == 2
    steps = client.list_workflow_steps(handle.workflow_id)
    assert len(steps) == 3
    assert "setEvent" in steps[0]["function_name"]


def test_get_events(dbos: DBOS) -> None:

    @DBOS.workflow()
    def events_workflow() -> str:
        # Set multiple events
        DBOS.set_event("event1", "badvalue")
        DBOS.set_event("event1", "value1")
        DBOS.set_event("event2", {"nested": "data", "count": 42})
        DBOS.set_event("event3", [1, 2, 3, 4, 5])
        return "completed"

    # Execute the workflow
    handle = DBOS.start_workflow(events_workflow)
    result = handle.get_result()
    assert result == "completed"

    # Get events, verify they are present with correct values
    def get_events() -> None:
        events = DBOS.get_all_events(handle.workflow_id)

        assert len(events) == 3
        assert events["event1"] == "value1"
        assert events["event2"] == {"nested": "data", "count": 42}
        assert events["event3"] == [1, 2, 3, 4, 5]

    # Verify it works
    get_events()

    # Run it as a workflow, verify it still works
    get_events_workflow = DBOS.workflow()(get_events)
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        get_events_workflow()
    steps = DBOS.list_workflow_steps(wfid)
    assert len(steps) == 1
    assert steps[0]["function_name"] == "DBOS.get_events"

    # Test with a workflow that has no events
    @DBOS.workflow()
    def no_events_workflow() -> str:
        return "no events"

    handle2 = DBOS.start_workflow(no_events_workflow)
    handle2.get_result()

    # Should return empty dict for workflow with no events
    events2 = DBOS.get_all_events(handle2.workflow_id)
    assert events2 == {}


def test_custom_serializer(
    config: DBOSConfig,
    cleanup_test_databases: None,
) -> None:

    class JsonSerializer(Serializer):
        def serialize(self, data: Any) -> str:
            return json.dumps(data)

        def deserialize(self, serialized_data: str) -> Any:
            return json.loads(serialized_data)

    # Configure DBOS with a JSON-based custom serializer
    DBOS.destroy(destroy_registry=True)
    config["serializer"] = JsonSerializer()
    DBOS(config=config)
    DBOS.launch()

    key = "key"
    val = "val"

    @DBOS.workflow()
    def recv_workflow(input: str) -> Any:
        DBOS.set_event(key, input)
        return DBOS.recv()

    expected_input = {
        "args": [val],
        "kwargs": {},
    }

    # Run an enqueued workflow testing workflow communication methods
    queue = Queue("example_queue")
    handle = queue.enqueue(recv_workflow, val)
    assert DBOS.get_event(handle.workflow_id, key) == val
    DBOS.send(handle.workflow_id, val)
    assert handle.get_result() == val
    assert handle.get_status().input == expected_input

    # Verify the workflow is correctly stored in the system database
    assert len(DBOS.list_workflows()) == 2
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(steps) == 3
    assert "setEvent" in steps[0]["function_name"]
    assert "DBOS.recv" in steps[1]["function_name"]
    assert steps[1]["output"] == val

    # Verify the client also supports custom serialization
    client = DBOSClient(
        system_database_url=config["system_database_url"],
        serializer=JsonSerializer(),
    )
    assert len(client.list_workflows()) == 2
    steps = client.list_workflow_steps(handle.workflow_id)
    assert len(steps) == 3
    assert "setEvent" in steps[0]["function_name"]
    assert "DBOS.recv" in steps[1]["function_name"]
    assert steps[1]["output"] == val
    handle = client.enqueue(
        {"queue_name": queue.name, "workflow_name": recv_workflow.__qualname__}, val
    )
    client.send(handle.workflow_id, val)
    assert handle.get_result() == val

    # Verify that a client without the custom serializer does not fail,
    # but emits warnings and falls back to returning raw strings

    client = DBOSClient(
        system_database_url=config["system_database_url"],
    )
    workflows = client.list_workflows()
    assert len(workflows) == 4
    assert cast(str, workflows[0].input) == json.dumps(expected_input)
    assert workflows[0].output == json.dumps(val)

    DBOS.destroy(destroy_registry=True)


def test_run_step(dbos: DBOS) -> None:
    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        res1 = DBOS.run_step(None, test_step, var, 1)
        res2 = DBOS.run_step({"name": "test_step"}, test_step, var2, 2)
        res3 = DBOS.run_step({"name": "concat"}, lambda: res1 + res2)
        return res1 + res2 + res3

    def test_step(var: str, sn: int) -> str:
        assert DBOS.step_id == sn
        step_status = DBOS.step_status
        assert step_status is not None
        assert step_status.step_id == sn
        assert step_status.current_attempt is None
        assert step_status.max_attempts is None
        return var

    def test_step_nwf(var: str) -> str:
        assert DBOS.step_status is None
        return var

    assert DBOS.run_step(None, test_step_nwf, "ha") == "ha"

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert test_workflow("bob", "bob") == "bobbobbobbob"
    steps = DBOS.list_workflow_steps(wfid)
    assert len(steps) == 3
    assert steps[0]["function_name"] == "test_run_step.<locals>.test_step"
    assert steps[1]["function_name"] == "test_step"
    assert steps[2]["function_name"] == "concat"

    assert (
        DBOS.start_workflow(test_workflow, "bob", "bob").get_result() == "bobbobbobbob"
    )

    @DBOS.workflow()
    def test_workflow_sca(var: str, var2: str) -> str:
        res1: str = DBOS.run_step(None, test_step_async, var, 1)
        res2: str = DBOS.run_step({"name": "test_step"}, test_step_async, var2, 2)
        return res1 + res2

    async def test_step_async(var: str, sn: int) -> str:
        assert DBOS.step_id == sn
        step_status = DBOS.step_status
        assert step_status is not None
        assert step_status.step_id == sn
        assert step_status.current_attempt is None
        assert step_status.max_attempts is None
        return var

    async def test_step_async_nwf(var: str) -> str:
        assert DBOS.step_status == None
        return var

    assert DBOS.run_step(None, test_step_async_nwf, "ha") == "ha"

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert test_workflow_sca("joe", "joe") == "joejoe"
    steps = DBOS.list_workflow_steps(wfid)
    assert len(steps) == 2
    assert steps[0]["function_name"] == "test_run_step.<locals>.test_step_async"
    assert steps[1]["function_name"] == "test_step"

    assert DBOS.start_workflow(test_workflow_sca, "joe", "joe").get_result() == "joejoe"

    @DBOS.workflow()
    async def test_workflow_acs(var: str, var2: str) -> str:
        res1 = await DBOS.run_step_async(None, test_step, var, 1)
        res2 = await DBOS.run_step_async({"name": "test_step"}, test_step, var2, 2)
        return res1 + res2

    def test_step(var: str, sn: int) -> str:
        assert DBOS.step_id == sn
        step_status = DBOS.step_status
        assert step_status is not None
        assert step_status.step_id == sn
        assert step_status.current_attempt is None
        assert step_status.max_attempts is None
        return var

    assert DBOS.start_workflow(test_workflow_acs, "bob", "bob").get_result() == "bobbob"  # type: ignore

    @DBOS.workflow()
    def test_errors_wf() -> None:
        n_thrown_errors = 0

        def test_step_error() -> None:
            nonlocal n_thrown_errors
            n_thrown_errors += 1
            raise Exception()

        async def test_step_error_async() -> None:
            nonlocal n_thrown_errors
            n_thrown_errors += 1
            raise Exception()

        with pytest.raises(Exception) as exc_info:
            DBOS.run_step(
                {
                    "retries_allowed": True,
                    "max_attempts": 2,
                    "interval_seconds": 0.1,
                    "backoff_rate": 1,
                },
                test_step_error,
            )
        assert n_thrown_errors == 2
        with pytest.raises(Exception) as exc_info:
            DBOS.run_step(
                {
                    "retries_allowed": True,
                    "max_attempts": 2,
                    "interval_seconds": 0.1,
                    "backoff_rate": 1,
                },
                test_step_error_async,
            )
        assert n_thrown_errors == 4
        with pytest.raises(Exception) as exc_info:
            DBOS.run_step(
                {
                    "retries_allowed": False,
                    "max_attempts": 2,
                    "interval_seconds": 0.1,
                    "backoff_rate": 1,
                },
                test_step_error_async,
            )
        assert n_thrown_errors == 5

    test_errors_wf()


@pytest.mark.asyncio
async def test_run_step_async(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def test_workflow_acs(var: str, var2: str) -> str:
        res1 = await DBOS.run_step_async(None, test_step, var, 1)
        res2 = await DBOS.run_step_async({"name": "test_step"}, test_step, var2, 2)
        return res1 + res2

    def test_step(var: str, sn: int) -> str:
        assert DBOS.step_id == sn
        step_status = DBOS.step_status
        assert step_status is not None
        assert step_status.step_id == sn
        assert step_status.current_attempt is None
        assert step_status.max_attempts is None
        return var

    def test_step_nwf(var: str) -> str:
        assert DBOS.step_status is None
        return var

    assert (await DBOS.run_step_async(None, test_step_nwf, "ha")) == "ha"
    with pytest.raises(RuntimeError) as exc_info:
        DBOS.run_step(None, test_step_nwf, "ha")

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert (await test_workflow_acs("bob", "bob")) == "bobbob"
    steps = await DBOS.list_workflow_steps_async(wfid)
    assert len(steps) == 2
    assert steps[0]["function_name"] == "test_run_step_async.<locals>.test_step"
    assert steps[1]["function_name"] == "test_step"

    assert (
        await (
            await DBOS.start_workflow_async(test_workflow_acs, "bob", "bob")
        ).get_result()
        == "bobbob"
    )

    @DBOS.workflow()
    async def test_workflow(var: str, var2: str) -> str:
        res1: str = await DBOS.run_step_async(None, test_step_async, var, 1)
        res2: str = await DBOS.run_step_async(
            {"name": "test_step"}, test_step_async, var2, 2
        )
        return res1 + res2

    async def test_step_async(var: str, sn: int) -> str:
        assert DBOS.step_id == sn
        step_status = DBOS.step_status
        assert step_status is not None
        assert step_status.step_id == sn
        assert step_status.current_attempt is None
        assert step_status.max_attempts is None
        return var

    async def test_step_async_nwf(var: str) -> str:
        assert DBOS.step_status == None
        return var

    assert (await DBOS.run_step_async(None, test_step_async_nwf, "ha")) == "ha"

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert (await test_workflow("joe", "joe")) == "joejoe"
    steps = await DBOS.list_workflow_steps_async(wfid)
    assert len(steps) == 2
    assert steps[0]["function_name"] == "test_run_step_async.<locals>.test_step_async"
    assert steps[1]["function_name"] == "test_step"

    assert (
        await (
            await DBOS.start_workflow_async(test_workflow, "joe", "joe")
        ).get_result()
        == "joejoe"
    )

    @DBOS.workflow()
    async def test_errors_wf_async() -> None:
        n_thrown_errors = 0

        async def test_step_error() -> None:
            nonlocal n_thrown_errors
            n_thrown_errors += 1
            raise Exception()

        async def test_step_error_async() -> None:
            nonlocal n_thrown_errors
            n_thrown_errors += 1
            raise Exception()

        with pytest.raises(Exception) as exc_info:
            await DBOS.run_step_async(
                {
                    "retries_allowed": True,
                    "max_attempts": 2,
                    "interval_seconds": 0.1,
                    "backoff_rate": 1,
                },
                test_step_error,
            )
        assert n_thrown_errors == 2
        with pytest.raises(Exception) as exc_info:
            await DBOS.run_step_async(
                {
                    "retries_allowed": True,
                    "max_attempts": 2,
                    "interval_seconds": 0.1,
                    "backoff_rate": 1,
                },
                test_step_error_async,
            )
        assert n_thrown_errors == 4
        with pytest.raises(Exception) as exc_info:
            await DBOS.run_step_async(
                {
                    "retries_allowed": False,
                    "max_attempts": 2,
                    "interval_seconds": 0.1,
                    "backoff_rate": 1,
                },
                test_step_error_async,
            )
        assert n_thrown_errors == 5

    await test_errors_wf_async()
