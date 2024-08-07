import time
import uuid

import pytest
import sqlalchemy as sa

from dbos_transact.communicator import CommunicatorContext
from dbos_transact.dbos import DBOS
from dbos_transact.dbos_config import ConfigFile
from dbos_transact.transaction import TransactionContext
from dbos_transact.workflow import WorkflowContext


def test_simple_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    comm_counter: int = 0

    @dbos.workflow()
    def test_workflow(ctx: WorkflowContext, var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(ctx.txn_ctx(), var2)
        res2 = test_communicator(ctx.comm_ctx(), var)
        ctx.logger.info("I'm test_workflow")
        return res + res2

    @dbos.transaction()
    def test_transaction(ctx: TransactionContext, var2: str) -> str:
        rows = ctx.session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        ctx.logger.info("I'm test_transaction")
        return var2 + str(rows[0][0])

    @dbos.communicator()
    def test_communicator(ctx: CommunicatorContext, var: str) -> str:
        nonlocal comm_counter
        comm_counter += 1
        ctx.logger.info("I'm test_communicator")
        return var

    assert test_workflow(dbos.wf_ctx(), "bob", "bob") == "bob1bob"

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    assert test_workflow(dbos.wf_ctx(wfuuid), "alice", "alice") == "alice1alice"
    assert test_workflow(dbos.wf_ctx(wfuuid), "alice", "alice") == "alice1alice"
    assert txn_counter == 2  # Only increment once
    assert comm_counter == 2  # Only increment once

    # Test we can execute the workflow by uuid
    handle = dbos.execute_workflow_uuid(wfuuid)
    assert handle.get_result() == "alice1alice"
    assert wf_counter == 4


def test_exception_workflow(dbos: DBOS) -> None:
    txn_counter: int = 0
    wf_counter: int = 0
    comm_counter: int = 0

    @dbos.transaction()
    def exception_transaction(ctx: TransactionContext, var: str) -> str:
        nonlocal txn_counter
        txn_counter += 1
        raise Exception(var)

    @dbos.communicator()
    def exception_communicator(ctx: CommunicatorContext, var: str) -> str:
        nonlocal comm_counter
        comm_counter += 1
        raise Exception(var)

    @dbos.workflow()
    def exception_workflow(ctx: WorkflowContext) -> None:
        nonlocal wf_counter
        wf_counter += 1
        err1 = None
        err2 = None
        try:
            exception_transaction(ctx.txn_ctx(), "test error")
        except Exception as e:
            err1 = e

        try:
            exception_communicator(ctx.comm_ctx(), "test error")
        except Exception as e:
            err2 = e
        assert err1 == err2 and err1 is not None
        raise err1

    with pytest.raises(Exception) as exc_info:
        exception_workflow(dbos.wf_ctx())

    assert "test error" in str(exc_info.value)

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with pytest.raises(Exception) as exc_info:
        exception_workflow(dbos.wf_ctx(wfuuid))
    assert "test error" in str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        exception_workflow(dbos.wf_ctx(wfuuid))
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
    def test_workflow(ctx: WorkflowContext, var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(ctx.txn_ctx(), var2)
        return res + var

    @dbos.transaction()
    def test_transaction(ctx: TransactionContext, var2: str) -> str:
        rows = ctx.session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    wfuuid = str(uuid.uuid4())
    assert test_workflow(dbos.wf_ctx(wfuuid), "bob", "bob") == "bob1bob"

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
    def test_workflow(ctx: WorkflowContext, var: str) -> str:
        nonlocal wf_counter
        if var == test_var:
            wf_counter += 1
        return var

    wfuuid = str(uuid.uuid4())
    assert test_workflow(dbos.wf_ctx(wfuuid), test_var) == test_var

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
    def test_workflow(ctx: WorkflowContext, var: str) -> str:
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
    def test_workflow(ctx: WorkflowContext, var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(ctx.txn_ctx(), var2)
        return res + var

    @dbos.transaction()
    def test_transaction(ctx: TransactionContext, var2: str) -> str:
        rows = ctx.session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    wfuuid = str(uuid.uuid4())
    handle = dbos.start_workflow(test_workflow, dbos.wf_ctx(wfuuid), "bob", "bob")
    assert handle.get_result() == "bob1bob"
    handle = dbos.start_workflow(test_workflow, dbos.wf_ctx(wfuuid), "bob", "bob")
    assert handle.get_result() == "bob1bob"
    assert test_workflow(dbos.wf_ctx(wfuuid), "bob", "bob") == "bob1bob"
    assert txn_counter == 1
    assert wf_counter == 3
