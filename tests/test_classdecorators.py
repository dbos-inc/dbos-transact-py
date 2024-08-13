import uuid

import pytest
import sqlalchemy as sa

from dbos_transact.context import SetWorkflowUUID
from dbos_transact.dbos import DBOS


class DBOSTestClassInst:
    def __init__(self) -> None:
        self.txn_counter: int = 0
        self.wf_counter: int = 0
        self.comm_counter: int = 0

    # Not yet.... @DBOS.workflow
    def test_workflow(self, var: str, var2: str) -> str:
        self.wf_counter += 1
        res = self.test_transaction(var2)
        res2 = self.test_communicator(var)
        DBOS.logger.info("I'm test_workflow")
        return res + res2

    def test_transaction(self, var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        self.txn_counter += 1
        DBOS.logger.info("I'm test_transaction")
        return var2 + str(rows[0][0])

    def test_communicator(self, var: str) -> str:
        self.comm_counter += 1
        DBOS.logger.info("I'm test_communicator")
        return var


class DBOSTestClassStatic:
    txn_counter: int = 0
    wf_counter: int = 0
    comm_counter: int = 0

    @staticmethod
    def test_workflow(var: str, var2: str) -> str:
        DBOSTestClassStatic.wf_counter += 1
        res = DBOSTestClassStatic.test_transaction(var2)
        res2 = DBOSTestClassStatic.test_communicator(var)
        DBOS.logger.info("I'm test_workflow")
        return res + res2

    @staticmethod
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        DBOSTestClassStatic.txn_counter += 1
        DBOS.logger.info("I'm test_transaction")
        return var2 + str(rows[0][0])

    @staticmethod
    def test_communicator(var: str) -> str:
        DBOSTestClassStatic.comm_counter += 1
        DBOS.logger.info("I'm test_communicator")
        return var


class DBOSTestClassClass:
    txn_counter: int = 0
    wf_counter: int = 0
    comm_counter: int = 0

    @classmethod
    def test_workflow(cls, var: str, var2: str) -> str:
        cls.wf_counter += 1
        res = cls.test_transaction(var2)
        res2 = cls.test_communicator(var)
        DBOS.logger.info("I'm test_workflow")
        return res + res2

    @classmethod
    def test_transaction(cls, var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        cls.txn_counter += 1
        DBOS.logger.info("I'm test_transaction")
        return var2 + str(rows[0][0])

    @classmethod
    def test_communicator(cls, var: str) -> str:
        cls.comm_counter += 1
        DBOS.logger.info("I'm test_communicator")
        return var


""""
def test_simple_workflow(dbos: DBOS) -> None:    
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
"""
