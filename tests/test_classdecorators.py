import uuid

import pytest
import sqlalchemy as sa

from dbos_transact.context import SetWorkflowUUID
from dbos_transact.dbos import (
    DBOS,
    dbos_example_class_decorator,
    dbos_example_decorator,
)


@dbos_example_class_decorator
class DBOSTestClassInst:
    def __init__(self) -> None:
        self.txn_counter: int = 0
        self.wf_counter: int = 0
        self.comm_counter: int = 0

    @dbos_example_decorator
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


def test_simple_workflow(dbos: DBOS) -> None:
    class DBOSTestClassStatic:
        txn_counter: int = 0
        wf_counter: int = 0
        comm_counter: int = 0

        @dbos.workflow()
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

    assert DBOSTestClassStatic.test_workflow("bob", "bob") == "bob1bob"
