from typing import Optional

import pytest
import sqlalchemy as sa

# Public API
from dbos import DBOS, DBOSConfiguredInstance, SetWorkflowUUID

# Private API used because this is a test
from dbos.context import DBOSContextEnsure, assert_current_dbos_context
from dbos.dbos_config import ConfigFile

DBOS.clear()  # In case of other tests leaving it


def test_dbos_singleton(config: ConfigFile) -> None:
    assert config is not None
    # Initialize singleton
    dbos: DBOS = DBOS(None, config)

    @DBOS().dbos_class()
    class DBOSTestClass(DBOSConfiguredInstance):
        txn_counter_c = 0
        wf_counter_c = 0
        comm_counter_c = 0

        def __init__(self) -> None:
            super().__init__("myconfig")
            self.txn_counter: int = 0
            self.wf_counter: int = 0
            self.comm_counter: int = 0

        @classmethod
        @DBOS().workflow()
        def test_workflow_cls(cls, var: str, var2: str) -> str:
            cls.wf_counter_c += 1
            res = DBOSTestClass.test_transaction_cls(var2)
            res2 = DBOSTestClass.test_communicator_cls(var)
            return res + res2

        @classmethod
        @DBOS().transaction()
        def test_transaction_cls(cls, var2: str) -> str:
            rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
            cls.txn_counter_c += 1
            return var2 + str(rows[0][0])

        @classmethod
        @DBOS().communicator()
        def test_communicator_cls(cls, var: str) -> str:
            cls.comm_counter_c += 1
            return var

        @DBOS().workflow()
        def test_workflow(self, var: str, var2: str) -> str:
            self.wf_counter += 1
            res = self.test_transaction(var2)
            res2 = self.test_communicator(var)
            return res + res2

        @DBOS().transaction()
        def test_transaction(self, var2: str) -> str:
            rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
            self.txn_counter += 1
            return var2 + str(rows[0][0])

        @DBOS().communicator()
        def test_communicator(self, var: str) -> str:
            self.comm_counter += 1
            return var

        @DBOS().workflow()
        @DBOS().required_roles(["admin"])
        def test_func_admin(self, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "admin"
            return self.config_name + ":" + var

    @DBOS().workflow()
    def wfFunc(arg: str) -> str:
        assert DBOS.workflow_id == "wfid"
        return arg + "1"

    with SetWorkflowUUID("wfid"):
        res = wfFunc("f")
    assert res == "f1"

    res = DBOSTestClass.test_workflow_cls("a", "b")
    assert res == "b1a"

    # inst = DBOSTestClass()
    # res = inst.test_workflow("c", "d")
    # assert res == "d1c"
    #
    # with DBOSContextEnsure():
    #    ctx = assert_current_dbos_context()
    #    ctx.authenticated_roles = ["user", "admin"]
    #    res = inst.test_func_admin("admin")
    #    assert res == "myconfig:admin"

    dbos.destroy()
