import uuid

import pytest
import sqlalchemy as sa

from dbos.context import DBOSContextEnsure, SetWorkflowUUID, assert_current_dbos_context
from dbos.dbos import DBOS


class DBOSTestClassInstNN:
    @DBOS.required_roles(["user"])
    def test_func(self, var: str) -> str:
        return var


class DBOSTestClassInst:
    def __init__(self) -> None:
        self.instance_name = "myname"

    @DBOS.required_roles(["user"])
    def test_func(self, var: str) -> str:
        return var


class DBOSTestClassStatic:
    @staticmethod
    @DBOS.required_roles(["user"])
    def test_func(var: str) -> str:
        return var


class DBOSTestClassClass:
    @classmethod
    @DBOS.required_roles(["user"])
    def test_func(cls, var: str) -> str:
        return var


@DBOS.required_roles(["user"])
def tfunc(var: str) -> str:
    assert assert_current_dbos_context().assumed_role == "user"
    return var


def test_required_roles() -> None:
    with pytest.raises(Exception) as exc_info:
        tfunc("bare-ctx")
    assert (
        str(exc_info.value)
        == "DBOS Error 8: Function tfunc requires a role, but was called in a context without authentication information"
    )

    with DBOSContextEnsure():
        with pytest.raises(Exception) as exc_info:
            tfunc("bare-ctx")
        assert (
            str(exc_info.value)
            == "DBOS Error 8: Function tfunc requires a role, but was called in a context without authentication information"
        )

        ctx = assert_current_dbos_context()
        ctx.authenticated_roles = ["a", "b", "c"]

        with pytest.raises(Exception) as exc_info:
            tfunc("bare-ctx")
        assert (
            str(exc_info.value)
            == "DBOS Error 8: Function tfunc has required roles, but user is not authenticated for any of them"
        )

        ctx.authenticated_roles = ["a", "b", "c", "user"]
        tfunc("bare-ctx")


def test_required_roles_class(dbos: DBOS) -> None:
    @DBOS.default_required_roles(["user"])
    class DBOSTestClassRR:
        def __init__(self) -> None:
            self.instance_name = "myname"

        @dbos.workflow()
        def test_func_user(self, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "user"
            return var

        @dbos.workflow()
        @dbos.required_roles(["admin"])
        def test_func_admin(self, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "admin"
            return var

        @dbos.required_roles(["admin"])
        @dbos.workflow()
        def test_func_admin_r(self, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "admin"
            return var

        @classmethod
        @dbos.workflow()
        def test_func_user_c(cls, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "user"
            return var

        @classmethod
        @dbos.workflow()
        @dbos.required_roles(["admin"])
        def test_func_admin_c(cls, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "admin"
            return var

        @classmethod
        @dbos.required_roles(["admin"])
        @dbos.workflow()
        def test_func_admin_r_c(cls, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "admin"
            return var

        @staticmethod
        @dbos.workflow()
        def test_func_user_s(var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "user"
            return var

        @staticmethod
        @dbos.workflow()
        @dbos.required_roles(["admin"])
        def test_func_admin_s(var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "admin"
            return var

        @staticmethod
        @dbos.required_roles(["admin"])
        @dbos.workflow()
        def test_func_admin_r_s(var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "admin"
            return var

    # This checks that the interception occurs in all methods
    inst = DBOSTestClassRR()
    with pytest.raises(Exception) as exc_info:
        inst.test_func_user("inst-no-ctx")
    assert "DBOS Error 8" in str(exc_info.value)
    with pytest.raises(Exception) as exc_info:
        inst.test_func_admin("inst-no-ctx")
    assert "DBOS Error 8" in str(exc_info.value)
    with pytest.raises(Exception) as exc_info:
        inst.test_func_admin_r("inst-no-ctx")
    assert "DBOS Error 8" in str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        DBOSTestClassRR.test_func_user_c("class-no-ctx")
    assert "DBOS Error 8" in str(exc_info.value)
    with pytest.raises(Exception) as exc_info:
        DBOSTestClassRR.test_func_admin_c("inst-no-ctx")
    assert "DBOS Error 8" in str(exc_info.value)
    with pytest.raises(Exception) as exc_info:
        DBOSTestClassRR.test_func_admin_r_c("inst-no-ctx")
    assert "DBOS Error 8" in str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        DBOSTestClassRR.test_func_user_s("class-no-ctx")
    assert "DBOS Error 8" in str(exc_info.value)
    with pytest.raises(Exception) as exc_info:
        DBOSTestClassRR.test_func_admin_s("inst-no-ctx")
    assert "DBOS Error 8" in str(exc_info.value)
    with pytest.raises(Exception) as exc_info:
        DBOSTestClassRR.test_func_admin_r_s("inst-no-ctx")
    assert "DBOS Error 8" in str(exc_info.value)

    with DBOSContextEnsure():
        ctx = assert_current_dbos_context()
        # Admin is allowed all
        ctx.authenticated_roles = ["user", "admin"]

        inst.test_func_user("inst-no-ctx")
        inst.test_func_admin("inst-no-ctx")
        inst.test_func_admin_r("inst-no-ctx")

        DBOSTestClassRR.test_func_user_c("class-no-ctx")
        DBOSTestClassRR.test_func_admin_c("inst-no-ctx")
        DBOSTestClassRR.test_func_admin_r_c("inst-no-ctx")

        DBOSTestClassRR.test_func_user_s("class-no-ctx")
        DBOSTestClassRR.test_func_admin_s("inst-no-ctx")
        DBOSTestClassRR.test_func_admin_r_s("inst-no-ctx")

        # User is allowed some
        ctx.authenticated_roles = ["user"]

        inst.test_func_user("inst-no-ctx")
        DBOSTestClassRR.test_func_user_c("class-no-ctx")
        DBOSTestClassRR.test_func_user_s("class-no-ctx")

        # But not all...
        with pytest.raises(Exception) as exc_info:
            inst.test_func_admin("inst-no-ctx")
        assert "DBOS Error 8" in str(exc_info.value)
        with pytest.raises(Exception) as exc_info:
            inst.test_func_admin_r("inst-no-ctx")
        assert "DBOS Error 8" in str(exc_info.value)
        with pytest.raises(Exception) as exc_info:
            DBOSTestClassRR.test_func_admin_c("inst-no-ctx")
        assert "DBOS Error 8" in str(exc_info.value)
        with pytest.raises(Exception) as exc_info:
            DBOSTestClassRR.test_func_admin_r_c("inst-no-ctx")
        assert "DBOS Error 8" in str(exc_info.value)
        with pytest.raises(Exception) as exc_info:
            DBOSTestClassRR.test_func_admin_s("inst-no-ctx")
        assert "DBOS Error 8" in str(exc_info.value)
        with pytest.raises(Exception) as exc_info:
            DBOSTestClassRR.test_func_admin_r_s("inst-no-ctx")
        assert "DBOS Error 8" in str(exc_info.value)


# We can put classes in functions to test decorators for now...
def test_simple_workflow_static(dbos: DBOS) -> None:
    class DBOSTestClassStatic:
        txn_counter: int = 0
        wf_counter: int = 0
        comm_counter: int = 0

        @staticmethod
        @dbos.workflow()
        def test_workflow(var: str, var2: str) -> str:
            DBOSTestClassStatic.wf_counter += 1
            res = DBOSTestClassStatic.test_transaction(var2)
            res2 = DBOSTestClassStatic.test_communicator(var)
            DBOS.logger.info("I'm test_workflow")
            return res + res2

        @staticmethod
        @dbos.transaction()
        def test_transaction(var2: str) -> str:
            rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
            DBOSTestClassStatic.txn_counter += 1
            DBOS.logger.info("I'm test_transaction")
            return var2 + str(rows[0][0])

        @staticmethod
        @dbos.communicator()
        def test_communicator(var: str) -> str:
            DBOSTestClassStatic.comm_counter += 1
            DBOS.logger.info("I'm test_communicator")
            return var

    assert DBOSTestClassStatic.test_workflow("bob", "bob") == "bob1bob"
    wfh = dbos.start_workflow(DBOSTestClassStatic.test_workflow, "bob", "bob")
    assert wfh.get_result() == "bob1bob"
    assert DBOSTestClassStatic.txn_counter == 2
    assert DBOSTestClassStatic.wf_counter == 2
    assert DBOSTestClassStatic.comm_counter == 2


def test_simple_workflow_class(dbos: DBOS) -> None:
    class DBOSTestClassClass:
        txn_counter: int = 0
        wf_counter: int = 0
        comm_counter: int = 0

        @classmethod
        @dbos.workflow()
        def test_workflow(cls, var: str, var2: str) -> str:
            DBOSTestClassClass.wf_counter += 1
            res = DBOSTestClassClass.test_transaction(var2)
            res2 = DBOSTestClassClass.test_communicator(var)
            DBOS.logger.info("I'm test_workflow")
            return res + res2

        @classmethod
        @dbos.transaction()
        def test_transaction(cls, var2: str) -> str:
            rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
            DBOSTestClassClass.txn_counter += 1
            DBOS.logger.info("I'm test_transaction")
            return var2 + str(rows[0][0])

        @classmethod
        @dbos.communicator()
        def test_communicator(cls, var: str) -> str:
            DBOSTestClassClass.comm_counter += 1
            DBOS.logger.info("I'm test_communicator")
            return var

    assert DBOSTestClassClass.test_workflow("bob", "bob") == "bob1bob"
    wfh = dbos.start_workflow(DBOSTestClassClass.test_workflow, "bob", "bob")
    assert wfh.get_result() == "bob1bob"
    assert DBOSTestClassClass.txn_counter == 2
    assert DBOSTestClassClass.wf_counter == 2
    assert DBOSTestClassClass.comm_counter == 2


def test_simple_workflow_inst(dbos: DBOS) -> None:
    class DBOSTestClassInst:
        def __init__(self) -> None:
            self.txn_counter: int = 0
            self.wf_counter: int = 0
            self.comm_counter: int = 0

        @dbos.workflow()
        def test_workflow(self, var: str, var2: str) -> str:
            self.wf_counter += 1
            res = self.test_transaction(var2)
            res2 = self.test_communicator(var)
            DBOS.logger.info("I'm test_workflow")
            return res + res2

        @dbos.transaction()
        def test_transaction(self, var2: str) -> str:
            rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
            self.txn_counter += 1
            DBOS.logger.info("I'm test_transaction")
            return var2 + str(rows[0][0])

        @dbos.communicator()
        def test_communicator(self, var: str) -> str:
            self.comm_counter += 1
            DBOS.logger.info("I'm test_communicator")
            return var

    inst = DBOSTestClassInst()
    assert inst.test_workflow("bob", "bob") == "bob1bob"
    wfh = dbos.start_workflow(inst.test_workflow, "bob", "bob")
    assert wfh.get_result() == "bob1bob"
    assert inst.txn_counter == 2
    assert inst.wf_counter == 2
    assert inst.comm_counter == 2
