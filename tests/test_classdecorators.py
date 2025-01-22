from typing import Optional

import pytest
import sqlalchemy as sa

# Public API
from dbos import DBOS, DBOSConfiguredInstance, SetWorkflowID

# Private API used because this is a test
from dbos._context import DBOSContextEnsure, assert_current_dbos_context


def test_required_roles(dbos: DBOS) -> None:
    @DBOS.required_roles(["user"])
    def tfunc(var: str) -> str:
        assert assert_current_dbos_context().assumed_role == "user"
        return var

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
    class DBOSTestClassRR(DBOSConfiguredInstance):
        def __init__(self) -> None:
            super().__init__("myconfig")

        @DBOS.workflow()
        def test_func_user(self, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "user"
            return var

        @DBOS.workflow()
        @DBOS.required_roles(["admin"])
        def test_func_admin(self, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "admin"
            return var

        @DBOS.required_roles(["admin"])
        @DBOS.workflow()
        def test_func_admin_r(self, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "admin"
            return var

        @classmethod
        @DBOS.workflow()
        def test_func_user_c(cls, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "user"
            return var

        @classmethod
        @DBOS.workflow()
        @DBOS.required_roles(["admin"])
        def test_func_admin_c(cls, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "admin"
            return var

        @classmethod
        @DBOS.required_roles(["admin"])
        @DBOS.workflow()
        def test_func_admin_r_c(cls, var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "admin"
            return var

        @staticmethod
        @DBOS.workflow()
        def test_func_user_s(var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "user"
            return var

        @staticmethod
        @DBOS.workflow()
        @DBOS.required_roles(["admin"])
        def test_func_admin_s(var: str) -> str:
            assert assert_current_dbos_context().assumed_role == "admin"
            return var

        @staticmethod
        @DBOS.required_roles(["admin"])
        @DBOS.workflow()
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
    @DBOS.dbos_class()
    class DBOSTestClassStatic:
        txn_counter: int = 0
        wf_counter: int = 0
        step_counter: int = 0

        @staticmethod
        @DBOS.workflow()
        def test_workflow(var: str, var2: str) -> str:
            DBOSTestClassStatic.wf_counter += 1
            res = DBOSTestClassStatic.test_transaction(var2)
            res2 = DBOSTestClassStatic.test_step(var)
            DBOS.logger.info("I'm test_workflow")
            return res + res2

        @staticmethod
        @DBOS.transaction()
        def test_transaction(var2: str) -> str:
            rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
            DBOSTestClassStatic.txn_counter += 1
            DBOS.logger.info("I'm test_transaction")
            return var2 + str(rows[0][0])

        @staticmethod
        @DBOS.step()
        def test_step(var: str) -> str:
            DBOSTestClassStatic.step_counter += 1
            DBOS.logger.info("I'm test_step")
            return var

    assert DBOSTestClassStatic.test_workflow("bob", "bob") == "bob1bob"
    wfh = dbos.start_workflow(DBOSTestClassStatic.test_workflow, "bob", "bob")
    assert wfh.get_result() == "bob1bob"
    assert DBOSTestClassStatic.txn_counter == 2
    assert DBOSTestClassStatic.wf_counter == 2
    assert DBOSTestClassStatic.step_counter == 2


def test_simple_workflow_class(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class DBOSTestClassClass:
        txn_counter: int = 0
        wf_counter: int = 0
        step_counter: int = 0

        @classmethod
        @DBOS.workflow()
        def test_workflow(cls, var: str, var2: str) -> str:
            DBOSTestClassClass.wf_counter += 1
            res = DBOSTestClassClass.test_transaction(var2)
            res2 = DBOSTestClassClass.test_step(var)
            DBOS.logger.info("I'm test_workflow")
            return res + res2

        @classmethod
        @DBOS.transaction()
        def test_transaction(cls, var2: str) -> str:
            rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
            DBOSTestClassClass.txn_counter += 1
            DBOS.logger.info("I'm test_transaction")
            return var2 + str(rows[0][0])

        @classmethod
        @DBOS.step()
        def test_step(cls, var: str) -> str:
            DBOSTestClassClass.step_counter += 1
            DBOS.logger.info("I'm test_step")
            return var

    assert DBOSTestClassClass.test_workflow("bob", "bob") == "bob1bob"
    wfh = dbos.start_workflow(DBOSTestClassClass.test_workflow, "bob", "bob")
    assert wfh.get_result() == "bob1bob"
    assert DBOSTestClassClass.txn_counter == 2
    assert DBOSTestClassClass.wf_counter == 2
    assert DBOSTestClassClass.step_counter == 2


def test_no_instname(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class DBOSTestClassInst:
        @DBOS.workflow()
        def test_workflow(self) -> str:
            return "Nope"

    with pytest.raises(Exception) as exc_info:
        DBOSTestClassInst().test_workflow()
    assert "Function target appears to be a class instance, but does not have `config_name` set"


def test_simple_workflow_inst(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class DBOSTestClassInst(DBOSConfiguredInstance):
        def __init__(self) -> None:
            super().__init__("bob")
            self.txn_counter: int = 0
            self.wf_counter: int = 0
            self.step_counter: int = 0

        @DBOS.workflow()
        def test_workflow(self, var: str, var2: str) -> str:
            self.wf_counter += 1
            res = self.test_transaction(var2)
            res2 = self.test_step(var)
            DBOS.logger.info("I'm test_workflow")
            return res + res2

        @DBOS.transaction()
        def test_transaction(self, var2: str) -> str:
            rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
            self.txn_counter += 1
            DBOS.logger.info("I'm test_transaction")
            return var2 + str(rows[0][0])

        @DBOS.step()
        def test_step(self, var: str) -> str:
            self.step_counter += 1
            DBOS.logger.info("I'm test_step")
            return var

    inst = DBOSTestClassInst()
    assert inst.test_workflow("bob", "bob") == "bob1bob"

    wfh = dbos.start_workflow(inst.test_workflow, "bob", "bob")
    stati = dbos.get_workflow_status(wfh.get_workflow_id())
    assert stati
    assert stati.config_name == "bob"
    assert stati.class_name == "DBOSTestClassInst"
    stat = wfh.get_status()
    assert stat
    assert stat.config_name == "bob"
    assert stat.class_name == "DBOSTestClassInst"

    assert wfh.get_result() == "bob1bob"
    assert inst.txn_counter == 2
    assert inst.wf_counter == 2
    assert inst.step_counter == 2


def test_forgotten_decorator(dbos: DBOS) -> None:
    class DBOSTestRegErr(DBOSConfiguredInstance):
        def __init__(self) -> None:
            super().__init__("bob")
            self.txn_counter: int = 0
            self.wf_counter: int = 0
            self.step_counter: int = 0

        @DBOS.workflow()
        def test_workflow1(self) -> str:
            return "Forgot class decorator!"

        @classmethod
        @DBOS.workflow()
        def test_workflow2(cls) -> str:
            return "Forgot class decorator!"

    inst = DBOSTestRegErr()
    with pytest.raises(Exception) as exc_info:
        inst.test_workflow1()
    assert (
        "Function target appears to be a class instance, but is not properly registered"
        in str(exc_info.value)
    )

    with pytest.raises(Exception) as exc_info:
        DBOSTestRegErr.test_workflow2()

    assert (
        "Function target appears to be a class, but is not properly registered"
        in str(exc_info.value)
    )


def test_duplicate_reg(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class DBOSTestRegDup(DBOSConfiguredInstance):
        def __init__(self) -> None:
            super().__init__("bob")

    # Duplicate class registration
    with pytest.raises(Exception) as exc_info:

        @DBOS.dbos_class()
        class DBOSTestRegDup(DBOSConfiguredInstance):  # type: ignore
            def __init__(self) -> None:
                super().__init__("bob")

    assert "Duplicate type registration for class 'DBOSTestRegDup'" == str(
        exc_info.value
    )

    # Dupliocate instance registration
    inst = DBOSTestRegDup()
    with pytest.raises(Exception) as exc_info:
        inst = DBOSTestRegDup()

    assert (
        "Duplicate instance registration for class 'DBOSTestRegDup' instance 'bob'"
        == str(exc_info.value)
    )


def test_class_recovery(dbos: DBOS) -> None:
    exc_cnt: int = 0

    @DBOS.dbos_class()
    class DBOSTestClassRec:
        @classmethod
        @DBOS.workflow()
        def check_cls(cls, arg1: str) -> str:
            nonlocal exc_cnt
            exc_cnt += 1
            assert arg1 == "arg1"
            assert cls == DBOSTestClassRec
            return "ran"

    with SetWorkflowID("run1"):
        assert "ran" == DBOSTestClassRec.check_cls("arg1")

    assert exc_cnt == 1

    # Test we can execute the workflow by uuid as recovery would do
    handle = DBOS.execute_workflow_id("run1")
    assert handle.get_result() == "ran"
    assert exc_cnt == 2


def test_inst_recovery(dbos: DBOS) -> None:
    exc_cnt: int = 0
    last_inst: Optional[DBOSTestInstRec] = None

    @DBOS.dbos_class()
    class DBOSTestInstRec(DBOSConfiguredInstance):
        def __init__(self) -> None:
            super().__init__("bob")

        @DBOS.workflow()
        def check_inst(self, arg1: str) -> str:
            nonlocal exc_cnt
            nonlocal last_inst
            exc_cnt += 1
            assert arg1 == "arg1"
            last_inst = self
            return "ran2"

    inst = DBOSTestInstRec()
    with SetWorkflowID("run2"):
        assert "ran2" == inst.check_inst("arg1")

    assert exc_cnt == 1
    assert last_inst is inst

    # Test we can execute the workflow by uuid as recovery would do
    last_inst = None
    handle = DBOS.execute_workflow_id("run2")
    assert handle.get_result() == "ran2"
    assert exc_cnt == 2
    assert last_inst is inst
