import threading
import uuid
from typing import Callable, Optional

import pytest
import sqlalchemy as sa

# Public API
from dbos import DBOS, DBOSConfiguredInstance, Queue, SetWorkflowID

# Private API used because this is a test
from dbos._context import DBOSContextEnsure, assert_current_dbos_context
from dbos._dbos_config import DBOSConfig
from tests.conftest import queue_entries_are_cleaned_up


def test_required_roles(dbos: DBOS) -> None:
    @DBOS.required_roles(["user"])
    @DBOS.workflow(name="tfunc")
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
    assert stati.class_name and "DBOSTestClassInst" in stati.class_name
    stat = wfh.get_status()
    assert stat
    assert stat.config_name == "bob"
    assert stat.class_name and "DBOSTestClassInst" in stat.class_name

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

    assert (
        "Duplicate type registration for class 'test_duplicate_reg.<locals>.DBOSTestRegDup'"
        == str(exc_info.value)
    )

    # Duplicate instance registration
    inst = DBOSTestRegDup()
    with pytest.raises(Exception) as exc_info:
        inst = DBOSTestRegDup()

    assert (
        "Duplicate instance registration for class 'test_duplicate_reg.<locals>.DBOSTestRegDup' instance 'bob'"
        == str(exc_info.value)
    )

    # There should be no collision when the duplicate class names are in
    # different modules if they're specified with different names.
    from tests import dupname_classdefs1, dupname_classdefsa

    # Two instances of the same class may be registered if they have different
    # instance_name.
    # Duplicate instance registration error still occurs with identical class
    # name and instance_name.
    alice = dupname_classdefs1.DBOSTestRegDup("alice")
    bob = dupname_classdefsa.DBOSTestRegDup("bob")
    bob2 = dupname_classdefs1.DBOSTestRegDup("bob")

    with pytest.raises(Exception) as exc_info:
        bob2 = dupname_classdefs1.DBOSTestRegDup("bob")

    assert (
        "Duplicate instance registration for class 'AnotherDBOSTestRegDup' instance 'bob'"
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
    handle = DBOS._execute_workflow_id("run1")
    assert handle.get_result() == "ran"
    assert exc_cnt == 1


def test_inst_recovery(dbos: DBOS) -> None:
    wfid = str(uuid.uuid4())
    exc_cnt: int = 0
    last_inst: Optional["TestClass"] = None

    @DBOS.dbos_class()
    class TestClass(DBOSConfiguredInstance):
        def __init__(self) -> None:
            super().__init__("test_class")

        @DBOS.workflow()
        def check_inst(self, arg1: str) -> str:
            nonlocal exc_cnt
            nonlocal last_inst
            exc_cnt += 1
            assert arg1 == "arg1"
            last_inst = self
            return "ran2"

    inst = TestClass()
    with SetWorkflowID(wfid):
        assert "ran2" == inst.check_inst("arg1")

    assert exc_cnt == 1
    assert last_inst is inst

    # Test we can execute the workflow by uuid as recovery would do
    last_inst = None
    handle = DBOS._execute_workflow_id(wfid)
    assert handle.get_result() == "ran2"
    assert exc_cnt == 1
    # Workflow has finished so last_inst should be None
    assert last_inst is None

    status = DBOS.retrieve_workflow(wfid).get_status()
    assert status.class_name and "TestClass" in status.class_name
    assert status.config_name == "test_class"


def test_inst_async_recovery(dbos: DBOS) -> None:
    wfid = str(uuid.uuid4())
    event = threading.Event()

    @DBOS.dbos_class()
    class TestClass(DBOSConfiguredInstance):

        def __init__(self, multiplier: int) -> None:
            self.multiply: Callable[[int], int] = lambda x: x * multiplier
            super().__init__("test_class")

        @DBOS.workflow()
        def workflow(self, x: int) -> int:
            event.wait()
            return self.multiply(x)

    input = 2
    multiplier = 5
    inst = TestClass(multiplier)

    with SetWorkflowID(wfid):
        orig_handle = DBOS.start_workflow(inst.workflow, input)

    status = orig_handle.get_status()
    assert status.class_name and "TestClass" in status.class_name
    assert status.config_name == "test_class"

    recovery_handle = DBOS._execute_workflow_id(wfid)

    event.set()
    assert orig_handle.get_result() == input * multiplier
    assert recovery_handle.get_result() == input * multiplier


def test_inst_async_step_recovery(dbos: DBOS) -> None:
    wfid = str(uuid.uuid4())
    event = threading.Event()

    @DBOS.dbos_class()
    class TestClass(DBOSConfiguredInstance):

        def __init__(self, multiplier: int) -> None:
            self.multiply: Callable[[int], int] = lambda x: x * multiplier
            super().__init__("test_class")

        @DBOS.step()
        def step(self, x: int) -> int:
            event.wait()
            return self.multiply(x)

    input = 2
    multiplier = 5
    inst = TestClass(multiplier)

    with SetWorkflowID(wfid):
        orig_handle = DBOS.start_workflow(inst.step, input)

    status = orig_handle.get_status()
    assert status.class_name and "TestClass" in status.class_name
    assert status.config_name == "test_class"

    recovery_handle = DBOS._execute_workflow_id(wfid)

    event.set()
    assert orig_handle.get_result() == input * multiplier
    assert recovery_handle.get_result() == input * multiplier


def test_step_recovery(dbos: DBOS) -> None:
    wfid = str(uuid.uuid4())
    thread_event = threading.Event()
    blocking_event = threading.Event()
    return_value = None

    @DBOS.dbos_class()
    class TestClass(DBOSConfiguredInstance):

        def __init__(self, multiplier: int) -> None:
            self.multiply: Callable[[int], int] = lambda x: x * multiplier
            super().__init__("test_class")

        @DBOS.step()
        def step(self, x: int) -> int:
            thread_event.set()
            blocking_event.wait()
            return self.multiply(x)

    input = 2
    multiplier = 5
    inst = TestClass(multiplier)

    # We're testing synchronously calling the step, but need to do so
    # asynchronously. Hence, a thread.
    def call_step() -> None:
        with SetWorkflowID(wfid):
            nonlocal return_value
            return_value = DBOS.start_workflow(inst.step, input).get_result()

    thread = threading.Thread(target=call_step)
    thread.start()
    thread_event.wait()

    status = DBOS.retrieve_workflow(wfid).get_status()
    assert status.class_name and "TestClass" in status.class_name
    assert status.config_name == "test_class"

    recovery_handle = DBOS._execute_workflow_id(wfid)

    blocking_event.set()
    thread.join()
    assert return_value == input * multiplier
    assert recovery_handle.get_result() == input * multiplier


def test_class_queue_recovery(dbos: DBOS) -> None:
    step_counter: int = 0
    queued_steps = 5
    multiplier = 5

    wfid = str(uuid.uuid4())
    queue = Queue("test_queue")
    step_events = [threading.Event() for _ in range(queued_steps)]
    event = threading.Event()

    @DBOS.dbos_class()
    class TestClass(DBOSConfiguredInstance):
        def __init__(self, multiplier: int) -> None:
            self.multiply: Callable[[int], int] = lambda x: x * multiplier
            super().__init__("test_class")

        @DBOS.workflow()
        def test_workflow(self) -> list[int]:
            assert DBOS.workflow_id == wfid
            handles = []
            for i in range(queued_steps):
                h = queue.enqueue(self.test_step, i)
                handles.append(h)
            return [h.get_result() for h in handles]

        @DBOS.step()
        def test_step(self, i: int) -> int:
            nonlocal step_counter
            step_counter += 1
            step_events[i].set()
            event.wait()
            return self.multiply(i)

    inst = TestClass(multiplier)

    # Start the workflow. Wait for all five steps to start. Verify that they started.
    with SetWorkflowID(wfid):
        original_handle = DBOS.start_workflow(inst.test_workflow)
    for e in step_events:
        e.wait()
        e.clear()

    assert step_counter == 5

    # Recover the workflow, then resume it.
    recovery_handles = DBOS._recover_pending_workflows()
    # Wait until the 2nd invocation of the workflows are dequeued and executed
    for e in step_events:
        e.wait()
    event.set()

    # There should be one handle for the workflow and another for each queued step.
    assert len(recovery_handles) == queued_steps + 1
    # Verify that both the recovered and original workflows complete correctly.
    result = [i * multiplier for i in range(5)]
    for h in recovery_handles:
        status = h.get_status()
        assert status.class_name and "TestClass" in status.class_name
        assert status.config_name == "test_class"
        if h.get_workflow_id() == wfid:
            assert h.get_result() == result
    assert original_handle.get_result() == result
    # Each step should start twice, once originally and once in recovery.
    assert step_counter == 10

    # Rerun the workflow. Because each step is complete, none should start again.
    with SetWorkflowID(wfid):
        assert inst.test_workflow() == result
    assert step_counter == 10

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_class_static_queue_recovery(dbos: DBOS) -> None:
    step_counter: int = 0
    queued_steps = 5

    wfid = str(uuid.uuid4())
    queue = Queue("test_queue")
    step_events = [threading.Event() for _ in range(queued_steps)]
    event = threading.Event()

    @DBOS.dbos_class()
    class TestClass:
        @staticmethod
        @DBOS.workflow()
        def test_workflow() -> list[int]:
            assert DBOS.workflow_id == wfid
            handles = []
            for i in range(queued_steps):
                h = queue.enqueue(TestClass.test_step, i)
                handles.append(h)
            return [h.get_result() for h in handles]

        @staticmethod
        @DBOS.step()
        def test_step(i: int) -> int:
            nonlocal step_counter
            step_counter += 1
            step_events[i].set()
            event.wait()
            return i

    # Start the workflow. Wait for all five steps to start. Verify that they started.
    with SetWorkflowID(wfid):
        original_handle = DBOS.start_workflow(TestClass.test_workflow)
    for e in step_events:
        e.wait()
        e.clear()

    assert step_counter == 5

    # Recover the workflow, then resume it.
    recovery_handles = DBOS._recover_pending_workflows()
    # Wait until the 2nd invocation of the workflows are dequeued and executed
    for e in step_events:
        e.wait()
    event.set()

    # There should be one handle for the workflow and another for each queued step.
    assert len(recovery_handles) == queued_steps + 1
    # Verify that both the recovered and original workflows complete correctly.
    result = [i for i in range(5)]
    for h in recovery_handles:
        status = h.get_status()
        # Class name is not recorded for static methods
        assert status.class_name == None
        assert status.config_name == None
        if h.get_workflow_id() == wfid:
            assert h.get_result() == result
    assert original_handle.get_result() == result
    # Each step should start twice, once originally and once in recovery.
    assert step_counter == 10

    # Rerun the workflow. Because each step is complete, none should start again.
    with SetWorkflowID(wfid):
        assert TestClass.test_workflow() == result
    assert step_counter == 10

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_class_classmethod_queue_recovery(dbos: DBOS) -> None:
    step_counter: int = 0
    multiplier = 5
    queued_steps = 5

    wfid = str(uuid.uuid4())
    queue = Queue("test_queue")
    step_events = [threading.Event() for _ in range(queued_steps)]
    event = threading.Event()

    @DBOS.dbos_class()
    class TestClass:
        multiply: Callable[[int], int] = lambda _: 0

        @classmethod
        @DBOS.workflow()
        def test_workflow(cls) -> list[int]:
            cls.multiply = lambda x: x * multiplier
            assert DBOS.workflow_id == wfid
            handles = []
            for i in range(queued_steps):
                h = queue.enqueue(TestClass.test_step, i)
                handles.append(h)
            return [h.get_result() for h in handles]

        @classmethod
        @DBOS.step()
        def test_step(cls, i: int) -> int:
            nonlocal step_counter
            step_counter += 1
            step_events[i].set()
            event.wait()
            return cls.multiply(i)

    # Start the workflow. Wait for all five steps to start. Verify that they started.
    with SetWorkflowID(wfid):
        original_handle = DBOS.start_workflow(TestClass.test_workflow)
    for e in step_events:
        e.wait()
        e.clear()

    assert step_counter == 5

    # Recover the workflow, then resume it.
    recovery_handles = DBOS._recover_pending_workflows()
    # Wait until the 2nd invocation of the workflows are dequeued and executed
    for e in step_events:
        e.wait()
    event.set()

    # There should be one handle for the workflow and another for each queued step.
    assert len(recovery_handles) == queued_steps + 1
    # Verify that both the recovered and original workflows complete correctly.
    result = [i * multiplier for i in range(5)]
    for h in recovery_handles:
        status = h.get_status()
        # Class name is recorded for class methods
        assert status.class_name and "TestClass" in status.class_name
        assert status.config_name == None
        if h.get_workflow_id() == wfid:
            assert h.get_result() == result
    assert original_handle.get_result() == result
    # Each step should start twice, once originally and once in recovery.
    assert step_counter == 10

    # Rerun the workflow. Because each step is complete, none should start again.
    with SetWorkflowID(wfid):
        assert TestClass.test_workflow() == result
    assert step_counter == 10

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_inst_txn(dbos: DBOS) -> None:
    wfid = str(uuid.uuid4())

    @DBOS.dbos_class()
    class TestClass(DBOSConfiguredInstance):

        def __init__(self, multiplier: int) -> None:
            self.multiply: Callable[[int], int] = lambda x: x * multiplier
            super().__init__("test_class")

        @DBOS.transaction()
        def transaction(self, x: int) -> int:
            return self.multiply(x)

    input = 2
    multiplier = 5
    inst = TestClass(multiplier)

    with SetWorkflowID(wfid):
        assert inst.transaction(input) == input * multiplier
    status = DBOS.retrieve_workflow(wfid).get_status()
    assert status.class_name and "TestClass" in status.class_name
    assert status.config_name == "test_class"

    handle = DBOS.start_workflow(inst.transaction, input)
    assert handle.get_result() == input * multiplier
    status = handle.get_status()
    assert status.class_name and "TestClass" in status.class_name
    assert status.config_name == "test_class"


def test_mixed_methods(dbos: DBOS) -> None:

    @DBOS.dbos_class()
    class TestClass(DBOSConfiguredInstance):

        def __init__(self, multiplier: int) -> None:
            self.multiply: Callable[[int], int] = lambda x: x * multiplier
            super().__init__("test_class")

        @DBOS.workflow()
        def instance_workflow(self, x: int) -> int:
            return self.multiply(x)

        @classmethod
        @DBOS.workflow()
        def classmethod_workflow(cls, x: int) -> int:
            return x

        @staticmethod
        @DBOS.workflow()
        def staticmethod_workflow(x: int) -> int:
            return x

    input = 2
    multiplier = 5
    inst = TestClass(multiplier)

    handle = DBOS.start_workflow(inst.instance_workflow, input)
    assert handle.get_result() == input * multiplier
    status = handle.get_status()
    assert status.class_name and "TestClass" in status.class_name
    assert status.config_name == "test_class"

    handle = DBOS.start_workflow(inst.classmethod_workflow, input)
    assert handle.get_result() == input
    status = handle.get_status()
    assert status.class_name and "TestClass" in status.class_name
    assert status.config_name == None

    handle = DBOS.start_workflow(inst.staticmethod_workflow, input)
    assert handle.get_result() == input
    status = handle.get_status()
    assert status.class_name == None
    assert status.config_name == None


def test_class_step_without_dbos(dbos: DBOS, config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)

    @DBOS.dbos_class()
    class TestClass(DBOSConfiguredInstance):
        def __init__(self, x: int) -> None:
            self.x = x
            super().__init__("test")

        @DBOS.step()
        def step(self, x: int) -> int:
            return self.x + x

    input = 5
    inst = TestClass(input)
    assert inst.step(input) == input + input

    DBOS(config=config)

    assert inst.step(input) == input + input

    DBOS.launch()

    assert inst.step(input) == input + input


def test_class_with_only_steps(dbos: DBOS) -> None:

    class StepClass:
        def __init__(self, x: int) -> None:
            self.x = x

        @DBOS.step()
        def step(self, x: int, expr: Callable[[int, int], int]) -> int:
            return expr(self.x, x)

    input = 5
    inst = StepClass(5)

    l = lambda x, y: x + y

    @DBOS.workflow()
    def test_workflow() -> int:
        return inst.step(input, l) + inst.step(input, l)

    handle = DBOS.start_workflow(test_workflow)
    assert handle.get_result() == input * 4

    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(steps) == 2
    assert steps[1]["output"] == steps[1]["output"] == input * 2
