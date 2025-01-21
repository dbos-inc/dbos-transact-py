import sqlalchemy as sa

# Public API
from dbos import DBOS, DBOSConfiguredInstance


@DBOS.dbos_class()
class DBOSTestClass(DBOSConfiguredInstance):
    txn_counter_c = 0
    wf_counter_c = 0
    step_counter_c = 0

    def __init__(self) -> None:
        super().__init__("myconfig")
        self.txn_counter: int = 0
        self.wf_counter: int = 0
        self.step_counter: int = 0

    @classmethod
    @DBOS.workflow()
    def test_workflow_cls(cls, var: str, var2: str) -> str:
        cls.wf_counter_c += 1
        res = DBOSTestClass.test_transaction_cls(var2)
        res2 = DBOSTestClass.test_step_cls(var)
        return res + res2

    @classmethod
    @DBOS.transaction()
    def test_transaction_cls(cls, var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        cls.txn_counter_c += 1
        return var2 + str(rows[0][0])

    @classmethod
    @DBOS.step()
    def test_step_cls(cls, var: str) -> str:
        cls.step_counter_c += 1
        return var

    @DBOS.workflow()
    def test_workflow(self, var: str, var2: str) -> str:
        self.wf_counter += 1
        res = self.test_transaction(var2)
        res2 = self.test_step(var)
        return res + res2

    @DBOS.transaction()
    def test_transaction(self, var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        self.txn_counter += 1
        return var2 + str(rows[0][0])

    @DBOS.step()
    def test_step(self, var: str) -> str:
        self.step_counter += 1
        return var

    @DBOS.workflow()
    @DBOS.required_roles(["admin"])
    def test_func_admin(self, var: str) -> str:
        assert DBOS.assumed_role == "admin"
        return self.config_name + ":" + var


@DBOS.default_required_roles(["user"])
class DBOSTestRoles(DBOSConfiguredInstance):
    @staticmethod
    @DBOS.workflow()
    def greetfunc(name: str) -> str:
        return f"Hello {name}"


@DBOS.dbos_class()
class DBOSSendRecv:
    send_counter: int = 0
    recv_counter: int = 0

    @staticmethod
    @DBOS.workflow()
    def test_send_workflow(dest_uuid: str, topic: str) -> str:
        DBOS.send(dest_uuid, "test1")
        DBOS.send(dest_uuid, "test2", topic=topic)
        DBOS.send(dest_uuid, "test3")
        DBOSSendRecv.send_counter += 1
        return dest_uuid

    @staticmethod
    @DBOS.workflow()
    def test_recv_workflow(topic: str) -> str:
        msg1 = DBOS.recv(topic, timeout_seconds=10)
        msg2 = DBOS.recv(timeout_seconds=10)
        msg3 = DBOS.recv(timeout_seconds=10)
        DBOSSendRecv.recv_counter += 1
        return "-".join([str(msg1), str(msg2), str(msg3)])
