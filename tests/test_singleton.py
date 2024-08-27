import pytest

# Public API
from dbos import DBOS, SetWorkflowUUID

# Private API used because this is a test
from dbos.context import DBOSContextEnsure, assert_current_dbos_context
from tests.conftest import default_config


def test_dbos_singleton() -> None:
    # Initialize singleton
    DBOS.clear()  # In case of other tests leaving it
    dbos: DBOS = DBOS(None, default_config())

    assert dbos.config is not None
    assert DBOS().config is not None

    from tests.classdefs import DBOSTestClass, wfFunc

    assert DBOS().config is not None

    with SetWorkflowUUID("wfid"):
        res = wfFunc("f")
    assert res == "f1"

    res = DBOSTestClass.test_workflow_cls("a", "b")
    assert res == "b1a"

    inst = DBOSTestClass()
    res = inst.test_workflow("c", "d")
    assert res == "d1c"

    with DBOSContextEnsure():
        ctx = assert_current_dbos_context()
        ctx.authenticated_roles = ["user", "admin"]
        res = inst.test_func_admin("admin")
        assert res == "myconfig:admin"

    dbos.destroy()
