import time

import pytest

# Public API
from dbos import DBOS, SetWorkflowID, WorkflowHandle

# Private API used because this is a test
from dbos._context import DBOSContextEnsure, assert_current_dbos_context
from tests.conftest import default_config


def test_dbos_singleton(cleanup_test_databases: None) -> None:
    # Initialize singleton
    DBOS.destroy()  # In case of other tests leaving it

    # Simulate an app that does some imports of its own code, then defines DBOS,
    #    then imports more
    from tests.classdefs import (
        DBOSSendRecv,
        DBOSTestClass,
        DBOSTestRoles,
        DBOSTestWrapperMethods,
    )

    dbos: DBOS = DBOS(config=default_config())

    from tests.more_classdefs import DBOSWFEvents, wfFunc

    DBOS.launch()  # Usually framework (fastapi) does this via lifecycle event

    # Basics
    with SetWorkflowID("wfid"):
        res = wfFunc("f")
    assert res == "f1"

    res = DBOSTestClass.test_workflow_cls("a", "b")
    assert res == "b1a"

    inst = DBOSTestClass()
    res = inst.test_workflow("c", "d")
    assert res == "d1c"

    wh = DBOS.start_workflow(DBOSTestClass.test_workflow_cls, "a", "b")
    assert wh.get_result() == "b1a"

    wh = DBOS.start_workflow(inst.test_workflow, "c", "d")
    assert wh.get_result() == "d1c"
    stati = DBOS.get_workflow_status(wh.get_workflow_id())
    assert stati
    assert stati.config_name == "myconfig"
    assert stati.class_name and "DBOSTestClass" in stati.class_name
    wfhr: WorkflowHandle[str] = DBOS.retrieve_workflow(wh.get_workflow_id())
    assert wfhr.workflow_id == wh.get_workflow_id()

    # Test wrapper class methods
    wrapper_inst = DBOSTestWrapperMethods("dynamicconfig")
    res = wrapper_inst.test_workflow("x", "y")
    assert res == "y1x"

    wh = DBOS.start_workflow(wrapper_inst.test_workflow, "x", "y")
    assert wh.get_result() == "y1x"
    stati = DBOS.get_workflow_status(wh.get_workflow_id())
    assert stati
    assert stati.config_name is None
    assert stati.class_name is None
    assert stati.name == "dynamicconfig_test_workflow"
    wfhr = DBOS.retrieve_workflow(wh.get_workflow_id())
    assert wfhr.workflow_id == wh.get_workflow_id()

    stepi = DBOS.list_workflow_steps(wh.get_workflow_id())
    assert len(stepi) == 2
    assert stepi[0]["function_name"] == "dynamicconfig_test_transaction"
    assert stepi[1]["function_name"] == "dynamicconfig_test_step"

    # Roles

    with DBOSContextEnsure():
        with pytest.raises(Exception) as exc_info:
            DBOSTestRoles.greetfunc("Nobody")
        assert "DBOS Error 8" in str(exc_info.value)

        ctx = assert_current_dbos_context()
        ctx.authenticated_roles = ["user", "admin"]
        res = inst.test_func_admin("admin")
        assert res == "myconfig:admin"

        assert DBOSTestRoles.greetfunc("user") == "Hello user"

    # Send / Recv
    dest_uuid = str("sruuid1")

    with SetWorkflowID(dest_uuid):
        handle = dbos.start_workflow(DBOSSendRecv.test_recv_workflow, "testtopic")
        assert handle.get_workflow_id() == dest_uuid

    send_uuid = str("sruuid2")
    with SetWorkflowID(send_uuid):
        res = DBOSSendRecv.test_send_workflow(handle.get_workflow_id(), "testtopic")
        assert res == dest_uuid

    begin_time = time.time()
    assert handle.get_result() == "test2-test1-test3"
    duration = time.time() - begin_time
    assert duration < 3.0

    # Events
    wfuuid = str("sendwf1")
    with SetWorkflowID(wfuuid):
        DBOSWFEvents.test_setevent_workflow()
    with SetWorkflowID(wfuuid):
        DBOSWFEvents.test_setevent_workflow()

    value1 = DBOSWFEvents.test_getevent_workflow(wfuuid, "key1")
    assert value1 == "value1"

    value2 = DBOSWFEvents.test_getevent_workflow(wfuuid, "key2")
    assert value2 == "value2"

    # Run getEvent outside of a workflow
    value1 = DBOS.get_event(wfuuid, "key1")
    assert value1 == "value1"

    value2 = DBOS.get_event(wfuuid, "key2")
    assert value2 == "value2"

    begin_time = time.time()
    value3 = DBOSWFEvents.test_getevent_workflow(wfuuid, "key3")
    assert value3 is None
    duration = time.time() - begin_time
    assert duration < 1  # None is from the event not from the timeout

    DBOS.destroy()


def test_dbos_singleton_negative(cleanup_test_databases: None) -> None:
    # Initialize singleton
    DBOS.destroy()  # In case of other tests leaving it

    # Simulate an app that does some imports of its own code, then defines DBOS,
    #    then imports more
    from tests.classdefs import DBOSTestClass

    DBOS(config=default_config())

    # Something should have launched
    with pytest.raises(Exception) as exc_info:
        DBOSTestClass.test_workflow_cls("a", "b")
    assert "launch" in str(exc_info.value)

    DBOS.destroy()
