import time

import pytest

# Public API
from dbos import DBOS, SetWorkflowUUID, WorkflowHandle

# Private API used because this is a test
from dbos.context import DBOSContextEnsure, assert_current_dbos_context
from tests.conftest import default_config


def test_dbos_singleton() -> None:
    # Initialize singleton
    DBOS.clear_global_instance()  # In case of other tests leaving it

    from tests.classdefs import DBOSSendRecv, DBOSTestClass, DBOSTestRoles

    dbos: DBOS = DBOS(None, default_config())

    from tests.more_classdefs import DBOSWFEvents, wfFunc

    dbos.launch()  # Usually framework (fastapi) does this

    # Basics
    with SetWorkflowUUID("wfid"):
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
    stati = DBOS.get_workflow_status(wh.get_workflow_uuid())
    assert stati
    assert stati.config_name == "myconfig"
    assert stati.class_name == "DBOSTestClass"
    wfhr: WorkflowHandle[str] = DBOS.retrieve_workflow(wh.get_workflow_uuid())
    assert wfhr.workflow_uuid == wh.get_workflow_uuid()

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

    with SetWorkflowUUID(dest_uuid):
        handle = dbos.start_workflow(DBOSSendRecv.test_recv_workflow, "testtopic")
        assert handle.get_workflow_uuid() == dest_uuid

    send_uuid = str("sruuid2")
    with SetWorkflowUUID(send_uuid):
        res = DBOSSendRecv.test_send_workflow(handle.get_workflow_uuid(), "testtopic")
        assert res == dest_uuid

    begin_time = time.time()
    assert handle.get_result() == "test2-test1-test3"
    duration = time.time() - begin_time
    assert duration < 3.0  # Shouldn't take more than 3 seconds to run

    # Events
    wfuuid = str("sendwf1")
    with SetWorkflowUUID(wfuuid):
        DBOSWFEvents.test_setevent_workflow()
    with SetWorkflowUUID(wfuuid):
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

    dbos.destroy()