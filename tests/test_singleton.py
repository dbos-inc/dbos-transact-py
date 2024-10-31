import os
import subprocess
import sys
import time
from os import path

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
    from tests.classdefs import DBOSSendRecv, DBOSTestClass, DBOSTestRoles

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
    assert stati.class_name == "DBOSTestClass"
    wfhr: WorkflowHandle[str] = DBOS.retrieve_workflow(wh.get_workflow_id())
    assert wfhr.workflow_id == wh.get_workflow_id()

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

    dbos: DBOS = DBOS(config=default_config())

    # Don't initialize DBOS twice
    with pytest.raises(Exception) as exc_info:
        DBOS(config=default_config())
    assert "conflicting configuration" in str(exc_info.value)

    # Something should have launched
    with pytest.raises(Exception) as exc_info:
        DBOSTestClass.test_workflow_cls("a", "b")
    assert "launch" in str(exc_info.value)

    DBOS.destroy()


config_string = """name: test-app
language: python
database:
  hostname: localhost
  port: 5432
  username: postgres
  password: ${PGPASSWORD}
  app_db_name: dbostestpy
runtimeConfig:
  start:
    - python3 main.py
application:
  service_url: 'https://service.org'
  service_config:
    port: 80
    user: "user"
    password: "password"
"""


def test_config_before_singleton(cleanup_test_databases: None) -> None:
    # Initialize singleton
    DBOS.destroy()  # In case of other tests leaving it

    # Write the config to a text file for the moment
    with open("dbos-config.yaml", "w") as file:
        file.write(config_string)

    try:
        # Simulate an app that does some imports of its own code, then defines DBOS,
        #    then imports more
        from tests.classdefs import DBOSTestClass

        x = DBOS.config.get("language")
        assert x == "python"
        y = DBOS.config["language"]
        assert y == "python"
        url = DBOS.config["application"]["service_url"]
        assert url == "https://service.org"
        port = DBOS.config["application"]["service_config"]["port"]
        assert port == 80

        # This is OK, it meant load_config anyway
        dbos: DBOS = DBOS()

    finally:
        # Initialize singleton
        DBOS.destroy()  # In case of other tests leaving it
        os.remove("dbos-config.yaml")


def test_config_before_singleton_negative(cleanup_test_databases: None) -> None:
    # Initialize singleton
    DBOS.destroy()  # In case of other tests leaving it

    # Write the config to a text file for the moment
    with open("dbos-config.yaml", "w") as file:
        file.write(config_string)

    try:
        x = DBOS.config.get("language")
        assert x == "python"

        # Not OK, config already loaded in the default way
        with pytest.raises(Exception) as exc_info:
            DBOS(config=default_config())
        assert "configured multiple" in str(exc_info.value)
    finally:
        DBOS.destroy()
        os.remove("dbos-config.yaml")


def test_dbos_atexit_no_dbos(cleanup_test_databases: None) -> None:
    # Run the .py as a separate process
    result = subprocess.run(
        [sys.executable, path.join("tests", "atexit_no_ctor.py")],
        capture_output=True,
        text=True,
    )

    # Assert that the output contains the warning message
    assert "DBOS exiting; functions were registered" in result.stdout


def test_dbos_atexit_no_launch(cleanup_test_databases: None) -> None:
    # Run the .py as a separate process
    result = subprocess.run(
        [sys.executable, path.join("tests", "atexit_no_launch.py")],
        capture_output=True,
        text=True,
    )

    # Assert that the output contains the warning message
    assert "DBOS exists but launch() was not called" in result.stdout
