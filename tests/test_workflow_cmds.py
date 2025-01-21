import logging
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import List, cast

import pytest
import sqlalchemy as sa

# Public API
from dbos import (
    DBOS,
    ConfigFile,
    SetWorkflowID,
    WorkflowHandle,
    WorkflowStatusString,
    _workflow_commands,
)


def test_list_workflow(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing list_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    output = _workflow_commands._list_workflows(
        config, 10, None, None, None, None, False, None
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"
    assert output[0] != None, "Expected output to be not None"
    if output[0] != None:
        assert output[0].workflowUUID.strip(), "field_name is an empty string"


def test_list_workflow_limit(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing list_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    simple_workflow()
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    output = _workflow_commands._list_workflows(
        config, 2, None, None, None, None, False, None
    )
    assert len(output) == 2, f"Expected list length to be 1, but got {len(output)}"


def test_list_workflow_status(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing list_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    output = _workflow_commands._list_workflows(
        config, 10, None, None, None, "PENDING", False, None
    )
    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"

    output = _workflow_commands._list_workflows(
        config, 10, None, None, None, "SUCCESS", False, None
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"


def test_list_workflow_start_end_times(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing list_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    now = datetime.now()
    starttime = (now - timedelta(seconds=20)).isoformat()
    print(starttime)

    # run the workflow
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    endtime = datetime.now().isoformat()
    print(endtime)

    output = _workflow_commands._list_workflows(
        config, 10, None, starttime, endtime, None, False, None
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    newstarttime = (now - timedelta(seconds=30)).isoformat()
    newendtime = starttime

    output = _workflow_commands._list_workflows(
        config, 10, None, newstarttime, newendtime, None, False, None
    )
    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"


def test_list_workflow_end_times_positive(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing list_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    now = datetime.now()

    time_0 = (now - timedelta(seconds=40)).isoformat()

    time_1 = (now - timedelta(seconds=20)).isoformat()

    # run the workflow
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    time_2 = datetime.now().isoformat()

    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    time_3 = datetime.now().isoformat()

    output = _workflow_commands._list_workflows(
        config, 10, None, time_0, time_1, None, False, None
    )

    assert len(output) == 0, f"Expected list length to be 0, but got {len(output)}"

    output = _workflow_commands._list_workflows(
        config, 10, None, time_1, time_2, None, False, None
    )

    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    output = _workflow_commands._list_workflows(
        config, 10, None, time_1, time_3, None, False, None
    )
    assert len(output) == 2, f"Expected list length to be 2, but got {len(output)}"


def test_get_workflow(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing get_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    output = _workflow_commands._list_workflows(
        config, 10, None, None, None, None, False, None
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    assert output[0] != None, "Expected output to be not None"

    wfUuid = output[0].workflowUUID

    info = _workflow_commands._get_workflow(config, wfUuid, True)
    assert info is not None, "Expected output to be not None"

    if info is not None:
        assert info.workflowUUID == wfUuid, f"Expected workflow_uuid to be {wfUuid}"


def test_cancel_workflow(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing get_workflow")

    @DBOS.workflow()
    def simple_workflow() -> None:
        time.sleep(3)
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    # get the workflow list
    output = _workflow_commands._list_workflows(
        config, 10, None, None, None, None, False, None
    )
    # assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    print(output[0])
    assert output[0] != None, "Expected output to be not None"
    wfUuid = output[0].workflowUUID

    _workflow_commands._cancel_workflow(config, wfUuid)

    info = _workflow_commands._get_workflow(config, wfUuid, True)
    assert info is not None, "Expected info to be not None"
    if info is not None:
        assert info.status == "CANCELLED", f"Expected status to be CANCELLED"
