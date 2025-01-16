import logging
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import List

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
    def simple_workflow():
        print("Executed Simple workflow")

    # run the workflow
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    # get the workflow list
    output = _workflow_commands._list_workflows(
        config, 10, None, None, None, None, False, None
    )
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"
    assert output[0]["workflow_uuid"].strip(), "field_name is an empty string"


def test_list_workflow_limit(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing list_workflow")

    @DBOS.workflow()
    def simple_workflow():
        print("Executed Simple workflow")

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
    def simple_workflow():
        print("Executed Simple workflow")

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
    def simple_workflow():
        print("Executed Simple workflow")

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
