import datetime
import logging
import threading
import time
import uuid
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
    print(output)
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"
    assert output[0]["workflow_uuid"].strip(), "field_name is an empty string"

    print("Test successful")
