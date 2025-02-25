import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

# Public API
from dbos import (
    DBOS,
    ConfigFile,
    Queue,
    SetWorkflowID,
    WorkflowStatusString,
    _workflow_commands,
)


def test_basic(dbos: DBOS, config: ConfigFile) -> None:

    steps_completed = 0

    @DBOS.step()
    def step_one():
        nonlocal steps_completed
        steps_completed += 1
        print("Step one completed!")

    @DBOS.step()
    def step_two():
        nonlocal steps_completed
        steps_completed += 1
        print("Step two completed!")

    @DBOS.workflow()
    def simple_workflow() -> None:
        step_one()
        dbos.sleep(1)
        step_two()
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    time.sleep(1)  # wait for the workflow to complete
    assert (
        steps_completed == 2
    ), f"Expected steps_completed to be 2, but got {steps_completed}"


def test_two_steps_cancel(dbos: DBOS, config: ConfigFile) -> None:

    steps_completed = 0

    @DBOS.step()
    def step_one():
        nonlocal steps_completed
        steps_completed += 1
        print("Step one completed!")

    @DBOS.step()
    def step_two():
        nonlocal steps_completed
        steps_completed += 1
        print("Step two completed!")

    @DBOS.workflow()
    def simple_workflow() -> None:
        step_one()
        dbos.sleep(2)
        step_two()
        print("Executed Simple workflow")
        return

    # run the workflow
    try:
        wfuuid = str(uuid.uuid4())
        with SetWorkflowID(wfuuid):
            simple_workflow()

        dbos.cancel_workflow(wfuuid)
    except Exception as e:
        # time.sleep(1)  # wait for the workflow to complete
        assert (
            steps_completed == 1
        ), f"Expected steps_completed to be 1, but got {steps_completed}"

    dbos.resume_workflow(wfuuid)
    time.sleep(1)

    assert (
        steps_completed == 2
    ), f"Expected steps_completed to be 2, but got {steps_completed}"
