import threading
import time
from datetime import datetime, timedelta, timezone

# Public API
from dbos import DBOS, ConfigFile, Queue, WorkflowStatusString, _workflow_commands


def test_basic(dbos: DBOS, config: ConfigFile) -> None:
    print("Testing list_workflow")

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
