import threading
import time
import uuid

import pytest

# Public API
from dbos import DBOS, ConfigFile, SetWorkflowID


def test_cancel_resume(dbos: DBOS, config: ConfigFile) -> None:
    steps_completed = 0
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.step()
    def step_one() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.transaction()
    def step_two() -> None:
        nonlocal steps_completed
        steps_completed += 1

    @DBOS.workflow()
    def simple_workflow() -> None:
        step_one()
        main_thread_event.set()
        workflow_event.wait()
        step_two()

    # Start the workflow and cancel it.
    # Verify it stops after step one but before step two
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(simple_workflow)
    DBOS.cancel_workflow(wfid)
    workflow_event.set()
    with pytest.raises(Exception):
        handle.get_result()
    assert steps_completed == 1

    # Resume the workflow. Verify it completes successfully.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == None
    assert steps_completed == 2

    # Resume the workflow again. Verify it does not run again.
    handle = DBOS.resume_workflow(wfid)
    assert handle.get_result() == None
    assert steps_completed == 2
