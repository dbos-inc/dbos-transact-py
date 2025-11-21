# mypy: disable-error-code="no-redef"
import pytest

from dbos import DBOS, DBOSConfig
from dbos._error import DBOSUnexpectedStepError


def test_patch(dbos: DBOS, config: DBOSConfig) -> None:

    @DBOS.step()
    def step_one() -> int:
        return 1

    @DBOS.step()
    def step_two() -> int:
        return 2

    @DBOS.step()
    def step_three() -> int:
        return 3

    @DBOS.workflow()
    def workflow() -> int:
        a = step_one()
        b = step_two()
        return a + b

    # Register and run the first version of a workflow
    handle = DBOS.start_workflow(workflow)
    v1_id = handle.workflow_id
    assert handle.get_result() == 3

    # Recreate DBOS with a new (patched) version of a workflow
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    step_one = DBOS.step()(step_one)
    step_two = DBOS.step()(step_two)
    step_three = DBOS.step()(step_three)

    @DBOS.workflow()
    def workflow() -> int:
        if DBOS.patch():
            a = step_three()
        else:
            a = step_one()
        b = step_two()
        return a + b

    DBOS.launch()

    # Verify a new execution runs the post-patch workflow
    # and stores a patch marker
    handle = DBOS.start_workflow(workflow)
    v2_id = handle.workflow_id
    assert handle.get_result() == 5
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(DBOS.list_workflow_steps(handle.workflow_id)) == 3
    assert steps[0]["function_name"] == "DBOS.patch"

    # Verify an execution containing the patch marker
    # can recover past the patch marker
    handle = DBOS.fork_workflow(v2_id, 3)
    assert handle.get_result() == 5
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(DBOS.list_workflow_steps(handle.workflow_id)) == 3
    assert steps[0]["function_name"] == "DBOS.patch"

    # Verify an old execution runs the pre-patch workflow
    # and does not store a patch marker
    handle = DBOS.fork_workflow(v1_id, 2)
    assert handle.get_result() == 3
    assert len(DBOS.list_workflow_steps(handle.workflow_id)) == 2

    # Recreate DBOS with a another (patched) version of a workflow
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    step_one = DBOS.step()(step_one)
    step_two = DBOS.step()(step_two)
    step_three = DBOS.step()(step_three)

    @DBOS.workflow()
    def workflow() -> int:
        if DBOS.patch("v3"):
            a = step_two()
        elif DBOS.patch():
            a = step_three()
        else:
            a = step_one()
        b = step_two()
        return a + b

    DBOS.launch()

    # Verify a new execution runs the post-patch workflow
    # and stores a patch marker
    handle = DBOS.start_workflow(workflow)
    v3_id = handle.workflow_id
    assert handle.get_result() == 4
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(DBOS.list_workflow_steps(handle.workflow_id)) == 3
    assert steps[0]["function_name"] == "DBOS.patch-v3"

    # Verify an execution containing the v3 patch marker
    # recovers to v3
    handle = DBOS.fork_workflow(v3_id, 3)
    assert handle.get_result() == 4
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(DBOS.list_workflow_steps(handle.workflow_id)) == 3
    assert steps[0]["function_name"] == "DBOS.patch-v3"

    # Verify an execution containing the v2 patch marker
    # recovers to v2
    handle = DBOS.fork_workflow(v2_id, 3)
    assert handle.get_result() == 5
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(DBOS.list_workflow_steps(handle.workflow_id)) == 3
    assert steps[0]["function_name"] == "DBOS.patch"

    # Verify a v1 execution recovers the pre-patch workflow
    # and does not store a patch marker
    handle = DBOS.fork_workflow(v1_id, 2)
    assert handle.get_result() == 3
    assert len(DBOS.list_workflow_steps(handle.workflow_id)) == 2

    # Now, let's deprecate the patch
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    step_one = DBOS.step()(step_one)
    step_two = DBOS.step()(step_two)
    step_three = DBOS.step()(step_three)

    @DBOS.workflow()
    def workflow() -> int:
        DBOS.deprecate_patch("v3")
        a = step_two()
        b = step_two()
        return a + b

    DBOS.launch()

    # Verify a new execution runs the final workflow
    # but does not store a patch marker
    handle = DBOS.start_workflow(workflow)
    v4_id = handle.workflow_id
    assert handle.get_result() == 4
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(DBOS.list_workflow_steps(handle.workflow_id)) == 2

    # Verify an execution containing the v3 patch marker
    # recovers to v3
    handle = DBOS.fork_workflow(v3_id, 3)
    assert handle.get_result() == 4
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(DBOS.list_workflow_steps(handle.workflow_id)) == 3
    assert steps[0]["function_name"] == "DBOS.patch-v3"

    # Verify an execution containing the v2 patch marker
    # cleanly fails
    handle = DBOS.fork_workflow(v2_id, 3)
    with pytest.raises(DBOSUnexpectedStepError):
        handle.get_result()

    # Verify a v1 execution cleanly fails
    handle = DBOS.fork_workflow(v1_id, 2)
    with pytest.raises(DBOSUnexpectedStepError):
        handle.get_result()
