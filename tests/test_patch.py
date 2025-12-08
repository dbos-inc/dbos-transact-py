# mypy: disable-error-code="no-redef"
import pytest

from dbos import DBOS, DBOSConfig
from dbos._error import DBOSUnexpectedStepError


def test_patch(dbos: DBOS, config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    config["enable_patching"] = True
    test_version = "test_version"
    config["application_version"] = test_version
    DBOS(config=config)

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

    DBOS.launch()
    assert DBOS.application_version == test_version

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
        if DBOS.patch("v2"):
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
    assert steps[0]["function_name"] == "DBOS.patch-v2"

    # Verify an execution containing the patch marker
    # can recover past the patch marker
    handle = DBOS.fork_workflow(v2_id, 3)
    assert handle.get_result() == 5
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(DBOS.list_workflow_steps(handle.workflow_id)) == 3
    assert steps[0]["function_name"] == "DBOS.patch-v2"

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
        elif DBOS.patch("v2"):
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
    assert steps[0]["function_name"] == "DBOS.patch-v2"

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

    # Verify an execution sans patch marker recovers correctly
    handle = DBOS.fork_workflow(v4_id, 3)
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

    # Finally, let's remove the patch
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    step_one = DBOS.step()(step_one)
    step_two = DBOS.step()(step_two)
    step_three = DBOS.step()(step_three)

    @DBOS.workflow()
    def workflow() -> int:
        a = step_two()
        b = step_two()
        return a + b

    DBOS.launch()

    # Verify an execution from the deprecated patch works
    # sans patch marker
    handle = DBOS.fork_workflow(v4_id, 3)
    assert handle.get_result() == 4
    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(DBOS.list_workflow_steps(handle.workflow_id)) == 2

    # Verify an execution containing the v3 patch marker
    # cleanly fails
    handle = DBOS.fork_workflow(v3_id, 3)
    with pytest.raises(DBOSUnexpectedStepError):
        handle.get_result()

    # Verify an execution containing the v2 patch marker
    # cleanly fails
    handle = DBOS.fork_workflow(v2_id, 3)
    with pytest.raises(DBOSUnexpectedStepError):
        handle.get_result()

    # Verify a v1 execution cleanly fails
    handle = DBOS.fork_workflow(v1_id, 2)
    with pytest.raises(DBOSUnexpectedStepError):
        handle.get_result()


@pytest.mark.asyncio
async def test_patch_async(dbos: DBOS, config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    config["enable_patching"] = True
    DBOS(config=config)

    @DBOS.step()
    async def step_one() -> int:
        return 1

    @DBOS.step()
    async def step_two() -> int:
        return 2

    @DBOS.step()
    async def step_three() -> int:
        return 3

    @DBOS.workflow()
    async def workflow() -> int:
        a = await step_one()
        b = await step_two()
        return a + b

    DBOS.launch()

    # Register and run the first version of a workflow
    handle = await DBOS.start_workflow_async(workflow)
    v1_id = handle.workflow_id
    assert await handle.get_result() == 3

    # Recreate DBOS with a new (patched) version of a workflow
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    step_one = DBOS.step()(step_one)
    step_two = DBOS.step()(step_two)
    step_three = DBOS.step()(step_three)

    @DBOS.workflow()
    async def workflow() -> int:
        if await DBOS.patch_async("v2"):
            a = await step_three()
        else:
            a = await step_one()
        b = await step_two()
        return a + b

    DBOS.launch()
    assert DBOS.application_version == "PATCHING_ENABLED"

    # Verify a new execution runs the post-patch workflow
    # and stores a patch marker
    handle = await DBOS.start_workflow_async(workflow)
    assert await handle.get_result() == 5
    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    assert len(await DBOS.list_workflow_steps_async(handle.workflow_id)) == 3
    assert steps[0]["function_name"] == "DBOS.patch-v2"

    # Verify an old execution runs the pre-patch workflow
    # and does not store a patch marker
    handle = await DBOS.fork_workflow_async(v1_id, 2)
    assert await handle.get_result() == 3
    assert len(await DBOS.list_workflow_steps_async(handle.workflow_id)) == 2

    # Now, let's deprecate the patch
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    step_one = DBOS.step()(step_one)
    step_two = DBOS.step()(step_two)
    step_three = DBOS.step()(step_three)

    @DBOS.workflow()
    async def workflow() -> int:
        await DBOS.deprecate_patch_async("v3")
        a = await step_two()
        b = await step_two()
        return a + b

    DBOS.launch()

    # Verify a new execution runs the final workflow
    # but does not store a patch marker
    handle = await DBOS.start_workflow_async(workflow)
    assert await handle.get_result() == 4
    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    assert len(await DBOS.list_workflow_steps_async(handle.workflow_id)) == 2
