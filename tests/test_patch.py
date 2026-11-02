# mypy: disable-error-code="no-redef"
import asyncio
import logging
import uuid
from typing import Any, Coroutine

import pytest
import sqlalchemy as sa

from dbos import DBOS, DBOSConfig, SetWorkflowID
from dbos._dbos import WorkflowHandleAsync, _get_dbos_instance
from dbos._error import DBOSAwaitedWorkflowCancelledError, DBOSUnexpectedStepError
from dbos._schemas.system_database import SystemSchema

from .conftest import set_workflow_status


def test_patch(
    dbos: DBOS, config: DBOSConfig, skip_with_sqlite_imprecise_time: None
) -> None:
    # Patching needs no flag: a fixed application_version is enough for every
    # deployment to recover and dequeue each other's workflows.
    DBOS.destroy(destroy_registry=True)
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
async def test_patch_async(
    dbos: DBOS,
    config: DBOSConfig,
    skip_with_sqlite_imprecise_time: None,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The deprecated flag still pins the application version, and says it is deprecated.
    DBOS.destroy(destroy_registry=True)
    config["enable_patching"] = True
    caplog.set_level(logging.WARNING, "dbos")
    monkeypatch.setattr(logging.getLogger("dbos"), "propagate", True)
    DBOS(config=config)
    assert "enable_patching is deprecated" in caplog.text

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


def _simulate_crash_at(dbos_instance: DBOS, workflow_id: str, position: int) -> None:
    """Leave the history as a crash would while position was still running:
    its checkpoint gone, later ones in place, and the workflow pending."""
    sys_db = dbos_instance._sys_db
    with sys_db.engine.begin() as c:
        c.execute(
            sa.delete(SystemSchema.operation_outputs).where(
                SystemSchema.operation_outputs.c.workflow_uuid == workflow_id,
                SystemSchema.operation_outputs.c.function_id == position,
            )
        )
    set_workflow_status(sys_db, workflow_id, "PENDING")


def test_patch_before_waiting_recv(dbos: DBOS, config: DBOSConfig) -> None:
    # A recv checkpoints its timeout sleep, at the position after its own,
    # while it waits. A patch added before that recv finds the recv's position
    # empty on recovery; it must read the sleep past it as "already passed",
    # not insert a marker that shifts the recv onto the sleep's position.
    DBOS.destroy(destroy_registry=True)
    config["application_version"] = "test_version"
    DBOS(config=config)

    @DBOS.step()
    def step_one() -> int:
        return 1

    @DBOS.workflow()
    def workflow() -> str:
        step_one()
        return str(DBOS.recv("approval", timeout_seconds=60))

    DBOS.launch()

    # v1: step_one at 1, recv at 2, its timeout sleep at 3.
    v1_id = str(uuid.uuid4())
    with SetWorkflowID(v1_id):
        handle = DBOS.start_workflow(workflow)
    DBOS.send(v1_id, "first", topic="approval")
    assert handle.get_result() == "first"
    names = [s["function_name"] for s in DBOS.list_workflow_steps(v1_id)]
    assert names[1:] == ["DBOS.recv", "DBOS.sleep"]

    # The process died while the recv waited: positions 1 and 3 recorded, 2 not.
    _simulate_crash_at(_get_dbos_instance(), v1_id, 2)

    # v2 adds a patch between the step and the recv.
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    step_one = DBOS.step()(step_one)
    patched_runs: list[str] = []

    @DBOS.workflow()
    def workflow() -> str:
        step_one()
        if DBOS.patch("before-wait"):
            patched_runs.append(str(DBOS.workflow_id))
        return str(DBOS.recv("approval", timeout_seconds=60))

    # Launch recovers the v1 execution, which resumes on the old path.
    DBOS.launch()
    DBOS.send(v1_id, "second", topic="approval")
    assert DBOS.retrieve_workflow(v1_id).get_result() == "second"
    assert patched_runs == []
    names = [s["function_name"] for s in DBOS.list_workflow_steps(v1_id)]
    assert names[1:] == ["DBOS.recv", "DBOS.sleep"]

    # A new execution takes the patch and records its marker before the recv.
    v2_id = str(uuid.uuid4())
    with SetWorkflowID(v2_id):
        handle = DBOS.start_workflow(workflow)
    DBOS.send(v2_id, "third", topic="approval")
    assert handle.get_result() == "third"
    assert patched_runs == [v2_id]
    names = [s["function_name"] for s in DBOS.list_workflow_steps(v2_id)]
    assert names[1:] == ["DBOS.patch-before-wait", "DBOS.recv", "DBOS.sleep"]


async def _wait_all(*aws: Coroutine[Any, Any, int]) -> int:
    done, pending = await DBOS.asyncio_wait(
        list(aws), return_when=asyncio.ALL_COMPLETED
    )
    assert not pending
    return sum(t.result() for t in done)


async def _wait_for_checkpoint(workflow_id: str, name_suffix: str) -> None:
    for _ in range(200):
        steps = await DBOS.list_workflow_steps_async(workflow_id)
        if any(s["function_name"].endswith(name_suffix) for s in steps):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"no checkpoint named *{name_suffix} for {workflow_id}")


@pytest.mark.asyncio
async def test_patch_async_before_concurrent_steps(
    dbos: DBOS, config: DBOSConfig
) -> None:
    # Concurrent steps take their positions up front and checkpoint in any
    # order, so an execution can stop with a later position recorded and an
    # earlier one empty. A patch added before them must treat the later
    # checkpoint as proof the execution passed that point on the old code.
    DBOS.destroy(destroy_registry=True)
    config["application_version"] = "test_version"
    DBOS(config=config)

    step_one_started = asyncio.Event()
    step_one_release = asyncio.Event()
    step_one_cancelled = asyncio.Event()

    @DBOS.step(preemptible=True)
    async def step_one() -> int:
        step_one_started.set()
        try:
            await step_one_release.wait()
        except asyncio.CancelledError:
            step_one_cancelled.set()
            raise
        return 1

    @DBOS.step()
    async def step_two() -> int:
        return 2

    @DBOS.workflow()
    async def workflow() -> int:
        return await _wait_all(step_one(), step_two())

    DBOS.launch()

    # v1: asyncio_wait takes position 1, the steps 2 and 3. step_two
    # checkpoints while step_one is still running, then the workflow is
    # cancelled, which preempts step_one: positions 1 and 2 stay empty.
    v1_id = str(uuid.uuid4())
    with SetWorkflowID(v1_id):
        handle = await DBOS.start_workflow_async(workflow)
    await step_one_started.wait()
    await _wait_for_checkpoint(v1_id, "step_two")
    await DBOS.cancel_workflow_async(v1_id)
    await step_one_cancelled.wait()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await handle.get_result()
    names = [s["function_name"] for s in await DBOS.list_workflow_steps_async(v1_id)]
    assert len(names) == 1 and names[0].endswith("step_two")

    # v2 adds a patch before the concurrent steps.
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    step_one = DBOS.step(preemptible=True)(step_one)
    step_two = DBOS.step()(step_two)
    patched_runs: list[str] = []

    @DBOS.workflow()
    async def workflow() -> int:
        if await DBOS.patch_async("before-steps"):
            patched_runs.append(str(DBOS.workflow_id))
        return await _wait_all(step_one(), step_two())

    DBOS.launch()

    # Resumed on v2, the v1 execution stays on the old path: the wait and
    # step_one take positions 1 and 2, step_two replays at 3.
    step_one_release.set()
    resumed: WorkflowHandleAsync[int] = await DBOS.resume_workflow_async(v1_id)
    assert await resumed.get_result() == 3
    assert patched_runs == []
    names = [s["function_name"] for s in await DBOS.list_workflow_steps_async(v1_id)]
    assert len(names) == 3
    assert names[0] == "DBOS.asyncio_wait"
    assert names[1].endswith("step_one")
    assert names[2].endswith("step_two")

    # A new execution takes the patch and records its marker first.
    v2_id = str(uuid.uuid4())
    with SetWorkflowID(v2_id):
        handle = await DBOS.start_workflow_async(workflow)
    assert await handle.get_result() == 3
    assert patched_runs == [v2_id]
    names = [s["function_name"] for s in await DBOS.list_workflow_steps_async(v2_id)]
    assert len(names) == 4
    assert names[0] == "DBOS.patch-before-steps"
