"""Public DBOS methods called from a workflow are either checkpointed as steps,
refused, or deterministic. These tests cover the methods that used to be none
of the three."""

import asyncio
import time
import uuid
from datetime import datetime
from typing import Any, List, Optional, Tuple

import pytest

from dbos import DBOS, DBOSConfig
from dbos._error import DBOSException
from dbos._serialization import DBOSPortableJSONSerializer


def step_names(workflow_id: str) -> List[str]:
    return [s["function_name"] for s in DBOS.list_workflow_steps(workflow_id)]


async def step_names_async(workflow_id: str) -> List[str]:
    steps = await DBOS.list_workflow_steps_async(workflow_id)
    return [s["function_name"] for s in steps]


def test_queue_api_is_checkpointed(dbos: DBOS) -> None:
    queue_name = f"durable-q-{uuid.uuid4()}"

    @DBOS.workflow()
    def queue_workflow() -> Tuple[str, Optional[int], bool]:
        registered = DBOS.register_queue(queue_name, global_concurrency=3)
        retrieved = DBOS.retrieve_queue(queue_name)
        listed = any(q.name == queue_name for q in DBOS.list_queues())
        DBOS.delete_queue(queue_name)
        assert retrieved is not None
        # Queue's public getters re-read the live row, so check the checkpointed value.
        return registered.name, retrieved._concurrency, listed

    handle = DBOS.start_workflow(queue_workflow)
    assert handle.get_result() == (queue_name, 3, True)
    expected = [
        "DBOS.registerQueue",
        "DBOS.retrieveQueue",
        "DBOS.listQueues",
        "DBOS.deleteQueue",
    ]
    assert step_names(handle.workflow_id) == expected
    assert DBOS.retrieve_queue(queue_name) is None

    # The queue is gone, but a replay returns the recorded reads.
    forked = DBOS.fork_workflow(handle.workflow_id, len(expected))
    assert forked.get_result() == (queue_name, 3, True)
    assert step_names(forked.workflow_id) == expected


def test_queue_api_checkpoints_under_portable_serializer(
    config: DBOSConfig, cleanup_test_databases: None
) -> None:
    # Queue results are checkpointed as plain data, so JSON serializers store them.
    DBOS.destroy(destroy_registry=True)
    config["serializer"] = DBOSPortableJSONSerializer()
    DBOS(config=config)
    DBOS.launch()
    try:
        queue_name = f"durable-q-{uuid.uuid4()}"
        limiter = {"limit": 5, "period": 1.5}

        @DBOS.workflow()
        def queue_workflow() -> Tuple[str, str, Any, bool]:
            registered = DBOS.register_queue(
                queue_name, global_concurrency=3, limiter={"limit": 5, "period": 1.5}
            )
            retrieved = DBOS.retrieve_queue(queue_name)
            assert retrieved is not None
            listed = any(q.name == queue_name for q in DBOS.list_queues())
            return registered.name, retrieved.name, retrieved._limiter, listed

        handle = DBOS.start_workflow(queue_workflow)
        # JSON returns the workflow's tuple as a list.
        assert tuple(handle.get_result()) == (queue_name, queue_name, limiter, True)

        # The replay rebuilds the queues from the checkpointed data.
        forked = DBOS.fork_workflow(handle.workflow_id, 4)
        assert tuple(forked.get_result()) == (queue_name, queue_name, limiter, True)
        retrieved = DBOS.retrieve_queue(queue_name)
        assert retrieved is not None and retrieved.global_concurrency == 3
    finally:
        DBOS.destroy(destroy_registry=True)


@pytest.mark.asyncio
async def test_queue_api_async_is_checkpointed(dbos: DBOS) -> None:
    queue_name = f"durable-q-{uuid.uuid4()}"

    @DBOS.workflow()
    async def queue_workflow() -> Tuple[str, Optional[int], bool]:
        registered = await DBOS.register_queue_async(queue_name, global_concurrency=3)
        retrieved = await DBOS.retrieve_queue_async(queue_name)
        listed = any(q.name == queue_name for q in await DBOS.list_queues_async())
        await DBOS.delete_queue_async(queue_name)
        assert retrieved is not None
        # Queue's public getters re-read the live row, so check the checkpointed value.
        return registered.name, retrieved._concurrency, listed

    handle = await DBOS.start_workflow_async(queue_workflow)
    assert await handle.get_result() == (queue_name, 3, True)
    expected = [
        "DBOS.registerQueue",
        "DBOS.retrieveQueue",
        "DBOS.listQueues",
        "DBOS.deleteQueue",
    ]
    assert await step_names_async(handle.workflow_id) == expected

    forked = await DBOS.fork_workflow_async(handle.workflow_id, len(expected))
    assert await forked.get_result() == (queue_name, 3, True)


def test_application_version_api_is_checkpointed(dbos: DBOS) -> None:
    current = DBOS.application_version

    @DBOS.workflow()
    def version_workflow() -> Tuple[bool, str]:
        DBOS.set_latest_application_version(current)
        listed = any(
            v["version_name"] == current for v in DBOS.list_application_versions()
        )
        return listed, DBOS.get_latest_application_version()["version_name"]

    handle = DBOS.start_workflow(version_workflow)
    assert handle.get_result() == (True, current)
    assert step_names(handle.workflow_id) == [
        "DBOS.setLatestApplicationVersion",
        "DBOS.listApplicationVersions",
        "DBOS.getLatestApplicationVersion",
    ]
    promoted_at = DBOS.get_latest_application_version()["version_timestamp"]

    # Replaying the workflow does not promote its version again: the timestamp
    # it wrote stands, rather than moving to the replay's clock.
    time.sleep(0.01)
    forked = DBOS.fork_workflow(handle.workflow_id, 3)
    assert forked.get_result() == (True, current)
    assert step_names(forked.workflow_id) == step_names(handle.workflow_id)
    assert DBOS.get_latest_application_version()["version_timestamp"] == promoted_at

    # Replaying from the start does run the promotion again, at a later time.
    refork = DBOS.fork_workflow(handle.workflow_id, 1)
    assert refork.get_result() == (True, current)
    assert DBOS.get_latest_application_version()["version_timestamp"] > promoted_at


@pytest.mark.asyncio
async def test_application_version_api_async_is_checkpointed(dbos: DBOS) -> None:
    current = DBOS.application_version

    @DBOS.workflow()
    async def version_workflow() -> str:
        await DBOS.set_latest_application_version_async(current)
        await DBOS.list_application_versions_async()
        return (await DBOS.get_latest_application_version_async())["version_name"]

    handle = await DBOS.start_workflow_async(version_workflow)
    assert await handle.get_result() == current
    assert await step_names_async(handle.workflow_id) == [
        "DBOS.setLatestApplicationVersion",
        "DBOS.listApplicationVersions",
        "DBOS.getLatestApplicationVersion",
    ]


@pytest.mark.asyncio
async def test_concurrent_async_calls_number_steps_in_call_order(dbos: DBOS) -> None:
    @DBOS.workflow()
    def target_workflow(scheduled_at: datetime, ctx: Any) -> None:
        pass

    schedule_name = f"durable-sched-{uuid.uuid4()}"

    @DBOS.workflow()
    async def concurrent_workflow() -> None:
        # Each call takes its step ID before its first await, so the
        # numbering follows call order whatever order the threads run in.
        await asyncio.gather(
            DBOS.create_schedule_async(
                schedule_name=schedule_name,
                workflow_fn=target_workflow,
                schedule="0 0 1 1 *",
            ),
            DBOS.list_queues_async(),
            DBOS.get_latest_application_version_async(),
            DBOS.list_application_versions_async(),
            DBOS.retrieve_queue_async("no-such-queue"),
        )
        await asyncio.gather(
            DBOS.get_schedule_async(schedule_name),
            DBOS.list_schedules_async(),
        )
        await DBOS.delete_schedule_async(schedule_name)

    handle = await DBOS.start_workflow_async(concurrent_workflow)
    await handle.get_result()
    assert await step_names_async(handle.workflow_id) == [
        "DBOS.createSchedule",
        "DBOS.listQueues",
        "DBOS.getLatestApplicationVersion",
        "DBOS.listApplicationVersions",
        "DBOS.retrieveQueue",
        "DBOS.getSchedule",
        "DBOS.listSchedules",
        "DBOS.deleteSchedule",
    ]


def test_create_schedule_queue_check_is_inside_step(dbos: DBOS) -> None:
    @DBOS.workflow()
    def target_workflow(scheduled_at: datetime, ctx: Any) -> None:
        pass

    queue_name = f"sched-q-{uuid.uuid4()}"
    schedule_name = f"durable-sched-{uuid.uuid4()}"
    DBOS.register_queue(queue_name)

    @DBOS.workflow()
    def schedule_workflow() -> Optional[str]:
        DBOS.create_schedule(
            schedule_name=schedule_name,
            workflow_fn=target_workflow,
            schedule="0 0 1 1 *",
            queue_name=queue_name,
        )
        sched = DBOS.get_schedule(schedule_name)
        return sched["queue_name"] if sched else None

    handle = DBOS.start_workflow(schedule_workflow)
    assert handle.get_result() == queue_name

    # Deleting the queue does not make a replay fail its (already recorded) check.
    DBOS.delete_schedule(schedule_name)
    DBOS.delete_queue(queue_name)
    forked = DBOS.fork_workflow(handle.workflow_id, 2)
    assert forked.get_result() is None
    assert step_names(forked.workflow_id) == [
        "DBOS.createSchedule",
        "DBOS.getSchedule",
    ]

    # A failed check is recorded as the step's error.
    @DBOS.workflow()
    def undeclared_queue_workflow() -> str:
        try:
            DBOS.create_schedule(
                schedule_name=f"durable-sched-{uuid.uuid4()}",
                workflow_fn=target_workflow,
                schedule="0 0 1 1 *",
                queue_name=queue_name,
            )
        except DBOSException as e:
            return str(e)
        return "created"

    err_handle = DBOS.start_workflow(undeclared_queue_workflow)
    assert "is not declared" in err_handle.get_result()
    steps = DBOS.list_workflow_steps(err_handle.workflow_id)
    assert [s["function_name"] for s in steps] == ["DBOS.createSchedule"]
    assert steps[0]["error"] is not None


def test_migrate_refused_in_workflow_body(dbos: DBOS, config: DBOSConfig) -> None:
    system_database_url = config["system_database_url"]
    assert system_database_url is not None
    sharding = bool(config.get("sharding"))

    @DBOS.step()
    def migrate_step() -> None:
        DBOS.migrate(system_database_url, sharding=sharding)

    @DBOS.workflow()
    def migrate_workflow() -> None:
        DBOS.migrate(system_database_url, sharding=sharding)

    @DBOS.workflow()
    def migrate_from_step_workflow() -> None:
        migrate_step()

    with pytest.raises(DBOSException, match="DBOS.migrate cannot be called"):
        migrate_workflow()
    # A step is the right place for the side effect.
    migrate_from_step_workflow()
