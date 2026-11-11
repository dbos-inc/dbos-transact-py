"""Public DBOS methods called from a workflow are either checkpointed as steps,
refused, or deterministic. These tests cover the methods that used to be none
of the three."""

import asyncio
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

import dbos._dbos as dbos_module
from dbos import DBOS, DBOSConfig, Queue
from dbos._context import snapshot_step_context
from dbos._error import DBOSException
from dbos._registrations import get_dbos_func_name
from dbos._serialization import DBOSPortableJSONSerializer, DefaultSerializer

OTHER_APP = "durability-other-app"


def steps_of(workflow_id: str) -> List[Tuple[int, str]]:
    return [
        (s["function_id"], s["function_name"])
        for s in DBOS.list_workflow_steps(workflow_id)
    ]


async def steps_of_async(workflow_id: str) -> List[Tuple[int, str]]:
    steps = await DBOS.list_workflow_steps_async(workflow_id)
    return [(s["function_id"], s["function_name"]) for s in steps]


def queue_fields(queue: Queue) -> Dict[str, Any]:
    # The checkpointed configuration, read from the fields: the public getters
    # re-read the live row, which would hide what the checkpoint restored.
    return {
        "name": queue.name,
        "application_name": queue.application_name,
        "concurrency": queue._concurrency,
        "worker_concurrency": queue._worker_concurrency,
        "limiter": queue._limiter,
        "partition_concurrency": queue._partition_concurrency,
        "partition_worker_concurrency": queue._partition_worker_concurrency,
        "partition_limiter": queue._partition_limiter,
        "polling_interval_sec": queue._polling_interval_sec,
        "database_backed_queue": queue.database_backed_queue,
    }


@pytest.mark.parametrize(
    "make_serializer",
    [
        pytest.param(DefaultSerializer, id="pickle"),
        pytest.param(DBOSPortableJSONSerializer, id="portable_json"),
    ],
)
def test_queue_api_is_checkpointed(
    config: DBOSConfig, cleanup_test_databases: None, make_serializer: Any
) -> None:
    # Queues are checkpointed as plain data, so every serializer stores them.
    DBOS.destroy(destroy_registry=True)
    config["serializer"] = make_serializer()
    dbos = DBOS(config=config)
    DBOS.launch()
    try:
        queue_name = f"durable-q-{uuid.uuid4()}"
        params: Dict[str, Any] = {
            "global_concurrency": 7,
            "worker_concurrency": 3,
            "limiter": {"limit": 5, "period": 1.5},
            "polling_interval_sec": 0.25,
        }
        fields = {
            "name": queue_name,
            "application_name": dbos._sys_db.app_name,
            "concurrency": 7,
            "worker_concurrency": 3,
            "limiter": {"limit": 5, "period": 1.5},
            "partition_concurrency": None,
            "partition_worker_concurrency": None,
            "partition_limiter": None,
            "polling_interval_sec": 0.25,
            "database_backed_queue": True,
        }

        @DBOS.workflow()
        def child(x: int) -> int:
            return x

        @DBOS.workflow()
        def queue_workflow() -> List[Any]:
            # A validation error raises before the call takes a step ID.
            with pytest.raises(ValueError):
                DBOS.register_queue("_dbos_reserved")
            registered = DBOS.register_queue(queue_name, **params)
            # A plain read that takes no step.
            DBOS.retrieve_queue(queue_name)
            result = registered.enqueue(child, 3).get_result()
            listed = [
                queue_fields(q) for q in DBOS.list_queues() if q.name == queue_name
            ]
            DBOS.delete_queue(queue_name)
            return [queue_fields(registered), listed, result]

        expected = [fields, [fields], 3]
        expected_steps = [
            (1, "DBOS.registerQueue"),
            (2, get_dbos_func_name(child)),
            (3, "DBOS.getResult"),
            (4, "DBOS.listQueues"),
            (5, "DBOS.deleteQueue"),
        ]
        handle = DBOS.start_workflow(queue_workflow)
        assert handle.get_result() == expected
        assert steps_of(handle.workflow_id) == expected_steps
        assert DBOS.retrieve_queue(queue_name) is None

        # The queue is gone, so a replay can only get these fields from the checkpoints.
        forked = DBOS.fork_workflow(handle.workflow_id, 5)
        assert forked.get_result() == expected
        assert steps_of(forked.workflow_id) == expected_steps

        # Replaying registerQueue returns a queue that still enqueues.
        DBOS.register_queue(queue_name, **params)
        forked = DBOS.fork_workflow(handle.workflow_id, 2)
        assert forked.get_result() == expected
        steps = DBOS.list_workflow_steps(forked.workflow_id)
        child_status = DBOS.get_workflow_status(str(steps[1]["child_workflow_id"]))
        assert child_status is not None and child_status.queue_name == queue_name
    finally:
        DBOS.destroy(destroy_registry=True)


@pytest.mark.asyncio
async def test_queue_api_async_is_checkpointed(dbos: DBOS) -> None:
    queue_name = f"durable-q-{uuid.uuid4()}"
    other_queue = f"durable-q-{uuid.uuid4()}"
    dbos._sys_db.upsert_queue(
        name=other_queue,
        concurrency=None,
        worker_concurrency=None,
        rate_limit_max=None,
        rate_limit_period_sec=None,
        polling_interval_sec=1.0,
        update_existing=True,
        application_name=OTHER_APP,
    )
    # A partitioned queue, so the partition fields go through the checkpoint too.
    fields = {
        "name": queue_name,
        "application_name": dbos._sys_db.app_name,
        "concurrency": 3,
        "worker_concurrency": 2,
        "limiter": None,
        "partition_concurrency": 2,
        "partition_worker_concurrency": 1,
        "partition_limiter": {"limit": 4, "period": 2.5},
        "polling_interval_sec": 0.5,
        "database_backed_queue": True,
    }

    @DBOS.workflow()
    async def queue_workflow() -> Tuple[Dict[str, Any], List[Dict[str, Any]], str]:
        # Another application's queue makes this fail, and the error is recorded.
        try:
            await DBOS.register_queue_async(other_queue)
            collision = ""
        except DBOSException as e:
            collision = str(e)
        registered = await DBOS.register_queue_async(
            queue_name,
            global_concurrency=3,
            worker_concurrency=2,
            partition_concurrency=2,
            partition_worker_concurrency=1,
            partition_limiter={"limit": 4, "period": 2.5},
            polling_interval_sec=0.5,
        )
        # A plain read that takes no step.
        await DBOS.retrieve_queue_async(queue_name)
        listed = [
            queue_fields(q)
            for q in await DBOS.list_queues_async()
            if q.name == queue_name
        ]
        await DBOS.delete_queue_async(queue_name)
        return queue_fields(registered), listed, collision

    handle = await DBOS.start_workflow_async(queue_workflow)
    result = await handle.get_result()
    assert result[:2] == (fields, [fields])
    assert "already registered by application" in result[2]
    expected_steps = [
        (1, "DBOS.registerQueue"),
        (2, "DBOS.registerQueue"),
        (3, "DBOS.listQueues"),
        (4, "DBOS.deleteQueue"),
    ]
    assert await steps_of_async(handle.workflow_id) == expected_steps
    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    assert steps[0]["error"] is not None

    # With both queues gone, a replay returns the recorded results, including the
    # recorded error, instead of registering the other queue.
    await DBOS.delete_queue_async(other_queue)
    forked = await DBOS.fork_workflow_async(handle.workflow_id, 4)
    assert await forked.get_result() == result
    assert await steps_of_async(forked.workflow_id) == expected_steps
    assert await DBOS.retrieve_queue_async(other_queue) is None


def test_application_version_api_is_checkpointed(dbos: DBOS) -> None:
    current = DBOS.application_version

    @DBOS.step()
    def promote() -> None:
        DBOS.set_latest_application_version(current)

    @DBOS.workflow()
    def version_workflow() -> Tuple[int, str, int]:
        # Promoting a version is refused in a workflow body, before it takes a step
        # ID, but allowed in a step.
        with pytest.raises(
            DBOSException, match="DBOS.set_latest_application_version cannot be called"
        ):
            DBOS.set_latest_application_version(current)
        promote()
        listed_at = next(
            v["version_timestamp"]
            for v in DBOS.list_application_versions()
            if v["version_name"] == current
        )
        latest = DBOS.get_latest_application_version()
        return listed_at, latest["version_name"], latest["version_timestamp"]

    handle = DBOS.start_workflow(version_workflow)
    result = handle.get_result()
    promoted_at = result[2]
    assert result == (promoted_at, current, promoted_at)
    expected_steps = [
        (1, get_dbos_func_name(promote)),
        (2, "DBOS.listApplicationVersions"),
        (3, "DBOS.getLatestApplicationVersion"),
    ]
    assert steps_of(handle.workflow_id) == expected_steps

    # Replaying every step does not promote the version again.
    time.sleep(0.01)
    forked = DBOS.fork_workflow(handle.workflow_id, 4)
    assert forked.get_result() == result
    assert steps_of(forked.workflow_id) == expected_steps
    assert DBOS.get_latest_application_version()["version_timestamp"] == promoted_at

    # Once the live timestamp moves, a full replay can only return the old one
    # from the recorded reads.
    DBOS.set_latest_application_version(current)
    assert DBOS.get_latest_application_version()["version_timestamp"] > promoted_at
    forked = DBOS.fork_workflow(handle.workflow_id, 4)
    assert forked.get_result() == result


@pytest.mark.asyncio
async def test_concurrent_async_calls_number_steps_in_call_order(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Every async variant must claim its step ID on the event loop. A claim from a
    # worker thread is delayed, so it lands after every claim made on the loop in
    # the meantime; the gather ends with an extra on-loop claim so even its last
    # variant has a later claim to land behind.
    def snapshot_delayed_off_loop(*args: Any, **kwargs: Any) -> Any:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            time.sleep(0.2)
        return snapshot_step_context(*args, **kwargs)

    monkeypatch.setattr(dbos_module, "snapshot_step_context", snapshot_delayed_off_loop)

    @DBOS.workflow()
    def target_workflow(scheduled_at: datetime, ctx: Any) -> None:
        pass

    schedule_name = f"durable-sched-{uuid.uuid4()}"
    queue_name = f"sched-q-{uuid.uuid4()}"
    await DBOS.register_queue_async(queue_name)

    @DBOS.workflow()
    async def concurrent_workflow() -> str:
        # Validation errors and refusals raise before the calls take a step ID.
        with pytest.raises(ValueError):
            await DBOS.register_queue_async("_dbos_reserved")
        with pytest.raises(DBOSException, match="cannot be called from a workflow"):
            await DBOS.set_latest_application_version_async(DBOS.application_version)
        with pytest.raises(DBOSException, match="Invalid cron schedule"):
            await DBOS.create_schedule_async(
                schedule_name="bad", workflow_fn=target_workflow, schedule="nope"
            )
        await asyncio.gather(
            DBOS.create_schedule_async(
                schedule_name=schedule_name,
                workflow_fn=target_workflow,
                schedule="0 0 1 1 *",
                queue_name=queue_name,
            ),
            DBOS.get_schedule_async(schedule_name),
            DBOS.list_schedules_async(),
            DBOS.delete_schedule_async(f"no-such-schedule-{uuid.uuid4()}"),
            DBOS.register_queue_async(f"durable-q-{uuid.uuid4()}"),
            DBOS.list_queues_async(),
            DBOS.delete_queue_async(f"no-such-queue-{uuid.uuid4()}"),
            DBOS.get_latest_application_version_async(),
            DBOS.list_application_versions_async(),
            DBOS.list_queues_async(),
        )
        await DBOS.delete_schedule_async(schedule_name)
        # A failed queue check is recorded as the step's error.
        try:
            await DBOS.create_schedule_async(
                schedule_name=f"durable-sched-{uuid.uuid4()}",
                workflow_fn=target_workflow,
                schedule="0 0 1 1 *",
                queue_name=f"no-such-queue-{uuid.uuid4()}",
            )
        except DBOSException as e:
            return str(e)
        return "created"

    expected_steps = [
        (1, "DBOS.createSchedule"),
        (2, "DBOS.getSchedule"),
        (3, "DBOS.listSchedules"),
        (4, "DBOS.deleteSchedule"),
        (5, "DBOS.registerQueue"),
        (6, "DBOS.listQueues"),
        (7, "DBOS.deleteQueue"),
        (8, "DBOS.getLatestApplicationVersion"),
        (9, "DBOS.listApplicationVersions"),
        (10, "DBOS.listQueues"),
        (11, "DBOS.deleteSchedule"),
        (12, "DBOS.createSchedule"),
    ]
    handle = await DBOS.start_workflow_async(concurrent_workflow)
    assert "is not declared" in await handle.get_result()
    assert await steps_of_async(handle.workflow_id) == expected_steps
    steps = await DBOS.list_workflow_steps_async(handle.workflow_id)
    assert steps[11]["error"] is not None

    # The queue check is recorded with createSchedule: with the queue deleted, the
    # replay returns the recorded result instead of failing the check.
    await DBOS.delete_queue_async(queue_name)
    forked = await DBOS.fork_workflow_async(handle.workflow_id, 2)
    assert "is not declared" in await forked.get_result()
    assert await steps_of_async(forked.workflow_id) == expected_steps


def test_migrate_refused_in_workflow_body(dbos: DBOS, tmp_path: Path) -> None:
    # A throwaway database, so the step half does not migrate the test's own.
    database_path = tmp_path / "migrate.sqlite"
    system_database_url = f"sqlite:///{database_path}"

    @DBOS.step()
    def migrate_step() -> None:
        DBOS.migrate(system_database_url)

    @DBOS.workflow()
    def migrate_workflow() -> None:
        DBOS.migrate(system_database_url)

    @DBOS.workflow()
    def migrate_from_step_workflow() -> None:
        migrate_step()

    with pytest.raises(DBOSException, match="DBOS.migrate cannot be called"):
        migrate_workflow()
    assert not database_path.exists()
    # A step is the right place for the side effect.
    migrate_from_step_workflow()
    assert database_path.exists()
