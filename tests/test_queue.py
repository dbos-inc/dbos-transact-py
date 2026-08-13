import asyncio
import gc
import multiprocessing
import multiprocessing.synchronize
import os
import subprocess
import threading
import time
import uuid
import weakref
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, List

import pytest
import sqlalchemy as sa
from psycopg import errors
from pydantic import BaseModel
from sqlalchemy.exc import OperationalError

from dbos import (
    DBOS,
    DBOSClient,
    DBOSConfig,
    DBOSConfiguredInstance,
    DBOSContextSetAuth,
    EnqueueOptions,
    Queue,
    SetEnqueueOptions,
    SetWorkflowAttributes,
    SetWorkflowID,
    SetWorkflowTimeout,
    WorkflowHandle,
)
from dbos._context import assert_current_dbos_context
from dbos._dbos import WorkflowHandleAsync
from dbos._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSException,
    DBOSQueueDeduplicatedError,
)
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import WorkflowStatusString
from dbos._utils import GlobalParams
from tests.conftest import (
    default_config,
    imprecise_timestamps,
    queue_entries_are_cleaned_up,
    retry_until_success,
    retry_until_success_async,
    set_workflow_status,
    using_sqlite,
)


def test_simple_queue(dbos: DBOS) -> None:
    wf_counter: int = 0
    step_counter: int = 0

    wfid = str(uuid.uuid4())

    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> str:
        assert DBOS.workflow_id == wfid
        nonlocal wf_counter
        wf_counter += 1
        var1 = test_step(var1)
        return var1 + var2

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return var + "d"

    DBOS.register_queue("test_queue")

    with SetWorkflowID(wfid):
        handle = DBOS.enqueue_workflow("test_queue", test_workflow, "abc", "123")
    assert handle.get_result() == "abcd123"
    with SetWorkflowID(wfid):
        assert test_workflow("abc", "123") == "abcd123"
    assert wf_counter == 1
    assert step_counter == 1

    # Verify started_at_epoch_ms is set correctly
    status = handle.get_status()
    assert status.dequeued_at and status.created_at
    # Both are database-stamped, so a second-resolution clock can land the enqueue and its dequeue on one tick.
    if imprecise_timestamps():
        assert status.dequeued_at >= status.created_at
    else:
        assert status.dequeued_at > status.created_at


def test_in_memory_queues(dbos: DBOS, config: DBOSConfig) -> None:
    """Cover the legacy `Queue(...)` constructor API and confirm in-memory and
    database-backed queues coexist correctly.
    """
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    queue_one = Queue("in_memory_queue_one")
    queue_two = Queue("in_memory_queue_two", concurrency=2)

    # Re-declaring an in-memory queue raises.
    with pytest.raises(Exception):
        Queue(queue_one.name)

    @DBOS.workflow()
    def workflow(val: str) -> str:
        return val + "!"

    # listen_queues accepts a mix of Queue objects and string names.
    DBOS.listen_queues([queue_one, "db_backed_queue"])
    DBOS.launch()

    # Register the database-backed queue post-launch (it requires _sys_db).
    DBOS.register_queue("db_backed_queue")

    # In-memory listened queue runs workflows.
    handle_one = queue_one.enqueue(workflow, "hello")
    assert handle_one.get_result() == "hello!"

    # Database-backed listened queue also runs workflows.
    handle_db = DBOS.enqueue_workflow("db_backed_queue", workflow, "db")
    assert handle_db.get_result() == "db!"

    # Workflows enqueued on a queue we are not listening to stay ENQUEUED.
    handle_two = queue_two.enqueue(workflow, "world")
    time.sleep(2)
    assert handle_two.get_status().status == "ENQUEUED"

    # Restart listening to queue_two and confirm the pending workflow runs.
    DBOS.destroy()
    DBOS(config=config)
    DBOS.listen_queues([queue_two])
    DBOS.launch()

    assert DBOS.retrieve_workflow(handle_two.workflow_id).get_result() == "world!"


def test_queue_crud(dbos: DBOS) -> None:
    queue_name = f"test_crud_queue_{uuid.uuid4()}"

    # retrieve_queue returns None when nothing is registered.
    assert DBOS.retrieve_queue(queue_name) is None
    # list_queues returns no rows when nothing is registered.
    assert DBOS.list_queues() == []

    # register_queue persists a fully configured queue.
    registered = DBOS.register_queue(
        queue_name,
        concurrency=10,
        limiter={"limit": 5, "period": 1.5},
        worker_concurrency=2,
        priority_enabled=True,
        polling_interval_sec=2.5,
    )
    assert registered.name == queue_name
    assert registered.database_backed_queue is True
    # Database-backed queues are not added to the in-memory registry.
    assert queue_name not in dbos._registry.queue_info_map

    # retrieve_queue reconstructs the queue from the database.
    retrieved = DBOS.retrieve_queue(queue_name)
    assert retrieved is not None
    assert retrieved.name == queue_name
    assert retrieved.concurrency == 10
    assert retrieved.worker_concurrency == 2
    assert retrieved.limiter == {"limit": 5, "period": 1.5}
    assert retrieved.priority_enabled is True
    assert retrieved.partition_queue is False
    assert retrieved.polling_interval_sec == 2.5
    assert retrieved.database_backed_queue is True
    assert queue_name not in dbos._registry.queue_info_map

    # list_queues includes the registered queue with the same configuration.
    listed = DBOS.list_queues()
    assert [q.name for q in listed] == [queue_name]
    only = listed[0]
    assert only.database_backed_queue is True
    assert only.concurrency == 10
    assert only.worker_concurrency == 2
    assert only.limiter == {"limit": 5, "period": 1.5}
    assert only.priority_enabled is True
    assert only.partition_queue is False
    assert only.polling_interval_sec == 2.5

    # on_conflict="never_update" leaves the existing row alone.
    DBOS.register_queue(queue_name, concurrency=99, on_conflict="never_update")
    retrieved = DBOS.retrieve_queue(queue_name)
    assert retrieved is not None
    assert retrieved.concurrency == 10
    assert retrieved.limiter == {"limit": 5, "period": 1.5}

    # on_conflict="always_update" overwrites every column.
    DBOS.register_queue(queue_name, concurrency=20, on_conflict="always_update")
    retrieved = DBOS.retrieve_queue(queue_name)
    assert retrieved is not None
    assert retrieved.concurrency == 20
    assert retrieved.worker_concurrency is None
    assert retrieved.limiter is None
    assert retrieved.priority_enabled is False
    assert retrieved.polling_interval_sec == 1.0

    # on_conflict="update_if_latest_version" updates when the running version
    # is the latest registered version.
    DBOS.register_queue(
        queue_name, concurrency=30, on_conflict="update_if_latest_version"
    )
    retrieved = DBOS.retrieve_queue(queue_name)
    assert retrieved is not None
    assert retrieved.concurrency == 30

    # If a newer application version exists, update_if_latest_version no-ops.
    newer_version = f"newer-{uuid.uuid4()}"
    dbos._sys_db.create_application_version(newer_version)
    dbos._sys_db.update_application_version_timestamp(
        newer_version, int(time.time() * 1000) + 1_000_000
    )
    DBOS.register_queue(
        queue_name, concurrency=999, on_conflict="update_if_latest_version"
    )
    retrieved = DBOS.retrieve_queue(queue_name)
    assert retrieved is not None
    assert retrieved.concurrency == 30


def test_queue_dynamic_config(dbos: DBOS) -> None:
    queue_name = f"test_dyn_queue_{uuid.uuid4()}"
    queue = DBOS.register_queue(
        queue_name,
        concurrency=4,
        worker_concurrency=2,
        priority_enabled=False,
        polling_interval_sec=1.0,
    )

    # Setters write to the database; getters read from it.
    queue.set_concurrency(8)
    queue.set_worker_concurrency(3)
    queue.set_limiter({"limit": 7, "period": 2.0})
    queue.set_priority_enabled(True)
    queue.set_partition_queue(True)
    queue.set_polling_interval_sec(0.5)

    fresh = DBOS.retrieve_queue(queue_name)
    for q in [queue, fresh]:
        assert q is not None
        assert q.concurrency == 8
        assert q.worker_concurrency == 3
        assert q.limiter == {"limit": 7, "period": 2.0}
        assert q.priority_enabled is True
        assert q.partition_queue is True
        assert q.polling_interval_sec == 0.5

    # Setters validate. worker_concurrency cannot exceed concurrency.
    with pytest.raises(ValueError):
        queue.set_worker_concurrency(100)
    # polling_interval must be positive.
    with pytest.raises(ValueError):
        queue.set_polling_interval_sec(0.0)

    # Limiter can be cleared.
    queue.set_limiter(None)
    q = DBOS.retrieve_queue(queue_name)
    assert q is not None
    assert q.limiter is None

    # set_concurrency / set_worker_concurrency refresh from the database before
    # cross-field validation, so a stale local cache cannot let a contradictory
    # configuration slip through.
    one = DBOS.retrieve_queue(queue_name)
    two = DBOS.retrieve_queue(queue_name)
    assert one is not None and two is not None
    one.set_concurrency(10)
    one.set_worker_concurrency(5)
    # ``two`` still has the old cached values, but its setter must consult the
    # database before rejecting an inconsistent change.
    with pytest.raises(ValueError):
        two.set_concurrency(2)
    with pytest.raises(ValueError):
        two.set_worker_concurrency(20)

    # In-memory queues read from their local fields, not the database.
    legacy = Queue(f"legacy_dyn_queue_{uuid.uuid4()}", concurrency=2)
    assert legacy.concurrency == 2
    # In-memory queues do not support setters.
    with pytest.raises(DBOSException):
        legacy.set_concurrency(5)


def test_client_queue_crud(dbos: DBOS, client: DBOSClient) -> None:
    queue_name = f"test_client_queue_{uuid.uuid4()}"

    # retrieve_queue returns None for an unknown queue.
    assert client.retrieve_queue(queue_name) is None
    # list_queues returns no rows when nothing is registered.
    assert client.list_queues() == []

    # register_queue persists configuration without depending on the DBOS
    # singleton's _sys_db.
    queue = client.register_queue(
        queue_name,
        concurrency=4,
        limiter={"limit": 5, "period": 1.5},
        worker_concurrency=2,
        priority_enabled=True,
        polling_interval_sec=2.5,
    )
    assert queue.name == queue_name
    assert queue.database_backed_queue is True
    assert queue._client_system_database is client._sys_db

    # Getters route through the client's SystemDatabase.
    retrieved = client.retrieve_queue(queue_name)
    assert retrieved is not None
    assert retrieved._client_system_database is client._sys_db
    assert retrieved.concurrency == 4
    assert retrieved.worker_concurrency == 2
    assert retrieved.limiter == {"limit": 5, "period": 1.5}
    assert retrieved.priority_enabled is True
    assert retrieved.polling_interval_sec == 2.5

    # list_queues returns queues bound to the client's SystemDatabase.
    listed = client.list_queues()
    assert [q.name for q in listed] == [queue_name]
    only = listed[0]
    assert only._client_system_database is client._sys_db
    assert only.concurrency == 4
    assert only.worker_concurrency == 2
    assert only.limiter == {"limit": 5, "period": 1.5}
    assert only.priority_enabled is True
    assert only.polling_interval_sec == 2.5

    # Setters write through the client's SystemDatabase too.
    retrieved.set_concurrency(8)
    fresh = DBOS.retrieve_queue(queue_name)
    assert fresh is not None
    assert fresh.concurrency == 8

    # Enqueueing on a client-bound queue is forbidden.
    @DBOS.workflow()
    def echo(x: int) -> int:
        return x

    with pytest.raises(DBOSException):
        retrieved.enqueue(echo, 42)

    # Speed up the worker so the test finishes quickly, then enqueue through
    # the DBOS singleton onto the client-registered queue and verify it runs.
    retrieved.set_polling_interval_sec(0.1)
    handle = DBOS.enqueue_workflow(queue_name, echo, 42)
    assert handle.get_result() == 42
    assert handle.get_status().queue_name == queue_name

    # Clients have no application version, so update_if_latest_version is
    # rejected.
    with pytest.raises(DBOSException):
        client.register_queue(
            queue_name, concurrency=1, on_conflict="update_if_latest_version"
        )

    # The default for clients is always_update: re-registering with new
    # config overwrites the existing row.
    client.register_queue(queue_name, concurrency=99)
    overwritten = DBOS.retrieve_queue(queue_name)
    assert overwritten is not None
    assert overwritten.concurrency == 99

    # delete_queue removes the row; subsequent retrievals return None and
    # deleting again is a harmless no-op.
    client.delete_queue(queue_name)
    assert client.retrieve_queue(queue_name) is None
    assert DBOS.retrieve_queue(queue_name) is None
    client.delete_queue(queue_name)


@pytest.mark.asyncio
async def test_queue_crud_async(dbos: DBOS) -> None:
    queue_name = f"test_crud_async_queue_{uuid.uuid4()}"

    # retrieve_queue_async returns None when nothing is registered.
    assert await DBOS.retrieve_queue_async(queue_name) is None
    # list_queues_async returns no rows when nothing is registered.
    assert await DBOS.list_queues_async() == []

    # register_queue_async persists a fully configured queue.
    registered = await DBOS.register_queue_async(
        queue_name,
        concurrency=10,
        limiter={"limit": 5, "period": 1.5},
        worker_concurrency=2,
        priority_enabled=True,
        polling_interval_sec=2.5,
    )
    assert registered.name == queue_name
    assert registered.database_backed_queue is True

    retrieved = await DBOS.retrieve_queue_async(queue_name)
    assert retrieved is not None
    assert await retrieved.get_concurrency_async() == 10
    assert await retrieved.get_worker_concurrency_async() == 2
    assert await retrieved.get_limiter_async() == {"limit": 5, "period": 1.5}
    assert await retrieved.get_priority_enabled_async() is True
    assert await retrieved.get_polling_interval_sec_async() == 2.5

    # list_queues_async includes the registered queue.
    listed = await DBOS.list_queues_async()
    assert [q.name for q in listed] == [queue_name]
    only = listed[0]
    assert only.database_backed_queue is True
    assert await only.get_concurrency_async() == 10
    assert await only.get_worker_concurrency_async() == 2
    assert await only.get_limiter_async() == {"limit": 5, "period": 1.5}
    assert await only.get_priority_enabled_async() is True
    assert await only.get_polling_interval_sec_async() == 2.5

    # Async setters write to the database; async getters see the change.
    await retrieved.set_concurrency_async(8)
    await retrieved.set_worker_concurrency_async(3)
    await retrieved.set_limiter_async({"limit": 7, "period": 2.0})
    await retrieved.set_priority_enabled_async(True)
    await retrieved.set_partition_queue_async(True)
    await retrieved.set_polling_interval_sec_async(0.5)

    fresh = await DBOS.retrieve_queue_async(queue_name)
    assert fresh is not None
    assert await fresh.get_concurrency_async() == 8
    assert await fresh.get_worker_concurrency_async() == 3
    assert await fresh.get_limiter_async() == {"limit": 7, "period": 2.0}
    assert await fresh.get_priority_enabled_async() is True
    assert await fresh.get_partition_queue_async() is True
    assert await fresh.get_polling_interval_sec_async() == 0.5

    # Async setters validate. worker_concurrency cannot exceed concurrency.
    with pytest.raises(ValueError):
        await retrieved.set_worker_concurrency_async(100)
    # polling_interval must be positive.
    with pytest.raises(ValueError):
        await retrieved.set_polling_interval_sec_async(0.0)

    # Limiter can be cleared via async setter.
    await retrieved.set_limiter_async(None)
    cleared = await DBOS.retrieve_queue_async(queue_name)
    assert cleared is not None
    assert await cleared.get_limiter_async() is None

    # In-memory queues do not support async setters either.
    legacy = Queue(f"legacy_async_dyn_queue_{uuid.uuid4()}", concurrency=2)
    with pytest.raises(DBOSException):
        await legacy.set_concurrency_async(5)

    # Sync DBOS.register_queue / retrieve_queue / delete_queue / list_queues
    # raise when called from a running event loop; async callers must use the
    # *_async variants.
    with pytest.raises(RuntimeError):
        DBOS.register_queue(queue_name)
    with pytest.raises(RuntimeError):
        DBOS.retrieve_queue(queue_name)
    with pytest.raises(RuntimeError):
        DBOS.delete_queue(queue_name)
    with pytest.raises(RuntimeError):
        DBOS.list_queues()

    # delete_queue_async removes the row; subsequent retrievals return None
    # and deleting again is a harmless no-op.
    await DBOS.delete_queue_async(queue_name)
    assert await DBOS.retrieve_queue_async(queue_name) is None
    await DBOS.delete_queue_async(queue_name)


@pytest.mark.asyncio
async def test_client_queue_crud_async(dbos: DBOS, client: DBOSClient) -> None:
    queue_name = f"test_client_async_queue_{uuid.uuid4()}"

    assert await client.retrieve_queue_async(queue_name) is None
    assert await client.list_queues_async() == []

    queue = await client.register_queue_async(
        queue_name,
        concurrency=4,
        limiter={"limit": 5, "period": 1.5},
        worker_concurrency=2,
        priority_enabled=True,
        polling_interval_sec=2.5,
    )
    assert queue.name == queue_name
    assert queue.database_backed_queue is True
    assert queue._client_system_database is client._sys_db

    retrieved = await client.retrieve_queue_async(queue_name)
    assert retrieved is not None
    assert retrieved._client_system_database is client._sys_db
    assert await retrieved.get_concurrency_async() == 4
    assert await retrieved.get_worker_concurrency_async() == 2
    assert await retrieved.get_limiter_async() == {"limit": 5, "period": 1.5}
    assert await retrieved.get_priority_enabled_async() is True
    assert await retrieved.get_polling_interval_sec_async() == 2.5

    # list_queues_async returns queues bound to the client's SystemDatabase.
    listed = await client.list_queues_async()
    assert [q.name for q in listed] == [queue_name]
    only = listed[0]
    assert only._client_system_database is client._sys_db
    assert await only.get_concurrency_async() == 4
    assert await only.get_worker_concurrency_async() == 2
    assert await only.get_limiter_async() == {"limit": 5, "period": 1.5}
    assert await only.get_priority_enabled_async() is True
    assert await only.get_polling_interval_sec_async() == 2.5

    await retrieved.set_concurrency_async(8)
    fresh = await DBOS.retrieve_queue_async(queue_name)
    assert fresh is not None
    assert await fresh.get_concurrency_async() == 8

    # Clients have no application version, so update_if_latest_version is
    # rejected for the async API too.
    with pytest.raises(DBOSException):
        await client.register_queue_async(
            queue_name, concurrency=1, on_conflict="update_if_latest_version"
        )

    await client.delete_queue_async(queue_name)
    assert await client.retrieve_queue_async(queue_name) is None


def test_enqueue_on_nonexistent_queue(dbos: DBOS) -> None:
    """``DBOS.enqueue_workflow`` on a non-existent queue does not raise; the
    workflow is recorded as ENQUEUED and starts running once the queue is
    registered."""
    queue_name = f"test_nonexistent_queue_{uuid.uuid4()}"

    @DBOS.workflow()
    def echo(x: int) -> int:
        return x

    handle = DBOS.enqueue_workflow(queue_name, echo, 7)

    # Nothing dispatches the workflow yet — it stays ENQUEUED.
    time.sleep(1.0)
    assert handle.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Registering the queue brings up a worker that picks up the orphan.
    DBOS.register_queue(queue_name, polling_interval_sec=0.1)
    assert handle.get_result() == 7


def test_queue_delete_and_recreate(dbos: DBOS) -> None:
    """Create a queue, run a workflow on it, delete it, recreate it, and verify
    the recreated queue still processes workflows."""
    queue_name = f"test_delete_recreate_{uuid.uuid4()}"

    @DBOS.workflow()
    def echo(x: int) -> int:
        return x

    DBOS.register_queue(queue_name, polling_interval_sec=0.1)

    # Initial queue works.
    handle1 = DBOS.enqueue_workflow(queue_name, echo, 1)
    assert handle1.get_result() == 1

    # Delete the queue. Subsequent retrievals should return None.
    DBOS.delete_queue(queue_name)
    assert DBOS.retrieve_queue(queue_name) is None

    # Recreate the queue with a different config; the new row should be used.
    time.sleep(2)
    DBOS.register_queue(queue_name, concurrency=5, polling_interval_sec=0.1)
    recreated = DBOS.retrieve_queue(queue_name)
    assert recreated is not None
    assert recreated.concurrency == 5

    # The recreated queue still processes workflows.
    handle2 = DBOS.enqueue_workflow(queue_name, echo, 2)
    assert handle2.get_result() == 2

    DBOS.delete_queue(queue_name)


def test_dynamic_concurrency_takes_effect(dbos: DBOS) -> None:
    """Verify that updating a queue's concurrency at runtime is picked up by
    the worker thread on its next poll iteration.
    """
    queue_name = f"test_dyn_runtime_{uuid.uuid4()}"
    queue = DBOS.register_queue(queue_name, concurrency=1, polling_interval_sec=0.1)

    started = threading.Semaphore(0)
    release = threading.Event()

    @DBOS.workflow()
    def blocking() -> None:
        started.release()
        release.wait()

    handles = [DBOS.enqueue_workflow(queue_name, blocking) for _ in range(3)]

    # With concurrency=1, only one workflow should start.
    assert started.acquire(timeout=5)
    time.sleep(1.0)  # Plenty of poll iterations for a second to start (it shouldn't).
    assert not started.acquire(blocking=False)

    # Bump concurrency. The worker reloads from DB on its next iteration and
    # should immediately start the remaining two.
    queue.set_concurrency(3)
    assert started.acquire(timeout=5)
    assert started.acquire(timeout=5)

    # Release all three; everything completes.
    release.set()
    for handle in handles:
        handle.get_result()
    assert queue_entries_are_cleaned_up(dbos)


def test_one_at_a_time(dbos: DBOS) -> None:
    wf_counter = 0
    flag = False
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.workflow()
    def workflow_one() -> None:
        nonlocal wf_counter
        wf_counter += 1
        main_thread_event.set()
        workflow_event.wait()

    @DBOS.workflow()
    def workflow_two() -> None:
        nonlocal flag
        flag = True

    DBOS.register_queue("test_queue", concurrency=1)
    handle1 = DBOS.enqueue_workflow("test_queue", workflow_one)
    assert handle1.get_status().queue_name == "test_queue"
    handle2 = DBOS.enqueue_workflow("test_queue", workflow_two)

    main_thread_event.wait()
    time.sleep(2)  # Verify the other task isn't scheduled on subsequent poller ticks.
    assert not flag
    workflow_event.set()
    assert handle1.get_result() == None
    assert handle2.get_result() == None
    assert flag
    assert wf_counter == 1
    assert queue_entries_are_cleaned_up(dbos)


def test_one_at_a_time_with_limiter(dbos: DBOS) -> None:
    wf_counter = 0
    flag = False
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.workflow()
    def workflow_one() -> None:
        nonlocal wf_counter
        wf_counter += 1
        main_thread_event.set()
        workflow_event.wait()

    @DBOS.workflow()
    def workflow_two() -> None:
        nonlocal flag
        flag = True

    DBOS.register_queue("test_queue", concurrency=1, limiter={"limit": 10, "period": 1})
    handle1 = DBOS.enqueue_workflow("test_queue", workflow_one)
    handle2 = DBOS.enqueue_workflow("test_queue", workflow_two)

    main_thread_event.wait()
    time.sleep(2)  # Verify the other task isn't scheduled on subsequent poller ticks.
    assert not flag
    workflow_event.set()
    assert handle1.get_result() == None
    assert handle2.get_result() == None
    assert flag
    assert wf_counter == 1
    assert queue_entries_are_cleaned_up(dbos)


def test_queue_childwf(dbos: DBOS) -> None:
    DBOS.register_queue("child_queue", concurrency=3)

    @DBOS.workflow()
    def test_child_wf(val: str) -> str:
        DBOS.recv("release", 30)
        return val + "d"

    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> str:
        wfh1 = DBOS.enqueue_workflow("child_queue", test_child_wf, var1)
        wfh2 = DBOS.enqueue_workflow("child_queue", test_child_wf, var2)
        wfh3 = DBOS.enqueue_workflow("child_queue", test_child_wf, var1)
        wfh4 = DBOS.enqueue_workflow("child_queue", test_child_wf, var2)

        DBOS.sleep(1)
        assert wfh4.get_status().status == "ENQUEUED"

        DBOS.send(wfh1.get_workflow_id(), "go", "release")
        DBOS.send(wfh2.get_workflow_id(), "go", "release")
        DBOS.send(wfh3.get_workflow_id(), "go", "release")
        DBOS.send(wfh4.get_workflow_id(), "go", "release")

        return (
            wfh1.get_result()
            + wfh2.get_result()
            + wfh3.get_result()
            + wfh4.get_result()
        )

    assert test_workflow("a", "b") == "adbdadbd"


def test_queue_step(dbos: DBOS) -> None:
    step_counter: int = 0
    wfid = str(uuid.uuid4())

    @DBOS.step()
    def test_step(var: str) -> str:
        assert DBOS.workflow_id == wfid
        nonlocal step_counter
        step_counter += 1
        return var + "1"

    DBOS.register_queue("test_queue")

    with SetWorkflowID(wfid):
        handle = DBOS.enqueue_workflow("test_queue", test_step, "abc")
    assert handle.get_result() == "abc1"
    with SetWorkflowID(wfid):
        handle = DBOS.enqueue_workflow("test_queue", test_step, "abc")
    assert handle.get_result() == "abc1"
    assert step_counter == 1


def test_queue_transaction(dbos: DBOS) -> None:
    step_counter: int = 0
    wfid = str(uuid.uuid4())

    @DBOS.transaction()
    def test_transaction(var: str) -> str:
        assert DBOS.workflow_id == wfid
        nonlocal step_counter
        step_counter += 1
        return var + "1"

    DBOS.register_queue("test_queue")

    with SetWorkflowID(wfid):
        handle = DBOS.enqueue_workflow("test_queue", test_transaction, "abc")
    assert handle.get_result() == "abc1"
    with SetWorkflowID(wfid):
        assert test_transaction("abc") == "abc1"
    assert step_counter == 1


def test_limiter(dbos: DBOS) -> None:

    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> float:
        assert var1 == "abc" and var2 == "123"
        return time.time()

    limit = 5
    period = 1.8
    DBOS.register_queue("test_queue", limiter={"limit": limit, "period": period})

    handles: list[WorkflowHandle[float]] = []
    times: list[float] = []

    # Launch a number of tasks equal to three times the limit.
    # This should lead to three "waves" of the limit tasks being
    # executed simultaneously, followed by a wait of the period,
    # followed by the next wave.
    num_waves = 3
    for _ in range(limit * num_waves):
        h = DBOS.enqueue_workflow("test_queue", test_workflow, "abc", "123")
        handles.append(h)
    for h in handles:
        times.append(h.get_result())

    # Verify that each "wave" of tasks started at the ~same time. Use a
    # generous tolerance: under CI load tasks within a wave can be spread
    # out by hundreds of ms even though the limiter released them together.
    for wave in range(num_waves):
        for i in range(wave * limit, (wave + 1) * limit - 1):
            assert times[i + 1] - times[i] < 1.0

    # Verify that the gap between "waves" is ~equal to the period. The
    # tolerance has to cover the same intra-wave skew (since we're
    # comparing the first task of each wave, not the wave start times),
    # so use a window wider than the worst-case intra-wave spread.
    for wave in range(num_waves - 1):
        assert times[limit * (wave + 1)] - times[limit * wave] > period - 1.0
        assert times[limit * (wave + 1)] - times[limit * wave] < period + 1.0

    # Verify all workflows get the SUCCESS status eventually
    for h in handles:
        assert h.get_status().status == WorkflowStatusString.SUCCESS.value

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_limiter_dequeue_blocks_on_peer_claim(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """A peer mid-claim must block a rate-limited dequeue, not be skipped past.

    Under skip_locked the two dequeuers claim disjoint rows, each against its own
    pre-claim snapshot of the limiter budget, so each spends it in full.
    """

    @DBOS.workflow()
    def noop() -> str:
        return "done"

    limit = 2
    # A version this executor never runs, so the live queue worker leaves these rows alone.
    parked_version = "parked-version"
    queue = DBOS.register_queue(
        "limiter_lock_queue",
        limiter={"limit": limit, "period": 60},
        priority_enabled=True,
    )
    # Distinct priorities so the head of the queue is deterministic.
    ids = []
    for priority in range(1, limit * 2 + 1):
        with SetEnqueueOptions(priority=priority, app_version=parked_version):
            ids.append(DBOS.enqueue_workflow(queue.name, noop).workflow_id)

    ws = SystemSchema.workflow_status
    head = (
        sa.select(ws.c.workflow_uuid)
        .where(ws.c.queue_name == queue.name)
        .where(ws.c.status == WorkflowStatusString.ENQUEUED.value)
        .order_by(ws.c.priority.asc(), ws.c.created_at.asc())
        .limit(limit)
        .with_for_update()
    )
    with dbos._sys_db.engine.begin() as peer:
        # A peer dequeuer holding an open claim on the whole limiter budget.
        assert [row[0] for row in peer.execute(head)] == ids[:limit]
        with pytest.raises(OperationalError) as exc_info:
            dbos._sys_db.start_queued_workflows(
                queue, "test-executor", parked_version, None
            )
    assert isinstance(exc_info.value.orig, errors.LockNotAvailable)

    # Nothing was admitted behind the peer's back.
    for id in ids:
        status = dbos._sys_db.get_workflow_status(id)
        assert status is not None
        assert status["status"] == WorkflowStatusString.ENQUEUED.value


def test_multiple_queues(dbos: DBOS) -> None:

    wf_counter = 0
    flag = False
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.workflow()
    def workflow_one() -> None:
        nonlocal wf_counter
        wf_counter += 1
        main_thread_event.set()
        workflow_event.wait()

    @DBOS.workflow()
    def workflow_two() -> None:
        nonlocal flag
        flag = True

    DBOS.register_queue("test_concurrency_queue", concurrency=1)
    handle1 = DBOS.enqueue_workflow("test_concurrency_queue", workflow_one)
    assert handle1.get_status().queue_name == "test_concurrency_queue"
    handle2 = DBOS.enqueue_workflow("test_concurrency_queue", workflow_two)

    @DBOS.workflow()
    def limited_workflow(var1: str, var2: str) -> float:
        assert var1 == "abc" and var2 == "123"
        return time.time()

    limit = 5
    period = 1.8
    DBOS.register_queue("test_limit_queue", limiter={"limit": limit, "period": period})

    handles: list[WorkflowHandle[float]] = []
    times: list[float] = []

    # Launch a number of tasks equal to three times the limit.
    # This should lead to three "waves" of the limit tasks being
    # executed simultaneously, followed by a wait of the period,
    # followed by the next wave.
    num_waves = 3
    for _ in range(limit * num_waves):
        h = DBOS.enqueue_workflow("test_limit_queue", limited_workflow, "abc", "123")
        handles.append(h)
    for h in handles:
        times.append(h.get_result())

    # Verify that each "wave" of tasks started at the ~same time. Use a
    # generous tolerance: under CI load tasks within a wave can be spread
    # out by hundreds of ms even though the limiter released them together.
    for wave in range(num_waves):
        for i in range(wave * limit, (wave + 1) * limit - 1):
            assert times[i + 1] - times[i] < 1.0

    # Verify that the gap between "waves" is ~equal to the period. The
    # tolerance has to cover the same intra-wave skew (since we're
    # comparing the first task of each wave, not the wave start times),
    # so use a window wider than the worst-case intra-wave spread.
    for wave in range(num_waves - 1):
        assert times[limit * (wave + 1)] - times[limit * wave] > period - 1.0
        assert times[limit * (wave + 1)] - times[limit * wave] < period + 1.0

    # Verify all workflows get the SUCCESS status eventually
    for h in handles:
        assert h.get_status().status == WorkflowStatusString.SUCCESS.value

    # Verify that during all this time, the second task
    # was not launched on the concurrency-limited queue.
    # Then, finish the first task and verify the second
    # task runs on schedule.
    assert not flag
    workflow_event.set()
    assert handle1.get_result() == None
    assert handle2.get_result() == None
    assert flag
    assert wf_counter == 1

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_queue_workflow_in_recovered_workflow(dbos: DBOS, sqlite_path: Path) -> None:
    # We don't want to be taking queued jobs while subprocess runs
    DBOS.destroy()

    # Set up environment variables to trigger the crash in subprocess
    env = os.environ.copy()
    env["DIE_ON_PURPOSE"] = "true"

    # Run the script as a subprocess to get a workflow stuck
    process = subprocess.run(
        ["python", "tests/queuedworkflow.py", str(sqlite_path)],
        cwd=os.getcwd(),
        env=env,
        capture_output=True,
        text=True,
    )
    # print ("Process Return: ")
    # print (process.stdout)
    # print (process.stderr)
    assert process.returncode != 0  # Crashed

    # Run script again without crash
    process = subprocess.run(
        ["python", "tests/queuedworkflow.py", str(sqlite_path)],
        cwd=os.getcwd(),
        env=os.environ,
        capture_output=True,
        text=True,
    )
    # print ("Process Return: ")
    # print (process.stdout)
    # print (process.stderr)
    assert process.returncode == 0  # Ran to completion

    # Launch DBOS to check answer
    dbos = DBOS(config=default_config(sqlite_path))
    DBOS.launch()
    wfh: WorkflowHandle[int] = DBOS.retrieve_workflow("testqueuedwfcrash")
    assert wfh.get_result() == 5
    assert wfh.get_status().status == "SUCCESS"
    assert queue_entries_are_cleaned_up(dbos)
    return


def test_one_at_a_time_with_worker_concurrency(dbos: DBOS) -> None:
    wf_counter = 0
    flag = False
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.workflow()
    def workflow_one() -> None:
        nonlocal wf_counter
        wf_counter += 1
        main_thread_event.set()  # Signal main thread we got running
        workflow_event.wait()  # Wait to complete

    @DBOS.workflow()
    def workflow_two() -> None:
        nonlocal flag
        flag = True

    DBOS.register_queue("test_queue", worker_concurrency=1)
    handle1 = DBOS.enqueue_workflow("test_queue", workflow_one)
    handle2 = DBOS.enqueue_workflow("test_queue", workflow_two)

    # Wait until the first task is dequeued
    main_thread_event.wait()
    # Let pass a few dequeuing intervals
    time.sleep(2)
    # 2nd task should not have been dequeued
    assert not flag
    # Unlock the first task
    workflow_event.set()
    # Both tasks should have completed
    assert handle1.get_result() == None
    assert handle2.get_result() == None
    assert flag
    assert wf_counter == 1, f"wf_counter={wf_counter}"
    assert queue_entries_are_cleaned_up(dbos)


# Declare a workflow globally (we need it to be registered across process under a known name)
# Counting, not a flag: several dequeued workflows start at once, and an Event would
# collapse their starts into one, leaving the waiter below short.
start_counter = threading.Semaphore(0)
end_event = threading.Event()


@DBOS.workflow()
def worker_concurrency_test_workflow() -> None:
    start_counter.release()
    end_event.wait()


local_concurrency_limit: int = 5
global_concurrency_limit: int = local_concurrency_limit * 2


def run_dbos_test_in_process(
    i: int,
    start_signal: multiprocessing.synchronize.Event,
    end_signal: multiprocessing.synchronize.Event,
    sqlite_path: Path,
) -> None:
    config = default_config(sqlite_path)
    dbos_config: DBOSConfig = {
        "name": "test-app",
        "system_database_url": config["system_database_url"],
        "application_database_url": config["application_database_url"],
        "admin_port": 8001 + i,
    }
    dbos = DBOS(config=dbos_config)
    DBOS.launch()

    # The queue is already registered in the database by the parent process;
    # the queue manager picks it up via list_queues.
    # Wait to dequeue as many tasks as we can locally
    for _ in range(0, local_concurrency_limit):
        start_counter.acquire()
    # Signal the parent process we've dequeued
    start_signal.set()
    # Wait for the parent process to signal we can move on
    end_signal.wait()
    # Complete the task. 1 set should unblock them all
    end_event.set()

    # Now whatever is in the queue should be cleared up fast (start/end events are already set)
    queue_entries_are_cleaned_up(dbos)


# Test global concurrency and worker utilization by carefully filling the queue up to 1) the local limit 2) the global limit
# For the global limit, we fill the queue in 2 steps, ensuring that the 2nd worker is able to cap its local utilization even
# after having dequeued some tasks already
def test_worker_concurrency_with_n_dbos_instances(
    dbos: DBOS, sqlite_path: Path, skip_with_sqlite: None
) -> None:
    # Ensure children processes do not share global variables (including DBOS instance) with the parent
    multiprocessing.set_start_method("spawn")

    # Re-initialize so the parent opts out of dequeuing — only the children should
    # dequeue and run workflows.
    config = default_config(sqlite_path)
    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.listen_queues([])
    DBOS.launch()

    DBOS.register_queue(
        "test_queue",
        worker_concurrency=local_concurrency_limit,
        concurrency=global_concurrency_limit,
    )

    # First, start local concurrency limit tasks
    handles = []
    for _ in range(0, local_concurrency_limit):
        handles.append(
            DBOS.enqueue_workflow("test_queue", worker_concurrency_test_workflow)
        )

    # Start 2 workers
    processes = []
    start_signals = []
    end_signals = []
    manager = multiprocessing.Manager()
    for i in range(0, 2):
        os.environ["DBOS__VMID"] = f"test-executor-{i}"
        os.environ["DBOS__APPVERSION"] = GlobalParams.app_version
        start_signal = manager.Event()
        start_signals.append(start_signal)
        end_signal = manager.Event()
        end_signals.append(end_signal)
        process = multiprocessing.Process(
            target=run_dbos_test_in_process,
            args=(i, start_signal, end_signal, sqlite_path),
        )
        process.start()
        processes.append(process)
    del os.environ["DBOS__VMID"]
    del os.environ["DBOS__APPVERSION"]

    # Check that a single worker was able to acquire all the tasks
    loop = True
    while loop:
        for signal in start_signals:
            signal.wait(timeout=1)
            if signal.is_set():
                loop = False
    executors = []
    for handle in handles:
        status = handle.get_status()
        assert status.status == WorkflowStatusString.PENDING.value
        executors.append(status.executor_id)
    assert len(set(executors)) == 1

    # Now enqueue less than the local concurrency limit. Check that the 2nd worker acquired them. We won't have a signal set from the worker so we need to sleep a little.
    handles = []
    for _ in range(0, local_concurrency_limit - 1):
        handles.append(
            DBOS.enqueue_workflow("test_queue", worker_concurrency_test_workflow)
        )
    time.sleep(2)
    executors = []
    for handle in handles:
        status = handle.get_status()
        assert status.status == WorkflowStatusString.PENDING.value
        executors.append(status.executor_id)
    assert len(set(executors)) == 1

    # Now, enqueue two more tasks. This means qlen > local concurrency limit * 2 and qlen > global concurrency limit
    # We should have 1 tasks PENDING and 1 ENQUEUED, thus meeting both local and global concurrency limits
    handles = []
    for _ in range(0, 2):
        handles.append(
            DBOS.enqueue_workflow("test_queue", worker_concurrency_test_workflow)
        )
    # we can check the signal because the 2nd executor will set it
    num_dequeued = 0
    while num_dequeued < 2:
        for signal in start_signals:
            signal.wait(timeout=1)
            if signal.is_set():
                num_dequeued += 1
    executors = []
    statuses = []
    for handle in handles:
        status = handle.get_status()
        statuses.append(status.status)
        executors.append(status.executor_id)
    assert set(statuses) == {
        WorkflowStatusString.PENDING.value,
        WorkflowStatusString.ENQUEUED.value,
    }
    assert len(set(executors)) == 2
    assert "local" in executors

    # Now check in the DB that global concurrency is met
    with dbos._sys_db.engine.begin() as conn:
        query = (
            sa.select(sa.func.count())
            .select_from(SystemSchema.workflow_status)
            .where(
                SystemSchema.workflow_status.c.status
                == WorkflowStatusString.PENDING.value
            )
        )
        row = conn.execute(query).fetchone()

        assert row is not None, "Query returned no results"
        count = row[0]
        assert (
            count == global_concurrency_limit
        ), f"Expected {global_concurrency_limit} workflows, found {count}"

    # Signal the workers they can move on
    for signal in end_signals:
        signal.set()

    for process in processes:
        process.join()

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


# Test error cases where we have duplicated workflows starting with the same workflow ID.
def test_duplicate_workflow_id(dbos: DBOS) -> None:
    wfid = str(uuid.uuid4())

    @DBOS.workflow()
    def test_workflow(var1: str) -> str:
        DBOS.sleep(1)
        return var1

    @DBOS.workflow()
    def test_dup_workflow() -> None:
        DBOS.sleep(0.1)
        return

    @DBOS.dbos_class()
    class TestDup:
        @classmethod
        @DBOS.workflow()
        def test_workflow(cls, var1: str) -> str:
            DBOS.sleep(0.1)
            return var1

    @DBOS.dbos_class()
    class TestDupInst(DBOSConfiguredInstance):
        def __init__(self, config_name: str):
            self.config_name = config_name
            super().__init__(config_name)

        @DBOS.workflow()
        def test_workflow(self, var1: str) -> str:
            DBOS.sleep(0.1)
            return self.config_name + ":" + var1

    with SetWorkflowID(wfid):
        origHandle = DBOS.start_workflow(test_workflow, "abc")
        # The second one will generate a warning message but no error.
        test_dup_workflow()

    # It's okay to call the same workflow with the same ID again.
    with SetWorkflowID(wfid):
        same_handle = DBOS.start_workflow(test_workflow, "abc")

    # Call with a different function name is not allowed.
    with SetWorkflowID(wfid):
        with pytest.raises(Exception) as exc_info:
            DBOS.start_workflow(test_dup_workflow)
        assert "Workflow already exists with a different function name" in str(
            exc_info.value
        )

    # Call the same function name in a different class is not allowed.
    with SetWorkflowID(wfid):
        with pytest.raises(Exception) as exc_info:
            DBOS.start_workflow(TestDup.test_workflow, "abc")
        assert "Workflow already exists with a different function name" in str(
            exc_info.value
        )
    # Normal invocation is fine.
    res = TestDup.test_workflow("abc")
    assert res == "abc"

    # Call the same function name from a different instance is not allowed.
    wfid2 = str(uuid.uuid4())
    inst = TestDupInst("myconfig")
    with SetWorkflowID(wfid2):
        # Normal invocation is fine.
        res = inst.test_workflow("abc")
        assert res == "myconfig:abc"

    inst2 = TestDupInst("myconfig2")
    with SetWorkflowID(wfid2):
        with pytest.raises(Exception) as exc_info:
            inst2.test_workflow("abc")
        assert "Workflow already exists with a different config name" in str(
            exc_info.value
        )

    # Call the same function in a different queue would generate a warning, but is allowed.
    DBOS.register_queue("test_queue")
    with SetWorkflowID(wfid):
        handle = DBOS.enqueue_workflow("test_queue", test_workflow, "abc")
    assert handle.get_result() == "abc"

    # Call with a different input still uses the recorded input.
    with SetWorkflowID(wfid):
        res = test_workflow("def")
        # We want to see the warning message, but the result is non-deterministic
        # TODO: in the future, we may want to always use the recorded inputs.
        assert res == "abc" or res == "def"

    assert origHandle.get_result() == "abc"
    assert same_handle.get_result() == "abc"


def test_queue_recovery(dbos: DBOS) -> None:
    step_counter: int = 0
    step_enqueued: int = 0
    queued_steps = 5

    wfid = str(uuid.uuid4())
    DBOS.register_queue("test_queue")

    @DBOS.workflow()
    def test_workflow() -> list[int]:
        nonlocal step_enqueued
        assert DBOS.workflow_id == wfid
        handles = []
        for i in range(queued_steps):
            step_enqueued += 1
            h = DBOS.enqueue_workflow("test_queue", test_step, i)
            handles.append(h)
        return [h.get_result() for h in handles]

    @DBOS.step()
    def test_step(i: int) -> int:
        nonlocal step_counter
        step_counter += 1
        return i

    # Start the workflow. Wait for all five steps to start. Verify that they started.
    with SetWorkflowID(wfid):
        original_handle = DBOS.start_workflow(test_workflow)
    original_handle.get_result()

    assert step_counter == 5

    # Recover the workflow, then resume it.
    for h in DBOS.list_workflows(workflow_id_prefix=wfid):
        set_workflow_status(dbos._sys_db, h.workflow_id, "PENDING")
    recovery_handles = DBOS._recover_pending_workflows()
    # There should be one handle for the workflow and another for each queued step.
    assert len(recovery_handles) == queued_steps + 1
    # Verify that both the recovered and original workflows complete correctly.
    for rh in recovery_handles:
        if rh.get_workflow_id() == wfid:
            assert rh.get_result() == [0, 1, 2, 3, 4]
    assert original_handle.get_result() == [0, 1, 2, 3, 4]
    # Each step should start twice, once originally and once in recovery.
    assert step_counter == 5
    assert step_enqueued == 10

    # Rerun the workflow. Because each step is complete, none should start again.
    with SetWorkflowID(wfid):
        assert test_workflow() == [0, 1, 2, 3, 4]
    assert step_counter == 5
    assert step_enqueued == 10

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_queue_concurrency_under_recovery(dbos: DBOS) -> None:
    event = threading.Event()
    wf_events = [threading.Event() for _ in range(2)]
    counter = 0

    @DBOS.workflow()
    def blocked_workflow(i: int) -> None:
        wf_events[i].set()
        nonlocal counter
        counter += 1
        event.wait()

    @DBOS.workflow()
    def noop() -> None:
        pass

    DBOS.register_queue(
        "test_queue", worker_concurrency=2
    )  # covers global concurrency limit because we have a single process
    handle1 = DBOS.enqueue_workflow("test_queue", blocked_workflow, 0)
    handle2 = DBOS.enqueue_workflow("test_queue", blocked_workflow, 1)
    handle3 = DBOS.enqueue_workflow("test_queue", noop)

    # Wait for the two first workflows to be dequeued
    for e in wf_events:
        e.wait()
        e.clear()

    assert counter == 2
    assert handle1.get_status().status == WorkflowStatusString.PENDING.value
    assert handle2.get_status().status == WorkflowStatusString.PENDING.value
    assert handle3.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Manually update the database to pretend the 3rd workflow is PENDING and comes from another executor
    with dbos._sys_db.engine.begin() as c:
        query = (
            sa.update(SystemSchema.workflow_status)
            .values(status=WorkflowStatusString.PENDING.value, executor_id="other")
            .where(
                SystemSchema.workflow_status.c.workflow_uuid
                == handle3.get_workflow_id()
            )
        )
        c.execute(query)

    # Trigger workflow recovery. The two first workflows should still be blocked but the 3rd one enqueued
    recovered_other_handles = DBOS._recover_pending_workflows(["other"])
    assert handle1.get_status().status == WorkflowStatusString.PENDING.value
    assert handle2.get_status().status == WorkflowStatusString.PENDING.value
    assert len(recovered_other_handles) == 1
    assert recovered_other_handles[0].get_workflow_id() == handle3.get_workflow_id()
    assert handle3.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Trigger workflow recovery for "local". The two first workflows should be re-enqueued then dequeued again
    recovered_local_handles = DBOS._recover_pending_workflows(["local"])
    assert len(recovered_local_handles) == 2
    for h in recovered_local_handles:
        assert h.get_workflow_id() in [
            handle1.get_workflow_id(),
            handle2.get_workflow_id(),
        ]
    assert counter == 2  # These will not run, they are already running

    # Because tasks are re-enqueued in order, the 3rd task is head of line blocked
    assert handle3.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Unblock the first two workflows
    event.set()

    # Verify all queue entries eventually get cleaned up.
    assert handle1.get_result() == None
    assert handle2.get_result() == None
    assert handle3.get_result() == None
    assert handle3.get_status().executor_id == "local"
    assert queue_entries_are_cleaned_up(dbos)


def test_cancelling_queued_workflows(
    dbos: DBOS, skip_with_sqlite_imprecise_time: None
) -> None:
    start_event = threading.Event()
    blocking_event = threading.Event()

    @DBOS.workflow()
    def stuck_workflow() -> None:
        start_event.set()
        # Bounded so a failing assertion below fails the test instead of hanging teardown.
        blocking_event.wait(timeout=30)

    @DBOS.workflow()
    def regular_workflow() -> None:
        return

    # Enqueue both the blocked workflow and a regular workflow on a queue with concurrency 1
    DBOS.register_queue("test_queue", concurrency=1)
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        blocked_handle = DBOS.enqueue_workflow("test_queue", stuck_workflow)
    regular_handle = DBOS.enqueue_workflow("test_queue", regular_workflow)

    # Verify that the blocked workflow starts and is PENDING while the regular workflow remains ENQUEUED.
    start_event.wait()
    blocked_status = blocked_handle.get_status()
    assert blocked_status.status == WorkflowStatusString.PENDING.value
    # The dequeue refreshes updated_at, and nothing else has written this row since it was enqueued.
    assert blocked_status.created_at and blocked_status.updated_at
    assert blocked_status.updated_at > blocked_status.created_at
    assert regular_handle.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Cancel the blocked workflow. Verify this lets the regular workflow run.
    dbos.cancel_workflow(wfid)
    assert blocked_handle.get_status().status == WorkflowStatusString.CANCELLED.value
    assert regular_handle.get_result() == None

    # Unblock the cancelled workflow's body. Even though it now runs to
    # completion, CANCELLED is terminal: it must not overwrite CANCELLED with
    # SUCCESS, and awaiting its result must raise.
    blocking_event.set()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        blocked_handle.get_result()
    assert blocked_handle.get_status().status == WorkflowStatusString.CANCELLED.value

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_timeout_queue(dbos: DBOS) -> None:
    @DBOS.workflow()
    def blocking_workflow() -> None:
        assert assert_current_dbos_context().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is not None
        while True:
            DBOS.sleep(0.1)

    @DBOS.workflow()
    def normal_workflow() -> None:
        assert assert_current_dbos_context().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is not None
        return

    DBOS.register_queue("test_queue", concurrency=1, polling_interval_sec=0.1)

    # Enqueue a few blocked workflow
    num_workflows = 3
    handles: list[WorkflowHandle[None]] = []
    for _ in range(num_workflows):
        with SetWorkflowTimeout(0.1):
            handle = DBOS.enqueue_workflow("test_queue", blocking_workflow)
            handles.append(handle)

    # Also enqueue a normal workflow. Its timeout is generous so a slow CI
    # runner cannot push the instantly-returning workflow past its deadline.
    with SetWorkflowTimeout(5.0):
        normal_handle = DBOS.enqueue_workflow("test_queue", normal_workflow)

    # Verify the blocked workflows are cancelled
    for handle in handles:
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            handle.get_result()

    # Verify the normal workflow succeeds
    normal_handle.get_result()

    # Verify if a parent called with a timeout enqueues a blocked child
    # the deadline propagates and the child is also cancelled.
    child_id = str(uuid.uuid4())
    DBOS.register_queue("regular_queue", polling_interval_sec=0.1)

    @DBOS.workflow()
    def parent_workflow() -> None:
        with SetWorkflowID(child_id):
            handle = DBOS.enqueue_workflow("regular_queue", blocking_workflow)
        handle.get_result()

    with SetWorkflowTimeout(1.0):
        handle = DBOS.enqueue_workflow("regular_queue", parent_workflow)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        DBOS.retrieve_workflow(child_id).get_result()

    # Verify if a parent called with a timeout enqueues a blocked child
    # then exits the deadline propagates and the child is cancelled.

    @DBOS.workflow()
    def exiting_parent_workflow() -> str:
        handle = DBOS.enqueue_workflow("regular_queue", blocking_workflow)
        return handle.get_workflow_id()

    with SetWorkflowTimeout(1.0):
        child_id = exiting_parent_workflow()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        DBOS.retrieve_workflow(child_id).get_result()

    # Verify if a parent called with a timeout enqueues a child that
    # never starts because the queue is blocked, the deadline propagates
    # and both parent and child are cancelled.
    child_id = str(uuid.uuid4())
    DBOS.register_queue("stuck_queue", concurrency=1, polling_interval_sec=0.1)

    start_event = threading.Event()
    blocking_event = threading.Event()

    @DBOS.workflow()
    def stuck_workflow() -> None:
        start_event.set()
        blocking_event.wait()

    stuck_handle = DBOS.enqueue_workflow("stuck_queue", stuck_workflow)
    start_event.wait()

    @DBOS.workflow()
    def blocked_parent_workflow() -> None:
        with SetWorkflowID(child_id):
            DBOS.enqueue_workflow("stuck_queue", blocking_workflow)
        while True:
            DBOS.sleep(0.1)

    with SetWorkflowTimeout(1.0):
        handle = DBOS.start_workflow(blocked_parent_workflow)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        DBOS.retrieve_workflow(child_id).get_result()
    blocking_event.set()
    stuck_handle.get_result()

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)

    # Verify all timeout tasks completed
    assert len(dbos._timeout_tasks) == 0


@pytest.mark.asyncio
async def test_timeout_queue_async(dbos: DBOS, config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()

    @DBOS.workflow()
    async def blocking_workflow() -> None:
        assert assert_current_dbos_context().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is not None
        while True:
            await DBOS.sleep_async(0.1)

    @DBOS.workflow()
    async def normal_workflow() -> None:
        assert assert_current_dbos_context().workflow_timeout_ms is None
        assert assert_current_dbos_context().workflow_deadline_epoch_ms is not None
        return

    await DBOS.register_queue_async(
        "test_queue_async", concurrency=1, polling_interval_sec=0.1
    )

    # Enqueue a few blocked workflows
    num_workflows = 3
    handles: list[WorkflowHandleAsync[None]] = []
    for _ in range(num_workflows):
        with SetWorkflowTimeout(0.1):
            handle = await DBOS.enqueue_workflow_async(
                "test_queue_async", blocking_workflow
            )
            handles.append(handle)

    # Also enqueue a normal workflow. Its timeout is generous so a slow CI
    # runner cannot push the instantly-returning workflow past its deadline.
    with SetWorkflowTimeout(5.0):
        normal_handle = await DBOS.enqueue_workflow_async(
            "test_queue_async", normal_workflow
        )

    # Verify the blocked workflows are cancelled
    for handle in handles:
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            await handle.get_result()

    # Verify the normal workflow succeeds
    await normal_handle.get_result()

    # Verify if a parent called with a timeout enqueues a blocked child
    # the deadline propagates and the child is also cancelled.
    child_id = str(uuid.uuid4())
    await DBOS.register_queue_async("regular_queue_async", polling_interval_sec=0.1)

    @DBOS.workflow()
    async def parent_workflow() -> None:
        with SetWorkflowID(child_id):
            handle = await DBOS.enqueue_workflow_async(
                "regular_queue_async", blocking_workflow
            )
        await handle.get_result()

    # Use a generous timeout: when the parent is enqueued (not started
    # directly), the deadline clock starts before the queue poller picks
    # it up. Under CI load, polling + scheduling jitter can consume most
    # of a 1s window and the parent's body would be cancelled before it
    # could enqueue the child row.
    with SetWorkflowTimeout(5.0):
        handle = await DBOS.enqueue_workflow_async(
            "regular_queue_async", parent_workflow
        )
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await handle.get_result()

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await (await DBOS.retrieve_workflow_async(child_id)).get_result()

    # Verify if a parent called with a timeout enqueues a blocked child
    # then exits the deadline propagates and the child is cancelled.

    @DBOS.workflow()
    async def exiting_parent_workflow() -> str:
        handle = await DBOS.enqueue_workflow_async(
            "regular_queue_async", blocking_workflow
        )
        return handle.get_workflow_id()

    with SetWorkflowTimeout(1.0):
        child_id = await exiting_parent_workflow()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await (await DBOS.retrieve_workflow_async(child_id)).get_result()

    # Verify if a parent called with a timeout enqueues a child that
    # never starts because the queue is blocked, the deadline propagates
    # and both parent and child are cancelled.
    child_id = str(uuid.uuid4())
    await DBOS.register_queue_async(
        "stuck_queue_async", concurrency=1, polling_interval_sec=0.1
    )

    start_event = asyncio.Event()
    blocking_event = asyncio.Event()

    @DBOS.workflow()
    async def stuck_workflow() -> None:
        start_event.set()
        await blocking_event.wait()

    stuck_handle = await DBOS.enqueue_workflow_async(
        "stuck_queue_async", stuck_workflow
    )
    await start_event.wait()

    @DBOS.workflow()
    async def blocked_parent_workflow() -> None:
        with SetWorkflowID(child_id):
            await DBOS.enqueue_workflow_async("stuck_queue_async", blocking_workflow)
        while True:
            await DBOS.sleep_async(0.1)

    with SetWorkflowTimeout(1.0):
        handle = await DBOS.start_workflow_async(blocked_parent_workflow)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await handle.get_result()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        await (await DBOS.retrieve_workflow_async(child_id)).get_result()
    blocking_event.set()
    await stuck_handle.get_result()

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)

    # Verify all timeout tasks completed
    assert len(dbos._timeout_tasks) == 0


def test_resuming_queued_workflows(dbos: DBOS) -> None:
    start_event = threading.Event()
    blocking_event = threading.Event()

    @DBOS.workflow()
    def stuck_workflow() -> None:
        start_event.set()
        blocking_event.wait()

    @DBOS.workflow()
    def regular_workflow() -> None:
        return

    # Enqueue a blocked workflow and two regular workflows on a queue with concurrency 1
    DBOS.register_queue("test_queue", concurrency=1)
    wfid = str(uuid.uuid4())
    blocked_handle = DBOS.enqueue_workflow("test_queue", stuck_workflow)
    with SetWorkflowID(wfid):
        regular_handle_1 = DBOS.enqueue_workflow("test_queue", regular_workflow)
    regular_handle_2 = DBOS.enqueue_workflow("test_queue", regular_workflow)

    # Verify that the blocked workflow starts and is PENDING while the regular workflows remain ENQUEUED.
    start_event.wait()
    assert blocked_handle.get_status().status == WorkflowStatusString.PENDING.value
    assert regular_handle_1.get_status().status == WorkflowStatusString.ENQUEUED.value
    assert regular_handle_2.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Resume a regular workflow. Verify it completes.
    dbos.resume_workflow(wfid)
    assert regular_handle_1.get_result() == None

    # Complete the blocked workflow. Verify the second regular workflow also completes.
    blocking_event.set()
    assert blocked_handle.get_result() == None
    assert regular_handle_2.get_result() == None

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_resuming_queued_partitioned_workflows(
    dbos: DBOS, skip_with_sqlite_imprecise_time: None
) -> None:
    start_event = threading.Event()
    blocking_event = threading.Event()

    @DBOS.workflow()
    def stuck_workflow() -> None:
        start_event.set()
        blocking_event.wait()

    @DBOS.workflow()
    def regular_workflow() -> None:
        return

    # Enqueue a blocked workflow and two regular workflows on a queue with concurrency 1
    DBOS.register_queue("test_queue", concurrency=1, partition_queue=True)
    wfid = str(uuid.uuid4())
    with SetEnqueueOptions(queue_partition_key="key"):
        blocked_handle = DBOS.enqueue_workflow("test_queue", stuck_workflow)
        with SetWorkflowID(wfid):
            regular_handle_1 = DBOS.enqueue_workflow("test_queue", regular_workflow)
        regular_handle_2 = DBOS.enqueue_workflow("test_queue", regular_workflow)

    # Verify that the blocked workflow starts and is PENDING while the regular workflows remain ENQUEUED.
    start_event.wait()
    assert blocked_handle.get_status().status == WorkflowStatusString.PENDING.value
    assert regular_handle_1.get_status().status == WorkflowStatusString.ENQUEUED.value
    assert regular_handle_2.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Resume a regular workflow. Verify it completes.
    dbos.resume_workflow(wfid)
    assert regular_handle_1.get_result() == None

    # Complete the blocked workflow. Verify the second regular workflow also completes.
    blocking_event.set()
    assert blocked_handle.get_result() == None
    assert regular_handle_2.get_result() == None

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


def test_dlq_enqueued_workflows(dbos: DBOS) -> None:
    start_event = threading.Event()
    blocking_event = threading.Event()
    max_recovery_attempts = 10
    recovery_count = 0

    @DBOS.workflow(max_recovery_attempts=max_recovery_attempts)
    def blocked_workflow() -> None:
        start_event.set()
        nonlocal recovery_count
        recovery_count += 1
        blocking_event.wait()

    @DBOS.workflow()
    def regular_workflow() -> None:
        return

    # Enqueue both the blocked workflow and a regular workflow on a queue with concurrency 1
    DBOS.register_queue("test_queue", concurrency=1)
    blocked_handle = DBOS.enqueue_workflow("test_queue", blocked_workflow)
    regular_handle = DBOS.enqueue_workflow("test_queue", regular_workflow)

    # Enqueue the blocked workflow repeatedly, verify recovery attempts is not increased
    for _ in range(max_recovery_attempts):
        with SetWorkflowID(blocked_handle.workflow_id):
            DBOS.enqueue_workflow("test_queue", blocked_workflow)
    recovery_attempts = blocked_handle.get_status().recovery_attempts
    assert recovery_attempts is not None and recovery_attempts <= 1

    # Verify that the blocked workflow starts and is PENDING while the regular workflow remains ENQUEUED.
    start_event.wait()
    assert blocked_handle.get_status().status == WorkflowStatusString.PENDING.value
    assert regular_handle.get_status().status == WorkflowStatusString.ENQUEUED.value
    blocking_event.set()
    assert blocked_handle.get_result() == None

    # Attempt to recover the blocked workflow the maximum number of times
    for i in range(max_recovery_attempts):
        start_event.clear()
        set_workflow_status(dbos._sys_db, blocked_handle.workflow_id, "PENDING")
        rh = DBOS._recover_pending_workflows()
        start_event.wait()
        assert recovery_count == i + 2
        recovery_attempts = blocked_handle.get_status().recovery_attempts
        assert recovery_attempts == i + 2
        rh[0].get_result()

    # Verify an additional recovery throws puts the workflow in the DLQ status.
    set_workflow_status(dbos._sys_db, blocked_handle.workflow_id, "PENDING")
    DBOS._recover_pending_workflows()
    time.sleep(2)

    with dbos._sys_db.engine.begin() as c:
        query = sa.select(SystemSchema.workflow_status.c.recovery_attempts).where(
            SystemSchema.workflow_status.c.workflow_uuid
            == blocked_handle.get_workflow_id()
        )
        result = c.execute(query)
        row = result.fetchone()
        assert row is not None
        assert row[0] == max_recovery_attempts + 2
    assert (
        blocked_handle.get_status().status
        == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value
    )

    # Verify the blocked workflow entering the DLQ lets the regular workflow run
    assert regular_handle.get_result() == None

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)


@pytest.mark.asyncio
async def test_simple_queue_async(dbos: DBOS) -> None:
    wf_counter: int = 0
    step_counter: int = 0

    wfid = str(uuid.uuid4())

    @DBOS.workflow()
    async def test_workflow(var1: str, var2: str) -> str:
        assert DBOS.workflow_id == wfid
        nonlocal wf_counter
        wf_counter += 1
        var1 = await test_step(var1)
        return var1 + var2

    @DBOS.step()
    async def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return var + "d"

    await DBOS.register_queue_async("test_queue")

    with SetWorkflowID(wfid):
        handle = await DBOS.enqueue_workflow_async(
            "test_queue", test_workflow, "abc", "123"
        )
    assert (await handle.get_result()) == "abcd123"
    with SetWorkflowID(wfid):
        assert (await test_workflow("abc", "123")) == "abcd123"
    assert wf_counter == 1  # Direct re-invoke does not re-run the body (#762)
    assert step_counter == 1


def test_enqueue_options_require_a_queue(dbos: DBOS) -> None:
    # These options are interpreted by the queue machinery alone, so setting one
    # without a queue silently does nothing -- and persisting a dedup ID is unsafe,
    # because it becomes a unique-constraint violation once anything assigns the
    # row a queue name (e.g. recovery).

    @DBOS.workflow()
    def test_workflow(var: str) -> str:
        return var

    options: List[dict[str, Any]] = [
        {"deduplication_id": "dedup_without_queue"},
        {"priority": 5},
        {"queue_partition_key": "key_without_queue"},
        {"delay_seconds": 30},
    ]
    for option in options:
        wfid = str(uuid.uuid4())
        with pytest.raises(DBOSException) as exc_info:
            with SetEnqueueOptions(**option):
                with SetWorkflowID(wfid):
                    DBOS.start_workflow(test_workflow, "bob")
        message = str(exc_info.value)
        assert "not being enqueued" in message
        assert next(iter(option)) in message
        # The call must be rejected before any row is written, or it would leave
        # exactly the orphaned PENDING row this validation exists to prevent.
        assert DBOS.get_workflow_status(wfid) is None

    # app_version is excluded: without a queue it still decides which executors
    # can recover the workflow, so pinning it is meaningful and must keep working.
    pinned_id = str(uuid.uuid4())
    with SetEnqueueOptions(app_version="some_other_version"):
        with SetWorkflowID(pinned_id):
            DBOS.start_workflow(test_workflow, "bob")
    pinned = DBOS.get_workflow_status(pinned_id)
    assert pinned is not None and pinned.app_version == "some_other_version"


@pytest.mark.asyncio
async def test_enqueue_options_require_a_queue_async(dbos: DBOS) -> None:
    # start_workflow_async carries the same validation on a separate code path.

    @DBOS.workflow()
    async def test_workflow(var: str) -> str:
        return var

    wfid = str(uuid.uuid4())
    with pytest.raises(DBOSException) as exc_info:
        with SetEnqueueOptions(deduplication_id="dedup_without_queue"):
            with SetWorkflowID(wfid):
                await DBOS.start_workflow_async(test_workflow, "bob")
    assert "not being enqueued" in str(exc_info.value)
    assert await DBOS.get_workflow_status_async(wfid) is None


def test_queue_deduplication(dbos: DBOS) -> None:
    queue_name = "test_dedup_queue"
    DBOS.register_queue(queue_name)
    workflow_event = threading.Event()

    @DBOS.workflow()
    def child_workflow(var1: str) -> str:
        workflow_event.wait()
        return var1 + "-c"

    @DBOS.workflow()
    def test_workflow(var1: str) -> str:
        # Make sure the child workflow is not blocked by the same deduplication ID
        child_handle = DBOS.enqueue_workflow(queue_name, child_workflow, var1)
        workflow_event.wait()
        return child_handle.get_result() + "-p"

    # Make sure only one workflow is running at a time
    wfid = str(uuid.uuid4())
    dedup_id = "my_dedup_id"
    with SetEnqueueOptions(deduplication_id=dedup_id):
        with SetWorkflowID(wfid):
            handle1 = DBOS.enqueue_workflow(queue_name, test_workflow, "abc")
    assert handle1.get_status().deduplication_id == dedup_id

    # Enqueue the same workflow with a different deduplication ID should be fine.
    with SetEnqueueOptions(deduplication_id="my_other_dedup_id"):
        another_handle = DBOS.enqueue_workflow(queue_name, test_workflow, "ghi")

    # Enqueue a workflow without deduplication ID should be fine.
    nodedup_handle1 = DBOS.enqueue_workflow(queue_name, test_workflow, "jkl")

    # Enqueued multiple times without deduplication ID but with different inputs should be fine, but get the result of the first one.
    with SetWorkflowID(wfid):
        nodedup_handle2 = DBOS.enqueue_workflow(queue_name, test_workflow, "mno")

    # Enqueue the same workflow with the same deduplication ID should raise an exception.
    wfid2 = str(uuid.uuid4())
    with SetEnqueueOptions(deduplication_id=dedup_id):
        with SetWorkflowID(wfid2):
            with pytest.raises(Exception) as exc_info:
                DBOS.enqueue_workflow(queue_name, test_workflow, "def")
        assert (
            f"Workflow {wfid2} was deduplicated due to an existing workflow in queue {queue_name} with deduplication ID {dedup_id}."
            in str(exc_info.value)
        )

    # Now unblock the first two workflows and wait for them to finish.
    workflow_event.set()
    assert handle1.get_result() == "abc-c-p"
    assert another_handle.get_result() == "ghi-c-p"
    assert nodedup_handle1.get_result() == "jkl-c-p"
    assert nodedup_handle2.get_result() == "abc-c-p"

    # Invoke the workflow again with the same deduplication ID now should be fine because it's no longer in the queue.
    with SetEnqueueOptions(deduplication_id=dedup_id):
        with SetWorkflowID(wfid2):
            handle2 = DBOS.enqueue_workflow(queue_name, test_workflow, "def")
    assert handle2.get_result() == "def-c-p"

    assert queue_entries_are_cleaned_up(dbos)


def test_queue_deduplication_recovery(dbos: DBOS) -> None:
    queue_name = "test_dedup_queue"
    DBOS.register_queue(queue_name)
    workflow_event = threading.Event()
    dedup_id = "my_dedup_id"

    @DBOS.workflow()
    def child_workflow() -> None:
        workflow_event.wait()

    @DBOS.workflow()
    def test_workflow() -> str:
        with SetEnqueueOptions(deduplication_id=dedup_id):
            handle = DBOS.enqueue_workflow(queue_name, child_workflow)
            with pytest.raises(DBOSQueueDeduplicatedError):
                DBOS.enqueue_workflow(queue_name, child_workflow)
        return handle.workflow_id

    parent_id = str(uuid.uuid4())
    with SetWorkflowID(parent_id):
        child_id = test_workflow()
    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(child_id)
    workflow_event.set()
    handle.get_result()

    steps = DBOS.list_workflow_steps(parent_id)
    assert len(steps) == 2
    assert steps[0]["child_workflow_id"] == child_id
    assert isinstance(steps[1]["error"], DBOSQueueDeduplicatedError)

    set_workflow_status(dbos._sys_db, parent_id, "PENDING")
    DBOS._recover_pending_workflows()
    assert DBOS.retrieve_workflow(parent_id).get_result() == child_id

    assert queue_entries_are_cleaned_up(dbos)


@pytest.mark.asyncio
async def test_queue_deduplication_async(dbos: DBOS) -> None:
    queue_name = "test_dedup_queue_async"
    await DBOS.register_queue_async(queue_name)
    workflow_event = asyncio.Event()

    @DBOS.workflow()
    async def child_workflow(var1: str) -> str:
        await workflow_event.wait()
        return var1 + "-c"

    @DBOS.workflow()
    async def test_workflow(var1: str) -> str:
        # Make sure the child workflow is not blocked by the same deduplication ID
        child_handle = await DBOS.enqueue_workflow_async(
            queue_name, child_workflow, var1
        )
        await workflow_event.wait()
        return (await child_handle.get_result()) + "-p"

    # Make sure only one workflow is running at a time
    wfid = str(uuid.uuid4())
    dedup_id = "my_dedup_id"
    with SetEnqueueOptions(deduplication_id=dedup_id):
        with SetWorkflowID(wfid):
            handle1 = await DBOS.enqueue_workflow_async(
                queue_name, test_workflow, "abc"
            )

    # Enqueue the same workflow with a different deduplication ID should be fine.
    with SetEnqueueOptions(deduplication_id="my_other_dedup_id"):
        another_handle = await DBOS.enqueue_workflow_async(
            queue_name, test_workflow, "ghi"
        )

    # Enqueue a workflow without deduplication ID should be fine.
    nodedup_handle1 = await DBOS.enqueue_workflow_async(
        queue_name, test_workflow, "jkl"
    )

    # Enqueued multiple times without deduplication ID but with different inputs should be fine, but get the result of the first one.
    with SetWorkflowID(wfid):
        nodedup_handle2 = await DBOS.enqueue_workflow_async(
            queue_name, test_workflow, "mno"
        )

    # Enqueue the same workflow with the same deduplication ID should raise an exception.
    wfid2 = str(uuid.uuid4())
    with SetEnqueueOptions(deduplication_id=dedup_id):
        with SetWorkflowID(wfid2):
            with pytest.raises(Exception) as exc_info:
                await DBOS.enqueue_workflow_async(queue_name, test_workflow, "def")
        assert (
            f"Workflow {wfid2} was deduplicated due to an existing workflow in queue {queue_name} with deduplication ID {dedup_id}."
            in str(exc_info.value)
        )

    # Now unblock the first two workflows and wait for them to finish.
    workflow_event.set()
    assert (await handle1.get_result()) == "abc-c-p"
    assert (await another_handle.get_result()) == "ghi-c-p"
    assert (await nodedup_handle1.get_result()) == "jkl-c-p"
    assert (await nodedup_handle2.get_result()) == "abc-c-p"

    # Invoke the workflow again with the same deduplication ID now should be fine because it's no longer in the queue.
    with SetEnqueueOptions(deduplication_id=dedup_id):
        with SetWorkflowID(wfid2):
            handle2 = await DBOS.enqueue_workflow_async(
                queue_name, test_workflow, "def"
            )
    assert (await handle2.get_result()) == "def-c-p"

    assert queue_entries_are_cleaned_up(dbos)


def test_priority_queue(dbos: DBOS) -> None:
    # Make sure that we can enqueue workflows with different priorities correctly
    DBOS.register_queue("test_queue_priority", concurrency=1, priority_enabled=True)
    DBOS.register_queue("test_queue_child")

    workflow_event = threading.Event()
    wf_priority_list = []

    @DBOS.workflow()
    def child_workflow(p: int) -> int:
        workflow_event.wait()
        return p

    @DBOS.workflow()
    def test_workflow(priority: int) -> int:
        wf_priority_list.append(priority)
        # Make sure the priority is not propagated
        assert assert_current_dbos_context().priority == None
        child_handle = DBOS.enqueue_workflow(
            "test_queue_child", child_workflow, priority
        )
        workflow_event.wait()
        return child_handle.get_result() + priority

    # Enqueue an invalid priority
    with pytest.raises(Exception) as exc_info:
        with SetEnqueueOptions(priority=-100):
            DBOS.enqueue_workflow("test_queue_priority", test_workflow, -100)
    assert "Invalid priority" in str(exc_info.value)

    wf_handles: list[WorkflowHandle[int]] = []
    # First, enqueue a workflow without priority
    handle = DBOS.enqueue_workflow("test_queue_priority", test_workflow, 0)
    wf_handles.append(handle)

    # Then, enqueue a workflow with priority 1 to 5
    for i in range(1, 6):
        with SetEnqueueOptions(priority=i):
            handle = DBOS.enqueue_workflow("test_queue_priority", test_workflow, i)
            assert handle.get_status().priority == i
        wf_handles.append(handle)

    # Finally, enqueue two workflows without priority again
    wf_handles.append(DBOS.enqueue_workflow("test_queue_priority", test_workflow, 6))
    wf_handles.append(DBOS.enqueue_workflow("test_queue_priority", test_workflow, 7))

    # The finish sequence should be 0, 6, 7, 1, 2, 3, 4, 5
    workflow_event.set()
    for i in range(len(wf_handles)):
        res = wf_handles[i].get_result()
        assert res == i * 2

    assert wf_priority_list == [0, 6, 7, 1, 2, 3, 4, 5]
    assert queue_entries_are_cleaned_up(dbos)


@pytest.mark.asyncio
async def test_priority_queue_async(dbos: DBOS) -> None:
    # Make sure that we can enqueue workflows with different priorities correctly
    await DBOS.register_queue_async(
        "test_queue_priority_async", concurrency=1, priority_enabled=True
    )
    await DBOS.register_queue_async("test_queue_child_async")

    workflow_event = asyncio.Event()
    wf_priority_list = []

    @DBOS.workflow()
    async def child_workflow(p: int) -> int:
        await workflow_event.wait()
        return p

    @DBOS.workflow()
    async def test_workflow(priority: int) -> int:
        wf_priority_list.append(priority)
        # Make sure the priority is not propagated
        assert assert_current_dbos_context().priority == None
        child_handle = await DBOS.enqueue_workflow_async(
            "test_queue_child_async", child_workflow, priority
        )
        await workflow_event.wait()
        return (await child_handle.get_result()) + priority

    # Enqueue an invalid priority
    with pytest.raises(Exception) as exc_info:
        with SetEnqueueOptions(priority=-100):
            await DBOS.enqueue_workflow_async(
                "test_queue_priority_async", test_workflow, -100
            )
    assert "Invalid priority" in str(exc_info.value)

    wf_handles: List[WorkflowHandleAsync[int]] = []
    # First, enqueue a workflow without priority
    handle = await DBOS.enqueue_workflow_async(
        "test_queue_priority_async", test_workflow, 0
    )
    wf_handles.append(handle)

    # Then, enqueue a workflow with priority 1 to 5
    for i in range(1, 6):
        with SetEnqueueOptions(priority=i):
            handle = await DBOS.enqueue_workflow_async(
                "test_queue_priority_async", test_workflow, i
            )
        wf_handles.append(handle)

    # Finally, enqueue two workflows without priority again
    wf_handles.append(
        await DBOS.enqueue_workflow_async("test_queue_priority_async", test_workflow, 6)
    )
    wf_handles.append(
        await DBOS.enqueue_workflow_async("test_queue_priority_async", test_workflow, 7)
    )

    # The finish sequence should be 0, 6, 7, 1, 2, 3, 4, 5
    workflow_event.set()
    for i in range(len(wf_handles)):
        res = await wf_handles[i].get_result()
        assert res == i * 2

    assert wf_priority_list == [0, 6, 7, 1, 2, 3, 4, 5]
    assert queue_entries_are_cleaned_up(dbos)


@pytest.mark.asyncio
async def test_enqueue_async_validation(dbos: DBOS) -> None:
    """Enqueue-time validation rules fire for in-memory queues. Database-backed
    queues skip these checks to avoid a per-enqueue DB round-trip, so the
    assertions below intentionally exercise the in-memory queue path."""

    @DBOS.workflow()
    async def noop_workflow() -> None:
        return

    no_priority_q = Queue(f"async_validation_no_priority_{uuid.uuid4()}")
    priority_q = Queue(
        f"async_validation_priority_{uuid.uuid4()}", priority_enabled=True
    )
    partition_q = Queue(
        f"async_validation_partition_{uuid.uuid4()}", partition_queue=True
    )
    no_partition_q = Queue(f"async_validation_no_partition_{uuid.uuid4()}")

    # Priority on a non-priority queue
    with pytest.raises(Exception, match="Priority is not enabled for queue"):
        with SetEnqueueOptions(priority=1):
            await no_priority_q.enqueue_async(noop_workflow)

    # Partition queue requires a partition key
    with pytest.raises(Exception, match="without a partition key"):
        await partition_q.enqueue_async(noop_workflow)

    # Partition key on a non-partitioned queue
    with pytest.raises(
        Exception, match="only use a partition key on a partition-enabled queue"
    ):
        with SetEnqueueOptions(queue_partition_key="key"):
            await no_partition_q.enqueue_async(noop_workflow)

    # Deduplication is not supported for partitioned queues
    with pytest.raises(
        Exception, match="Deduplication is not supported for partitioned queues"
    ):
        with SetEnqueueOptions(queue_partition_key="key", deduplication_id="dedupe"):
            await partition_q.enqueue_async(noop_workflow)

    # Sanity check: priority on a priority-enabled in-memory queue works.
    with SetEnqueueOptions(priority=1):
        await priority_q.enqueue_async(noop_workflow)


def test_worker_concurrency_across_versions(dbos: DBOS, client: DBOSClient) -> None:
    DBOS.register_queue("test_worker_concurrency_across_versions", worker_concurrency=1)

    @DBOS.workflow()
    def test_workflow() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    # First enqueue a workflow on the other version, then on the current version
    other_version = "other_version"
    other_version_handle: WorkflowHandle[None] = client.enqueue(
        {
            "queue_name": "test_worker_concurrency_across_versions",
            "workflow_name": test_workflow.__qualname__,
            "app_version": other_version,
        }
    )
    handle = DBOS.enqueue_workflow(
        "test_worker_concurrency_across_versions", test_workflow
    )

    # Verify the workflow on the current version completes, but the other version is still ENQUEUED
    assert handle.get_result()
    assert other_version_handle.get_status().status == "ENQUEUED"

    # Change the version, verify the other version complets
    GlobalParams.app_version = other_version
    assert other_version_handle.get_result()


def test_timeout_queue_recovery(dbos: DBOS) -> None:
    DBOS.register_queue("test_queue")
    evt = threading.Event()

    @DBOS.workflow()
    def blocking_workflow() -> None:
        evt.set()
        while True:
            DBOS.sleep(0.1)

    timeout = 3.0
    enqueue_time = time.time()
    with SetWorkflowTimeout(timeout):
        original_handle = DBOS.enqueue_workflow("test_queue", blocking_workflow)

    # Verify the workflow's timeout is properly configured
    evt.wait()
    original_status = original_handle.get_status()
    assert original_status.workflow_timeout_ms == timeout * 1000
    assert (
        original_status.workflow_deadline_epoch_ms is not None
        and original_status.workflow_deadline_epoch_ms > enqueue_time * 1000
    )
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        original_handle.get_result()

    # Reset the workflow to PENDING so it can be recovered. (update_workflow_outcome
    # cannot be used here: it will not move a workflow out of the terminal
    # CANCELLED state.)
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING"})
            .where(
                SystemSchema.workflow_status.c.workflow_uuid
                == original_handle.workflow_id
            )
        )
    # Recover the workflow. Verify its deadline remains the same
    handles = DBOS._recover_pending_workflows()
    assert len(handles) == 1
    recovered_handle = handles[0]
    recovered_status = recovered_handle.get_status()
    assert recovered_status.workflow_timeout_ms == timeout * 1000
    assert (
        recovered_status.workflow_deadline_epoch_ms
        == original_status.workflow_deadline_epoch_ms
    )

    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        recovered_handle.get_result()


def test_unsetting_timeout(dbos: DBOS) -> None:

    DBOS.register_queue("test_queue")

    @DBOS.workflow()
    def child() -> str:
        for _ in range(5):
            DBOS.sleep(1)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    @DBOS.workflow()
    def parent(child_one: str, child_two: str) -> None:
        with SetWorkflowID(child_two):
            with SetWorkflowTimeout(None):
                DBOS.enqueue_workflow("test_queue", child)

        with SetWorkflowID(child_one):
            DBOS.enqueue_workflow("test_queue", child)

    child_one, child_two = str(uuid.uuid4()), str(uuid.uuid4())
    with SetWorkflowTimeout(2.0):
        DBOS.enqueue_workflow("test_queue", parent, child_one, child_two).get_result()

    # Verify child one, which has a propagated timeout, is cancelled
    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(child_one)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()

    # Verify child two, which doesn't have a timeout, succeeds
    handle = DBOS.retrieve_workflow(child_two)
    assert handle.get_result() == child_two


def test_queue_executor_id(dbos: DBOS) -> None:

    DBOS.register_queue("test-queue")

    @DBOS.workflow()
    def example_workflow() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    # Set an executor ID
    original_executor_id = str(uuid.uuid4())
    GlobalParams.executor_id = original_executor_id

    # Enqueue the workflow, validate its executor ID
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.enqueue_workflow("test-queue", example_workflow)
    assert handle.get_result() == wfid
    assert handle.get_status().executor_id == original_executor_id

    # Set a new executor ID
    new_executor_id = str(uuid.uuid4())
    GlobalParams.executor_id = new_executor_id

    # Re-enqueue the workflow, verify its executor ID does not change.
    with SetWorkflowID(wfid):
        handle = DBOS.enqueue_workflow("test-queue", example_workflow)
        assert handle.get_result() == wfid
    assert handle.get_status().executor_id == original_executor_id


# Non-basic types must be declared in an importable scope (so not inside a function)
# to be serializable and deserializable
class InnerType(BaseModel):
    one: str
    two: int


class OuterType(BaseModel):
    inner: InnerType


def test_complex_type(dbos: DBOS) -> None:
    DBOS.register_queue("test_queue")

    @DBOS.workflow()
    def workflow(input: OuterType) -> OuterType:
        return input

    # Verify a workflow with non-basic inputs and outputs can be enqueued
    inner = InnerType(one="one", two=2)
    outer = OuterType(inner=inner)

    handle = DBOS.enqueue_workflow("test_queue", workflow, outer)
    result = handle.get_result()

    def check(result: Any) -> None:
        assert isinstance(result, OuterType)
        assert isinstance(result.inner, InnerType)
        assert result.inner.one == outer.inner.one
        assert result.inner.two == outer.inner.two

    check(result)

    # Verify a workflow with non-basic inputs and outputs can be recovered
    start_event = threading.Event()
    event = threading.Event()

    @DBOS.workflow()
    def blocked_workflow(input: OuterType) -> OuterType:
        start_event.set()
        event.wait()
        return input

    handle = DBOS.enqueue_workflow("test_queue", blocked_workflow, outer)

    start_event.wait()
    recovery_handle = DBOS._recover_pending_workflows()[0]
    event.set()
    check(handle.get_result())
    check(recovery_handle.get_result())


def test_enqueue_version(dbos: DBOS) -> None:

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    DBOS.register_queue("queue")
    input = 5

    # Enqueue the app on a different version, verify it has that version
    future_version = str(uuid.uuid4())
    with SetEnqueueOptions(app_version=future_version):
        handle = DBOS.enqueue_workflow("queue", workflow, input)
    assert handle.get_status().app_version == future_version

    # Change the global version, verify it works
    GlobalParams.app_version = future_version
    assert handle.get_result() == input


@pytest.mark.asyncio
async def test_enqueue_version_async(dbos: DBOS) -> None:

    @DBOS.workflow()
    async def workflow(x: int) -> int:
        return x

    await DBOS.register_queue_async("queue")
    input = 5

    # Enqueue the app on a different version, verify it has that version
    future_version = str(uuid.uuid4())
    with SetEnqueueOptions(app_version=future_version):
        handle = await DBOS.enqueue_workflow_async("queue", workflow, input)
    assert (await handle.get_status()).app_version == future_version

    # Change the global version, verify it works
    GlobalParams.app_version = future_version
    assert await handle.get_result() == input


def test_dequeue_no_version_requires_latest(dbos: DBOS, client: DBOSClient) -> None:
    # A worker dequeues version-less (application_version IS NULL) workflows only when running the latest registered version; own-version workflows are always dequeued.
    queue_name = f"test_dequeue_latest_{uuid.uuid4()}"
    DBOS.register_queue(queue_name)

    @DBOS.workflow()
    def workflow(x: int) -> int:
        return x

    current_version = GlobalParams.app_version

    # Register a newer application version so this worker is no longer the latest.
    newer_version = f"newer-{uuid.uuid4()}"
    dbos._sys_db.create_application_version(newer_version)
    dbos._sys_db.update_application_version_timestamp(
        newer_version, int(time.time() * 1000) + 1_000_000
    )

    # Enqueue a version-less workflow (client enqueue with no app_version).
    versionless_handle: WorkflowHandle[int] = client.enqueue(
        {
            "queue_name": queue_name,
            "workflow_name": workflow.__qualname__,
        },
        5,
    )

    # Enqueue a workflow tagged with this worker's current version.
    versioned_handle = DBOS.enqueue_workflow(queue_name, workflow, 7)

    # The version-tagged workflow is dequeued and completes.
    assert versioned_handle.get_result() == 7

    # The version-less workflow is NOT dequeued: this worker is not the latest.
    assert versionless_handle.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Make this worker the latest version again; now the version-less workflow runs.
    dbos._sys_db.update_application_version_timestamp(
        current_version, int(time.time() * 1000) + 2_000_000
    )
    assert versionless_handle.get_result() == 5


def test_queue_partitions(dbos: DBOS, client: DBOSClient) -> None:

    blocking_event = threading.Event()
    waiting_event = threading.Event()

    @DBOS.workflow()
    def blocked_workflow() -> str:
        waiting_event.set()
        blocking_event.wait()
        assert DBOS.workflow_id
        return DBOS.workflow_id

    @DBOS.workflow()
    def normal_workflow() -> str:
        assert DBOS.workflow_id
        return DBOS.workflow_id

    DBOS.register_queue("queue", partition_queue=True, worker_concurrency=1)

    blocked_partition_key = "blocked"
    normal_partition_key = "normal"

    # Enqueue a blocked workflow and a normal workflow on
    # the blocked partition. Verify the blocked workflow starts
    # but the normal workflow is stuck behind it.
    with SetEnqueueOptions(queue_partition_key=blocked_partition_key):
        blocked_blocked_handle = DBOS.enqueue_workflow("queue", blocked_workflow)
        blocked_normal_handle = DBOS.enqueue_workflow("queue", normal_workflow)

    waiting_event.wait()
    assert (
        blocked_blocked_handle.get_status().status == WorkflowStatusString.PENDING.value
    )
    assert (
        blocked_normal_handle.get_status().status == WorkflowStatusString.ENQUEUED.value
    )
    assert (
        blocked_blocked_handle.get_status().queue_partition_key
        == blocked_normal_handle.get_status().queue_partition_key
        == blocked_partition_key
    )
    # Enqueue a normal workflow on the other partition and verify it runs normally
    with SetEnqueueOptions(queue_partition_key=normal_partition_key):
        normal_handle = DBOS.enqueue_workflow("queue", normal_workflow)

    assert normal_handle.get_result()

    # Unblock the blocked partition and verify its workflows complete
    blocking_event.set()
    assert blocked_blocked_handle.get_result()
    assert blocked_normal_handle.get_result()

    # Confirm client enqueue works with partitions
    client_handle: WorkflowHandle[None] = client.enqueue(
        {
            "queue_name": "queue",
            "workflow_name": normal_workflow.__qualname__,
            "queue_partition_key": blocked_partition_key,
        }
    )
    assert client_handle.get_result()


def test_partition_serialization_failure_skips_key(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A serialization failure on one partition key skips only that partition:
    the same sweep still dequeues other partitions, with no queue-wide backoff."""
    from psycopg import errors
    from sqlalchemy.exc import OperationalError

    # concurrency=2 keeps this queue on the per-partition sweep loop; only concurrency=1 uses the batched path.
    queue = Queue(
        f"serialization_skip_{uuid.uuid4().hex[:8]}",
        concurrency=2,
        partition_queue=True,
        polling_interval_sec=0.25,
    )
    # Sorts before the healthy key, so an escaping error would abort the sweep first.
    poisoned_key = "a_poisoned"
    healthy_key = "z_healthy"
    poison_active = threading.Event()
    poison_active.set()

    @DBOS.workflow()
    def wf() -> str:
        assert DBOS.workflow_id
        return DBOS.workflow_id

    real_start = dbos._sys_db.start_queued_workflows

    def poisoned_start(
        queue_arg: Queue,
        executor_id: str,
        app_version: str,
        queue_partition_key: Any = None,
        local_running_count: int = 0,
    ) -> List[str]:
        if (
            poison_active.is_set()
            and queue_arg.name == queue.name
            and queue_partition_key == poisoned_key
        ):
            raise OperationalError("dequeue", None, errors.SerializationFailure())
        return real_start(
            queue_arg,
            executor_id,
            app_version,
            queue_partition_key,
            local_running_count,
        )

    monkeypatch.setattr(dbos._sys_db, "start_queued_workflows", poisoned_start)

    with SetEnqueueOptions(queue_partition_key=poisoned_key):
        poisoned_handle = queue.enqueue(wf)
    with SetEnqueueOptions(queue_partition_key=healthy_key):
        healthy_handle = queue.enqueue(wf)

    # The healthy partition drains even though the poisoned one fails every sweep.
    assert healthy_handle.get_result()
    assert poisoned_handle.get_status().status == WorkflowStatusString.ENQUEUED.value

    # Once the contention clears, the poisoned partition drains on a later sweep.
    poison_active.clear()
    assert poisoned_handle.get_result()


@pytest.mark.asyncio
async def test_partition_queue_worker_concurrency_async(dbos: DBOS) -> None:
    """worker_concurrency is enforced *per partition* on a partitioned queue.

    Each partition independently runs up to worker_concurrency async workflows
    concurrently on this worker; partitions neither share the limit nor block
    one another.
    """

    worker_concurrency = 2
    partitions = ["partition-a", "partition-b"]
    wfs_per_partition = 4

    unblock_event = threading.Event()
    # Async workflows run on the background event loop thread while these
    # assertions run on the test's loop, so guard the shared counters with a lock.
    lock = threading.Lock()
    running: dict[str, int] = {p: 0 for p in partitions}
    max_running: dict[str, int] = {p: 0 for p in partitions}

    @DBOS.workflow()
    async def blocking_workflow(partition: str) -> str:
        with lock:
            running[partition] += 1
            max_running[partition] = max(max_running[partition], running[partition])
        while not unblock_event.is_set():
            await asyncio.sleep(0.05)
        with lock:
            running[partition] -= 1
        assert DBOS.workflow_id is not None
        return DBOS.workflow_id

    await DBOS.register_queue_async(
        "queue", partition_queue=True, worker_concurrency=worker_concurrency
    )

    # Enqueue more workflows per partition than the worker may run at once.
    handles: list[WorkflowHandleAsync[str]] = []
    for partition in partitions:
        with SetEnqueueOptions(queue_partition_key=partition):
            for _ in range(wfs_per_partition):
                handles.append(
                    await DBOS.enqueue_workflow_async(
                        "queue", blocking_workflow, partition
                    )
                )

    # Each partition should independently saturate its worker_concurrency limit.
    # If the limit were shared across partitions, they would compete and could
    # not all reach worker_concurrency at the same time.
    def all_partitions_saturated() -> None:
        with lock:
            for partition in partitions:
                assert running[partition] == worker_concurrency, (
                    f"partition {partition} has {running[partition]} running, "
                    f"expected {worker_concurrency}"
                )

    await retry_until_success_async(all_partitions_saturated)

    # Let several polling intervals pass and confirm no partition ever exceeded
    # its worker_concurrency limit.
    await asyncio.sleep(2)
    with lock:
        for partition in partitions:
            assert max_running[partition] == worker_concurrency

    # Unblock everything and confirm every workflow completes successfully.
    unblock_event.set()
    for handle in handles:
        assert await handle.get_result() is not None

    assert queue_entries_are_cleaned_up(dbos)


def _enqueue_partition_rows(
    dbos: DBOS,
    func: Any,
    queue_name: str,
    prefix: str,
    partitions: List[str],
    per_partition: int,
) -> dict[str, List[str]]:
    """Insert ENQUEUED rows for func across partitions of an unpolled queue.
    Returns each partition's workflow IDs in enqueue (created_at) order."""
    from dbos._core import prepare_enqueued_workflow

    ids: dict[str, List[str]] = {p: [] for p in partitions}
    statuses = []
    for partition in partitions:
        for i in range(per_partition):
            wfid = f"{prefix}-{partition}-{i}"
            statuses.append(
                prepare_enqueued_workflow(
                    dbos,
                    func,
                    (wfid,),
                    {},
                    queue_name=queue_name,
                    workflow_id=wfid,
                    queue_partition_key=partition,
                )
            )
            ids[partition].append(wfid)
    inserted = dbos._sys_db.init_workflows(statuses)
    assert len(inserted) == len(partitions) * per_partition
    return ids


def test_partitioned_batch_dequeue_sweep_cap(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A sweep admits at most PARTITIONED_DEQUEUE_SWEEP_CAP heads in partition
    order; admitted partitions are PENDING-gated, so the next sweep rotates
    onward to the remaining partitions."""

    @DBOS.workflow()
    def batch_wf(value: str) -> None:
        pass

    queue_name = f"unpolled-sweep-{uuid.uuid4().hex[:8]}"
    queue = Queue(
        queue_name, concurrency=1, partition_queue=True, database_backed_queue=True
    )
    partitions = [f"p{i}" for i in range(8)]
    ids = _enqueue_partition_rows(dbos, batch_wf, queue_name, "sweep", partitions, 1)

    monkeypatch.setattr(type(dbos._sys_db), "PARTITIONED_DEQUEUE_SWEEP_CAP", 5)

    def start() -> List[str]:
        return dbos._sys_db.start_queued_partitioned_workflows(
            queue, GlobalParams.executor_id, GlobalParams.app_version
        )

    assert start() == [ids[p][0] for p in partitions[:5]]
    assert start() == [ids[p][0] for p in partitions[5:]]
    assert start() == []


def test_partitioned_batch_dequeue_exclusive_direct(dbos: DBOS) -> None:
    """With concurrency=1, one batched call admits exactly each partition's
    head-of-line row; a partition admits nothing more until its head finishes."""

    @DBOS.workflow()
    def batch_wf(value: str) -> None:
        pass

    queue_name = f"unpolled-excl-{uuid.uuid4().hex[:8]}"
    queue = Queue(
        queue_name, concurrency=1, partition_queue=True, database_backed_queue=True
    )
    partitions = ["p0", "p1", "p2"]
    ids = _enqueue_partition_rows(dbos, batch_wf, queue_name, "excl", partitions, 3)

    def start() -> List[str]:
        return dbos._sys_db.start_queued_partitioned_workflows(
            queue, GlobalParams.executor_id, GlobalParams.app_version
        )

    # Heads only, one per partition
    assert start() == [ids[p][0] for p in partitions]
    # Every partition has a PENDING head, so nothing else is admitted
    assert start() == []
    # Completing one partition's head opens that partition alone
    set_workflow_status(dbos._sys_db, ids["p1"][0], WorkflowStatusString.SUCCESS.value)
    assert start() == [ids["p1"][1]]


def test_partitioned_batch_dequeue_skips_requeued_rows(dbos: DBOS) -> None:
    """A candidate moved to another queue mid-sweep (e.g. by resume_workflows,
    which rewrites queue_name while leaving status ENQUEUED) must be dropped by
    the claim guard, not flipped and run under the wrong queue."""
    from sqlalchemy import event

    @DBOS.workflow()
    def batch_wf(value: str) -> None:
        pass

    queue_name = f"unpolled-requeue-{uuid.uuid4().hex[:8]}"
    queue = Queue(
        queue_name, concurrency=1, partition_queue=True, database_backed_queue=True
    )
    ids = _enqueue_partition_rows(dbos, batch_wf, queue_name, "requeue", ["p0"], 3)
    head_id = ids["p0"][0]
    # Resume onto another unpolled queue: the internal queue is live, and its worker would drain the row before the assertions below.
    resume_target = f"unpolled-resume-{uuid.uuid4().hex[:8]}"

    # Fire between the candidate snapshot (starts WITH RECURSIVE) and the lock select, which is the sweep's only SELECT keyed on workflow_uuid.
    moved = threading.Event()

    def before_cursor_execute(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        stmt = statement.lstrip().upper()
        if (
            not moved.is_set()
            and stmt.startswith("SELECT")
            and "WORKFLOW_UUID IN" in stmt
        ):
            moved.set()  # Set first: resume_workflows itself runs a SELECT ... IN
            dbos._sys_db.resume_workflows([head_id], queue_name=resume_target)

    event.listen(dbos._sys_db.engine, "before_cursor_execute", before_cursor_execute)
    try:
        ret = dbos._sys_db.start_queued_partitioned_workflows(
            queue, GlobalParams.executor_id, GlobalParams.app_version
        )
    finally:
        event.remove(
            dbos._sys_db.engine, "before_cursor_execute", before_cursor_execute
        )
    assert moved.is_set()
    # The moved head was the sole candidate and must not have been claimed
    assert ret == []
    status = dbos._sys_db.get_workflow_status(head_id)
    assert status is not None
    assert status["status"] == WorkflowStatusString.ENQUEUED.value
    assert status["queue_name"] == resume_target

    # The next sweep sees the remaining row as the partition's new head
    assert dbos._sys_db.start_queued_partitioned_workflows(
        queue, GlobalParams.executor_id, GlobalParams.app_version
    ) == [ids["p0"][1]]

    if not using_sqlite():
        return
    # SQLite holds no row locks, so a racer can still move a candidate after the lock select:
    # the flip's own guard must reject it and RETURNING must report it as unclaimed.
    set_workflow_status(dbos._sys_db, ids["p0"][1], WorkflowStatusString.SUCCESS.value)
    moved_late = threading.Event()

    def before_update(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        if not moved_late.is_set() and statement.lstrip().upper().startswith("UPDATE"):
            moved_late.set()  # Set first: resume_workflows itself runs an UPDATE
            dbos._sys_db.resume_workflows([ids["p0"][2]], queue_name=resume_target)

    event.listen(dbos._sys_db.engine, "before_cursor_execute", before_update)
    try:
        assert (
            dbos._sys_db.start_queued_partitioned_workflows(
                queue, GlobalParams.executor_id, GlobalParams.app_version
            )
            == []
        )
    finally:
        event.remove(dbos._sys_db.engine, "before_cursor_execute", before_update)
    assert moved_late.is_set()
    late_status = dbos._sys_db.get_workflow_status(ids["p0"][2])
    assert late_status is not None
    assert late_status["status"] == WorkflowStatusString.ENQUEUED.value


def test_partitioned_batch_dequeue_contention(dbos: DBOS) -> None:
    """Two workers batch-dequeueing concurrently never co-admit into a
    partition: candidates are heads only, so racing flips target the same row
    and SKIP LOCKED/status rechecks make one side lose."""

    @DBOS.workflow()
    def batch_wf(value: str) -> None:
        pass

    queue_name = f"unpolled-race-{uuid.uuid4().hex[:8]}"
    queue = Queue(
        queue_name, concurrency=1, partition_queue=True, database_backed_queue=True
    )
    partitions = [f"p{i}" for i in range(4)]
    ids = _enqueue_partition_rows(dbos, batch_wf, queue_name, "race", partitions, 2)

    barrier = threading.Barrier(2)

    def call(slot: int) -> List[str]:
        barrier.wait()
        return dbos._sys_db.start_queued_partitioned_workflows(
            queue, f"executor-{slot}", GlobalParams.app_version
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(call, slot) for slot in range(2)]
        # future.result() re-raises: a racer that dies must fail the test, not silently return nothing
        admitted = [id for future in futures for id in future.result()]

    # Whichever racer wins each head, every head is claimed exactly once and no follower is
    assert sorted(admitted) == sorted(ids[p][0] for p in partitions)
    with dbos._sys_db.engine.begin() as c:
        rows = c.execute(
            sa.select(
                SystemSchema.workflow_status.c.queue_partition_key, sa.func.count()
            )
            .where(SystemSchema.workflow_status.c.queue_name == queue_name)
            .where(
                SystemSchema.workflow_status.c.status
                == WorkflowStatusString.PENDING.value
            )
            .group_by(SystemSchema.workflow_status.c.queue_partition_key)
        ).fetchall()
    # The returned IDs match the rows actually flipped: one PENDING per partition
    assert {key: count for key, count in rows} == {p: 1 for p in partitions}


def test_partitioned_queue_fallback_routing(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Partitioned configs the batched path does not support -- a rate limiter, and
    worker_concurrency=0 (the pause-dequeue idiom, which it would ignore) -- route to
    the per-partition sweep, which drains the first and honors the pause on the second.
    """

    @DBOS.workflow()
    def routed_wf(tag: str) -> str:
        return tag

    # Both are concurrency=1 partitioned queues, so only the limiter / the pause excludes them.
    limiter_queue = Queue(
        f"limiter_fallback_{uuid.uuid4().hex[:8]}",
        concurrency=1,
        limiter={"limit": 10, "period": 60},
        partition_queue=True,
        polling_interval_sec=0.25,
    )
    paused_queue = Queue(
        f"paused_fallback_{uuid.uuid4().hex[:8]}",
        concurrency=1,
        worker_concurrency=0,
        partition_queue=True,
        polling_interval_sec=0.25,
    )

    batched_queues: List[str] = []
    swept_queues: List[str] = []
    real_batched = dbos._sys_db.start_queued_partitioned_workflows
    real_single = dbos._sys_db.start_queued_workflows

    def spying_batched(queue_arg: Queue, *args: Any, **kwargs: Any) -> List[str]:
        batched_queues.append(queue_arg.name)
        return real_batched(queue_arg, *args, **kwargs)

    def spying_single(queue_arg: Queue, *args: Any, **kwargs: Any) -> List[str]:
        swept_queues.append(queue_arg.name)
        return real_single(queue_arg, *args, **kwargs)

    monkeypatch.setattr(
        dbos._sys_db, "start_queued_partitioned_workflows", spying_batched
    )
    monkeypatch.setattr(dbos._sys_db, "start_queued_workflows", spying_single)

    handles = []
    for partition in ["p0", "p1"]:
        with SetEnqueueOptions(queue_partition_key=partition):
            handles.append(limiter_queue.enqueue(routed_wf, f"limited-{partition}"))
    with SetEnqueueOptions(queue_partition_key="p0"):
        paused_handle = paused_queue.enqueue(routed_wf, "paused")

    for handle in handles:
        assert handle.get_result()

    def paused_queue_swept() -> None:
        assert paused_queue.name in swept_queues

    retry_until_success(paused_queue_swept, interval=0.1, max_attempts=100)
    assert limiter_queue.name not in batched_queues
    assert paused_queue.name not in batched_queues
    assert paused_handle.get_status().status == WorkflowStatusString.ENQUEUED.value


def test_partitioned_batch_dequeue_version_gating(dbos: DBOS) -> None:
    """The batched path dequeues only rows of this worker's app version, plus
    version-less rows while this worker runs the latest registered version. A
    partition holding no eligible row is skipped without disturbing the others."""

    @DBOS.workflow()
    def batch_wf(value: str) -> None:
        pass

    queue_name = f"unpolled-ver-{uuid.uuid4().hex[:8]}"
    queue = Queue(
        queue_name, concurrency=1, partition_queue=True, database_backed_queue=True
    )
    ids = _enqueue_partition_rows(dbos, batch_wf, queue_name, "ver", ["p0", "p1"], 3)

    def pin_version(wfid: str, version: Any) -> None:
        with dbos._sys_db.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
                .values(application_version=version)
            )

    pin_version(ids["p0"][0], "some-other-version")
    pin_version(ids["p0"][1], GlobalParams.app_version)
    pin_version(ids["p0"][2], None)
    # p1 is entirely ineligible: its head probe yields nothing, so the partition never appears below.
    for wfid in ids["p1"]:
        pin_version(wfid, "some-other-version")

    def start() -> List[str]:
        return dbos._sys_db.start_queued_partitioned_workflows(
            queue, GlobalParams.executor_id, GlobalParams.app_version
        )

    # The other-version row is invisible, so the head is row 1; version-less row 2 follows once it completes (this worker is latest).
    assert start() == [ids["p0"][1]]
    set_workflow_status(dbos._sys_db, ids["p0"][1], WorkflowStatusString.SUCCESS.value)
    assert start() == [ids["p0"][2]]

    # Registering a newer version demotes this worker: version-less rows now belong to the newer one.
    set_workflow_status(dbos._sys_db, ids["p0"][2], WorkflowStatusString.ENQUEUED.value)
    pin_version(
        ids["p0"][2], None
    )  # dequeueing it above stamped this worker's version on
    now_ms = int(time.time() * 1000)
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.application_versions).values(
                version_id="newer-than-this-worker",
                version_name="newer-than-this-worker",
                version_timestamp=now_ms + 3_600_000,
                created_at=now_ms,
            )
        )
    assert start() == []


def test_partitioned_batch_dequeue_sqlite_plan(dbos: DBOS) -> None:
    """The batched candidates query must seek idx_workflow_status_partition_dequeue_v2.
    SQLite's partial-index prover runs at prepare time, so this regresses silently
    (full scan) if the literal predicate conjuncts are dropped from the query."""
    if not using_sqlite():
        pytest.skip("Plan assertion is SQLite-specific")

    @DBOS.workflow()
    def batch_wf(value: str) -> None:
        pass

    queue_name = f"unpolled-plan-{uuid.uuid4().hex[:8]}"
    queue = Queue(
        queue_name, concurrency=1, partition_queue=True, database_backed_queue=True
    )
    ids = _enqueue_partition_rows(dbos, batch_wf, queue_name, "plan", ["p0", "p1"], 2)

    captured: List[Any] = []

    def before_cursor_execute(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        # The candidates query is the only WITH RECURSIVE this sweep emits (get_queue_partitions, the other one, runs on the fallback path).
        if "recursive" in statement.lower():
            captured.append((statement, parameters))

    from sqlalchemy import event

    event.listen(dbos._sys_db.engine, "before_cursor_execute", before_cursor_execute)
    try:
        ret = dbos._sys_db.start_queued_partitioned_workflows(
            queue, GlobalParams.executor_id, GlobalParams.app_version
        )
    finally:
        event.remove(
            dbos._sys_db.engine, "before_cursor_execute", before_cursor_execute
        )
    assert ret == [ids["p0"][0], ids["p1"][0]]
    assert captured
    statement, parameters = captured[0]
    with dbos._sys_db.engine.connect() as conn:
        plan = conn.exec_driver_sql(
            f"EXPLAIN QUERY PLAN {statement}", parameters
        ).fetchall()
    details = [str(row[-1]) for row in plan]
    assert any("idx_workflow_status_partition_dequeue_v2" in d for d in details)
    # Every workflow_status access must be a seek: asserting only that the index is named somewhere still passes when one probe (e.g. the PENDING gate) regresses to a scan.
    assert not [d for d in details if d.startswith("SCAN") and "workflow_status" in d]


def test_partitioned_queue_global_exclusivity(
    dbos: DBOS,
    monkeypatch: pytest.MonkeyPatch,
    skip_with_sqlite_imprecise_time: None,
) -> None:
    """End-to-end concurrency=1: the queue worker dispatches to the batched path, and a
    partition runs strictly one workflow at a time in FIFO order, gated globally (no
    worker_concurrency is set, so only the PENDING-row check holds followers back)."""

    order_lock = threading.Lock()
    execution_order: List[str] = []
    blocking_event = threading.Event()
    waiting_event = threading.Event()

    @DBOS.workflow()
    def head_workflow() -> str:
        with order_lock:
            execution_order.append("head")
        waiting_event.set()
        blocking_event.wait()
        assert DBOS.workflow_id
        return DBOS.workflow_id

    @DBOS.workflow()
    def tagged_workflow(tag: str) -> str:
        with order_lock:
            execution_order.append(tag)
        return tag

    queue = Queue(
        f"exclusive_{uuid.uuid4().hex[:8]}",
        concurrency=1,
        partition_queue=True,
        polling_interval_sec=0.25,
    )

    # Every other batched-path test calls the sweep directly, so this is what pins the dispatch itself.
    batched_queues: List[str] = []
    real_batched = dbos._sys_db.start_queued_partitioned_workflows

    def spying_batched(queue_arg: Queue, *args: Any, **kwargs: Any) -> List[str]:
        batched_queues.append(queue_arg.name)
        return real_batched(queue_arg, *args, **kwargs)

    monkeypatch.setattr(
        dbos._sys_db, "start_queued_partitioned_workflows", spying_batched
    )

    with SetEnqueueOptions(queue_partition_key="a"):
        head_handle = queue.enqueue(head_workflow)
        follower_1 = queue.enqueue(tagged_workflow, "a1")
        follower_2 = queue.enqueue(tagged_workflow, "a2")
    with SetEnqueueOptions(queue_partition_key="b"):
        other_handle = queue.enqueue(tagged_workflow, "b1")

    waiting_event.wait()
    # Partition b drains while a's head blocks; its completion proves a full sweep ran, making the follower assertions meaningful.
    assert other_handle.get_result() == "b1"
    assert follower_1.get_status().status == WorkflowStatusString.ENQUEUED.value
    assert follower_2.get_status().status == WorkflowStatusString.ENQUEUED.value

    blocking_event.set()
    assert head_handle.get_result()
    assert follower_1.get_result() == "a1"
    assert follower_2.get_result() == "a2"
    assert [tag for tag in execution_order if tag != "b1"] == ["head", "a1", "a2"]
    assert queue.name in batched_queues
    assert queue_entries_are_cleaned_up(dbos)


def test_polling_interval(dbos: DBOS) -> None:
    DBOS.register_queue("queue", polling_interval_sec=0.1)

    @DBOS.workflow()
    def workflow() -> str:
        assert DBOS.workflow_id
        return DBOS.workflow_id

    assert DBOS.enqueue_workflow("queue", workflow).get_result()

    for _ in range(10):
        start_time = time.time()
        assert DBOS.enqueue_workflow("queue", workflow).get_result(
            polling_interval_sec=0.1
        )
        assert time.time() - start_time < 1.0


def test_listen_queue(
    dbos: DBOS, config: DBOSConfig, skip_with_sqlite_imprecise_time: None
) -> None:
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    @DBOS.workflow()
    def workflow() -> str:
        assert DBOS.workflow_id
        return DBOS.workflow_id

    queue_list = ["queue_one"]
    DBOS.listen_queues(queue_list)
    DBOS.launch()
    DBOS.register_queue("queue_one")
    DBOS.register_queue("queue_two")

    # While only listening to queue one, only workflows enqueued there execute
    handle_one = DBOS.enqueue_workflow("queue_one", workflow)
    handle_two = DBOS.enqueue_workflow("queue_two", workflow)
    assert handle_one.get_result()
    assert handle_two.get_status().status == "ENQUEUED"

    DBOS.destroy()
    DBOS(config=config)
    DBOS.listen_queues(["queue_two"])
    DBOS.launch()

    # Listening to queue two completes its workflows
    assert DBOS.retrieve_workflow(handle_two.workflow_id).get_result()
    # Verify the internal queue works
    assert DBOS.fork_workflow(handle_two.workflow_id, 0).get_result()


def test_wait_first_queue(dbos: DBOS) -> None:
    num_tasks = 5
    DBOS.register_queue("wait_first_queue", concurrency=num_tasks)

    go_events = [threading.Event() for _ in range(num_tasks)]
    consumed_events = [threading.Event() for _ in range(num_tasks)]

    @DBOS.workflow()
    def process_task(task_id: int) -> str:
        go_events[task_id].wait()
        return f"result-{task_id}"

    @DBOS.workflow()
    def process_tasks() -> List[str]:
        handles: List[WorkflowHandle[str]] = []
        for i in range(num_tasks):
            handle = DBOS.enqueue_workflow("wait_first_queue", process_task, i)
            handles.append(handle)

        results: List[str] = []
        remaining = list(handles)
        for round_idx in range(num_tasks):
            completed = DBOS.wait_first(remaining)
            results.append(completed.get_result())
            remaining = [h for h in remaining if h.workflow_id != completed.workflow_id]
            consumed_events[round_idx].set()
        return results

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = dbos.start_workflow(process_tasks)

    # Release tasks in reverse order, waiting for each to be consumed
    for round_idx, task_id in enumerate(reversed(range(num_tasks))):
        go_events[task_id].set()
        consumed_events[round_idx].wait()

    result = handle.get_result()
    expected = [f"result-{i}" for i in reversed(range(num_tasks))]
    assert result == expected

    # Verify the steps are correct:
    # 5 enqueue steps + 5 (waitFirst, getResult) pairs = 15 steps
    steps = DBOS.list_workflow_steps(wfid)
    assert len(steps) == num_tasks * 3

    # First num_tasks steps are the enqueues
    for i in range(num_tasks):
        assert steps[i]["function_id"] == i + 1
        assert steps[i]["function_name"] == process_task.__qualname__
        assert steps[i]["child_workflow_id"] is not None

    # Remaining steps alternate between waitFirst and getResult
    for i in range(num_tasks):
        wait_step = steps[num_tasks + i * 2]
        result_step = steps[num_tasks + i * 2 + 1]
        assert wait_step["function_name"] == "DBOS.waitFirst"
        assert result_step["function_name"] == "DBOS.getResult"

    # Fork from the last waitFirst step and verify same result
    last_wait_first_step = steps[num_tasks + (num_tasks - 1) * 2]["function_id"]
    forked_handle = DBOS.fork_workflow(wfid, last_wait_first_step)
    assert forked_handle.get_result() == expected

    assert queue_entries_are_cleaned_up(dbos)


def test_delay(
    dbos: DBOS, client: DBOSClient, skip_with_sqlite_imprecise_time: None
) -> None:
    # dequeued_at is database-stamped and delay_until_epoch_ms is not, so the
    # assertions below need the two clocks at the same resolution.
    DBOS.register_queue("test_delay_queue", polling_interval_sec=0.1)

    @DBOS.workflow()
    def test_workflow() -> None:
        pass

    delay_seconds = 2.0

    # Test via SetEnqueueOptions
    t_before = int(time.time() * 1000)
    with SetEnqueueOptions(delay_seconds=delay_seconds):
        handle = DBOS.enqueue_workflow("test_delay_queue", test_workflow)
    t_after = int(time.time() * 1000)

    status = handle.get_status()
    assert status.status == WorkflowStatusString.DELAYED.value
    assert status.delay_until_epoch_ms is not None
    assert status.delay_until_epoch_ms >= t_before + int(delay_seconds * 1000)
    assert status.delay_until_epoch_ms <= t_after + int(delay_seconds * 1000)

    handle.get_result()

    final_status = handle.get_status()
    assert final_status.status == WorkflowStatusString.SUCCESS.value
    assert final_status.dequeued_at is not None
    assert final_status.dequeued_at >= status.delay_until_epoch_ms

    # Test via client enqueue
    t_before = int(time.time() * 1000)
    client_handle: WorkflowHandle[None] = client.enqueue(
        {
            "queue_name": "test_delay_queue",
            "workflow_name": test_workflow.__qualname__,
            "delay_seconds": delay_seconds,
        }
    )
    t_after = int(time.time() * 1000)

    client_status = client_handle.get_status()
    assert client_status.status == WorkflowStatusString.DELAYED.value
    assert client_status.delay_until_epoch_ms is not None
    assert client_status.delay_until_epoch_ms >= t_before + int(delay_seconds * 1000)
    assert client_status.delay_until_epoch_ms <= t_after + int(delay_seconds * 1000)

    client_handle.get_result()

    final_client_status = client_handle.get_status()
    assert final_client_status.status == WorkflowStatusString.SUCCESS.value
    assert final_client_status.dequeued_at is not None
    assert final_client_status.dequeued_at >= client_status.delay_until_epoch_ms

    # Delayed workflows appear in list_workflows and list_queued_workflows
    with SetEnqueueOptions(delay_seconds=60.0):
        listed_handle = DBOS.enqueue_workflow("test_delay_queue", test_workflow)
    all_workflows = DBOS.list_workflows(status=WorkflowStatusString.DELAYED.value)
    assert any(w.workflow_id == listed_handle.workflow_id for w in all_workflows)
    queued_workflows = DBOS.list_queued_workflows()
    assert any(w.workflow_id == listed_handle.workflow_id for w in queued_workflows)

    # wait_first treats DELAYED as active and unblocks when it completes
    with SetEnqueueOptions(delay_seconds=1.0):
        wait_handle = DBOS.enqueue_workflow("test_delay_queue", test_workflow)
    assert wait_handle.get_status().status == WorkflowStatusString.DELAYED.value
    completed = DBOS.wait_first([wait_handle])
    assert completed.workflow_id == wait_handle.workflow_id
    completed.get_result()
    assert completed.get_status().status == WorkflowStatusString.SUCCESS.value

    # Deduplication: a second enqueue with the same dedup ID should fail while DELAYED
    dedup_id = str(uuid.uuid4())
    with SetEnqueueOptions(delay_seconds=60.0, deduplication_id=dedup_id):
        dedup_handle = DBOS.enqueue_workflow("test_delay_queue", test_workflow)
    assert dedup_handle.get_status().status == WorkflowStatusString.DELAYED.value
    with pytest.raises(DBOSQueueDeduplicatedError):
        with SetEnqueueOptions(delay_seconds=60.0, deduplication_id=dedup_id):
            DBOS.enqueue_workflow("test_delay_queue", test_workflow)


def test_delay_cancel_resume_list(dbos: DBOS) -> None:
    DBOS.register_queue("test_delay_cancel_resume_queue", polling_interval_sec=0.1)

    @DBOS.workflow()
    def test_workflow() -> str:
        return "done"

    # Cancel a DELAYED workflow — it should never run
    with SetEnqueueOptions(delay_seconds=60.0):
        cancel_handle = DBOS.enqueue_workflow(
            "test_delay_cancel_resume_queue", test_workflow
        )
    assert cancel_handle.get_status().status == WorkflowStatusString.DELAYED.value
    DBOS.cancel_workflow(cancel_handle.workflow_id)
    assert cancel_handle.get_status().status == WorkflowStatusString.CANCELLED.value

    # Verify it never appears in the queue after cancellation
    queued = DBOS.list_queued_workflows()
    assert not any(w.workflow_id == cancel_handle.workflow_id for w in queued)

    # Resume a DELAYED workflow — it should run immediately, bypassing the delay
    with SetEnqueueOptions(delay_seconds=60.0):
        resume_handle = DBOS.enqueue_workflow(
            "test_delay_cancel_resume_queue", test_workflow
        )
    assert resume_handle.get_status().status == WorkflowStatusString.DELAYED.value
    DBOS.resume_workflow(resume_handle.workflow_id)
    assert resume_handle.get_result() == "done"
    final = resume_handle.get_status()
    assert final.status == WorkflowStatusString.SUCCESS.value


def test_set_workflow_delay(dbos: DBOS) -> None:
    DBOS.register_queue("test_set_workflow_delay_queue", polling_interval_sec=0.1)

    @DBOS.workflow()
    def test_workflow() -> str:
        return "done"

    # Enqueue with a long delay, then shorten it with set_workflow_delay
    with SetEnqueueOptions(delay_seconds=600.0):
        handle = DBOS.enqueue_workflow("test_set_workflow_delay_queue", test_workflow)
    assert handle.get_status().status == WorkflowStatusString.DELAYED.value

    # Use delay_seconds to set a short delay
    DBOS.set_workflow_delay(handle.workflow_id, delay_seconds=0.5)
    status = handle.get_status()
    assert status.status == WorkflowStatusString.DELAYED.value
    # The delay should have been shortened
    assert status.delay_until_epoch_ms is not None
    assert status.delay_until_epoch_ms < int(time.time() * 1000) + 5000

    # Should complete after the short delay
    assert handle.get_result() == "done"
    assert handle.get_status().status == WorkflowStatusString.SUCCESS.value

    # Test with delay_until_epoch_ms (absolute timestamp)
    with SetEnqueueOptions(delay_seconds=600.0):
        handle2 = DBOS.enqueue_workflow("test_set_workflow_delay_queue", test_workflow)
    assert handle2.get_status().status == WorkflowStatusString.DELAYED.value

    soon = int(time.time() * 1000) + 500  # 0.5 seconds from now
    DBOS.set_workflow_delay(handle2.workflow_id, delay_until_epoch_ms=soon)
    status2 = handle2.get_status()
    assert status2.status == WorkflowStatusString.DELAYED.value
    assert status2.delay_until_epoch_ms == soon

    assert handle2.get_result() == "done"
    assert handle2.get_status().status == WorkflowStatusString.SUCCESS.value


def test_enqueued_async_workflow_survives_gc(dbos: DBOS) -> None:
    """Regression test for https://github.com/dbos-inc/dbos-transact-py/issues/710"""

    entered = threading.Event()
    interrupted: List[str] = []
    loops: List[asyncio.AbstractEventLoop] = []
    fut_refs: List["weakref.ref[asyncio.Future[str]]"] = []

    @DBOS.workflow()
    async def hanging_workflow() -> str:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str] = loop.create_future()
        loops.append(loop)
        # Expose the future only weakly so the test does not itself keep the
        # workflow task reachable.
        fut_refs.append(weakref.ref(fut))
        entered.set()
        try:
            # Suspend on a future rooted only in this frame, as application
            # code awaiting library internals can. Only the reference in
            # dbos._workflow_tasks keeps this task reachable.
            return await fut
        except BaseException as exc:
            interrupted.append(type(exc).__name__)
            raise

    DBOS.register_queue("gc_pin_queue")
    handle = DBOS.enqueue_workflow("gc_pin_queue", hanging_workflow)

    assert entered.wait(timeout=30)
    time.sleep(0.2)  # let the workflow suspend on its future

    # The running task must be pinned in the instance-level strong-reference set
    assert len(dbos._workflow_tasks) == 1

    gc.collect()
    time.sleep(0.2)

    # Unpinned, gc.collect() destroys the task: interrupted == ["GeneratorExit"]
    assert interrupted == []
    assert len(dbos._workflow_tasks) == 1

    # The workflow is still alive: unblock it and verify it completes.
    fut = fut_refs[0]()
    assert fut is not None, "workflow's pending future was garbage-collected"
    loops[0].call_soon_threadsafe(fut.set_result, "done")
    assert handle.get_result() == "done"  # type: ignore

    # Once the workflow completes, the done-callback must release the strong
    # reference so finished tasks are not leaked. The result is recorded
    # before the task finishes, so poll briefly for the callback to run.
    def check_task_released() -> None:
        assert not dbos._workflow_tasks

    retry_until_success(check_task_released, interval=0.1, max_attempts=50)


def test_enqueue_with_options(dbos: DBOS) -> None:
    @DBOS.workflow(name="with_options_target")
    def with_options_target(x: int, y: int = 0) -> int:
        return x + y

    DBOS.register_queue("with_options_queue")

    handle: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(
        {"workflow_name": "with_options_target", "queue_name": "with_options_queue"},
        5,
        y=3,
    )
    assert handle.get_result() == 8

    status = handle.get_status()
    assert status.name == "with_options_target"
    assert status.queue_name == "with_options_queue"
    assert queue_entries_are_cleaned_up(dbos)


def test_enqueue_with_options_passthrough(dbos: DBOS) -> None:
    @DBOS.workflow(name="with_options_passthrough_target")
    def with_options_passthrough_target(x: int) -> int:
        return x * 2

    DBOS.register_queue("with_options_passthrough_queue")

    wfid = str(uuid.uuid4())
    handle: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(
        {
            "workflow_name": "with_options_passthrough_target",
            "queue_name": "with_options_passthrough_queue",
            "workflow_id": wfid,
            "app_version": GlobalParams.app_version,
            "deduplication_id": "dedup-key",
            "authenticated_user": "alice",
            "authenticated_roles": ["admin"],
        },
        21,
    )
    assert handle.get_workflow_id() == wfid
    assert handle.get_result() == 42

    status = handle.get_status()
    assert status.app_version == GlobalParams.app_version
    assert status.authenticated_user == "alice"
    assert status.authenticated_roles == ["admin"]

    # The deduplication ID is released once the first workflow completes.
    handle2: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(
        {
            "workflow_name": "with_options_passthrough_target",
            "queue_name": "with_options_passthrough_queue",
            "deduplication_id": "dedup-key",
        },
        21,
    )
    assert handle2.get_result() == 42

    # Park a workflow to hold a deduplication ID for the collisions below.
    parked: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(
        {
            "workflow_name": "with_options_passthrough_target",
            "queue_name": "with_options_passthrough_queue",
            "deduplication_id": "held-key",
            "delay_seconds": 3600,
            "workflow_timeout": 300,
        },
        21,
    )
    # An explicit timeout survives; it is not overwritten by the ambient deadline.
    assert parked.get_status().workflow_timeout_ms == 300000

    # Colliding with a held key outside a workflow raises directly.
    with pytest.raises(DBOSQueueDeduplicatedError):
        DBOS.enqueue_workflow_with_options(
            {
                "workflow_name": "with_options_passthrough_target",
                "queue_name": "with_options_passthrough_queue",
                "deduplication_id": "held-key",
            },
            21,
        )

    @DBOS.workflow()
    def with_options_dedup_parent() -> int:
        handle: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(
            {
                "workflow_name": "with_options_passthrough_target",
                "queue_name": "with_options_passthrough_queue",
                "deduplication_id": "held-key",
            },
            21,
        )
        return handle.get_result()

    parent_id = str(uuid.uuid4())
    with SetWorkflowID(parent_id):
        with pytest.raises(DBOSQueueDeduplicatedError):
            with_options_dedup_parent()

    # The failed enqueue is checkpointed against the parent: an error, no child.
    steps = DBOS.list_workflow_steps(parent_id)
    assert len(steps) == 1
    assert steps[0]["error"] is not None
    assert steps[0]["child_workflow_id"] is None

    # Free the key BEFORE recovery, so a re-attempted enqueue would now succeed.
    # Only a replay of the checkpointed error can still raise here.
    DBOS.cancel_workflow(parked.get_workflow_id())
    set_workflow_status(dbos._sys_db, parent_id, "PENDING")
    recovered = [
        h for h in DBOS._recover_pending_workflows() if h.get_workflow_id() == parent_id
    ]
    assert len(recovered) == 1
    with pytest.raises(DBOSQueueDeduplicatedError):
        recovered[0].get_result()


def test_enqueue_with_options_unknown_workflow(dbos: DBOS) -> None:
    """A name this executor cannot resolve is enqueued without complaint: the
    point of the API is that the target is implemented elsewhere."""
    DBOS.register_queue("with_options_unknown_queue")

    handle: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(
        {
            "workflow_name": "not_registered_anywhere",
            "queue_name": "with_options_unknown_queue",
            # Parks the row so no worker dequeues a name it cannot run.
            "delay_seconds": 3600,
        },
        1,
    )
    status = handle.get_status()
    assert status.status == WorkflowStatusString.DELAYED.value
    # The target may live in another executor, so the row is left unpinned.
    assert status.app_version is None
    DBOS.cancel_workflow(handle.get_workflow_id())


def test_enqueue_with_options_child(dbos: DBOS) -> None:
    child_counter: int = 0

    @DBOS.workflow(name="with_options_child")
    def with_options_child(x: int) -> int:
        nonlocal child_counter
        child_counter += 1
        return x + 1

    @DBOS.workflow()
    def with_options_parent(x: int) -> int:
        # One options dict, enqueued twice: the caller's copy must not be mutated.
        options: EnqueueOptions = {
            "workflow_name": "with_options_child",
            "queue_name": "with_options_child_queue",
        }
        first: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(options, x)
        second: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(
            options, x + 10
        )
        return first.get_result() + second.get_result()

    DBOS.register_queue("with_options_child_queue")

    wfid = str(uuid.uuid4())
    with SetWorkflowTimeout(300):
        with SetWorkflowID(wfid):
            assert with_options_parent(1) == 14
    assert child_counter == 2

    # Each child is recorded against the parent, with an ID from the parent's
    # function counter; the trailing steps are the two get_results.
    steps = DBOS.list_workflow_steps(wfid)
    assert len(steps) == 4
    assert steps[0]["function_name"] == "with_options_child"
    assert steps[0]["child_workflow_id"] == f"{wfid}-1"
    assert steps[1]["child_workflow_id"] == f"{wfid}-2"
    assert DBOS.retrieve_workflow(f"{wfid}-1").get_status().parent_workflow_id == wfid

    # The children inherit the parent's deadline instead of running unbounded.
    parent_deadline = (
        DBOS.retrieve_workflow(wfid).get_status().workflow_deadline_epoch_ms
    )
    assert parent_deadline is not None
    for child_id in (f"{wfid}-1", f"{wfid}-2"):
        child_status = DBOS.retrieve_workflow(child_id).get_status()
        assert child_status.workflow_deadline_epoch_ms == parent_deadline

    # On recovery the parent re-runs but returns the recorded children, not new ones.
    # A re-attempted enqueue would rebuild the same deterministic ID and upsert
    # over the finished child, so watch updated_at: only a replay leaves it alone.
    child_updated_at = DBOS.retrieve_workflow(f"{wfid}-1").get_status().updated_at
    set_workflow_status(dbos._sys_db, wfid, "PENDING")
    handles = DBOS._recover_pending_workflows()
    assert len(handles) == 1
    assert handles[0].get_result() == 14
    assert child_counter == 2
    assert (
        DBOS.retrieve_workflow(f"{wfid}-1").get_status().updated_at == child_updated_at
    )


@pytest.mark.asyncio
async def test_enqueue_with_options_async(dbos: DBOS) -> None:
    @DBOS.workflow(name="with_options_async_child")
    async def with_options_async_child(x: int) -> int:
        return x + 1

    @DBOS.workflow()
    async def with_options_async_parent(x: int) -> int:
        handle: WorkflowHandleAsync[int] = (
            await DBOS.enqueue_workflow_with_options_async(
                {
                    "workflow_name": "with_options_async_child",
                    "queue_name": "with_options_async_queue",
                },
                x,
            )
        )
        return await handle.get_result()

    await DBOS.register_queue_async("with_options_async_queue")

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert await with_options_async_parent(1) == 2

    steps = await DBOS.list_workflow_steps_async(wfid)
    assert steps[0]["function_name"] == "with_options_async_child"
    assert steps[0]["child_workflow_id"] == f"{wfid}-1"


def test_enqueue_with_options_ambient_context(dbos: DBOS) -> None:
    @DBOS.workflow(name="with_options_ctx_target")
    def with_options_ctx_target(x: int) -> int:
        return x

    DBOS.register_queue("with_options_ctx_queue")

    # Ambient enqueue options apply here like they do to any other enqueue.
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with SetEnqueueOptions(app_version="ambient-version"):
            handle: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(
                {
                    "workflow_name": "with_options_ctx_target",
                    "queue_name": "with_options_ctx_queue",
                },
                1,
            )
    assert handle.get_workflow_id() == wfid
    status = handle.get_status()
    assert status.app_version == "ambient-version"
    # No executor runs that version, so the workflow stays enqueued.
    assert status.status == WorkflowStatusString.ENQUEUED.value
    DBOS.cancel_workflow(wfid)

    # Ambient deduplication_id, priority and delay all reach the row. The
    # explicit None must count as unset, or it would defeat the fallback.
    ambient_opts: EnqueueOptions = {
        "workflow_name": "with_options_ctx_target",
        "queue_name": "with_options_ctx_queue",
    }
    ambient_opts["deduplication_id"] = None  # type: ignore[typeddict-item]
    with SetEnqueueOptions(
        deduplication_id="ambient-dedup", priority=5, delay_seconds=3600
    ):
        ambient: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(
            ambient_opts, 3
        )
    ambient_status = ambient.get_status()
    assert ambient_status.status == WorkflowStatusString.DELAYED.value
    assert ambient_status.deduplication_id == "ambient-dedup"
    assert ambient_status.priority == 5
    DBOS.cancel_workflow(ambient.get_workflow_id())

    # An ambient partition key reaches the row too, where it is rejected
    # alongside an explicit deduplication ID.
    with pytest.raises(DBOSException):
        with SetEnqueueOptions(queue_partition_key="tenant-1"):
            DBOS.enqueue_workflow_with_options(
                {
                    "workflow_name": "with_options_ctx_target",
                    "queue_name": "with_options_ctx_queue",
                    "deduplication_id": "never-written",
                },
                4,
            )

    # Ambient authentication is inherited when the options do not set it.
    with DBOSContextSetAuth("bob", ["auditor"]):
        with SetEnqueueOptions(delay_seconds=3600):
            authed: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(
                {
                    "workflow_name": "with_options_ctx_target",
                    "queue_name": "with_options_ctx_queue",
                },
                5,
            )
    authed_status = authed.get_status()
    assert authed_status.authenticated_user == "bob"
    assert authed_status.authenticated_roles == ["auditor"]
    DBOS.cancel_workflow(authed.get_workflow_id())

    # Options attributes merge with ambient ones rather than replacing them.
    with SetWorkflowAttributes({"ambient": "a"}):
        with SetEnqueueOptions(delay_seconds=3600):
            merged: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(
                {
                    "workflow_name": "with_options_ctx_target",
                    "queue_name": "with_options_ctx_queue",
                    "attributes": {"explicit": "b"},
                },
                6,
            )
    assert merged.get_status().attributes == {"ambient": "a", "explicit": "b"}
    DBOS.cancel_workflow(merged.get_workflow_id())

    # An explicit option still wins over the ambient one.
    with SetEnqueueOptions(app_version="ambient-version"):
        explicit: WorkflowHandle[int] = DBOS.enqueue_workflow_with_options(
            {
                "workflow_name": "with_options_ctx_target",
                "queue_name": "with_options_ctx_queue",
                "app_version": GlobalParams.app_version,
            },
            2,
        )
    assert explicit.get_result() == 2
