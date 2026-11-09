import asyncio
import threading
import time
import uuid
from typing import Any, List, Tuple

import pytest
import sqlalchemy as sa
from pytest_mock import MockerFixture

from dbos import DBOS, SetWorkflowID
from dbos._error import DBOSAwaitedWorkflowCancelledError
from dbos._schemas.system_database import SystemSchema
from dbos._sys_db import WorkflowStatusString, _dbos_workflow_results_channel

# A waiter that only polled would sleep this long before its second check, so
# finishing well inside _PROMPT shows the completion woke it.
_LONG_POLL = 10.0
_PROMPT = 5.0


def _gated_workflow(count: int) -> Tuple[List[threading.Event], Any]:
    gates = [threading.Event() for _ in range(count)]

    @DBOS.workflow()
    def gated(i: int) -> int:
        gates[i].wait()
        return i

    return gates, gated


def _open(gates: List[threading.Event]) -> None:
    for gate in gates:
        gate.set()


def test_get_result_wakes_on_completion(dbos: DBOS) -> None:
    DBOS.register_queue("wakeup_queue", polling_interval_sec=0.01)
    gates, gated = _gated_workflow(1)
    try:
        handle = DBOS.enqueue_workflow("wakeup_queue", gated, 0)
        threading.Timer(0.5, gates[0].set).start()
        start = time.monotonic()
        assert handle.get_result(polling_interval_sec=_LONG_POLL) == 0
        assert time.monotonic() - start < _PROMPT
        # The waiter unregistered itself.
        assert dbos._sys_db.workflow_results_map.snapshot() == []
    finally:
        _open(gates)


def test_get_result_inside_workflow_wakes_on_completion(dbos: DBOS) -> None:
    DBOS.register_queue("wakeup_queue", polling_interval_sec=0.01)
    gates, gated = _gated_workflow(1)

    @DBOS.workflow()
    def parent() -> float:
        handle = DBOS.enqueue_workflow("wakeup_queue", gated, 0)
        start = time.monotonic()
        assert handle.get_result(polling_interval_sec=_LONG_POLL) == 0
        return time.monotonic() - start

    try:
        threading.Timer(0.5, gates[0].set).start()
        assert parent() < _PROMPT
    finally:
        _open(gates)


@pytest.mark.asyncio
async def test_get_result_async_wakes_on_completion(dbos: DBOS) -> None:
    await DBOS.register_queue_async("wakeup_queue", polling_interval_sec=0.01)
    gates, gated = _gated_workflow(1)
    try:
        handle = await DBOS.enqueue_workflow_async("wakeup_queue", gated, 0)
        asyncio.get_running_loop().call_later(0.5, gates[0].set)
        start = time.monotonic()
        assert await handle.get_result(polling_interval_sec=_LONG_POLL) == 0
        assert time.monotonic() - start < _PROMPT
        assert dbos._sys_db.workflow_results_map.snapshot() == []
    finally:
        _open(gates)


def test_wait_first_wakes_on_completion(dbos: DBOS) -> None:
    DBOS.register_queue("wakeup_queue", polling_interval_sec=0.01)
    gates, gated = _gated_workflow(3)
    try:
        handles = [DBOS.enqueue_workflow("wakeup_queue", gated, i) for i in range(3)]
        threading.Timer(0.5, gates[1].set).start()
        start = time.monotonic()
        first = DBOS.wait_first(handles, polling_interval_sec=_LONG_POLL)
        assert time.monotonic() - start < _PROMPT
        assert first.get_workflow_id() == handles[1].get_workflow_id()
        assert dbos._sys_db.workflow_results_map.snapshot() == []
    finally:
        _open(gates)
    for handle in handles:
        handle.get_result()


@pytest.mark.asyncio
async def test_wait_first_async_wakes_on_completion(dbos: DBOS) -> None:
    await DBOS.register_queue_async("wakeup_queue", polling_interval_sec=0.01)
    gates, gated = _gated_workflow(2)
    try:
        handles = [
            await DBOS.enqueue_workflow_async("wakeup_queue", gated, i)
            for i in range(2)
        ]
        asyncio.get_running_loop().call_later(0.5, gates[0].set)
        start = time.monotonic()
        first = await DBOS.wait_first_async(handles, polling_interval_sec=_LONG_POLL)
        assert time.monotonic() - start < _PROMPT
        assert first.get_workflow_id() == handles[0].get_workflow_id()
    finally:
        _open(gates)
    for handle in handles:
        await handle.get_result()


def test_cancel_wakes_get_result(dbos: DBOS) -> None:
    DBOS.register_queue("wakeup_queue", polling_interval_sec=0.01)
    gates, gated = _gated_workflow(1)
    try:
        handle = DBOS.enqueue_workflow("wakeup_queue", gated, 0)
        threading.Timer(
            0.5, DBOS.cancel_workflow, args=[handle.get_workflow_id()]
        ).start()
        start = time.monotonic()
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            handle.get_result(polling_interval_sec=_LONG_POLL)
        assert time.monotonic() - start < _PROMPT
    finally:
        _open(gates)


def test_completion_emits_cross_process_hint(dbos: DBOS, mocker: MockerFixture) -> None:
    @DBOS.workflow()
    def quick() -> str:
        return "done"

    @DBOS.workflow()
    def failing() -> None:
        raise ValueError("boom")

    spy = mocker.spy(dbos._sys_db, "_signal_notification")
    ok_id, err_id = str(uuid.uuid4()), str(uuid.uuid4())
    with SetWorkflowID(ok_id):
        assert quick() == "done"
    with SetWorkflowID(err_id):
        with pytest.raises(ValueError):
            failing()
    hinted = [
        c.args[1]
        for c in spy.call_args_list
        if c.args[0] == _dbos_workflow_results_channel
    ]
    assert ok_id in hinted and err_id in hinted


def test_get_result_falls_back_to_polling(dbos: DBOS, mocker: MockerFixture) -> None:
    """A completion no signal reaches (e.g. written by another process without
    LISTEN/NOTIFY) is still noticed within the polling interval."""
    DBOS.register_queue("wakeup_queue", polling_interval_sec=0.01)
    gates, gated = _gated_workflow(1)
    try:
        handle = DBOS.enqueue_workflow("wakeup_queue", gated, 0)
        deadline = time.monotonic() + _PROMPT
        while handle.get_status().status != WorkflowStatusString.PENDING.value:
            assert time.monotonic() < deadline
            time.sleep(0.05)
        # Finish the workflow behind DBOS's back, so no signal is sent.
        mocker.patch.object(dbos._sys_db, "_signal_workflow_results")
        with dbos._sys_db.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .where(
                    SystemSchema.workflow_status.c.workflow_uuid
                    == handle.get_workflow_id()
                )
                .values(status=WorkflowStatusString.CANCELLED.value)
            )
        start = time.monotonic()
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            handle.get_result(polling_interval_sec=0.2)
        assert time.monotonic() - start < _PROMPT
    finally:
        _open(gates)
