import asyncio
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Coroutine,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

import pytest
from sqlalchemy import text

# Public API
from dbos import DBOS, SetWorkflowID, StepInfo
from dbos._error import DBOSNonExistentWorkflowError
from tests.conftest import using_sqlite


def test_concurrent_workflows(dbos: DBOS) -> None:
    @DBOS.workflow()
    def test_workflow() -> str:
        time.sleep(1)
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    def test_thread(id: str) -> str:
        with SetWorkflowID(id):
            return test_workflow()

    num_threads = 10
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures: list[Tuple[str, Future[str]]] = []
        for _ in range(num_threads):
            id = str(uuid.uuid4())
            futures.append((id, executor.submit(test_thread, id)))
        for id, future in futures:
            assert id == future.result()


def test_concurrent_getevent(dbos: DBOS) -> None:
    @DBOS.workflow()
    def test_workflow(event_name: str, value: str) -> str:
        DBOS.set_event(event_name, value)
        return value

    def test_thread(id: str, event_name: str) -> str:
        return cast(str, DBOS.get_event(id, event_name, 5))

    wfuuid = str(uuid.uuid4())
    event_name = "test_event"
    with ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(test_thread, wfuuid, event_name)
        future2 = executor.submit(test_thread, wfuuid, event_name)

        expected_message = "test message"
        with SetWorkflowID(wfuuid):
            test_workflow(event_name, expected_message)

        # Both should return the same message (shared handle, NOT concurrent)
        assert future1.result() == future2.result()
        assert future1.result() == expected_message
        # Make sure the event map is empty
        assert not dbos._sys_db.workflow_events_map._dict


# Async concurrency tests


@dataclass(frozen=True)
class Thing:
    func: Callable[[], Coroutine[Any, Any, str]]
    expected: str


async def run_things_serial_or_conc(conc: bool, things: List[Thing]) -> None:
    if conc:
        # `asyncio.gather` without return_exceptions is harmful (like TS Promise.all or race)
        #    return_exceptions, which will wait for all, capture exceptions as values, and is OK.
        tasks: List[asyncio.Task[str]] = [asyncio.create_task(t.func()) for t in things]

        results: List[Union[str, BaseException]] = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        for i, (thing, result) in enumerate(zip(things, results)):
            if isinstance(result, BaseException):
                # Make failures loud
                raise AssertionError(
                    f"Thing[{i}] raised {type(result).__name__}: {result}"
                ) from result
            assert (
                result == thing.expected
            ), f"Thing[{i}] expected {thing.expected!r}, got {result!r}"

    else:
        for i, thing in enumerate(things):
            result = await thing.func()
            assert (
                result == thing.expected
            ), f"Thing[{i}] expected {thing.expected!r}, got {result!r}"


def compare_wf_runs(
    wfsteps_serial: List[StepInfo], wfsteps_concurrent: List[StepInfo]
) -> None:
    iconc = 0
    i = 0
    while i < len(wfsteps_serial):
        s = wfsteps_serial[i]
        c = wfsteps_concurrent[iconc]

        print(
            f"Output of {c['function_name']}@{c['function_id']}: {c['output']!r} "
            f"vs. {s['function_name']}@{s['function_id']}: {s['output']!r}"
        )

        # Allow extra DBOS.sleep in concurrent run that may interleave
        if c["function_id"] < s["function_id"] and c["function_name"] == "DBOS.sleep":
            iconc += 1
            continue

        assert c["function_id"] == s["function_id"]
        assert c["function_name"] == s["function_name"]
        assert c["error"] == s["error"]

        if c["function_name"] in {"DBOS.now", "DBOS.randomUUID", "DBOS.sleep"}:
            # output may differ
            pass
        else:
            assert c["output"] == s["output"]

        i += 1
        iconc += 1


@pytest.mark.asyncio
async def test_gather_manythings(dbos: DBOS) -> None:
    @DBOS.workflow()
    async def simple_wf() -> str:
        return "WF Ran"

    @DBOS.step()
    async def simple_step() -> str:
        return "Step Ran"

    @DBOS.dbos_class()
    class ConcurrTestClass:
        cnt: ClassVar[int] = 0

        @staticmethod
        @DBOS.step()
        async def test_read_write_function(id: int) -> int:
            ConcurrTestClass.cnt += 1
            return id

        @staticmethod
        @DBOS.step()
        async def test_step(id: int) -> int:
            ConcurrTestClass.cnt += 1
            return id

        @staticmethod
        @DBOS.step()
        async def test_step_str(id: str) -> str:
            return id

        @staticmethod
        @DBOS.step(
            retries_allowed=True, max_attempts=5, interval_seconds=0.01, backoff_rate=1
        )
        async def test_step_retry(id: str) -> str:
            ss = DBOS.step_status
            assert ss is not None
            assert ss.current_attempt is not None
            if ss.current_attempt <= 2:
                raise Exception("Not yet")
            return id

        @staticmethod
        @DBOS.workflow()
        async def receive_workflow(topic: str, timeout: int) -> Any:
            return await DBOS.recv_async(topic, timeout)

    @DBOS.workflow()
    async def run_a_lot_of_things_at_once(conc: bool) -> None:
        async def t_sleep() -> str:
            await DBOS.sleep_async(0.5)
            return "slept"

        async def t_run_step1() -> str:
            return await DBOS.run_step_async({"name": "runStep1"}, lambda: "ranStep")

        async def t_run_step2() -> str:
            return await DBOS.run_step_async({"name": "runStep2"}, lambda: "ranStep")

        async def t_run_step_retry() -> str:
            def body() -> str:
                ss = DBOS.step_status
                assert ss is not None
                assert ss.current_attempt is not None
                if ss.current_attempt <= 2:
                    raise Exception("Not yet")
                return "ranStep"

            return await DBOS.run_step_async(
                {
                    "name": "runStepRetry",
                    "retries_allowed": True,
                    "max_attempts": 5,
                    "interval_seconds": 0.01,
                    "backoff_rate": 1,
                },
                body,
            )

        async def t_function_calls_function() -> str:
            return str(await ConcurrTestClass.test_read_write_function(2))

        async def t_set_event() -> str:
            await DBOS.set_event_async("eventkey", "eval")
            return "set"

        async def t_get_event() -> str:
            wfid = DBOS.workflow_id
            assert wfid is not None
            v = await DBOS.get_event_async(wfid, "eventkey")
            return v if v is not None else "Nope"

        async def t_send_msg() -> str:
            wfid = DBOS.workflow_id
            assert wfid is not None
            await DBOS.send_async(wfid, "msg", "topic")
            return "sent"

        async def t_step_str_3() -> str:
            return await ConcurrTestClass.test_step_str("3")

        async def t_recv_msg() -> str:
            v = await DBOS.recv_async("topic")
            return v if v is not None else "Nope"

        async def t_get_workflow_status_nosuch() -> str:
            st = await DBOS.get_workflow_status_async("nosuchwv")
            return st.status if st is not None else "Nope"

        async def t_retrieve_workflow_nosuch() -> str:
            with pytest.raises(DBOSNonExistentWorkflowError):
                await DBOS.retrieve_workflow_async("nosuchwv", existing_workflow=True)
                return "Yep"
            return "Nope"

        async def t_liststeps() -> str:
            steps = await DBOS.list_workflow_steps_async("nosuchwv")
            return f"{len(steps)}"

        async def t_listwfs() -> str:
            wfs = await DBOS.list_workflows_async(workflow_id_prefix="aaaa")
            return f"{len(wfs)}"

        async def t_listqwfs() -> str:
            wfs = await DBOS.list_queued_workflows_async(workflow_id_prefix="aaaa")
            return f"{len(wfs)}"

        async def t_step_retry_4() -> str:
            return await ConcurrTestClass.test_step_retry("4")

        async def t_start_child() -> str:
            with SetWorkflowID(f"{DBOS.workflow_id}-cwf"):
                await DBOS.start_workflow_async(simple_wf)
            return "started"

        async def t_get_child_result() -> str:
            res = await DBOS.get_result_async(f"{DBOS.workflow_id}-cwf")
            assert res is not None
            return cast(str, res)

        async def t_write_stream() -> str:
            await DBOS.write_stream_async("stream", "val")
            return "wrote"

        async def t_read_stream() -> str:
            wfid = DBOS.workflow_id
            assert wfid is not None
            ait = DBOS.read_stream_async(wfid, "stream")
            item = await ait.__anext__()
            return cast(str, item)

        things: List[Thing] = [
            Thing(func=t_sleep, expected="slept"),
            Thing(func=t_run_step1, expected="ranStep"),
            Thing(func=t_run_step2, expected="ranStep"),
            Thing(func=t_listqwfs, expected="0"),
            Thing(func=t_run_step_retry, expected="ranStep"),
            Thing(func=t_function_calls_function, expected="2"),
            Thing(func=t_set_event, expected="set"),
            Thing(func=t_get_event, expected="eval"),
            Thing(func=t_send_msg, expected="sent"),
            Thing(func=t_step_str_3, expected="3"),
            Thing(func=t_recv_msg, expected="msg"),
            Thing(func=t_get_workflow_status_nosuch, expected="Nope"),
            Thing(func=t_listwfs, expected="0"),
            Thing(func=t_retrieve_workflow_nosuch, expected="Nope"),
            Thing(func=t_step_retry_4, expected="4"),
            Thing(func=simple_wf, expected="WF Ran"),
            Thing(func=t_start_child, expected="started"),
            Thing(func=t_get_child_result, expected="WF Ran"),
            Thing(func=simple_step, expected="Step Ran"),
            Thing(func=t_write_stream, expected="wrote"),
            Thing(func=t_read_stream, expected="val"),
            Thing(func=t_liststeps, expected="0"),
        ]

        await run_things_serial_or_conc(conc, things)

    wfid_serial = str(uuid.uuid4())
    wfid_concurrent = str(uuid.uuid4())

    with SetWorkflowID(wfid_serial):
        await run_a_lot_of_things_at_once(conc=False)  # from earlier translation

    with SetWorkflowID(wfid_concurrent):
        await run_a_lot_of_things_at_once(conc=True)

    wfsteps_serial = await DBOS.list_workflow_steps_async(wfid_serial)
    wfsteps_concurrent = await DBOS.list_workflow_steps_async(wfid_concurrent)

    assert wfsteps_serial is not None
    assert wfsteps_concurrent is not None
    compare_wf_runs(wfsteps_serial, wfsteps_concurrent)

    await run_a_lot_of_things_at_once(conc=True)


@pytest.mark.asyncio
async def test_gather_manysteps(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class ConcurrTestClass:
        @staticmethod
        @DBOS.step()
        async def test_step_str(id: str) -> str:
            return id

        @staticmethod
        @DBOS.step(
            retries_allowed=True, max_attempts=5, interval_seconds=0.01, backoff_rate=1
        )
        async def test_step_retry(id: str) -> str:
            ss = DBOS.step_status
            assert ss is not None
            assert ss.current_attempt is not None
            if ss.current_attempt <= 2:
                raise Exception("Not yet")
            return id

    @DBOS.workflow()
    async def run_a_lot_of_steps_at_once(conc: bool) -> None:
        async def t_run_step1a() -> str:
            return await DBOS.run_step_async({"name": "runStep1a"}, lambda: "ranStep")

        async def _retry_body() -> str:
            ss = DBOS.step_status
            assert ss is not None
            assert ss.current_attempt is not None
            if ss.current_attempt <= 2:
                raise Exception("Not yet")
            return "ranStep"

        async def t_run_step_retry1() -> str:
            return await DBOS.run_step_async(
                {
                    "name": "runStepRetry1",
                    "retries_allowed": True,
                    "max_attempts": 5,
                    "interval_seconds": 0.02,
                    "backoff_rate": 1,
                },
                _retry_body,
            )

        async def t_run_step_retry2() -> str:
            return await DBOS.run_step_async(
                {
                    "name": "runStepRetry2",
                    "retries_allowed": True,
                    "max_attempts": 5,
                    "interval_seconds": 0.01,
                    "backoff_rate": 1,
                },
                _retry_body,
            )

        async def t_run_step2() -> str:
            return await DBOS.run_step_async({"name": "runStep2"}, lambda: "ranStep")

        async def t_step_str_3() -> str:
            return await ConcurrTestClass.test_step_str("3")

        async def t_run_step_retry3() -> str:
            return await DBOS.run_step_async(
                {
                    "name": "runStepRetry3",
                    "retries_allowed": True,
                    "max_attempts": 5,
                    "interval_seconds": 0.03,
                    "backoff_rate": 1,
                },
                _retry_body,
            )

        async def t_step_retry_4() -> str:
            return await ConcurrTestClass.test_step_retry("4")

        things: List[Thing] = [
            Thing(func=t_run_step1a, expected="ranStep"),
            Thing(func=t_run_step_retry1, expected="ranStep"),
            Thing(func=t_run_step_retry2, expected="ranStep"),
            Thing(func=t_run_step2, expected="ranStep"),
            Thing(func=t_step_str_3, expected="3"),
            Thing(func=t_run_step_retry3, expected="ranStep"),
            Thing(func=t_step_retry_4, expected="4"),
            # TODO: Add some sync steps?
        ]

        await run_things_serial_or_conc(conc, things)

    wfid_serial = str(uuid.uuid4())
    wfid_concurrent = str(uuid.uuid4())

    with SetWorkflowID(wfid_serial):
        await run_a_lot_of_steps_at_once(conc=False)

    with SetWorkflowID(wfid_concurrent):
        await run_a_lot_of_steps_at_once(conc=True)

    wfsteps_serial = await DBOS.list_workflow_steps_async(wfid_serial)
    wfsteps_concurrent = await DBOS.list_workflow_steps_async(wfid_concurrent)

    assert wfsteps_serial is not None
    assert wfsteps_concurrent is not None
    compare_wf_runs(wfsteps_serial, wfsteps_concurrent)

    fwf1 = await DBOS.fork_workflow_async(wfid_concurrent, 3)
    await fwf1.get_result()

    fwf2 = await DBOS.fork_workflow_async(wfid_concurrent, 5)
    await fwf2.get_result()

    fwf3 = await DBOS.fork_workflow_async(wfid_concurrent, 7)
    await fwf3.get_result()
