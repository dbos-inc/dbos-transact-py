import asyncio
import sys
import threading
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import Any, Optional

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

from dbos import DBOS, DBOSConfig, SetWorkflowID, WorkflowHandleAsync
from dbos._debug_trigger import DebugAction, DebugTriggers
from dbos._error import DBOSWorkflowConflictIDError
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import serialize_value_as
from dbos._sys_db import OperationResultInternal, WorkflowStatusString
from tests.conftest import (
    reexecute_workflow_by_id,
    retry_until_success_async,
    set_workflow_status,
)


@pytest.mark.asyncio
async def test_simple_workflow(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class TryConcExec:
        conc_exec = 0
        max_conc = 0

        conc_wf = 0
        max_wf = 0

        @staticmethod
        @DBOS.step()
        async def testConcStep() -> None:
            TryConcExec.conc_exec += 1
            TryConcExec.max_conc = max(TryConcExec.conc_exec, TryConcExec.max_conc)
            await asyncio.sleep(1)
            TryConcExec.conc_exec -= 1

        @staticmethod
        @DBOS.workflow()
        async def testConcWorkflow() -> None:
            TryConcExec.conc_wf += 1
            TryConcExec.max_wf = max(TryConcExec.conc_wf, TryConcExec.max_wf)
            await asyncio.sleep(0.5)
            await TryConcExec.testConcStep()
            await asyncio.sleep(0.5)
            TryConcExec.conc_wf -= 1

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        wfh1 = await DBOS.start_workflow_async(TryConcExec.testConcWorkflow)
    with SetWorkflowID(wfid):
        wfh2 = await DBOS.start_workflow_async(TryConcExec.testConcWorkflow)

    await wfh1.get_result()
    await wfh2.get_result()
    assert TryConcExec.max_conc == 1
    assert TryConcExec.max_wf == 1

    # Direct exec part
    wfid2 = str(uuid.uuid4())
    with SetWorkflowID(wfid2):
        cr1 = TryConcExec.testConcWorkflow()
    with SetWorkflowID(wfid2):
        cr2 = TryConcExec.testConcWorkflow()
    await cr1
    await cr2

    assert TryConcExec.max_conc == 1
    assert TryConcExec.max_wf == 1

    # Test workflow recovery
    def recover_in_thread() -> None:
        set_workflow_status(dbos._sys_db, wfid, "PENDING")
        for handle in DBOS._recover_pending_workflows():
            handle.get_result()
        # Two dequeue dispatches of one ID race: only the active-workflow guard stops a double run.
        wfh1r = reexecute_workflow_by_id(dbos, wfid)
        wfh2r = reexecute_workflow_by_id(dbos, wfid)
        wfh1r.get_result()
        wfh2r.get_result()

    await asyncio.to_thread(recover_in_thread)

    assert TryConcExec.max_conc == 1
    assert TryConcExec.max_wf == 1


@pytest.mark.asyncio
async def test_step_undoredo(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class CatchPlainException1:
        execNum = 0
        started = False
        completed = False
        aborted = False
        trouble = False

        @staticmethod
        @DBOS.step()
        async def testStartAction() -> None:
            await asyncio.sleep(1)
            CatchPlainException1.started = True

        @staticmethod
        @DBOS.step()
        async def testCompleteAction() -> None:
            assert CatchPlainException1.started
            await asyncio.sleep(1)
            CatchPlainException1.completed = True

        @staticmethod
        @DBOS.step()
        async def testCancelAction() -> None:
            CatchPlainException1.aborted = True
            CatchPlainException1.started = False

        @staticmethod
        async def reportTrouble() -> None:
            CatchPlainException1.trouble = True
            assert str("Trouble?") == "None!"

        @staticmethod
        @DBOS.workflow()
        async def testConcWorkflow() -> None:
            try:
                # Step 1, tell external system to start processing
                await CatchPlainException1.testStartAction()
            except Exception:
                # If we fail for any reason, try to abort
                try:
                    await CatchPlainException1.testCancelAction()
                except Exception:
                    # Take some other notification action (sysadmin!)
                    await CatchPlainException1.reportTrouble()

            # Step 2, finish the process
            await CatchPlainException1.testCompleteAction()

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        wfh1 = await DBOS.start_workflow_async(CatchPlainException1.testConcWorkflow)
    with SetWorkflowID(wfid):
        wfh2 = await DBOS.start_workflow_async(CatchPlainException1.testConcWorkflow)

    await wfh1.get_result()
    await wfh2.get_result()

    print(
        f"Started: {CatchPlainException1.started}; "
        f"Completed: {CatchPlainException1.completed}; "
        f"Aborted: {CatchPlainException1.aborted}; "
        f"Trouble: {CatchPlainException1.trouble}"
    )
    assert CatchPlainException1.started
    assert CatchPlainException1.completed
    assert not CatchPlainException1.trouble


@pytest.mark.asyncio
async def test_step_undoredo2(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class UsingFinallyClause:
        execNum = 0
        started = False
        completed = False
        aborted = False
        trouble = False

        @staticmethod
        @DBOS.step()
        async def testStartAction() -> None:
            await asyncio.sleep(1)
            UsingFinallyClause.started = True

        @staticmethod
        @DBOS.step()
        async def testCompleteAction() -> None:
            assert UsingFinallyClause.started
            await asyncio.sleep(1)
            UsingFinallyClause.completed = True

        @staticmethod
        @DBOS.step()
        async def testCancelAction() -> None:
            UsingFinallyClause.aborted = True
            UsingFinallyClause.started = False

        @staticmethod
        async def reportTrouble() -> None:
            UsingFinallyClause.trouble = True
            assert str("Trouble?") == "None!"

        @staticmethod
        @DBOS.workflow()
        async def testConcWorkflow() -> None:
            finished = False
            try:
                # Step 1, tell external system to start processing
                await UsingFinallyClause.testStartAction()

                # Step 2, finish the process
                await UsingFinallyClause.testCompleteAction()

                finished = True
            finally:
                if not finished:
                    # If we fail for any reason, try to abort
                    try:
                        await UsingFinallyClause.testCancelAction()
                    except Exception:
                        await UsingFinallyClause.reportTrouble()

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        wfh1 = await DBOS.start_workflow_async(UsingFinallyClause.testConcWorkflow)
    with SetWorkflowID(wfid):
        wfh2 = await DBOS.start_workflow_async(UsingFinallyClause.testConcWorkflow)

    await wfh1.get_result()
    await wfh2.get_result()

    print(
        f"Started: {UsingFinallyClause.started}; "
        f"Completed: {UsingFinallyClause.completed}; "
        f"Aborted: {UsingFinallyClause.aborted}; "
        f"Trouble: {UsingFinallyClause.trouble}"
    )
    assert UsingFinallyClause.started
    assert UsingFinallyClause.completed
    assert not UsingFinallyClause.trouble


@pytest.mark.asyncio
async def test_step_sequence(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class TryConcExec2:
        curExec = 0
        curStep = 0

        @staticmethod
        @DBOS.step()
        async def step1() -> None:
            # This makes the step take a while ... sometimes.
            if TryConcExec2.curExec % 2 == 0:
                TryConcExec2.curExec += 1
                await asyncio.sleep(1)
            TryConcExec2.curStep = 1

        @staticmethod
        @DBOS.step()
        async def step2() -> None:
            TryConcExec2.curStep = 2

        @staticmethod
        @DBOS.workflow()
        async def testConcWorkflow() -> None:
            await TryConcExec2.step1()
            await TryConcExec2.step2()

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        wfh1 = await DBOS.start_workflow_async(TryConcExec2.testConcWorkflow)
    with SetWorkflowID(wfid):
        wfh2 = await DBOS.start_workflow_async(TryConcExec2.testConcWorkflow)

    await wfh1.get_result()
    await wfh2.get_result()
    assert TryConcExec2.curStep == 2


@pytest.mark.asyncio
async def test_commit_hiccup(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class TryDbGlitch:
        @staticmethod
        @DBOS.step()
        async def step1() -> str:
            await asyncio.sleep(1)
            return "Yay!"

        @staticmethod
        @DBOS.workflow()
        async def testWorkflow() -> str:
            res = await TryDbGlitch.step1()
            return res + ""

    assert await TryDbGlitch.testWorkflow() == "Yay!"

    DebugTriggers.set_debug_trigger(
        DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT,
        DebugAction().set_exception_to_throw(
            OperationalError(
                statement=None,
                params=None,
                orig=BaseException("Connection lost"),
                connection_invalidated=True,
            )
        ),
    )

    assert await TryDbGlitch.testWorkflow() == "Yay!"

    DebugTriggers.set_debug_trigger(
        DebugTriggers.DEBUG_TRIGGER_INITWF_COMMIT,
        DebugAction().set_exception_to_throw(
            OperationalError(
                statement=None,
                params=None,
                orig=BaseException("Connection lost"),
                connection_invalidated=True,
            )
        ),
    )

    assert await TryDbGlitch.testWorkflow() == "Yay!"


@pytest.mark.asyncio
async def test_parked_duplicate_does_not_hold_a_thread(
    dbos: DBOS, config: DBOSConfig
) -> None:
    """A duplicate async execution that loses the checkpoint race parks on the owning
    execution's outcome. That wait must happen on the event loop: the park runs inside
    asyncio.to_thread, so a blocking poll there pins one of DBOS's executor threads for
    as long as the owner takes to finish, and enough parked duplicates leave no thread
    for any other async work in the process."""
    workers = 2
    config["max_executor_threads"] = workers
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()

    # The IDs whose step checkpoint loses the race to a concurrent execution.
    lost_ids: set[str] = set()
    parked_ids: set[str] = set()

    original_record = dbos._sys_db.record_operation_result

    def losing_record(
        result: OperationResultInternal,
        *,
        completed_at_epoch_ms: Optional[int] = None,
    ) -> None:
        # What the loser of a checkpoint race sees: the owner's row is already there.
        if result["workflow_uuid"] in lost_ids:
            raise DBOSWorkflowConflictIDError(result["workflow_uuid"])
        original_record(result, completed_at_epoch_ms=completed_at_epoch_ms)

    original_check = dbos._sys_db.check_workflow_result

    def tracking_check(workflow_id: str, *, fail_if_missing: bool = False) -> Any:
        # Every park polls this, blocking or not: a check is proof the run reached it.
        if workflow_id in lost_ids:
            parked_ids.add(workflow_id)
        return original_check(workflow_id, fail_if_missing=fail_if_missing)

    dbos._sys_db.record_operation_result = losing_record  # type: ignore[method-assign]
    dbos._sys_db.check_workflow_result = tracking_check  # type: ignore[method-assign]

    def blocked_executor_threads() -> list[str]:
        """Executor threads sitting inside the blocking park, for the failure message."""
        frames = sys._current_frames()
        return [
            thread.name
            for thread in threading.enumerate()
            if thread.name.startswith("dbos-executor-")
            and thread.ident in frames
            and any(
                frame.name == "await_workflow_result"
                for frame in traceback.extract_stack(frames[thread.ident])
            )
        ]

    @DBOS.step()
    async def duplicated_step() -> str:
        return "step output"

    @DBOS.workflow()
    async def duplicate_workflow() -> str:
        return await duplicated_step()

    @DBOS.workflow()
    async def unrelated_workflow() -> str:
        return "unblocked"

    handles: list[WorkflowHandleAsync[str]] = []
    try:
        for _ in range(workers):
            wfid = str(uuid.uuid4())
            lost_ids.add(wfid)
            with SetWorkflowID(wfid):
                handles.append(await DBOS.start_workflow_async(duplicate_workflow))

        def all_parked() -> None:
            assert parked_ids == lost_ids, f"parked so far: {parked_ids}"

        await retry_until_success_async(all_parked, interval=0.1, max_attempts=300)

        # Every duplicate now waits for an outcome only the owning execution can write.
        # Unrelated async work must still be able to run.
        try:
            assert (
                await asyncio.wait_for(unrelated_workflow(), timeout=20) == "unblocked"
            )
        except asyncio.TimeoutError:
            pytest.fail(
                f"parked duplicates hold every executor thread: {blocked_executor_threads()}"
            )
    finally:
        # Publish the outcome the parked duplicates wait for, as the owner would.
        # In a finally: a failed assertion must not strand them polling forever.
        serval, _ = serialize_value_as("owner outcome", None, dbos._serializer)
        with dbos._sys_db.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .values(status=WorkflowStatusString.SUCCESS.value, output=serval)
                .where(SystemSchema.workflow_status.c.workflow_uuid.in_(lost_ids))
            )

    for handle in handles:
        assert (
            await asyncio.wait_for(handle.get_result(), timeout=30) == "owner outcome"
        )
