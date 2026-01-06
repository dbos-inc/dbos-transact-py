import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import TYPE_CHECKING, Any

import pytest
from sqlalchemy.exc import OperationalError

from dbos import DBOS, SetWorkflowID
from dbos._debug_trigger import DebugAction, DebugTriggers

if TYPE_CHECKING:
    from dbos._dbos import WorkflowHandle


def reexecute_workflow_by_id(dbos: DBOS, wfid: str) -> "WorkflowHandle[Any]":
    dbos._sys_db.update_workflow_outcome(wfid, "PENDING")
    return dbos._execute_workflow_id(wfid)


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

    # Recovery part (TODO should it be async)
    wfh1r: WorkflowHandle[str] = reexecute_workflow_by_id(dbos, wfid)
    wfh2r: WorkflowHandle[str] = reexecute_workflow_by_id(dbos, wfid)
    wfh1r.get_result()
    wfh2r.get_result()

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
