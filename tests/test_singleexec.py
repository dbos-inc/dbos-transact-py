import uuid
from concurrent.futures import ThreadPoolExecutor, wait
from time import sleep
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import OperationalError

from dbos import DBOS, SetWorkflowID
from dbos._debug_trigger import DebugAction, DebugTriggers

if TYPE_CHECKING:
    from dbos._dbos import WorkflowHandle


def reexecute_workflow_by_id(dbos: DBOS, wfid: str) -> "WorkflowHandle[Any]":
    dbos._sys_db.update_workflow_outcome(wfid, "PENDING")
    return dbos._execute_workflow_id(wfid)


def test_simple_workflow(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class TryConcExec:
        conc_exec = 0
        max_conc = 0

        conc_wf = 0
        max_wf = 0

        @DBOS.step()
        @staticmethod
        def testConcStep() -> None:
            TryConcExec.conc_exec += 1
            TryConcExec.max_conc = max(TryConcExec.conc_exec, TryConcExec.max_conc)
            sleep(1)
            TryConcExec.conc_exec -= 1

        @DBOS.workflow()
        @staticmethod
        def testConcWorkflow() -> None:
            TryConcExec.conc_wf += 1
            TryConcExec.max_wf = max(TryConcExec.conc_wf, TryConcExec.max_wf)
            sleep(0.5)
            TryConcExec.testConcStep()
            sleep(0.5)
            TryConcExec.conc_wf -= 1

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        wfh1 = DBOS.start_workflow(TryConcExec.testConcWorkflow)
    with SetWorkflowID(wfid):
        wfh2 = DBOS.start_workflow(TryConcExec.testConcWorkflow)

    wfh1.get_result()
    wfh2.get_result()
    assert TryConcExec.max_conc == 1
    assert TryConcExec.max_wf == 1

    # Recovery part
    wfh1r: WorkflowHandle[str] = reexecute_workflow_by_id(dbos, wfid)
    wfh2r: WorkflowHandle[str] = reexecute_workflow_by_id(dbos, wfid)
    wfh1r.get_result()
    wfh2r.get_result()

    assert TryConcExec.max_conc == 1
    assert TryConcExec.max_wf == 1

    # Direct exec part
    def run(wfid: str) -> None:
        with SetWorkflowID(wfid):
            TryConcExec.testConcWorkflow()

    wfid2 = str(uuid.uuid4())
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(run, wfid2),
            executor.submit(run, wfid2),
        ]
        wait(futures)

    assert TryConcExec.max_conc == 1
    assert TryConcExec.max_wf == 1


def test_step_undoredo(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class CatchPlainException1:
        execNum = 0
        started = False
        completed = False
        aborted = False
        trouble = False

        @DBOS.step()
        @staticmethod
        def testStartAction() -> None:
            sleep(1)
            CatchPlainException1.started = True

        @DBOS.step()
        @staticmethod
        def testCompleteAction() -> None:
            assert CatchPlainException1.started
            sleep(1)
            CatchPlainException1.completed = True

        @DBOS.step()
        @staticmethod
        def testCancelAction() -> None:
            CatchPlainException1.aborted = True
            CatchPlainException1.started = False

        @staticmethod
        def reportTrouble() -> None:
            CatchPlainException1.trouble = True
            assert str("Trouble?") == "None!"

        @DBOS.workflow()
        @staticmethod
        def testConcWorkflow() -> None:
            try:
                # Step 1, tell external system to start processing
                CatchPlainException1.testStartAction()
            except Exception:
                # If we fail for any reason, try to abort
                try:
                    CatchPlainException1.testCancelAction()
                except Exception:
                    # Take some other notification action (sysadmin!)
                    CatchPlainException1.reportTrouble()

            # Step 2, finish the process
            CatchPlainException1.testCompleteAction()

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        wfh1 = DBOS.start_workflow(CatchPlainException1.testConcWorkflow)
    with SetWorkflowID(wfid):
        wfh2 = DBOS.start_workflow(CatchPlainException1.testConcWorkflow)

    wfh1.get_result()
    wfh2.get_result()

    print(
        f"Started: {CatchPlainException1.started}; "
        f"Completed: {CatchPlainException1.completed}; "
        f"Aborted: {CatchPlainException1.aborted}; "
        f"Trouble: {CatchPlainException1.trouble}"
    )
    assert CatchPlainException1.started
    assert CatchPlainException1.completed
    assert not CatchPlainException1.trouble


def test_step_undoredo2(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class UsingFinallyClause:
        execNum = 0
        started = False
        completed = False
        aborted = False
        trouble = False

        @DBOS.step()
        @staticmethod
        def testStartAction() -> None:
            sleep(1)
            UsingFinallyClause.started = True

        @DBOS.step()
        @staticmethod
        def testCompleteAction() -> None:
            assert UsingFinallyClause.started
            sleep(1)
            UsingFinallyClause.completed = True

        @DBOS.step()
        @staticmethod
        def testCancelAction() -> None:
            UsingFinallyClause.aborted = True
            UsingFinallyClause.started = False

        @staticmethod
        def reportTrouble() -> None:
            UsingFinallyClause.trouble = True
            assert str("Trouble?") == "None!"

        @DBOS.workflow()
        @staticmethod
        def testConcWorkflow() -> None:
            finished = False
            try:
                # Step 1, tell external system to start processing
                UsingFinallyClause.testStartAction()

                # Step 2, finish the process
                UsingFinallyClause.testCompleteAction()

                finished = True
            finally:
                if not finished:
                    # If we fail for any reason, try to abort
                    try:
                        UsingFinallyClause.testCancelAction()
                    except Exception:
                        UsingFinallyClause.reportTrouble()

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        wfh1 = DBOS.start_workflow(UsingFinallyClause.testConcWorkflow)
    with SetWorkflowID(wfid):
        wfh2 = DBOS.start_workflow(UsingFinallyClause.testConcWorkflow)

    wfh1.get_result()
    wfh2.get_result()

    print(
        f"Started: {UsingFinallyClause.started}; "
        f"Completed: {UsingFinallyClause.completed}; "
        f"Aborted: {UsingFinallyClause.aborted}; "
        f"Trouble: {UsingFinallyClause.trouble}"
    )
    assert UsingFinallyClause.started
    assert UsingFinallyClause.completed
    assert not UsingFinallyClause.trouble


def test_step_sequence(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class TryConcExec2:
        curExec = 0
        curStep = 0

        @DBOS.step()
        @staticmethod
        def step1() -> None:
            # This makes the step take a while ... sometimes.
            if TryConcExec2.curExec % 2 == 0:
                TryConcExec2.curExec += 1
                sleep(1)
            TryConcExec2.curStep = 1

        @DBOS.step()
        @staticmethod
        def step2() -> None:
            TryConcExec2.curStep = 2

        @DBOS.workflow()
        @staticmethod
        def testConcWorkflow() -> None:
            TryConcExec2.step1()
            TryConcExec2.step2()

    wfid = str(uuid.uuid4())

    with SetWorkflowID(wfid):
        wfh1 = DBOS.start_workflow(TryConcExec2.testConcWorkflow)
    with SetWorkflowID(wfid):
        wfh2 = DBOS.start_workflow(TryConcExec2.testConcWorkflow)

    wfh1.get_result()
    wfh2.get_result()
    assert TryConcExec2.curStep == 2


def test_commit_hiccup(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class TryDbGlitch:
        @DBOS.step()
        @staticmethod
        def step1() -> str:
            sleep(1)
            return "Yay!"

        @DBOS.workflow()
        @staticmethod
        def testWorkflow() -> str:
            return TryDbGlitch.step1()

    assert TryDbGlitch.testWorkflow() == "Yay!"
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

    assert TryDbGlitch.testWorkflow() == "Yay!"
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
    assert TryDbGlitch.testWorkflow() == "Yay!"


def test_status_wf(dbos: DBOS) -> None:
    """Test use of name `status`."""

    @DBOS.step()
    def stepf(s: str) -> None:
        print(s)

    @DBOS.workflow()
    def status_workflow(status: str = "None") -> None:
        stepf(status)

    status_workflow()
    status_workflow(status="Starting")
    DBOS.start_workflow(status_workflow).get_result()
    DBOS.start_workflow(status_workflow, status="Ending").get_result()
