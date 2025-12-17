import uuid
from time import sleep

from dbos import DBOS, SetWorkflowID


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
    # assert TryConcExec.max_conc == 1
    # assert TryConcExec.max_wf == 1

    # Recovery part
    """
    wfh1r = reexecute_workflow_by_id(wfid);
    wfh2r = reexecute_workflow_by_id(wfid);
    wfh1r.get_result();
    wfh2r.get_result();
    """
    # assert TryConcExec.max_conc == 1
    # assert TryConcExec.max_wf == 1


def test_step_undoredo(dbos: DBOS):
    @DBOS.dbos_class()
    class CatchPlainException1:
        execNum = 0
        started = False
        completed = False
        aborted = False
        trouble = False

        @DBOS.step()
        @staticmethod
        def testStartAction():
            sleep(1)
            CatchPlainException1.started = True

        @DBOS.step()
        @staticmethod
        def testCompleteAction():
            assert CatchPlainException1.started
            sleep(1)
            CatchPlainException1.completed = True

        @DBOS.step()
        @staticmethod
        def testCancelAction():
            CatchPlainException1.aborted = True
            CatchPlainException1.started = False

        @staticmethod
        def reportTrouble():
            CatchPlainException1.trouble = True
            assert "Trouble?" == "None!"

        @DBOS.workflow()
        @staticmethod
        def testConcWorkflow():
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
    # with SetWorkflowID(wfid):
    #    wfh2 = DBOS.start_workflow(CatchPlainException1.testConcWorkflow)

    wfh1.get_result()
    # wfh2.get_result()

    print(
        f"Started: {CatchPlainException1.started}; "
        f"Completed: {CatchPlainException1.completed}; "
        f"Aborted: {CatchPlainException1.aborted}; "
        f"Trouble: {CatchPlainException1.trouble}"
    )
    assert CatchPlainException1.started
    assert CatchPlainException1.completed
    assert not CatchPlainException1.trouble


def test_step_undoredo2(dbos: DBOS):
    @DBOS.dbos_class()
    class UsingFinallyClause:
        execNum = 0
        started = False
        completed = False
        aborted = False
        trouble = False

        @DBOS.step()
        @staticmethod
        def testStartAction():
            sleep(1)
            UsingFinallyClause.started = True

        @DBOS.step()
        @staticmethod
        def testCompleteAction():
            assert UsingFinallyClause.started
            sleep(1)
            UsingFinallyClause.completed = True

        @DBOS.step()
        @staticmethod
        def testCancelAction():
            UsingFinallyClause.aborted = True
            UsingFinallyClause.started = False

        @staticmethod
        def reportTrouble():
            UsingFinallyClause.trouble = True
            assert "Trouble?" == "None!"

        @DBOS.workflow()
        @staticmethod
        def testConcWorkflow():
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
    # with SetWorkflowID(wfid):
    #    wfh2 = DBOS.start_workflow(UsingFinallyClause.testConcWorkflow)

    wfh1.get_result()
    # wfh2.get_result()

    print(
        f"Started: {UsingFinallyClause.started}; "
        f"Completed: {UsingFinallyClause.completed}; "
        f"Aborted: {UsingFinallyClause.aborted}; "
        f"Trouble: {UsingFinallyClause.trouble}"
    )
    assert UsingFinallyClause.started
    assert UsingFinallyClause.completed
    assert not UsingFinallyClause.trouble
