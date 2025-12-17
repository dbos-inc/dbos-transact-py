import uuid
from time import sleep

import sqlalchemy as sa

# Public API
from dbos import DBOS, SetWorkflowID


def test_simple_workflow(dbos: DBOS) -> None:
    @DBOS.dbos_class()
    class TryConcExec:
        conc_exec = 0
        max_conc = 0

        conc_wf = 0
        max_wf = 0

        @DBOS.step()
        def testConcStep() -> None:
            TryConcExec.conc_exec += 1
            TryConcExec.max_conc = max(TryConcExec.conc_exec, TryConcExec.max_conc)
            sleep(1)
            TryConcExec.conc_exec -= 1

        @DBOS.workflow()
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
    """
    const wfh1r = await reexecuteWorkflowById(workflowUUID);
    const wfh2r = await reexecuteWorkflowById(workflowUUID);
    await wfh1r!.getResult();
    await wfh2r!.getResult();
    expect(TryConcExec.maxConc).toBe(1);
    expect(TryConcExec.maxWf).toBe(1);
    """
