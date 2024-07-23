from dbos_transact.dbos import DBOS
from dbos_transact.workflows import WorkflowContext


def test_dbos(dbos: DBOS) -> None:

    @dbos.workflow()
    def test_workflow(ctx: WorkflowContext, var: str, var2: str) -> str:
        return var

    assert test_workflow(dbos.wf_ctx(), "bob", "bob") == "bob"
