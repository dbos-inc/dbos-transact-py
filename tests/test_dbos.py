import pytest

from dbos_transact.dbos import DBOS
from dbos_transact.workflows import WorkflowContext


def test_simple_workflow(dbos: DBOS) -> None:

    @dbos.workflow()
    def test_workflow(ctx: WorkflowContext, var: str, var2: str) -> str:
        return var + var2

    assert test_workflow(dbos.wf_ctx(), "bob", "bob") == "bobbob"


def test_exception_workflow(dbos: DBOS) -> None:

    @dbos.workflow()
    def exception_workflow(ctx: WorkflowContext) -> None:
        raise Exception("test error")

    with pytest.raises(Exception) as exc_info:
        exception_workflow(dbos.wf_ctx())

    assert "test error" in str(exc_info.value)
