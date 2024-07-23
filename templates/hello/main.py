from dbos_transact import DBOS, WorkflowContext

dbos = DBOS()


@dbos.workflow()
def example_workflow(ctx: WorkflowContext, var: str) -> str:
    return var


if __name__ == "__main__":
    assert example_workflow(dbos.wf_ctx(), "mike") == "mike"
