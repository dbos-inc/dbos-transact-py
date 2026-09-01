from dbos import DBOS


@DBOS.workflow()
def duplicated_workflow_name(x: int) -> str:
    """Duplicates the registered name of a workflow in dupname_workflows1.py"""
    return f"a:{x}"
