from dbos import DBOS


@DBOS.workflow()
def duplicated_workflow_name(x: int) -> str:
    """Duplicates the registered name of a workflow in dupname_workflowsa.py"""
    return f"one:{x}"
