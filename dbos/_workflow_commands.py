from typing import Any

from rich import print

from dbos import DBOS

from . import load_config
from ._dbos_config import _is_valid_app_name
from ._sys_db import GetWorkflowsInput, GetWorkflowsOutput, SystemDatabase


def _list_workflows(
    li: int,
    user: str,
    starttime: str,
    endtime: str,
    status: str,
    request: bool,
    appversion: str,
) -> None:
    print(
        f"Listing steps limit {li} user {user} st {starttime} et {endtime} status {status} req {request}"
    )
    config = load_config()
    sys_db = None

    try:
        sys_db = SystemDatabase(config)
    except Exception as e:
        typer.echo(f"DBOS system schema migration failed: {e}")
    finally:
        if sys_db:
            sys_db.destroy()

    input = GetWorkflowsInput()
    input.authenticated_user = user
    input.start_time = starttime
    input.end_time = endtime
    input.status = status
    input.application_version = appversion
    input.limit = li

    output: GetWorkflowsOutput = sys_db.get_workflows(input)

    print(output)

    infos = []

    # TODO reverse the workflow uuids

    if output.workflow_uuids is None:
        print("No workflows found")
        return {}

    for workflow_id in output.workflow_uuids:
        info = _get_workflow_info(
            sys_db, workflow_id, request
        )  # Call the method for each ID
        infos.append(info)

    # print(json.dumps(infos))
    print(infos)


def _get_workflow(uuid: str, request: bool) -> str:
    print(f"Getting workflow info for {uuid}")
    config = load_config()
    sys_db = None

    try:
        sys_db = SystemDatabase(config)
    except Exception as e:
        typer.echo(f"DBOS system schema migration failed: {e}")
    finally:
        if sys_db:
            sys_db.destroy()

    info = _get_workflow_info(sys_db, uuid, request)
    return info


def _cancel_workflow(uuid: str) -> str:
    print(f"Getting workflow info for {uuid}")
    config = load_config()
    sys_db = None

    try:
        sys_db = SystemDatabase(config)
    except Exception as e:
        typer.echo(f"DBOS system schema migration failed: {e}")
    finally:
        if sys_db:
            sys_db.destroy()

    sys_db.set_workflow_status(uuid, "CANCELLED", False)


def _reattempt_workflow(uuid: str, startNewWorkflow: bool) -> str:
    print(f"Reattempt workflow info for {uuid}")
    config = load_config()

    dbos = DBOS(config=config)
    dbos.launch()

    if startNewWorkflow != True:
        dbos._sys_db.set_workflow_status(uuid, "PENDING", True)

    handle = dbos.execute_workflow_id(uuid)
    output = handle.result()
    print(output)
    dbos.destroy()


def _get_workflow_info(
    sys_db: SystemDatabase, workflowUUID: str, getRequest: bool
) -> str:
    info = sys_db.get_workflow_status(workflowUUID)
    if info is None:
        return {}

    info["workflowUUID"] = workflowUUID

    input_data = sys_db.get_workflow_inputs(workflowUUID)
    if input_data is not None:
        info["input"] = input_data

    if info.get("status") == "SUCCESS":
        result = sys_db.await_workflow_result(workflowUUID)
        info["output"] = result
    elif info.get("status") == "ERROR":
        try:
            sys_db.await_workflow_result(workflowUUID)
        except Exception as e:
            info["error"] = str(e)

    if not getRequest:
        info.pop("request", None)

    return info
