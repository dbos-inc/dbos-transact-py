from typing import Any, List, cast

import typer
from rich import print

from dbos import DBOS

from . import load_config
from ._dbos_config import ConfigFile, _is_valid_app_name
from ._sys_db import (
    GetWorkflowsInput,
    GetWorkflowsOutput,
    SystemDatabase,
    WorkflowStatuses,
    WorkflowStatusInternal,
    WorkflowStatusString,
)


def _list_workflows(
    config: ConfigFile,
    li: int,
    user: str | None,
    starttime: str | None,
    endtime: str | None,
    status: str | None,
    request: bool,
    appversion: str | None,
) -> List[WorkflowStatusInternal]:

    sys_db = None

    try:
        sys_db = SystemDatabase(config)
    except Exception as e:
        typer.echo(f"DBOS system schema migration failed: {e}")
        return []
    finally:
        if sys_db:
            sys_db.destroy()

    input = GetWorkflowsInput()
    input.authenticated_user = user
    input.start_time = starttime
    input.end_time = endtime
    if status is not None:
        input.status = cast(WorkflowStatuses, status)
    input.application_version = appversion
    input.limit = li

    output: GetWorkflowsOutput = sys_db.get_workflows(input)

    infos: List[WorkflowStatusInternal] = []

    # TODO reverse the workflow uuids

    if output.workflow_uuids is None:
        typer.echo("No workflows found")
        return {}

    for workflow_id in output.workflow_uuids:
        info = _get_workflow_info(
            sys_db, workflow_id, request
        )  # Call the method for each ID
        if info is not None:
            infos.append(info)

    return infos


def _get_workflow(
    config: ConfigFile, uuid: str, request: bool
) -> WorkflowStatusInternal | None:
    print(f"Getting workflow info for {uuid}")
    # config = load_config()
    sys_db = None

    try:
        sys_db = SystemDatabase(config)
    except Exception as e:
        typer.echo(f"DBOS system schema migration failed: {e}")
        return None
    finally:
        if sys_db:
            sys_db.destroy()

    info = _get_workflow_info(sys_db, uuid, request)
    return info


def _cancel_workflow(config: ConfigFile, uuid: str) -> None:
    # config = load_config()
    sys_db = None

    try:
        sys_db = SystemDatabase(config)
    except Exception as e:
        typer.echo(f"DBOS system schema migration failed: {e}")
        return None
    finally:
        if sys_db:
            sys_db.destroy()

    sys_db.set_workflow_status(uuid, WorkflowStatusString.CANCELLED, False)
    return


def _reattempt_workflow(uuid: str, startNewWorkflow: bool) -> None:
    print(f"Reattempt workflow info for {uuid} not implemented")
    return


def _get_workflow_info(
    sys_db: SystemDatabase, workflowUUID: str, getRequest: bool
) -> WorkflowStatusInternal:
    info = sys_db.get_workflow_status(workflowUUID)
    if info is None:
        return WorkflowStatusInternal()

    info["workflow_uuid"] = workflowUUID

    # no input field
    # input_data = sys_db.get_workflow_inputs(workflowUUID)
    # if input_data is not None:
    #    info["input"] = input_data

    if info.get("status") == "SUCCESS":
        result = sys_db.await_workflow_result(workflowUUID)
        info["output"] = result
    elif info.get("status") == "ERROR":
        try:
            sys_db.await_workflow_result(workflowUUID)
        except Exception as e:
            info["error"] = str(e)

    if not getRequest:
        info["request"] = None

    return info
