from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Optional, ParamSpec, TypeVar

from dbos import utils
from dbos.context import (
    DBOSContext,
    DBOSContextEnsure,
    DBOSContextSwap,
    EnterDBOSWorkflow,
    OperationType,
    SetWorkflowUUID,
    TracedAttributes,
    assert_current_dbos_context,
)
from dbos.error import (
    DBOSNonExistentWorkflowError,
    DBOSRecoveryError,
    DBOSWorkflowConflictUUIDError,
    DBOSWorkflowFunctionNotFoundError,
)
from dbos.system_database import WorkflowInputs, WorkflowStatusInternal
from dbos.workflow import WorkflowHandle, WorkflowStatus

if TYPE_CHECKING:
    from dbos.dbos import DBOS, Workflow

P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values


class _WorkflowHandleFuture(WorkflowHandle[R]):

    def __init__(self, workflow_uuid: str, future: Future[R], dbos: "DBOS"):
        super().__init__(workflow_uuid)
        self.future = future
        self.dbos = dbos

    def get_result(self) -> R:
        return self.future.result()

    def get_status(self) -> WorkflowStatus:
        stat = self.dbos.get_workflow_status(self.workflow_uuid)
        if stat is None:
            raise DBOSNonExistentWorkflowError(self.workflow_uuid)
        return stat


class _WorkflowHandlePolling(WorkflowHandle[R]):

    def __init__(self, workflow_uuid: str, dbos: "DBOS"):
        super().__init__(workflow_uuid)
        self.dbos = dbos

    def get_result(self) -> R:
        res: R = self.dbos.sys_db.await_workflow_result(self.workflow_uuid)
        return res

    def get_status(self) -> WorkflowStatus:
        stat = self.dbos.get_workflow_status(self.workflow_uuid)
        if stat is None:
            raise DBOSNonExistentWorkflowError(self.workflow_uuid)
        return stat


def _init_workflow(
    dbos: "DBOS",
    ctx: DBOSContext,
    inputs: WorkflowInputs,
    wf_name: str,
    class_name: Optional[str],
    config_name: Optional[str],
) -> WorkflowStatusInternal:
    wfid = (
        ctx.workflow_uuid
        if len(ctx.workflow_uuid) > 0
        else ctx.id_assigned_for_next_workflow
    )
    status: WorkflowStatusInternal = {
        "workflow_uuid": wfid,
        "status": "PENDING",
        "name": wf_name,
        "class_name": class_name,
        "config_name": config_name,
        "output": None,
        "error": None,
        "app_id": ctx.app_id,
        "app_version": ctx.app_version,
        "executor_id": ctx.executor_id,
        "request": (utils.serialize(ctx.request) if ctx.request is not None else None),
        "recovery_attempts": None,
    }
    dbos.sys_db.update_workflow_status(status, False, ctx.in_recovery)

    # If we have an instance name, the first arg is the instance and do not serialize
    if class_name is not None:
        inputs = {"args": inputs["args"][1:], "kwargs": inputs["kwargs"]}
    dbos.sys_db.update_workflow_inputs(wfid, utils.serialize(inputs))

    return status


def _execute_workflow(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
    func: "Workflow[P, R]",
    *args: Any,
    **kwargs: Any,
) -> R:
    try:
        output = func(*args, **kwargs)
    except DBOSWorkflowConflictUUIDError:
        # Retrieve the workflow handle and wait for the result.
        wf_handle: WorkflowHandle[R] = dbos.retrieve_workflow(status["workflow_uuid"])
        output = wf_handle.get_result()
        return output
    except Exception as error:
        status["status"] = "ERROR"
        status["error"] = utils.serialize(error)
        dbos.sys_db.update_workflow_status(status)
        raise error

    status["status"] = "SUCCESS"
    status["output"] = utils.serialize(output)
    dbos.sys_db.update_workflow_status(status)
    return output


def _execute_workflow_wthread(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
    func: "Workflow[P, R]",
    ctx: DBOSContext,
    *args: Any,
    **kwargs: Any,
) -> R:
    attributes: TracedAttributes = {
        "name": func.__name__,
        "operationType": OperationType.WORKFLOW.value,
    }
    with DBOSContextSwap(ctx):
        with EnterDBOSWorkflow(attributes):
            try:
                return _execute_workflow(dbos, status, func, *args, **kwargs)
            except Exception as e:
                dbos.logger.error(
                    f"Exception encountered in asynchronous workflow: {repr(e)}"
                )
                raise e


def execute_workflow_uuid(dbos: "DBOS", workflow_uuid: str) -> WorkflowHandle[Any]:
    status = dbos.sys_db.get_workflow_status(workflow_uuid)
    if not status:
        raise DBOSRecoveryError(workflow_uuid, "Workflow status not found")
    inputs = dbos.sys_db.get_workflow_inputs(workflow_uuid)
    if not inputs:
        raise DBOSRecoveryError(workflow_uuid, "Workflow inputs not found")
    wf_func = dbos.workflow_info_map.get(status["name"], None)
    if not wf_func:
        raise DBOSWorkflowFunctionNotFoundError(
            workflow_uuid, "Workflow function not found"
        )
    with DBOSContextEnsure():
        ctx = assert_current_dbos_context()
        request = status["request"]
        ctx.request = utils.deserialize(request) if request is not None else None
        if status["config_name"] is not None:
            config_name = status["config_name"]
            class_name = status["class_name"]
            iname = f"{class_name}/{config_name}"
            if iname not in dbos.instance_info_map:
                raise DBOSWorkflowFunctionNotFoundError(
                    workflow_uuid,
                    f"Cannot execute workflow because instance '{iname}' is not registered",
                )
            with SetWorkflowUUID(workflow_uuid):
                return dbos.start_workflow(
                    wf_func,
                    dbos.instance_info_map[iname],
                    *inputs["args"],
                    **inputs["kwargs"],
                )
        elif status["class_name"] is not None:
            class_name = status["class_name"]
            if class_name not in dbos.class_info_map:
                raise DBOSWorkflowFunctionNotFoundError(
                    workflow_uuid,
                    f"Cannot execute workflow because class '{class_name}' is not registered",
                )
            with SetWorkflowUUID(workflow_uuid):
                return dbos.start_workflow(
                    wf_func,
                    dbos.class_info_map[class_name],
                    *inputs["args"],
                    **inputs["kwargs"],
                )
        else:
            with SetWorkflowUUID(workflow_uuid):
                return dbos.start_workflow(wf_func, *inputs["args"], **inputs["kwargs"])
