import sys
import traceback
from concurrent.futures import Future
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Optional, Tuple, TypeVar, cast

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec, TypeAlias
else:
    from typing import ParamSpec, TypeAlias

from dbos import utils
from dbos.context import (
    DBOSAssumeRole,
    DBOSContext,
    DBOSContextEnsure,
    DBOSContextSwap,
    EnterDBOSChildWorkflow,
    EnterDBOSWorkflow,
    OperationType,
    SetWorkflowUUID,
    TracedAttributes,
    assert_current_dbos_context,
    get_local_dbos_context,
)
from dbos.error import (
    DBOSNonExistentWorkflowError,
    DBOSRecoveryError,
    DBOSWorkflowConflictUUIDError,
    DBOSWorkflowFunctionNotFoundError,
)
from dbos.registrations import (
    get_config_name,
    get_dbos_class_name,
    get_dbos_func_name,
    get_func_info,
    get_or_create_func_info,
)
from dbos.roles import check_required_roles
from dbos.system_database import WorkflowInputs, WorkflowStatusInternal
from dbos.workflow import WorkflowHandle, WorkflowStatus

if TYPE_CHECKING:
    from dbos.dbos import DBOS, Workflow

P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values
F = TypeVar("F", bound=Callable[..., Any])


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
                    f"Exception encountered in asynchronous workflow: {traceback.format_exc()}"
                )
                raise e


def _execute_workflow_uuid(dbos: "DBOS", workflow_uuid: str) -> WorkflowHandle[Any]:
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
                return _start_workflow(
                    dbos,
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
                return _start_workflow(
                    dbos,
                    wf_func,
                    dbos.class_info_map[class_name],
                    *inputs["args"],
                    **inputs["kwargs"],
                )
        else:
            with SetWorkflowUUID(workflow_uuid):
                return _start_workflow(
                    dbos, wf_func, *inputs["args"], **inputs["kwargs"]
                )


def _workflow_wrapper(dbos: "DBOS", func: F) -> F:
    func.__orig_func = func  # type: ignore

    fi = get_or_create_func_info(func)

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        rr: Optional[str] = check_required_roles(func, fi)
        attributes: TracedAttributes = {
            "name": func.__name__,
            "operationType": OperationType.WORKFLOW.value,
        }
        inputs: WorkflowInputs = {
            "args": args,
            "kwargs": kwargs,
        }
        ctx = get_local_dbos_context()
        if ctx and ctx.is_workflow():
            with EnterDBOSChildWorkflow(attributes), DBOSAssumeRole(rr):
                ctx = assert_current_dbos_context()  # Now the child ctx
                status = _init_workflow(
                    dbos,
                    ctx,
                    inputs=inputs,
                    wf_name=get_dbos_func_name(func),
                    class_name=get_dbos_class_name(fi, func, args),
                    config_name=get_config_name(fi, func, args),
                )

                return _execute_workflow(dbos, status, func, *args, **kwargs)
        else:
            with EnterDBOSWorkflow(attributes), DBOSAssumeRole(rr):
                ctx = assert_current_dbos_context()
                status = _init_workflow(
                    dbos,
                    ctx,
                    inputs=inputs,
                    wf_name=get_dbos_func_name(func),
                    class_name=get_dbos_class_name(fi, func, args),
                    config_name=get_config_name(fi, func, args),
                )

                return _execute_workflow(dbos, status, func, *args, **kwargs)

    wrapped_func = cast(F, wrapper)
    return wrapped_func


def _start_workflow(
    dbos: "DBOS",
    func: "Workflow[P, R]",
    *args: P.args,
    **kwargs: P.kwargs,
) -> WorkflowHandle[R]:
    fself: Optional[object] = None
    if hasattr(func, "__self__"):
        fself = func.__self__

    fi = get_func_info(func)
    if fi is None:
        raise DBOSWorkflowFunctionNotFoundError(
            "<NONE>", f"start_workflow: function {func.__name__} is not registered"
        )

    func = cast("Workflow[P, R]", func.__orig_func)  # type: ignore

    inputs: WorkflowInputs = {
        "args": args,
        "kwargs": kwargs,
    }

    # Sequence of events for starting a workflow:
    #   First - is there a WF already running?
    #      (and not in tx/comm as that is an error)
    #   Assign an ID to the workflow, if it doesn't have an app-assigned one
    #      If this is a root workflow, assign a new UUID
    #      If this is a child workflow, assign parent wf id with call# suffix
    #   Make a (system) DB record for the workflow
    #   Pass the new context to a worker thread that will run the wf function
    cur_ctx = get_local_dbos_context()
    if cur_ctx is not None and cur_ctx.is_within_workflow():
        assert cur_ctx.is_workflow()  # Not in tx / comm
        cur_ctx.function_id += 1
        if len(cur_ctx.id_assigned_for_next_workflow) == 0:
            cur_ctx.id_assigned_for_next_workflow = (
                cur_ctx.workflow_uuid + "-" + str(cur_ctx.function_id)
            )

    new_wf_ctx = DBOSContext() if cur_ctx is None else cur_ctx.create_child()
    new_wf_ctx.id_assigned_for_next_workflow = new_wf_ctx.assign_workflow_id()
    new_wf_uuid = new_wf_ctx.id_assigned_for_next_workflow

    gin_args: Tuple[Any, ...] = args
    if fself is not None:
        gin_args = (fself,)

    status = _init_workflow(
        dbos,
        new_wf_ctx,
        inputs=inputs,
        wf_name=get_dbos_func_name(func),
        class_name=get_dbos_class_name(fi, func, gin_args),
        config_name=get_config_name(fi, func, gin_args),
    )

    if fself is not None:
        future = dbos.executor.submit(
            cast(Callable[..., R], _execute_workflow_wthread),
            dbos,
            status,
            func,
            new_wf_ctx,
            fself,
            *args,
            **kwargs,
        )
    else:
        future = dbos.executor.submit(
            cast(Callable[..., R], _execute_workflow_wthread),
            dbos,
            status,
            func,
            new_wf_ctx,
            *args,
            **kwargs,
        )
    return _WorkflowHandleFuture(new_wf_uuid, future, dbos)
