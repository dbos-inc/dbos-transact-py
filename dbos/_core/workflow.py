import asyncio
import inspect
import json
import sys
import traceback
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Callable, Generic, Optional, Tuple, TypeVar, cast

from dbos._core.types import WorkflowInputs, WorkflowStatusInternal

from ..types import WorkflowStatusString
from . import serialization

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from ..context import (
    DBOSContext,
    DBOSContextEnsure,
    DBOSContextSwap,
    EnterDBOSWorkflow,
    OperationType,
    SetWorkflowID,
    TracedAttributes,
    assert_current_dbos_context,
    get_local_dbos_context,
)
from ..error import (
    DBOSNonExistentWorkflowError,
    DBOSRecoveryError,
    DBOSWorkflowConflictIDError,
    DBOSWorkflowFunctionNotFoundError,
)
from ..registrations import (
    DEFAULT_MAX_RECOVERY_ATTEMPTS,
    get_config_name,
    get_dbos_class_name,
    get_dbos_func_name,
    get_func_info,
    get_temp_workflow_type,
)

if TYPE_CHECKING:
    from ..dbos import DBOS, Workflow, WorkflowHandle, WorkflowStatus


# these are duped in dbos.py
P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values
F = TypeVar("F", bound=Callable[..., Any])


async def _get_status_async(dbos: "DBOS", workflow_id: str) -> "WorkflowStatus":
    stat = await dbos.get_workflow_status_async(workflow_id)
    if stat is None:
        raise DBOSNonExistentWorkflowError(workflow_id)
    return stat


def _get_status_sync(dbos: "DBOS", workflow_id: str) -> "WorkflowStatus":
    stat = dbos.get_workflow_status(workflow_id)
    if stat is None:
        raise DBOSNonExistentWorkflowError(workflow_id)
    return stat


class WorkflowHandleFuture(Generic[R]):

    def __init__(self, workflow_id: str, future: Future[R], dbos: "DBOS"):
        self.workflow_id = workflow_id
        self.future = future
        self.dbos = dbos

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(self) -> R:
        return self.future.result()

    async def get_result_async(self) -> R:
        return await asyncio.wrap_future(self.future)

    def get_status(self) -> "WorkflowStatus":
        return _get_status_sync(self.dbos, self.workflow_id)

    async def get_status_async(self) -> "WorkflowStatus":
        return await _get_status_async(self.dbos, self.workflow_id)


class WorkflowHandlePolling(Generic[R]):

    def __init__(self, workflow_id: str, dbos: "DBOS"):
        self.workflow_id = workflow_id
        self.dbos = dbos

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(self) -> R:
        res: R = self.dbos._sys_db.await_workflow_result_sync(self.workflow_id)
        return res

    async def get_result_async(self) -> R:
        res: R = await self.dbos._sys_db.await_workflow_result_async(self.workflow_id)
        return res

    def get_status(self) -> "WorkflowStatus":
        return _get_status_sync(self.dbos, self.workflow_id)

    async def get_status_async(self) -> "WorkflowStatus":
        return await _get_status_async(self.dbos, self.workflow_id)


def init_workflow_sync(
    dbos: "DBOS",
    ctx: DBOSContext,
    inputs: WorkflowInputs,
    wf_name: str,
    class_name: Optional[str],
    config_name: Optional[str],
    temp_wf_type: Optional[str],
    queue: Optional[str] = None,
    max_recovery_attempts: int = DEFAULT_MAX_RECOVERY_ATTEMPTS,
) -> WorkflowStatusInternal:
    wfid = (
        ctx.workflow_id
        if len(ctx.workflow_id) > 0
        else ctx.id_assigned_for_next_workflow
    )
    status: WorkflowStatusInternal = {
        "workflow_uuid": wfid,
        "status": (
            WorkflowStatusString.PENDING.value
            if queue is None
            else WorkflowStatusString.ENQUEUED.value
        ),
        "name": wf_name,
        "class_name": class_name,
        "config_name": config_name,
        "output": None,
        "error": None,
        "app_id": ctx.app_id,
        "app_version": ctx.app_version,
        "executor_id": ctx.executor_id,
        "request": (
            serialization.serialize(ctx.request) if ctx.request is not None else None
        ),
        "recovery_attempts": None,
        "authenticated_user": ctx.authenticated_user,
        "authenticated_roles": (
            json.dumps(ctx.authenticated_roles) if ctx.authenticated_roles else None
        ),
        "assumed_role": ctx.assumed_role,
        "queue_name": queue,
    }

    # If we have a class name, the first arg is the instance and do not serialize
    if class_name is not None:
        inputs = {"args": inputs["args"][1:], "kwargs": inputs["kwargs"]}

    if temp_wf_type != "transaction" or queue is not None:
        # Synchronously record the status and inputs for workflows and single-step workflows
        # We also have to do this for single-step workflows because of the foreign key constraint on the operation outputs table
        # TODO: Make this transactional (and with the queue step below)
        dbos._sys_db.update_workflow_status_sync(
            status, False, ctx.in_recovery, max_recovery_attempts=max_recovery_attempts
        )
        dbos._sys_db.update_workflow_inputs_sync(
            wfid, serialization.serialize_args(inputs)
        )
    else:
        # Buffer the inputs for single-transaction workflows, but don't buffer the status
        dbos._sys_db.buffer_workflow_inputs(wfid, serialization.serialize_args(inputs))

    if queue is not None:
        dbos._sys_db.enqueue_sync(wfid, queue)

    return status


async def init_workflow_async(
    dbos: "DBOS",
    ctx: DBOSContext,
    inputs: WorkflowInputs,
    wf_name: str,
    class_name: Optional[str],
    config_name: Optional[str],
    temp_wf_type: Optional[str],
    queue: Optional[str] = None,
    max_recovery_attempts: int = DEFAULT_MAX_RECOVERY_ATTEMPTS,
) -> WorkflowStatusInternal:
    wfid = (
        ctx.workflow_id
        if len(ctx.workflow_id) > 0
        else ctx.id_assigned_for_next_workflow
    )
    status: WorkflowStatusInternal = {
        "workflow_uuid": wfid,
        "status": (
            WorkflowStatusString.PENDING.value
            if queue is None
            else WorkflowStatusString.ENQUEUED.value
        ),
        "name": wf_name,
        "class_name": class_name,
        "config_name": config_name,
        "output": None,
        "error": None,
        "app_id": ctx.app_id,
        "app_version": ctx.app_version,
        "executor_id": ctx.executor_id,
        "request": (
            serialization.serialize(ctx.request) if ctx.request is not None else None
        ),
        "recovery_attempts": None,
        "authenticated_user": ctx.authenticated_user,
        "authenticated_roles": (
            json.dumps(ctx.authenticated_roles) if ctx.authenticated_roles else None
        ),
        "assumed_role": ctx.assumed_role,
        "queue_name": queue,
    }

    # If we have a class name, the first arg is the instance and do not serialize
    if class_name is not None:
        inputs = {"args": inputs["args"][1:], "kwargs": inputs["kwargs"]}

    if temp_wf_type != "transaction" or queue is not None:
        # Synchronously record the status and inputs for workflows and single-step workflows
        # We also have to do this for single-step workflows because of the foreign key constraint on the operation outputs table
        # TODO: Make this transactional (and with the queue step below)
        await dbos._sys_db.update_workflow_status_async(
            status, False, ctx.in_recovery, max_recovery_attempts=max_recovery_attempts
        )
        await dbos._sys_db.update_workflow_inputs_async(
            wfid, serialization.serialize_args(inputs)
        )
    else:
        # Buffer the inputs for single-transaction workflows, but don't buffer the status
        dbos._sys_db.buffer_workflow_inputs(wfid, serialization.serialize_args(inputs))

    if queue is not None:
        await dbos._sys_db.enqueue_async(wfid, queue)

    return status


def execute_workflow_sync(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
    func: "Workflow[P, R]",
    *args: Any,
    **kwargs: Any,
) -> R:
    try:
        output = func(*args, **kwargs)
        status["status"] = "SUCCESS"
        status["output"] = serialization.serialize(output)
        if status["queue_name"] is not None:
            queue = dbos._registry.queue_info_map[status["queue_name"]]
            dbos._sys_db.remove_from_queue_sync(status["workflow_uuid"], queue)
        dbos._sys_db.buffer_workflow_status(status)
    except DBOSWorkflowConflictIDError:
        # Retrieve the workflow handle and wait for the result.
        # Must use existing_workflow=False because workflow status might not be set yet for single transaction workflows.
        wf_handle: "WorkflowHandle[R]" = dbos.retrieve_workflow(
            status["workflow_uuid"], existing_workflow=False
        )
        output = wf_handle.get_result()
        return output
    except Exception as error:
        status["status"] = "ERROR"
        status["error"] = serialization.serialize_exception(error)
        if status["queue_name"] is not None:
            queue = dbos._registry.queue_info_map[status["queue_name"]]
            dbos._sys_db.remove_from_queue_sync(status["workflow_uuid"], queue)
        dbos._sys_db.update_workflow_status_sync(status)
        raise

    return output


async def execute_workflow_async(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
    func: "Workflow[P, R]",
    *args: Any,
    **kwargs: Any,
) -> R:
    try:
        output: R = await (
            func(*args, **kwargs)
            if inspect.iscoroutinefunction(func)
            else asyncio.to_thread(func, *args, **kwargs)
        )
        status["status"] = "SUCCESS"
        status["output"] = serialization.serialize(output)
        if status["queue_name"] is not None:
            queue = dbos._registry.queue_info_map[status["queue_name"]]
            await dbos._sys_db.remove_from_queue_async(status["workflow_uuid"], queue)
        dbos._sys_db.buffer_workflow_status(status)
    except DBOSWorkflowConflictIDError:
        # Retrieve the workflow handle and wait for the result.
        # Must use existing_workflow=False because workflow status might not be set yet for single transaction workflows.
        wf_handle: "WorkflowHandle[R]" = await dbos.retrieve_workflow_async(
            status["workflow_uuid"], existing_workflow=False
        )
        output = wf_handle.get_result()
        return output
    except Exception as error:
        status["status"] = "ERROR"
        status["error"] = serialization.serialize_exception(error)
        if status["queue_name"] is not None:
            queue = dbos._registry.queue_info_map[status["queue_name"]]
            await dbos._sys_db.remove_from_queue_async(status["workflow_uuid"], queue)
        await dbos._sys_db.update_workflow_status_async(status)
        raise

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
                return execute_workflow_sync(dbos, status, func, *args, **kwargs)
            except Exception:
                dbos.logger.error(
                    f"Exception encountered in asynchronous workflow: {traceback.format_exc()}"
                )
                raise


def execute_workflow_id(dbos: "DBOS", workflow_id: str) -> "WorkflowHandle[Any]":
    status = dbos._sys_db.get_workflow_status_sync(workflow_id)
    if not status:
        raise DBOSRecoveryError(workflow_id, "Workflow status not found")
    inputs = dbos._sys_db.get_workflow_inputs_sync(workflow_id)
    if not inputs:
        raise DBOSRecoveryError(workflow_id, "Workflow inputs not found")
    wf_func = dbos._registry.workflow_info_map.get(status["name"], None)
    if not wf_func:
        raise DBOSWorkflowFunctionNotFoundError(
            workflow_id, "Workflow function not found"
        )
    with DBOSContextEnsure():
        ctx = assert_current_dbos_context()
        request = status["request"]
        ctx.request = (
            serialization.deserialize(request) if request is not None else None
        )
        if status["config_name"] is not None:
            config_name = status["config_name"]
            class_name = status["class_name"]
            iname = f"{class_name}/{config_name}"
            if iname not in dbos._registry.instance_info_map:
                raise DBOSWorkflowFunctionNotFoundError(
                    workflow_id,
                    f"Cannot execute workflow because instance '{iname}' is not registered",
                )
            with SetWorkflowID(workflow_id):
                return start_workflow(
                    dbos,
                    wf_func,
                    status["queue_name"],
                    True,
                    dbos._registry.instance_info_map[iname],
                    *inputs["args"],
                    **inputs["kwargs"],
                )
        elif status["class_name"] is not None:
            class_name = status["class_name"]
            if class_name not in dbos._registry.class_info_map:
                raise DBOSWorkflowFunctionNotFoundError(
                    workflow_id,
                    f"Cannot execute workflow because class '{class_name}' is not registered",
                )
            with SetWorkflowID(workflow_id):
                return start_workflow(
                    dbos,
                    wf_func,
                    status["queue_name"],
                    True,
                    dbos._registry.class_info_map[class_name],
                    *inputs["args"],
                    **inputs["kwargs"],
                )
        else:
            with SetWorkflowID(workflow_id):
                return start_workflow(
                    dbos,
                    wf_func,
                    status["queue_name"],
                    True,
                    *inputs["args"],
                    **inputs["kwargs"],
                )


def start_workflow(
    dbos: "DBOS",
    func: "Workflow[P, R]",
    queue_name: Optional[str],
    execute_workflow: bool,
    *args: P.args,
    **kwargs: P.kwargs,
) -> "WorkflowHandle[R]":
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
    #      (and not in step as that is an error)
    #   Assign an ID to the workflow, if it doesn't have an app-assigned one
    #      If this is a root workflow, assign a new ID
    #      If this is a child workflow, assign parent wf id with call# suffix
    #   Make a (system) DB record for the workflow
    #   Pass the new context to a worker thread that will run the wf function
    cur_ctx = get_local_dbos_context()
    if cur_ctx is not None and cur_ctx.is_within_workflow():
        assert cur_ctx.is_workflow()  # Not in a step
        cur_ctx.function_id += 1
        if len(cur_ctx.id_assigned_for_next_workflow) == 0:
            cur_ctx.id_assigned_for_next_workflow = (
                cur_ctx.workflow_id + "-" + str(cur_ctx.function_id)
            )

    new_wf_ctx = DBOSContext() if cur_ctx is None else cur_ctx.create_child()
    new_wf_ctx.id_assigned_for_next_workflow = new_wf_ctx.assign_workflow_id()
    new_wf_id = new_wf_ctx.id_assigned_for_next_workflow

    gin_args: Tuple[Any, ...] = args
    if fself is not None:
        gin_args = (fself,)

    status = init_workflow_sync(
        dbos,
        new_wf_ctx,
        inputs=inputs,
        wf_name=get_dbos_func_name(func),
        class_name=get_dbos_class_name(fi, func, gin_args),
        config_name=get_config_name(fi, func, gin_args),
        temp_wf_type=get_temp_workflow_type(func),
        queue=queue_name,
        max_recovery_attempts=fi.max_recovery_attempts,
    )

    if not execute_workflow:
        return WorkflowHandlePolling(new_wf_id, dbos)

    submit_args = (
        (dbos, status, func, new_wf_ctx, fself) + args
        if fself is not None
        else (dbos, status, func, new_wf_ctx) + args
    )

    future = dbos._executor.submit(
        cast(Callable[..., R], _execute_workflow_wthread),
        *submit_args,
        **kwargs,
    )
    return WorkflowHandleFuture(new_wf_id, future, dbos)
