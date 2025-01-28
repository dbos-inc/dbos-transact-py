import asyncio
import functools
import inspect
import json
import sys
import time
import traceback
from concurrent.futures import Future
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Generic,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

from dbos._outcome import Immediate, NoResult, Outcome, Pending

from ._app_db import ApplicationDatabase, TransactionResultInternal

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from . import _serialization
from ._context import (
    DBOSAssumeRole,
    DBOSContext,
    DBOSContextEnsure,
    DBOSContextSwap,
    EnterDBOSChildWorkflow,
    EnterDBOSStep,
    EnterDBOSTransaction,
    EnterDBOSWorkflow,
    OperationType,
    SetWorkflowID,
    TracedAttributes,
    assert_current_dbos_context,
    get_local_dbos_context,
)
from ._error import (
    DBOSException,
    DBOSMaxStepRetriesExceeded,
    DBOSNonExistentWorkflowError,
    DBOSRecoveryError,
    DBOSWorkflowConflictIDError,
    DBOSWorkflowFunctionNotFoundError,
)
from ._registrations import (
    DEFAULT_MAX_RECOVERY_ATTEMPTS,
    get_config_name,
    get_dbos_class_name,
    get_dbos_func_name,
    get_func_info,
    get_or_create_func_info,
    get_temp_workflow_type,
    set_dbos_func_name,
    set_temp_workflow_type,
)
from ._roles import check_required_roles
from ._serialization import WorkflowInputs
from ._sys_db import (
    GetEventWorkflowContext,
    OperationResultInternal,
    WorkflowStatusInternal,
    WorkflowStatusString,
)

if TYPE_CHECKING:
    from ._dbos import (
        DBOS,
        Workflow,
        WorkflowHandle,
        WorkflowStatus,
        DBOSRegistry,
        IsolationLevel,
    )

from sqlalchemy.exc import DBAPIError, InvalidRequestError

P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values
F = TypeVar("F", bound=Callable[..., Any])

TEMP_SEND_WF_NAME = "<temp>.temp_send_workflow"


class WorkflowHandleFuture(Generic[R]):

    def __init__(self, workflow_id: str, future: Future[R], dbos: "DBOS"):
        self.workflow_id = workflow_id
        self.future = future
        self.dbos = dbos

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(self) -> R:
        return self.future.result()

    def get_status(self) -> "WorkflowStatus":
        stat = self.dbos.get_workflow_status(self.workflow_id)
        if stat is None:
            raise DBOSNonExistentWorkflowError(self.workflow_id)
        return stat


class WorkflowHandlePolling(Generic[R]):

    def __init__(self, workflow_id: str, dbos: "DBOS"):
        self.workflow_id = workflow_id
        self.dbos = dbos

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(self) -> R:
        res: R = self.dbos._sys_db.await_workflow_result(self.workflow_id)
        return res

    def get_status(self) -> "WorkflowStatus":
        stat = self.dbos.get_workflow_status(self.workflow_id)
        if stat is None:
            raise DBOSNonExistentWorkflowError(self.workflow_id)
        return stat


def _init_workflow(
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
            _serialization.serialize(ctx.request) if ctx.request is not None else None
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

    wf_status = status["status"]
    if temp_wf_type != "transaction" or queue is not None:
        # Synchronously record the status and inputs for workflows and single-step workflows
        # We also have to do this for single-step workflows because of the foreign key constraint on the operation outputs table
        # TODO: Make this transactional (and with the queue step below)
        wf_status = dbos._sys_db.update_workflow_status(
            status, False, ctx.in_recovery, max_recovery_attempts=max_recovery_attempts
        )
        # TODO: Modify the inputs if they were changed by `update_workflow_inputs`
        dbos._sys_db.update_workflow_inputs(wfid, _serialization.serialize_args(inputs))
    else:
        # Buffer the inputs for single-transaction workflows, but don't buffer the status
        dbos._sys_db.buffer_workflow_inputs(wfid, _serialization.serialize_args(inputs))

    if queue is not None and wf_status == WorkflowStatusString.ENQUEUED.value:
        dbos._sys_db.enqueue(wfid, queue)

    status["status"] = wf_status
    return status


def _get_wf_invoke_func(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
) -> Callable[[Callable[[], R]], R]:
    def persist(func: Callable[[], R]) -> R:
        try:
            output = func()
            status["status"] = "SUCCESS"
            status["output"] = _serialization.serialize(output)
            if status["queue_name"] is not None:
                queue = dbos._registry.queue_info_map[status["queue_name"]]
                dbos._sys_db.remove_from_queue(status["workflow_uuid"], queue)
            dbos._sys_db.buffer_workflow_status(status)
            return output
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
            status["error"] = _serialization.serialize_exception(error)
            if status["queue_name"] is not None:
                queue = dbos._registry.queue_info_map[status["queue_name"]]
                dbos._sys_db.remove_from_queue(status["workflow_uuid"], queue)
            dbos._sys_db.update_workflow_status(status)
            raise

    return persist


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
                result = (
                    Outcome[R]
                    .make(functools.partial(func, *args, **kwargs))
                    .then(_get_wf_invoke_func(dbos, status))
                )
                if isinstance(result, Immediate):
                    return cast(Immediate[R], result)()
                else:
                    return asyncio.run(cast(Pending[R], result)())
            except Exception:
                dbos.logger.error(
                    f"Exception encountered in asynchronous workflow: {traceback.format_exc()}"
                )
                raise


def execute_workflow_by_id(
    dbos: "DBOS", workflow_id: str, startNew: bool = False
) -> "WorkflowHandle[Any]":
    status = dbos._sys_db.get_workflow_status(workflow_id)
    if not status:
        raise DBOSRecoveryError(workflow_id, "Workflow status not found")
    inputs = dbos._sys_db.get_workflow_inputs(workflow_id)
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
            _serialization.deserialize(request) if request is not None else None
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

            if startNew:
                return start_workflow(
                    dbos,
                    wf_func,
                    status["queue_name"],
                    True,
                    dbos._registry.instance_info_map[iname],
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

            if startNew:
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
                        dbos._registry.class_info_map[class_name],
                        *inputs["args"],
                        **inputs["kwargs"],
                    )
        else:
            if startNew:
                return start_workflow(
                    dbos,
                    wf_func,
                    status["queue_name"],
                    True,
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


@overload
def start_workflow(
    dbos: "DBOS",
    func: "Workflow[P, Coroutine[Any, Any, R]]",
    queue_name: Optional[str],
    execute_workflow: bool,
    *args: P.args,
    **kwargs: P.kwargs,
) -> "WorkflowHandle[R]": ...


@overload
def start_workflow(
    dbos: "DBOS",
    func: "Workflow[P, R]",
    queue_name: Optional[str],
    execute_workflow: bool,
    *args: P.args,
    **kwargs: P.kwargs,
) -> "WorkflowHandle[R]": ...


def start_workflow(
    dbos: "DBOS",
    func: "Workflow[P, Union[R, Coroutine[Any, Any, R]]]",
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

    status = _init_workflow(
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

    wf_status = status["status"]

    if (
        not execute_workflow
        or wf_status == WorkflowStatusString.ERROR.value
        or wf_status == WorkflowStatusString.SUCCESS.value
    ):
        dbos.logger.debug(
            f"Workflow {new_wf_id} already completed with status {wf_status}. Directly returning a workflow handle."
        )
        return WorkflowHandlePolling(new_wf_id, dbos)

    if fself is not None:
        future = dbos._executor.submit(
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
        future = dbos._executor.submit(
            cast(Callable[..., R], _execute_workflow_wthread),
            dbos,
            status,
            func,
            new_wf_ctx,
            *args,
            **kwargs,
        )
    return WorkflowHandleFuture(new_wf_id, future, dbos)


def workflow_wrapper(
    dbosreg: "DBOSRegistry",
    func: Callable[P, R],
    max_recovery_attempts: int = DEFAULT_MAX_RECOVERY_ATTEMPTS,
) -> Callable[P, R]:
    func.__orig_func = func  # type: ignore

    fi = get_or_create_func_info(func)
    fi.max_recovery_attempts = max_recovery_attempts

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> R:
        if dbosreg.dbos is None:
            raise DBOSException(
                f"Function {func.__name__} invoked before DBOS initialized"
            )
        dbos = dbosreg.dbos

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
        enterWorkflowCtxMgr = (
            EnterDBOSChildWorkflow if ctx and ctx.is_workflow() else EnterDBOSWorkflow
        )

        wfOutcome = Outcome[R].make(functools.partial(func, *args, **kwargs))

        def init_wf() -> Callable[[Callable[[], R]], R]:
            ctx = assert_current_dbos_context()  # Now the child ctx
            status = _init_workflow(
                dbos,
                ctx,
                inputs=inputs,
                wf_name=get_dbos_func_name(func),
                class_name=get_dbos_class_name(fi, func, args),
                config_name=get_config_name(fi, func, args),
                temp_wf_type=get_temp_workflow_type(func),
                max_recovery_attempts=max_recovery_attempts,
            )
            # TODO: maybe modify the parameters if they've been changed by `_init_workflow`
            dbos.logger.debug(
                f"Running workflow, id: {ctx.workflow_id}, name: {get_dbos_func_name(func)}"
            )

            return _get_wf_invoke_func(dbos, status)

        outcome = (
            wfOutcome.wrap(init_wf)
            .also(DBOSAssumeRole(rr))
            .also(enterWorkflowCtxMgr(attributes))
        )
        return outcome()  # type: ignore

    return wrapper


def decorate_workflow(
    reg: "DBOSRegistry", max_recovery_attempts: int
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def _workflow_decorator(func: Callable[P, R]) -> Callable[P, R]:
        wrapped_func = workflow_wrapper(reg, func, max_recovery_attempts)
        reg.register_wf_function(func.__qualname__, wrapped_func)
        return wrapped_func

    return _workflow_decorator


def decorate_transaction(
    dbosreg: "DBOSRegistry", isolation_level: "IsolationLevel" = "SERIALIZABLE"
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        def invoke_tx(*args: Any, **kwargs: Any) -> Any:
            if dbosreg.dbos is None:
                raise DBOSException(
                    f"Function {func.__name__} invoked before DBOS initialized"
                )
            dbos = dbosreg.dbos
            with dbos._app_db.sessionmaker() as session:
                attributes: TracedAttributes = {
                    "name": func.__name__,
                    "operationType": OperationType.TRANSACTION.value,
                }
                with EnterDBOSTransaction(session, attributes=attributes):
                    ctx = assert_current_dbos_context()
                    txn_output: TransactionResultInternal = {
                        "workflow_uuid": ctx.workflow_id,
                        "function_id": ctx.function_id,
                        "output": None,
                        "error": None,
                        "txn_snapshot": "",  # TODO: add actual snapshot
                        "executor_id": None,
                        "txn_id": None,
                    }
                    retry_wait_seconds = 0.001
                    backoff_factor = 1.5
                    max_retry_wait_seconds = 2.0
                    while True:
                        has_recorded_error = False
                        txn_error: Optional[Exception] = None
                        try:
                            with session.begin():
                                # This must be the first statement in the transaction!
                                session.connection(
                                    execution_options={
                                        "isolation_level": isolation_level
                                    }
                                )
                                # Check recorded output for OAOO
                                recorded_output = (
                                    ApplicationDatabase.check_transaction_execution(
                                        session,
                                        ctx.workflow_id,
                                        ctx.function_id,
                                    )
                                )
                                if recorded_output:
                                    dbos.logger.debug(
                                        f"Replaying transaction, id: {ctx.function_id}, name: {attributes['name']}"
                                    )
                                    if recorded_output["error"]:
                                        deserialized_error = (
                                            _serialization.deserialize_exception(
                                                recorded_output["error"]
                                            )
                                        )
                                        has_recorded_error = True
                                        raise deserialized_error
                                    elif recorded_output["output"]:
                                        return _serialization.deserialize(
                                            recorded_output["output"]
                                        )
                                    else:
                                        raise Exception(
                                            "Output and error are both None"
                                        )
                                else:
                                    dbos.logger.debug(
                                        f"Running transaction, id: {ctx.function_id}, name: {attributes['name']}"
                                    )

                                output = func(*args, **kwargs)
                                txn_output["output"] = _serialization.serialize(output)
                                assert (
                                    ctx.sql_session is not None
                                ), "Cannot find a database connection"
                                ApplicationDatabase.record_transaction_output(
                                    ctx.sql_session, txn_output
                                )
                                break
                        except DBAPIError as dbapi_error:
                            if dbapi_error.orig.sqlstate == "40001":  # type: ignore
                                # Retry on serialization failure
                                ctx.get_current_span().add_event(
                                    "Transaction Serialization Failure",
                                    {"retry_wait_seconds": retry_wait_seconds},
                                )
                                time.sleep(retry_wait_seconds)
                                retry_wait_seconds = min(
                                    retry_wait_seconds * backoff_factor,
                                    max_retry_wait_seconds,
                                )
                                continue
                            txn_error = dbapi_error
                            raise
                        except InvalidRequestError as invalid_request_error:
                            dbos.logger.error(
                                f"InvalidRequestError in transaction {func.__qualname__} \033[1m Hint: Do not call commit() or rollback() within a DBOS transaction.\033[0m"
                            )
                            txn_error = invalid_request_error
                            raise
                        except Exception as error:
                            txn_error = error
                            raise
                        finally:
                            # Don't record the error if it was already recorded
                            if txn_error and not has_recorded_error:
                                txn_output["error"] = (
                                    _serialization.serialize_exception(txn_error)
                                )
                                dbos._app_db.record_transaction_error(txn_output)
            return output

        if inspect.iscoroutinefunction(func):
            raise DBOSException(
                f"Function {func.__name__} is a coroutine function, but DBOS.transaction does not support coroutine functions"
            )

        fi = get_or_create_func_info(func)

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            rr: Optional[str] = check_required_roles(func, fi)
            # Entering transaction is allowed:
            #  In a workflow (that is not in a step already)
            #  Not in a workflow (we will start the single op workflow)
            ctx = get_local_dbos_context()
            if ctx and ctx.is_within_workflow():
                assert (
                    ctx.is_workflow()
                ), "Transactions must be called from within workflows"
                with DBOSAssumeRole(rr):
                    return invoke_tx(*args, **kwargs)
            else:
                tempwf = dbosreg.workflow_info_map.get("<temp>." + func.__qualname__)
                assert tempwf
                return tempwf(*args, **kwargs)

        def temp_wf(*args: Any, **kwargs: Any) -> Any:
            return wrapper(*args, **kwargs)

        wrapped_wf = workflow_wrapper(dbosreg, temp_wf)
        set_dbos_func_name(temp_wf, "<temp>." + func.__qualname__)
        set_temp_workflow_type(temp_wf, "transaction")
        dbosreg.register_wf_function(get_dbos_func_name(temp_wf), wrapped_wf)
        wrapper.__orig_func = temp_wf  # type: ignore

        return cast(F, wrapper)

    return decorator


def decorate_step(
    dbosreg: "DBOSRegistry",
    *,
    retries_allowed: bool = False,
    interval_seconds: float = 1.0,
    max_attempts: int = 3,
    backoff_rate: float = 2.0,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(func: Callable[P, R]) -> Callable[P, R]:

        def invoke_step(*args: Any, **kwargs: Any) -> Any:
            if dbosreg.dbos is None:
                raise DBOSException(
                    f"Function {func.__name__} invoked before DBOS initialized"
                )
            dbos = dbosreg.dbos

            attributes: TracedAttributes = {
                "name": func.__name__,
                "operationType": OperationType.STEP.value,
            }

            attempts = max_attempts if retries_allowed else 1
            max_retry_interval_seconds: float = 3600  # 1 Hour

            def on_exception(attempt: int, error: BaseException) -> float:
                dbos.logger.warning(
                    f"Step being automatically retried. (attempt {attempt} of {attempts}). {traceback.format_exc()}"
                )
                ctx = assert_current_dbos_context()
                ctx.get_current_span().add_event(
                    f"Step attempt {attempt} failed",
                    {
                        "error": str(error),
                        "retryIntervalSeconds": interval_seconds,
                    },
                )
                return min(
                    interval_seconds * (backoff_rate**attempt),
                    max_retry_interval_seconds,
                )

            def record_step_result(func: Callable[[], R]) -> R:
                ctx = assert_current_dbos_context()
                step_output: OperationResultInternal = {
                    "workflow_uuid": ctx.workflow_id,
                    "function_id": ctx.function_id,
                    "output": None,
                    "error": None,
                }

                try:
                    output = func()
                    step_output["output"] = _serialization.serialize(output)
                    return output
                except Exception as error:
                    step_output["error"] = _serialization.serialize_exception(error)
                    raise
                finally:
                    dbos._sys_db.record_operation_result(step_output)

            def check_existing_result() -> Union[NoResult, R]:
                ctx = assert_current_dbos_context()
                recorded_output = dbos._sys_db.check_operation_execution(
                    ctx.workflow_id, ctx.function_id
                )
                if recorded_output:
                    dbos.logger.debug(
                        f"Replaying step, id: {ctx.function_id}, name: {attributes['name']}"
                    )
                    if recorded_output["error"] is not None:
                        deserialized_error = _serialization.deserialize_exception(
                            recorded_output["error"]
                        )
                        raise deserialized_error
                    elif recorded_output["output"] is not None:
                        return cast(
                            R, _serialization.deserialize(recorded_output["output"])
                        )
                    else:
                        raise Exception("Output and error are both None")
                else:
                    dbos.logger.debug(
                        f"Running step, id: {ctx.function_id}, name: {attributes['name']}"
                    )
                    return NoResult()

            stepOutcome = Outcome[R].make(functools.partial(func, *args, **kwargs))
            if retries_allowed:
                stepOutcome = stepOutcome.retry(
                    max_attempts, on_exception, lambda i: DBOSMaxStepRetriesExceeded()
                )

            outcome = (
                stepOutcome.then(record_step_result)
                .intercept(check_existing_result)
                .also(EnterDBOSStep(attributes))
            )
            return outcome()

        fi = get_or_create_func_info(func)

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            rr: Optional[str] = check_required_roles(func, fi)
            # Entering step is allowed:
            #  In a step already, just call the original function directly.
            #  In a workflow (that is not in a step already)
            #  Not in a workflow (we will start the single op workflow)
            ctx = get_local_dbos_context()
            if ctx and ctx.is_step():
                # Call the original function directly
                return func(*args, **kwargs)
            if ctx and ctx.is_within_workflow():
                assert ctx.is_workflow(), "Steps must be called from within workflows"
                with DBOSAssumeRole(rr):
                    return invoke_step(*args, **kwargs)
            else:
                tempwf = dbosreg.workflow_info_map.get("<temp>." + func.__qualname__)
                assert tempwf
                return tempwf(*args, **kwargs)

        def temp_wf_sync(*args: Any, **kwargs: Any) -> Any:
            return wrapper(*args, **kwargs)

        async def temp_wf_async(*args: Any, **kwargs: Any) -> Any:
            return await wrapper(*args, **kwargs)

        # Other code in transact-py depends on the name of temporary workflow functions to be "temp_wf"
        # so set the name of both sync and async temporary workflow functions explicitly
        temp_wf_sync.__name__ = "temp_wf"
        temp_wf_async.__name__ = "temp_wf"

        temp_wf = temp_wf_async if inspect.iscoroutinefunction(func) else temp_wf_sync
        wrapped_wf = workflow_wrapper(dbosreg, temp_wf)
        set_dbos_func_name(temp_wf, "<temp>." + func.__qualname__)
        set_temp_workflow_type(temp_wf, "step")
        dbosreg.register_wf_function(get_dbos_func_name(temp_wf), wrapped_wf)
        wrapper.__orig_func = temp_wf  # type: ignore

        return cast(Callable[P, R], wrapper)

    return decorator


def send(
    dbos: "DBOS", destination_id: str, message: Any, topic: Optional[str] = None
) -> None:
    def do_send(destination_id: str, message: Any, topic: Optional[str]) -> None:
        attributes: TracedAttributes = {
            "name": "send",
        }
        with EnterDBOSStep(attributes):
            ctx = assert_current_dbos_context()
            dbos._sys_db.send(
                ctx.workflow_id,
                ctx.curr_step_function_id,
                destination_id,
                message,
                topic,
            )

    ctx = get_local_dbos_context()
    if ctx and ctx.is_within_workflow():
        assert ctx.is_workflow(), "send() must be called from within a workflow"
        return do_send(destination_id, message, topic)
    else:
        wffn = dbos._registry.workflow_info_map.get(TEMP_SEND_WF_NAME)
        assert wffn
        wffn(destination_id, message, topic)


def recv(dbos: "DBOS", topic: Optional[str] = None, timeout_seconds: float = 60) -> Any:
    cur_ctx = get_local_dbos_context()
    if cur_ctx is not None:
        # Must call it within a workflow
        assert cur_ctx.is_workflow(), "recv() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "recv",
        }
        with EnterDBOSStep(attributes):
            ctx = assert_current_dbos_context()
            ctx.function_id += 1  # Reserve for the sleep
            timeout_function_id = ctx.function_id
            return dbos._sys_db.recv(
                ctx.workflow_id,
                ctx.curr_step_function_id,
                timeout_function_id,
                topic,
                timeout_seconds,
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("recv() must be called from within a workflow")


def set_event(dbos: "DBOS", key: str, value: Any) -> None:
    cur_ctx = get_local_dbos_context()
    if cur_ctx is not None:
        # Must call it within a workflow
        assert (
            cur_ctx.is_workflow()
        ), "set_event() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "set_event",
        }
        with EnterDBOSStep(attributes):
            ctx = assert_current_dbos_context()
            dbos._sys_db.set_event(
                ctx.workflow_id, ctx.curr_step_function_id, key, value
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("set_event() must be called from within a workflow")


def get_event(
    dbos: "DBOS", workflow_id: str, key: str, timeout_seconds: float = 60
) -> Any:
    cur_ctx = get_local_dbos_context()
    if cur_ctx is not None and cur_ctx.is_within_workflow():
        # Call it within a workflow
        assert (
            cur_ctx.is_workflow()
        ), "get_event() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "get_event",
        }
        with EnterDBOSStep(attributes):
            ctx = assert_current_dbos_context()
            ctx.function_id += 1
            timeout_function_id = ctx.function_id
            caller_ctx: GetEventWorkflowContext = {
                "workflow_uuid": ctx.workflow_id,
                "function_id": ctx.curr_step_function_id,
                "timeout_function_id": timeout_function_id,
            }
            return dbos._sys_db.get_event(workflow_id, key, timeout_seconds, caller_ctx)
    else:
        # Directly call it outside of a workflow
        return dbos._sys_db.get_event(workflow_id, key, timeout_seconds)
