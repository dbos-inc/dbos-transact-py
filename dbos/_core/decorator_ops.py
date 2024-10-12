import asyncio
import time
import traceback
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar, cast

from .. import utils
from ..application_database import ApplicationDatabase, TransactionResultInternal
from ..context import (
    DBOSAssumeRole,
    EnterDBOSChildWorkflow,
    EnterDBOSStep,
    EnterDBOSTransaction,
    EnterDBOSWorkflow,
    OperationType,
    TracedAttributes,
    assert_current_dbos_context,
    get_local_dbos_context,
)
from ..error import DBOSException, DBOSMaxStepRetriesExceeded
from ..registrations import (
    DEFAULT_MAX_RECOVERY_ATTEMPTS,
    get_config_name,
    get_dbos_class_name,
    get_dbos_func_name,
    get_or_create_func_info,
    get_temp_workflow_type,
    set_dbos_func_name,
    set_temp_workflow_type,
)
from ..roles import check_required_roles
from .types import OperationResultInternal, WorkflowInputs
from .workflow import execute_workflow_async, execute_workflow_sync, init_workflow

if TYPE_CHECKING:
    from ..dbos import _DBOSRegistry, IsolationLevel

from sqlalchemy.exc import DBAPIError

# these are duped in dbos.py
F = TypeVar("F", bound=Callable[..., Any])


def workflow(reg: "_DBOSRegistry", max_recovery_attempts: int) -> Callable[[F], F]:
    def _workflow_decorator(func: F) -> F:
        wrapped_func = workflow_wrapper(reg, func, max_recovery_attempts)
        reg.register_wf_function(func.__qualname__, wrapped_func)
        return wrapped_func

    return _workflow_decorator


def workflow_wrapper(
    dbosreg: "_DBOSRegistry",
    func: F,
    max_recovery_attempts: int = DEFAULT_MAX_RECOVERY_ATTEMPTS,
) -> F:
    func.__orig_func = func  # type: ignore

    fi = get_or_create_func_info(func)
    fi.max_recovery_attempts = max_recovery_attempts

    # @wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
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
        with enterWorkflowCtxMgr(attributes), DBOSAssumeRole(rr):
            ctx = assert_current_dbos_context()  # Now the child ctx
            status = asyncio.run(
                init_workflow(
                    dbos,
                    ctx,
                    inputs=inputs,
                    wf_name=get_dbos_func_name(func),
                    class_name=get_dbos_class_name(fi, func, args),
                    config_name=get_config_name(fi, func, args),
                    temp_wf_type=get_temp_workflow_type(func),
                )
            )

            return execute_workflow_sync(dbos, status, func, *args, **kwargs)

    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
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
        with enterWorkflowCtxMgr(attributes), DBOSAssumeRole(rr):
            ctx = assert_current_dbos_context()  # Now the child ctx
            status = await init_workflow(
                dbos,
                ctx,
                inputs=inputs,
                wf_name=get_dbos_func_name(func),
                class_name=get_dbos_class_name(fi, func, args),
                config_name=get_config_name(fi, func, args),
                temp_wf_type=get_temp_workflow_type(func),
                max_recovery_attempts=max_recovery_attempts,
            )

            dbos.logger.debug(
                f"Running workflow, id: {ctx.workflow_id}, name: {get_dbos_func_name(func)}"
            )
            return await execute_workflow_async(dbos, status, func, *args, **kwargs)

    wrapper = wraps(func)(
        async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    )
    wrapped_func = cast(F, wrapper)
    return wrapped_func


def transaction(
    dbosreg: "_DBOSRegistry", isolation_level: "IsolationLevel" = "SERIALIZABLE"
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        def invoke_tx_sync(*args: Any, **kwargs: Any) -> Any:
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
                with EnterDBOSTransaction(session, attributes=attributes) as ctx:
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
                                            utils.deserialize_exception(
                                                recorded_output["error"]
                                            )
                                        )
                                        has_recorded_error = True
                                        raise deserialized_error
                                    elif recorded_output["output"]:
                                        return utils.deserialize(
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
                                txn_output["output"] = utils.serialize(output)
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
                            raise
                        except Exception as error:
                            # Don't record the error if it was already recorded
                            if not has_recorded_error:
                                txn_output["error"] = utils.serialize_exception(error)
                                dbos._app_db.record_transaction_error(txn_output)
                            raise
            return output

        async def invoke_tx_async(*args: Any, **kwargs: Any) -> Any:
            if dbosreg.dbos is None:
                raise DBOSException(
                    f"Function {func.__name__} invoked before DBOS initialized"
                )
            dbos = dbosreg.dbos
            async with dbos._app_db.async_sessionmaker() as session:
                attributes: TracedAttributes = {
                    "name": func.__name__,
                    "operationType": OperationType.TRANSACTION.value,
                }
                with EnterDBOSTransaction(session, attributes=attributes) as ctx:
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
                        try:
                            async with session.begin():
                                # This must be the first statement in the transaction!
                                await session.connection(
                                    execution_options={
                                        "isolation_level": isolation_level
                                    }
                                )
                                # Check recorded output for OAOO
                                recorded_output = await ApplicationDatabase.check_transaction_execution_async(
                                    session,
                                    ctx.workflow_id,
                                    ctx.function_id,
                                )
                                if recorded_output:
                                    if recorded_output["error"]:
                                        deserialized_error = (
                                            utils.deserialize_exception(
                                                recorded_output["error"]
                                            )
                                        )
                                        has_recorded_error = True
                                        raise deserialized_error
                                    elif recorded_output["output"]:
                                        return utils.deserialize(
                                            recorded_output["output"]
                                        )
                                    else:
                                        raise Exception(
                                            "Output and error are both None"
                                        )
                                output = await func(*args, **kwargs)
                                txn_output["output"] = utils.serialize(output)
                                assert (
                                    ctx.async_sql_session is not None
                                ), "Cannot find a database connection"
                                await ApplicationDatabase.record_transaction_output_async(
                                    ctx.async_sql_session, txn_output
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
                            raise
                        except Exception as error:
                            # Don't record the error if it was already recorded
                            if not has_recorded_error:
                                txn_output["error"] = utils.serialize_exception(error)
                                await dbos._app_db.record_transaction_error_async(
                                    txn_output
                                )
                            raise
            return output

        fi = get_or_create_func_info(func)

        def wrapper_sync(*args: Any, **kwargs: Any) -> Any:
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
                    return invoke_tx_sync(*args, **kwargs)
            else:
                tempwf = dbosreg.workflow_info_map.get("<temp>." + func.__qualname__)
                assert tempwf
                return tempwf(*args, **kwargs)

        async def wrapper_async(*args: Any, **kwargs: Any) -> Any:
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
                    return await invoke_tx_async(*args, **kwargs)
            else:
                tempwf = dbosreg.workflow_info_map.get("<temp>." + func.__qualname__)
                assert tempwf
                return await tempwf(*args, **kwargs)

        wrapper = wraps(func)(
            wrapper_async if asyncio.iscoroutinefunction(func) else wrapper_sync
        )

        def temp_wf_sync(*args: Any, **kwargs: Any) -> Any:
            return wrapper_sync(*args, **kwargs)

        async def temp_wf_async(*args: Any, **kwargs: Any) -> Any:
            return await wrapper_async(*args, **kwargs)

        temp_wf = temp_wf_async if asyncio.iscoroutinefunction(func) else temp_wf_sync

        wrapped_wf = workflow_wrapper(dbosreg, temp_wf)
        set_dbos_func_name(temp_wf, "<temp>." + func.__qualname__)
        set_temp_workflow_type(temp_wf, "transaction")
        dbosreg.register_wf_function(get_dbos_func_name(temp_wf), wrapped_wf)
        wrapper.__orig_func = temp_wf  # type: ignore

        return cast(F, wrapper)

    return decorator


def step(
    dbosreg: "_DBOSRegistry",
    *,
    retries_allowed: bool = False,
    interval_seconds: float = 1.0,
    max_attempts: int = 3,
    backoff_rate: float = 2.0,
) -> Callable[[F], F]:
    def decorator(func: F) -> F:

        def invoke_step_sync(*args: Any, **kwargs: Any) -> Any:
            if dbosreg.dbos is None:
                raise DBOSException(
                    f"Function {func.__name__} invoked before DBOS initialized"
                )
            dbos = dbosreg.dbos

            attributes: TracedAttributes = {
                "name": func.__name__,
                "operationType": OperationType.STEP.value,
            }
            with EnterDBOSStep(attributes) as ctx:
                step_output: OperationResultInternal = {
                    "workflow_uuid": ctx.workflow_id,
                    "function_id": ctx.function_id,
                    "output": None,
                    "error": None,
                }
                recorded_output = asyncio.run(
                    dbos._sys_db.check_operation_execution(
                        ctx.workflow_id, ctx.function_id
                    )
                )
                if recorded_output:
                    dbos.logger.debug(
                        f"Replaying step, id: {ctx.function_id}, name: {attributes['name']}"
                    )
                    if recorded_output["error"] is not None:
                        deserialized_error = utils.deserialize_exception(
                            recorded_output["error"]
                        )
                        raise deserialized_error
                    elif recorded_output["output"] is not None:
                        return utils.deserialize(recorded_output["output"])
                    else:
                        raise Exception("Output and error are both None")
                else:
                    dbos.logger.debug(
                        f"Running step, id: {ctx.function_id}, name: {attributes['name']}"
                    )

                output = None
                error = None
                local_max_attempts = max_attempts if retries_allowed else 1
                max_retry_interval_seconds: float = 3600  # 1 Hour
                local_interval_seconds = interval_seconds
                for attempt in range(1, local_max_attempts + 1):
                    try:
                        output = func(*args, **kwargs)
                        step_output["output"] = utils.serialize(output)
                        error = None
                        break
                    except Exception as err:
                        error = err
                        if retries_allowed:
                            dbos.logger.warning(
                                f"Step being automatically retried. (attempt {attempt} of {local_max_attempts}). {traceback.format_exc()}"
                            )
                            ctx.get_current_span().add_event(
                                f"Step attempt {attempt} failed",
                                {
                                    "error": str(error),
                                    "retryIntervalSeconds": local_interval_seconds,
                                },
                            )
                            if attempt == local_max_attempts:
                                error = DBOSMaxStepRetriesExceeded()
                            else:
                                time.sleep(local_interval_seconds)
                                local_interval_seconds = min(
                                    local_interval_seconds * backoff_rate,
                                    max_retry_interval_seconds,
                                )

                step_output["error"] = (
                    utils.serialize_exception(error) if error is not None else None
                )
                asyncio.run(dbos._sys_db.record_operation_result(step_output))

                if error is not None:
                    raise error
                return output

        async def invoke_step_async(*args: Any, **kwargs: Any) -> Any:
            if dbosreg.dbos is None:
                raise DBOSException(
                    f"Function {func.__name__} invoked before DBOS initialized"
                )
            dbos = dbosreg.dbos

            attributes: TracedAttributes = {
                "name": func.__name__,
                "operationType": OperationType.STEP.value,
            }
            with EnterDBOSStep(attributes) as ctx:
                step_output: OperationResultInternal = {
                    "workflow_uuid": ctx.workflow_id,
                    "function_id": ctx.function_id,
                    "output": None,
                    "error": None,
                }
                recorded_output = await dbos._sys_db.check_operation_execution(
                    ctx.workflow_id, ctx.function_id
                )
                if recorded_output:
                    if recorded_output["error"] is not None:
                        deserialized_error = utils.deserialize_exception(
                            recorded_output["error"]
                        )
                        raise deserialized_error
                    elif recorded_output["output"] is not None:
                        return utils.deserialize(recorded_output["output"])
                    else:
                        raise Exception("Output and error are both None")
                output = None
                error = None
                local_max_attempts = max_attempts if retries_allowed else 1
                max_retry_interval_seconds: float = 3600  # 1 Hour
                local_interval_seconds = interval_seconds
                for attempt in range(1, local_max_attempts + 1):
                    try:
                        output = await func(*args, **kwargs)
                        step_output["output"] = utils.serialize(output)
                        error = None
                        break
                    except Exception as err:
                        error = err
                        if retries_allowed:
                            dbos.logger.warning(
                                f"Step being automatically retried. (attempt {attempt} of {local_max_attempts}). {traceback.format_exc()}"
                            )
                            ctx.get_current_span().add_event(
                                f"Step attempt {attempt} failed",
                                {
                                    "error": str(error),
                                    "retryIntervalSeconds": local_interval_seconds,
                                },
                            )
                            if attempt == local_max_attempts:
                                error = DBOSMaxStepRetriesExceeded()
                            else:
                                time.sleep(local_interval_seconds)
                                local_interval_seconds = min(
                                    local_interval_seconds * backoff_rate,
                                    max_retry_interval_seconds,
                                )

                step_output["error"] = (
                    utils.serialize_exception(error) if error is not None else None
                )
                await dbos._sys_db.record_operation_result(step_output)

                if error is not None:
                    raise error
                return output

        fi = get_or_create_func_info(func)

        def wrapper_sync(*args: Any, **kwargs: Any) -> Any:
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
                    return invoke_step_sync(*args, **kwargs)
            else:
                tempwf = dbosreg.workflow_info_map.get("<temp>." + func.__qualname__)
                assert tempwf
                return tempwf(*args, **kwargs)

        async def wrapper_async(*args: Any, **kwargs: Any) -> Any:
            rr: Optional[str] = check_required_roles(func, fi)
            # Entering step is allowed:
            #  In a step already, just call the original function directly.
            #  In a workflow (that is not in a step already)
            #  Not in a workflow (we will start the single op workflow)
            ctx = get_local_dbos_context()
            if ctx and ctx.is_step():
                # Call the original function directly
                return await func(*args, **kwargs)
            if ctx and ctx.is_within_workflow():
                assert ctx.is_workflow(), "Steps must be called from within workflows"
                with DBOSAssumeRole(rr):
                    return await invoke_step_async(*args, **kwargs)
            else:
                tempwf = dbosreg.workflow_info_map.get("<temp>." + func.__qualname__)
                assert tempwf
                return await tempwf(*args, **kwargs)

        wrapper = wraps(func)(
            wrapper_async if asyncio.iscoroutinefunction(func) else wrapper_sync
        )

        def temp_wf_sync(*args: Any, **kwargs: Any) -> Any:
            return wrapper(*args, **kwargs)

        async def temp_wf_async(*args: Any, **kwargs: Any) -> Any:
            return await wrapper_async(*args, **kwargs)

        temp_wf = temp_wf_async if asyncio.iscoroutinefunction(func) else temp_wf_sync
        wrapped_wf = workflow_wrapper(dbosreg, temp_wf)
        set_dbos_func_name(temp_wf, "<temp>." + func.__qualname__)
        set_temp_workflow_type(temp_wf, "step")
        dbosreg.register_wf_function(get_dbos_func_name(temp_wf), wrapped_wf)
        wrapper.__orig_func = temp_wf  # type: ignore

        return cast(F, wrapper)

    return decorator
