from __future__ import annotations

import inspect
import os
import sys
import threading
import time
import traceback
import types
from concurrent.futures import Future, ThreadPoolExecutor
from enum import Enum
from functools import wraps
from logging import Logger
from types import FunctionType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypedDict,
    TypeVar,
    cast,
    overload,
)

from opentelemetry.trace import Span

from dbos.scheduler.scheduler import ScheduledWorkflow, scheduler_loop

from .tracer import dbos_tracer

if TYPE_CHECKING:
    from fastapi import FastAPI

from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec, TypeAlias
else:
    from typing import ParamSpec, TypeAlias

import dbos.utils as utils
from dbos.admin_sever import AdminServer
from dbos.context import (
    DBOSAssumeRole,
    DBOSContext,
    DBOSContextEnsure,
    DBOSContextSwap,
    EnterDBOSChildWorkflow,
    EnterDBOSCommunicator,
    EnterDBOSTransaction,
    EnterDBOSWorkflow,
    OperationType,
    SetWorkflowRecovery,
    SetWorkflowUUID,
    TracedAttributes,
    assert_current_dbos_context,
    get_local_dbos_context,
)
from dbos.error import (
    DBOSCommunicatorMaxRetriesExceededError,
    DBOSException,
    DBOSNonExistentWorkflowError,
    DBOSNotAuthorizedError,
    DBOSRecoveryError,
    DBOSWorkflowConflictUUIDError,
    DBOSWorkflowFunctionNotFoundError,
)
from dbos.workflow import WorkflowHandle, WorkflowStatus

from .application_database import ApplicationDatabase, TransactionResultInternal
from .dbos_config import ConfigFile, load_config
from .logger import config_logger, dbos_logger
from .system_database import (
    GetEventWorkflowContext,
    OperationResultInternal,
    SystemDatabase,
    WorkflowInputs,
    WorkflowStatusInternal,
)

if TYPE_CHECKING:
    from .fastapi import Request

# Most DBOS functions are just any callable F, so decorators / wrappers work on F
# There are cases where the parameters P and return value R should be separate
#   Such as for start_workflow, which will return WorkflowHandle[R]
#   In those cases, use something like Workflow[P,R]
F = TypeVar("F", bound=Callable[..., Any])

P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values


class DBOSCallProtocol(Protocol[P, R]):
    __name__: str
    __qualname__: str

    def __call__(*args: P.args, **kwargs: P.kwargs) -> R: ...


Workflow: TypeAlias = DBOSCallProtocol[P, R]

TEMP_SEND_WF_NAME = "<temp>.temp_send_workflow"


def get_dbos_func_name(f: Any) -> str:
    if hasattr(f, "dbos_function_name"):
        return str(getattr(f, "dbos_function_name"))
    if hasattr(f, "__qualname__"):
        return str(getattr(f, "__qualname__"))
    return "<unknown>"


def set_dbos_func_name(f: Any, name: str) -> None:
    setattr(f, "dbos_function_name", name)


class WorkflowInputContext(TypedDict):
    workflow_uuid: str


G = TypeVar("G")  # A generic type for ClassPropertyDescriptor getters


class ClassPropertyDescriptor(Generic[G]):
    def __init__(self, fget: Callable[..., G]) -> None:
        self.fget = fget

    def __get__(self, obj: Any, objtype: Optional[Any] = None) -> G:
        if objtype is None:
            objtype = type(obj)
        if self.fget is None:
            raise AttributeError("unreadable attribute")
        return self.fget(objtype)


def classproperty(func: Callable[..., G]) -> ClassPropertyDescriptor[G]:
    return ClassPropertyDescriptor(func)


class WorkflowHandleFuture(WorkflowHandle[R]):

    def __init__(self, workflow_uuid: str, future: Future[R], dbos: DBOS):
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


class PollingWorkflowHandle(WorkflowHandle[R]):

    def __init__(self, workflow_uuid: str, dbos: DBOS):
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


IsolationLevel = Literal[
    "SERIALIZABLE",
    "REPEATABLE READ",
    "READ COMMITTED",
]


class DBOSClassInfo:
    def __init__(self) -> None:
        self.def_required_roles: Optional[List[str]] = None


class DBOSFuncType(Enum):
    Unknown = 0
    Bare = 1
    Static = 2
    Class = 3
    Instance = 4


class DBOSFuncInfo:
    def __init__(self) -> None:
        self.class_info: Optional[DBOSClassInfo] = None
        self.func_type: DBOSFuncType = DBOSFuncType.Unknown
        self.required_roles: Optional[List[str]] = None


def get_or_create_class_info(cls: Type[Any]) -> DBOSClassInfo:
    if hasattr(cls, "dbos_class_decorator_info"):
        ci: DBOSClassInfo = getattr(cls, "dbos_class_decorator_info")
        return ci
    ci = DBOSClassInfo()
    setattr(cls, "dbos_class_decorator_info", ci)

    # Tell all DBOS functions about this
    for name, attribute in cls.__dict__.items():
        # Check if the attribute is a function or method
        if isinstance(attribute, (FunctionType, staticmethod, classmethod)):
            dbft = DBOSFuncType.Unknown
            if isinstance(attribute, staticmethod):
                attribute = attribute.__func__
                dbft = DBOSFuncType.Static
            elif isinstance(attribute, classmethod):
                attribute = attribute.__func__
                dbft = DBOSFuncType.Class
            elif isinstance(attribute, FunctionType):
                dbft = DBOSFuncType.Instance

            # Walk down the __wrapped__ chain
            wrapped = attribute
            while True:
                # Annotate based on the type
                if hasattr(wrapped, "dbos_func_decorator_info"):
                    fi: DBOSFuncInfo = getattr(wrapped, "dbos_func_decorator_info")
                    fi.class_info = ci
                    fi.func_type = dbft

                if not hasattr(wrapped, "__wrapped__"):
                    break
                wrapped = wrapped.__wrapped__

    return ci


def get_func_info(func: Callable[..., Any]) -> Optional[DBOSFuncInfo]:
    while True:
        if hasattr(func, "dbos_func_decorator_info"):
            fi: DBOSFuncInfo = getattr(func, "dbos_func_decorator_info")
            return fi
        if not hasattr(func, "__wrapped__"):
            break
        func = func.__wrapped__
    return None


def get_or_create_func_info(func: Callable[..., Any]) -> DBOSFuncInfo:
    fi = get_func_info(func)
    if fi is not None:
        return fi

    fi = DBOSFuncInfo()
    setattr(func, "dbos_func_decorator_info", fi)
    return fi


def get_class_info(cls: Type[Any]) -> Optional[DBOSClassInfo]:
    if hasattr(cls, "dbos_class_decorator_info"):
        ci: DBOSClassInfo = getattr(cls, "dbos_class_decorator_info")
        return ci
    return None


def get_class_info_for_func(fi: Optional[DBOSFuncInfo]) -> Optional[DBOSClassInfo]:
    if fi and fi.class_info:
        return fi.class_info

    # Bare function or function on something else
    return None


def get_config_name(
    fi: Optional[DBOSFuncInfo], func: Callable[..., Any], args: Tuple[Any, ...]
) -> Optional[str]:
    if fi and fi.func_type != DBOSFuncType.Unknown and len(args) > 0:
        if fi.func_type == DBOSFuncType.Instance:
            first_arg = args[0]
            if hasattr(first_arg, "config_name"):
                iname: str = getattr(first_arg, "config_name")
                return str(iname)
            else:
                raise Exception(
                    "Function target appears to be a class instance, but does not have `config_name` set"
                )
        return None

    # Check for improperly-registered functions
    if len(args) > 0:
        first_arg = args[0]
        if isinstance(first_arg, type):
            raise Exception(
                "Function target appears to be a class, but is not properly registered"
            )
        else:
            # Check if the function signature has "self" as the first parameter name
            #   This is not 100% reliable but it is better than nothing for detecting footguns
            sig = inspect.signature(func)
            parameters = list(sig.parameters.values())
            if parameters and parameters[0].name == "self":
                raise Exception(
                    "Function target appears to be a class instance, but is not properly registered"
                )

    # Bare function or function on something else
    return None


def get_dbos_class_name(
    fi: Optional[DBOSFuncInfo], func: Callable[..., Any], args: Tuple[Any, ...]
) -> Optional[str]:
    if fi and fi.func_type != DBOSFuncType.Unknown and len(args) > 0:
        if fi.func_type == DBOSFuncType.Instance:
            first_arg = args[0]
            return str(first_arg.__class__.__name__)
        if fi.func_type == DBOSFuncType.Class:
            first_arg = args[0]
            return str(first_arg.__name__)
        return None

    # Check for improperly-registered functions
    if len(args) > 0:
        first_arg = args[0]
        if isinstance(first_arg, type):
            raise Exception(
                "Function target appears to be a class, but is not properly registered"
            )
        else:
            # Check if the function signature has "self" as the first parameter name
            #   This is not 100% reliable but it is better than nothing for detecting footguns
            sig = inspect.signature(func)
            parameters = list(sig.parameters.values())
            if parameters and parameters[0].name == "self":
                raise Exception(
                    "Function target appears to be a class instance, but is not properly registered"
                )

    # Bare function or function on something else
    return None


class DBOS:
    def __init__(
        self, fastapi: Optional["FastAPI"] = None, config: Optional[ConfigFile] = None
    ) -> None:
        if config is None:
            config = load_config()
        config_logger(config)
        dbos_tracer.config(config)
        dbos_logger.info("Initializing DBOS")
        self.config = config
        self.sys_db = SystemDatabase(config)
        self.app_db = ApplicationDatabase(config)
        self.workflow_info_map: dict[str, Workflow[..., Any]] = {}
        self.class_info_map: dict[str, type] = {}
        self.instance_info_map: dict[str, object] = {}
        self.executor = ThreadPoolExecutor(max_workers=64)
        self.admin_server = AdminServer(dbos=self)
        self.stop_events: List[threading.Event] = []
        if fastapi is not None:
            from dbos.fastapi import setup_fastapi_middleware

            setup_fastapi_middleware(fastapi)
        if not os.environ.get("DBOS__VMID"):
            workflow_ids = self.sys_db.get_pending_workflows("local")
            self.executor.submit(self._startup_recovery_thread, workflow_ids)

        # Listen to notifications
        self.executor.submit(self.sys_db._notification_listener)

        # Register send_stub as a workflow
        def send_temp_workflow(
            destination_uuid: str, message: Any, topic: Optional[str]
        ) -> None:
            self.send(destination_uuid, message, topic)

        temp_send_wf = self._workflow_wrapper(send_temp_workflow)
        set_dbos_func_name(send_temp_workflow, TEMP_SEND_WF_NAME)
        self._register_wf_function(TEMP_SEND_WF_NAME, temp_send_wf)
        dbos_logger.info("DBOS initialized")
        for handler in dbos_logger.handlers:
            handler.flush()

    def destroy(self) -> None:
        for event in self.stop_events:
            event.set()
        self.sys_db.destroy()
        self.app_db.destroy()
        self.admin_server.stop()
        self.executor.shutdown(cancel_futures=True)

    def _workflow_wrapper(self, func: F) -> F:
        func.__orig_func = func  # type: ignore

        fi = get_or_create_func_info(func)

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            rr: Optional[str] = DBOS._check_required_roles(func, args, fi)
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
                    status = self._init_workflow(
                        ctx,
                        inputs=inputs,
                        wf_name=get_dbos_func_name(func),
                        class_name=get_dbos_class_name(fi, func, args),
                        config_name=get_config_name(fi, func, args),
                    )

                    return self._execute_workflow(status, func, *args, **kwargs)
            else:
                with EnterDBOSWorkflow(attributes), DBOSAssumeRole(rr):
                    ctx = assert_current_dbos_context()
                    status = self._init_workflow(
                        ctx,
                        inputs=inputs,
                        wf_name=get_dbos_func_name(func),
                        class_name=get_dbos_class_name(fi, func, args),
                        config_name=get_config_name(fi, func, args),
                    )

                    return self._execute_workflow(status, func, *args, **kwargs)

        wrapped_func = cast(F, wrapper)
        return wrapped_func

    def _register_wf_function(self, name: str, wrapped_func: F) -> None:
        self.workflow_info_map[name] = wrapped_func

    def _workflow_decorator(self, func: F) -> F:
        wrapped_func = self._workflow_wrapper(func)
        self._register_wf_function(func.__qualname__, wrapped_func)
        return wrapped_func

    def workflow(self) -> Callable[[F], F]:
        return self._workflow_decorator

    def start_workflow(
        self,
        func: Workflow[P, R],
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

        func = cast(Workflow[P, R], func.__orig_func)  # type: ignore

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

        new_wf_ctx = (
            DBOSContext()
            if cur_ctx is None
            else cur_ctx.create_child() if cur_ctx.is_within_workflow() else cur_ctx
        )
        new_wf_ctx.id_assigned_for_next_workflow = new_wf_ctx.assign_workflow_id()
        new_wf_uuid = new_wf_ctx.id_assigned_for_next_workflow

        gin_args: Tuple[Any, ...] = args
        if fself is not None:
            gin_args = (fself,)

        status = self._init_workflow(
            new_wf_ctx,
            inputs=inputs,
            wf_name=get_dbos_func_name(func),
            class_name=get_dbos_class_name(fi, func, gin_args),
            config_name=get_config_name(fi, func, gin_args),
        )

        if fself is not None:
            future = self.executor.submit(
                cast(Callable[..., R], self._execute_workflow_wthread),
                status,
                func,
                new_wf_ctx,
                fself,
                *args,
                **kwargs,
            )
        else:
            future = self.executor.submit(
                cast(Callable[..., R], self._execute_workflow_wthread),
                status,
                func,
                new_wf_ctx,
                *args,
                **kwargs,
            )
        return WorkflowHandleFuture(new_wf_uuid, future, self)

    def retrieve_workflow(
        self, workflow_uuid: str, existing_workflow: bool = True
    ) -> WorkflowHandle[R]:
        if existing_workflow:
            stat = self.get_workflow_status(workflow_uuid)
            if stat is None:
                raise DBOSNonExistentWorkflowError(workflow_uuid)
        return PollingWorkflowHandle(workflow_uuid, self)

    def get_workflow_status(self, workflow_uuid: str) -> Optional[WorkflowStatus]:
        ctx = get_local_dbos_context()
        if ctx and ctx.is_within_workflow():
            ctx.function_id += 1
            stat = self.sys_db.get_workflow_status_within_wf(
                workflow_uuid, ctx.workflow_uuid, ctx.function_id
            )
        else:
            stat = self.sys_db.get_workflow_status(workflow_uuid)
        if stat is None:
            return None

        return WorkflowStatus(
            workflow_uuid=workflow_uuid,
            status=stat["status"],
            name=stat["name"],
            recovery_attempts=stat["recovery_attempts"],
            class_name=stat["class_name"],
            config_name=stat["config_name"],
            authenticated_user=None,
            assumed_role=None,
            authenticatedRoles=None,
        )

    def _init_workflow(
        self,
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
            "request": (
                utils.serialize(ctx.request) if ctx.request is not None else None
            ),
            "recovery_attempts": None,
        }
        self.sys_db.update_workflow_status(status, False, ctx.in_recovery)

        # If we have an instance name, the first arg is the instance and do not serialize
        if class_name is not None:
            inputs = {"args": inputs["args"][1:], "kwargs": inputs["kwargs"]}
        self.sys_db.update_workflow_inputs(wfid, utils.serialize(inputs))

        return status

    def _execute_workflow_wthread(
        self,
        status: WorkflowStatusInternal,
        func: Workflow[P, R],
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
                return self._execute_workflow(status, func, *args, **kwargs)

    def _execute_workflow(
        self,
        status: WorkflowStatusInternal,
        func: Workflow[P, R],
        *args: Any,
        **kwargs: Any,
    ) -> R:
        try:
            output = func(*args, **kwargs)
        except DBOSWorkflowConflictUUIDError:
            # Retrieve the workflow handle and wait for the result.
            wf_handle: WorkflowHandle[R] = self.retrieve_workflow(DBOS.workflow_id)
            output = wf_handle.get_result()
            return output
        except Exception as error:
            status["status"] = "ERROR"
            status["error"] = utils.serialize(error)
            self.sys_db.update_workflow_status(status)
            raise error

        status["status"] = "SUCCESS"
        status["output"] = utils.serialize(output)
        self.sys_db.update_workflow_status(status)
        return output

    def transaction(
        self, isolation_level: IsolationLevel = "SERIALIZABLE"
    ) -> Callable[[F], F]:
        def decorator(func: F) -> F:
            def invoke_tx(*args: Any, **kwargs: Any) -> Any:
                with self.app_db.sessionmaker() as session:
                    attributes: TracedAttributes = {
                        "name": func.__name__,
                        "operationType": OperationType.TRANSACTION.value,
                    }
                    with EnterDBOSTransaction(session, attributes=attributes) as ctx:
                        txn_output: TransactionResultInternal = {
                            "workflow_uuid": ctx.workflow_uuid,
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
                                            ctx.workflow_uuid,
                                            ctx.function_id,
                                        )
                                    )
                                    if recorded_output:
                                        if recorded_output["error"]:
                                            deserialized_error = utils.deserialize(
                                                recorded_output["error"]
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
                                    output = func(*args, **kwargs)
                                    txn_output["output"] = utils.serialize(output)
                                    assert ctx.sql_session is not None
                                    ApplicationDatabase.record_transaction_output(
                                        ctx.sql_session, txn_output
                                    )
                                    break
                            except DBAPIError as dbapi_error:
                                if dbapi_error.orig.pgcode == "40001":  # type: ignore
                                    # Retry on serialization failure
                                    DBOS.span.add_event(
                                        "Transaction Serialization Failure",
                                        {"retry_wait_seconds": retry_wait_seconds},
                                    )
                                    time.sleep(retry_wait_seconds)
                                    retry_wait_seconds = min(
                                        retry_wait_seconds * backoff_factor,
                                        max_retry_wait_seconds,
                                    )
                                    continue
                                raise dbapi_error
                            except Exception as error:
                                # Don't record the error if it was already recorded
                                if not has_recorded_error:
                                    txn_output["error"] = utils.serialize(error)
                                    self.app_db.record_transaction_error(txn_output)
                                raise error
                return output

            fi = get_or_create_func_info(func)

            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                rr: Optional[str] = DBOS._check_required_roles(func, args, fi)
                # Entering transaction is allowed:
                #  In a workflow (that is not in a transaction / comm already)
                #  Not in a workflow (we will start the single op workflow)
                ctx = get_local_dbos_context()
                if ctx and ctx.is_within_workflow():
                    assert ctx.is_workflow()
                    with DBOSAssumeRole(rr):
                        return invoke_tx(*args, **kwargs)
                else:
                    tempwf = self.workflow_info_map.get("<temp>." + func.__qualname__)
                    assert tempwf
                    return tempwf(*args, **kwargs)

            def temp_wf(*args: Any, **kwargs: Any) -> Any:
                return wrapper(*args, **kwargs)

            wrapped_wf = self._workflow_wrapper(temp_wf)
            set_dbos_func_name(temp_wf, "<temp>." + func.__qualname__)
            self._register_wf_function(get_dbos_func_name(temp_wf), wrapped_wf)

            return cast(F, wrapper)

        return decorator

    # Mirror the CommunicatorConfig from TS. However, we disable retries by default.
    def communicator(
        self,
        *,
        retries_allowed: bool = False,
        interval_seconds: float = 1.0,
        max_attempts: int = 3,
        backoff_rate: float = 2.0,
    ) -> Callable[[F], F]:
        def decorator(func: F) -> F:

            def invoke_comm(*args: Any, **kwargs: Any) -> Any:

                attributes: TracedAttributes = {
                    "name": func.__name__,
                    "operationType": OperationType.COMMUNICATOR.value,
                }
                with EnterDBOSCommunicator(attributes) as ctx:
                    comm_output: OperationResultInternal = {
                        "workflow_uuid": ctx.workflow_uuid,
                        "function_id": ctx.function_id,
                        "output": None,
                        "error": None,
                    }
                    recorded_output = self.sys_db.check_operation_execution(
                        ctx.workflow_uuid, ctx.function_id
                    )
                    if recorded_output:
                        if recorded_output["error"] is not None:
                            deserialized_error = utils.deserialize(
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
                            output = func(*args, **kwargs)
                            comm_output["output"] = utils.serialize(output)
                            error = None
                            break
                        except Exception as err:
                            error = err
                            if retries_allowed:
                                dbos_logger.warning(
                                    f"Communicator being automatically retried. (attempt {attempt} of {local_max_attempts}). {traceback.format_exc()}"
                                )
                                DBOS.span.add_event(
                                    f"Communicator attempt {attempt} failed",
                                    {
                                        "error": str(error),
                                        "retryIntervalSeconds": local_interval_seconds,
                                    },
                                )
                                if attempt == local_max_attempts:
                                    error = DBOSCommunicatorMaxRetriesExceededError()
                                else:
                                    time.sleep(local_interval_seconds)
                                    local_interval_seconds = min(
                                        local_interval_seconds * backoff_rate,
                                        max_retry_interval_seconds,
                                    )

                    comm_output["error"] = (
                        utils.serialize(error) if error is not None else None
                    )
                    self.sys_db.record_operation_result(comm_output)
                    if error is not None:
                        raise error
                    return output

            fi = get_or_create_func_info(func)

            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                rr: Optional[str] = DBOS._check_required_roles(func, args, fi)
                # Entering communicator is allowed:
                #  In a workflow (that is not in a transaction / comm already)
                #  Not in a workflow (we will start the single op workflow)
                ctx = get_local_dbos_context()
                if ctx and ctx.is_within_workflow():
                    assert ctx.is_workflow()
                    with DBOSAssumeRole(rr):
                        return invoke_comm(*args, **kwargs)
                else:
                    tempwf = self.workflow_info_map.get("<temp>." + func.__qualname__)
                    assert tempwf
                    return tempwf(*args, **kwargs)

            def temp_wf(*args: Any, **kwargs: Any) -> Any:
                return wrapper(*args, **kwargs)

            wrapped_wf = self._workflow_wrapper(temp_wf)
            set_dbos_func_name(temp_wf, "<temp>." + func.__qualname__)
            self._register_wf_function(get_dbos_func_name(temp_wf), wrapped_wf)

            return cast(F, wrapper)

        return decorator

    def register_instance(self, inst: object) -> None:
        config_name = getattr(inst, "config_name")
        class_name = inst.__class__.__name__
        fn = f"{class_name}/{config_name}"
        if fn in self.instance_info_map:
            if self.instance_info_map[fn] is not inst:
                raise Exception(
                    f"Duplicate instance registration for class '{class_name}' instance '{config_name}'"
                )
        else:
            self.instance_info_map[fn] = inst

    def register_class(self, cls: type, ci: DBOSClassInfo) -> None:
        class_name = cls.__name__
        if class_name in self.class_info_map:
            if self.class_info_map[class_name] is not cls:
                raise Exception(f"Duplicate type registration for class '{class_name}'")
        else:
            self.class_info_map[class_name] = cls

    def default_required_roles(
        self, roles: List[str]
    ) -> Callable[[Type[Any]], Type[Any]]:
        def set_roles(cls: Type[Any]) -> Type[Any]:
            ci = get_or_create_class_info(cls)
            self.register_class(cls, ci)
            ci.def_required_roles = roles
            return cls

        return set_roles

    def dbos_class(self) -> Callable[[Type[Any]], Type[Any]]:
        def create_class_info(cls: Type[Any]) -> Type[Any]:
            ci = get_or_create_class_info(cls)
            self.register_class(cls, ci)
            return cls

        return create_class_info

    @staticmethod
    def _check_required_roles(
        func: Callable[..., Any], args: Tuple[Any, ...], fi: Optional[DBOSFuncInfo]
    ) -> Optional[str]:
        # Check required roles
        required_roles: Optional[List[str]] = None
        # First, we need to know if this has class info and func info
        ci = get_class_info_for_func(fi)
        if ci is not None:
            required_roles = ci.def_required_roles
        if fi and fi.required_roles is not None:
            required_roles = fi.required_roles

        if required_roles is None or len(required_roles) == 0:
            return None  # Nothing to check

        ctx = get_local_dbos_context()
        if ctx is None or ctx.authenticated_roles is None:
            raise DBOSNotAuthorizedError(
                f"Function {func.__name__} requires a role, but was called in a context without authentication information"
            )

        for r in required_roles:
            if r in ctx.authenticated_roles:
                return r

        raise DBOSNotAuthorizedError(
            f"Function {func.__name__} has required roles, but user is not authenticated for any of them"
        )

    def required_roles(
        self,
        roles: List[str],
    ) -> Callable[[F], F]:
        def set_roles(func: F) -> F:
            fi = get_or_create_func_info(func)
            fi.required_roles = roles

            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                rr: Optional[str] = DBOS._check_required_roles(func, args, fi)
                with DBOSAssumeRole(rr):
                    return func(*args, **kwargs)

            return cast(F, wrapper)

        return set_roles

    def send(
        self, destination_uuid: str, message: Any, topic: Optional[str] = None
    ) -> None:
        def do_send(destination_uuid: str, message: Any, topic: Optional[str]) -> None:
            attributes: TracedAttributes = {
                "name": "send",
            }
            with EnterDBOSCommunicator(attributes) as ctx:
                self.sys_db.send(
                    ctx.workflow_uuid,
                    ctx.curr_comm_function_id,
                    destination_uuid,
                    message,
                    topic,
                )

        ctx = get_local_dbos_context()
        if ctx and ctx.is_within_workflow():
            assert ctx.is_workflow()
            return do_send(destination_uuid, message, topic)
        else:
            wffn = self.workflow_info_map.get(TEMP_SEND_WF_NAME)
            assert wffn
            wffn(destination_uuid, message, topic)

    def recv(self, topic: Optional[str] = None, timeout_seconds: float = 60) -> Any:
        cur_ctx = get_local_dbos_context()
        if cur_ctx is not None:
            # Must call it within a workflow
            assert cur_ctx.is_workflow()
            attributes: TracedAttributes = {
                "name": "recv",
            }
            with EnterDBOSCommunicator(attributes) as ctx:
                ctx.function_id += 1  # Reserve for the sleep
                timeout_function_id = ctx.function_id
                return self.sys_db.recv(
                    ctx.workflow_uuid,
                    ctx.curr_comm_function_id,
                    timeout_function_id,
                    topic,
                    timeout_seconds,
                )
        else:
            # Cannot call it from outside of a workflow
            raise DBOSException("recv() must be called within a workflow")

    def sleep(self, seconds: float) -> None:
        attributes: TracedAttributes = {
            "name": "sleep",
        }
        if seconds <= 0:
            return
        with EnterDBOSCommunicator(attributes) as ctx:
            self.sys_db.sleep(ctx.workflow_uuid, ctx.curr_comm_function_id, seconds)

    def set_event(self, key: str, value: Any) -> None:
        cur_ctx = get_local_dbos_context()
        if cur_ctx is not None:
            # Must call it within a workflow
            assert cur_ctx.is_workflow()
            attributes: TracedAttributes = {
                "name": "set_event",
            }
            with EnterDBOSCommunicator(attributes) as ctx:
                self.sys_db.set_event(
                    ctx.workflow_uuid, ctx.curr_comm_function_id, key, value
                )
        else:
            # Cannot call it from outside of a workflow
            raise DBOSException("set_event() must be called within a workflow")

    def get_event(
        self, workflow_uuid: str, key: str, timeout_seconds: float = 60
    ) -> Any:
        cur_ctx = get_local_dbos_context()
        if cur_ctx is not None and cur_ctx.is_within_workflow():
            # Call it within a workflow
            assert cur_ctx.is_workflow()
            attributes: TracedAttributes = {
                "name": "get_event",
            }
            with EnterDBOSCommunicator(attributes) as ctx:
                ctx.function_id += 1
                timeout_function_id = ctx.function_id
                caller_ctx: GetEventWorkflowContext = {
                    "workflow_uuid": ctx.workflow_uuid,
                    "function_id": ctx.curr_comm_function_id,
                    "timeout_function_id": timeout_function_id,
                }
                return self.sys_db.get_event(
                    workflow_uuid, key, timeout_seconds, caller_ctx
                )
        else:
            # Directly call it outside of a workflow
            return self.sys_db.get_event(workflow_uuid, key, timeout_seconds)

    def scheduled(self, cron: str) -> Callable[[ScheduledWorkflow], ScheduledWorkflow]:
        def decorator(func: ScheduledWorkflow) -> ScheduledWorkflow:
            stop_event = threading.Event()
            self.stop_events.append(stop_event)
            self.executor.submit(scheduler_loop, func, cron, stop_event)
            return func

        return decorator

    def execute_workflow_uuid(self, workflow_uuid: str) -> WorkflowHandle[Any]:
        """
        This function is used to execute a workflow by a UUID for recovery.
        """
        status = self.sys_db.get_workflow_status(workflow_uuid)
        if not status:
            raise DBOSRecoveryError(workflow_uuid, "Workflow status not found")
        inputs = self.sys_db.get_workflow_inputs(workflow_uuid)
        if not inputs:
            raise DBOSRecoveryError(workflow_uuid, "Workflow inputs not found")
        wf_func = self.workflow_info_map.get(status["name"], None)
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
                if iname not in self.instance_info_map:
                    raise DBOSWorkflowFunctionNotFoundError(
                        workflow_uuid,
                        f"Cannot execute workflow because instance '{iname}' is not registered",
                    )
                with SetWorkflowUUID(workflow_uuid):
                    return self.start_workflow(
                        wf_func,
                        self.instance_info_map[iname],
                        *inputs["args"],
                        **inputs["kwargs"],
                    )
            elif status["class_name"] is not None:
                class_name = status["class_name"]
                if class_name not in self.class_info_map:
                    raise DBOSWorkflowFunctionNotFoundError(
                        workflow_uuid,
                        f"Cannot execute workflow because class '{class_name}' is not registered",
                    )
                with SetWorkflowUUID(workflow_uuid):
                    return self.start_workflow(
                        wf_func,
                        self.class_info_map[class_name],
                        *inputs["args"],
                        **inputs["kwargs"],
                    )
            else:
                with SetWorkflowUUID(workflow_uuid):
                    return self.start_workflow(
                        wf_func, *inputs["args"], **inputs["kwargs"]
                    )

    def recover_pending_workflows(
        self, executor_ids: List[str] = ["local"]
    ) -> List[WorkflowHandle[Any]]:
        """
        Find all PENDING workflows and execute them.
        """
        workflow_handles: List[WorkflowHandle[Any]] = []
        for executor_id in executor_ids:
            if executor_id == "local" and os.environ.get("DBOS__VMID"):
                dbos_logger.debug(
                    f"Skip local recovery because it's running in a VM: {os.environ.get('DBOS__VMID')}"
                )
            dbos_logger.debug(
                f"Recovering pending workflows for executor: {executor_id}"
            )
            workflow_ids = self.sys_db.get_pending_workflows(executor_id)
            dbos_logger.debug(f"Pending workflows: {workflow_ids}")

            for workflowID in workflow_ids:
                with SetWorkflowRecovery():
                    handle = self.execute_workflow_uuid(workflowID)
                workflow_handles.append(handle)

        dbos_logger.info("Recovered pending workflows")
        return workflow_handles

    @classproperty
    def logger(cls) -> Logger:
        return dbos_logger  # TODO get from context if appropriate...

    @classproperty
    def sql_session(cls) -> Session:
        ctx = assert_current_dbos_context()
        assert ctx.is_transaction()
        rv = ctx.sql_session
        assert rv
        return rv

    @classproperty
    def workflow_id(cls) -> str:
        ctx = assert_current_dbos_context()
        assert ctx.is_within_workflow()
        return ctx.workflow_uuid

    @classproperty
    def parent_workflow_id(cls) -> str:
        ctx = assert_current_dbos_context()
        assert ctx.is_within_workflow()
        return ctx.parent_workflow_uuid

    @classproperty
    def span(cls) -> Span:
        ctx = assert_current_dbos_context()
        return ctx.get_current_span()

    @classproperty
    def request(cls) -> Optional["Request"]:
        ctx = assert_current_dbos_context()
        return ctx.request

    def _startup_recovery_thread(self, workflow_ids: List[str]) -> None:
        """
        A background thread that attempts to recover local pending workflows on startup.
        """
        stop_event = threading.Event()
        self.stop_events.append(stop_event)
        while not stop_event.is_set() and len(workflow_ids) > 0:
            try:
                for workflowID in list(workflow_ids):
                    with SetWorkflowRecovery():
                        self.execute_workflow_uuid(workflowID)
                    workflow_ids.remove(workflowID)
            except DBOSWorkflowFunctionNotFoundError:
                time.sleep(1)
            except Exception as e:
                dbos_logger.error(
                    f"Exception encountered when recovering workflows: {repr(e)}"
                )
                raise e


class DBOSConfiguredInstance:
    def __init__(self, config_name: str, dbos: Optional[DBOS]) -> None:
        self.config_name = config_name
        if dbos is not None:
            dbos.register_instance(self)
