from __future__ import annotations

import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from logging import Logger
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    List,
    Literal,
    Optional,
    Protocol,
    Type,
    TypeVar,
)

from opentelemetry.trace import Span

from dbos.core import (
    TEMP_SEND_WF_NAME,
    _communicator,
    _execute_workflow_uuid,
    _get_event,
    _recv,
    _send,
    _set_event,
    _start_workflow,
    _transaction,
    _workflow_wrapper,
    _WorkflowHandlePolling,
)
from dbos.decorators import classproperty
from dbos.recovery import recover_pending_workflows, startup_recovery_thread
from dbos.registrations import (
    DBOSClassInfo,
    get_or_create_class_info,
    set_dbos_func_name,
)
from dbos.roles import default_required_roles, required_roles
from dbos.scheduler.scheduler import ScheduledWorkflow, scheduled

from .tracer import dbos_tracer

if TYPE_CHECKING:
    from fastapi import FastAPI
    from .fastapi import Request

from sqlalchemy.orm import Session

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec, TypeAlias
else:
    from typing import ParamSpec, TypeAlias

from dbos.admin_sever import AdminServer
from dbos.context import (
    EnterDBOSCommunicator,
    TracedAttributes,
    assert_current_dbos_context,
    get_local_dbos_context,
)
from dbos.error import DBOSNonExistentWorkflowError

from .application_database import ApplicationDatabase
from .dbos_config import ConfigFile, load_config, set_env_vars
from .logger import config_logger, dbos_logger, init_logger
from .system_database import SystemDatabase

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


IsolationLevel = Literal[
    "SERIALIZABLE",
    "REPEATABLE READ",
    "READ COMMITTED",
]


class DBOS:
    def __init__(
        self, fastapi: Optional["FastAPI"] = None, config: Optional[ConfigFile] = None
    ) -> None:
        init_logger()
        if config is None:
            config = load_config()
        config_logger(config)
        set_env_vars(config)
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
            self.executor.submit(startup_recovery_thread, self, workflow_ids)

        # Listen to notifications
        self.executor.submit(self.sys_db._notification_listener)

        # Register send_stub as a workflow
        def send_temp_workflow(
            destination_uuid: str, message: Any, topic: Optional[str]
        ) -> None:
            self.send(destination_uuid, message, topic)

        temp_send_wf = _workflow_wrapper(self, send_temp_workflow)
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

    def _register_wf_function(self, name: str, wrapped_func: F) -> None:
        self.workflow_info_map[name] = wrapped_func

    def _workflow_decorator(self, func: F) -> F:
        wrapped_func = _workflow_wrapper(self, func)
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
        return _start_workflow(self, func, *args, **kwargs)

    def retrieve_workflow(
        self, workflow_uuid: str, existing_workflow: bool = True
    ) -> WorkflowHandle[R]:
        if existing_workflow:
            stat = self.get_workflow_status(workflow_uuid)
            if stat is None:
                raise DBOSNonExistentWorkflowError(workflow_uuid)
        return _WorkflowHandlePolling(workflow_uuid, self)

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

    def transaction(
        self, isolation_level: IsolationLevel = "SERIALIZABLE"
    ) -> Callable[[F], F]:
        return _transaction(self, isolation_level)

    # Mirror the CommunicatorConfig from TS. However, we disable retries by default.
    def communicator(
        self,
        *,
        retries_allowed: bool = False,
        interval_seconds: float = 1.0,
        max_attempts: int = 3,
        backoff_rate: float = 2.0,
    ) -> Callable[[F], F]:
        return _communicator(
            self,
            retries_allowed=retries_allowed,
            interval_seconds=interval_seconds,
            max_attempts=max_attempts,
            backoff_rate=backoff_rate,
        )

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

    def dbos_class(self) -> Callable[[Type[Any]], Type[Any]]:
        def create_class_info(cls: Type[Any]) -> Type[Any]:
            ci = get_or_create_class_info(cls)
            self.register_class(cls, ci)
            return cls

        return create_class_info

    def default_required_roles(
        self, roles: List[str]
    ) -> Callable[[Type[Any]], Type[Any]]:
        return default_required_roles(self, roles)

    def required_roles(self, roles: List[str]) -> Callable[[F], F]:
        return required_roles(roles)

    def scheduled(self, cron: str) -> Callable[[ScheduledWorkflow], ScheduledWorkflow]:
        return scheduled(self, cron)

    def send(
        self, destination_uuid: str, message: Any, topic: Optional[str] = None
    ) -> None:
        return _send(self, destination_uuid, message, topic)

    def recv(self, topic: Optional[str] = None, timeout_seconds: float = 60) -> Any:
        return _recv(self, topic, timeout_seconds)

    def sleep(self, seconds: float) -> None:
        attributes: TracedAttributes = {
            "name": "sleep",
        }
        if seconds <= 0:
            return
        with EnterDBOSCommunicator(attributes) as ctx:
            self.sys_db.sleep(ctx.workflow_uuid, ctx.curr_comm_function_id, seconds)

    def set_event(self, key: str, value: Any) -> None:
        return _set_event(self, key, value)

    def get_event(
        self, workflow_uuid: str, key: str, timeout_seconds: float = 60
    ) -> Any:
        return _get_event(self, workflow_uuid, key, timeout_seconds)

    def execute_workflow_uuid(self, workflow_uuid: str) -> WorkflowHandle[Any]:
        """
        This function is used to execute a workflow by a UUID for recovery.
        """
        return _execute_workflow_uuid(self, workflow_uuid)

    def recover_pending_workflows(
        self, executor_ids: List[str] = ["local"]
    ) -> List[WorkflowHandle[Any]]:
        """
        Find all PENDING workflows and execute them.
        """
        return recover_pending_workflows(self, executor_ids)

    @classproperty
    def logger(cls) -> Logger:
        return dbos_logger  # TODO get from context if appropriate...

    @classproperty
    def sql_session(cls) -> Session:
        ctx = assert_current_dbos_context()
        assert (
            ctx.is_transaction()
        ), "sql_session is only available within a transaction."
        rv = ctx.sql_session
        assert rv
        return rv

    @classproperty
    def workflow_id(cls) -> str:
        ctx = assert_current_dbos_context()
        assert (
            ctx.is_within_workflow()
        ), "workflow_id is only available within a workflow, transaction, or communicator."
        return ctx.workflow_uuid

    @classproperty
    def parent_workflow_id(cls) -> str:
        ctx = assert_current_dbos_context()
        assert (
            ctx.is_within_workflow()
        ), "parent_workflow_id is only available within a workflow."
        return ctx.parent_workflow_uuid

    @classproperty
    def span(cls) -> Span:
        ctx = assert_current_dbos_context()
        return ctx.get_current_span()

    @classproperty
    def request(cls) -> Optional["Request"]:
        ctx = assert_current_dbos_context()
        return ctx.request


@dataclass
class WorkflowStatus:
    workflow_uuid: str
    status: str
    name: str
    class_name: Optional[str]
    config_name: Optional[str]
    authenticated_user: Optional[str]
    assumed_role: Optional[str]
    authenticatedRoles: Optional[List[str]]
    recovery_attempts: Optional[int]


class WorkflowHandle(Generic[R], Protocol):
    def __init__(self, workflow_uuid: str) -> None: ...

    workflow_uuid: str

    def get_workflow_uuid(self) -> str: ...
    def get_result(self) -> R: ...
    def get_status(self) -> WorkflowStatus: ...


class DBOSConfiguredInstance:
    def __init__(self, config_name: str, dbos: Optional[DBOS]) -> None:
        self.config_name = config_name
        if dbos is not None:
            dbos.register_instance(self)
