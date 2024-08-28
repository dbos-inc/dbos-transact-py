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
from dbos.recovery import _recover_pending_workflows, _startup_recovery_thread
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
from dbos.error import DBOSException, DBOSNonExistentWorkflowError

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

_dbos_global_instance: Optional[DBOS] = None


def get_dbos_instance() -> DBOS:
    # Currently get / init the global singleton
    #   (It could also check context?)
    return DBOS()


class DBOS:
    ### Lifecycles ###
    # We provide a singleton, created / accessed as `DBOS(args)`
    #  Access to the the singleton via `DBOS.<thing>`
    #
    # If an application wants to control lifecycle of DBOS via singleton:
    #  Create DBOS with `DBOS()`
    #   Use DBOS or the instance returned from DBOS()
    #  clear_global_instance() to get rid of it so that DBOS() returns a new one

    def __new__(
        cls: Type[DBOS],
        fastapi: Optional["FastAPI"] = None,
        config: Optional[ConfigFile] = None,
    ) -> DBOS:
        global _dbos_global_instance
        if _dbos_global_instance is None:
            _dbos_global_instance = super().__new__(cls)
            _dbos_global_instance.__init__(fastapi=fastapi, config=config)  # type: ignore
        return _dbos_global_instance

    @classmethod
    def clear_global_instance(cls) -> None:
        global _dbos_global_instance
        if _dbos_global_instance is not None:
            _dbos_global_instance.destroy()
        _dbos_global_instance = None

    def __init__(
        self, fastapi: Optional["FastAPI"] = None, config: Optional[ConfigFile] = None
    ) -> None:
        if hasattr(self, "_initialized") and self._initialized:
            return

        self._initialized: bool = True
        init_logger()
        if config is None:
            config = load_config()
        config_logger(config)
        set_env_vars(config)
        dbos_tracer.config(config)
        dbos_logger.info("Initializing DBOS")
        self.config: ConfigFile = config
        self._launched = False
        self._sys_db: Optional[SystemDatabase] = None
        self._app_db: Optional[ApplicationDatabase] = None
        self.workflow_info_map: dict[str, Workflow[..., Any]] = {}
        self.class_info_map: dict[str, type] = {}
        self.instance_info_map: dict[str, object] = {}
        self.executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=64)
        self._admin_server: Optional[AdminServer] = None
        self.stop_events: List[threading.Event] = []
        self.fastapi = fastapi
        if self.fastapi is not None:
            from dbos.fastapi import setup_fastapi_middleware

            setup_fastapi_middleware(self.fastapi)

        # Register send_stub as a workflow
        def send_temp_workflow(
            destination_uuid: str, message: Any, topic: Optional[str]
        ) -> None:
            self.send(destination_uuid, message, topic)

        temp_send_wf = _workflow_wrapper(self, send_temp_workflow)
        set_dbos_func_name(send_temp_workflow, TEMP_SEND_WF_NAME)
        self._register_wf_function(TEMP_SEND_WF_NAME, temp_send_wf)

    @property
    def sys_db(self) -> SystemDatabase:
        if self._sys_db is None:
            self.launch()
        if self._sys_db is None:
            raise DBOSException("System database not initialized")
        rv: SystemDatabase = self._sys_db
        return rv

    @property
    def app_db(self) -> ApplicationDatabase:
        if self._app_db is None:
            self.launch()
        if self._app_db is None:
            raise DBOSException("Application database not initialized")
        rv: ApplicationDatabase = self._app_db
        return rv

    @property
    def admin_server(self) -> AdminServer:
        if self._admin_server is None:
            self.launch()
        if self._admin_server is None:
            raise DBOSException("Admin server is not initialized")
        rv: AdminServer = self._admin_server
        return rv

    def launch(self) -> None:
        self._launched = True
        self._sys_db = SystemDatabase(self.config)
        self._app_db = ApplicationDatabase(self.config)
        self._admin_server = AdminServer(dbos=self)
        if not os.environ.get("DBOS__VMID"):
            workflow_ids = self.sys_db.get_pending_workflows("local")
            self.executor.submit(_startup_recovery_thread, self, workflow_ids)

        # Listen to notifications
        self.executor.submit(self.sys_db._notification_listener)

        dbos_logger.info("DBOS initialized")
        for handler in dbos_logger.handlers:
            handler.flush()

    def destroy(self) -> None:
        self._initialized = False
        for event in self.stop_events:
            event.set()
        if self._sys_db is not None:
            self._sys_db.destroy()
            self._sys_db = None
        if self._app_db is not None:
            self._app_db.destroy()
            self._app_db = None
        if self._admin_server is not None:
            self._admin_server.stop()
            self._admin_server = None
        # CB Should this be done before DBs are destroyed?
        self.executor.shutdown(cancel_futures=True)

    def _register_wf_function(self, name: str, wrapped_func: F) -> None:
        self.workflow_info_map[name] = wrapped_func

    def _workflow_decorator(self, func: F) -> F:
        wrapped_func = _workflow_wrapper(self, func)
        self._register_wf_function(func.__qualname__, wrapped_func)
        return wrapped_func

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

    def _create_class_info(self, cls: Type[Any]) -> Type[Any]:
        ci = get_or_create_class_info(cls)
        self.register_class(cls, ci)
        return cls

    # Decorators for DBOS functionality
    @classmethod
    def workflow(cls) -> Callable[[F], F]:
        return get_dbos_instance()._workflow_decorator

    @classmethod
    def transaction(
        cls, isolation_level: IsolationLevel = "SERIALIZABLE"
    ) -> Callable[[F], F]:
        return _transaction(get_dbos_instance(), isolation_level)

    # Mirror the CommunicatorConfig from TS. However, we disable retries by default.
    @classmethod
    def communicator(
        cls,
        *,
        retries_allowed: bool = False,
        interval_seconds: float = 1.0,
        max_attempts: int = 3,
        backoff_rate: float = 2.0,
    ) -> Callable[[F], F]:
        return _communicator(
            get_dbos_instance(),
            retries_allowed=retries_allowed,
            interval_seconds=interval_seconds,
            max_attempts=max_attempts,
            backoff_rate=backoff_rate,
        )

    @classmethod
    def dbos_class(cls) -> Callable[[Type[Any]], Type[Any]]:
        return get_dbos_instance()._create_class_info

    @classmethod
    def default_required_roles(
        cls, roles: List[str]
    ) -> Callable[[Type[Any]], Type[Any]]:
        return default_required_roles(get_dbos_instance(), roles)

    @classmethod
    def required_roles(cls, roles: List[str]) -> Callable[[F], F]:
        return required_roles(roles)

    @classmethod
    def scheduled(cls, cron: str) -> Callable[[ScheduledWorkflow], ScheduledWorkflow]:
        return scheduled(get_dbos_instance(), cron)

    @classmethod
    def start_workflow(
        cls,
        func: Workflow[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> WorkflowHandle[R]:
        return _start_workflow(get_dbos_instance(), func, *args, **kwargs)

    @classmethod
    def get_workflow_status(cls, workflow_uuid: str) -> Optional[WorkflowStatus]:
        ctx = get_local_dbos_context()
        if ctx and ctx.is_within_workflow():
            ctx.function_id += 1
            stat = get_dbos_instance().sys_db.get_workflow_status_within_wf(
                workflow_uuid, ctx.workflow_uuid, ctx.function_id
            )
        else:
            stat = get_dbos_instance().sys_db.get_workflow_status(workflow_uuid)
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

    @classmethod
    def retrieve_workflow(
        cls, workflow_uuid: str, existing_workflow: bool = True
    ) -> WorkflowHandle[R]:
        dbos = get_dbos_instance()
        if existing_workflow:
            stat = dbos.get_workflow_status(workflow_uuid)
            if stat is None:
                raise DBOSNonExistentWorkflowError(workflow_uuid)
        return _WorkflowHandlePolling(workflow_uuid, dbos)

    @classmethod
    def send(
        cls, destination_uuid: str, message: Any, topic: Optional[str] = None
    ) -> None:
        return _send(get_dbos_instance(), destination_uuid, message, topic)

    @classmethod
    def recv(cls, topic: Optional[str] = None, timeout_seconds: float = 60) -> Any:
        return _recv(get_dbos_instance(), topic, timeout_seconds)

    @classmethod
    def sleep(cls, seconds: float) -> None:
        attributes: TracedAttributes = {
            "name": "sleep",
        }
        if seconds <= 0:
            return
        with EnterDBOSCommunicator(attributes) as ctx:
            get_dbos_instance().sys_db.sleep(
                ctx.workflow_uuid, ctx.curr_comm_function_id, seconds
            )

    @classmethod
    def set_event(cls, key: str, value: Any) -> None:
        return _set_event(get_dbos_instance(), key, value)

    @classmethod
    def get_event(
        cls, workflow_uuid: str, key: str, timeout_seconds: float = 60
    ) -> Any:
        return _get_event(get_dbos_instance(), workflow_uuid, key, timeout_seconds)

    @classmethod
    def execute_workflow_uuid(cls, workflow_uuid: str) -> WorkflowHandle[Any]:
        """
        This function is used to execute a workflow by a UUID for recovery.
        """
        return _execute_workflow_uuid(get_dbos_instance(), workflow_uuid)

    @classmethod
    def recover_pending_workflows(
        cls, executor_ids: List[str] = ["local"]
    ) -> List[WorkflowHandle[Any]]:
        """
        Find all PENDING workflows and execute them.
        """
        return _recover_pending_workflows(get_dbos_instance(), executor_ids)

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
    def __init__(self, config_name: str, dbos: Optional[DBOS] = None) -> None:
        self.config_name = config_name
        if dbos is not None:
            assert isinstance(dbos, DBOS)
            dbos.register_instance(self)
        else:
            DBOS().register_instance(self)
