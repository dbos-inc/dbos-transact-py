from __future__ import annotations

import atexit
import json
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
    Tuple,
    Type,
    TypeVar,
    cast,
)

from opentelemetry.trace import Span

from dbos.core import (
    TEMP_SEND_WF_NAME,
    _execute_workflow_id,
    _get_event,
    _recv,
    _send,
    _set_event,
    _start_workflow,
    _step,
    _transaction,
    _workflow,
    _workflow_wrapper,
    _WorkflowHandlePolling,
)
from dbos.decorators import classproperty
from dbos.recovery import _recover_pending_workflows, _startup_recovery_thread
from dbos.registrations import (
    DBOSClassInfo,
    get_or_create_class_info,
    set_dbos_func_name,
    set_temp_workflow_type,
)
from dbos.roles import default_required_roles, required_roles
from dbos.scheduler.scheduler import ScheduledWorkflow, scheduled

from .tracer import dbos_tracer

if TYPE_CHECKING:
    from fastapi import FastAPI
    from dbos.kafka import KafkaConsumerWorkflow
    from .request import Request
    from flask import Flask

from sqlalchemy.orm import Session

from dbos.request import Request

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec, TypeAlias
else:
    from typing import ParamSpec, TypeAlias

from dbos.admin_sever import AdminServer
from dbos.context import (
    EnterDBOSStep,
    TracedAttributes,
    assert_current_dbos_context,
    get_local_dbos_context,
)
from dbos.error import DBOSException, DBOSNonExistentWorkflowError

from .application_database import ApplicationDatabase
from .dbos_config import ConfigFile, load_config, set_env_vars
from .logger import add_otlp_to_all_loggers, config_logger, dbos_logger, init_logger
from .system_database import SystemDatabase

# Most DBOS functions are just any callable F, so decorators / wrappers work on F
# There are cases where the parameters P and return value R should be separate
#   Such as for start_workflow, which will return WorkflowHandle[R]
#   In those cases, use something like Workflow[P,R]
F = TypeVar("F", bound=Callable[..., Any])

P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values

T = TypeVar("T")


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
_dbos_global_registry: Optional[_DBOSRegistry] = None


def _get_dbos_instance() -> DBOS:
    global _dbos_global_instance
    if _dbos_global_instance is not None:
        return _dbos_global_instance
    raise DBOSException("No DBOS was created yet")


def _get_or_create_dbos_registry() -> _DBOSRegistry:
    # Currently get / init the global registry
    global _dbos_global_registry
    if _dbos_global_registry is None:
        _dbos_global_registry = _DBOSRegistry()
    return _dbos_global_registry


_RegisteredJob = Tuple[
    threading.Event, Callable[..., Any], Tuple[Any, ...], dict[str, Any]
]


class _DBOSRegistry:
    def __init__(self) -> None:
        self.workflow_info_map: dict[str, Workflow[..., Any]] = {}
        self.class_info_map: dict[str, type] = {}
        self.instance_info_map: dict[str, object] = {}
        self.pollers: list[_RegisteredJob] = []
        self.dbos: Optional[DBOS] = None
        self.config: Optional[ConfigFile] = None

    def register_wf_function(self, name: str, wrapped_func: F) -> None:
        self.workflow_info_map[name] = wrapped_func

    def register_class(self, cls: type, ci: DBOSClassInfo) -> None:
        class_name = cls.__name__
        if class_name in self.class_info_map:
            if self.class_info_map[class_name] is not cls:
                raise Exception(f"Duplicate type registration for class '{class_name}'")
        else:
            self.class_info_map[class_name] = cls

    def create_class_info(self, cls: Type[T]) -> Type[T]:
        ci = get_or_create_class_info(cls)
        self.register_class(cls, ci)
        return cls

    def register_poller(
        self, evt: threading.Event, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> None:
        if self.dbos and self.dbos._launched:
            self.dbos.stop_events.append(evt)
            self.dbos.executor.submit(func, *args, **kwargs)
        else:
            self.pollers.append((evt, func, args, kwargs))

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


class DBOS:
    """
    Main access class for DBOS functionality.

    `DBOS` contains functions and properties for:
    1. Decorating classes, workflows, and steps
    2. Starting workflow functions
    3. Retrieving workflow status information
    4. Interacting with workflows via events and messages
    5. Accessing context, including the current user, request, SQL session, logger, and tracer

    """

    ### Lifecycles ###
    # We provide a singleton, created / accessed as `DBOS(args)`
    #  Access to the the singleton, or current context, via `DBOS.<thing>`
    #
    # If a DBOS decorator is used before there is a DBOS, the information gets
    #  put in _dbos_global_registry.  When the DBOS is finally created, it will
    #  get picked up.  Information can be added later.
    #
    # If an application wants to control lifecycle of DBOS via singleton:
    #  Create DBOS with `DBOS()`
    #   Use DBOS or the instance returned from DBOS()
    #  DBOS.destroy() to get rid of it so that DBOS() returns a new one

    def __new__(
        cls: Type[DBOS],
        *,
        config: Optional[ConfigFile] = None,
        fastapi: Optional["FastAPI"] = None,
        flask: Optional["Flask"] = None,
    ) -> DBOS:
        global _dbos_global_instance
        global _dbos_global_registry
        if _dbos_global_instance is None:
            if (
                _dbos_global_registry is not None
                and _dbos_global_registry.config is not None
            ):
                if config is not None and config is not _dbos_global_registry.config:
                    raise DBOSException(
                        f"DBOS configured multiple times with conflicting information"
                    )
                config = _dbos_global_registry.config
            _dbos_global_instance = super().__new__(cls)
            _dbos_global_instance.__init__(fastapi=fastapi, config=config, flask=flask)  # type: ignore
        else:
            if (config is not None and _dbos_global_instance.config is not config) or (
                _dbos_global_instance.fastapi is not fastapi
            ):
                raise DBOSException(
                    f"DBOS Initialized multiple times with conflicting configuration / fastapi information"
                )
        return _dbos_global_instance

    @classmethod
    def destroy(cls) -> None:
        global _dbos_global_instance
        global _dbos_global_registry
        if _dbos_global_instance is not None:
            _dbos_global_instance._destroy()
        _dbos_global_instance = None
        _dbos_global_registry = None

    def __init__(
        self,
        *,
        config: Optional[ConfigFile] = None,
        fastapi: Optional["FastAPI"] = None,
        flask: Optional["Flask"] = None,
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
        self._launched: bool = False
        self._sys_db: Optional[SystemDatabase] = None
        self._app_db: Optional[ApplicationDatabase] = None
        self._registry: _DBOSRegistry = _get_or_create_dbos_registry()
        self._registry.dbos = self
        self._admin_server: Optional[AdminServer] = None
        self.stop_events: List[threading.Event] = []
        self.fastapi: Optional["FastAPI"] = fastapi
        self.flask: Optional["Flask"] = flask
        self._executor: Optional[ThreadPoolExecutor] = None

        # If using FastAPI, set up middleware and lifecycle events
        if self.fastapi is not None:
            from fastapi.requests import Request as FARequest
            from fastapi.responses import JSONResponse

            async def dbos_error_handler(
                request: FARequest, gexc: Exception
            ) -> JSONResponse:
                exc: DBOSException = cast(DBOSException, gexc)
                status_code = 500
                if exc.status_code is not None:
                    status_code = exc.status_code
                return JSONResponse(
                    status_code=status_code,
                    content={
                        "message": str(exc.message),
                        "dbos_error_code": str(exc.dbos_error_code),
                        "dbos_error": str(exc.__class__.__name__),
                    },
                )

            self.fastapi.add_exception_handler(DBOSException, dbos_error_handler)

            from dbos.fastapi import setup_fastapi_middleware

            setup_fastapi_middleware(self.fastapi)
            self.fastapi.on_event("startup")(self._launch)
            self.fastapi.on_event("shutdown")(self._destroy)

        # If using Flask, set up middleware
        if self.flask is not None:
            from dbos.flask import setup_flask_middleware

            setup_flask_middleware(self.flask)

        # Register send_stub as a workflow
        def send_temp_workflow(
            destination_id: str, message: Any, topic: Optional[str]
        ) -> None:
            self.send(destination_id, message, topic)

        temp_send_wf = _workflow_wrapper(self._registry, send_temp_workflow)
        set_dbos_func_name(send_temp_workflow, TEMP_SEND_WF_NAME)
        set_temp_workflow_type(send_temp_workflow, "send")
        self._registry.register_wf_function(TEMP_SEND_WF_NAME, temp_send_wf)

        for handler in dbos_logger.handlers:
            handler.flush()

    @property
    def executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            raise DBOSException("Executor accessed before DBOS was launched")
        rv: ThreadPoolExecutor = self._executor
        return rv

    @property
    def sys_db(self) -> SystemDatabase:
        if self._sys_db is None:
            raise DBOSException("System database accessed before DBOS was launched")
        rv: SystemDatabase = self._sys_db
        return rv

    @property
    def app_db(self) -> ApplicationDatabase:
        if self._app_db is None:
            raise DBOSException(
                "Application database accessed before DBOS was launched"
            )
        rv: ApplicationDatabase = self._app_db
        return rv

    @property
    def admin_server(self) -> AdminServer:
        if self._admin_server is None:
            raise DBOSException("Admin server accessed before DBOS was launched")
        rv: AdminServer = self._admin_server
        return rv

    @classmethod
    def launch(cls) -> None:
        if _dbos_global_instance is not None:
            _dbos_global_instance._launch()

    def _launch(self) -> None:
        if self._launched:
            dbos_logger.warning(f"DBOS was already launched")
            return
        self._launched = True
        self._executor = ThreadPoolExecutor(max_workers=64)
        self._sys_db = SystemDatabase(self.config)
        self._app_db = ApplicationDatabase(self.config)
        self._admin_server = AdminServer(dbos=self)

        if not os.environ.get("DBOS__VMID"):
            workflow_ids = self.sys_db.get_pending_workflows("local")
            self.executor.submit(_startup_recovery_thread, self, workflow_ids)

        # Listen to notifications
        self.executor.submit(self.sys_db._notification_listener)

        # Start flush workflow buffers thread
        self.executor.submit(self.sys_db.flush_workflow_buffers)

        # Grab any pollers that were deferred and start them
        for evt, func, args, kwargs in self._registry.pollers:
            self.stop_events.append(evt)
            self.executor.submit(func, *args, **kwargs)
        self._registry.pollers = []

        dbos_logger.info("DBOS launched")

        # Flush handlers and add OTLP to all loggers if enabled
        # to enable their export in DBOS Cloud
        for handler in dbos_logger.handlers:
            handler.flush()
        add_otlp_to_all_loggers()

    def _destroy(self) -> None:
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
        # CB - This needs work, some things ought to stop before DBs are tossed out,
        #  on the other hand it hangs to move it
        if self._executor is not None:
            self._executor.shutdown(cancel_futures=True)
            self._executor = None

    @classmethod
    def register_instance(cls, inst: object) -> None:
        return _get_or_create_dbos_registry().register_instance(inst)

    # Decorators for DBOS functionality
    @classmethod
    def workflow(cls) -> Callable[[F], F]:
        """Decorate a function for use as a DBOS workflow."""
        return _workflow(_get_or_create_dbos_registry())

    @classmethod
    def transaction(
        cls, isolation_level: IsolationLevel = "SERIALIZABLE"
    ) -> Callable[[F], F]:
        """
        Decorate a function for use as a DBOS transaction.

        Args:
            isolation_level(IsolationLevel): Transaction isolation level

        """
        return _transaction(_get_or_create_dbos_registry(), isolation_level)

    @classmethod
    def step(
        cls,
        *,
        retries_allowed: bool = False,
        interval_seconds: float = 1.0,
        max_attempts: int = 3,
        backoff_rate: float = 2.0,
    ) -> Callable[[F], F]:
        """
        Decorate and configure a function for use as a DBOS step.

        Args:
            retries_allowed(bool): If true, enable retries on thrown exceptions
            interval_seconds(float): Time between retry attempts
            backoff_rate(float): Multiplier for exponentially increasing `interval_seconds` between retries
            max_attempts(int): Maximum number of retries before raising an exception

        """

        return _step(
            _get_or_create_dbos_registry(),
            retries_allowed=retries_allowed,
            interval_seconds=interval_seconds,
            max_attempts=max_attempts,
            backoff_rate=backoff_rate,
        )

    @classmethod
    def dbos_class(cls) -> Callable[[Type[T]], Type[T]]:
        """
        Decorate a class that contains DBOS member functions.

        All DBOS classes must be decorated, as this associates the class with
        its member functions.
        """

        return _get_or_create_dbos_registry().create_class_info

    @classmethod
    def default_required_roles(cls, roles: List[str]) -> Callable[[Type[T]], Type[T]]:
        """
        Decorate a class with the default list of required roles.

        The defaults can be overridden on a per-function basis with `required_roles`.

        Args:
            roles(List[str]): The list of roles to be used on class member functions

        """

        return default_required_roles(_get_or_create_dbos_registry(), roles)

    @classmethod
    def required_roles(cls, roles: List[str]) -> Callable[[F], F]:
        """
        Decorate a function with a list of required access roles.

        If the function is called from a context that does not provide at least one
        role from the list, a `DBOSNotAuthorizedError` exception will be raised.

        Args:
            roles(List[str]): The list of roles to be used on class member functions

        """

        return required_roles(roles)

    @classmethod
    def scheduled(cls, cron: str) -> Callable[[ScheduledWorkflow], ScheduledWorkflow]:
        """Decorate a workflow function with its invocation schedule."""

        return scheduled(_get_or_create_dbos_registry(), cron)

    @classmethod
    def kafka_consumer(
        cls, config: dict[str, Any], topics: list[str]
    ) -> Callable[[KafkaConsumerWorkflow], KafkaConsumerWorkflow]:
        """Decorate a function to be used as a Kafka consumer."""
        try:
            from dbos.kafka import kafka_consumer

            return kafka_consumer(_get_or_create_dbos_registry(), config, topics)
        except ModuleNotFoundError as e:
            raise DBOSException(
                f"{e.name} dependency not found. Please install {e.name} via your package manager."
            ) from e

    @classmethod
    def start_workflow(
        cls,
        func: Workflow[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> WorkflowHandle[R]:
        """Invoke a workflow function in the background, returning a handle to the ongoing execution."""
        return _start_workflow(_get_dbos_instance(), func, *args, **kwargs)

    @classmethod
    def get_workflow_status(cls, workflow_id: str) -> Optional[WorkflowStatus]:
        """Return the status of a workflow execution."""
        ctx = get_local_dbos_context()
        if ctx and ctx.is_within_workflow():
            ctx.function_id += 1
            stat = _get_dbos_instance().sys_db.get_workflow_status_within_wf(
                workflow_id, ctx.workflow_id, ctx.function_id
            )
        else:
            stat = _get_dbos_instance().sys_db.get_workflow_status(workflow_id)
        if stat is None:
            return None

        return WorkflowStatus(
            workflow_id=workflow_id,
            status=stat["status"],
            name=stat["name"],
            recovery_attempts=stat["recovery_attempts"],
            class_name=stat["class_name"],
            config_name=stat["config_name"],
            authenticated_user=stat["authenticated_user"],
            assumed_role=stat["assumed_role"],
            authenticated_roles=(
                json.loads(stat["authenticated_roles"])
                if stat["authenticated_roles"] is not None
                else None
            ),
        )

    @classmethod
    def retrieve_workflow(
        cls, workflow_id: str, existing_workflow: bool = True
    ) -> WorkflowHandle[R]:
        """Return a `WorkflowHandle` for a workflow execution."""
        dbos = _get_dbos_instance()
        if existing_workflow:
            stat = dbos.get_workflow_status(workflow_id)
            if stat is None:
                raise DBOSNonExistentWorkflowError(workflow_id)
        return _WorkflowHandlePolling(workflow_id, dbos)

    @classmethod
    def send(
        cls, destination_id: str, message: Any, topic: Optional[str] = None
    ) -> None:
        """Send a message to a workflow execution."""
        return _send(_get_dbos_instance(), destination_id, message, topic)

    @classmethod
    def recv(cls, topic: Optional[str] = None, timeout_seconds: float = 60) -> Any:
        """
        Receive a workflow message.

        This function is to be called from within a workflow.
        `recv` will return the message sent on `topic`, waiting if necessary.
        """
        return _recv(_get_dbos_instance(), topic, timeout_seconds)

    @classmethod
    def sleep(cls, seconds: float) -> None:
        """
        Sleep for the specified time (in seconds).

        It is important to use `DBOS.sleep` (as opposed to any other sleep) within workflows,
        as the `DBOS.sleep`s are durable and completed sleeps will be skipped during recovery.
        """

        attributes: TracedAttributes = {
            "name": "sleep",
        }
        if seconds <= 0:
            return
        with EnterDBOSStep(attributes) as ctx:
            _get_dbos_instance().sys_db.sleep(
                ctx.workflow_id, ctx.curr_step_function_id, seconds
            )

    @classmethod
    def set_event(cls, key: str, value: Any) -> None:
        """
        Set a workflow event.

        This function is to be called from within a workflow.

        `set_event` sets the `value` of `key` for the current workflow instance ID.
        This `value` can then be retrieved by other functions, using `get_event` below.

        Each workflow invocation should only call set_event once per `key`.

        Args:
            key(str): The event key / name within the workflow
            value(Any): A serializable value to associate with the key

        """
        return _set_event(_get_dbos_instance(), key, value)

    @classmethod
    def get_event(cls, workflow_id: str, key: str, timeout_seconds: float = 60) -> Any:
        """
        Return the `value` of a workflow event, waiting for it to occur if necessary.

        `get_event` waits for a corresponding `set_event` by the workflow with ID `workflow_id` with the same `key`.

        Args:
            workflow_id(str): The workflow instance ID that is expected to call `set_event` on `key`
            key(str): The event key / name within the workflow
            timeout_seconds(float): The amount of time to wait, in case `set_event` has not yet been called byt the workflow

        """
        return _get_event(_get_dbos_instance(), workflow_id, key, timeout_seconds)

    @classmethod
    def execute_workflow_id(cls, workflow_id: str) -> WorkflowHandle[Any]:
        """Execute a workflow by ID (for recovery)."""
        return _execute_workflow_id(_get_dbos_instance(), workflow_id)

    @classmethod
    def recover_pending_workflows(
        cls, executor_ids: List[str] = ["local"]
    ) -> List[WorkflowHandle[Any]]:
        """Find all PENDING workflows and execute them."""
        return _recover_pending_workflows(_get_dbos_instance(), executor_ids)

    @classproperty
    def logger(cls) -> Logger:
        """Return the DBOS `Logger` for the current context."""
        return dbos_logger  # TODO get from context if appropriate...

    @classproperty
    def config(cls) -> ConfigFile:
        """Return the DBOS `ConfigFile` for the current context."""
        global _dbos_global_instance
        if _dbos_global_instance is not None:
            return _dbos_global_instance.config
        reg = _get_or_create_dbos_registry()
        if reg.config is not None:
            return reg.config
        config = load_config()
        reg.config = config
        return config

    @classproperty
    def sql_session(cls) -> Session:
        """Return the SQLAlchemy `Session` for the current context, which must be within a transaction function."""
        ctx = assert_current_dbos_context()
        assert ctx.is_transaction(), "db is only available within a transaction."
        rv = ctx.sql_session
        assert rv
        return rv

    @classproperty
    def workflow_id(cls) -> str:
        """Return the workflow ID for the current context, which must be executing a workflow function."""
        ctx = assert_current_dbos_context()
        assert (
            ctx.is_within_workflow()
        ), "workflow_id is only available within a DBOS operation."
        return ctx.workflow_id

    @classproperty
    def parent_workflow_id(cls) -> str:
        """
        Return the workflow ID for the parent workflow.

        `parent_workflow_id` must be accessed from within a workflow function.
        """

        ctx = assert_current_dbos_context()
        assert (
            ctx.is_within_workflow()
        ), "parent_workflow_id is only available within a workflow."
        return ctx.parent_workflow_id

    @classproperty
    def span(cls) -> Span:
        """Return the tracing `Span` associated with the current context."""
        ctx = assert_current_dbos_context()
        return ctx.get_current_span()

    @classproperty
    def request(cls) -> Optional["Request"]:
        """Return the HTTP `Request`, if any, associated with the current context."""
        ctx = assert_current_dbos_context()
        return ctx.request

    @classproperty
    def authenticated_user(cls) -> Optional[str]:
        """Return the current authenticated user, if any, associated with the current context."""
        ctx = assert_current_dbos_context()
        return ctx.authenticated_user

    @classproperty
    def authenticated_roles(cls) -> Optional[List[str]]:
        """Return the roles granted to the current authenticated user, if any, associated with the current context."""
        ctx = assert_current_dbos_context()
        return ctx.authenticated_roles

    @classproperty
    def assumed_role(cls) -> Optional[str]:
        """Return the role currently assumed by the authenticated user, if any, associated with the current context."""
        ctx = assert_current_dbos_context()
        return ctx.assumed_role

    @classmethod
    def set_authentication(
        cls, authenticated_user: Optional[str], authenticated_roles: Optional[List[str]]
    ) -> None:
        """Set the current authenticated user and granted roles into the current context."""
        ctx = assert_current_dbos_context()
        ctx.authenticated_user = authenticated_user
        ctx.authenticated_roles = authenticated_roles


@dataclass
class WorkflowStatus:
    """
    Status of workflow execution.

    This captures the state of a workflow execution at a point in time.

    Attributes:
        workflow_id(str):  The ID of the workflow execution
        status(str):  The status of the execution, from `WorkflowStatusString`
        name(str): The workflow function name
        class_name(str): For member functions, the name of the class containing the workflow function
        config_name(str): For instance member functions, the name of the class instance for the execution
        authenticated_user(str): The user who invoked the workflow
        assumed_role(str): The access role used by the user to allow access to the workflow function
        authenticated_roles(List[str]): List of all access roles available to the authenticated user
        recovery_attempts(int): Number of times the workflow has been restarted (usually by recovery)

    """

    workflow_id: str
    status: str
    name: str
    class_name: Optional[str]
    config_name: Optional[str]
    authenticated_user: Optional[str]
    assumed_role: Optional[str]
    authenticated_roles: Optional[List[str]]
    recovery_attempts: Optional[int]


class WorkflowHandle(Generic[R], Protocol):
    """
    Handle to a workflow function.

    `WorkflowHandle` represents a current or previous workflow function invocation,
    allowing its status and result to be accessed.

    Attributes:
        workflow_id(str): Workflow ID of the function invocation

    """

    def __init__(self, workflow_id: str) -> None: ...

    workflow_id: str

    def get_workflow_id(self) -> str:
        """Return the applicable workflow ID."""
        ...

    def get_result(self) -> R:
        """Return the result of the workflow function invocation, waiting if necessary."""
        ...

    def get_status(self) -> WorkflowStatus:
        """Return the current workflow function invocation status as `WorkflowStatus`."""
        ...


class DBOSConfiguredInstance:
    """
    Base class for classes containing DBOS member functions.

    When a class contains DBOS functions that access instance state, the DBOS workflow
    executor needs a name for the instance.  This name is recorded in the database, and
    used to refer to the proper instance upon recovery.

    Use `DBOSConfiguredInstance` to specify the instance name and register the instance
    with the DBOS workflow executor.

    Attributes:
        config_name(str):  Instance name

    """

    def __init__(self, config_name: str) -> None:
        self.config_name = config_name
        DBOS.register_instance(self)


# Apps that import DBOS probably don't exit.  If they do, let's see if
#   it looks like startup was abandoned or a call was forgotten...
def log_exit_info() -> None:
    if _dbos_global_registry is None:
        # Probably used as or for a support module
        return
    if _dbos_global_instance is None:
        print("DBOS exiting; functions were registered but DBOS() was not called")
        dbos_logger.warning(
            "DBOS exiting; functions were registered but DBOS() was not called"
        )
        return
    if not _dbos_global_instance._launched:
        print("DBOS exiting; DBOS exists but launch() was not called")
        dbos_logger.warning("DBOS exiting; DBOS exists but launch() was not called")
        return


# Register the exit hook
atexit.register(log_exit_info)
