from __future__ import annotations

import asyncio
import atexit
import json
import os
import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from logging import Logger
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Generic,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from opentelemetry.trace import Span

from ._classproperty import classproperty
from ._core import (
    TEMP_SEND_WF_NAME,
    WorkflowHandlePolling,
    decorate_step,
    decorate_transaction,
    decorate_workflow,
    execute_workflow_by_id,
    get_event,
    recv,
    send,
    set_event,
    start_workflow,
    workflow_wrapper,
)
from ._queue import Queue, queue_thread
from ._recovery import recover_pending_workflows, startup_recovery_thread
from ._registrations import (
    DEFAULT_MAX_RECOVERY_ATTEMPTS,
    DBOSClassInfo,
    get_or_create_class_info,
    set_dbos_func_name,
    set_temp_workflow_type,
)
from ._roles import default_required_roles, required_roles
from ._scheduler import ScheduledWorkflow, scheduled
from ._sys_db import WorkflowStatusString
from ._tracer import dbos_tracer

if TYPE_CHECKING:
    from fastapi import FastAPI
    from ._kafka import _KafkaConsumerWorkflow
    from ._request import Request
    from flask import Flask

from sqlalchemy.orm import Session

from ._request import Request

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec, TypeAlias
else:
    from typing import ParamSpec, TypeAlias

from ._admin_server import AdminServer
from ._app_db import ApplicationDatabase
from ._context import (
    EnterDBOSStep,
    TracedAttributes,
    assert_current_dbos_context,
    get_local_dbos_context,
)
from ._dbos_config import ConfigFile, load_config, set_env_vars
from ._error import DBOSException, DBOSNonExistentWorkflowError
from ._logger import add_otlp_to_all_loggers, dbos_logger
from ._sys_db import SystemDatabase

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
_dbos_global_registry: Optional[DBOSRegistry] = None


def _get_dbos_instance() -> DBOS:
    global _dbos_global_instance
    if _dbos_global_instance is not None:
        return _dbos_global_instance
    raise DBOSException("No DBOS was created yet")


def _get_or_create_dbos_registry() -> DBOSRegistry:
    # Currently get / init the global registry
    global _dbos_global_registry
    if _dbos_global_registry is None:
        _dbos_global_registry = DBOSRegistry()
    return _dbos_global_registry


RegisteredJob = Tuple[
    threading.Event, Callable[..., Any], Tuple[Any, ...], dict[str, Any]
]


class DBOSRegistry:
    def __init__(self) -> None:
        self.workflow_info_map: dict[str, Workflow[..., Any]] = {}
        self.class_info_map: dict[str, type] = {}
        self.instance_info_map: dict[str, object] = {}
        self.queue_info_map: dict[str, Queue] = {}
        self.pollers: list[RegisteredJob] = []
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
            self.dbos._executor.submit(func, *args, **kwargs)
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
    def destroy(cls, *, destroy_registry: bool = True) -> None:
        global _dbos_global_instance
        if _dbos_global_instance is not None:
            _dbos_global_instance._destroy()
        _dbos_global_instance = None
        if destroy_registry:
            global _dbos_global_registry
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
        if config is None:
            config = load_config()
        set_env_vars(config)
        dbos_tracer.config(config)
        dbos_logger.info("Initializing DBOS")
        self.config: ConfigFile = config
        self._launched: bool = False
        self._sys_db_field: Optional[SystemDatabase] = None
        self._app_db_field: Optional[ApplicationDatabase] = None
        self._registry: DBOSRegistry = _get_or_create_dbos_registry()
        self._registry.dbos = self
        self._admin_server_field: Optional[AdminServer] = None
        self.stop_events: List[threading.Event] = []
        self.fastapi: Optional["FastAPI"] = fastapi
        self.flask: Optional["Flask"] = flask
        self._executor_field: Optional[ThreadPoolExecutor] = None
        self._background_threads: List[threading.Thread] = []
        self._executor_id: str = os.environ.get("DBOS__VMID", "local")

        # If using FastAPI, set up middleware and lifecycle events
        if self.fastapi is not None:
            from ._fastapi import setup_fastapi_middleware

            setup_fastapi_middleware(self.fastapi, _get_dbos_instance())

        # If using Flask, set up middleware
        if self.flask is not None:
            from ._flask import setup_flask_middleware

            setup_flask_middleware(self.flask)

        # Register send_stub as a workflow
        def send_temp_workflow(
            destination_id: str, message: Any, topic: Optional[str]
        ) -> None:
            self.send(destination_id, message, topic)

        temp_send_wf = workflow_wrapper(self._registry, send_temp_workflow)
        set_dbos_func_name(send_temp_workflow, TEMP_SEND_WF_NAME)
        set_temp_workflow_type(send_temp_workflow, "send")
        self._registry.register_wf_function(TEMP_SEND_WF_NAME, temp_send_wf)

        for handler in dbos_logger.handlers:
            handler.flush()

    @property
    def _executor(self) -> ThreadPoolExecutor:
        if self._executor_field is None:
            raise DBOSException("Executor accessed before DBOS was launched")
        rv: ThreadPoolExecutor = self._executor_field
        return rv

    @property
    def _sys_db(self) -> SystemDatabase:
        if self._sys_db_field is None:
            raise DBOSException("System database accessed before DBOS was launched")
        rv: SystemDatabase = self._sys_db_field
        return rv

    @property
    def _app_db(self) -> ApplicationDatabase:
        if self._app_db_field is None:
            raise DBOSException(
                "Application database accessed before DBOS was launched"
            )
        rv: ApplicationDatabase = self._app_db_field
        return rv

    @property
    def _admin_server(self) -> AdminServer:
        if self._admin_server_field is None:
            raise DBOSException("Admin server accessed before DBOS was launched")
        rv: AdminServer = self._admin_server_field
        return rv

    @classmethod
    def launch(cls) -> None:
        if _dbos_global_instance is not None:
            _dbos_global_instance._launch()

    def _launch(self) -> None:
        try:
            if self._launched:
                dbos_logger.warning(f"DBOS was already launched")
                return
            self._launched = True
            self._executor_field = ThreadPoolExecutor(max_workers=64)
            self._sys_db_field = SystemDatabase(self.config)
            self._app_db_field = ApplicationDatabase(self.config)
            admin_port = self.config["runtimeConfig"].get("admin_port")
            if admin_port is None:
                admin_port = 3001
            self._admin_server_field = AdminServer(dbos=self, port=admin_port)

            if not os.environ.get("DBOS__VMID"):
                workflow_ids = self._sys_db.get_pending_workflows("local")
                self._executor.submit(startup_recovery_thread, self, workflow_ids)

            # Listen to notifications
            notification_listener_thread = threading.Thread(
                target=self._sys_db._notification_listener,
                daemon=True,
            )
            notification_listener_thread.start()
            self._background_threads.append(notification_listener_thread)

            # Start flush workflow buffers thread
            flush_workflow_buffers_thread = threading.Thread(
                target=self._sys_db.flush_workflow_buffers,
                daemon=True,
            )
            flush_workflow_buffers_thread.start()
            self._background_threads.append(flush_workflow_buffers_thread)

            # Start the queue thread
            evt = threading.Event()
            self.stop_events.append(evt)
            bg_queue_thread = threading.Thread(
                target=queue_thread, args=(evt, self), daemon=True
            )
            bg_queue_thread.start()
            self._background_threads.append(bg_queue_thread)

            # Grab any pollers that were deferred and start them
            for evt, func, args, kwargs in self._registry.pollers:
                self.stop_events.append(evt)
                poller_thread = threading.Thread(
                    target=func, args=args, kwargs=kwargs, daemon=True
                )
                poller_thread.start()
                self._background_threads.append(poller_thread)
            self._registry.pollers = []

            dbos_logger.info("DBOS launched")

            # Flush handlers and add OTLP to all loggers if enabled
            # to enable their export in DBOS Cloud
            for handler in dbos_logger.handlers:
                handler.flush()
            add_otlp_to_all_loggers()
        except Exception:
            dbos_logger.error(f"DBOS failed to launch: {traceback.format_exc()}")
            raise

    def _destroy(self) -> None:
        self._initialized = False
        for event in self.stop_events:
            event.set()
        if self._sys_db_field is not None:
            self._sys_db_field.destroy()
            self._sys_db_field = None
        if self._app_db_field is not None:
            self._app_db_field.destroy()
            self._app_db_field = None
        if self._admin_server_field is not None:
            self._admin_server_field.stop()
            self._admin_server_field = None
        # CB - This needs work, some things ought to stop before DBs are tossed out,
        #  on the other hand it hangs to move it
        if self._executor_field is not None:
            self._executor_field.shutdown(cancel_futures=True)
            self._executor_field = None
        for bg_thread in self._background_threads:
            bg_thread.join()

    @classmethod
    def register_instance(cls, inst: object) -> None:
        return _get_or_create_dbos_registry().register_instance(inst)

    # Decorators for DBOS functionality
    @classmethod
    def workflow(
        cls, *, max_recovery_attempts: int = DEFAULT_MAX_RECOVERY_ATTEMPTS
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """Decorate a function for use as a DBOS workflow."""
        return decorate_workflow(_get_or_create_dbos_registry(), max_recovery_attempts)

    @classmethod
    def transaction(
        cls, isolation_level: IsolationLevel = "SERIALIZABLE"
    ) -> Callable[[F], F]:
        """
        Decorate a function for use as a DBOS transaction.

        Args:
            isolation_level(IsolationLevel): Transaction isolation level

        """
        return decorate_transaction(_get_or_create_dbos_registry(), isolation_level)

    @classmethod
    def step(
        cls,
        *,
        retries_allowed: bool = False,
        interval_seconds: float = 1.0,
        max_attempts: int = 3,
        backoff_rate: float = 2.0,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """
        Decorate and configure a function for use as a DBOS step.

        Args:
            retries_allowed(bool): If true, enable retries on thrown exceptions
            interval_seconds(float): Time between retry attempts
            backoff_rate(float): Multiplier for exponentially increasing `interval_seconds` between retries
            max_attempts(int): Maximum number of retries before raising an exception

        """

        return decorate_step(
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
        cls,
        config: dict[str, Any],
        topics: list[str],
        in_order: bool = False,
    ) -> Callable[[_KafkaConsumerWorkflow], _KafkaConsumerWorkflow]:
        """Decorate a function to be used as a Kafka consumer."""
        try:
            from ._kafka import kafka_consumer

            return kafka_consumer(
                _get_or_create_dbos_registry(), config, topics, in_order
            )
        except ModuleNotFoundError as e:
            raise DBOSException(
                f"{e.name} dependency not found. Please install {e.name} via your package manager."
            ) from e

    @overload
    @classmethod
    def start_workflow(
        cls,
        func: Workflow[P, Coroutine[Any, Any, R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> WorkflowHandle[R]: ...

    @overload
    @classmethod
    def start_workflow(
        cls,
        func: Workflow[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> WorkflowHandle[R]: ...

    @classmethod
    def start_workflow(
        cls,
        func: Workflow[P, Union[R, Coroutine[Any, Any, R]]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> WorkflowHandle[R]:
        """Invoke a workflow function in the background, returning a handle to the ongoing execution."""
        return cast(
            WorkflowHandle[R],
            start_workflow(_get_dbos_instance(), func, None, True, *args, **kwargs),
        )

    @classmethod
    def get_workflow_status(cls, workflow_id: str) -> Optional[WorkflowStatus]:
        """Return the status of a workflow execution."""
        ctx = get_local_dbos_context()
        if ctx and ctx.is_within_workflow():
            ctx.function_id += 1
            stat = _get_dbos_instance()._sys_db.get_workflow_status_within_wf(
                workflow_id, ctx.workflow_id, ctx.function_id
            )
        else:
            stat = _get_dbos_instance()._sys_db.get_workflow_status(workflow_id)
        if stat is None:
            return None

        return WorkflowStatus(
            workflow_id=workflow_id,
            status=stat["status"],
            name=stat["name"],
            recovery_attempts=stat["recovery_attempts"],
            class_name=stat["class_name"],
            config_name=stat["config_name"],
            queue_name=stat["queue_name"],
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
        return WorkflowHandlePolling(workflow_id, dbos)

    @classmethod
    def send(
        cls, destination_id: str, message: Any, topic: Optional[str] = None
    ) -> None:
        """Send a message to a workflow execution."""
        return send(_get_dbos_instance(), destination_id, message, topic)

    @classmethod
    async def send_async(
        cls, destination_id: str, message: Any, topic: Optional[str] = None
    ) -> None:
        """Send a message to a workflow execution."""
        await asyncio.to_thread(lambda: DBOS.send(destination_id, message, topic))

    @classmethod
    def recv(cls, topic: Optional[str] = None, timeout_seconds: float = 60) -> Any:
        """
        Receive a workflow message.

        This function is to be called from within a workflow.
        `recv` will return the message sent on `topic`, waiting if necessary.
        """
        return recv(_get_dbos_instance(), topic, timeout_seconds)

    @classmethod
    async def recv_async(
        cls, topic: Optional[str] = None, timeout_seconds: float = 60
    ) -> Any:
        """
        Receive a workflow message.

        This function is to be called from within a workflow.
        `recv_async` will return the message sent on `topic`, asyncronously waiting if necessary.
        """
        return await asyncio.to_thread(lambda: DBOS.recv(topic, timeout_seconds))

    @classmethod
    def sleep(cls, seconds: float) -> None:
        """
        Sleep for the specified time (in seconds).

        It is important to use `DBOS.sleep` or `DBOS.sleep_async` (as opposed to any other sleep) within workflows,
        as the DBOS sleep methods are durable and completed sleeps will be skipped during recovery.
        """
        if seconds <= 0:
            return
        cur_ctx = get_local_dbos_context()
        if cur_ctx is not None:
            # Must call it within a workflow
            assert (
                cur_ctx.is_workflow()
            ), "sleep() must be called from within a workflow"
            attributes: TracedAttributes = {
                "name": "sleep",
            }
            with EnterDBOSStep(attributes):
                ctx = assert_current_dbos_context()
                _get_dbos_instance()._sys_db.sleep(
                    ctx.workflow_id, ctx.curr_step_function_id, seconds
                )
        else:
            # Cannot call it from outside of a workflow
            raise DBOSException("sleep() must be called from within a workflow")

    @classmethod
    async def sleep_async(cls, seconds: float) -> None:
        """
        Sleep for the specified time (in seconds).

        It is important to use `DBOS.sleep` or `DBOS.sleep_async` (as opposed to any other sleep) within workflows,
        as the DBOS sleep methods are durable and completed sleeps will be skipped during recovery.
        """
        await asyncio.to_thread(lambda: DBOS.sleep(seconds))

    @classmethod
    def set_event(cls, key: str, value: Any) -> None:
        """
        Set a workflow event.

        `set_event` sets the `value` of `key` for the current workflow instance ID.
        This `value` can then be retrieved by other functions, using `get_event` below.
        If the event `key` already exists, its `value` is updated.
        This function can only be called from within a workflow.

        Args:
            key(str): The event key / name within the workflow
            value(Any): A serializable value to associate with the key

        """
        return set_event(_get_dbos_instance(), key, value)

    @classmethod
    async def set_event_async(cls, key: str, value: Any) -> None:
        """
        Set a workflow event.

        `set_event_async` sets the `value` of `key` for the current workflow instance ID.
        This `value` can then be retrieved by other functions, using `get_event` below.
        If the event `key` already exists, its `value` is updated.
        This function can only be called from within a workflow.

        Args:
            key(str): The event key / name within the workflow
            value(Any): A serializable value to associate with the key

        """
        await asyncio.to_thread(lambda: DBOS.set_event(key, value))

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
        return get_event(_get_dbos_instance(), workflow_id, key, timeout_seconds)

    @classmethod
    async def get_event_async(
        cls, workflow_id: str, key: str, timeout_seconds: float = 60
    ) -> Any:
        """
        Return the `value` of a workflow event, waiting for it to occur if necessary.

        `get_event_async` waits for a corresponding `set_event` by the workflow with ID `workflow_id` with the same `key`.

        Args:
            workflow_id(str): The workflow instance ID that is expected to call `set_event` on `key`
            key(str): The event key / name within the workflow
            timeout_seconds(float): The amount of time to wait, in case `set_event` has not yet been called byt the workflow

        """
        return await asyncio.to_thread(
            lambda: DBOS.get_event(workflow_id, key, timeout_seconds)
        )

    @classmethod
    def execute_workflow_id(cls, workflow_id: str) -> WorkflowHandle[Any]:
        """Execute a workflow by ID (for recovery)."""
        return execute_workflow_by_id(_get_dbos_instance(), workflow_id)

    @classmethod
    def restart_workflow(cls, workflow_id: str) -> None:
        """Execute a workflow by ID (for recovery)."""
        execute_workflow_by_id(_get_dbos_instance(), workflow_id, True)

    @classmethod
    def recover_pending_workflows(
        cls, executor_ids: List[str] = ["local"]
    ) -> List[WorkflowHandle[Any]]:
        """Find all PENDING workflows and execute them."""
        return recover_pending_workflows(_get_dbos_instance(), executor_ids)

    @classmethod
    def cancel_workflow(cls, workflow_id: str) -> None:
        """Cancel a workflow by ID."""
        _get_dbos_instance()._sys_db.set_workflow_status(
            workflow_id, WorkflowStatusString.CANCELLED, False
        )

    @classmethod
    def resume_workflow(cls, workflow_id: str) -> None:
        """Resume a workflow by ID."""
        execute_workflow_by_id(_get_dbos_instance(), workflow_id, False)

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
        queue_name(str): For workflows that are or were queued, the queue name
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
    queue_name: Optional[str]
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
def _dbos_exit_hook() -> None:
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
    # If we get here, we're exiting normally
    _dbos_global_instance.destroy()


# Register the exit hook
atexit.register(_dbos_exit_hook)
