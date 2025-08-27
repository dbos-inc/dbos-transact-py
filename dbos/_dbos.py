from __future__ import annotations

import asyncio
import hashlib
import inspect
import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    Generator,
    Generic,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from opentelemetry.trace import Span
from rich import print

from dbos._conductor.conductor import ConductorWebsocket
from dbos._sys_db import SystemDatabase, WorkflowStatus
from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams
from dbos._workflow_commands import fork_workflow, list_queued_workflows, list_workflows

from ._classproperty import classproperty
from ._core import (
    TEMP_SEND_WF_NAME,
    WorkflowHandleAsyncPolling,
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
    start_workflow_async,
    workflow_wrapper,
)
from ._queue import Queue, queue_thread
from ._recovery import recover_pending_workflows, startup_recovery_thread
from ._registrations import (
    DEFAULT_MAX_RECOVERY_ATTEMPTS,
    DBOSClassInfo,
    _class_fqn,
    get_or_create_class_info,
    set_dbos_func_name,
    set_temp_workflow_type,
)
from ._roles import default_required_roles, required_roles
from ._scheduler import ScheduledWorkflow, scheduled
from ._sys_db import (
    StepInfo,
    SystemDatabase,
    WorkflowStatus,
    _dbos_stream_closed_sentinel,
    workflow_is_active,
)
from ._tracer import DBOSTracer, dbos_tracer

if TYPE_CHECKING:
    from fastapi import FastAPI
    from ._kafka import _KafkaConsumerWorkflow
    from flask import Flask

from sqlalchemy.orm import Session

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from ._admin_server import AdminServer
from ._app_db import ApplicationDatabase
from ._context import (
    EnterDBOSStep,
    StepStatus,
    TracedAttributes,
    assert_current_dbos_context,
    get_local_dbos_context,
)
from ._dbos_config import (
    ConfigFile,
    DBOSConfig,
    get_system_database_url,
    overwrite_config,
    process_config,
    translate_dbos_config_to_config_file,
)
from ._error import (
    DBOSConflictingRegistrationError,
    DBOSException,
    DBOSNonExistentWorkflowError,
)
from ._event_loop import BackgroundEventLoop
from ._logger import (
    add_otlp_to_all_loggers,
    add_transformer_to_all_loggers,
    config_logger,
    dbos_logger,
    init_logger,
)
from ._workflow_commands import get_workflow, list_workflow_steps

# Most DBOS functions are just any callable F, so decorators / wrappers work on F
# There are cases where the parameters P and return value R should be separate
#   Such as for start_workflow, which will return WorkflowHandle[R]
#   In those cases, use something like Workflow[P,R]
F = TypeVar("F", bound=Callable[..., Any])

P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values

T = TypeVar("T")

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
        self.workflow_info_map: dict[str, Callable[..., Any]] = {}
        self.function_type_map: dict[str, str] = {}
        self.class_info_map: dict[str, type] = {}
        self.instance_info_map: dict[str, object] = {}
        self.queue_info_map: dict[str, Queue] = {}
        self.pollers: list[RegisteredJob] = []
        self.dbos: Optional[DBOS] = None

    def register_wf_function(self, name: str, wrapped_func: F, functype: str) -> None:
        if name in self.function_type_map:
            if self.function_type_map[name] != functype:
                raise DBOSConflictingRegistrationError(name)
            if name != TEMP_SEND_WF_NAME:
                # Remove the `<temp>` prefix from the function name to avoid confusion
                truncated_name = name.replace("<temp>.", "")
                dbos_logger.warning(
                    f"Duplicate registration of function '{truncated_name}'. A function named '{truncated_name}' has already been registered with DBOS. All functions registered with DBOS must have unique names."
                )
        self.function_type_map[name] = functype
        self.workflow_info_map[name] = wrapped_func

    def register_class(self, cls: type, ci: DBOSClassInfo) -> None:
        class_name = _class_fqn(cls)
        if class_name in self.class_info_map:
            if self.class_info_map[class_name] is not cls:
                raise Exception(f"Duplicate type registration for class '{class_name}'")
        else:
            self.class_info_map[class_name] = cls

    def create_class_info(
        self, cls: Type[T], class_name: Optional[str] = None
    ) -> Type[T]:
        ci = get_or_create_class_info(cls, class_name)
        self.register_class(cls, ci)
        return cls

    def register_poller(
        self, evt: threading.Event, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> None:
        if self.dbos and self.dbos._launched:
            self.dbos.poller_stop_events.append(evt)
            self.dbos._executor.submit(func, *args, **kwargs)
        else:
            self.pollers.append((evt, func, args, kwargs))

    def register_instance(self, inst: object) -> None:
        config_name = getattr(inst, "config_name")
        class_name = _class_fqn(inst.__class__)
        if self.dbos and self.dbos._launched:
            dbos_logger.warning(
                f"Configured instance {config_name} of class {class_name} was registered after DBOS was launched. This may cause errors during workflow recovery. All configured instances should be instantiated before DBOS is launched."
            )
        fn = f"{class_name}/{config_name}"
        if fn in self.instance_info_map:
            if self.instance_info_map[fn] is not inst:
                raise Exception(
                    f"Duplicate instance registration for class '{class_name}' instance '{config_name}'"
                )
        else:
            self.instance_info_map[fn] = inst

    def compute_app_version(self) -> str:
        """
        An application's version is computed from a hash of the source of its workflows.
        This is guaranteed to be stable given identical source code because it uses an MD5 hash
        and because it iterates through the workflows in sorted order.
        This way, if the app's workflows are updated (which would break recovery), its version changes.
        App version can be manually set through the DBOS__APPVERSION environment variable.
        """
        hasher = hashlib.md5()
        sources = sorted(
            [inspect.getsource(wf) for wf in self.workflow_info_map.values()]
        )
        # Different DBOS versions should produce different app versions
        sources.append(GlobalParams.dbos_version)
        for source in sources:
            hasher.update(source.encode("utf-8"))
        return hasher.hexdigest()

    def get_internal_queue(self) -> Queue:
        """
        Get or create the internal queue used for the DBOS scheduler, for Kafka, and for
        programmatic resuming and restarting of workflows.
        """
        return Queue(INTERNAL_QUEUE_NAME)


class DBOS:
    """
    Main access class for DBOS functionality.

    `DBOS` contains functions and properties for:
    1. Decorating classes, workflows, and steps
    2. Starting workflow functions
    3. Retrieving workflow status information
    4. Interacting with workflows via events and messages
    5. Accessing context, including the current user, SQL session, logger, and tracer

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
        config: DBOSConfig,
        fastapi: Optional["FastAPI"] = None,
        flask: Optional["Flask"] = None,
        conductor_url: Optional[str] = None,
        conductor_key: Optional[str] = None,
    ) -> DBOS:
        global _dbos_global_instance
        global _dbos_global_registry
        if _dbos_global_instance is None:
            _dbos_global_instance = super().__new__(cls)
            _dbos_global_instance.__init__(fastapi=fastapi, config=config, flask=flask, conductor_url=conductor_url, conductor_key=conductor_key)  # type: ignore
        return _dbos_global_instance

    @classmethod
    def destroy(
        cls,
        *,
        destroy_registry: bool = False,
        workflow_completion_timeout_sec: int = 0,
    ) -> None:
        global _dbos_global_instance
        if _dbos_global_instance is not None:
            _dbos_global_instance._destroy(
                workflow_completion_timeout_sec=workflow_completion_timeout_sec,
            )
        _dbos_global_instance = None
        if destroy_registry:
            global _dbos_global_registry
            _dbos_global_registry = None
        GlobalParams.app_version = os.environ.get("DBOS__APPVERSION", "")
        GlobalParams.executor_id = os.environ.get("DBOS__VMID", "local")
        dbos_logger.info("DBOS successfully shut down")

    def __init__(
        self,
        *,
        config: DBOSConfig,
        fastapi: Optional["FastAPI"] = None,
        flask: Optional["Flask"] = None,
        conductor_url: Optional[str] = None,
        conductor_key: Optional[str] = None,
    ) -> None:
        if hasattr(self, "_initialized") and self._initialized:
            return

        self._initialized: bool = True

        self._launched: bool = False
        self._debug_mode: bool = False
        self._sys_db_field: Optional[SystemDatabase] = None
        self._app_db_field: Optional[ApplicationDatabase] = None
        self._registry: DBOSRegistry = _get_or_create_dbos_registry()
        self._registry.dbos = self
        self._admin_server_field: Optional[AdminServer] = None
        # Stop internal background threads (queue thread, timeout threads, etc.)
        self.background_thread_stop_events: List[threading.Event] = []
        # Stop pollers (event receivers) that can create new workflows (scheduler, Kafka)
        self.poller_stop_events: List[threading.Event] = []
        self.fastapi: Optional["FastAPI"] = fastapi
        self.flask: Optional["Flask"] = flask
        self._executor_field: Optional[ThreadPoolExecutor] = None
        self._background_threads: List[threading.Thread] = []
        self.conductor_url: Optional[str] = conductor_url
        self.conductor_key: Optional[str] = conductor_key
        self.conductor_websocket: Optional[ConductorWebsocket] = None
        self._background_event_loop: BackgroundEventLoop = BackgroundEventLoop()
        self._active_workflows_set: set[str] = set()

        # Globally set the application version and executor ID.
        # In DBOS Cloud, instead use the values supplied through environment variables.
        if not os.environ.get("DBOS__CLOUD") == "true":
            if (
                "application_version" in config
                and config["application_version"] is not None
            ):
                GlobalParams.app_version = config["application_version"]
            if "executor_id" in config and config["executor_id"] is not None:
                GlobalParams.executor_id = config["executor_id"]

        init_logger()

        # Translate user provided config to an internal format
        unvalidated_config = translate_dbos_config_to_config_file(config)
        if os.environ.get("DBOS__CLOUD") == "true":
            unvalidated_config = overwrite_config(unvalidated_config)

        if unvalidated_config is not None:
            self._config: ConfigFile = process_config(data=unvalidated_config)
        else:
            raise ValueError("No valid configuration was loaded.")

        config_logger(self._config)
        dbos_tracer.config(self._config)
        dbos_logger.info(f"Initializing DBOS (v{GlobalParams.dbos_version})")

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
        set_dbos_func_name(temp_send_wf, TEMP_SEND_WF_NAME)
        set_temp_workflow_type(send_temp_workflow, "send")
        self._registry.register_wf_function(TEMP_SEND_WF_NAME, temp_send_wf, "send")

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

    @property
    def debug_mode(self) -> bool:
        return self._debug_mode

    @classmethod
    def launch(cls, *, debug_mode: bool = False) -> None:
        if _dbos_global_instance is not None:
            _dbos_global_instance._launch(debug_mode=debug_mode)

    def _launch(self, *, debug_mode: bool = False) -> None:
        try:
            if self._launched:
                dbos_logger.warning(f"DBOS was already launched")
                return
            self._launched = True
            self._debug_mode = debug_mode
            if GlobalParams.app_version == "":
                GlobalParams.app_version = self._registry.compute_app_version()
            if self.conductor_key is not None:
                GlobalParams.executor_id = str(uuid.uuid4())
            dbos_logger.info(f"Executor ID: {GlobalParams.executor_id}")
            dbos_logger.info(f"Application version: {GlobalParams.app_version}")
            self._executor_field = ThreadPoolExecutor(max_workers=sys.maxsize)
            self._background_event_loop.start()
            assert self._config["database_url"] is not None
            assert self._config["database"]["sys_db_engine_kwargs"] is not None
            self._sys_db_field = SystemDatabase.create(
                system_database_url=get_system_database_url(self._config),
                engine_kwargs=self._config["database"]["sys_db_engine_kwargs"],
                debug_mode=debug_mode,
            )
            assert self._config["database"]["db_engine_kwargs"] is not None
            self._app_db_field = ApplicationDatabase.create(
                database_url=self._config["database_url"],
                engine_kwargs=self._config["database"]["db_engine_kwargs"],
                debug_mode=debug_mode,
            )

            if debug_mode:
                return

            # Run migrations for the system and application databases
            self._sys_db.run_migrations()
            self._app_db.run_migrations()

            admin_port = self._config.get("runtimeConfig", {}).get("admin_port")
            if admin_port is None:
                admin_port = 3001
            run_admin_server = self._config.get("runtimeConfig", {}).get(
                "run_admin_server"
            )
            if run_admin_server:
                try:
                    self._admin_server_field = AdminServer(dbos=self, port=admin_port)
                except Exception as e:
                    dbos_logger.warning(f"Failed to start admin server: {e}")

            workflow_ids = self._sys_db.get_pending_workflows(
                GlobalParams.executor_id, GlobalParams.app_version
            )
            if (len(workflow_ids)) > 0:
                self.logger.info(
                    f"Recovering {len(workflow_ids)} workflows from application version {GlobalParams.app_version}"
                )
            else:
                self.logger.info(
                    f"No workflows to recover from application version {GlobalParams.app_version}"
                )

            self._executor.submit(startup_recovery_thread, self, workflow_ids)

            # Listen to notifications
            notification_listener_thread = threading.Thread(
                target=self._sys_db._notification_listener,
                daemon=True,
            )
            notification_listener_thread.start()
            self._background_threads.append(notification_listener_thread)

            # Create the internal queue if it has not yet been created
            self._registry.get_internal_queue()

            # Start the queue thread
            evt = threading.Event()
            self.background_thread_stop_events.append(evt)
            bg_queue_thread = threading.Thread(
                target=queue_thread, args=(evt, self), daemon=True
            )
            bg_queue_thread.start()
            self._background_threads.append(bg_queue_thread)

            # Start the conductor thread if requested
            if self.conductor_key is not None:
                if self.conductor_url is None:
                    dbos_domain = os.environ.get("DBOS_DOMAIN", "cloud.dbos.dev")
                    self.conductor_url = f"wss://{dbos_domain}/conductor/v1alpha1"
                evt = threading.Event()
                self.background_thread_stop_events.append(evt)
                self.conductor_websocket = ConductorWebsocket(
                    self,
                    conductor_url=self.conductor_url,
                    conductor_key=self.conductor_key,
                    evt=evt,
                )
                self.conductor_websocket.start()
                self._background_threads.append(self.conductor_websocket)

            # Grab any pollers that were deferred and start them
            for evt, func, args, kwargs in self._registry.pollers:
                self.poller_stop_events.append(evt)
                poller_thread = threading.Thread(
                    target=func, args=args, kwargs=kwargs, daemon=True
                )
                poller_thread.start()
                self._background_threads.append(poller_thread)
            self._registry.pollers = []

            dbos_logger.info("DBOS launched!")

            if self.conductor_key is None and os.environ.get("DBOS__CLOUD") != "true":
                # Hint the user to open the URL to register and set up Conductor
                app_name = self._config["name"]
                conductor_registration_url = (
                    f"https://console.dbos.dev/self-host?appname={app_name}"
                )
                print(
                    f"[bold]To view and manage workflows, connect to DBOS Conductor at:[/bold] [bold blue]{conductor_registration_url}[/bold blue]"
                )

            # Flush handlers and add OTLP to all loggers if enabled
            # to enable their export in DBOS Cloud
            for handler in dbos_logger.handlers:
                handler.flush()
            add_otlp_to_all_loggers()
            add_transformer_to_all_loggers()
        except Exception as e:
            dbos_logger.error(f"DBOS failed to launch:", exc_info=e)
            raise

    @classmethod
    def reset_system_database(cls) -> None:
        """
        Destroy the DBOS system database. Useful for resetting the state of DBOS between tests.
        This is a destructive operation and should only be used in a test environment.
        More information on testing DBOS apps: https://docs.dbos.dev/python/tutorials/testing
        """
        if _dbos_global_instance is not None:
            _dbos_global_instance._reset_system_database()
        else:
            dbos_logger.warning(
                "reset_system_database has no effect because global DBOS object does not exist"
            )

    def _reset_system_database(self) -> None:
        assert (
            not self._launched
        ), "The system database cannot be reset after DBOS is launched. Resetting the system database is a destructive operation that should only be used in a test environment."

        SystemDatabase.reset_system_database(get_system_database_url(self._config))

    def _destroy(self, *, workflow_completion_timeout_sec: int) -> None:
        self._initialized = False
        for event in self.poller_stop_events:
            event.set()
        for event in self.background_thread_stop_events:
            event.set()
        if workflow_completion_timeout_sec > 0:
            deadline = time.time() + workflow_completion_timeout_sec
            while time.time() < deadline:
                time.sleep(1)
                active_workflows = len(self._active_workflows_set)
                if active_workflows > 0:
                    dbos_logger.info(
                        f"Attempting to shut down DBOS. {active_workflows} workflows remain active. IDs: {self._active_workflows_set}"
                    )
                else:
                    break
        self._background_event_loop.stop()
        if self._sys_db_field is not None:
            self._sys_db_field.destroy()
            self._sys_db_field = None
        if self._app_db_field is not None:
            self._app_db_field.destroy()
            self._app_db_field = None
        if self._admin_server_field is not None:
            self._admin_server_field.stop()
            self._admin_server_field = None
        if (
            self.conductor_websocket is not None
            and self.conductor_websocket.websocket is not None
        ):
            self.conductor_websocket.websocket.close()
        if self._executor_field is not None:
            self._executor_field.shutdown(wait=False, cancel_futures=True)
            self._executor_field = None
        for bg_thread in self._background_threads:
            bg_thread.join()

    @classmethod
    def register_instance(cls, inst: object) -> None:
        return _get_or_create_dbos_registry().register_instance(inst)

    # Decorators for DBOS functionality
    @classmethod
    def workflow(
        cls,
        *,
        name: Optional[str] = None,
        max_recovery_attempts: Optional[int] = DEFAULT_MAX_RECOVERY_ATTEMPTS,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """Decorate a function for use as a DBOS workflow."""
        return decorate_workflow(
            _get_or_create_dbos_registry(), name, max_recovery_attempts
        )

    @classmethod
    def transaction(
        cls,
        isolation_level: IsolationLevel = "SERIALIZABLE",
        *,
        name: Optional[str] = None,
    ) -> Callable[[F], F]:
        """
        Decorate a function for use as a DBOS transaction.

        Args:
            isolation_level(IsolationLevel): Transaction isolation level

        """
        return decorate_transaction(
            _get_or_create_dbos_registry(), name, isolation_level
        )

    @classmethod
    def step(
        cls,
        *,
        name: Optional[str] = None,
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
            name=name,
            retries_allowed=retries_allowed,
            interval_seconds=interval_seconds,
            max_attempts=max_attempts,
            backoff_rate=backoff_rate,
        )

    @classmethod
    def dbos_class(
        cls, class_name: Optional[str] = None
    ) -> Callable[[Type[T]], Type[T]]:
        """
        Decorate a class that contains DBOS member functions.

        All DBOS classes must be decorated, as this associates the class with
        its member functions. Class names must be globally unique. By default, the class name is class.__qualname__  but you can optionally provide a class name that is different from the default name.
        """

        def register_class(cls: Type[T]) -> Type[T]:
            # Register the class with the DBOS registry
            _get_or_create_dbos_registry().create_class_info(cls, class_name)
            return cls

        return register_class

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

    @classmethod
    def start_workflow(
        cls,
        func: Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> WorkflowHandle[R]:
        """Invoke a workflow function in the background, returning a handle to the ongoing execution."""
        return start_workflow(_get_dbos_instance(), func, None, True, *args, **kwargs)

    @classmethod
    async def start_workflow_async(
        cls,
        func: Callable[P, Coroutine[Any, Any, R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> WorkflowHandleAsync[R]:
        """Invoke a workflow function on the event loop, returning a handle to the ongoing execution."""
        await cls._configure_asyncio_thread_pool()
        return await start_workflow_async(
            _get_dbos_instance(), func, None, True, *args, **kwargs
        )

    @classmethod
    def get_workflow_status(cls, workflow_id: str) -> Optional[WorkflowStatus]:
        """Return the status of a workflow execution."""

        def fn() -> Optional[WorkflowStatus]:
            return get_workflow(_get_dbos_instance()._sys_db, workflow_id)

        return _get_dbos_instance()._sys_db.call_function_as_step(fn, "DBOS.getStatus")

    @classmethod
    async def get_workflow_status_async(
        cls, workflow_id: str
    ) -> Optional[WorkflowStatus]:
        await cls._configure_asyncio_thread_pool()
        """Return the status of a workflow execution."""
        return await asyncio.to_thread(cls.get_workflow_status, workflow_id)

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
    async def retrieve_workflow_async(
        cls, workflow_id: str, existing_workflow: bool = True
    ) -> WorkflowHandleAsync[R]:
        """Return a `WorkflowHandle` for a workflow execution."""
        dbos = _get_dbos_instance()
        await cls._configure_asyncio_thread_pool()
        if existing_workflow:
            stat = await dbos.get_workflow_status_async(workflow_id)
            if stat is None:
                raise DBOSNonExistentWorkflowError(workflow_id)
        return WorkflowHandleAsyncPolling(workflow_id, dbos)

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
        await cls._configure_asyncio_thread_pool()
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
        await cls._configure_asyncio_thread_pool()
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
        await cls._configure_asyncio_thread_pool()
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
        await cls._configure_asyncio_thread_pool()
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
        await cls._configure_asyncio_thread_pool()
        return await asyncio.to_thread(
            lambda: DBOS.get_event(workflow_id, key, timeout_seconds)
        )

    @classmethod
    def _execute_workflow_id(cls, workflow_id: str) -> WorkflowHandle[Any]:
        """Execute a workflow by ID (for recovery)."""
        return execute_workflow_by_id(_get_dbos_instance(), workflow_id)

    @classmethod
    def _recover_pending_workflows(
        cls, executor_ids: List[str] = ["local"]
    ) -> List[WorkflowHandle[Any]]:
        """Find all PENDING workflows and execute them."""
        return recover_pending_workflows(_get_dbos_instance(), executor_ids)

    @classmethod
    def cancel_workflow(cls, workflow_id: str) -> None:
        """Cancel a workflow by ID."""

        def fn() -> None:
            dbos_logger.info(f"Cancelling workflow: {workflow_id}")
            _get_dbos_instance()._sys_db.cancel_workflow(workflow_id)

        return _get_dbos_instance()._sys_db.call_function_as_step(
            fn, "DBOS.cancelWorkflow"
        )

    @classmethod
    async def cancel_workflow_async(cls, workflow_id: str) -> None:
        """Cancel a workflow by ID."""
        await cls._configure_asyncio_thread_pool()
        await asyncio.to_thread(cls.cancel_workflow, workflow_id)

    @classmethod
    async def _configure_asyncio_thread_pool(cls) -> None:
        """
        Configure the thread pool for asyncio.to_thread.

        This function is called before the first call to asyncio.to_thread.
        """
        loop = asyncio.get_running_loop()
        loop.set_default_executor(_get_dbos_instance()._executor)

    @classmethod
    def resume_workflow(cls, workflow_id: str) -> WorkflowHandle[Any]:
        """Resume a workflow by ID."""

        def fn() -> None:
            dbos_logger.info(f"Resuming workflow: {workflow_id}")
            _get_dbos_instance()._sys_db.resume_workflow(workflow_id)

        _get_dbos_instance()._sys_db.call_function_as_step(fn, "DBOS.resumeWorkflow")
        return cls.retrieve_workflow(workflow_id)

    @classmethod
    async def resume_workflow_async(cls, workflow_id: str) -> WorkflowHandleAsync[Any]:
        """Resume a workflow by ID."""
        await cls._configure_asyncio_thread_pool()
        await asyncio.to_thread(cls.resume_workflow, workflow_id)
        return await cls.retrieve_workflow_async(workflow_id)

    @classmethod
    def restart_workflow(cls, workflow_id: str) -> WorkflowHandle[Any]:
        """Restart a workflow with a new workflow ID"""
        return cls.fork_workflow(workflow_id, 1)

    @classmethod
    async def restart_workflow_async(cls, workflow_id: str) -> WorkflowHandleAsync[Any]:
        """Restart a workflow with a new workflow ID"""
        return await cls.fork_workflow_async(workflow_id, 1)

    @classmethod
    def fork_workflow(
        cls,
        workflow_id: str,
        start_step: int,
        *,
        application_version: Optional[str] = None,
    ) -> WorkflowHandle[Any]:
        """Restart a workflow with a new workflow ID from a specific step"""

        def fn() -> str:
            dbos_logger.info(f"Forking workflow: {workflow_id} from step {start_step}")
            return fork_workflow(
                _get_dbos_instance()._sys_db,
                _get_dbos_instance()._app_db,
                workflow_id,
                start_step,
                application_version=application_version,
            )

        new_id = _get_dbos_instance()._sys_db.call_function_as_step(
            fn, "DBOS.forkWorkflow"
        )
        return cls.retrieve_workflow(new_id)

    @classmethod
    async def fork_workflow_async(
        cls,
        workflow_id: str,
        start_step: int,
        *,
        application_version: Optional[str] = None,
    ) -> WorkflowHandleAsync[Any]:
        """Restart a workflow with a new workflow ID from a specific step"""
        await cls._configure_asyncio_thread_pool()
        new_id = await asyncio.to_thread(
            lambda: cls.fork_workflow(
                workflow_id, start_step, application_version=application_version
            ).get_workflow_id()
        )
        return await cls.retrieve_workflow_async(new_id)

    @classmethod
    def list_workflows(
        cls,
        *,
        workflow_ids: Optional[List[str]] = None,
        status: Optional[Union[str, List[str]]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[str] = None,
        app_version: Optional[str] = None,
        user: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
        workflow_id_prefix: Optional[str] = None,
        load_input: bool = True,
        load_output: bool = True,
    ) -> List[WorkflowStatus]:
        def fn() -> List[WorkflowStatus]:
            return list_workflows(
                _get_dbos_instance()._sys_db,
                workflow_ids=workflow_ids,
                status=status,
                start_time=start_time,
                end_time=end_time,
                name=name,
                app_version=app_version,
                user=user,
                limit=limit,
                offset=offset,
                sort_desc=sort_desc,
                workflow_id_prefix=workflow_id_prefix,
                load_input=load_input,
                load_output=load_output,
            )

        return _get_dbos_instance()._sys_db.call_function_as_step(
            fn, "DBOS.listWorkflows"
        )

    @classmethod
    async def list_workflows_async(
        cls,
        *,
        workflow_ids: Optional[List[str]] = None,
        status: Optional[Union[str, List[str]]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[str] = None,
        app_version: Optional[str] = None,
        user: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
        workflow_id_prefix: Optional[str] = None,
        load_input: bool = True,
        load_output: bool = True,
    ) -> List[WorkflowStatus]:
        await cls._configure_asyncio_thread_pool()
        return await asyncio.to_thread(
            cls.list_workflows,
            workflow_ids=workflow_ids,
            status=status,
            start_time=start_time,
            end_time=end_time,
            name=name,
            app_version=app_version,
            user=user,
            limit=limit,
            offset=offset,
            sort_desc=sort_desc,
            workflow_id_prefix=workflow_id_prefix,
            load_input=load_input,
            load_output=load_output,
        )

    @classmethod
    def list_queued_workflows(
        cls,
        *,
        queue_name: Optional[str] = None,
        status: Optional[Union[str, List[str]]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
        load_input: bool = True,
    ) -> List[WorkflowStatus]:
        def fn() -> List[WorkflowStatus]:
            return list_queued_workflows(
                _get_dbos_instance()._sys_db,
                queue_name=queue_name,
                status=status,
                start_time=start_time,
                end_time=end_time,
                name=name,
                limit=limit,
                offset=offset,
                sort_desc=sort_desc,
                load_input=load_input,
            )

        return _get_dbos_instance()._sys_db.call_function_as_step(
            fn, "DBOS.listQueuedWorkflows"
        )

    @classmethod
    async def list_queued_workflows_async(
        cls,
        *,
        queue_name: Optional[str] = None,
        status: Optional[Union[str, List[str]]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
        load_input: bool = True,
    ) -> List[WorkflowStatus]:
        await cls._configure_asyncio_thread_pool()
        return await asyncio.to_thread(
            cls.list_queued_workflows,
            queue_name=queue_name,
            status=status,
            start_time=start_time,
            end_time=end_time,
            name=name,
            limit=limit,
            offset=offset,
            sort_desc=sort_desc,
            load_input=load_input,
        )

    @classmethod
    def list_workflow_steps(cls, workflow_id: str) -> List[StepInfo]:
        def fn() -> List[StepInfo]:
            return list_workflow_steps(
                _get_dbos_instance()._sys_db, _get_dbos_instance()._app_db, workflow_id
            )

        return _get_dbos_instance()._sys_db.call_function_as_step(
            fn, "DBOS.listWorkflowSteps"
        )

    @classmethod
    async def list_workflow_steps_async(cls, workflow_id: str) -> List[StepInfo]:
        await cls._configure_asyncio_thread_pool()
        return await asyncio.to_thread(cls.list_workflow_steps, workflow_id)

    @classproperty
    def logger(cls) -> Logger:
        """Return the DBOS `Logger` for the current context."""
        return dbos_logger  # TODO get from context if appropriate...

    @classproperty
    def sql_session(cls) -> Session:
        """Return the SQLAlchemy `Session` for the current context, which must be within a transaction function."""
        ctx = assert_current_dbos_context()
        assert ctx.is_transaction(), "db is only available within a transaction."
        rv = ctx.sql_session
        assert rv
        return rv

    @classproperty
    def workflow_id(cls) -> Optional[str]:
        """Return the ID of the currently executing workflow. If a workflow is not executing, return None."""
        ctx = get_local_dbos_context()
        if ctx and ctx.is_within_workflow():
            return ctx.workflow_id
        else:
            return None

    @classproperty
    def step_id(cls) -> Optional[int]:
        """Return the step ID for the currently executing step. This is a unique identifier of the current step within the workflow. If a step is not currently executing, return None."""
        ctx = get_local_dbos_context()
        if ctx and (ctx.is_step() or ctx.is_transaction()):
            return ctx.function_id
        else:
            return None

    @classproperty
    def step_status(cls) -> Optional[StepStatus]:
        """Return the status of the currently executing step. If a step is not currently executing, return None."""
        ctx = get_local_dbos_context()
        if ctx and ctx.is_step():
            return ctx.step_status
        else:
            return None

    @classproperty
    def parent_workflow_id(cls) -> str:
        """
        This method is deprecated and should not be used.
        """
        dbos_logger.warning(
            "DBOS.parent_workflow_id is deprecated and should not be used"
        )
        ctx = assert_current_dbos_context()
        assert (
            ctx.is_within_workflow()
        ), "parent_workflow_id is only available within a workflow."
        return ctx.parent_workflow_id

    @classproperty
    def span(cls) -> Span:
        """Return the tracing `Span` associated with the current context."""
        ctx = assert_current_dbos_context()
        span = ctx.get_current_span()
        assert span
        return span

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

    @classmethod
    def write_stream(cls, key: str, value: Any) -> None:
        """
        Write a value to a stream.

        Args:
            key(str): The stream key / name within the workflow
            value(Any): A serializable value to write to the stream

        """
        ctx = get_local_dbos_context()
        if ctx is not None:
            # Must call it within a workflow
            if ctx.is_workflow():
                attributes: TracedAttributes = {
                    "name": "write_stream",
                }
                with EnterDBOSStep(attributes):
                    ctx = assert_current_dbos_context()
                    _get_dbos_instance()._sys_db.write_stream_from_workflow(
                        ctx.workflow_id, ctx.function_id, key, value
                    )
            elif ctx.is_step():
                _get_dbos_instance()._sys_db.write_stream_from_step(
                    ctx.workflow_id, key, value
                )
            else:
                raise DBOSException(
                    "write_stream() must be called from within a workflow or step"
                )
        else:
            # Cannot call it from outside of a workflow
            raise DBOSException(
                "write_stream() must be called from within a workflow or step"
            )

    @classmethod
    def close_stream(cls, key: str) -> None:
        """
        Close a stream.

        Args:
            key(str): The stream key / name within the workflow

        """
        ctx = get_local_dbos_context()
        if ctx is not None:
            # Must call it within a workflow
            if ctx.is_workflow():
                attributes: TracedAttributes = {
                    "name": "close_stream",
                }
                with EnterDBOSStep(attributes):
                    ctx = assert_current_dbos_context()
                    _get_dbos_instance()._sys_db.close_stream(
                        ctx.workflow_id, ctx.function_id, key
                    )
            else:
                raise DBOSException(
                    "close_stream() must be called from within a workflow"
                )
        else:
            # Cannot call it from outside of a workflow
            raise DBOSException("close_stream() must be called from within a workflow")

    @classmethod
    def read_stream(cls, workflow_id: str, key: str) -> Generator[Any, Any, None]:
        """
        Read values from a stream as a generator.

        This function reads values from a stream identified by the workflow_id and key,
        yielding each value in order until the stream is closed or the workflow terminates.

        Args:
            workflow_id(str): The workflow instance ID that owns the stream
            key(str): The stream key / name within the workflow

        Yields:
            Any: Each value in the stream until the stream is closed

        """
        offset = 0
        sys_db = _get_dbos_instance()._sys_db

        while True:
            try:
                value = sys_db.read_stream(workflow_id, key, offset)
                if value == _dbos_stream_closed_sentinel:
                    break
                yield value
                offset += 1
            except ValueError:
                # Poll the offset until a value arrives or the workflow terminates
                status = cls.retrieve_workflow(workflow_id).get_status().status
                if not workflow_is_active(status):
                    break
                time.sleep(1.0)
                continue

    @classmethod
    async def write_stream_async(cls, key: str, value: Any) -> None:
        """
        Write a value to a stream asynchronously.

        Args:
            key(str): The stream key / name within the workflow
            value(Any): A serializable value to write to the stream

        """
        await cls._configure_asyncio_thread_pool()
        await asyncio.to_thread(lambda: DBOS.write_stream(key, value))

    @classmethod
    async def close_stream_async(cls, key: str) -> None:
        """
        Close a stream asynchronously.

        Args:
            key(str): The stream key / name within the workflow

        """
        await cls._configure_asyncio_thread_pool()
        await asyncio.to_thread(lambda: DBOS.close_stream(key))

    @classmethod
    async def read_stream_async(
        cls, workflow_id: str, key: str
    ) -> AsyncGenerator[Any, None]:
        """
        Read values from a stream as an async generator.

        This function reads values from a stream identified by the workflow_id and key,
        yielding each value in order until the stream is closed or the workflow terminates.

        Args:
            workflow_id(str): The workflow instance ID that owns the stream
            key(str): The stream key / name within the workflow

        Yields:
            Any: Each value in the stream until the stream is closed

        """
        await cls._configure_asyncio_thread_pool()
        offset = 0
        sys_db = _get_dbos_instance()._sys_db

        while True:
            try:
                value = await asyncio.to_thread(
                    sys_db.read_stream, workflow_id, key, offset
                )
                if value == _dbos_stream_closed_sentinel:
                    break
                yield value
                offset += 1
            except ValueError:
                # Poll the offset until a value arrives or the workflow terminates
                status = (
                    await (await cls.retrieve_workflow_async(workflow_id)).get_status()
                ).status
                if not workflow_is_active(status):
                    break
                await asyncio.sleep(1.0)
                continue

    @classproperty
    def tracer(self) -> DBOSTracer:
        """Return the DBOS OpenTelemetry tracer."""
        return dbos_tracer


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


class WorkflowHandleAsync(Generic[R], Protocol):
    """
    Handle to a workflow function.

    `WorkflowHandleAsync` represents a current or previous workflow function invocation,
    allowing its status and result to be accessed.

    Attributes:
        workflow_id(str): Workflow ID of the function invocation

    """

    def __init__(self, workflow_id: str) -> None: ...

    workflow_id: str

    def get_workflow_id(self) -> str:
        """Return the applicable workflow ID."""
        ...

    async def get_result(self) -> R:
        """Return the result of the workflow function invocation, waiting if necessary."""
        ...

    async def get_status(self) -> WorkflowStatus:
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
