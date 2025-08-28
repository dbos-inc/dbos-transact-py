import asyncio
import sys
import time
import uuid
from typing import (
    Any,
    AsyncGenerator,
    Generator,
    Generic,
    List,
    Optional,
    TypedDict,
    TypeVar,
    Union,
)

from dbos._app_db import ApplicationDatabase
from dbos._context import MaxPriority, MinPriority
from dbos._sys_db import SystemDatabase

if sys.version_info < (3, 11):
    from typing_extensions import NotRequired
else:
    from typing import NotRequired

from dbos import _serialization
from dbos._dbos import WorkflowHandle, WorkflowHandleAsync
from dbos._dbos_config import (
    get_application_database_url,
    get_system_database_url,
    is_valid_database_url,
)
from dbos._error import DBOSException, DBOSNonExistentWorkflowError
from dbos._registrations import DEFAULT_MAX_RECOVERY_ATTEMPTS
from dbos._serialization import WorkflowInputs
from dbos._sys_db import (
    EnqueueOptionsInternal,
    StepInfo,
    SystemDatabase,
    WorkflowStatus,
    WorkflowStatusInternal,
    WorkflowStatusString,
    _dbos_stream_closed_sentinel,
    workflow_is_active,
)
from dbos._workflow_commands import (
    fork_workflow,
    get_workflow,
    list_queued_workflows,
    list_workflow_steps,
    list_workflows,
)

R = TypeVar("R", covariant=True)  # A generic type for workflow return values


class EnqueueOptions(TypedDict):
    workflow_name: str
    queue_name: str
    workflow_id: NotRequired[str]
    app_version: NotRequired[str]
    workflow_timeout: NotRequired[float]
    deduplication_id: NotRequired[str]
    priority: NotRequired[int]


def validate_enqueue_options(options: EnqueueOptions) -> None:
    priority = options.get("priority")
    if priority is not None and (priority < MinPriority or priority > MaxPriority):
        raise DBOSException(
            f"Invalid priority {priority}. Priority must be between {MinPriority}~{MaxPriority}."
        )


class WorkflowHandleClientPolling(Generic[R]):

    def __init__(self, workflow_id: str, sys_db: SystemDatabase):
        self.workflow_id = workflow_id
        self._sys_db = sys_db

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(self) -> R:
        res: R = self._sys_db.await_workflow_result(self.workflow_id)
        return res

    def get_status(self) -> WorkflowStatus:
        status = get_workflow(self._sys_db, self.workflow_id)
        if status is None:
            raise DBOSNonExistentWorkflowError(self.workflow_id)
        return status


class WorkflowHandleClientAsyncPolling(Generic[R]):

    def __init__(self, workflow_id: str, sys_db: SystemDatabase):
        self.workflow_id = workflow_id
        self._sys_db = sys_db

    def get_workflow_id(self) -> str:
        return self.workflow_id

    async def get_result(self) -> R:
        res: R = await asyncio.to_thread(
            self._sys_db.await_workflow_result, self.workflow_id
        )
        return res

    async def get_status(self) -> WorkflowStatus:
        status = await asyncio.to_thread(get_workflow, self._sys_db, self.workflow_id)
        if status is None:
            raise DBOSNonExistentWorkflowError(self.workflow_id)
        return status


class DBOSClient:
    def __init__(
        self,
        database_url: Optional[str] = None,  # DEPRECATED
        *,
        system_database_url: Optional[str] = None,
        application_database_url: Optional[str] = None,
        system_database: Optional[str] = None,  # DEPRECATED
    ):
        application_database_url = get_application_database_url(
            {
                "system_database_url": system_database_url,
                "database_url": (
                    database_url if database_url else application_database_url
                ),
            }
        )
        system_database_url = get_system_database_url(
            {
                "system_database_url": system_database_url,
                "database_url": application_database_url,
                "database": {"sys_db_name": system_database},
            }
        )
        assert is_valid_database_url(system_database_url)
        assert is_valid_database_url(application_database_url)
        # We only create database connections but do not run migrations
        self._sys_db = SystemDatabase.create(
            system_database_url=system_database_url,
            engine_kwargs={
                "pool_timeout": 30,
                "max_overflow": 0,
                "pool_size": 2,
            },
        )
        self._sys_db.check_connection()
        self._app_db = ApplicationDatabase.create(
            database_url=application_database_url,
            engine_kwargs={
                "pool_timeout": 30,
                "max_overflow": 0,
                "pool_size": 2,
            },
        )

    def destroy(self) -> None:
        self._sys_db.destroy()

    def _enqueue(self, options: EnqueueOptions, *args: Any, **kwargs: Any) -> str:
        validate_enqueue_options(options)
        workflow_name = options["workflow_name"]
        queue_name = options["queue_name"]

        max_recovery_attempts = options.get("max_recovery_attempts")
        if max_recovery_attempts is None:
            max_recovery_attempts = DEFAULT_MAX_RECOVERY_ATTEMPTS
        workflow_id = options.get("workflow_id")
        if workflow_id is None:
            workflow_id = str(uuid.uuid4())
        workflow_timeout = options.get("workflow_timeout", None)
        enqueue_options_internal: EnqueueOptionsInternal = {
            "deduplication_id": options.get("deduplication_id"),
            "priority": options.get("priority"),
            "app_version": options.get("app_version"),
        }

        inputs: WorkflowInputs = {
            "args": args,
            "kwargs": kwargs,
        }

        status: WorkflowStatusInternal = {
            "workflow_uuid": workflow_id,
            "status": WorkflowStatusString.ENQUEUED.value,
            "name": workflow_name,
            "class_name": None,
            "queue_name": queue_name,
            "app_version": enqueue_options_internal["app_version"],
            "config_name": None,
            "authenticated_user": None,
            "assumed_role": None,
            "authenticated_roles": None,
            "output": None,
            "error": None,
            "created_at": None,
            "updated_at": None,
            "executor_id": None,
            "recovery_attempts": None,
            "app_id": None,
            "workflow_timeout_ms": (
                int(workflow_timeout * 1000) if workflow_timeout is not None else None
            ),
            "workflow_deadline_epoch_ms": None,
            "deduplication_id": enqueue_options_internal["deduplication_id"],
            "priority": (
                enqueue_options_internal["priority"]
                if enqueue_options_internal["priority"] is not None
                else 0
            ),
            "inputs": _serialization.serialize_args(inputs),
        }

        self._sys_db.init_workflow(
            status,
            max_recovery_attempts=None,
        )
        return workflow_id

    def enqueue(
        self, options: EnqueueOptions, *args: Any, **kwargs: Any
    ) -> WorkflowHandle[R]:
        workflow_id = self._enqueue(options, *args, **kwargs)
        return WorkflowHandleClientPolling[R](workflow_id, self._sys_db)

    async def enqueue_async(
        self, options: EnqueueOptions, *args: Any, **kwargs: Any
    ) -> WorkflowHandleAsync[R]:
        workflow_id = await asyncio.to_thread(self._enqueue, options, *args, **kwargs)
        return WorkflowHandleClientAsyncPolling[R](workflow_id, self._sys_db)

    def retrieve_workflow(self, workflow_id: str) -> WorkflowHandle[R]:
        status = get_workflow(self._sys_db, workflow_id)
        if status is None:
            raise DBOSNonExistentWorkflowError(workflow_id)
        return WorkflowHandleClientPolling[R](workflow_id, self._sys_db)

    async def retrieve_workflow_async(self, workflow_id: str) -> WorkflowHandleAsync[R]:
        status = asyncio.to_thread(get_workflow, self._sys_db, workflow_id)
        if status is None:
            raise DBOSNonExistentWorkflowError(workflow_id)
        return WorkflowHandleClientAsyncPolling[R](workflow_id, self._sys_db)

    def send(
        self,
        destination_id: str,
        message: Any,
        topic: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> None:
        idempotency_key = idempotency_key if idempotency_key else str(uuid.uuid4())
        status: WorkflowStatusInternal = {
            "workflow_uuid": f"{destination_id}-{idempotency_key}",
            "status": WorkflowStatusString.SUCCESS.value,
            "name": "temp_workflow-send-client",
            "class_name": None,
            "queue_name": None,
            "config_name": None,
            "authenticated_user": None,
            "assumed_role": None,
            "authenticated_roles": None,
            "output": None,
            "error": None,
            "created_at": None,
            "updated_at": None,
            "executor_id": None,
            "recovery_attempts": None,
            "app_id": None,
            "app_version": None,
            "workflow_timeout_ms": None,
            "workflow_deadline_epoch_ms": None,
            "deduplication_id": None,
            "priority": 0,
            "inputs": _serialization.serialize_args({"args": (), "kwargs": {}}),
        }
        with self._sys_db.engine.begin() as conn:
            self._sys_db._insert_workflow_status(
                status, conn, max_recovery_attempts=None
            )
        self._sys_db.send(status["workflow_uuid"], 0, destination_id, message, topic)

    async def send_async(
        self,
        destination_id: str,
        message: Any,
        topic: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> None:
        return await asyncio.to_thread(
            self.send, destination_id, message, topic, idempotency_key
        )

    def get_event(self, workflow_id: str, key: str, timeout_seconds: float = 60) -> Any:
        return self._sys_db.get_event(workflow_id, key, timeout_seconds)

    async def get_event_async(
        self, workflow_id: str, key: str, timeout_seconds: float = 60
    ) -> Any:
        return await asyncio.to_thread(
            self.get_event, workflow_id, key, timeout_seconds
        )

    def cancel_workflow(self, workflow_id: str) -> None:
        self._sys_db.cancel_workflow(workflow_id)

    async def cancel_workflow_async(self, workflow_id: str) -> None:
        await asyncio.to_thread(self.cancel_workflow, workflow_id)

    def resume_workflow(self, workflow_id: str) -> WorkflowHandle[Any]:
        self._sys_db.resume_workflow(workflow_id)
        return WorkflowHandleClientPolling[Any](workflow_id, self._sys_db)

    async def resume_workflow_async(self, workflow_id: str) -> WorkflowHandleAsync[Any]:
        await asyncio.to_thread(self.resume_workflow, workflow_id)
        return WorkflowHandleClientAsyncPolling[Any](workflow_id, self._sys_db)

    def list_workflows(
        self,
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
        return list_workflows(
            self._sys_db,
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

    async def list_workflows_async(
        self,
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
        return await asyncio.to_thread(
            self.list_workflows,
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

    def list_queued_workflows(
        self,
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
        return list_queued_workflows(
            self._sys_db,
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

    async def list_queued_workflows_async(
        self,
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
        return await asyncio.to_thread(
            self.list_queued_workflows,
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

    def list_workflow_steps(self, workflow_id: str) -> List[StepInfo]:
        return list_workflow_steps(self._sys_db, self._app_db, workflow_id)

    async def list_workflow_steps_async(self, workflow_id: str) -> List[StepInfo]:
        return await asyncio.to_thread(self.list_workflow_steps, workflow_id)

    def fork_workflow(
        self,
        workflow_id: str,
        start_step: int,
        *,
        application_version: Optional[str] = None,
    ) -> WorkflowHandle[Any]:
        forked_workflow_id = fork_workflow(
            self._sys_db,
            self._app_db,
            workflow_id,
            start_step,
            application_version=application_version,
        )
        return WorkflowHandleClientPolling[Any](forked_workflow_id, self._sys_db)

    async def fork_workflow_async(
        self,
        workflow_id: str,
        start_step: int,
        *,
        application_version: Optional[str] = None,
    ) -> WorkflowHandleAsync[Any]:
        forked_workflow_id = await asyncio.to_thread(
            fork_workflow,
            self._sys_db,
            self._app_db,
            workflow_id,
            start_step,
            application_version=application_version,
        )
        return WorkflowHandleClientAsyncPolling[Any](forked_workflow_id, self._sys_db)

    def read_stream(self, workflow_id: str, key: str) -> Generator[Any, Any, None]:
        """
        Read values from a stream as a generator.
        This function reads values from a stream identified by the workflow_id and key,
        yielding each value in order until the stream is closed or the workflow terminates.

        Args:
            workflow_id: The ID of the workflow that wrote to the stream
            key: The stream key to read from

        Yields:
            The values written to the stream in order
        """
        offset = 0
        while True:
            try:
                value = self._sys_db.read_stream(workflow_id, key, offset)
                if value == _dbos_stream_closed_sentinel:
                    break
                yield value
                offset += 1
            except ValueError:
                # Poll the offset until a value arrives or the workflow terminates
                status = get_workflow(self._sys_db, workflow_id)
                if status is None:
                    break
                if not workflow_is_active(status.status):
                    break
                time.sleep(1.0)
                continue

    async def read_stream_async(
        self, workflow_id: str, key: str
    ) -> AsyncGenerator[Any, None]:
        """
        Read values from a stream as an async generator.
        This function reads values from a stream identified by the workflow_id and key,
        yielding each value in order until the stream is closed or the workflow terminates.

        Args:
            workflow_id: The ID of the workflow that wrote to the stream
            key: The stream key to read from

        Yields:
            The values written to the stream in order
        """
        offset = 0
        while True:
            try:
                value = await asyncio.to_thread(
                    self._sys_db.read_stream, workflow_id, key, offset
                )
                if value == _dbos_stream_closed_sentinel:
                    break
                yield value
                offset += 1
            except ValueError:
                # Poll the offset until a value arrives or the workflow terminates
                status = await asyncio.to_thread(
                    get_workflow, self._sys_db, workflow_id
                )
                if status is None:
                    break
                if not workflow_is_active(status.status):
                    break
                await asyncio.sleep(1.0)
                continue
