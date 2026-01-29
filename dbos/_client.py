import asyncio
import json
import time
from typing import (
    TYPE_CHECKING,
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

import sqlalchemy as sa

from dbos._context import MaxPriority, MinPriority
from dbos._core import DEFAULT_POLLING_INTERVAL, TEMP_SEND_WF_NAME
from dbos._sys_db import SystemDatabase
from dbos._utils import generate_uuid

if TYPE_CHECKING:
    from dbos._dbos import WorkflowHandle, WorkflowHandleAsync

from dbos._dbos_config import get_system_database_url, is_valid_database_url
from dbos._error import DBOSException, DBOSNonExistentWorkflowError
from dbos._registrations import DEFAULT_MAX_RECOVERY_ATTEMPTS
from dbos._serialization import DefaultSerializer, Serializer, WorkflowInputs
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
from dbos._workflow_commands import fork_workflow, get_workflow

R = TypeVar("R", covariant=True)  # A generic type for workflow return values


# Required EnqueueOptions fields
class _EnqueueOptionsRequired(TypedDict):
    workflow_name: str
    queue_name: str


# Optional EnqueueOptions fields
class EnqueueOptions(_EnqueueOptionsRequired, total=False):
    workflow_id: str
    app_version: str
    workflow_timeout: float
    deduplication_id: str
    priority: int
    max_recovery_attempts: int
    queue_partition_key: str
    authenticated_user: str
    authenticated_roles: list[str]


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

    def get_result(
        self, *, polling_interval_sec: float = DEFAULT_POLLING_INTERVAL
    ) -> R:
        res: R = self._sys_db.await_workflow_result(
            self.workflow_id, polling_interval_sec
        )
        return res

    def get_status(self) -> WorkflowStatus:
        status = get_workflow(self._sys_db, self.workflow_id)
        if status is None:
            raise DBOSNonExistentWorkflowError("target", self.workflow_id)
        return status


class WorkflowHandleClientAsyncPolling(Generic[R]):

    def __init__(self, workflow_id: str, sys_db: SystemDatabase):
        self.workflow_id = workflow_id
        self._sys_db = sys_db

    def get_workflow_id(self) -> str:
        return self.workflow_id

    async def get_result(
        self, *, polling_interval_sec: float = DEFAULT_POLLING_INTERVAL
    ) -> R:
        res: R = await asyncio.to_thread(
            self._sys_db.await_workflow_result, self.workflow_id, polling_interval_sec
        )
        return res

    async def get_status(self) -> WorkflowStatus:
        status = await asyncio.to_thread(get_workflow, self._sys_db, self.workflow_id)
        if status is None:
            raise DBOSNonExistentWorkflowError("target", self.workflow_id)
        return status


class DBOSClient:

    def __init__(
        self,
        database_url: Optional[str] = None,  # DEPRECATED
        *,
        system_database_url: Optional[str] = None,
        system_database_engine: Optional[sa.Engine] = None,
        application_database_url: Optional[str] = None,
        dbos_system_schema: Optional[str] = "dbos",
        serializer: Serializer = DefaultSerializer(),
    ):
        self._serializer = serializer
        if system_database_engine:
            if "sqlite" in system_database_engine.dialect.name:
                system_database_url = "sqlite:///custom_system_database_engine.sqlite"
            else:
                system_database_url = "postgresql://custom:system@database/engine"
        else:
            application_database_url = (
                database_url if database_url else application_database_url
            )
            system_database_url = get_system_database_url(
                {
                    "system_database_url": system_database_url,
                    "database_url": application_database_url,
                }
            )
            assert is_valid_database_url(system_database_url)
        # We only create database connections but do not run migrations
        self._sys_db = SystemDatabase.create(
            system_database_url=system_database_url,
            engine_kwargs={
                "connect_args": {"application_name": "dbos_transact_client"},
                "pool_timeout": 30,
                "max_overflow": 0,
                "pool_size": 2,
                "pool_pre_ping": True,
            },
            engine=system_database_engine,
            schema=dbos_system_schema,
            serializer=serializer,
            executor_id=None,
        )
        self._sys_db.check_connection()

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
            workflow_id = generate_uuid()
        workflow_timeout = options.get("workflow_timeout", None)
        enqueue_options_internal: EnqueueOptionsInternal = {
            "deduplication_id": options.get("deduplication_id"),
            "priority": options.get("priority"),
            "app_version": options.get("app_version"),
            "queue_partition_key": options.get("queue_partition_key"),
        }

        authenticated_user = options.get("authenticated_user")
        authenticated_roles = (
            json.dumps(options.get("authenticated_roles"))
            if options.get("authenticated_roles")
            else None
        )

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
            "authenticated_user": authenticated_user,
            "assumed_role": None,
            "authenticated_roles": authenticated_roles,
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
            "inputs": self._serializer.serialize(inputs),
            "queue_partition_key": enqueue_options_internal["queue_partition_key"],
            "forked_from": None,
            "parent_workflow_id": None,
            "started_at_epoch_ms": None,
            "owner_xid": None,
        }

        self._sys_db.init_workflow(
            status,
            max_recovery_attempts=None,
            owner_xid=None,
            is_dequeued_request=False,
            is_recovery_request=False,
        )
        return workflow_id

    def enqueue(
        self, options: EnqueueOptions, *args: Any, **kwargs: Any
    ) -> "WorkflowHandle[R]":
        workflow_id = self._enqueue(options, *args, **kwargs)
        return WorkflowHandleClientPolling[R](workflow_id, self._sys_db)

    async def enqueue_async(
        self, options: EnqueueOptions, *args: Any, **kwargs: Any
    ) -> "WorkflowHandleAsync[R]":
        workflow_id = await asyncio.to_thread(self._enqueue, options, *args, **kwargs)
        return WorkflowHandleClientAsyncPolling[R](workflow_id, self._sys_db)

    def retrieve_workflow(self, workflow_id: str) -> "WorkflowHandle[R]":
        status = get_workflow(self._sys_db, workflow_id)
        if status is None:
            raise DBOSNonExistentWorkflowError("target", workflow_id)
        return WorkflowHandleClientPolling[R](workflow_id, self._sys_db)

    async def retrieve_workflow_async(
        self, workflow_id: str
    ) -> "WorkflowHandleAsync[R]":
        status = await asyncio.to_thread(get_workflow, self._sys_db, workflow_id)
        if status is None:
            raise DBOSNonExistentWorkflowError("target", workflow_id)
        return WorkflowHandleClientAsyncPolling[R](workflow_id, self._sys_db)

    def send(
        self,
        destination_id: str,
        message: Any,
        topic: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> None:
        idempotency_key = idempotency_key if idempotency_key else generate_uuid()
        status: WorkflowStatusInternal = {
            "workflow_uuid": f"{destination_id}-{idempotency_key}",
            "status": WorkflowStatusString.SUCCESS.value,
            "name": TEMP_SEND_WF_NAME,
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
            "inputs": self._serializer.serialize({"args": (), "kwargs": {}}),
            "queue_partition_key": None,
            "forked_from": None,
            "parent_workflow_id": None,
            "started_at_epoch_ms": None,
            "owner_xid": None,
        }
        with self._sys_db.engine.begin() as conn:
            self._sys_db._insert_workflow_status(
                status,
                conn,
                max_recovery_attempts=None,
                owner_xid=None,
                is_dequeued_request=False,
                is_recovery_request=False,
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

    def resume_workflow(self, workflow_id: str) -> "WorkflowHandle[Any]":
        self._sys_db.resume_workflow(workflow_id)
        return WorkflowHandleClientPolling[Any](workflow_id, self._sys_db)

    async def resume_workflow_async(
        self, workflow_id: str
    ) -> "WorkflowHandleAsync[Any]":
        await asyncio.to_thread(self.resume_workflow, workflow_id)
        return WorkflowHandleClientAsyncPolling[Any](workflow_id, self._sys_db)

    def list_workflows(
        self,
        *,
        workflow_ids: Optional[List[str]] = None,
        status: Optional[str | list[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[str | list[str]] = None,
        app_version: Optional[str | list[str]] = None,
        forked_from: Optional[str | list[str]] = None,
        parent_workflow_id: Optional[str | list[str]] = None,
        user: Optional[str | list[str]] = None,
        queue_name: Optional[str | list[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
        workflow_id_prefix: Optional[str | list[str]] = None,
        load_input: bool = True,
        load_output: bool = True,
        executor_id: Optional[str | list[str]] = None,
        queues_only: bool = False,
    ) -> List[WorkflowStatus]:
        return self._sys_db.list_workflows(
            workflow_ids=workflow_ids,
            status=status,
            start_time=start_time,
            end_time=end_time,
            name=name,
            app_version=app_version,
            forked_from=forked_from,
            parent_workflow_id=parent_workflow_id,
            user=user,
            queue_name=queue_name,
            limit=limit,
            offset=offset,
            sort_desc=sort_desc,
            workflow_id_prefix=workflow_id_prefix,
            load_input=load_input,
            load_output=load_output,
            executor_id=executor_id,
            queues_only=queues_only,
        )

    async def list_workflows_async(
        self,
        *,
        workflow_ids: Optional[List[str]] = None,
        status: Optional[str | list[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[str | list[str]] = None,
        app_version: Optional[str | list[str]] = None,
        forked_from: Optional[str | list[str]] = None,
        parent_workflow_id: Optional[str | list[str]] = None,
        user: Optional[str | list[str]] = None,
        queue_name: Optional[str | list[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
        workflow_id_prefix: Optional[str | list[str]] = None,
        load_input: bool = True,
        load_output: bool = True,
        executor_id: Optional[str | list[str]] = None,
        queues_only: bool = False,
    ) -> List[WorkflowStatus]:
        return await asyncio.to_thread(
            self.list_workflows,
            workflow_ids=workflow_ids,
            status=status,
            start_time=start_time,
            end_time=end_time,
            name=name,
            app_version=app_version,
            forked_from=forked_from,
            parent_workflow_id=parent_workflow_id,
            user=user,
            queue_name=queue_name,
            limit=limit,
            offset=offset,
            sort_desc=sort_desc,
            workflow_id_prefix=workflow_id_prefix,
            load_input=load_input,
            load_output=load_output,
            executor_id=executor_id,
            queues_only=queues_only,
        )

    def list_queued_workflows(
        self,
        *,
        workflow_ids: Optional[List[str]] = None,
        status: Optional[str | list[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[str | list[str]] = None,
        app_version: Optional[str | list[str]] = None,
        forked_from: Optional[str | list[str]] = None,
        parent_workflow_id: Optional[str | list[str]] = None,
        user: Optional[str | list[str]] = None,
        queue_name: Optional[str | list[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
        workflow_id_prefix: Optional[str | list[str]] = None,
        load_input: bool = True,
        load_output: bool = True,
        executor_id: Optional[str | list[str]] = None,
    ) -> List[WorkflowStatus]:
        return self._sys_db.list_workflows(
            workflow_ids=workflow_ids,
            status=status,
            start_time=start_time,
            end_time=end_time,
            name=name,
            app_version=app_version,
            forked_from=forked_from,
            parent_workflow_id=parent_workflow_id,
            user=user,
            queue_name=queue_name,
            limit=limit,
            offset=offset,
            sort_desc=sort_desc,
            workflow_id_prefix=workflow_id_prefix,
            load_input=load_input,
            load_output=load_output,
            executor_id=executor_id,
            queues_only=True,
        )

    async def list_queued_workflows_async(
        self,
        *,
        workflow_ids: Optional[List[str]] = None,
        status: Optional[str | list[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[str | list[str]] = None,
        app_version: Optional[str | list[str]] = None,
        forked_from: Optional[str | list[str]] = None,
        parent_workflow_id: Optional[str | list[str]] = None,
        user: Optional[str | list[str]] = None,
        queue_name: Optional[str | list[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
        workflow_id_prefix: Optional[str | list[str]] = None,
        load_input: bool = True,
        load_output: bool = True,
        executor_id: Optional[str | list[str]] = None,
    ) -> List[WorkflowStatus]:
        return await asyncio.to_thread(
            self.list_queued_workflows,
            workflow_ids=workflow_ids,
            status=status,
            start_time=start_time,
            end_time=end_time,
            name=name,
            app_version=app_version,
            forked_from=forked_from,
            parent_workflow_id=parent_workflow_id,
            user=user,
            queue_name=queue_name,
            limit=limit,
            offset=offset,
            sort_desc=sort_desc,
            workflow_id_prefix=workflow_id_prefix,
            load_input=load_input,
            load_output=load_output,
            executor_id=executor_id,
        )

    def list_workflow_steps(self, workflow_id: str) -> List[StepInfo]:
        return self._sys_db.list_workflow_steps(workflow_id)

    async def list_workflow_steps_async(self, workflow_id: str) -> List[StepInfo]:
        return await asyncio.to_thread(self.list_workflow_steps, workflow_id)

    def fork_workflow(
        self,
        workflow_id: str,
        start_step: int,
        *,
        application_version: Optional[str] = None,
    ) -> "WorkflowHandle[Any]":
        forked_workflow_id = fork_workflow(
            self._sys_db,
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
    ) -> "WorkflowHandleAsync[Any]":
        forked_workflow_id = await asyncio.to_thread(
            fork_workflow,
            self._sys_db,
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
