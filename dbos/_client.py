import asyncio
import sys
import time
import uuid
from typing import Any, Generic, List, Optional, TypedDict, TypeVar

if sys.version_info < (3, 11):
    from typing_extensions import NotRequired
else:
    from typing import NotRequired

from dbos import _serialization
from dbos._dbos import WorkflowHandle, WorkflowHandleAsync
from dbos._dbos_config import parse_database_url_to_dbconfig
from dbos._error import DBOSNonExistentWorkflowError
from dbos._registrations import DEFAULT_MAX_RECOVERY_ATTEMPTS
from dbos._serialization import WorkflowInputs
from dbos._sys_db import SystemDatabase, WorkflowStatusInternal, WorkflowStatusString
from dbos._workflow_commands import (
    WorkflowStatus,
    get_workflow,
    list_queued_workflows,
    list_workflows,
)

R = TypeVar("R", covariant=True)  # A generic type for workflow return values


class EnqueueOptions(TypedDict):
    workflow_name: str
    queue_name: str
    workflow_id: NotRequired[str]
    app_version: NotRequired[str]
    workflow_timeout: NotRequired[float]


class WorkflowHandleClientPolling(Generic[R]):

    def __init__(self, workflow_id: str, sys_db: SystemDatabase):
        self.workflow_id = workflow_id
        self._sys_db = sys_db

    def get_workflow_id(self) -> str:
        return self.workflow_id

    def get_result(self) -> R:
        res: R = self._sys_db.await_workflow_result(self.workflow_id)
        return res

    def get_status(self) -> "WorkflowStatus":
        status = get_workflow(self._sys_db, self.workflow_id, True)
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

    async def get_status(self) -> "WorkflowStatus":
        status = await asyncio.to_thread(
            get_workflow, self._sys_db, self.workflow_id, True
        )
        if status is None:
            raise DBOSNonExistentWorkflowError(self.workflow_id)
        return status


class DBOSClient:
    def __init__(self, database_url: str, *, system_database: Optional[str] = None):
        db_config = parse_database_url_to_dbconfig(database_url)
        if system_database is not None:
            db_config["sys_db_name"] = system_database
        self._sys_db = SystemDatabase(db_config)

    def destroy(self) -> None:
        self._sys_db.destroy()

    def _enqueue(self, options: EnqueueOptions, *args: Any, **kwargs: Any) -> str:
        workflow_name = options["workflow_name"]
        queue_name = options["queue_name"]

        app_version = options.get("app_version")
        max_recovery_attempts = options.get("max_recovery_attempts")
        if max_recovery_attempts is None:
            max_recovery_attempts = DEFAULT_MAX_RECOVERY_ATTEMPTS
        workflow_id = options.get("workflow_id")
        if workflow_id is None:
            workflow_id = str(uuid.uuid4())
        workflow_timeout = options.get("workflow_timeout", None)

        status: WorkflowStatusInternal = {
            "workflow_uuid": workflow_id,
            "status": WorkflowStatusString.ENQUEUED.value,
            "name": workflow_name,
            "class_name": None,
            "queue_name": queue_name,
            "app_version": app_version,
            "config_name": None,
            "authenticated_user": None,
            "assumed_role": None,
            "authenticated_roles": None,
            "request": None,
            "output": None,
            "error": None,
            "created_at": None,
            "updated_at": None,
            "executor_id": None,
            "recovery_attempts": None,
            "app_id": None,
            "workflow_timeout": (  # UNIX epoch timestamp of the timeout in ms
                int((time.time() + workflow_timeout) * 1000)
                if workflow_timeout is not None
                else None
            ),
        }

        inputs: WorkflowInputs = {
            "args": args,
            "kwargs": kwargs,
        }

        self._sys_db.init_workflow(status, _serialization.serialize_args(inputs))
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
        status = get_workflow(self._sys_db, workflow_id, True)
        if status is None:
            raise DBOSNonExistentWorkflowError(workflow_id)
        return WorkflowHandleClientPolling[R](workflow_id, self._sys_db)

    async def retrieve_workflow_async(self, workflow_id: str) -> WorkflowHandleAsync[R]:
        status = asyncio.to_thread(get_workflow, self._sys_db, workflow_id, True)
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
            "request": None,
            "output": None,
            "error": None,
            "created_at": None,
            "updated_at": None,
            "executor_id": None,
            "recovery_attempts": None,
            "app_id": None,
            "app_version": None,
            "workflow_timeout": None,
        }
        with self._sys_db.engine.begin() as conn:
            self._sys_db.insert_workflow_status(status, conn)
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

    def resume_workflow(self, workflow_id: str) -> None:
        self._sys_db.resume_workflow(workflow_id)

    async def resume_workflow_async(self, workflow_id: str) -> None:
        await asyncio.to_thread(self.resume_workflow, workflow_id)

    def list_workflows(
        self,
        *,
        workflow_ids: Optional[List[str]] = None,
        status: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[str] = None,
        app_version: Optional[str] = None,
        user: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
        workflow_id_prefix: Optional[str] = None,
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
        )

    async def list_workflows_async(
        self,
        *,
        workflow_ids: Optional[List[str]] = None,
        status: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[str] = None,
        app_version: Optional[str] = None,
        user: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
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
        )

    def list_queued_workflows(
        self,
        *,
        queue_name: Optional[str] = None,
        status: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
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
        )

    async def list_queued_workflows_async(
        self,
        *,
        queue_name: Optional[str] = None,
        status: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        name: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_desc: bool = False,
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
        )
