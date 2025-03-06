import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import List, Optional, Type, TypedDict, TypeVar

from dbos._workflow_commands import WorkflowInformation


class MessageType(str, Enum):
    EXECUTOR_INFO = "executor_info"
    RECOVERY = "recovery"
    CANCEL = "cancel"
    LIST_WORKFLOWS = "list_workflows"
    LIST_QUEUED_WORKFLOWS = "list_queued_workflows"
    RESUME = "resume"
    RESTART = "restart"


T = TypeVar("T", bound="BaseMessage")


@dataclass
class BaseMessage:
    type: MessageType
    request_id: str

    @classmethod
    def from_json(cls: Type[T], json_str: str) -> T:
        """
        Safely load a JSON into a dataclass, loading only the
        attributes specified in the dataclass.
        """
        data = json.loads(json_str)
        all_annotations = {}
        for base_cls in cls.__mro__:
            if hasattr(base_cls, "__annotations__"):
                all_annotations.update(base_cls.__annotations__)
        kwargs = {k: v for k, v in data.items() if k in all_annotations}
        return cls(**kwargs)

    def to_json(self) -> str:
        dict_data = asdict(self)
        return json.dumps(dict_data)


@dataclass
class ExecutorInfoRequest(BaseMessage):
    pass


@dataclass
class ExecutorInfoResponse(BaseMessage):
    executor_id: str
    application_version: str


@dataclass
class RecoveryRequest(BaseMessage):
    executor_ids: List[str]


@dataclass
class RecoveryResponse(BaseMessage):
    success: bool


@dataclass
class CancelRequest(BaseMessage):
    workflow_id: str


@dataclass
class CancelResponse(BaseMessage):
    success: bool


@dataclass
class ResumeRequest(BaseMessage):
    workflow_id: str


@dataclass
class ResumeResponse(BaseMessage):
    success: bool


@dataclass
class RestartRequest(BaseMessage):
    workflow_id: str


@dataclass
class RestartResponse(BaseMessage):
    success: bool


class ListWorkflowsBody(TypedDict):
    workflow_uuids: List[str]
    workflow_name: Optional[str]
    authenticated_user: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    status: Optional[str]
    application_version: Optional[str]
    limit: Optional[int]
    offset: Optional[int]
    sort_desc: bool


@dataclass
class WorkflowsOutput:
    WorkflowUUID: str
    Status: Optional[str]
    WorkflowName: Optional[str]
    WorkflowClassName: Optional[str]
    WorkflowConfigName: Optional[str]
    AuthenticatedUser: Optional[str]
    AssumedRole: Optional[str]
    AuthenticatedRoles: Optional[str]
    Input: Optional[str]
    Output: Optional[str]
    Request: Optional[str]
    Error: Optional[str]
    CreatedAt: Optional[str]
    UpdatedAt: Optional[str]
    QueueName: Optional[str]
    ApplicationVersion: Optional[str]

    @classmethod
    def from_workflow_information(cls, info: WorkflowInformation) -> "WorkflowsOutput":
        # Convert fields to strings as needed
        created_at_str = str(info.created_at) if info.created_at is not None else None
        updated_at_str = str(info.updated_at) if info.updated_at is not None else None
        inputs_str = str(info.input) if info.input is not None else None
        outputs_str = str(info.output) if info.output is not None else None
        request_str = str(info.request) if info.request is not None else None

        return cls(
            WorkflowUUID=info.workflow_id,
            Status=info.status,
            WorkflowName=info.workflow_name,
            WorkflowClassName=info.workflow_class_name,
            WorkflowConfigName=info.workflow_config_name,
            AuthenticatedUser=info.authenticated_user,
            AssumedRole=info.assumed_role,
            AuthenticatedRoles=info.authenticated_roles,
            Input=inputs_str,
            Output=outputs_str,
            Request=request_str,
            Error=info.error,
            CreatedAt=created_at_str,
            UpdatedAt=updated_at_str,
            QueueName=info.queue_name,
            ApplicationVersion=info.app_version,
        )


@dataclass
class ListWorkflowsRequest(BaseMessage):
    body: ListWorkflowsBody


@dataclass
class ListWorkflowsResponse(BaseMessage):
    output: List[WorkflowsOutput]


class ListQueuedWorkflowsBody(TypedDict):
    workflow_name: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    status: Optional[str]
    queue_name: Optional[str]
    limit: Optional[int]
    offset: Optional[int]
    sort_desc: bool


@dataclass
class ListQueuedWorkflowsRequest(BaseMessage):
    body: ListQueuedWorkflowsBody


@dataclass
class ListQueuedWorkflowsResponse(BaseMessage):
    output: List[WorkflowsOutput]
