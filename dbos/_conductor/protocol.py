import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import List, Optional, Type, TypeVar


class MessageType(str, Enum):
    EXECUTOR_INFO = "executor_info"
    RECOVERY = "recovery"
    CANCEL = "cancel"
    LIST_WORKFLOWS = "list_workflows"


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
class ListWorkflowsBody:
    workflow_uuids: List[str]
    workflow_name: Optional[str]
    authenticated_user: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    status: Optional[str]
    application_version: Optional[str]
    limit: Optional[int]
    offset: Optional[int]


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


@dataclass
class ListWorkflowsRequest(BaseMessage):
    body: ListWorkflowsBody


@dataclass
class ListWorkflowsResponse(BaseMessage):
    output: List[WorkflowsOutput]
