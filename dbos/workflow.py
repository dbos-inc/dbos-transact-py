from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, List, Optional, TypeVar

R = TypeVar("R", covariant=True)


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


class WorkflowHandle(Generic[R], ABC):
    def __init__(self, workflow_uuid: str):
        self.workflow_uuid = workflow_uuid

    def get_workflow_uuid(self) -> str:
        return self.workflow_uuid

    @abstractmethod
    def get_result(self) -> R:
        """Should be handled in the subclasses"""
        raise Exception()

    @abstractmethod
    def get_status(self) -> WorkflowStatus:
        """Should be handled in the subclasses"""
        raise Exception()
