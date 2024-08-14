from abc import ABC, abstractmethod
from typing import Generic, TypeVar

R = TypeVar("R", covariant=True)


class WorkflowHandle(Generic[R], ABC):
    def __init__(self, workflow_uuid: str):
        self.workflow_uuid = workflow_uuid

    def get_workflow_uuid(self) -> str:
        return self.workflow_uuid

    @abstractmethod
    def get_result(self) -> R:
        """Should be handled in the subclasses"""
        raise Exception()

    # TODO get status
