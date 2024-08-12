from concurrent.futures import Future
from enum import Enum
from typing import Generic, TypeVar

R = TypeVar("R")


class WorkflowHandle(Generic[R]):

    def __init__(self, workflow_uuid: str, future: Future[R]):
        self.workflow_uuid = workflow_uuid
        self.future = future

    def get_workflow_uuid(self) -> str:
        return self.workflow_uuid

    def get_result(self) -> R:
        return self.future.result()
