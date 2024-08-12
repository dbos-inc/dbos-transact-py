from concurrent.futures import Future
from enum import Enum
from typing import Generic, Literal, TypeVar


class WorkflowNames:
    temp_workflow_name = "temp_workflow"


class TempWorkflowType(Enum):
    TRANSACTION = "transaction"
    PROCEDURE = "procedure"
    EXTERNAL = "external"
    SEND = "send"


TempWorkflowTypeLiteral = Literal["transaction", "procedure", "external", "send"]

R = TypeVar("R")


class WorkflowHandle(Generic[R]):

    def __init__(self, workflow_uuid: str, future: Future[R]):
        self.workflow_uuid = workflow_uuid
        self.future = future

    def get_workflow_uuid(self) -> str:
        return self.workflow_uuid

    def get_result(self) -> R:
        return self.future.result()
