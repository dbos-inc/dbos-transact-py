from concurrent.futures import Future
from typing import Generic, TypeVar, cast

from dbos_transact.communicator import CommunicatorContext
from dbos_transact.transaction import TransactionContext

R = TypeVar("R")


class WorkflowContext:

    def __init__(self, workflow_uuid: str):
        self.workflow_uuid = workflow_uuid
        self.function_id = 0

    def txn_ctx(self) -> TransactionContext:
        return cast(TransactionContext, self)

    def comm_ctx(self) -> CommunicatorContext:
        return cast(CommunicatorContext, self)


class WorkflowHandle(Generic[R]):

    def __init__(self, workflow_uuid: str, future: Future[R]):
        self.workflow_uuid = workflow_uuid
        self.future = future

    def get_workflow_uuid(self) -> str:
        return self.workflow_uuid

    def get_result(self) -> R:
        return self.future.result()
