from concurrent.futures import Future
from typing import TYPE_CHECKING, TypeVar

from dbos.error import DBOSNonExistentWorkflowError
from dbos.workflow import WorkflowHandle, WorkflowStatus

if TYPE_CHECKING:
    from dbos.dbos import DBOS

R = TypeVar("R", covariant=True)  # A generic type for workflow return values


class _WorkflowHandleFuture(WorkflowHandle[R]):

    def __init__(self, workflow_uuid: str, future: Future[R], dbos: "DBOS"):
        super().__init__(workflow_uuid)
        self.future = future
        self.dbos = dbos

    def get_result(self) -> R:
        return self.future.result()

    def get_status(self) -> WorkflowStatus:
        stat = self.dbos.get_workflow_status(self.workflow_uuid)
        if stat is None:
            raise DBOSNonExistentWorkflowError(self.workflow_uuid)
        return stat


class _WorkflowHandlePolling(WorkflowHandle[R]):

    def __init__(self, workflow_uuid: str, dbos: "DBOS"):
        super().__init__(workflow_uuid)
        self.dbos = dbos

    def get_result(self) -> R:
        res: R = self.dbos.sys_db.await_workflow_result(self.workflow_uuid)
        return res

    def get_status(self) -> WorkflowStatus:
        stat = self.dbos.get_workflow_status(self.workflow_uuid)
        if stat is None:
            raise DBOSNonExistentWorkflowError(self.workflow_uuid)
        return stat
