from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

from dbos import utils
from dbos.error import DBOSNonExistentWorkflowError, DBOSWorkflowConflictUUIDError
from dbos.system_database import WorkflowStatusInternal
from dbos.workflow import WorkflowHandle, WorkflowStatus

if TYPE_CHECKING:
    from dbos.dbos import DBOS, Workflow

P = ParamSpec("P")  # A generic type for workflow parameters
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


def _execute_workflow(
    dbos: "DBOS",
    status: WorkflowStatusInternal,
    func: "Workflow[P, R]",
    *args: Any,
    **kwargs: Any,
) -> R:
    try:
        output = func(*args, **kwargs)
    except DBOSWorkflowConflictUUIDError:
        # Retrieve the workflow handle and wait for the result.
        wf_handle: WorkflowHandle[R] = dbos.retrieve_workflow(DBOS.workflow_id)
        output = wf_handle.get_result()
        return output
    except Exception as error:
        status["status"] = "ERROR"
        status["error"] = utils.serialize(error)
        dbos.sys_db.update_workflow_status(status)
        raise error

    status["status"] = "SUCCESS"
    status["output"] = utils.serialize(output)
    dbos.sys_db.update_workflow_status(status)
    return output
