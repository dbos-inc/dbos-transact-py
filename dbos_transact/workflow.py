from concurrent.futures import Future
from typing import Generic, List, Optional, TypedDict, TypeVar

from dbos_transact.system_database import WorkflowInputs, WorkflowStatuses

R = TypeVar("R")


class WorkflowInformation(TypedDict):
    workflow_uuid: str
    status: WorkflowStatuses  # The status of the workflow.
    name: str  # The name of the workflow function.
    workflow_class_name: str  # The class name holding the workflow function.
    workflow_config_name: (
        str  # The name of the configuration, if the class needs configuration
    )
    authenticated_user: str  # The user who ran the workflow. Empty string if not set.
    assumed_role: str
    # The role used to run this workflow.  Empty string if authorization is not required.
    authenticatedRoles: List[str]
    # All roles the authenticated user has, if any.
    input: Optional[WorkflowInputs]
    output: Optional[str]
    error: Optional[str]
    request: Optional[str]


class WorkflowHandle(Generic[R]):

    def __init__(self, workflow_uuid: str, future: Future[R]):
        self.workflow_uuid = workflow_uuid
        self.future = future

    def get_workflow_uuid(self) -> str:
        return self.workflow_uuid

    def get_result(self) -> R:
        return self.future.result()


class GetWorkflowsInput:
    name: Optional[str]  # The name of the workflow function
    authenticated_user: Optional[str]  # The user who ran the workflow.
    start_time: Optional[str]  # Timestamp in ISO 8601 format
    endTime: Optional[str]  # Timestamp in ISO 8601 format
    status: Optional[WorkflowStatuses]
    application_version: Optional[
        str
    ]  # The application version that ran this workflow.
    limit: Optional[
        int
    ]  # Return up to this many workflows IDs. IDs are ordered by workflow creation time.


class GetWorkflowsOutput:
    workflow_uuids: List[str]
