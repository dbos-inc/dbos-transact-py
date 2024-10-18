from typing import Any, Dict, List, Optional, Tuple, TypedDict

from ..types import WorkflowStatuses


class TransactionResultInternal(TypedDict):
    workflow_uuid: str
    function_id: int
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)
    txn_id: Optional[str]
    txn_snapshot: str
    executor_id: Optional[str]


class WorkflowStatusInternal(TypedDict):
    workflow_uuid: str
    status: WorkflowStatuses
    name: str
    class_name: Optional[str]
    config_name: Optional[str]
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)
    executor_id: Optional[str]
    app_version: Optional[str]
    app_id: Optional[str]
    request: Optional[str]  # JSON (jsonpickle)
    recovery_attempts: Optional[int]
    authenticated_user: Optional[str]
    assumed_role: Optional[str]
    authenticated_roles: Optional[str]  # JSON list of roles.
    queue_name: Optional[str]


class RecordedResult(TypedDict):
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)


class GetWorkflowsOutput:
    def __init__(self, workflow_uuids: List[str]):
        self.workflow_uuids = workflow_uuids


class WorkflowInputs(TypedDict):
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


class WorkflowInformation(TypedDict, total=False):
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
    authenticated_roles: List[str]
    # All roles the authenticated user has, if any.
    input: Optional[WorkflowInputs]
    output: Optional[str]
    error: Optional[str]
    request: Optional[str]


class OperationResultInternal(TypedDict):
    workflow_uuid: str
    function_id: int
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)


class GetEventWorkflowContext(TypedDict):
    workflow_uuid: str
    function_id: int
    timeout_function_id: int
