from enum import Enum
from typing import TYPE_CHECKING, Literal, Optional


class WorkflowStatusString(Enum):
    """Enumeration of values allowed for `WorkflowStatus.status`."""

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    RETRIES_EXCEEDED = "RETRIES_EXCEEDED"
    CANCELLED = "CANCELLED"
    ENQUEUED = "ENQUEUED"


WorkflowStatuses = Literal[
    "PENDING", "SUCCESS", "ERROR", "RETRIES_EXCEEDED", "CANCELLED", "ENQUEUED"
]


class GetWorkflowsInput:
    """
    Structure for argument to `get_workflows` function.

    This specifies the search criteria for workflow retrieval by `get_workflows`.

    Attributes:
       name(str):  The name of the workflow function
       authenticated_user(str):  The name of the user who invoked the function
       start_time(str): Beginning of search range for time of invocation, in ISO 8601 format
       end_time(str): End of search range for time of invocation, in ISO 8601 format
       status(str): Current status of the workflow invocation (see `WorkflowStatusString`)
       application_version(str): Application version that invoked the workflow
       limit(int): Limit on number of returned records

    """

    def __init__(self) -> None:
        self.name: Optional[str] = None  # The name of the workflow function
        self.authenticated_user: Optional[str] = None  # The user who ran the workflow.
        self.start_time: Optional[str] = None  # Timestamp in ISO 8601 format
        self.end_time: Optional[str] = None  # Timestamp in ISO 8601 format
        self.status: Optional[WorkflowStatuses] = None
        self.application_version: Optional[str] = (
            None  # The application version that ran this workflow. = None
        )
        self.limit: Optional[int] = (
            None  # Return up to this many workflows IDs. IDs are ordered by workflow creation time.
        )
