"""Errors thrown by DBOS."""

from enum import Enum
from typing import Optional


class DBOSException(Exception):
    """
    Base class of DBOS Exceptions.

    Attributes:
        message(str): The error message string
        dbos_error_code(DBOSErrorCode): The error code, from the `DBOSErrorCode` enum

    """

    def __init__(self, message: str, dbos_error_code: Optional[int] = None):
        self.message = message
        self.dbos_error_code = dbos_error_code
        self.status_code: Optional[int] = None
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.dbos_error_code:
            return f"DBOS Error {self.dbos_error_code}: {self.message}"
        return f"DBOS Error: {self.message}"


class DBOSErrorCode(Enum):
    ConflictingIDError = 1
    RecoveryError = 2
    InitializationError = 3
    WorkflowFunctionNotFound = 4
    NonExistentWorkflowError = 5
    DeadLetterQueueError = 6
    MaxStepRetriesExceeded = 7
    NotAuthorized = 8
    ConflictingWorkflowError = 9


class DBOSWorkflowConflictIDError(DBOSException):
    """Exception raised when a workflow database record already exists."""

    def __init__(self, workflow_id: str):
        super().__init__(
            f"Conflicting workflow ID {workflow_id}",
            dbos_error_code=DBOSErrorCode.ConflictingIDError.value,
        )


class DBOSConflictingWorkflowError(DBOSException):
    """Exception raised different workflows started with the same workflow ID."""

    def __init__(self, workflow_id: str, message: Optional[str] = None):
        super().__init__(
            f"Conflicting workflow invocation with the same ID ({workflow_id}): {message}",
            dbos_error_code=DBOSErrorCode.ConflictingWorkflowError.value,
        )


class DBOSRecoveryError(DBOSException):
    """Exception raised when a workflow recovery fails."""

    def __init__(self, workflow_id: str, message: Optional[str] = None):
        super().__init__(
            f"Recovery error for workflow ID {workflow_id}: {message}",
            dbos_error_code=DBOSErrorCode.RecoveryError.value,
        )


class DBOSInitializationError(DBOSException):
    """Exception raised when DBOS initialization did not complete."""

    def __init__(self, message: str):
        super().__init__(
            f"Error initializing DBOS Transact: {message}",
            DBOSErrorCode.InitializationError.value,
        )


class DBOSWorkflowFunctionNotFoundError(DBOSException):
    """Exception raised when the database refers to a workflow function that is not registered in the codebase."""

    def __init__(self, workflow_id: str, message: Optional[str] = None):
        super().__init__(
            f"Workflow function not found for workflow ID {workflow_id}: {message}",
            dbos_error_code=DBOSErrorCode.WorkflowFunctionNotFound.value,
        )


class DBOSNonExistentWorkflowError(DBOSException):
    """Exception raised when a workflow database record does not exist for a given ID."""

    def __init__(self, destination_id: str):
        super().__init__(
            f"Sent to non-existent destination workflow ID: {destination_id}",
            dbos_error_code=DBOSErrorCode.NonExistentWorkflowError.value,
        )


class DBOSDeadLetterQueueError(DBOSException):
    """Exception raised when a workflow database record does not exist for a given ID."""

    def __init__(self, wf_id: str, max_retries: int):
        super().__init__(
            f"Workflow {wf_id} has been moved to the dead-letter queue after exceeding the maximum of ${max_retries} retries",
            dbos_error_code=DBOSErrorCode.DeadLetterQueueError.value,
        )


class DBOSNotAuthorizedError(DBOSException):
    """Exception raised by DBOS role-based security when the user is not authorized to access a function."""

    def __init__(self, msg: str):
        super().__init__(
            msg,
            dbos_error_code=DBOSErrorCode.NotAuthorized.value,
        )
        self.status_code = 403


class DBOSMaxStepRetriesExceeded(DBOSException):
    """Exception raised when a step was retried the maximimum number of times without success."""

    def __init__(self) -> None:
        super().__init__(
            "Step reached maximum retries.",
            dbos_error_code=DBOSErrorCode.MaxStepRetriesExceeded.value,
        )
