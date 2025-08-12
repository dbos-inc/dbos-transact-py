"""Errors thrown by DBOS."""

from enum import Enum
from typing import Any, Optional


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


class DBOSBaseException(BaseException):
    """
    This class is for DBOS exceptions that should not be caught by user code.
    It inherits from BaseException instead of Exception so it cannot be caught
    except by code specifically trying to catch it.

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
    MaxRecoveryAttemptsExceeded = 6
    MaxStepRetriesExceeded = 7
    NotAuthorized = 8
    ConflictingWorkflowError = 9
    WorkflowCancelled = 10
    UnexpectedStep = 11
    QueueDeduplicated = 12
    AwaitedWorkflowCancelled = 13
    ConflictingRegistrationError = 25


#######################################
## Exception
#######################################


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
            f"Could not execute workflow {workflow_id}: {message}",
            dbos_error_code=DBOSErrorCode.WorkflowFunctionNotFound.value,
        )


class DBOSNonExistentWorkflowError(DBOSException):
    """Exception raised when a workflow database record does not exist for a given ID."""

    def __init__(self, destination_id: str):
        super().__init__(
            f"Sent to non-existent destination workflow ID: {destination_id}",
            dbos_error_code=DBOSErrorCode.NonExistentWorkflowError.value,
        )


class MaxRecoveryAttemptsExceededError(DBOSException):
    """Exception raised when a workflow exceeds its max recovery attempts."""

    def __init__(self, wf_id: str, max_retries: int):
        super().__init__(
            f"Workflow {wf_id} has exceeded its maximum of {max_retries} execution or recovery attempts. Further attempts to execute or recover it will fail. See documentation for details: https://docs.dbos.dev/python/reference/decorators",
            dbos_error_code=DBOSErrorCode.MaxRecoveryAttemptsExceeded.value,
        )


class DBOSNotAuthorizedError(DBOSException):
    """Exception raised by DBOS role-based security when the user is not authorized to access a function."""

    def __init__(self, msg: str):
        self.msg = msg
        super().__init__(
            msg,
            dbos_error_code=DBOSErrorCode.NotAuthorized.value,
        )
        self.status_code = 403

    def __reduce__(self) -> Any:
        # Tell jsonpickle how to reconstruct this object
        return (self.__class__, (self.msg,))


class DBOSMaxStepRetriesExceeded(DBOSException):
    """Exception raised when a step was retried the maximimum number of times without success."""

    def __init__(
        self, step_name: str, max_retries: int, errors: list[Exception]
    ) -> None:
        self.step_name = step_name
        self.max_retries = max_retries
        self.errors = errors
        super().__init__(
            f"Step {step_name} has exceeded its maximum of {max_retries} retries",
            dbos_error_code=DBOSErrorCode.MaxStepRetriesExceeded.value,
        )

    def __reduce__(self) -> Any:
        # Tell jsonpickle how to reconstruct this object
        return (self.__class__, (self.step_name, self.max_retries, self.errors))


class DBOSConflictingRegistrationError(DBOSException):
    """Exception raised when conflicting decorators are applied to the same function."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"Operation (Name: {name}) is already registered with a conflicting function type",
            dbos_error_code=DBOSErrorCode.ConflictingRegistrationError.value,
        )


class DBOSUnexpectedStepError(DBOSException):
    """Exception raised when a step has an unexpected recorded name."""

    def __init__(
        self, workflow_id: str, step_id: int, expected_name: str, recorded_name: str
    ) -> None:
        super().__init__(
            f"During execution of workflow {workflow_id} step {step_id}, function {recorded_name} was recorded when {expected_name} was expected. Check that your workflow is deterministic.",
            dbos_error_code=DBOSErrorCode.UnexpectedStep.value,
        )


class DBOSQueueDeduplicatedError(DBOSException):
    """Exception raised when a workflow is deduplicated in the queue."""

    def __init__(
        self, workflow_id: str, queue_name: str, deduplication_id: str
    ) -> None:
        self.workflow_id = workflow_id
        self.queue_name = queue_name
        self.deduplication_id = deduplication_id
        super().__init__(
            f"Workflow {workflow_id} was deduplicated due to an existing workflow in queue {queue_name} with deduplication ID {deduplication_id}.",
            dbos_error_code=DBOSErrorCode.QueueDeduplicated.value,
        )

    def __reduce__(self) -> Any:
        # Tell jsonpickle how to reconstruct this object
        return (
            self.__class__,
            (self.workflow_id, self.queue_name, self.deduplication_id),
        )


class DBOSAwaitedWorkflowCancelledError(DBOSException):
    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        super().__init__(
            f"Awaited workflow {workflow_id} was cancelled",
            dbos_error_code=DBOSErrorCode.AwaitedWorkflowCancelled.value,
        )

    def __reduce__(self) -> Any:
        # Tell jsonpickle how to reconstruct this object
        return (self.__class__, (self.workflow_id,))


#######################################
## BaseException
#######################################


class DBOSWorkflowCancelledError(DBOSBaseException):
    """BaseException raised when the workflow has already been cancelled."""

    def __init__(self, msg: str) -> None:
        super().__init__(
            msg,
            dbos_error_code=DBOSErrorCode.WorkflowCancelled.value,
        )


class DBOSWorkflowConflictIDError(DBOSBaseException):
    """BaseException raised when a workflow database record already exists."""

    def __init__(self, workflow_id: str):
        super().__init__(
            f"Conflicting workflow ID {workflow_id}",
            dbos_error_code=DBOSErrorCode.ConflictingIDError.value,
        )
