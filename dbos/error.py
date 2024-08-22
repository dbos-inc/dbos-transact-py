from enum import Enum
from typing import Optional


class DBOSException(Exception):
    def __init__(self, message: str, dbos_error_code: Optional[int] = None):
        self.message = message
        self.dbos_error_code = dbos_error_code
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.dbos_error_code:
            return f"DBOS Error {self.dbos_error_code}: {self.message}"
        return f"DBOS Error: {self.message}"


class DBOSErrorCode(Enum):
    ConflictingUUIDError = 1
    RecoveryError = 2
    InitializationError = 3
    WorkflowFunctionNotFound = 4
    NonExistentWorkflowError = 5
    DuplicateWorkflowEventError = 6
    CommunicatorMaxRetriesExceeded = 7
    NotAuthorized = 8


class DBOSWorkflowConflictUUIDError(DBOSException):
    def __init__(self, workflow_uuid: str):
        super().__init__(
            f"Conflicting workflow UUID {workflow_uuid}",
            dbos_error_code=DBOSErrorCode.ConflictingUUIDError.value,
        )


class DBOSRecoveryError(DBOSException):
    def __init__(self, workflow_uuid: str, message: Optional[str] = None):
        super().__init__(
            f"Recovery error for workflow UUID {workflow_uuid}: {message}",
            dbos_error_code=DBOSErrorCode.RecoveryError.value,
        )


class DBOSInitializationError(DBOSException):
    def __init__(self, message: str):
        super().__init__(
            f"Error initializing DBOS Transact: {message}",
            DBOSErrorCode.InitializationError.value,
        )


class DBOSWorkflowFunctionNotFoundError(DBOSException):
    def __init__(self, workflow_uuid: str, message: Optional[str] = None):
        super().__init__(
            f"Workflow function not found for workflow UUID {workflow_uuid}: {message}",
            dbos_error_code=DBOSErrorCode.WorkflowFunctionNotFound.value,
        )


class DBOSNonExistentWorkflowError(DBOSException):
    def __init__(self, destination_uuid: str):
        super().__init__(
            f"Sent to non-existent destination workflow UUID: {destination_uuid}",
            dbos_error_code=DBOSErrorCode.NonExistentWorkflowError.value,
        )


class DBOSDuplicateWorkflowEventError(DBOSException):
    def __init__(self, workflow_uuid: str, key: str):
        super().__init__(
            f"Workflow {workflow_uuid} has already emitted an event with key {key}",
            dbos_error_code=DBOSErrorCode.DuplicateWorkflowEventError.value,
        )


class DBOSNotAuthorizedError(DBOSException):
    def __init__(self, msg: str):
        super().__init__(
            msg,
            dbos_error_code=DBOSErrorCode.NotAuthorized.value,
        )


class DBOSCommunicatorMaxRetriesExceededError(DBOSException):
    def __init__(self) -> None:
        super().__init__(
            "Communicator reached maximum retries.",
            dbos_error_code=DBOSErrorCode.CommunicatorMaxRetriesExceeded.value,
        )
