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
