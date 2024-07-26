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


ConflictingUUIDError = 1


class DBOSWorkflowConflictUUIDError(DBOSException):
    def __init__(self, workflow_uuid: str):
        super().__init__(
            f"Conflicting UUID {workflow_uuid}", dbos_error_code=ConflictingUUIDError
        )
