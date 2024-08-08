import os
from types import TracebackType
import uuid
from typing import Literal, Optional, Type

from .logger import dbos_logger

from sqlalchemy.orm import Session

from __future__ import annotations


class DBOSContext:
    def __init__(self) -> None:
        self.executor_id = os.environ.get("DBOS__VMID", "local")
        self.app_version = os.environ.get("DBOS__APPVERSION", "")
        self.app_id = os.environ.get("DBOS__APPID", "")

        self.logger = dbos_logger

        self.function_id: int = -1

        self.next_workflow_uuid: str = ""
        self.workflow_uuid: str = ""

        self.curr_comm_function_id: int = -1
        self.curr_tx_function_id: int = -1
        self.sql_session: Optional[Session] = None

    def start_workflow(self, wfid: Optional[str]) -> None:
        if wfid is None or len(wfid) == 0:
            if len(self.next_workflow_uuid) > 0:
                wfid = self.next_workflow_uuid
            else:
                wfid = str(uuid.uuid4())
        self.workflow_uuid = wfid

    def end_workflow(self) -> None:
        self.workflow_uuid = ""

    def is_in_workflow(self) -> bool:
        return len(self.workflow_uuid) > 0

    def is_workflow(self) -> bool:
        return (
            len(self.workflow_uuid) > 0
            and not self.is_communicator()
            and not self.is_transaction()
        )

    def is_transaction(self) -> bool:
        return self.sql_session is not None

    def is_communicator(self) -> bool:
        return self.curr_comm_function_id >= 0

    def start_communicator(self, fid: int) -> None:
        self.curr_comm_function_id = fid

    def end_communicator(self) -> None:
        self.curr_comm_function_id = -1

    def start_transaction(self, ses: Session, fid: int) -> None:
        self.sql_session = ses
        self.curr_tx_function_id = fid

    def end_transaction(self) -> None:
        self.sql_session = None
        self.curr_tx_function_id = -1


# TODO the tss / ContextVar code


def getCurrentDBOSContext() -> Optional[DBOSContext]:
    return None


def assertCurrentDBOSContext() -> DBOSContext:
    rv = getCurrentDBOSContext()
    assert rv
    return rv


def setCurrentDBOSContext(ctx: DBOSContext) -> None:
    return


class DBOSContextEnsure:
    def __init__(self) -> None:
        self.createdCtx = False

    def __enter__(self) -> DBOSContextEnsure:
        # Code to create a basic context
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        # Code to clean up the basic context if we created it
        return False  # Did not handle


# Set next WFID
class SetWorkflowUUID:
    def __init__(self, wfid: str) -> None:
        self.createdCtx = False
        self.wfid = wfid

    def __enter__(self) -> SetWorkflowUUID:
        # Code to create a basic context if there is not one
        # Set the next wfid
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        # Code to clean up the basic context if we created it
        return False  # Did not handle


# TODO Enter WF / TX / Comm
