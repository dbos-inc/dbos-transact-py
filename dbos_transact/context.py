from __future__ import annotations

import os
import uuid
from contextvars import ContextVar
from types import TracebackType
from typing import TYPE_CHECKING, Literal, Optional, Type

if TYPE_CHECKING:
    from .fastapi import Request

from sqlalchemy.orm import Session

from .logger import dbos_logger


class DBOSContext:
    def __init__(self) -> None:
        self.executor_id = os.environ.get("DBOS__VMID", "local")
        self.app_version = os.environ.get("DBOS__APPVERSION", "")
        self.app_id = os.environ.get("DBOS__APPID", "")

        self.logger = dbos_logger

        self.request: Optional["Request"] = None

        self.id_assigned_for_next_workflow: str = ""

        self.workflow_uuid: str = ""
        self.function_id: int = -1
        self.parent_workflow_uuid: str = ""
        self.parent_workflow_fid: int = -1

        self.curr_comm_function_id: int = -1
        self.curr_tx_function_id: int = -1
        self.sql_session: Optional[Session] = None

    def create_child(self) -> DBOSContext:
        rv = DBOSContext()
        rv.logger = self.logger
        rv.id_assigned_for_next_workflow = self.id_assigned_for_next_workflow
        self.id_assigned_for_next_workflow = ""
        rv.parent_workflow_uuid = self.workflow_uuid
        rv.parent_workflow_fid = self.function_id
        return rv

    def assign_workflow_id(self) -> str:
        if len(self.id_assigned_for_next_workflow) > 0:
            wfid = self.id_assigned_for_next_workflow
        else:
            wfid = str(uuid.uuid4())
        return wfid

    def start_workflow(self, wfid: Optional[str]) -> None:
        if wfid is None or len(wfid) == 0:
            wfid = self.assign_workflow_id()
            self.id_assigned_for_next_workflow = ""
        self.workflow_uuid = wfid
        self.function_id = 0

    def end_workflow(self) -> None:
        self.workflow_uuid = ""
        self.function_id = -1

    def is_within_workflow(self) -> bool:
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


##############################################################
##### Low-level context management (using contextvars)
##############################################################


dbos_context_var: ContextVar[Optional[DBOSContext]] = ContextVar(
    "dbos_context", default=None
)


def set_local_dbos_context(ctx: Optional[DBOSContext]) -> None:
    dbos_context_var.set(ctx)


def clear_local_dbos_context() -> None:
    dbos_context_var.set(None)


def get_local_dbos_context() -> Optional[DBOSContext]:
    return dbos_context_var.get()


def assert_current_dbos_context() -> DBOSContext:
    rv = get_local_dbos_context()
    assert rv
    return rv


##############################################################
##### High-level context management  (using contextlib)
##############################################################


class DBOSContextEnsure:
    def __init__(self) -> None:
        self.created_ctx = False

    def __enter__(self) -> DBOSContextEnsure:
        # Code to create a basic context
        ctx = get_local_dbos_context()
        if ctx is None:
            self.created_ctx = True
            set_local_dbos_context(DBOSContext())
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        # Code to clean up the basic context if we created it
        if self.created_ctx:
            clear_local_dbos_context()
        return False  # Did not handle


class DBOSContextSwap:
    def __init__(self, ctx: DBOSContext) -> None:
        self.next_ctx = ctx
        self.prev_ctx: Optional[DBOSContext] = None

    def __enter__(self) -> DBOSContextSwap:
        self.prev_ctx = get_local_dbos_context()
        set_local_dbos_context(self.next_ctx)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        assert get_local_dbos_context() == self.next_ctx
        set_local_dbos_context(self.prev_ctx)
        return False  # Did not handle


# Set next WFID
class SetWorkflowUUID:
    def __init__(self, wfid: str) -> None:
        self.created_ctx = False
        self.wfid = wfid

    def __enter__(self) -> SetWorkflowUUID:
        # Code to create a basic context
        ctx = get_local_dbos_context()
        if ctx is None:
            self.created_ctx = True
            set_local_dbos_context(DBOSContext())
        assert_current_dbos_context().id_assigned_for_next_workflow = self.wfid
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        # Code to clean up the basic context if we created it
        if self.created_ctx:
            clear_local_dbos_context()
        return False  # Did not handle


class EnterDBOSWorkflow:
    def __init__(self) -> None:
        self.created_ctx = False

    def __enter__(self) -> DBOSContext:
        # Code to create a basic context
        ctx = get_local_dbos_context()
        if ctx is None:
            self.created_ctx = True
            ctx = DBOSContext()
            set_local_dbos_context(ctx)
        assert not ctx.is_within_workflow()
        ctx.start_workflow(None)  # Will get from the context's next wf uuid
        return ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        assert assert_current_dbos_context().is_within_workflow()
        assert_current_dbos_context().end_workflow()
        # Code to clean up the basic context if we created it
        if self.created_ctx:
            clear_local_dbos_context()
        return False  # Did not handle


class EnterDBOSChildWorkflow:
    def __init__(self) -> None:
        self.parent_ctx: Optional[DBOSContext] = None
        self.child_ctx: Optional[DBOSContext] = None

    def __enter__(self) -> DBOSContext:
        ctx = assert_current_dbos_context()
        self.parent_ctx = ctx
        assert ctx.is_workflow()  # Is in a workflow and not in tx/comm
        ctx.function_id += 1
        if len(ctx.id_assigned_for_next_workflow) == 0:
            ctx.id_assigned_for_next_workflow = (
                ctx.workflow_uuid + "-" + str(ctx.function_id)
            )
        self.child_ctx = ctx.create_child()
        set_local_dbos_context(self.child_ctx)
        self.child_ctx.start_workflow(None)
        return self.child_ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        assert ctx.is_within_workflow()
        ctx.end_workflow()
        # Return to parent ctx
        assert self.parent_ctx
        set_local_dbos_context(self.parent_ctx)
        return False  # Did not handle


class EnterDBOSCommunicator:
    def __init__(self) -> None:
        pass

    def __enter__(self) -> DBOSContext:
        ctx = assert_current_dbos_context()
        assert ctx.is_workflow()
        ctx.function_id += 1
        ctx.start_communicator(ctx.function_id)
        return ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        assert ctx.is_communicator()
        ctx.end_communicator()
        return False  # Did not handle


class EnterDBOSTransaction:
    def __init__(self, sqls: Session) -> None:
        self.sqls = sqls

    def __enter__(self) -> DBOSContext:
        ctx = assert_current_dbos_context()
        assert ctx.is_workflow()
        ctx.function_id += 1
        ctx.start_transaction(self.sqls, ctx.function_id)
        return ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        assert ctx.is_transaction()
        ctx.end_transaction()
        return False  # Did not handle
