from __future__ import annotations

import os
import uuid
from contextvars import ContextVar
from enum import Enum
from types import TracebackType
from typing import TYPE_CHECKING, List, Literal, Optional, Type, TypedDict

from opentelemetry.trace import Span, Status, StatusCode

if TYPE_CHECKING:
    from .fastapi import Request

from sqlalchemy.orm import Session

from .logger import dbos_logger
from .tracer import dbos_tracer


# Values must be the same as in TypeScript Transact
class OperationType(Enum):
    HANDLER = "handler"
    WORKFLOW = "workflow"
    TRANSACTION = "transaction"
    COMMUNICATOR = "communicator"
    PROCEDURE = "procedure"


OperationTypes = Literal[
    "handler", "workflow", "transaction", "communicator", "procedure"
]


# Keys must be the same as in TypeScript Transact
class TracedAttributes(TypedDict, total=False):
    name: str
    operationUUID: Optional[str]
    operationType: Optional[OperationTypes]
    requestID: Optional[str]
    requestIP: Optional[str]
    requestURL: Optional[str]
    requestMethod: Optional[str]
    applicationID: Optional[str]
    applicationVersion: Optional[str]
    executorID: Optional[str]


class DBOSContext:
    def __init__(self) -> None:
        self.executor_id = os.environ.get("DBOS__VMID", "local")
        self.app_version = os.environ.get("DBOS__APPVERSION", "")
        self.app_id = os.environ.get("DBOS__APPID", "")

        self.logger = dbos_logger

        self.request: Optional["Request"] = None

        self.id_assigned_for_next_workflow: str = ""

        self.parent_workflow_uuid: str = ""
        self.parent_workflow_fid: int = -1
        self.workflow_uuid: str = ""
        self.function_id: int = -1
        self.in_recovery: bool = False

        self.curr_comm_function_id: int = -1
        self.curr_tx_function_id: int = -1
        self.sql_session: Optional[Session] = None
        self.spans: list[Span] = []

        self.authenticated_user: Optional[str] = None
        self.authenticated_roles: Optional[List[str]] = None
        self.assumed_role: Optional[str] = None

    def create_child(self) -> DBOSContext:
        rv = DBOSContext()
        rv.logger = self.logger
        rv.id_assigned_for_next_workflow = self.id_assigned_for_next_workflow
        self.id_assigned_for_next_workflow = ""
        rv.parent_workflow_uuid = self.workflow_uuid
        rv.parent_workflow_fid = self.function_id
        rv.in_recovery = self.in_recovery
        rv.authenticated_user = self.authenticated_user
        rv.authenticated_roles = (
            self.authenticated_roles[:]
            if self.authenticated_roles is not None
            else None
        )
        rv.request = self.request
        rv.assumed_role = self.assumed_role
        return rv

    def assign_workflow_id(self) -> str:
        if len(self.id_assigned_for_next_workflow) > 0:
            wfid = self.id_assigned_for_next_workflow
        else:
            wfid = str(uuid.uuid4())
        return wfid

    def start_workflow(self, wfid: Optional[str], attributes: TracedAttributes) -> None:
        if wfid is None or len(wfid) == 0:
            wfid = self.assign_workflow_id()
            self.id_assigned_for_next_workflow = ""
        self.workflow_uuid = wfid
        self.function_id = 0
        self._start_span(attributes)

    def end_workflow(self, exc_value: Optional[BaseException]) -> None:
        self.workflow_uuid = ""
        self.function_id = -1
        self._end_span(exc_value)

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

    def start_communicator(
        self,
        fid: int,
        attributes: TracedAttributes,
    ) -> None:
        self.curr_comm_function_id = fid
        self._start_span(attributes)

    def end_communicator(self, exc_value: Optional[BaseException]) -> None:
        self.curr_comm_function_id = -1
        self._end_span(exc_value)

    def start_transaction(
        self, ses: Session, fid: int, attributes: TracedAttributes
    ) -> None:
        self.sql_session = ses
        self.curr_tx_function_id = fid
        self._start_span(attributes)

    def end_transaction(self, exc_value: Optional[BaseException]) -> None:
        self.sql_session = None
        self.curr_tx_function_id = -1
        self._end_span(exc_value)

    def start_handler(self, attributes: TracedAttributes) -> None:
        self._start_span(attributes)

    def end_handler(self, exc_value: Optional[BaseException]) -> None:
        self._end_span(exc_value)

    def get_current_span(self) -> Span:
        return self.spans[-1]

    def _start_span(self, attributes: TracedAttributes) -> None:
        attributes["operationUUID"] = (
            self.workflow_uuid if len(self.workflow_uuid) > 0 else None
        )
        span = dbos_tracer.start_span(
            attributes, parent=self.spans[-1] if len(self.spans) > 0 else None
        )
        self.spans.append(span)

    def _end_span(self, exc_value: Optional[BaseException]) -> None:
        if exc_value is None:
            self.spans[-1].set_status(Status(StatusCode.OK))
        else:
            self.spans[-1].set_status(
                Status(StatusCode.ERROR, description=str(exc_value))
            )
        dbos_tracer.end_span(self.spans.pop())

    def set_authentication(
        self, user: Optional[str], roles: Optional[List[str]]
    ) -> None:
        self.authenticated_user = user
        self.authenticated_roles = roles


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
    assert rv, "No DBOS context found"
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


class SetWorkflowRecovery:
    def __init__(self) -> None:
        self.created_ctx = False

    def __enter__(self) -> SetWorkflowRecovery:
        # Code to create a basic context
        ctx = get_local_dbos_context()
        if ctx is None:
            self.created_ctx = True
            set_local_dbos_context(DBOSContext())
        assert_current_dbos_context().in_recovery = True

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        assert assert_current_dbos_context().in_recovery == True
        assert_current_dbos_context().in_recovery = False
        # Code to clean up the basic context if we created it
        if self.created_ctx:
            clear_local_dbos_context()
        return False  # Did not handle


class EnterDBOSWorkflow:
    def __init__(self, attributes: TracedAttributes) -> None:
        self.created_ctx = False
        self.attributes = attributes

    def __enter__(self) -> DBOSContext:
        # Code to create a basic context
        ctx = get_local_dbos_context()
        if ctx is None:
            self.created_ctx = True
            ctx = DBOSContext()
            set_local_dbos_context(ctx)
        assert not ctx.is_within_workflow()
        ctx.start_workflow(
            None, self.attributes
        )  # Will get from the context's next wf uuid
        return ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        assert ctx.is_within_workflow()
        ctx.end_workflow(exc_value)
        # Code to clean up the basic context if we created it
        if self.created_ctx:
            clear_local_dbos_context()
        return False  # Did not handle


class EnterDBOSChildWorkflow:
    def __init__(self, attributes: TracedAttributes) -> None:
        self.parent_ctx: Optional[DBOSContext] = None
        self.child_ctx: Optional[DBOSContext] = None
        self.attributes = attributes

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
        self.child_ctx.start_workflow(None, attributes=self.attributes)
        return self.child_ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        assert ctx.is_within_workflow()
        ctx.end_workflow(exc_value)
        # Return to parent ctx
        assert self.parent_ctx
        set_local_dbos_context(self.parent_ctx)
        return False  # Did not handle


class EnterDBOSCommunicator:
    def __init__(
        self,
        attributes: TracedAttributes,
    ) -> None:
        self.attributes = attributes

    def __enter__(self) -> DBOSContext:
        ctx = assert_current_dbos_context()
        assert ctx.is_workflow()
        ctx.function_id += 1
        ctx.start_communicator(ctx.function_id, attributes=self.attributes)
        return ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        assert ctx.is_communicator()
        ctx.end_communicator(exc_value)
        return False  # Did not handle


class EnterDBOSTransaction:
    def __init__(self, sqls: Session, attributes: TracedAttributes) -> None:
        self.sqls = sqls
        self.attributes = attributes

    def __enter__(self) -> DBOSContext:
        ctx = assert_current_dbos_context()
        assert ctx.is_workflow()
        ctx.function_id += 1
        ctx.start_transaction(self.sqls, ctx.function_id, attributes=self.attributes)
        return ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        assert ctx.is_transaction()
        ctx.end_transaction(exc_value)
        return False  # Did not handle


class EnterDBOSHandler:
    def __init__(self, attributes: TracedAttributes) -> None:
        self.created_ctx = False
        self.attributes = attributes

    def __enter__(self) -> EnterDBOSHandler:
        # Code to create a basic context
        ctx = get_local_dbos_context()
        if ctx is None:
            self.created_ctx = True
            set_local_dbos_context(DBOSContext())
        ctx = assert_current_dbos_context()
        ctx.start_handler(self.attributes)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        ctx.end_handler(exc_value)
        # Code to clean up the basic context if we created it
        if self.created_ctx:
            clear_local_dbos_context()
        return False  # Did not handle


class DBOSAssumeRole:
    def __init__(self, assume_role: Optional[str]) -> None:
        self.prior_role: Optional[str] = None
        self.assume_role = assume_role

    def __enter__(self) -> DBOSAssumeRole:
        ctx = assert_current_dbos_context()
        self.prior_role = ctx.assumed_role
        if self.assume_role is not None:
            ctx.assumed_role = self.assume_role
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        if self.assume_role is not None:
            assert ctx.assumed_role == self.assume_role
        ctx.assumed_role = self.prior_role
        return False  # Did not handle
