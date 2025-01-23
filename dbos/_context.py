from __future__ import annotations

import json
import os
import uuid
from contextlib import AbstractContextManager
from contextvars import ContextVar
from enum import Enum
from types import TracebackType
from typing import List, Literal, Optional, Type, TypedDict

from opentelemetry.trace import Span, Status, StatusCode
from sqlalchemy.orm import Session

from ._logger import dbos_logger
from ._request import Request
from ._tracer import dbos_tracer


# These are used to tag OTel traces
class OperationType(Enum):
    HANDLER = "handler"
    WORKFLOW = "workflow"
    TRANSACTION = "transaction"
    STEP = "step"
    PROCEDURE = "procedure"


OperationTypes = Literal["handler", "workflow", "transaction", "step", "procedure"]


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
    authenticatedUser: Optional[str]
    authenticatedUserRoles: Optional[str]
    authenticatedUserAssumedRole: Optional[str]


class DBOSContext:
    def __init__(self) -> None:
        self.executor_id = os.environ.get("DBOS__VMID", "local")
        self.app_version = os.environ.get("DBOS__APPVERSION", "")
        self.app_id = os.environ.get("DBOS__APPID", "")

        self.logger = dbos_logger

        self.request: Optional["Request"] = None

        self.id_assigned_for_next_workflow: str = ""
        self.is_within_set_workflow_id_block: bool = False

        self.parent_workflow_id: str = ""
        self.parent_workflow_fid: int = -1
        self.workflow_id: str = ""
        self.function_id: int = -1
        self.in_recovery: bool = False

        self.curr_step_function_id: int = -1
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
        rv.is_within_set_workflow_id_block = self.is_within_set_workflow_id_block
        rv.parent_workflow_id = self.workflow_id
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
            if self.is_within_set_workflow_id_block:
                self.logger.warning(
                    f"Multiple workflows started in the same SetWorkflowID block. Only the first workflow is assigned the specified workflow ID; subsequent workflows will use a generated workflow ID."
                )
            wfid = str(uuid.uuid4())
        return wfid

    def start_workflow(
        self,
        wfid: Optional[str],
        attributes: TracedAttributes,
        is_temp_workflow: bool = False,
    ) -> None:
        if wfid is None or len(wfid) == 0:
            wfid = self.assign_workflow_id()
            self.id_assigned_for_next_workflow = ""
        self.workflow_id = wfid
        self.function_id = 0
        if not is_temp_workflow:
            self._start_span(attributes)

    def end_workflow(
        self, exc_value: Optional[BaseException], is_temp_workflow: bool = False
    ) -> None:
        self.workflow_id = ""
        self.function_id = -1
        if not is_temp_workflow:
            self._end_span(exc_value)

    def is_within_workflow(self) -> bool:
        return len(self.workflow_id) > 0

    def is_workflow(self) -> bool:
        return (
            len(self.workflow_id) > 0
            and not self.is_step()
            and not self.is_transaction()
        )

    def is_transaction(self) -> bool:
        return self.sql_session is not None

    def is_step(self) -> bool:
        return self.curr_step_function_id >= 0

    def start_step(
        self,
        fid: int,
        attributes: TracedAttributes,
    ) -> None:
        self.curr_step_function_id = fid
        self._start_span(attributes)

    def end_step(self, exc_value: Optional[BaseException]) -> None:
        self.curr_step_function_id = -1
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
            self.workflow_id if len(self.workflow_id) > 0 else None
        )
        attributes["authenticatedUser"] = self.authenticated_user
        attributes["authenticatedUserRoles"] = (
            json.dumps(self.authenticated_roles)
            if self.authenticated_roles is not None
            else ""
        )
        attributes["authenticatedUserAssumedRole"] = self.assumed_role
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
        if user is not None and len(self.spans) > 0:
            self.spans[-1].set_attribute("authenticatedUser", user)
            self.spans[-1].set_attribute(
                "authenticatedUserRoles", json.dumps(roles) if roles is not None else ""
            )


##############################################################
##### Low-level context management (using contextvars)
##############################################################


_dbos_context_var: ContextVar[Optional[DBOSContext]] = ContextVar(
    "dbos_context", default=None
)


def _set_local_dbos_context(ctx: Optional[DBOSContext]) -> None:
    _dbos_context_var.set(ctx)


def _clear_local_dbos_context() -> None:
    _dbos_context_var.set(None)


def get_local_dbos_context() -> Optional[DBOSContext]:
    return _dbos_context_var.get()


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

    def __enter__(self) -> DBOSContext:
        # Code to create a basic context
        ctx = get_local_dbos_context()
        if ctx is None:
            self.created_ctx = True
            _set_local_dbos_context(DBOSContext())
        return assert_current_dbos_context()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        # Code to clean up the basic context if we created it
        if self.created_ctx:
            _clear_local_dbos_context()
        return False  # Did not handle


class DBOSContextSwap:
    def __init__(self, ctx: DBOSContext) -> None:
        self.next_ctx = ctx
        self.prev_ctx: Optional[DBOSContext] = None

    def __enter__(self) -> DBOSContextSwap:
        self.prev_ctx = get_local_dbos_context()
        _set_local_dbos_context(self.next_ctx)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        assert get_local_dbos_context() == self.next_ctx
        _set_local_dbos_context(self.prev_ctx)
        return False  # Did not handle


class SetWorkflowID:
    """
    Set the workflow ID to be used for the enclosed workflow invocation. Note: Only the first workflow will be started with the specified workflow ID within a `with SetWorkflowID` block.

    Typical Usage
        ```
        with SetWorkflowID(<workflow ID>):
            result = workflow_function(...)
        ```

        or
        ```
        with SetWorkflowID(<workflow ID>):
            wf_handle = start_workflow(workflow_function, ...)
        ```
    """

    def __init__(self, wfid: str) -> None:
        self.created_ctx = False
        self.wfid = wfid

    def __enter__(self) -> SetWorkflowID:
        # Code to create a basic context
        ctx = get_local_dbos_context()
        if ctx is None:
            self.created_ctx = True
            _set_local_dbos_context(DBOSContext())
        ctx = assert_current_dbos_context()
        ctx.id_assigned_for_next_workflow = self.wfid
        ctx.is_within_set_workflow_id_block = True
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        # Code to clean up the basic context if we created it
        assert_current_dbos_context().is_within_set_workflow_id_block = False
        if self.created_ctx:
            _clear_local_dbos_context()
        return False  # Did not handle


class SetWorkflowRecovery:
    def __init__(self) -> None:
        self.created_ctx = False

    def __enter__(self) -> SetWorkflowRecovery:
        # Code to create a basic context
        ctx = get_local_dbos_context()
        if ctx is None:
            self.created_ctx = True
            _set_local_dbos_context(DBOSContext())
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
            _clear_local_dbos_context()
        return False  # Did not handle


class EnterDBOSWorkflow(AbstractContextManager[DBOSContext, Literal[False]]):
    def __init__(self, attributes: TracedAttributes) -> None:
        self.created_ctx = False
        self.attributes = attributes
        self.is_temp_workflow = attributes["name"] == "temp_wf"

    def __enter__(self) -> DBOSContext:
        # Code to create a basic context
        ctx = get_local_dbos_context()
        if ctx is None:
            self.created_ctx = True
            ctx = DBOSContext()
            _set_local_dbos_context(ctx)
        assert not ctx.is_within_workflow()
        ctx.start_workflow(
            None, self.attributes, self.is_temp_workflow
        )  # Will get from the context's next workflow ID
        return ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        assert ctx.is_within_workflow()
        ctx.end_workflow(exc_value, self.is_temp_workflow)
        # Code to clean up the basic context if we created it
        if self.created_ctx:
            _clear_local_dbos_context()
        return False  # Did not handle


class EnterDBOSChildWorkflow(AbstractContextManager[DBOSContext, Literal[False]]):
    def __init__(self, attributes: TracedAttributes) -> None:
        self.parent_ctx: Optional[DBOSContext] = None
        self.child_ctx: Optional[DBOSContext] = None
        self.attributes = attributes

    def __enter__(self) -> DBOSContext:
        ctx = assert_current_dbos_context()
        self.parent_ctx = ctx
        assert ctx.is_workflow()  # Is in a workflow and not in a step
        ctx.function_id += 1
        if len(ctx.id_assigned_for_next_workflow) == 0:
            ctx.id_assigned_for_next_workflow = (
                ctx.workflow_id + "-" + str(ctx.function_id)
            )
        self.child_ctx = ctx.create_child()
        _set_local_dbos_context(self.child_ctx)
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
        _set_local_dbos_context(self.parent_ctx)
        return False  # Did not handle


class EnterDBOSStep:
    def __init__(
        self,
        attributes: TracedAttributes,
    ) -> None:
        self.attributes = attributes

    def __enter__(self) -> DBOSContext:
        ctx = assert_current_dbos_context()
        assert ctx.is_workflow()
        ctx.function_id += 1
        ctx.start_step(ctx.function_id, attributes=self.attributes)
        return ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        assert ctx.is_step()
        ctx.end_step(exc_value)
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
            _set_local_dbos_context(DBOSContext())
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
            _clear_local_dbos_context()
        return False  # Did not handle


class DBOSContextSetAuth(DBOSContextEnsure):
    def __init__(self, user: Optional[str], roles: Optional[List[str]]) -> None:
        self.created_ctx = False
        self.user = user
        self.roles = roles
        self.prev_user: Optional[str] = None
        self.prev_roles: Optional[List[str]] = None

    def __enter__(self) -> DBOSContext:
        ctx = get_local_dbos_context()
        if ctx is None:
            self.created_ctx = True
            _set_local_dbos_context(DBOSContext())
        ctx = assert_current_dbos_context()
        self.prev_user = ctx.authenticated_user
        self.prev_roles = ctx.authenticated_roles
        ctx.set_authentication(self.user, self.roles)
        return ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        ctx.set_authentication(self.prev_user, self.prev_roles)
        # Clean up the basic context if we created it
        if self.created_ctx:
            _clear_local_dbos_context()
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
