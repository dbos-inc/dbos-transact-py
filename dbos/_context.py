from __future__ import annotations

import json
import os
import time
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, replace
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    TypedDict,
    TypeVar,
    Union,
)

from dbos._serialization import WorkflowSerializationFormat
from dbos._shard_keys import clear_shard_key_state, restore_shard_key_state

if TYPE_CHECKING:
    from opentelemetry.context import Context as OtelContext
    from opentelemetry.trace import Span

    from dbos._datasource import AsyncSQLAlchemyDatasource, SQLAlchemyDatasource

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from dbos._utils import GlobalParams, generate_uuid

from ._error import DBOSException
from ._logger import dbos_logger
from ._tracer import dbos_tracer

# These are used to tag OTel traces
OperationTypes = Literal["workflow", "step"]

MaxPriority = 2**31 - 1  # 2,147,483,647
MinPriority = 1

# How to handle a collision with another workflow that has the same deduplication ID on the
# same queue. "reject" (the default) raises DBOSQueueDeduplicatedError; "return-existing"
# returns a handle to the workflow already holding the deduplication ID, discarding the
# colliding caller's arguments.
DuplicationPolicy = Literal["reject", "return-existing"]

# On a workflow ID already in use: "return-existing" (default) attaches to it; "reject" raises DBOSWorkflowIDInUseError.
WorkflowIDReusePolicy = Literal["return-existing", "reject"]

# Reserved workflow attribute holding the trace carrier set by PropagateOtelContext.
OTEL_CARRIER_ATTRIBUTE = "dbos.otelContext"


# Keys must be the same as in TypeScript Transact
class TracedAttributes(TypedDict, total=False):
    name: str
    operationUUID: Optional[str]
    operationType: Optional[OperationTypes]
    applicationID: Optional[str]
    applicationVersion: Optional[str]
    executorID: Optional[str]
    authenticatedUser: Optional[str]
    authenticatedUserRoles: Optional[str]
    authenticatedUserAssumedRole: Optional[str]
    queueName: Optional[str]


@dataclass
class StepStatus:
    """
    Status of a step execution.

    Attributes:
        step_id: The unique ID of this step in its workflow.
        current_attempt: For steps with automatic retries, which attempt number (zero-indexed) is currently executing.
        max_attempts: For steps with automatic retries, the maximum number of attempts that will be made before the step fails.

    """

    step_id: int
    current_attempt: Optional[int]
    max_attempts: Optional[int]


@dataclass
class ContextSpan:
    """
    A span that is used to track the context of a workflow or step execution.

    Attributes:
        span: The OpenTelemetry span object.
        context_manager: The context manager that is used to manage the span's lifecycle.
    """

    span: "Span"
    context_manager: "AbstractContextManager[Span]"


@dataclass
class AssignedWorkflowID:
    """A SetWorkflowID block's ID, taken by the first workflow started in the block."""

    workflow_id: str
    reuse_policy: Optional[WorkflowIDReusePolicy]
    consumed: bool = False


@dataclass(frozen=True)
class WorkflowStartOptions:
    """Options the Set* context managers apply to workflows started or enqueued in their block.
    Held in a task-local ContextVar, not on the DBOSContext that concurrent tasks share.
    """

    assigned_id: Optional[AssignedWorkflowID] = None
    # A user-specified workflow timeout. Takes priority over a propagated deadline.
    workflow_timeout_ms: Optional[int] = None
    # Set when the started workflow must not inherit the caller's propagated deadline.
    ignore_propagated_deadline: bool = False
    # A user-specified deduplication ID for the enqueuing workflow.
    deduplication_id: Optional[str] = None
    # How the enqueuing workflow reacts to a collision on its deduplication ID.
    duplication_policy: Optional[DuplicationPolicy] = None
    # A user-specified priority for the enqueuing workflow.
    priority: Optional[int] = None
    app_version: Optional[str] = None
    # If the workflow is enqueued on a partitioned queue, its partition key
    queue_partition_key: Optional[str] = None
    # The UNIX epoch timestamp before which the workflow should not be dequeued
    delay_until_epoch_ms: Optional[int] = None
    # Absolute cap (Unix epoch ms) beyond which bounces may not extend the next enqueued workflow's delay.
    debounce_deadline_epoch_ms: Optional[int] = None
    # Whether the next enqueued workflow is debounced (its dedup ID is a debounce key cleared on DELAYED->ENQUEUED).
    is_debounced: bool = False
    # The application the next debounced enqueue acts for; None means this one.
    debounce_application_name: Optional[str] = None
    # User-specified attributes to attach to the next started workflow.
    workflow_attributes: Optional[Dict[str, Any]] = None
    # Trace carrier for the next started workflow, set by PropagateOtelContext.
    otel_carrier: Optional[Dict[str, str]] = None
    # Shard key for the next workflow, set by SetShardKey.
    shard_key: Optional[str] = None


_NO_START_OPTIONS = WorkflowStartOptions()


@dataclass(frozen=True)
class Authentication:
    """The identity code acts as: an authenticated user, their granted roles, and the role assumed."""

    user: Optional[str] = None
    roles: Optional[List[str]] = None
    assumed_role: Optional[str] = None


_NO_AUTHENTICATION = Authentication()


def child_workflow_id(
    parent_id: str,
    function_id: int,
    parent_key: Optional[str],
    child_key: Optional[str],
) -> str:
    """A child's default ID: its parent's, suffixed with the call number. A parent ID is
    unique only within its shard key, so a child under another key carries the parent's.
    """
    base = f"{parent_id}-{function_id}"
    if parent_key is None or child_key is None or child_key == parent_key:
        return base
    return f"{len(parent_key)}:{parent_key}:{base}"


class DBOSContext:
    def __init__(self) -> None:
        self.executor_id = GlobalParams.executor_id
        self.app_id = os.environ.get("DBOS__APPID", "")

        self.logger = dbos_logger

        # The ID a context built by create_start_workflow_child will run its workflow under.
        self.id_assigned_for_next_workflow: str = ""
        # (parent ID, call number, parent's shard key) when the ID above is a derived default
        self.derived_from: Optional[Tuple[str, int, Optional[str]]] = None
        # Reuse policy for id_assigned_for_next_workflow, consumed along with it.
        self.workflow_id_reuse_policy: Optional[WorkflowIDReusePolicy] = None
        # The ambient Set* options this context's workflow was started under.
        self.start_options: WorkflowStartOptions = _NO_START_OPTIONS

        self.parent_workflow_id: str = ""
        self.parent_workflow_fid: int = -1
        self.workflow_id: str = ""
        # Shard key of the running workflow; None on an unsharded database.
        self.shard_key: Optional[str] = None
        self.function_id: int = -1
        # Token this execution wrote to the row's owner_xid; checkpoints land only while it still matches.
        self.owner_xid: Optional[str] = None

        self.curr_step_function_id: int = -1
        # Checkpointed stream reads that have reserved a step but not yet recorded it.
        self.active_stream_reads: int = 0
        self.sync_ds_session: Optional[Session] = None
        self.async_ds_session: Optional[AsyncSession] = None
        # Datasources this workflow called; its checkpoints go when it completes.
        self.used_datasources: list[
            Union[SQLAlchemyDatasource, AsyncSQLAlchemyDatasource]
        ] = []
        self.context_spans: list[ContextSpan] = []

        # Identity this context's workflow or step was created under; a DBOSContextSetAuth block can shadow it.
        self.authenticated_user: Optional[str] = None
        self.authenticated_roles: Optional[List[str]] = None
        self.assumed_role: Optional[str] = None
        self.step_status: Optional[StepStatus] = None

        self.serialization_type: WorkflowSerializationFormat = (
            WorkflowSerializationFormat.DEFAULT
        )

        # Deadline of the running workflow, propagated to its children (before it starts: the caller's).
        self.workflow_deadline_epoch_ms: Optional[int] = None

    def create_child(self, *, is_for_workflow: bool) -> DBOSContext:
        rv = DBOSContext()
        rv.logger = self.logger
        rv.parent_workflow_id = self.workflow_id
        rv.parent_workflow_fid = self.function_id
        auth = get_authentication(self)
        rv.authenticated_user = auth.user
        rv.authenticated_roles = auth.roles[:] if auth.roles is not None else None
        rv.assumed_role = auth.assumed_role
        if not is_for_workflow:
            rv.serialization_type = self.serialization_type
        return rv

    def snapshot_step_ctx(self, reserve_sleep_id: bool = False) -> DBOSContext:
        rv = self.create_child(is_for_workflow=False)
        rv.executor_id = self.executor_id
        rv.app_id = self.app_id
        rv.workflow_id = self.workflow_id
        rv.shard_key = self.shard_key
        rv.owner_xid = self.owner_xid
        rv.workflow_deadline_epoch_ms = self.workflow_deadline_epoch_ms
        self.function_id += 1
        rv.function_id = self.function_id
        if reserve_sleep_id:
            self.function_id += 1
        return rv

    @staticmethod
    def create_start_workflow_child(cur_ctx: Optional["DBOSContext"]) -> DBOSContext:
        # Sequence of events for starting a workflow:
        #   First - is there a WF already running?
        #      (and not in step as that is an error)
        #   Assign an ID to the workflow, if it doesn't have an app-assigned one
        #      If this is a root workflow, assign a new ID
        #      If this is a child workflow, assign parent wf id with call# suffix
        #   Make a (system) DB record for the workflow
        #   Pass the new context to a worker thread that will run the wf function
        options = get_workflow_start_options()
        if options.workflow_attributes is not None:
            # Copy so later mutation of the caller's dict cannot affect the child
            options = replace(
                options, workflow_attributes=dict(options.workflow_attributes)
            )
        in_set_workflow_id_block = options.assigned_id is not None
        assigned = take_assigned_workflow_id()
        wfid = assigned.workflow_id if assigned is not None else ""
        reuse_policy = assigned.reuse_policy if assigned is not None else None
        derived_from: Optional[Tuple[str, int, Optional[str]]] = None
        if cur_ctx is not None and cur_ctx.is_within_workflow():
            assert cur_ctx.is_workflow()  # Not in a step
            cur_ctx.function_id += 1
            if not wfid:
                derived_from = (
                    cur_ctx.workflow_id,
                    cur_ctx.function_id,
                    cur_ctx.shard_key,
                )
                wfid = child_workflow_id(*derived_from, options.shard_key)
        if not wfid:
            if in_set_workflow_id_block:
                dbos_logger.warning(
                    f"Multiple workflows started in the same SetWorkflowID block. Only the first workflow is assigned the specified workflow ID; subsequent workflows will use a generated workflow ID."
                )
            wfid = generate_uuid()

        new_wf_ctx = (
            DBOSContext()
            if cur_ctx is None
            else cur_ctx.create_child(is_for_workflow=True)
        )
        new_wf_ctx.id_assigned_for_next_workflow = wfid
        new_wf_ctx.workflow_id_reuse_policy = reuse_policy
        new_wf_ctx.derived_from = derived_from
        new_wf_ctx.shard_key = options.shard_key
        new_wf_ctx.start_options = options
        if cur_ctx is not None:
            new_wf_ctx.workflow_deadline_epoch_ms = cur_ctx.workflow_deadline_epoch_ms

        return new_wf_ctx

    def has_parent(self) -> bool:
        return len(self.parent_workflow_id) > 0

    def start_workflow(
        self,
        wfid: Optional[str],
        attributes: TracedAttributes,
    ) -> None:
        if wfid is None or len(wfid) == 0:
            wfid = self.id_assigned_for_next_workflow or generate_uuid()
            self.id_assigned_for_next_workflow = ""
            self.workflow_id_reuse_policy = None
        self.workflow_id = wfid
        self.function_id = 0
        self.active_stream_reads = 0
        self.used_datasources = []
        self._start_span(attributes)

    def end_workflow(self, exc_value: Optional[BaseException]) -> None:
        self.workflow_id = ""
        self.function_id = -1
        self.owner_xid = None
        self._end_span(exc_value)

    def is_within_workflow(self) -> bool:
        return len(self.workflow_id) > 0

    def is_workflow(self) -> bool:
        return len(self.workflow_id) > 0 and not self.is_step()

    def is_step(self) -> bool:
        return self.curr_step_function_id >= 0

    def start_step(
        self,
        fid: int,
        attributes: TracedAttributes,
    ) -> None:
        self.curr_step_function_id = fid
        self.step_status = StepStatus(fid, None, None)
        self._start_span(attributes)

    def end_step(self, exc_value: Optional[BaseException]) -> None:
        self.curr_step_function_id = -1
        self.step_status = None
        self._end_span(exc_value)

    def start_sync_ds_transaction(self, ses: Session) -> None:
        self.sync_ds_session = ses

    def end_sync_ds_transaction(self) -> None:
        self.sync_ds_session = None

    def start_async_ds_transaction(self, ses: AsyncSession) -> None:
        self.async_ds_session = ses

    def end_async_ds_transaction(self) -> None:
        self.async_ds_session = None

    """ Return the current DBOS span if any. It must be a span created by DBOS."""

    def get_current_dbos_span(self) -> "Optional[Span]":
        if len(self.context_spans) > 0:
            return self.context_spans[-1].span
        return None

    """ Return the current active span if any. It might not be a DBOS span."""

    def get_current_active_span(self) -> "Optional[Span]":
        return dbos_tracer.get_current_span()

    def _start_span(self, attributes: TracedAttributes) -> None:
        if dbos_tracer.disable_otlp:
            return
        from opentelemetry.trace import use_span

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
            attributes,
            parent=None,  # It'll use the current active span as the parent
        )
        # Activate the current span
        cm = use_span(
            span,
            end_on_exit=False,
            record_exception=False,
            set_status_on_exception=False,
        )
        self.context_spans.append(ContextSpan(span, cm))
        cm.__enter__()

    def _end_span(self, exc_value: Optional[BaseException]) -> None:
        if dbos_tracer.disable_otlp:
            return
        from opentelemetry.trace import Status, StatusCode

        context_span = self.context_spans.pop()
        if exc_value is None:
            context_span.span.set_status(Status(StatusCode.OK))
        else:
            context_span.span.set_status(
                Status(StatusCode.ERROR, description=str(exc_value))
            )
        dbos_tracer.end_span(context_span.span)
        context_span.context_manager.__exit__(None, None, None)


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


_workflow_start_options_var: ContextVar[WorkflowStartOptions] = ContextVar(
    "dbos_workflow_start_options", default=_NO_START_OPTIONS
)


def get_workflow_start_options() -> WorkflowStartOptions:
    return _workflow_start_options_var.get()


def take_assigned_workflow_id() -> Optional[AssignedWorkflowID]:
    """Consume the enclosing SetWorkflowID block's ID, or None if there is none left."""
    assigned = _workflow_start_options_var.get().assigned_id
    if assigned is None or assigned.consumed:
        return None
    assigned.consumed = True
    return assigned


_Value = TypeVar("_Value")


def _reset_context_var(
    var: ContextVar[_Value], token: Optional[Token[_Value]], saved: _Value
) -> None:
    try:
        assert token is not None
        var.reset(token)
    except ValueError:
        # The block exited in another context than it entered (e.g. an async generator resumed by another task).
        var.set(saved)


def _reject_reuse(manager: object, active: bool) -> None:
    if active:
        raise DBOSException(
            f"This {type(manager).__name__} is already in use; create a new one for each with block."
        )


@dataclass
class _AuthScope:
    """The identity of one DBOSContextSetAuth block, shared by everything running inside it."""

    user: Optional[str]
    roles: Optional[List[str]]


# Innermost DBOSContextSetAuth block's identity scope, task-local; None means the DBOSContext is the scope.
_auth_scope_var: ContextVar[Optional[_AuthScope]] = ContextVar(
    "dbos_auth_scope", default=None
)


def get_authentication(ctx: Optional[DBOSContext] = None) -> Authentication:
    """The identity code running under ctx (by default, the current context) acts as."""
    current = get_local_dbos_context()
    if ctx is None:
        ctx = current
        if ctx is None:
            return _NO_AUTHENTICATION
    scope = _auth_scope_var.get() if ctx is current else None
    if scope is not None:
        return Authentication(scope.user, scope.roles, ctx.assumed_role)
    return Authentication(
        ctx.authenticated_user, ctx.authenticated_roles, ctx.assumed_role
    )


def set_current_authentication(user: Optional[str], roles: Optional[List[str]]) -> None:
    """Change the identity of the innermost scope: the enclosing DBOSContextSetAuth block,
    else the current context."""
    ctx = assert_current_dbos_context()
    scope = _auth_scope_var.get()
    if scope is not None:
        scope.user = user
        scope.roles = roles
    else:
        ctx.authenticated_user = user
        ctx.authenticated_roles = roles


def current_owner_xid(
    workflow_id: str, ctx: Optional[DBOSContext] = None
) -> Optional[str]:
    """The ownership token of the execution writing for workflow_id, or None when there is nothing to check."""
    if ctx is None:
        ctx = get_local_dbos_context()
    if ctx is None or ctx.workflow_id != workflow_id:
        return None
    # Every execution path sets a token before its workflow runs; a context without one is a bug.
    assert (
        ctx.owner_xid is not None
    ), f"Workflow {workflow_id} is writing without an ownership token"
    return ctx.owner_xid


def assert_current_dbos_context() -> DBOSContext:
    rv = get_local_dbos_context()
    assert rv, "No DBOS context found"
    return rv


def snapshot_step_context(reserve_sleep_id: bool = False) -> Optional[DBOSContext]:
    ctx = get_local_dbos_context()
    if ctx is None:
        return None
    if ctx.is_workflow():
        return ctx.snapshot_step_ctx(reserve_sleep_id)
    return ctx


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


class _WorkflowStartOptionsBlock:
    """Base for the Set* context managers: overrides WorkflowStartOptions fields within the block."""

    def __init__(self) -> None:
        self._token: Optional[Token[WorkflowStartOptions]] = None
        self._saved: WorkflowStartOptions = _NO_START_OPTIONS
        self._created_ctx = False

    def _overrides(self) -> Dict[str, Any]:
        raise NotImplementedError

    def __enter__(self: _Block) -> _Block:
        _reject_reuse(self, self._token is not None)
        options = replace(_workflow_start_options_var.get(), **self._overrides())
        # Ensure a context, so DBOS context accessors work inside the block.
        self._created_ctx = get_local_dbos_context() is None
        if self._created_ctx:
            _set_local_dbos_context(DBOSContext())
        self._saved = _workflow_start_options_var.get()
        self._token = _workflow_start_options_var.set(options)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        token, self._token = self._token, None
        _reset_context_var(_workflow_start_options_var, token, self._saved)
        if self._created_ctx:
            _clear_local_dbos_context()
        return False  # Did not handle


_Block = TypeVar("_Block", bound=_WorkflowStartOptionsBlock)


def validate_workflow_id(workflow_id: Optional[str]) -> None:
    """Reject empty or whitespace-only workflow IDs."""
    if workflow_id is None or not workflow_id.strip():
        raise DBOSException(
            f"Invalid workflow ID {workflow_id!r}: workflow IDs must be non-empty and cannot be only whitespace."
        )


def validate_workflow_id_reuse_policy(
    workflow_id_reuse_policy: Optional[WorkflowIDReusePolicy],
) -> None:
    if workflow_id_reuse_policy is not None and workflow_id_reuse_policy not in (
        "return-existing",
        "reject",
    ):
        raise DBOSException(
            f"Invalid workflow_id_reuse_policy {workflow_id_reuse_policy}. "
            "Must be either 'return-existing' or 'reject'."
        )


class SetWorkflowID(_WorkflowStartOptionsBlock):
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

    def __init__(
        self,
        wfid: str,
        *,
        workflow_id_reuse_policy: Optional[WorkflowIDReusePolicy] = None,
    ) -> None:
        validate_workflow_id(wfid)
        validate_workflow_id_reuse_policy(workflow_id_reuse_policy)
        super().__init__()
        self.wfid = wfid
        self.workflow_id_reuse_policy = workflow_id_reuse_policy

    def _overrides(self) -> Dict[str, Any]:
        return {
            "assigned_id": AssignedWorkflowID(self.wfid, self.workflow_id_reuse_policy)
        }


class SetWorkflowTimeout(_WorkflowStartOptionsBlock):
    """
    Set the workflow timeout (in seconds) to be used for the enclosed workflow invocations.

    Typical Usage
        ```
        with SetWorkflowTimeout(<timeout in seconds>):
            result = workflow_function(...)
        ```
    """

    def __init__(self, workflow_timeout_sec: Optional[float]) -> None:
        if workflow_timeout_sec and not workflow_timeout_sec > 0:
            raise Exception(
                f"Invalid workflow timeout {workflow_timeout_sec}. Timeouts must be positive."
            )
        super().__init__()
        self.workflow_timeout_ms = (
            int(workflow_timeout_sec * 1000)
            if workflow_timeout_sec is not None
            else None
        )

    def _overrides(self) -> Dict[str, Any]:
        return {
            "workflow_timeout_ms": self.workflow_timeout_ms,
            "ignore_propagated_deadline": True,
        }


def validate_workflow_attributes(attributes: Optional[Dict[str, Any]]) -> None:
    """Validate that workflow attributes are a JSON-serializable dict (or None).

    Fail fast here rather than surfacing an opaque error later when the workflow
    status is recorded as JSON.
    """
    if attributes is not None and not isinstance(attributes, dict):
        raise DBOSException(
            f"Invalid workflow attributes {attributes}. Attributes must be a dict."
        )
    if attributes is not None:
        try:
            json.dumps(attributes)
        except (TypeError, ValueError) as e:
            raise DBOSException(
                f"Invalid workflow attributes {attributes}. "
                "Attributes must be JSON-serializable."
            ) from e


class SetWorkflowAttributes(_WorkflowStartOptionsBlock):
    """
    Set custom key-value attributes to be attached to workflows started or enqueued
    within the block. Attributes are recorded in the workflow status at creation and
    are not inherited by child workflows. They can be updated after creation with
    DBOS.update_workflow_attributes.

    Typical Usage
        ```
        with SetWorkflowAttributes({"customer": "acme", "region": "us-east-1"}):
            result = workflow_function(...)
        ```
    """

    def __init__(self, attributes: Optional[Dict[str, Any]]) -> None:
        validate_workflow_attributes(attributes)
        super().__init__()
        self.attributes = attributes

    def _overrides(self) -> Dict[str, Any]:
        return {"workflow_attributes": self.attributes}


def inject_trace_context(context: "Optional[OtelContext]") -> Dict[str, str]:
    """Serialize just the W3C trace context of `context` into a fresh carrier.

    Deliberately not the global composite propagator: that one also injects baggage,
    which is unbounded in size, commonly carries user data, and would be persisted in
    the workflow's attributes and shipped anywhere the status is read. Returns an empty
    carrier when there is no valid span context to propagate.
    """
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    carrier: Dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier, context=context)
    return carrier


def extract_trace_context(carrier: Dict[str, Any]) -> "Optional[OtelContext]":
    """Rebuild an OpenTelemetry context from a carrier written by inject_trace_context.

    Returns None when the carrier holds no usable span context, so callers can fall back
    to whatever context they already have instead of rooting a detached trace. Never
    raises: the carrier lives in the user-writable attributes map, so a bad value must
    not be able to stop a workflow from executing.
    """
    from opentelemetry.trace import get_current_span
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    try:
        extracted = TraceContextTextMapPropagator().extract(carrier)
    except Exception as e:
        # Non-string carrier values make the propagator's regex/len calls raise.
        dbos_logger.warning(
            f"Ignoring malformed {OTEL_CARRIER_ATTRIBUTE} workflow attribute: {e}"
        )
        return None
    if not get_current_span(extracted).get_span_context().is_valid:
        return None
    return extracted


def otel_carrier_from_attributes(
    attributes: Optional[Any],
) -> Optional[Dict[str, Any]]:
    """Read the trace carrier out of a workflow's persisted attributes, if present.

    Tolerates any shape: attributes are user-supplied and are not validated on every
    enqueue path, so a non-dict value here must not raise on the execution path.
    """
    if not isinstance(attributes, dict):
        return None
    carrier = attributes.get(OTEL_CARRIER_ATTRIBUTE)
    return carrier if isinstance(carrier, dict) else None


class PropagateOtelContext(_WorkflowStartOptionsBlock):
    """
    Propagate the current OpenTelemetry context (or optionally, a passed-in context)
    to all workflows started or enqueued in this block so their spans join the caller's
    trace. The propagated context is durably backed by the workflow's attributes.

    Only the W3C trace context (traceparent/tracestate) travels, not baggage.

    Not automatically inherited by child workflows; use PropagateOtelContext again
    inside a workflow to keep its children on the trace.

    Typical Usage
        ```
        with PropagateOtelContext():
            handle = queue.enqueue(workflow_function, ...)
        ```
    """

    def __init__(self, context: "Optional[OtelContext]" = None) -> None:
        super().__init__()
        self.context = context

    def _overrides(self) -> Dict[str, Any]:
        # Writes nothing when there is no valid context to propagate.
        carrier = inject_trace_context(self.context)
        return {"otel_carrier": carrier if carrier else None}


@contextmanager
def restore_otel_carrier(carrier: Optional[Any]) -> Generator[None, None, None]:
    """Put a carrier persisted by PropagateOtelContext back on the workflow start options.

    Dequeue and recovery rebuild the workflow status from a fresh context, so the stored
    carrier would not otherwise reach the executing workflow. Non-dict values are ignored.
    """
    saved = _workflow_start_options_var.get()
    token = _workflow_start_options_var.set(
        replace(saved, otel_carrier=carrier if isinstance(carrier, dict) else None)
    )
    try:
        yield
    finally:
        _reset_context_var(_workflow_start_options_var, token, saved)


class SetEnqueueOptions(_WorkflowStartOptionsBlock):
    """
    Set the workflow enqueue options for the enclosed enqueue operation.

    Usage:
        ```
        with SetEnqueueOptions(deduplication_id=<deduplication id>, priority=<priority>):
            queue.enqueue(...)
        ```
    """

    def __init__(
        self,
        *,
        deduplication_id: Optional[str] = None,
        priority: Optional[int] = None,
        app_version: Optional[str] = None,
        queue_partition_key: Optional[str] = None,
        delay_seconds: Optional[float] = None,
        duplication_policy: Optional[DuplicationPolicy] = None,
    ) -> None:
        super().__init__()
        self.deduplication_id: Optional[str] = deduplication_id
        self.duplication_policy: Optional[DuplicationPolicy] = duplication_policy
        if priority is not None and (priority < MinPriority or priority > MaxPriority):
            raise Exception(
                f"Invalid priority {priority}. Priority must be between {MinPriority}~{MaxPriority}."
            )
        self.priority: Optional[int] = priority
        self.app_version: Optional[str] = app_version
        self.queue_partition_key = queue_partition_key
        self.delay_until_epoch_ms: Optional[int] = (
            int((time.time() + delay_seconds) * 1000)
            if delay_seconds is not None
            else None
        )

    def _overrides(self) -> Dict[str, Any]:
        return {
            "deduplication_id": self.deduplication_id,
            "duplication_policy": self.duplication_policy,
            "priority": self.priority,
            "app_version": self.app_version,
            "queue_partition_key": self.queue_partition_key,
            "delay_until_epoch_ms": self.delay_until_epoch_ms,
        }


class SetShardKey(_WorkflowStartOptionsBlock):
    """Set the shard key of every workflow started, enqueued, or called in the block.
    Required on a sharded system database; a workflow never inherits its caller's key.
    """

    def __init__(self, shard_key: str) -> None:
        if not shard_key:
            raise ValueError("shard_key must be a non-empty string")
        super().__init__()
        self.shard_key = shard_key

    def _overrides(self) -> Dict[str, Any]:
        return {"shard_key": self.shard_key}


class SetWorkflowDebounce(_WorkflowStartOptionsBlock):
    """Internal: mark the next enqueued workflow as debounced.

    Sets the deduplication ID (a debounce key), the initial delay, the absolute
    debounce deadline, the is_debounced flag, and the application the debounce
    acts for on the workflow start options, restoring them on exit. Unlike
    SetEnqueueOptions, it leaves priority/app_version/partition untouched so a
    debounced workflow still inherits the caller's other options.

    It also drops any propagated workflow deadline: a debounce called inside a
    workflow that has a timeout would otherwise pass that workflow's absolute
    deadline to the debounced workflow, which — because a debounce delay can be
    long — could expire before the debounced workflow ever runs. The debounced
    workflow's own timeout (an explicit SetWorkflowTimeout, kept in
    workflow_timeout_ms) is unaffected and is applied fresh on dequeue.
    """

    def __init__(
        self,
        *,
        deduplication_id: str,
        delay_until_epoch_ms: int,
        debounce_deadline_epoch_ms: Optional[int],
        application_name: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.deduplication_id = deduplication_id
        self.delay_until_epoch_ms = delay_until_epoch_ms
        self.debounce_deadline_epoch_ms = debounce_deadline_epoch_ms
        self.application_name = application_name

    def _overrides(self) -> Dict[str, Any]:
        return {
            "deduplication_id": self.deduplication_id,
            "delay_until_epoch_ms": self.delay_until_epoch_ms,
            "debounce_deadline_epoch_ms": self.debounce_deadline_epoch_ms,
            "is_debounced": True,
            "debounce_application_name": self.application_name,
            # Don't inherit the caller workflow's deadline onto the debounced workflow.
            "ignore_propagated_deadline": True,
        }


class EnterDBOSWorkflow(AbstractContextManager[DBOSContext, Literal[False]]):
    def __init__(
        self, attributes: TracedAttributes, ctx: Optional[DBOSContext]
    ) -> None:
        self.attributes = attributes
        self.use_ctx = ctx
        self.prev_ctx: Optional[DBOSContext] = None
        self.shard_key_tokens: Optional[Any] = None
        self.start_options_token: Optional[Token[WorkflowStartOptions]] = None
        self.saved_start_options: WorkflowStartOptions = _NO_START_OPTIONS
        self.auth_token: Optional[Token[Optional[_AuthScope]]] = None
        self.saved_auth: Optional[_AuthScope] = None

    def __enter__(self) -> DBOSContext:
        self.prev_ctx = get_local_dbos_context()
        # The body starts with no caller's shard keys; its own lives on the context.
        self.shard_key_tokens = clear_shard_key_state()
        # Nor the caller's Set* options: they applied to this workflow, not its children.
        self.saved_start_options = _workflow_start_options_var.get()
        self.start_options_token = _workflow_start_options_var.set(_NO_START_OPTIONS)
        # The body starts in its own context's identity scope.
        self.saved_auth = _auth_scope_var.get()
        self.auth_token = _auth_scope_var.set(None)
        # Create a basic context if none exists
        if self.use_ctx is None:
            self.use_ctx = DBOSContext()
        _set_local_dbos_context(self.use_ctx)
        ctx = self.use_ctx
        assert not ctx.is_within_workflow()
        ctx.start_workflow(
            None, self.attributes
        )  # Will get from the context's next workflow ID
        return ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        assert ctx == self.use_ctx
        assert ctx.is_within_workflow()
        ctx.end_workflow(exc_value)
        # Clear any propagating timeout
        ctx.workflow_deadline_epoch_ms = None
        # Code to clean up the basic context if we created it
        _set_local_dbos_context(self.prev_ctx)
        _reset_context_var(
            _workflow_start_options_var,
            self.start_options_token,
            self.saved_start_options,
        )
        _reset_context_var(_auth_scope_var, self.auth_token, self.saved_auth)
        if self.shard_key_tokens is not None:
            restore_shard_key_state(self.shard_key_tokens)
        return False  # Did not handle


class EnterDBOSStepCtx:
    def __init__(self, attributes: TracedAttributes, ctx: DBOSContext) -> None:
        self.attributes = attributes
        self.prev_ctx: Optional[DBOSContext] = None
        self.use_ctx = ctx
        self.auth_token: Optional[Token[Optional[_AuthScope]]] = None
        self.saved_auth: Optional[_AuthScope] = None
        self.start_options_token: Optional[Token[WorkflowStartOptions]] = None
        self.saved_start_options: WorkflowStartOptions = _NO_START_OPTIONS

    def __enter__(self) -> DBOSContext:
        ctx = assert_current_dbos_context()
        assert ctx.is_workflow()
        self.prev_ctx = ctx
        _set_local_dbos_context(self.use_ctx)
        # The step starts in its own context's identity scope.
        self.saved_auth = _auth_scope_var.get()
        self.auth_token = _auth_scope_var.set(None)
        # The workflow's Set* options apply to the workflows it starts, not to its steps.
        self.saved_start_options = _workflow_start_options_var.get()
        self.start_options_token = _workflow_start_options_var.set(_NO_START_OPTIONS)
        self.use_ctx.start_step(self.use_ctx.function_id, attributes=self.attributes)
        return self.use_ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        assert ctx.is_step()
        ctx.end_step(exc_value)
        _set_local_dbos_context(self.prev_ctx)
        _reset_context_var(_auth_scope_var, self.auth_token, self.saved_auth)
        _reset_context_var(
            _workflow_start_options_var,
            self.start_options_token,
            self.saved_start_options,
        )
        return False  # Did not handle


class EnterDBOSStepRetry:
    def __init__(self, current_attempt: int, max_attempts: int) -> None:
        self.current_attempt = current_attempt
        self.max_attempts = max_attempts

    def __enter__(self) -> None:
        ctx = get_local_dbos_context()
        if ctx is not None and ctx.step_status is not None:
            ctx.step_status.current_attempt = self.current_attempt
            ctx.step_status.max_attempts = self.max_attempts

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = get_local_dbos_context()
        if ctx is not None and ctx.step_status is not None:
            ctx.step_status.current_attempt = None
            ctx.step_status.max_attempts = None
        return False  # Did not handle


class DBOSContextSetAuth(DBOSContextEnsure):
    def __init__(self, user: Optional[str], roles: Optional[List[str]]) -> None:
        self.created_ctx = False
        self.user = user
        self.roles = roles
        self.token: Optional[Token[Optional[_AuthScope]]] = None
        self.saved_auth: Optional[_AuthScope] = None

    def __enter__(self) -> DBOSContext:
        _reject_reuse(self, self.token is not None)
        self.created_ctx = get_local_dbos_context() is None
        if self.created_ctx:
            _set_local_dbos_context(DBOSContext())
        ctx = assert_current_dbos_context()
        self.saved_auth = _auth_scope_var.get()
        self.token = _auth_scope_var.set(_AuthScope(self.user, self.roles))
        return ctx

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        token, self.token = self.token, None
        _reset_context_var(_auth_scope_var, token, self.saved_auth)
        # Clean up the basic context if we created it
        if self.created_ctx:
            _clear_local_dbos_context()
        return False  # Did not handle


class DBOSAssumeRole:
    """Internal: assume a role on the current context. Only applied around a whole workflow
    body or a synchronous child/step context creation, so the role is execution identity.
    """

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


class UseLogAttributes:
    """Temporarily set context attributes for logging"""

    def __init__(self, *, workflow_id: str = "") -> None:
        self.workflow_id = workflow_id
        self.created_ctx = False

    def __enter__(self) -> UseLogAttributes:
        ctx = get_local_dbos_context()
        if ctx is None:
            self.created_ctx = True
            _set_local_dbos_context(DBOSContext())
        ctx = assert_current_dbos_context()
        self.saved_workflow_id = ctx.workflow_id
        ctx.workflow_id = self.workflow_id
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        ctx = assert_current_dbos_context()
        ctx.workflow_id = self.saved_workflow_id
        # Clean up the basic context if we created it
        if self.created_ctx:
            _clear_local_dbos_context()
        return False  # Did not handle
