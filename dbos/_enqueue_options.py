"""Options and status-row construction for enqueueing a workflow from options.

Shared by DBOSClient.enqueue, which enqueues from outside an application, and
DBOS.enqueue_workflow_with_options, which enqueues from inside one. Both build
the same ENQUEUED row from the same options, so a workflow enqueued either way
is indistinguishable to the executor that eventually runs it.
"""

import json
import time
from typing import TYPE_CHECKING, Any, Dict, Optional, TypedDict

from dbos._context import (
    OTEL_CARRIER_ATTRIBUTE,
    MaxPriority,
    MinPriority,
    inject_trace_context,
    validate_workflow_id,
)
from dbos._error import DBOSException
from dbos._serialization import Serializer, WorkflowSerializationFormat, serialize_args
from dbos._sys_db import WorkflowStatusInternal, WorkflowStatusString
from dbos._utils import generate_uuid

if TYPE_CHECKING:
    from opentelemetry.context import Context as OtelContext
else:
    # EnqueueOptions is public, so its annotations must stay resolvable at runtime for
    # get_type_hints()/pydantic without importing the optional opentelemetry package.
    OtelContext = Any


# Required EnqueueOptions fields
class _EnqueueOptionsRequired(TypedDict):
    workflow_name: str
    queue_name: str


# Optional EnqueueOptions fields
class EnqueueOptions(_EnqueueOptionsRequired, total=False):
    workflow_id: str
    app_version: str
    workflow_timeout: float
    delay_seconds: float
    deduplication_id: str
    priority: int
    max_recovery_attempts: int
    queue_partition_key: str
    authenticated_user: str
    authenticated_roles: list[str]
    serialization_type: WorkflowSerializationFormat
    class_name: str
    instance_name: str
    attributes: Dict[str, Any]
    # Owning application. Unset defaults to the caller's name.
    application_name: Optional[str]
    # Parents the enqueued workflow's span to this OpenTelemetry context, so it joins
    # this trace when it runs. The client-side PropagateOtelContext.
    otel_context: "OtelContext"


def validate_enqueue_options(options: EnqueueOptions) -> None:
    priority = options.get("priority")
    if priority is not None and (priority < MinPriority or priority > MaxPriority):
        raise DBOSException(
            f"Invalid priority {priority}. Priority must be between {MinPriority}~{MaxPriority}."
        )
    workflow_id = options.get("workflow_id")
    if workflow_id is not None:
        validate_workflow_id(workflow_id)


def attributes_with_otel_context(
    attributes: Optional[Dict[str, Any]], otel_context: "Optional[OtelContext]"
) -> Optional[Dict[str, Any]]:
    """Fold an otel_context option into the workflow's persisted attributes.

    The client-side counterpart of PropagateOtelContext. Records nothing when there is
    no valid context to propagate, and wins over a hand-written carrier in attributes.
    Only the W3C trace context travels, not baggage.
    """
    if otel_context is None:
        return attributes
    carrier = inject_trace_context(otel_context)
    if not carrier:
        return attributes
    merged = dict(attributes) if attributes else {}
    merged[OTEL_CARRIER_ATTRIBUTE] = carrier
    return merged


def build_enqueue_status(
    options: EnqueueOptions,
    serializer: Serializer,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> tuple[str, WorkflowStatusInternal]:
    """Build (without persisting) the ENQUEUED row described by these options.

    Fields DBOS-internal callers own (parent linkage, app id) are left unset here
    and stamped by the caller.
    """
    validate_enqueue_options(options)
    workflow_name = options["workflow_name"]
    queue_name = options["queue_name"]

    workflow_id = options.get("workflow_id")
    if workflow_id is None:
        workflow_id = generate_uuid()
    workflow_timeout = options.get("workflow_timeout", None)
    delay_seconds = options.get("delay_seconds", None)
    delay_until_epoch_ms: Optional[int] = (
        int((time.time() + delay_seconds) * 1000) if delay_seconds is not None else None
    )

    authenticated_user = options.get("authenticated_user")
    authenticated_roles = (
        json.dumps(options.get("authenticated_roles"))
        if options.get("authenticated_roles")
        else None
    )

    inputs, serialization = serialize_args(
        args,
        kwargs,
        options.get("serialization_type"),
        serializer,
    )

    attributes = attributes_with_otel_context(
        options.get("attributes"), options.get("otel_context")
    )

    status: WorkflowStatusInternal = {
        "workflow_uuid": workflow_id,
        "status": (
            WorkflowStatusString.DELAYED.value
            if delay_until_epoch_ms is not None
            else WorkflowStatusString.ENQUEUED.value
        ),
        "name": workflow_name,
        "class_name": options.get("class_name"),
        "queue_name": queue_name,
        "app_version": options.get("app_version"),
        "config_name": options.get("instance_name"),
        "authenticated_user": authenticated_user,
        "assumed_role": None,
        "authenticated_roles": authenticated_roles,
        "output": None,
        "error": None,
        "created_at": None,
        "updated_at": None,
        "executor_id": None,
        "recovery_attempts": None,
        "app_id": None,
        "workflow_timeout_ms": (
            int(workflow_timeout * 1000) if workflow_timeout is not None else None
        ),
        "workflow_deadline_epoch_ms": None,
        "deduplication_id": options.get("deduplication_id", None),
        "priority": (
            options.get("priority", 0)
            if options.get("priority", None) is not None
            else 0
        ),
        "inputs": inputs,
        "serialization": serialization,
        "queue_partition_key": options.get("queue_partition_key", None),
        "forked_from": None,
        "parent_workflow_id": None,
        "started_at_epoch_ms": None,
        "owner_xid": None,
        "delay_until_epoch_ms": delay_until_epoch_ms,
        "attributes": attributes,
        "schedule_name": None,
        # Set only by the debouncer via _enqueue_debounced, never from options.
        "debounce_deadline_epoch_ms": None,
        "is_debounced": False,
        "application_name": options.get("application_name"),
    }
    return workflow_id, status
