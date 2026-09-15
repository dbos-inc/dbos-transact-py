from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from . import SCHEMA_PLACEHOLDER


class SystemSchema:
    ### System table schema
    # Real schema is applied per-engine via schema_translate_map.
    metadata_obj = MetaData(schema=SCHEMA_PLACEHOLDER)

    workflow_status = Table(
        "workflow_status",
        metadata_obj,
        Column("workflow_uuid", Text, primary_key=True),
        Column("status", Text, nullable=True),
        Column("name", Text, nullable=True),
        Column("authenticated_user", Text, nullable=True),
        Column("assumed_role", Text, nullable=True),
        Column("authenticated_roles", Text, nullable=True),
        Column("output", Text, nullable=True),
        Column("error", Text, nullable=True),
        Column("executor_id", Text, nullable=True),
        Column(
            "created_at",
            BigInteger,
            nullable=False,
        ),
        Column(
            "updated_at",
            BigInteger,
            nullable=False,
        ),
        Column("application_version", Text, nullable=True),
        Column("application_id", Text, nullable=True),
        Column("class_name", String(255), nullable=True),
        Column("config_name", String(255), nullable=True),
        Column(
            "recovery_attempts",
            BigInteger,
            nullable=True,
        ),
        Column("queue_name", Text, nullable=True),
        Column("workflow_timeout_ms", BigInteger, nullable=True),
        Column("workflow_deadline_epoch_ms", BigInteger, nullable=True),
        Column("started_at_epoch_ms", BigInteger(), nullable=True),
        Column("deduplication_id", Text(), nullable=True),
        Column("inputs", Text()),
        Column("priority", Integer(), nullable=False),
        Column("queue_partition_key", Text()),
        Column("forked_from", Text()),
        Column("was_forked_from", Boolean, nullable=False, server_default="false"),
        Column("owner_xid", Text()),
        Column("parent_workflow_id", Text()),
        Column("serialization", Text()),
        Column("delay_until_epoch_ms", BigInteger, nullable=True),
        Column("rate_limited", Boolean, nullable=False, server_default="false"),
        Column("completed_at", BigInteger, nullable=True),
        Column("attributes", JSON().with_variant(JSONB(), "postgresql"), nullable=True),
        Column("schedule_name", Text, nullable=True),
        Column("debounce_deadline_epoch_ms", BigInteger, nullable=True),
        Column("is_debounced", Boolean, nullable=False, server_default="false"),
        # Owning application. NULL means unclaimed: any application may read and claim the row.
        Column("application_name", Text, nullable=True),
    )

    workflow_input = Table(
        "workflow_input",
        metadata_obj,
        Column("workflow_uuid", Text, primary_key=True),
        Column("inputs", Text, nullable=True),
        Column("retention_timestamp", BigInteger, nullable=False),
    )

    workflow_output = Table(
        "workflow_output",
        metadata_obj,
        Column("workflow_uuid", Text, primary_key=True),
        Column("output", Text, nullable=True),
        Column("error", Text, nullable=True),
        Column("retention_timestamp", BigInteger, nullable=False),
    )

    operation_outputs = Table(
        "operation_outputs",
        metadata_obj,
        Column("workflow_uuid", Text, nullable=False),
        Column("function_id", Integer, nullable=False),
        Column("function_name", Text, nullable=False),
        Column("output", Text, nullable=True),
        Column("error", Text, nullable=True),
        Column("child_workflow_id", Text, nullable=True),
        Column("started_at_epoch_ms", BigInteger, nullable=True),
        Column("completed_at_epoch_ms", BigInteger, nullable=True),
        Column("serialization", Text()),
        # Denormalized from the parent so step observability filters without a join.
        Column("application_name", Text, nullable=True),
        # Sweep order only: the payload sweep deletes by absence of a status row, so
        # this bounds what a round scans rather than deciding what it may delete.
        Column("retention_timestamp", BigInteger, nullable=False),
        PrimaryKeyConstraint("workflow_uuid", "function_id"),
    )

    notifications = Table(
        "notifications",
        metadata_obj,
        Column(
            "destination_uuid",
            Text,
            ForeignKey(
                "workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        Column("topic", Text, nullable=True),
        Column("message", Text, nullable=False),
        Column(
            "created_at_epoch_ms",
            BigInteger,
            nullable=False,
            server_default=text("(EXTRACT(epoch FROM now()) * 1000.0)::bigint"),
        ),
        Column(
            "message_uuid",
            Text,
            nullable=False,
            primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column("serialization", Text()),
        Column("consumed", Boolean, nullable=False, server_default="false"),
    )

    workflow_events = Table(
        "workflow_events",
        metadata_obj,
        Column(
            "workflow_uuid",
            Text,
            ForeignKey(
                "workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        Column("key", Text, nullable=False),
        Column("value", Text, nullable=False),
        Column("serialization", Text()),
        PrimaryKeyConstraint("workflow_uuid", "key"),
    )

    # This is an immutable version of workflow_events. Two tables are needed for backwards compatibility.
    workflow_events_history = Table(
        "workflow_events_history",
        metadata_obj,
        Column(
            "workflow_uuid",
            Text,
            ForeignKey(
                "workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        Column("key", Text, nullable=False),
        Column("value", Text, nullable=False),
        Column("function_id", Integer, nullable=False),
        Column("serialization", Text()),
        PrimaryKeyConstraint("workflow_uuid", "key", "function_id"),
    )

    streams = Table(
        "streams",
        metadata_obj,
        Column(
            "workflow_uuid",
            Text,
            ForeignKey(
                "workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        Column("key", Text, nullable=False),
        Column("value", Text, nullable=False),
        Column("offset", Integer, nullable=False),
        Column("function_id", Integer, nullable=False),
        Column("serialization", Text()),
        PrimaryKeyConstraint("workflow_uuid", "key", "offset"),
    )

    workflow_schedules = Table(
        "workflow_schedules",
        metadata_obj,
        Column("schedule_id", Text, primary_key=True),
        Column("schedule_name", Text, nullable=False, unique=True),
        Column("workflow_name", Text, nullable=False),
        Column("workflow_class_name", Text, nullable=True),
        Column("schedule", Text, nullable=False),
        Column("status", Text, nullable=False, server_default="ACTIVE"),
        Column("context", Text, nullable=False),
        Column("last_fired_at", Text, nullable=True),
        Column("automatic_backfill", Boolean, nullable=False, server_default="false"),
        Column("cron_timezone", Text, nullable=True),
        Column("queue_name", Text, nullable=True),
        # Owning application. NULL means unclaimed; schedule_name stays globally unique.
        Column("application_name", Text, nullable=True),
    )

    application_versions = Table(
        "application_versions",
        metadata_obj,
        Column("version_id", Text, primary_key=True),
        Column("version_name", Text, nullable=False, unique=True),
        Column(
            "version_timestamp",
            BigInteger,
            nullable=False,
        ),
        Column(
            "created_at",
            BigInteger,
            nullable=False,
        ),
        # Owning application. NULL means unclaimed; version_name's global uniqueness is being retired for per-owner unique indexes, so no write may infer it as an ON CONFLICT arbiter.
        Column("application_name", Text, nullable=True),
    )

    queues = Table(
        "queues",
        metadata_obj,
        Column(
            "queue_id",
            Text,
            primary_key=True,
            server_default=text("gen_random_uuid()::TEXT"),
        ),
        Column("name", Text, nullable=False, unique=True),
        Column("concurrency", Integer, nullable=True),
        Column("worker_concurrency", Integer, nullable=True),
        Column("rate_limit_max", Integer, nullable=True),
        Column("rate_limit_period_sec", Float, nullable=True),
        # Legacy columns, written for other SDKs but no longer read: every queue
        # is a priority queue, and partitioning follows the partition_* limits.
        Column("priority_enabled", Boolean, nullable=False, server_default="false"),
        Column("partition_queue", Boolean, nullable=False, server_default="false"),
        # Any of these being set means the queue is partitioned; each applies per partition.
        Column("partition_concurrency", Integer, nullable=True),
        Column("partition_worker_concurrency", Integer, nullable=True),
        Column("partition_rate_limit_max", Integer, nullable=True),
        Column("partition_rate_limit_period_sec", Float, nullable=True),
        Column("polling_interval_sec", Float, nullable=False, server_default="1.0"),
        Column("created_at", BigInteger, nullable=False),
        Column("updated_at", BigInteger, nullable=False),
        # Owning application. NULL means unclaimed; name stays globally unique.
        Column("application_name", Text, nullable=True),
    )
