from __future__ import annotations

from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    text,
)


class SystemSchema:
    ### System table schema
    metadata_obj = MetaData(schema="dbos")
    sysdb_suffix = "_dbos_sys"

    @classmethod
    def set_schema(cls, schema_name: Optional[str]) -> None:
        """
        Set the schema for all DBOS system tables.

        Args:
            schema_name: The name of the schema to use for system tables
        """
        cls.metadata_obj.schema = schema_name
        cls.workflow_status.schema = schema_name
        cls.operation_outputs.schema = schema_name
        cls.notifications.schema = schema_name
        cls.workflow_events.schema = schema_name
        cls.streams.schema = schema_name
        cls.workflow_events_history.schema = schema_name
        cls.workflow_schedules.schema = schema_name
        cls.application_versions.schema = schema_name
        cls.queues.schema = schema_name

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
        Index("workflow_status_created_at_index", "created_at"),
        Index(
            "idx_workflow_status_delayed",
            "delay_until_epoch_ms",
            postgresql_where=text("status = 'DELAYED'"),
            sqlite_where=text("status = 'DELAYED'"),
        ),
        Index(
            "idx_workflow_status_pending",
            "workflow_uuid",
            postgresql_where=text("status = 'PENDING'"),
            sqlite_where=text("status = 'PENDING'"),
        ),
        Index(
            "idx_workflow_status_failed",
            "status",
            "created_at",
            postgresql_where=text(
                "status IN ('ERROR', 'CANCELLED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED')"
            ),
            sqlite_where=text(
                "status IN ('ERROR', 'CANCELLED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED')"
            ),
        ),
        Index(
            "idx_workflow_status_in_flight",
            "queue_name",
            "status",
            "priority",
            "created_at",
            postgresql_where=text("status IN ('ENQUEUED', 'PENDING')"),
            sqlite_where=text("status IN ('ENQUEUED', 'PENDING')"),
        ),
        Index(
            "idx_workflow_status_rate_limited",
            "queue_name",
            "started_at_epoch_ms",
            postgresql_where=text("rate_limited = TRUE"),
            sqlite_where=text("rate_limited = TRUE"),
        ),
        Index(
            "uq_workflow_status_queue_name_dedup_id",
            "queue_name",
            "deduplication_id",
            unique=True,
            postgresql_where=text("deduplication_id IS NOT NULL"),
            sqlite_where=text("deduplication_id IS NOT NULL"),
        ),
    )

    operation_outputs = Table(
        "operation_outputs",
        metadata_obj,
        Column(
            "workflow_uuid",
            Text,
            ForeignKey(
                "workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        Column("function_id", Integer, nullable=False),
        Column("function_name", Text, nullable=False),
        Column("output", Text, nullable=True),
        Column("error", Text, nullable=True),
        Column("child_workflow_id", Text, nullable=True),
        Column("started_at_epoch_ms", BigInteger, nullable=True),
        Column("completed_at_epoch_ms", BigInteger, nullable=True),
        Column("serialization", Text()),
        PrimaryKeyConstraint("workflow_uuid", "function_id"),
        Index(
            "idx_operation_outputs_completed_at_function_name",
            "completed_at_epoch_ms",
            "function_name",
        ),
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
        Index("idx_workflow_topic", "destination_uuid", "topic"),
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
        Column("priority_enabled", Boolean, nullable=False, server_default="false"),
        Column("partition_queue", Boolean, nullable=False, server_default="false"),
        Column("polling_interval_sec", Float, nullable=False, server_default="1.0"),
        Column("created_at", BigInteger, nullable=False),
        Column("updated_at", BigInteger, nullable=False),
    )
