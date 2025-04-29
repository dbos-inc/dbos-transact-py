from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)


class SystemSchema:
    ### System table schema
    metadata_obj = MetaData(schema="dbos")
    sysdb_suffix = "_dbos_sys"

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
            server_default=text("(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint"),
        ),
        Column(
            "updated_at",
            BigInteger,
            nullable=False,
            server_default=text("(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint"),
        ),
        Column("application_version", Text, nullable=True),
        Column("application_id", Text, nullable=True),
        Column("class_name", String(255), nullable=True, server_default=text("NULL")),
        Column("config_name", String(255), nullable=True, server_default=text("NULL")),
        Column(
            "recovery_attempts",
            BigInteger,
            nullable=True,
            server_default=text("'0'::bigint"),
        ),
        Column("queue_name", Text, nullable=True),
        Column("workflow_timeout_ms", BigInteger, nullable=True),
        Column("workflow_deadline_epoch_ms", BigInteger, nullable=True),
        Index("workflow_status_created_at_index", "created_at"),
        Index("workflow_status_executor_id_index", "executor_id"),
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
        Column("function_name", Text, nullable=False, default=""),
        Column("output", Text, nullable=True),
        Column("error", Text, nullable=True),
        Column("child_workflow_id", Text, nullable=True),
        PrimaryKeyConstraint("workflow_uuid", "function_id"),
    )

    workflow_inputs = Table(
        "workflow_inputs",
        metadata_obj,
        Column(
            "workflow_uuid",
            Text,
            ForeignKey(
                "workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"
            ),
            primary_key=True,
            nullable=False,
        ),
        Column("inputs", Text, nullable=False),
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
            server_default=text("(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint"),
        ),
        Column(
            "message_uuid",
            Text,
            nullable=False,
            server_default=text("uuid_generate_v4()"),
        ),
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
        PrimaryKeyConstraint("workflow_uuid", "key"),
    )

    scheduler_state = Table(
        "scheduler_state",
        metadata_obj,
        Column("workflow_fn_name", Text, primary_key=True, nullable=False),
        Column("last_run_time", BigInteger, nullable=False),
    )

    workflow_queue = Table(
        "workflow_queue",
        metadata_obj,
        Column(
            "workflow_uuid",
            Text,
            ForeignKey(
                "workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"
            ),
            nullable=False,
            primary_key=True,
        ),
        # Column("executor_id", Text), # This column is deprecated. Do *not* use it.
        Column("queue_name", Text, nullable=False),
        Column(
            "created_at_epoch_ms",
            BigInteger,
            nullable=False,
            server_default=text("(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint"),
        ),
        Column(
            "started_at_epoch_ms",
            BigInteger(),
        ),
        Column(
            "completed_at_epoch_ms",
            BigInteger(),
        ),
        Column(
            "deduplication_id",
            Text,
            nullable=True,
        ),
        Column(
            "priority",
            Integer,
            nullable=False,
            server_default=text("'0'::int"),
        ),
        UniqueConstraint(
            "queue_name", "deduplication_id", name="uq_workflow_queue_name_dedup_id"
        ),
    )
