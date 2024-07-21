from alembic import command
from alembic.config import Config
from sqlalchemy import (
    URL,
    BigInteger,
    Column,
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

from .dbos_config import ConfigFile


def migrate_system_db(sysdb_url: str, migration_dir: str) -> None:
    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", migration_dir)
    alembic_cfg.set_main_option("sqlalchemy.url", sysdb_url)
    command.upgrade(alembic_cfg, "head")  # Upgrade to the latest migration


def get_sysdb_url(config: ConfigFile) -> str:
    sysdb_name = config["database"]["app_db_name"] + SystemSchema.sysdb_suffix
    if "sys_db_name" in config["database"] and config["database"]["sys_db_name"]:
        sysdb_name = config["database"]["sys_db_name"]  # Use the custom name if provided
    db_url = URL.create(
        "postgresql",
        username=config["database"]["username"],
        password=config["database"]["password"],
        host=config["database"]["hostname"],
        port=config["database"]["port"],
        database=sysdb_name,
    )
    return db_url.render_as_string(hide_password=False)


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
        Column("request", Text, nullable=True),
        Column("output", Text, nullable=True),
        Column("error", Text, nullable=True),
        Column("executor_id", Text, nullable=True),
        Column("created_at", BigInteger, nullable=False, server_default=text("(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint")),
        Column("updated_at", BigInteger, nullable=False, server_default=text("(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint")),
        Column("application_version", Text, nullable=True),
        Column("application_id", Text, nullable=True),
        Column("class_name", String(255), nullable=True, server_default=text("NULL")),
        Column("config_name", String(255), nullable=True, server_default=text("NULL")),
        Column("recovery_attempts", BigInteger, nullable=True, server_default=text("'0'::bigint")),
        Index("workflow_status_created_at_index", "created_at"),
        Index("workflow_status_executor_id_index", "executor_id"),
    )

    operation_outputs = Table(
        "operation_outputs",
        metadata_obj,
        Column("workflow_uuid", Text, ForeignKey("workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"), nullable=False),
        Column("function_id", Integer, nullable=False),
        Column("output", Text, nullable=True),
        Column("error", Text, nullable=True),
        PrimaryKeyConstraint("workflow_uuid", "function_id"),
    )

    workflow_inputs = Table(
        "workflow_inputs",
        metadata_obj,
        Column(
            "workflow_uuid",
            Text,
            ForeignKey("workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        ),
        Column("inputs", Text, nullable=False),
    )

    notifications = Table(
        "notifications",
        metadata_obj,
        Column("destination_uuid", Text, ForeignKey("workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"), nullable=False),
        Column("topic", Text, nullable=True),
        Column("message", Text, nullable=False),
        Column("created_at_epoch_ms", BigInteger, nullable=False, server_default=text("(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint")),
        Column("message_uuid", Text, nullable=False, server_default=text("uuid_generate_v4()")),
        Index("idx_workflow_topic", "destination_uuid", "topic"),
    )

    workflow_events = Table(
        "workflow_events",
        metadata_obj,
        Column("workflow_uuid", Text, ForeignKey("workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"), nullable=False),
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
