"""
Add system tables.

Revision ID: 5c361fc04708
Revises:
Create Date: 2024-07-21 13:06:13.724602
# mypy: allow-untyped-defs, allow-untyped-calls
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5c361fc04708"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(sa.schema.CreateSchema(name="dbos", if_not_exists=True))
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "scheduler_state",
        sa.Column("workflow_fn_name", sa.Text(), nullable=False),
        sa.Column("last_run_time", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("workflow_fn_name"),
        schema="dbos",
    )
    op.create_table(
        "workflow_status",
        sa.Column("workflow_uuid", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("authenticated_user", sa.Text(), nullable=True),
        sa.Column("assumed_role", sa.Text(), nullable=True),
        sa.Column("authenticated_roles", sa.Text(), nullable=True),
        sa.Column("request", sa.Text(), nullable=True),
        sa.Column("output", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("executor_id", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.BigInteger(),
            server_default=sa.text(
                "(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint"
            ),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.BigInteger(),
            server_default=sa.text(
                "(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint"
            ),
            nullable=False,
        ),
        sa.Column("application_version", sa.Text(), nullable=True),
        sa.Column("application_id", sa.Text(), nullable=True),
        sa.Column(
            "class_name",
            sa.String(length=255),
            server_default=sa.text("NULL"),
            nullable=True,
        ),
        sa.Column(
            "config_name",
            sa.String(length=255),
            server_default=sa.text("NULL"),
            nullable=True,
        ),
        sa.Column(
            "recovery_attempts",
            sa.BigInteger(),
            server_default=sa.text("'0'::bigint"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("workflow_uuid"),
        schema="dbos",
    )
    op.create_index(
        "workflow_status_created_at_index",
        "workflow_status",
        ["created_at"],
        unique=False,
        schema="dbos",
    )
    op.create_index(
        "workflow_status_executor_id_index",
        "workflow_status",
        ["executor_id"],
        unique=False,
        schema="dbos",
    )
    op.create_table(
        "notifications",
        sa.Column("destination_uuid", sa.Text(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=True),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column(
            "created_at_epoch_ms",
            sa.BigInteger(),
            server_default=sa.text(
                "(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint"
            ),
            nullable=False,
        ),
        sa.Column(
            "message_uuid",
            sa.Text(),
            server_default=sa.text("uuid_generate_v4()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["destination_uuid"],
            ["dbos.workflow_status.workflow_uuid"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        schema="dbos",
    )
    op.create_index(
        "idx_workflow_topic",
        "notifications",
        ["destination_uuid", "topic"],
        unique=False,
        schema="dbos",
    )
    op.create_table(
        "operation_outputs",
        sa.Column("workflow_uuid", sa.Text(), nullable=False),
        sa.Column("function_id", sa.Integer(), nullable=False),
        sa.Column("output", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["workflow_uuid"],
            ["dbos.workflow_status.workflow_uuid"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("workflow_uuid", "function_id"),
        schema="dbos",
    )
    op.create_table(
        "workflow_events",
        sa.Column("workflow_uuid", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["workflow_uuid"],
            ["dbos.workflow_status.workflow_uuid"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("workflow_uuid", "key"),
        schema="dbos",
    )
    op.create_table(
        "workflow_inputs",
        sa.Column("workflow_uuid", sa.Text(), nullable=False),
        sa.Column("inputs", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["workflow_uuid"],
            ["dbos.workflow_status.workflow_uuid"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("workflow_uuid"),
        schema="dbos",
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("workflow_inputs", schema="dbos")
    op.drop_table("workflow_events", schema="dbos")
    op.drop_table("operation_outputs", schema="dbos")
    op.drop_index("idx_workflow_topic", table_name="notifications", schema="dbos")
    op.drop_table("notifications", schema="dbos")
    op.drop_index(
        "workflow_status_executor_id_index", table_name="workflow_status", schema="dbos"
    )
    op.drop_index(
        "workflow_status_created_at_index", table_name="workflow_status", schema="dbos"
    )
    op.drop_table("workflow_status", schema="dbos")
    op.drop_table("scheduler_state", schema="dbos")
    # ### end Alembic commands ###
