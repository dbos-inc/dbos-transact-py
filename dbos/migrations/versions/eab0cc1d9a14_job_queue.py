"""
Add workflow queue table.

Revision ID: eab0cc1d9a14
Revises: a3b18ad34abe
Create Date: 2024-09-13 14:50:00.531294

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "eab0cc1d9a14"
down_revision: Union[str, None] = "a3b18ad34abe"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "job_queue",
        sa.Column("workflow_uuid", sa.Text(), nullable=False),
        sa.Column("queue_name", sa.Text(), nullable=False),
        sa.Column(
            "created_at_epoch_ms",
            sa.BigInteger(),
            server_default=sa.text(
                "(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint"
            ),
            nullable=False,
            primary_key=True,
        ),
        sa.ForeignKeyConstraint(
            ["workflow_uuid"],
            ["dbos.workflow_status.workflow_uuid"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        schema="dbos",
    )
    op.add_column(
        "workflow_status",
        sa.Column(
            "queue_name",
            sa.Text(),
        ),
        schema="dbos",
    )


def downgrade() -> None:
    op.drop_table("job_queue", schema="dbos")
    op.drop_column("workflow_status", "queue_name", schema="dbos")
