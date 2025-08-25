"""consolidate_queues

Revision ID: 66478e1b95e5
Revises: 933e86bdac6a
Create Date: 2025-05-21 10:14:25.674613

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "66478e1b95e5"
down_revision: Union[str, None] = "933e86bdac6a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add new columns to workflow_status table
    op.add_column(
        "workflow_status",
        sa.Column("started_at_epoch_ms", sa.BigInteger(), nullable=True),
        schema="dbos",
    )

    op.add_column(
        "workflow_status",
        sa.Column("deduplication_id", sa.Text(), nullable=True),
        schema="dbos",
    )

    op.add_column(
        "workflow_status",
        sa.Column(
            "priority", sa.Integer(), nullable=False, server_default=sa.text("'0'::int")
        ),
        schema="dbos",
    )

    # Add unique constraint for deduplication_id
    op.create_unique_constraint(
        "uq_workflow_status_queue_name_dedup_id",
        "workflow_status",
        ["queue_name", "deduplication_id"],
        schema="dbos",
    )

    # Add index on status field
    op.create_index(
        "workflow_status_status_index", "workflow_status", ["status"], schema="dbos"
    )


def downgrade() -> None:
    # Drop indexes
    op.drop_index(
        "workflow_status_status_index", table_name="workflow_status", schema="dbos"
    )

    # Drop unique constraint
    op.drop_constraint(
        "uq_workflow_status_queue_name_dedup_id", "workflow_status", schema="dbos"
    )

    # Drop columns
    op.drop_column("workflow_status", "priority", schema="dbos")
    op.drop_column("workflow_status", "deduplication_id", schema="dbos")
    op.drop_column("workflow_status", "started_at_epoch_ms", schema="dbos")
