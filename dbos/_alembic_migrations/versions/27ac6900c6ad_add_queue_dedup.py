"""add queue dedup

Revision ID: 27ac6900c6ad
Revises: 83f3732ae8e7
Create Date: 2025-04-23 16:18:48.530047

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "27ac6900c6ad"
down_revision: Union[str, None] = "83f3732ae8e7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "workflow_queue",
        sa.Column(
            "deduplication_id",
            sa.Text(),
            nullable=True,
        ),
        schema="dbos",
    )

    # Unique constraint for queue_name, deduplication_id
    op.create_unique_constraint(
        "uq_workflow_queue_name_dedup_id",
        "workflow_queue",
        ["queue_name", "deduplication_id"],
        schema="dbos",
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_workflow_queue_name_dedup_id", "workflow_queue", schema="dbos"
    )
    op.drop_column("workflow_queue", "deduplication_id", schema="dbos")
