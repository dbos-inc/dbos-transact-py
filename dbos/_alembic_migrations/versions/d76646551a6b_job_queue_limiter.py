"""
Adjust workflow queue to add columns for rate limiter.

Revision ID: d76646551a6b
Revises: 50f3227f0b4b
Create Date: 2024-09-25 14:48:10.218015

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d76646551a6b"
down_revision: Union[str, None] = "50f3227f0b4b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "job_queue",
        sa.Column(
            "started_at_epoch_ms",
            sa.BigInteger(),
        ),
        schema="dbos",
    )
    op.add_column(
        "job_queue",
        sa.Column(
            "completed_at_epoch_ms",
            sa.BigInteger(),
        ),
        schema="dbos",
    )


def downgrade() -> None:
    op.drop_column("job_queue", "started_at_epoch_ms", schema="dbos")
    op.drop_column("job_queue", "completed_at_epoch_ms", schema="dbos")
