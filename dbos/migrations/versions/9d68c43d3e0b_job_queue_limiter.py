"""job_queue_limiter

Revision ID: 9d68c43d3e0b
Revises: eab0cc1d9a14
Create Date: 2024-09-25 13:28:19.683860

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9d68c43d3e0b"
down_revision: Union[str, None] = "eab0cc1d9a14"
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


def downgrade() -> None:
    op.drop_column("job_queue", "started_at_epoch_ms", schema="dbos")
