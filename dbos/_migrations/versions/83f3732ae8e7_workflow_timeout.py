"""workflow_timeout

Revision ID: 83f3732ae8e7
Revises: f4b9b32ba814
Create Date: 2025-04-16 17:05:36.642395

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "83f3732ae8e7"
down_revision: Union[str, None] = "f4b9b32ba814"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "workflow_status",
        sa.Column(
            "workflow_timeout_ms",
            sa.BigInteger(),
            nullable=True,
        ),
        schema="dbos",
    )
    op.add_column(
        "workflow_status",
        sa.Column(
            "workflow_deadline_epoch_ms",
            sa.BigInteger(),
            nullable=True,
        ),
        schema="dbos",
    )


def downgrade() -> None:
    op.drop_column("workflow_status", "workflow_deadline_epoch_ms", schema="dbos")
    op.drop_column("workflow_status", "workflow_timeout_ms", schema="dbos")
