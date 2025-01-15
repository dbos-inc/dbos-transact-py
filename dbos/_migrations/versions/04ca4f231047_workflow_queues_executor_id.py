"""workflow_queues_executor_id

Revision ID: 04ca4f231047
Revises: d76646551a6c
Create Date: 2025-01-15 15:05:08.043190

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "04ca4f231047"
down_revision: Union[str, None] = "d76646551a6c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "workflow_queue",
        sa.Column(
            "executor_id",
            sa.Text(),
            nullable=True,
        ),
        schema="dbos",
    )


def downgrade() -> None:
    op.drop_column("workflow_queue", "executor_id", schema="dbos")
