"""add queue priority

Revision ID: 933e86bdac6a
Revises: 27ac6900c6ad
Create Date: 2025-04-25 18:17:40.462737

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "933e86bdac6a"
down_revision: Union[str, None] = "27ac6900c6ad"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "workflow_queue",
        sa.Column(
            "priority",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("'0'::int"),
        ),
        schema="dbos",
    )


def downgrade() -> None:
    op.drop_column("workflow_queue", "priority", schema="dbos")
