"""consolidate_inputs

Revision ID: d994145b47b6
Revises: 66478e1b95e5
Create Date: 2025-05-23 08:09:15.515009

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d994145b47b6"
down_revision: Union[str, None] = "66478e1b95e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "workflow_status",
        sa.Column("inputs", sa.Text(), nullable=True),
        schema="dbos",
    )


def downgrade() -> None:
    op.drop_column("workflow_status", "inputs", schema="dbos")
