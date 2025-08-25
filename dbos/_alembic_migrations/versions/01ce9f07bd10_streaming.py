"""streaming

Revision ID: 01ce9f07bd10
Revises: d994145b47b6
Create Date: 2025-08-05 10:20:46.424975

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "01ce9f07bd10"
down_revision: Union[str, None] = "d994145b47b6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create streams table
    op.create_table(
        "streams",
        sa.Column("workflow_uuid", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("offset", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["workflow_uuid"],
            ["dbos.workflow_status.workflow_uuid"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("workflow_uuid", "key", "offset"),
        schema="dbos",
    )


def downgrade() -> None:
    # Drop streams table
    op.drop_table("streams", schema="dbos")
