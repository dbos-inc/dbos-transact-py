"""add_instance_name

Revision ID: 358630821841
Revises: a3b18ad34abe
Create Date: 2024-08-20 22:30:55.918920

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "358630821841"
down_revision: Union[str, None] = "a3b18ad34abe"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "workflow_status",
        sa.Column("inst_name", sa.Text(), nullable=True),
        schema="dbos",
    )


def downgrade() -> None:
    op.drop_column("workflow_status", "inst_name", schema="dbos")
