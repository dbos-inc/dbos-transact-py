"""Set default value of recovery_attempts to 1

Revision ID: 258053ffda6c
Revises: 04ca4f231047
Create Date: 2025-02-03 11:21:27.190328

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "258053ffda6c"
down_revision: Union[str, None] = "04ca4f231047"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "workflow_status",
        "recovery_attempts",
        schema="dbos",
        server_default=sa.text("1"),
    )


def downgrade() -> None:
    op.alter_column(
        "workflow_status",
        "recovery_attempts",
        schema="dbos",
        server_default=sa.text("0"),
    )
