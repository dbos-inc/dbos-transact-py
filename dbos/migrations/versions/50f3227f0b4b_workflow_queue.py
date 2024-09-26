"""workflow_queue

Revision ID: 50f3227f0b4c
Revises: 50f3227f0b4b
Create Date: 2024-09-26 12:00:00.0

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "50f3227f0b4c"
down_revision: Union[str, None] = "50f3227f0b4b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.rename_table("job_queue", "workflow_queue", schema="dbos")


def downgrade() -> None:
    op.rename_table("workflow_queue", "job_queue", schema="dbos")
