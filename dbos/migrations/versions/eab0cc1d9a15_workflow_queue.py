"""workflow_queue

Revision ID: eab0cc1d9a15
Revises: eab0cc1d9a14
Create Date: 2024-09-24 12:00:00.0

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "eab0cc1d9a15"
down_revision: Union[str, None] = "eab0cc1d9a14"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.rename_table("job_queue", "workflow_queue", schema="dbos")


def downgrade() -> None:
    op.rename_table("workflow_queue", "job_queue", schema="dbos")
