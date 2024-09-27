"""workflow_queue

Revision ID: d76646551a6c
Revises: d76646551a6b
Create Date: 2024-09-27 12:00:00.0

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d76646551a6c"
down_revision: Union[str, None] = "d76646551a6b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.rename_table("job_queue", "workflow_queue", schema="dbos")
    op.execute("CREATE VIEW dbos.job_queue AS SELECT * FROM dbos.workflow_queue;")


def downgrade() -> None:
    op.execute("DROP VIEW dbos.job_queue;")
    op.rename_table("workflow_queue", "job_queue", schema="dbos")
