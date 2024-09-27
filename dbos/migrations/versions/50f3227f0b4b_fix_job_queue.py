"""
Fix job queue PK.

Revision ID: 50f3227f0b4b
Revises: eab0cc1d9a14
Create Date: 2024-09-25 14:03:53.308068

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "50f3227f0b4b"
down_revision: Union[str, None] = "eab0cc1d9a14"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("job_queue_pkey", "job_queue", schema="dbos", type_="primary")

    op.create_primary_key(
        "job_queue_pkey", "job_queue", ["workflow_uuid"], schema="dbos"
    )


def downgrade() -> None:
    # Reverting the changes
    op.drop_constraint("job_queue_pkey", "job_queue", schema="dbos", type_="primary")

    op.create_primary_key(
        "job_queue_pkey", "job_queue", ["created_at_epoch_ms"], schema="dbos"
    )
