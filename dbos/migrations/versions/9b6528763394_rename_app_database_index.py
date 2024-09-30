"""rename app database index

Revision ID: 9b6528763394
Revises: d76646551a6c
Create Date: 2024-09-30 20:21:10.573687

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9b6528763394"
down_revision: Union[str, None] = "d76646551a6c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Rename the index
    op.execute(
        "ALTER INDEX dbos.workflow_status_created_at_index RENAME TO transaction_outputs_created_at_index"
    )


def downgrade() -> None:
    # Revert the index name change
    op.execute(
        "ALTER INDEX dbos.transaction_outputs_created_at_index RENAME TO workflow_status_created_at_index"
    )
