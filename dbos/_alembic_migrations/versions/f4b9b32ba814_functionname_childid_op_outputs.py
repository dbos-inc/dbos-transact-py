"""functionname_childid_op_outputs

Revision ID: f4b9b32ba814
Revises: 04ca4f231047
Create Date: 2025-03-21 14:32:43.091074

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f4b9b32ba814"
down_revision: Union[str, None] = "04ca4f231047"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "operation_outputs",
        sa.Column(
            "function_name",
            sa.Text(),
            nullable=False,
            server_default="",
        ),
        schema="dbos",
    )

    op.add_column(
        "operation_outputs",
        sa.Column(
            "child_workflow_id",
            sa.Text(),
            nullable=True,
        ),
        schema="dbos",
    )


def downgrade() -> None:
    op.drop_column("operation_outputs", "function_name", schema="dbos")
    op.drop_column("operation_outputs", "child_workflow_id", schema="dbos")
