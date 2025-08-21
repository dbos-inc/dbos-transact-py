"""dbos_migrations

Revision ID: 471b60d64126
Revises: 01ce9f07bd10
Create Date: 2025-08-21 14:22:31.455266

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "471b60d64126"
down_revision: Union[str, None] = "01ce9f07bd10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create dbos_migrations table
    op.create_table(
        "dbos_migrations",
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("version"),
        schema="dbos",
    )

    # Insert initial version 1
    op.execute("INSERT INTO dbos.dbos_migrations (version) VALUES (1)")


def downgrade() -> None:
    op.drop_table("dbos_migrations", schema="dbos")
