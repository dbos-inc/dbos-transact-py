"""init

Revision ID: c6b516e182b2
Revises: 
Create Date: 2024-07-31 18:06:42.500040

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c6b516e182b2"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            "CREATE TABLE dbos_hello (name TEXT PRIMARY KEY, greet_count INT DEFAULT 0)"
        )
    )
    op.execute(
        sa.text(
            "CREATE TABLE dbos_greetings (greeting_name TEXT, greeting_note_content TEXT)"
        )
    )


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE dbos_hello"))
    op.execute(sa.text("DROP TABLE dbos_greetings"))
