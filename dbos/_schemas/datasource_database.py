from typing import Optional

from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
)


def datasource_outputs_table(schema: Optional[str]) -> Table:
    # Built per datasource, so each instance targets its own schema (None = unqualified).
    return Table(
        "datasource_outputs",
        MetaData(schema=schema),
        Column("workflow_id", Text),
        Column("step_id", Integer),
        Column("output", Text, nullable=True),
        Column("error", Text, nullable=True),
        Column("serialization", Text, nullable=True),
        Column(
            "created_at",
            BigInteger,
            nullable=False,
        ),
        PrimaryKeyConstraint("workflow_id", "step_id"),
    )
