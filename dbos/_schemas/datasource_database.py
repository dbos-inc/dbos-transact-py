from sqlalchemy import (
    BigInteger,
    Column,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
    text,
)


class DatasourceSchema:
    schema = "dbos"
    metadata_obj = MetaData(schema=schema)

    datasource_outputs = Table(
        "datasource_outputs",
        metadata_obj,
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
