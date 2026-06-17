from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
)

from . import SCHEMA_PLACEHOLDER


class DatasourceSchema:
    # Real schema is applied per-engine via schema_translate_map.
    metadata_obj = MetaData(schema=SCHEMA_PLACEHOLDER)

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
