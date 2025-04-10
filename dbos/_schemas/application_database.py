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


class ApplicationSchema:
    schema = "dbos"
    metadata_obj = MetaData(schema=schema)

    transaction_outputs = Table(
        "transaction_outputs",
        metadata_obj,
        Column("workflow_uuid", Text),
        Column("function_id", Integer),
        Column("output", Text, nullable=True),
        Column("error", Text, nullable=True),
        Column("txn_id", Text, nullable=True),
        Column("txn_snapshot", Text),
        Column("executor_id", Text, nullable=True),
        Column("function_name", Text, nullable=False, server_default=""),
        Column(
            "created_at",
            BigInteger,
            nullable=False,
            server_default=text("(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint"),
        ),
        Index("transaction_outputs_created_at_index", "created_at"),
        PrimaryKeyConstraint("workflow_uuid", "function_id"),
    )
