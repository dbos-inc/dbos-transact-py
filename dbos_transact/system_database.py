from sqlalchemy import MetaData, Table, Column, Integer, Text, PrimaryKeyConstraint, ForeignKey, BigInteger, String, text

metadata_obj = MetaData(schema="dbos")

workflow_status = Table(
    "workflow_status",
    metadata_obj,
    Column("workflow_uuid", Text, primary_key=True),
    Column("status", Text, nullable=True),
    Column("name", Text, nullable=True),
    Column('authenticated_user', Text, nullable=True),
    Column('assumed_role', Text, nullable=True),
    Column('authenticated_roles', Text, nullable=True),
    Column('request', Text, nullable=True),
    Column('output', Text, nullable=True),
    Column('error', Text, nullable=True),
    Column('executor_id', Text, nullable=True),
    Column('created_at', BigInteger, nullable=False, server_default=text("(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint")),
    Column('updated_at', BigInteger, nullable=False, server_default=text("(EXTRACT(epoch FROM now()) * 1000::numeric)::bigint")),
    Column('application_version', Text, nullable=True),
    Column('application_id', Text, nullable=True),
    Column('class_name', String(255), nullable=True, server_default=text("NULL")),
    Column('config_name', String(255), nullable=True, server_default=text("NULL")),
    Column('recovery_attempts', BigInteger, nullable=True, server_default=text("'0'::bigint")),
)

operation_outputs = Table(
    "operation_outputs",
    metadata_obj,
    Column("workflow_uuid", Text, ForeignKey("workflow_status.workflow_uuid"), nullable=False),
    Column("function_id", Integer, nullable=False),
    Column("output", Text, nullable=True),
    Column("error", Text, nullable=True),
    PrimaryKeyConstraint("workflow_uuid", "function_id"),
)

workflow_inputs = Table(
    "workflow_inputs",
    metadata_obj,
    Column("workflow_uuid", Text, ForeignKey("workflow_status.workflow_uuid"), primary_key=True, nullable=False),
    Column("inputs", Text, nullable=False),
)

