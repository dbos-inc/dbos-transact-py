from sqlalchemy import Column, Integer, MetaData, String, Table

metadata = MetaData()

dbos_hello = Table(
    "dbos_hello",
    metadata,
    Column("greet_count", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False),
)
