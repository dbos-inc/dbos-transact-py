from sqlalchemy import Column, Integer, MetaData, String, Table

metadata = MetaData()

dbos_hello = Table(
    "dbos_hello",
    metadata,
    Column("name", String, primary_key=True),
    Column("greet_count", Integer, default=0),
)
