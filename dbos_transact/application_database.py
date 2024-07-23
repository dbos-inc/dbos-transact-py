import sqlalchemy as sa

from dbos_transact.schemas.application_database import ApplicationSchema

from .dbos_config import ConfigFile


class ApplicationDatabase:

    def __init__(self, config: ConfigFile):
        self.config = config

        app_db_name = config["database"]["app_db_name"]

        # If the system database does not already exist, create it
        postgres_db_url = sa.URL.create(
            "postgresql",
            username=config["database"]["username"],
            password=config["database"]["password"],
            host=config["database"]["hostname"],
            port=config["database"]["port"],
            database="postgres",
        )
        postgres_db_engine = sa.create_engine(postgres_db_url)
        with postgres_db_engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            if not conn.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname=:db_name"),
                parameters={"db_name": app_db_name},
            ).scalar():
                conn.execute(sa.text(f"CREATE DATABASE {app_db_name}"))
        postgres_db_engine.dispose()

        app_db_url = sa.URL.create(
            "postgresql",
            username=config["database"]["username"],
            password=config["database"]["password"],
            host=config["database"]["hostname"],
            port=config["database"]["port"],
            database=app_db_name,
        )
        self.engine = sa.create_engine(app_db_url)
        with self.engine.connect() as conn:
            schema_creation_query = sa.text(
                f"CREATE SCHEMA IF NOT EXISTS {ApplicationSchema.schema}"
            )
            conn.execute(schema_creation_query)
            conn.commit()
        ApplicationSchema.metadata_obj.create_all(self.engine)

    def destroy(self) -> None:
        self.engine.dispose()
