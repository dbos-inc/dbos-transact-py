import json
from typing import Any, Optional, TypedDict

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg

from dbos_transact.schemas.application_database import ApplicationSchema

from .dbos_config import ConfigFile


class TransactionResultInternal(TypedDict):
    workflow_uuid: str
    function_id: int
    output: Optional[str]  # Base64-encoded pickle
    error: Optional[str]  # Base64-encoded pickle
    txn_id: Optional[str]
    txn_snapshot: str
    executor_id: Optional[str]


class ApplicationDatabase:

    def __init__(self, config: ConfigFile):
        self.config = config

        app_db_name = config["database"]["app_db_name"]

        # If the application database does not already exist, create it
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

        # Create a connection pool for the application database
        app_db_url = sa.URL.create(
            "postgresql",
            username=config["database"]["username"],
            password=config["database"]["password"],
            host=config["database"]["hostname"],
            port=config["database"]["port"],
            database=app_db_name,
        )
        self.engine = sa.create_engine(app_db_url)

        # Create the dbos schema and transaction_outputs table in the application database
        with self.engine.begin() as conn:
            schema_creation_query = sa.text(
                f"CREATE SCHEMA IF NOT EXISTS {ApplicationSchema.schema}"
            )
            conn.execute(schema_creation_query)
        ApplicationSchema.metadata_obj.create_all(self.engine)

    def destroy(self) -> None:
        self.engine.dispose()

    @staticmethod
    def record_transaction_output(
        conn: sa.Connection, output: TransactionResultInternal
    ) -> None:
        conn.execute(
            pg.insert(ApplicationSchema.transaction_outputs).values(
                workflow_uuid=output["workflow_uuid"],
                function_id=output["function_id"],
                output=output["output"] if output["output"] else None,
                error=None,
                txn_id=sa.text("(select pg_current_xact_id_if_assigned()::text)"),
                txn_snapshot=output["txn_snapshot"],
                executor_id=output["executor_id"] if output["executor_id"] else None,
            )
        )

    def record_transaction_error(self, output: TransactionResultInternal) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                pg.insert(ApplicationSchema.transaction_outputs).values(
                    workflow_uuid=output["workflow_uuid"],
                    function_id=output["function_id"],
                    output=None,
                    error=output["error"] if output["error"] else None,
                    txn_id=sa.text("(select pg_current_xact_id_if_assigned()::text)"),
                    txn_snapshot=output["txn_snapshot"],
                    executor_id=(
                        output["executor_id"] if output["executor_id"] else None
                    ),
                )
            )
