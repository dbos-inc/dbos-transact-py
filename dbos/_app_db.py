from typing import List, Optional, TypedDict

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from . import _serialization
from ._dbos_config import ConfigFile, DatabaseConfig
from ._error import DBOSWorkflowConflictIDError
from ._schemas.application_database import ApplicationSchema
from ._sys_db import StepInfo


class TransactionResultInternal(TypedDict):
    workflow_uuid: str
    function_id: int
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)
    txn_id: Optional[str]
    txn_snapshot: str
    executor_id: Optional[str]
    function_name: Optional[str]


class RecordedResult(TypedDict):
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)


class ApplicationDatabase:

    def __init__(self, database: DatabaseConfig, *, debug_mode: bool = False):

        app_db_name = database["app_db_name"]

        # If the application database does not already exist, create it
        if not debug_mode:
            postgres_db_url = sa.URL.create(
                "postgresql+psycopg",
                username=database["username"],
                password=database["password"],
                host=database["hostname"],
                port=database["port"],
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
            "postgresql+psycopg",
            username=database["username"],
            password=database["password"],
            host=database["hostname"],
            port=database["port"],
            database=app_db_name,
        )

        connect_args = {}
        if (
            "connectionTimeoutMillis" in database
            and database["connectionTimeoutMillis"]
        ):
            connect_args["connect_timeout"] = int(
                database["connectionTimeoutMillis"] / 1000
            )

        self.engine = sa.create_engine(
            app_db_url,
            pool_size=database["app_db_pool_size"],
            max_overflow=0,
            pool_timeout=30,
            connect_args=connect_args,
        )
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.debug_mode = debug_mode

        # Create the dbos schema and transaction_outputs table in the application database
        if not debug_mode:
            with self.engine.begin() as conn:
                schema_creation_query = sa.text(
                    f"CREATE SCHEMA IF NOT EXISTS {ApplicationSchema.schema}"
                )
                conn.execute(schema_creation_query)

            inspector = inspect(self.engine)
            if not inspector.has_table(
                "transaction_outputs", schema=ApplicationSchema.schema
            ):
                ApplicationSchema.metadata_obj.create_all(self.engine)
            else:
                columns = inspector.get_columns(
                    "transaction_outputs", schema=ApplicationSchema.schema
                )
                column_names = [col["name"] for col in columns]

                if "function_name" not in column_names:
                    # Column missing, alter table to add it
                    with self.engine.connect() as conn:
                        conn.execute(
                            text(
                                f"""
                            ALTER TABLE {ApplicationSchema.schema}.transaction_outputs
                            ADD COLUMN function_name TEXT NOT NULL DEFAULT '';
                            """
                            )
                        )
                        conn.commit()

    def destroy(self) -> None:
        self.engine.dispose()

    @staticmethod
    def record_transaction_output(
        session: Session, output: TransactionResultInternal
    ) -> None:
        try:
            session.execute(
                pg.insert(ApplicationSchema.transaction_outputs).values(
                    workflow_uuid=output["workflow_uuid"],
                    function_id=output["function_id"],
                    output=output["output"],
                    error=None,
                    txn_id=sa.text("(select pg_current_xact_id_if_assigned()::text)"),
                    txn_snapshot=output["txn_snapshot"],
                    executor_id=(
                        output["executor_id"] if output["executor_id"] else None
                    ),
                    function_name=output["function_name"],
                )
            )
        except DBAPIError as dbapi_error:
            if dbapi_error.orig.sqlstate == "23505":  # type: ignore
                raise DBOSWorkflowConflictIDError(output["workflow_uuid"])
            raise

    def record_transaction_error(self, output: TransactionResultInternal) -> None:
        if self.debug_mode:
            raise Exception("called record_transaction_error in debug mode")
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    pg.insert(ApplicationSchema.transaction_outputs).values(
                        workflow_uuid=output["workflow_uuid"],
                        function_id=output["function_id"],
                        output=None,
                        error=output["error"],
                        txn_id=sa.text(
                            "(select pg_current_xact_id_if_assigned()::text)"
                        ),
                        txn_snapshot=output["txn_snapshot"],
                        executor_id=(
                            output["executor_id"] if output["executor_id"] else None
                        ),
                        function_name=output["function_name"],
                    )
                )
        except DBAPIError as dbapi_error:
            if dbapi_error.orig.sqlstate == "23505":  # type: ignore
                raise DBOSWorkflowConflictIDError(output["workflow_uuid"])
            raise

    @staticmethod
    def check_transaction_execution(
        session: Session, workflow_uuid: str, function_id: int
    ) -> Optional[RecordedResult]:
        rows = session.execute(
            sa.select(
                ApplicationSchema.transaction_outputs.c.output,
                ApplicationSchema.transaction_outputs.c.error,
            ).where(
                ApplicationSchema.transaction_outputs.c.workflow_uuid == workflow_uuid,
                ApplicationSchema.transaction_outputs.c.function_id == function_id,
            )
        ).all()
        if len(rows) == 0:
            return None
        result: RecordedResult = {
            "output": rows[0][0],
            "error": rows[0][1],
        }
        return result

    def get_transactions(self, workflow_uuid: str) -> List[StepInfo]:
        with self.engine.begin() as conn:
            rows = conn.execute(
                sa.select(
                    ApplicationSchema.transaction_outputs.c.function_id,
                    ApplicationSchema.transaction_outputs.c.function_name,
                    ApplicationSchema.transaction_outputs.c.output,
                    ApplicationSchema.transaction_outputs.c.error,
                ).where(
                    ApplicationSchema.transaction_outputs.c.workflow_uuid
                    == workflow_uuid,
                )
            ).all()
        return [
            StepInfo(
                function_id=row[0],
                function_name=row[1],
                output=(
                    _serialization.deserialize(row[2]) if row[2] is not None else row[2]
                ),
                error=(
                    _serialization.deserialize_exception(row[3])
                    if row[3] is not None
                    else row[3]
                ),
                child_workflow_id=None,
            )
            for row in rows
        ]

    def clone_workflow_transactions(
        self, src_workflow_id: str, forked_workflow_id: str, start_step: int
    ) -> None:
        """
        Copies all steps from dbos.transctions_outputs where function_id < input function_id
        into a new workflow_uuid. Returns the new workflow_uuid.
        """
        print(
            f"Clone_workflow_transactions from {src_workflow_id} to {forked_workflow_id}"
        )

        with self.engine.begin() as conn:
            # Select the rows you want to copy

            max_function_id_row = conn.execute(
                sa.select(
                    sa.func.max(ApplicationSchema.transaction_outputs.c.function_id)
                ).where(
                    ApplicationSchema.transaction_outputs.c.workflow_uuid
                    == src_workflow_id
                )
            ).fetchone()

            max_function_id = max_function_id_row[0] if max_function_id_row else None

            print(f"transaction rows Max function id: {max_function_id}")

            if max_function_id is not None and start_step > max_function_id:
                raise DBOSWorkflowConflictIDError(
                    f"Forked workflow start step {start_step} is greater than max step function id {max_function_id} in original workflow {src_workflow_id}"
                )

            rows = conn.execute(
                sa.select(
                    ApplicationSchema.transaction_outputs.c.function_id,
                    ApplicationSchema.transaction_outputs.c.output,
                    ApplicationSchema.transaction_outputs.c.error,
                    ApplicationSchema.transaction_outputs.c.txn_id,
                    ApplicationSchema.transaction_outputs.c.txn_snapshot,
                    ApplicationSchema.transaction_outputs.c.executor_id,
                    ApplicationSchema.transaction_outputs.c.function_name,
                ).where(
                    (
                        ApplicationSchema.transaction_outputs.c.workflow_uuid
                        == src_workflow_id
                    )
                    & (ApplicationSchema.transaction_outputs.c.function_id < start_step)
                )
            ).fetchall()

            if rows:
                # Prepare the new rows to insert
                new_rows = [
                    {
                        "workflow_uuid": forked_workflow_id,
                        "function_id": row.function_id,
                        "output": row.output,
                        "error": row.error,
                        "txn_id": row.txn_id,
                        "txn_snapshot": row.txn_snapshot,
                        "executor_id": row.executor_id,
                        "function_name": row.function_name,
                    }
                    for row in rows
                ]

                print(
                    f"Cloning transaction rows from {src_workflow_id} to {forked_workflow_id} {rows}"
                )
                # Insert the new rows
                conn.execute(sa.insert(ApplicationSchema.transaction_outputs), new_rows)
            else:
                print(
                    f"No transaction rows found for workflow {src_workflow_id} to clone."
                )
