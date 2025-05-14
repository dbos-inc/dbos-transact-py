from typing import Any, Dict, List, Optional, TypedDict

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from . import _serialization
from ._error import DBOSUnexpectedStepError, DBOSWorkflowConflictIDError
from ._logger import dbos_logger
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

    def __init__(
        self,
        *,
        database_url: str,
        engine_kwargs: Dict[str, Any],
        debug_mode: bool = False,
    ):
        app_db_url = sa.make_url(database_url).set(drivername="postgresql+psycopg")

        if engine_kwargs is None:
            engine_kwargs = {}

        self.engine = sa.create_engine(
            app_db_url,
            **engine_kwargs,
        )
        self._engine_kwargs = engine_kwargs
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.debug_mode = debug_mode

    def run_migrations(self) -> None:
        if self.debug_mode:
            dbos_logger.warning(
                "Application database migrations are skipped in debug mode."
            )
            return
        # Check if the database exists
        app_db_url = self.engine.url
        postgres_db_engine = sa.create_engine(
            app_db_url.set(database="postgres"),
            **self._engine_kwargs,
        )
        with postgres_db_engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            if not conn.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname=:db_name"),
                parameters={"db_name": app_db_url.database},
            ).scalar():
                conn.execute(sa.text(f"CREATE DATABASE {app_db_url.database}"))
        postgres_db_engine.dispose()

        # Create the dbos schema and transaction_outputs table in the application database
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
        session: Session, workflow_id: str, function_id: int, function_name: str
    ) -> Optional[RecordedResult]:
        rows = session.execute(
            sa.select(
                ApplicationSchema.transaction_outputs.c.output,
                ApplicationSchema.transaction_outputs.c.error,
                ApplicationSchema.transaction_outputs.c.function_name,
            ).where(
                ApplicationSchema.transaction_outputs.c.workflow_uuid == workflow_id,
                ApplicationSchema.transaction_outputs.c.function_id == function_id,
            )
        ).all()
        if len(rows) == 0:
            return None
        output, error, recorded_function_name = rows[0][0], rows[0][1], rows[0][2]
        if function_name != recorded_function_name:
            raise DBOSUnexpectedStepError(
                workflow_id=workflow_id,
                step_id=function_id,
                expected_name=function_name,
                recorded_name=recorded_function_name,
            )
        result: RecordedResult = {
            "output": output,
            "error": error,
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

        with self.engine.begin() as conn:

            insert_stmt = sa.insert(ApplicationSchema.transaction_outputs).from_select(
                [
                    "workflow_uuid",
                    "function_id",
                    "output",
                    "error",
                    "txn_id",
                    "txn_snapshot",
                    "executor_id",
                    "function_name",
                ],
                sa.select(
                    sa.literal(forked_workflow_id).label("workflow_uuid"),
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
                ),
            )

            conn.execute(insert_stmt)
