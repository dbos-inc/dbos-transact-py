from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypedDict

import psycopg
import sqlalchemy as sa
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from dbos._migration import get_sqlite_timestamp_expr

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


class ApplicationDatabase(ABC):

    def __init__(
        self,
        *,
        database_url: str,
        engine_kwargs: Dict[str, Any],
        debug_mode: bool = False,
    ):
        self.engine = self._create_engine(database_url, engine_kwargs)
        self._engine_kwargs = engine_kwargs
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.debug_mode = debug_mode

    @abstractmethod
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        """Create a database engine specific to the database type."""
        pass

    @abstractmethod
    def run_migrations(self) -> None:
        """Run database migrations specific to the database type."""
        pass

    def destroy(self) -> None:
        self.engine.dispose()

    def record_transaction_output(
        self, session: Session, output: TransactionResultInternal
    ) -> None:
        try:
            session.execute(
                sa.insert(ApplicationSchema.transaction_outputs).values(
                    workflow_uuid=output["workflow_uuid"],
                    function_id=output["function_id"],
                    output=output["output"],
                    error=None,
                    txn_id="",
                    txn_snapshot=output["txn_snapshot"],
                    executor_id=(
                        output["executor_id"] if output["executor_id"] else None
                    ),
                    function_name=output["function_name"],
                )
            )
        except DBAPIError as dbapi_error:
            if self._is_unique_constraint_violation(dbapi_error):
                raise DBOSWorkflowConflictIDError(output["workflow_uuid"])
            raise

    def record_transaction_error(self, output: TransactionResultInternal) -> None:
        if self.debug_mode:
            raise Exception("called record_transaction_error in debug mode")
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    sa.insert(ApplicationSchema.transaction_outputs).values(
                        workflow_uuid=output["workflow_uuid"],
                        function_id=output["function_id"],
                        output=None,
                        error=output["error"],
                        txn_id="",
                        txn_snapshot=output["txn_snapshot"],
                        executor_id=(
                            output["executor_id"] if output["executor_id"] else None
                        ),
                        function_name=output["function_name"],
                    )
                )
        except DBAPIError as dbapi_error:
            if self._is_unique_constraint_violation(dbapi_error):
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

    def garbage_collect(
        self, cutoff_epoch_timestamp_ms: int, pending_workflow_ids: list[str]
    ) -> None:
        with self.engine.begin() as c:
            delete_query = sa.delete(ApplicationSchema.transaction_outputs).where(
                ApplicationSchema.transaction_outputs.c.created_at
                < cutoff_epoch_timestamp_ms
            )

            if len(pending_workflow_ids) > 0:
                delete_query = delete_query.where(
                    ~ApplicationSchema.transaction_outputs.c.workflow_uuid.in_(
                        pending_workflow_ids
                    )
                )

            c.execute(delete_query)

    @abstractmethod
    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation."""
        pass

    @abstractmethod
    def _is_serialization_error(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a serialization/concurrency error."""
        pass

    @staticmethod
    def create(
        database_url: str,
        engine_kwargs: Dict[str, Any],
        debug_mode: bool = False,
    ) -> "ApplicationDatabase":
        """Factory method to create the appropriate ApplicationDatabase implementation based on URL."""
        if database_url.startswith("sqlite"):
            return SQLiteApplicationDatabase(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                debug_mode=debug_mode,
            )
        else:
            # Default to PostgreSQL for postgresql://, postgres://, or other URLs
            return PostgresApplicationDatabase(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                debug_mode=debug_mode,
            )


class PostgresApplicationDatabase(ApplicationDatabase):
    """PostgreSQL-specific implementation of ApplicationDatabase."""

    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        """Create a PostgreSQL engine."""
        app_db_url = sa.make_url(database_url).set(drivername="postgresql+psycopg")

        if engine_kwargs is None:
            engine_kwargs = {}

        # TODO: Make the schema dynamic so this isn't needed
        ApplicationSchema.transaction_outputs.schema = "dbos"

        return sa.create_engine(
            app_db_url,
            **engine_kwargs,
        )

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
            # Check if schema exists first
            schema_exists = conn.execute(
                sa.text(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = :schema_name"
                ),
                parameters={"schema_name": ApplicationSchema.schema},
            ).scalar()

            if not schema_exists:
                schema_creation_query = sa.text(
                    f"CREATE SCHEMA {ApplicationSchema.schema}"
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

    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation in PostgreSQL."""
        return dbapi_error.orig.sqlstate == "23505"  # type: ignore

    def _is_serialization_error(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a serialization/concurrency error in PostgreSQL."""
        # 40001: serialization_failure (MVCC conflict)
        # 40P01: deadlock_detected
        driver_error = dbapi_error.orig
        return (
            driver_error is not None
            and isinstance(driver_error, psycopg.OperationalError)
            and driver_error.sqlstate in ("40001", "40P01")
        )


class SQLiteApplicationDatabase(ApplicationDatabase):
    """SQLite-specific implementation of ApplicationDatabase."""

    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        """Create a SQLite engine."""
        # TODO: Make the schema dynamic so this isn't needed
        ApplicationSchema.transaction_outputs.schema = None
        return sa.create_engine(database_url)

    def run_migrations(self) -> None:
        if self.debug_mode:
            dbos_logger.warning(
                "Application database migrations are skipped in debug mode."
            )
            return

        with self.engine.begin() as conn:
            # Check if table exists
            result = conn.execute(
                sa.text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='transaction_outputs'"
                )
            ).fetchone()

            if result is None:
                conn.execute(
                    sa.text(
                        f"""
                        CREATE TABLE transaction_outputs (
                            workflow_uuid TEXT NOT NULL,
                            function_id INTEGER NOT NULL,
                            output TEXT,
                            error TEXT,
                            txn_id TEXT,
                            txn_snapshot TEXT NOT NULL,
                            executor_id TEXT,
                            function_name TEXT NOT NULL DEFAULT '',
                            created_at BIGINT NOT NULL DEFAULT {get_sqlite_timestamp_expr()},
                            PRIMARY KEY (workflow_uuid, function_id)
                        )
                        """
                    )
                )
                conn.execute(
                    sa.text(
                        "CREATE INDEX transaction_outputs_created_at_index ON transaction_outputs (created_at)"
                    )
                )

    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation in SQLite."""
        return "UNIQUE constraint failed" in str(dbapi_error.orig)

    def _is_serialization_error(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a serialization/concurrency error in SQLite."""
        # SQLite database is locked or busy errors
        error_msg = str(dbapi_error.orig).lower()
        return (
            "database is locked" in error_msg or "database table is locked" in error_msg
        )
