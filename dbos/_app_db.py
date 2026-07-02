from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypedDict

import psycopg
import sqlalchemy as sa
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from dbos._migration import get_sqlite_timestamp_expr
from dbos._serialization import Serializer

from ._error import DBOSUnexpectedStepError, DBOSWorkflowConflictIDError
from ._logger import dbos_logger
from ._schemas import SCHEMA_PLACEHOLDER
from ._schemas.application_database import ApplicationSchema
from ._sys_db import StepInfo


class TransactionResultInternal(TypedDict):
    workflow_uuid: str
    function_id: int
    output: Optional[str]  # determined by `serialization`
    error: Optional[str]  # determined by `serialization`
    serialization: Optional[str]
    txn_id: Optional[str]
    txn_snapshot: str
    executor_id: Optional[str]
    function_name: Optional[str]


class RecordedResult(TypedDict):
    output: Optional[str]  # determined by `serialization`
    error: Optional[str]  # determined by `serialization`
    serialization: Optional[str]


class ApplicationDatabase(ABC):

    @staticmethod
    def create(
        database_url: str,
        engine_kwargs: Dict[str, Any],
        schema: Optional[str],
        serializer: Serializer,
    ) -> "ApplicationDatabase":
        """Factory method to create the appropriate ApplicationDatabase implementation based on URL."""
        if database_url.startswith("sqlite"):
            return SQLiteApplicationDatabase(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                schema=schema,
                serializer=serializer,
            )
        else:
            # Default to PostgreSQL for postgresql://, postgres://, or other URLs
            return PostgresApplicationDatabase(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                schema=schema,
                serializer=serializer,
            )

    def __init__(
        self,
        *,
        database_url: str,
        engine_kwargs: Dict[str, Any],
        serializer: Serializer,
        schema: Optional[str],
    ):
        # Log application database connection information
        printable_url = sa.make_url(database_url).render_as_string(hide_password=True)
        dbos_logger.info(
            f"Initializing DBOS application database with URL: {printable_url}"
        )
        if not database_url.startswith("sqlite"):
            dbos_logger.info(
                f"DBOS application database engine parameters: {engine_kwargs}"
            )

        # Configure and initialize the application database
        if database_url.startswith("sqlite"):
            self.schema = None
        else:
            self.schema = schema if schema else "dbos"
        # Translate the placeholder schema to this instance's schema per-engine (None for SQLite = unqualified).
        self.engine = self._create_engine(
            database_url, engine_kwargs
        ).execution_options(schema_translate_map={SCHEMA_PLACEHOLDER: self.schema})
        self._engine_kwargs = engine_kwargs
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.serializer = serializer

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
            "serialization": None,
        }
        return result

    def garbage_collect(
        self,
        cutoff_epoch_timestamp_ms: int,
        pending_workflow_ids: list[str],
        batch_size: Optional[int],
    ) -> None:
        if batch_size is not None and batch_size < 1:
            raise ValueError(f"batch_size must be a positive integer, got {batch_size}")
        gc_conditions = [
            ApplicationSchema.transaction_outputs.c.created_at
            < cutoff_epoch_timestamp_ms
        ]
        if len(pending_workflow_ids) > 0:
            gc_conditions.append(
                ~ApplicationSchema.transaction_outputs.c.workflow_uuid.in_(
                    pending_workflow_ids
                )
            )

        if batch_size is None:
            with self.engine.begin() as c:
                c.execute(
                    sa.delete(ApplicationSchema.transaction_outputs).where(
                        *gc_conditions
                    )
                )
            return

        # Delete in batches, each committed in its own transaction, by advancing
        # a created_at watermark: every batch is one range scan at the bottom of
        # transaction_outputs_created_at_index and never revisits deleted rows.
        while True:
            with self.engine.begin() as c:
                # Find the created_at of the batch_size-th oldest eligible row
                step = c.execute(
                    sa.select(ApplicationSchema.transaction_outputs.c.created_at)
                    .where(*gc_conditions)
                    .order_by(ApplicationSchema.transaction_outputs.c.created_at)
                    .limit(1)
                    .offset(batch_size - 1)
                ).scalar()
                if step is None:
                    # Fewer than batch_size eligible rows remain: delete them and stop
                    c.execute(
                        sa.delete(ApplicationSchema.transaction_outputs).where(
                            *gc_conditions
                        )
                    )
                    break
                # Delete the batch; created_at ties may push it slightly over batch_size
                c.execute(
                    sa.delete(ApplicationSchema.transaction_outputs).where(
                        *gc_conditions,
                        ApplicationSchema.transaction_outputs.c.created_at <= step,
                    )
                )

    def delete_transaction_outputs(self, workflow_ids: list[str]) -> None:
        """Delete transaction outputs for the specified workflows."""
        with self.engine.begin() as c:
            c.execute(
                sa.delete(ApplicationSchema.transaction_outputs).where(
                    ApplicationSchema.transaction_outputs.c.workflow_uuid.in_(
                        workflow_ids
                    )
                )
            )

    @abstractmethod
    def _is_unique_constraint_violation(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a unique constraint violation."""
        pass

    @abstractmethod
    def _is_serialization_error(self, dbapi_error: DBAPIError) -> bool:
        """Check if the error is a serialization/concurrency error."""
        pass


class PostgresApplicationDatabase(ApplicationDatabase):
    """PostgreSQL-specific implementation of ApplicationDatabase."""

    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        """Create a PostgreSQL engine."""
        app_db_url = sa.make_url(database_url).set(drivername="postgresql+psycopg")

        if engine_kwargs is None:
            engine_kwargs = {}

        return sa.create_engine(
            app_db_url,
            **engine_kwargs,
        )

    def run_migrations(self) -> None:
        # Check if the database exists
        app_db_url = self.engine.url
        try:
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
        except Exception:
            dbos_logger.warning(
                f"Could not connect to postgres database to verify existence of {app_db_url.database}. Continuing..."
            )
        finally:
            postgres_db_engine.dispose()

        # Create the dbos schema and transaction_outputs table in the application database
        with self.engine.begin() as conn:
            # Check if schema exists first
            schema_exists = conn.execute(
                sa.text(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = :schema_name"
                ),
                parameters={"schema_name": self.schema},
            ).scalar()

            if not schema_exists:
                schema_creation_query = sa.text(f'CREATE SCHEMA "{self.schema}"')
                conn.execute(schema_creation_query)

        inspector = inspect(self.engine)
        if not inspector.has_table("transaction_outputs", schema=self.schema):
            ApplicationSchema.metadata_obj.create_all(self.engine)
        else:
            columns = inspector.get_columns("transaction_outputs", schema=self.schema)
            column_names = [col["name"] for col in columns]

            if "function_name" not in column_names:
                # Column missing, alter table to add it
                with self.engine.connect() as conn:
                    conn.execute(
                        text(
                            f"""
                        ALTER TABLE \"{self.schema}\".transaction_outputs
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
        sqlite_kwargs = engine_kwargs.copy()
        connect_args = sqlite_kwargs.get("connect_args", {})
        if connect_args:
            filtered_keys = [
                k for k in connect_args if k in ("application_name", "connect_timeout")
            ]
            if filtered_keys:
                dbos_logger.debug(
                    f"Ignoring PostgreSQL-specific connect_args for SQLite: {filtered_keys}"
                )
            sqlite_connect_args = {
                k: v
                for k, v in connect_args.items()
                if k not in ("application_name", "connect_timeout")
            }
            sqlite_kwargs["connect_args"] = sqlite_connect_args
        engine = sa.create_engine(database_url, **sqlite_kwargs)

        # A generous busy timeout rides out lock contention rather than
        # failing fast with "database is locked" (the sqlite3 default is
        # only 5 seconds, which slow disks can exceed under load).
        @sa.event.listens_for(engine, "connect")
        def set_sqlite_pragmas(dbapi_conn: Any, connection_record: Any) -> None:
            dbapi_conn.execute("PRAGMA busy_timeout=30000")

        return engine

    def run_migrations(self) -> None:
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
