from typing import Optional, TypedDict

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dbos._utils import run_coroutine

from ._dbos_config import ConfigFile
from ._error import DBOSWorkflowConflictIDError
from ._schemas.application_database import ApplicationSchema


class TransactionResultInternal(TypedDict):
    workflow_uuid: str
    function_id: int
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)
    txn_id: Optional[str]
    txn_snapshot: str
    executor_id: Optional[str]


class RecordedResult(TypedDict):
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)


class ApplicationDatabase:

    def __init__(self, config: ConfigFile):
        self.config = config

        app_db_name = config["database"]["app_db_name"]

        # If the application database does not already exist, create it
        postgres_db_url = sa.URL.create(
            "postgresql+psycopg",
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
            "postgresql+psycopg",
            username=config["database"]["username"],
            password=config["database"]["password"],
            host=config["database"]["hostname"],
            port=config["database"]["port"],
            database=app_db_name,
        )

        engine = sa.create_engine(
            app_db_url, pool_size=20, max_overflow=5, pool_timeout=30
        )

        try:
            # Create the dbos schema and transaction_outputs table in the application database
            with engine.begin() as conn:
                schema_creation_query = sa.text(
                    f"CREATE SCHEMA IF NOT EXISTS {ApplicationSchema.schema}"
                )
                conn.execute(schema_creation_query)
            ApplicationSchema.metadata_obj.create_all(engine)
        finally:
            engine.dispose()

        self.engine = create_async_engine(
            app_db_url, pool_size=20, max_overflow=5, pool_timeout=30
        )
        self.sessionmaker = async_sessionmaker(bind=self.engine)

    def destroy_sync(self) -> None:
        run_coroutine(self.destroy())

    async def destroy(self) -> None:
        await self.engine.dispose()

    @staticmethod
    async def record_transaction_output(
        session: AsyncSession, output: TransactionResultInternal
    ) -> None:
        try:
            await session.execute(
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
                )
            )
        except DBAPIError as dbapi_error:
            if dbapi_error.orig.sqlstate == "23505":  # type: ignore
                raise DBOSWorkflowConflictIDError(output["workflow_uuid"])
            raise

    async def record_transaction_error(self, output: TransactionResultInternal) -> None:
        try:
            async with self.engine.begin() as conn:
                await conn.execute(
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
                    )
                )
        except DBAPIError as dbapi_error:
            if dbapi_error.orig.sqlstate == "23505":  # type: ignore
                raise DBOSWorkflowConflictIDError(output["workflow_uuid"])
            raise

    @staticmethod
    async def check_transaction_execution(
        session: AsyncSession, workflow_uuid: str, function_id: int
    ) -> Optional[RecordedResult]:
        rows = (
            await session.execute(
                sa.select(
                    ApplicationSchema.transaction_outputs.c.output,
                    ApplicationSchema.transaction_outputs.c.error,
                ).where(
                    ApplicationSchema.transaction_outputs.c.workflow_uuid
                    == workflow_uuid,
                    ApplicationSchema.transaction_outputs.c.function_id == function_id,
                )
            )
        ).all()
        if len(rows) == 0:
            return None
        result: RecordedResult = {
            "output": rows[0][0],
            "error": rows[0][1],
        }
        return result
