import inspect
from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Optional,
    ParamSpec,
    TypedDict,
    TypeVar,
    overload,
)

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)
from sqlalchemy.orm import Session, sessionmaker

from dbos._app_db import RecordedResult
from dbos._core import StepOptions
from dbos._dbos import DBOS, IsolationLevel, check_async
from dbos._error import DBOSException
from dbos._schemas.datasource_database import DatasourceSchema

from ._logger import dbos_logger
from ._serialization import Serializer

P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values


class DatasourceOptions(TypedDict, total=False):
    name: Optional[str]
    isolation_level: Optional[IsolationLevel]


class AsyncDatasource(ABC):
    @staticmethod
    def create(
        database_url: str,
        engine_kwargs: Dict[str, Any],
        engine: Optional[AsyncEngine],
        schema: Optional[str],
        serializer: Serializer,
    ) -> "AsyncDatasource":
        if database_url.startswith("sqlite"):
            from ._datasource_sqlite import SqliteAsyncDatasource

            return SqliteAsyncDatasource(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
            )
        else:
            from ._datasource_postgres import PostgresAsyncDatasource

            return PostgresAsyncDatasource(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
            )

    def __init__(
        self,
        *,
        database_url: str,
        engine_kwargs: Dict[str, Any],
        engine: Optional[AsyncEngine],
        schema: Optional[str],
        serializer: Serializer,
    ):
        import sqlalchemy.dialects.postgresql as pg
        import sqlalchemy.dialects.sqlite as sq

        if engine:
            dbos_logger.info("Initializing SyncDatasource with custom engine")
        else:
            printable_url = sa.make_url(database_url).render_as_string(
                hide_password=True
            )
            dbos_logger.info(
                f"Initializing DBOS SyncDatasource with URL: {printable_url}"
            )
            if not database_url.startswith("sqlite"):
                dbos_logger.info(
                    f"DBOS SyncDatasource engine parameters: {engine_kwargs}"
                )
        self.dialect = sq if database_url.startswith("sqlite") else pg
        self.schema = (
            None
            if database_url.startswith("sqlite")
            else (schema if schema else "dbos")
        )
        DatasourceSchema.datasource_outputs.schema = self.schema
        if engine:
            self.engine = engine
            self.created_engine = False
        else:
            self.engine = self._create_engine(database_url, engine_kwargs)
            self.created_engine = True
        self._engine_kwargs = engine_kwargs
        self.sessionmaker = async_sessionmaker(bind=self.engine)
        self.serializer = serializer

    @abstractmethod
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> AsyncEngine:
        """Create a database engine specific to the database type."""
        pass

    @abstractmethod
    async def run_migrations(self) -> None:
        """Run database migrations specific to the database type."""
        pass

    def sql_session(cls) -> AsyncSession:
        raise NotImplementedError()

    async def _check_execution(
        self, workflow_id: str, step_id: int
    ) -> None | RecordedResult:
        async with self.engine.connect() as conn:
            result = await conn.execute(
                sa.select(
                    DatasourceSchema.datasource_outputs.c.output,
                    DatasourceSchema.datasource_outputs.c.error,
                    DatasourceSchema.datasource_outputs.c.serialization,
                ).where(
                    DatasourceSchema.datasource_outputs.c.workflow_id == workflow_id,
                    DatasourceSchema.datasource_outputs.c.step_id == step_id,
                )
            )
            row = result.first()
            return (
                None
                if row is None
                else {"output": row[0], "error": row[1], "serialization": row[2]}
            )

    async def _record_error(
        self,
        workflow_id: str,
        step_id: int,
        error: str,
        serialization: str | None,
    ):
        async with self.engine.begin() as conn:
            await self._record_result(
                conn, workflow_id, step_id, None, error, serialization
            )

    async def _record_result(
        self,
        conn: AsyncConnection,
        workflow_id: str,
        step_id: int,
        output: str | None,
        error: str | None,
        serialization: str | None,
    ):
        await conn.execute(
            sa.insert(DatasourceSchema.datasource_outputs).values(
                workflow_id=workflow_id,
                step_id=step_id,
                output=output,
                error=error,
                serialization=serialization,
            )
        )

    @overload
    async def run_tx_async(
        self,
        ds_options: Optional[DatasourceOptions],
        func: Callable[P, Coroutine[Any, Any, R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R: ...

    @overload
    async def run_tx_async(
        self,
        ds_options: Optional[DatasourceOptions],
        func: Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R: ...

    async def run_tx_async(
        self,
        ds_options: Optional[DatasourceOptions],
        func: Callable[P, Coroutine[Any, Any, R]] | Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        name = (
            ds_options["name"]
            if ds_options and ds_options.get("name") is not None
            else func.__qualname__
        )
        isolation_level = (
            ds_options["isolation_level"]
            if ds_options and ds_options.get("isolation_level") is not None
            else "SERIALIZABLE"
        )
        if not inspect.iscoroutinefunction(func):
            raise DBOSException(
                f"Function {name} is not a coroutine function, but AsyncDatasource.run_tx_async requires a coroutine functions"
            )

        raise NotImplementedError()


class SyncDatasource(ABC):
    @staticmethod
    def create(
        database_url: str,
        engine_kwargs: Dict[str, Any],
        engine: Optional[sa.Engine],
        schema: Optional[str],
        serializer: Serializer,
    ) -> "SyncDatasource":
        if database_url.startswith("sqlite"):
            from ._datasource_sqlite import SqliteSyncDatasource

            return SqliteSyncDatasource(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
            )
        else:
            from ._datasource_postgres import PostgresSyncDatasource

            return PostgresSyncDatasource(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
            )

    def __init__(
        self,
        *,
        database_url: str,
        engine_kwargs: Dict[str, Any],
        engine: Optional[sa.Engine],
        schema: Optional[str],
        serializer: Serializer,
    ):
        import sqlalchemy.dialects.postgresql as pg
        import sqlalchemy.dialects.sqlite as sq

        if engine:
            dbos_logger.info("Initializing SyncDatasource with custom engine")
        else:
            printable_url = sa.make_url(database_url).render_as_string(
                hide_password=True
            )
            dbos_logger.info(
                f"Initializing DBOS SyncDatasource with URL: {printable_url}"
            )
            if not database_url.startswith("sqlite"):
                dbos_logger.info(
                    f"DBOS SyncDatasource engine parameters: {engine_kwargs}"
                )
        self.dialect = sq if database_url.startswith("sqlite") else pg
        self.schema = (
            None
            if database_url.startswith("sqlite")
            else (schema if schema else "dbos")
        )
        DatasourceSchema.datasource_outputs.schema = self.schema
        if engine:
            self.engine = engine
            self.created_engine = False
        else:
            self.engine = self._create_engine(database_url, engine_kwargs)
            self.created_engine = True
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

    def sql_session(cls) -> Session:
        raise NotImplementedError()

    @overload
    def run_tx(
        self,
        ds_options: Optional[DatasourceOptions],
        func: Callable[P, Coroutine[Any, Any, R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R: ...

    @overload
    def run_tx(
        self,
        ds_options: Optional[DatasourceOptions],
        func: Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R: ...

    def run_tx(
        self,
        ds_options: Optional[DatasourceOptions],
        func: Callable[P, Coroutine[Any, Any, R]] | Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Invoke a step function and checkpoint its result."""
        check_async("run_step")

        raise NotImplementedError()
