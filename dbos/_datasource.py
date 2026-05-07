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
from sqlalchemy.ext.asyncio import AsyncEngine

from dbos._core import StepOptions
from dbos._dbos import DBOS, IsolationLevel, check_async

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
            dbos_logger.info("Initializing AsyncDatasource with custom engine")
        else:
            printable_db_url = sa.make_url(database_url).render_as_string(
                hide_password=True
            )
            dbos_logger.info(
                f"Initializing DBOS AsyncDatasource with URL: {printable_db_url}"
            )
            if database_url.startswith("sqlite"):
                dbos_logger.info(
                    f"Using SQLite as a AsyncDatasource. The SQLite AsyncDatasource is for development and testing. PostgreSQL is recommended for production use."
                )
            else:
                dbos_logger.info(
                    f"DBOS AsyncDatasource engine parameters: {engine_kwargs}"
                )
        self.dialect = sq if database_url.startswith("sqlite") else pg
        self.serializer = serializer
        self.schema = (
            None
            if database_url.startswith("sqlite")
            else (schema if schema else "dbos")
        )
        if engine:
            self.engine = engine
            self.created_engine = False
        else:
            self.engine = self._create_engine(database_url, engine_kwargs)
            self.created_engine = True
        self._engine_kwargs = engine_kwargs

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
            printable_db_url = sa.make_url(database_url).render_as_string(
                hide_password=True
            )
            dbos_logger.info(
                f"Initializing DBOS SyncDatasource with URL: {printable_db_url}"
            )
            if database_url.startswith("sqlite"):
                dbos_logger.info(
                    f"Using SQLite as a SyncDatasource. The SQLite SyncDatasource is for development and testing. PostgreSQL is recommended for production use."
                )
            else:
                dbos_logger.info(
                    f"DBOS SyncDatasource engine parameters: {engine_kwargs}"
                )
        self.dialect = sq if database_url.startswith("sqlite") else pg
        self.serializer = serializer
        self.schema = (
            None
            if database_url.startswith("sqlite")
            else (schema if schema else "dbos")
        )
        if engine:
            self.engine = engine
            self.created_engine = False
        else:
            self.engine = self._create_engine(database_url, engine_kwargs)
            self.created_engine = True
        self._engine_kwargs = engine_kwargs

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
