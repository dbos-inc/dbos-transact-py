from typing import Any, Dict

import psycopg
import sqlalchemy as sa
from sqlalchemy import URL
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from dbos._datasource import AsyncSQLAlchemyDatasource, SQLAlchemyDatasource

from ._datasource_migration import migrate_datasource, verify_datasource_migrations


def _is_postgres_serialization_error(error: Exception) -> bool:
    """Check if the error is a retryable PostgreSQL serialization/concurrency error.

    40001: serialization_failure (MVCC conflict)
    40P01: deadlock_detected
    """
    if not isinstance(error, DBAPIError):
        return False
    driver_error = error.orig
    return (
        driver_error is not None
        and isinstance(driver_error, psycopg.OperationalError)
        and driver_error.sqlstate in ("40001", "40P01")
    )


def _make_url(database_url: str) -> URL:
    return sa.make_url(database_url).set(drivername="postgresql+psycopg")


class PostgresAsyncDatasource(AsyncSQLAlchemyDatasource):
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> AsyncEngine:
        if engine_kwargs is None:
            engine_kwargs = {}
        return create_async_engine(_make_url(database_url), **engine_kwargs)

    async def run_migrations(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(migrate_datasource, self.schema)

    async def verify_migrations(self) -> None:
        async with self.engine.connect() as conn:
            await conn.run_sync(
                verify_datasource_migrations, self.schema, self.engine.url
            )

    def _is_serialization_error(self, error: Exception) -> bool:
        return _is_postgres_serialization_error(error)


class PostgresSyncDatasource(SQLAlchemyDatasource):
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        if engine_kwargs is None:
            engine_kwargs = {}
        return sa.create_engine(_make_url(database_url), **engine_kwargs)

    def run_migrations(self) -> None:
        with self.engine.begin() as conn:
            migrate_datasource(conn, self.schema)

    def verify_migrations(self) -> None:
        with self.engine.connect() as conn:
            verify_datasource_migrations(conn, self.schema, self.engine.url)

    def _is_serialization_error(self, error: Exception) -> bool:
        return _is_postgres_serialization_error(error)
