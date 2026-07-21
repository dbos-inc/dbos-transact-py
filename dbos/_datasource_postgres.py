from typing import Any, Dict

import sqlalchemy as sa
from sqlalchemy import URL
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from dbos._datasource import AsyncSQLAlchemyDatasource, SQLAlchemyDatasource
from dbos._pg_errors import is_serialization_error

from ._logger import dbos_logger


def _is_postgres_serialization_error(error: Exception) -> bool:
    """Check if the error is a retryable PostgreSQL serialization/concurrency error.

    40001: serialization_failure (MVCC conflict)
    40P01: deadlock_detected
    """
    if not isinstance(error, DBAPIError):
        return False
    return is_serialization_error(error)


def _make_url(database_url: str) -> URL:
    return sa.make_url(database_url).set(drivername="postgresql+psycopg")


def _schema_sql(schema: str) -> sa.TextClause:
    return sa.text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')


def _table_sql(schema: str) -> sa.TextClause:
    return sa.text(
        f"""
        CREATE TABLE IF NOT EXISTS "{schema}".datasource_outputs (
            workflow_id TEXT NOT NULL,
            step_id INT NOT NULL,
            output TEXT,
            error TEXT,
            serialization TEXT,
            created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
            PRIMARY KEY (workflow_id, step_id)
        )"""
    )


class PostgresAsyncDatasource(AsyncSQLAlchemyDatasource):
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> AsyncEngine:
        if engine_kwargs is None:
            engine_kwargs = {}
        return create_async_engine(_make_url(database_url), **engine_kwargs)

    async def run_migrations(self) -> None:
        assert self.schema is not None
        async with self.engine.begin() as conn:
            await conn.execute(_schema_sql(self.schema))
            await conn.execute(_table_sql(self.schema))

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
        assert self.schema is not None
        with self.engine.begin() as conn:
            conn.execute(_schema_sql(self.schema))
            conn.execute(_table_sql(self.schema))

    def _is_serialization_error(self, error: Exception) -> bool:
        return _is_postgres_serialization_error(error)
