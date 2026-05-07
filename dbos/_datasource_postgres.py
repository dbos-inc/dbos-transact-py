from typing import Any, Dict

import sqlalchemy as sa
from sqlalchemy import URL, event
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from dbos._datasource import AsyncDatasource, SyncDatasource

from ._logger import dbos_logger


def _make_url(database_url: str) -> URL:
    return sa.make_url(database_url).set(drivername="postgresql+psycopg")


class PostgresAsyncDatasource(AsyncDatasource):
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> AsyncEngine:
        url = _make_url(database_url)
        return create_async_engine(url, **engine_kwargs)

    async def run_migrations(self) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(sa.text(f'CREATE SCHEMA "{self.schema}"'))
            await conn.execute(
                sa.text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}".datasource_outputs (
                        workflow_id TEXT NOT NULL,
                        step_id INT NOT NULL,
                        output TEXT,
                        error TEXT,
                        serialization TEXT,
                        created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
                        PRIMARY KEY (workflow_id, step_id)
                    )"""
                )
            )


class PostgresSyncDatasource(SyncDatasource):
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        url = _make_url(database_url)
        return sa.create_engine(url, **engine_kwargs)

    def run_migrations(self) -> None:
        """Run database migrations specific to the database type."""
        with self.engine.begin() as conn:
            conn.execute(sa.text(f'CREATE SCHEMA "{self.schema}"'))
            conn.execute(
                sa.text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}".datasource_outputs (
                        workflow_id TEXT NOT NULL,
                        step_id INT NOT NULL,
                        output TEXT,
                        error TEXT,
                        serialization TEXT,
                        created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
                        PRIMARY KEY (workflow_id, step_id)
                    )"""
                )
            )
