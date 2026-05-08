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
        if engine_kwargs is None:
            engine_kwargs = {}
        return create_async_engine(url, **engine_kwargs)

    async def run_migrations(self) -> None:
        ds_db_url = self.engine.url
        try:
            pg_ds_engine = create_async_engine(
                ds_db_url.set(database="postgres"), **self._engine_kwargs
            )
            async with pg_ds_engine.connect() as conn:
                await conn.execution_options(isolation_level="AUTOCOMMIT")
                if not (
                    await conn.execute(
                        sa.text("SELECT 1 FROM pg_database WHERE datname=:db_name"),
                        parameters={"db_name": ds_db_url.database},
                    )
                ).scalar():
                    await conn.execute(sa.text(f"CREATE DATABASE {ds_db_url.database}"))
        except Exception:
            dbos_logger.warning(
                f"Could not connect to postgres database to verify existence of {ds_db_url.database}. Continuing..."
            )
        finally:
            await pg_ds_engine.dispose()

        async with self.engine.begin() as conn:
            await conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"'))
            await conn.execute(
                sa.text(
                    f"""
                    CREATE TABLE IF NOT EXISTS "{self.schema}".datasource_outputs (
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
        if engine_kwargs is None:
            engine_kwargs = {}
        return sa.create_engine(url, **engine_kwargs)

    def run_migrations(self) -> None:
        """Run database migrations specific to the database type."""
        with self.engine.begin() as conn:
            conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"'))
            conn.execute(
                sa.text(
                    f"""
                    CREATE TABLE IF NOT EXISTS "{self.schema}".datasource_outputs (
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
