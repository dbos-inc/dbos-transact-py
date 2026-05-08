from typing import Any, Dict

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from dbos._datasource import AsyncDatasource, SyncDatasource
from dbos._migration import get_sqlite_timestamp_expr
from dbos._schemas.datasource_database import DatasourceSchema

from ._logger import dbos_logger


class SqliteAsyncDatasource(AsyncDatasource):
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> AsyncEngine:
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
        engine = create_async_engine(database_url, **sqlite_kwargs)

        # Use IMMEDIATE transactions to serialize writers and prevent race conditions
        @event.listens_for(engine, "connect")
        def set_sqlite_immediate(dbapi_conn: Any, connection_record: Any) -> None:
            dbapi_conn.isolation_level = "IMMEDIATE"
            dbapi_conn.execute("PRAGMA foreign_keys=ON")

        return engine

    async def run_migrations(self) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(sa.text("PRAGMA foreign_keys = ON"))
            result = await conn.execute(
                sa.text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='dbos_datasource_outputs'"
                )
            )

            if result.fetchone() is None:
                await conn.execute(
                    sa.text(
                        f"""
                        CREATE TABLE datasource_outputs (
                            workflow_id TEXT NOT NULL,
                            step_id INTEGER NOT NULL,
                            output TEXT,
                            error TEXT,
                            serialization TEXT,
                            created_at INTEGER NOT NULL DEFAULT {get_sqlite_timestamp_expr()},
                            PRIMARY KEY (workflow_id, step_id)
                        )"""
                    )
                )


class SqliteSyncDatasource(SyncDatasource):
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        DatasourceSchema.datasource_outputs.schema = None
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
        return sa.create_engine(database_url, **sqlite_kwargs)

    def run_migrations(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(sa.text("PRAGMA foreign_keys = ON"))
            result = conn.execute(
                sa.text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='datasource_outputs'"
                )
            )
            if result.fetchone() is None:
                conn.execute(
                    sa.text(
                        f"""
                        CREATE TABLE datasource_outputs (
                            workflow_id TEXT NOT NULL,
                            step_id INTEGER NOT NULL,
                            output TEXT,
                            error TEXT,
                            serialization TEXT,
                            created_at BIGINT NOT NULL DEFAULT {get_sqlite_timestamp_expr()},
                            PRIMARY KEY (workflow_id, step_id)
                        )"""
                    )
                )
