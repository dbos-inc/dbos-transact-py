from typing import Any, Dict

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from dbos._datasource import AsyncDatasource, SyncDatasource
from dbos._migration import get_sqlite_timestamp_expr
from dbos._schemas.datasource_database import DatasourceSchema

from ._logger import dbos_logger

_PG_ONLY_CONNECT_ARGS = frozenset(("application_name", "connect_timeout"))

_CHECK_TABLE_SQL = sa.text(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='datasource_outputs'"
)

_CREATE_TABLE_SQL = sa.text(
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


def _filter_sqlite_kwargs(engine_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    kwargs = engine_kwargs.copy()
    connect_args = kwargs.get("connect_args", {})
    if connect_args:
        filtered_keys = [k for k in connect_args if k in _PG_ONLY_CONNECT_ARGS]
        if filtered_keys:
            dbos_logger.debug(
                f"Ignoring PostgreSQL-specific connect_args for SQLite: {filtered_keys}"
            )
        kwargs["connect_args"] = {
            k: v for k, v in connect_args.items() if k not in _PG_ONLY_CONNECT_ARGS
        }
    return kwargs


class SqliteAsyncDatasource(AsyncDatasource):
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> AsyncEngine:
        engine = create_async_engine(
            database_url, **_filter_sqlite_kwargs(engine_kwargs)
        )

        # Use IMMEDIATE transactions to serialize writers and prevent race conditions
        @event.listens_for(engine, "connect")
        def set_sqlite_immediate(dbapi_conn: Any, connection_record: Any) -> None:
            dbapi_conn.isolation_level = "IMMEDIATE"
            dbapi_conn.execute("PRAGMA foreign_keys=ON")

        return engine

    async def run_migrations(self) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(sa.text("PRAGMA foreign_keys = ON"))
            result = await conn.execute(_CHECK_TABLE_SQL)
            if result.fetchone() is None:
                await conn.execute(_CREATE_TABLE_SQL)


class SqliteSyncDatasource(SyncDatasource):
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        DatasourceSchema.datasource_outputs.schema = None
        return sa.create_engine(database_url, **_filter_sqlite_kwargs(engine_kwargs))

    def run_migrations(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(sa.text("PRAGMA foreign_keys = ON"))
            result = conn.execute(_CHECK_TABLE_SQL)
            if result.fetchone() is None:
                conn.execute(_CREATE_TABLE_SQL)
