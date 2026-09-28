from typing import Any, Dict

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from dbos._datasource import AsyncSQLAlchemyDatasource, SQLAlchemyDatasource

from ._datasource_migration import migrate_datasource, verify_datasource_migrations
from ._logger import dbos_logger


def _is_sqlite_serialization_error(error: Exception) -> bool:
    """Check if the error is a retryable SQLite busy/locked error."""
    if not isinstance(error, DBAPIError):
        return False
    msg = str(error.orig).lower()
    return "database is locked" in msg or "database table is locked" in msg


_PG_ONLY_CONNECT_ARGS = frozenset(("application_name", "connect_timeout"))


def _set_sqlite_pragmas(dbapi_conn: Any, connection_record: Any) -> None:
    # Match the system database: serialize writers and ride out lock contention
    # rather than failing fast with "database is locked" (the sqlite3 default is
    # only 5 seconds, which slow disks can exceed under load).
    dbapi_conn.isolation_level = "IMMEDIATE"
    dbapi_conn.execute("PRAGMA busy_timeout=30000")
    dbapi_conn.execute("PRAGMA foreign_keys=ON")


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


class SqliteAsyncDatasource(AsyncSQLAlchemyDatasource):
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> AsyncEngine:
        engine = create_async_engine(
            database_url, **_filter_sqlite_kwargs(engine_kwargs)
        )
        # AsyncEngine events must be attached to the underlying sync engine
        event.listens_for(engine.sync_engine, "connect")(_set_sqlite_pragmas)
        return engine

    async def run_migrations(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(migrate_datasource, None)

    async def verify_migrations(self) -> None:
        async with self.engine.connect() as conn:
            await conn.run_sync(verify_datasource_migrations, None, self.engine.url)

    def _is_serialization_error(self, error: Exception) -> bool:
        return _is_sqlite_serialization_error(error)


class SqliteSyncDatasource(SQLAlchemyDatasource):
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        engine = sa.create_engine(database_url, **_filter_sqlite_kwargs(engine_kwargs))
        event.listens_for(engine, "connect")(_set_sqlite_pragmas)
        return engine

    def run_migrations(self) -> None:
        with self.engine.begin() as conn:
            migrate_datasource(conn, None)

    def verify_migrations(self) -> None:
        with self.engine.connect() as conn:
            verify_datasource_migrations(conn, None, self.engine.url)

    def _is_serialization_error(self, error: Exception) -> bool:
        return _is_sqlite_serialization_error(error)
