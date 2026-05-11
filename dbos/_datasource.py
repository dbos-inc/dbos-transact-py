import inspect
from abc import ABC, abstractmethod
from functools import wraps
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Optional,
    ParamSpec,
    TypedDict,
    TypeVar,
    Union,
    cast,
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
from dbos._context import DBOSContextEnsure, get_local_dbos_context
from dbos._dbos import IsolationLevel
from dbos._error import DBOSException
from dbos._schemas.datasource_database import DatasourceSchema
from dbos._serialization import (
    DBOSDefaultSerializer,
    Serializer,
    deserialize_exception,
    deserialize_value,
    serialize_exception,
    serialize_value,
)

from ._logger import dbos_logger

P = ParamSpec("P")
R = TypeVar("R")


def _parse_ds_options(
    ds_options: Optional["DatasourceOptions"], func: Callable[..., Any]
) -> "tuple[str, str]":
    name = (ds_options.get("name") if ds_options else None) or func.__qualname__
    isolation_level: str = (
        ds_options.get("isolation_level") if ds_options else None
    ) or "SERIALIZABLE"
    return name, isolation_level


def _replay_recorded(recorded: "RecordedResult", serializer: "Serializer") -> Any:
    if recorded["error"]:
        raise deserialize_exception(
            recorded["error"], recorded["serialization"], serializer
        )
    elif recorded["output"] is not None:
        return deserialize_value(
            recorded["output"], recorded["serialization"], serializer
        )
    else:
        raise DBOSException("Datasource recorded output and error are both None")


def _row_to_result(row: Any) -> Optional[RecordedResult]:
    return (
        None
        if row is None
        else {"output": row[0], "error": row[1], "serialization": row[2]}
    )


def _log_datasource_init(
    name: str,
    database_url: str,
    engine_kwargs: Dict[str, Any],
    has_engine: bool,
) -> None:
    if has_engine:
        dbos_logger.info(f"Initializing {name} with custom engine")
    else:
        printable_url = sa.make_url(database_url).render_as_string(hide_password=True)
        dbos_logger.info(f"Initializing DBOS {name} with URL: {printable_url}")
        if not database_url.startswith("sqlite"):
            dbos_logger.info(f"DBOS {name} engine parameters: {engine_kwargs}")


def _resolve_schema(database_url: str, schema: Optional[str]) -> Optional[str]:
    return None if database_url.startswith("sqlite") else (schema or "dbos")


class DatasourceOptions(TypedDict, total=False):
    name: Optional[str]
    isolation_level: Optional[IsolationLevel]


class AsyncDatasource(ABC):

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

        _log_datasource_init(
            "AsyncDatasource", database_url, engine_kwargs, bool(engine)
        )
        self.dialect = sq if database_url.startswith("sqlite") else pg
        self.schema = _resolve_schema(database_url, schema)
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

    @staticmethod
    async def create(
        database_url: str,
        engine_kwargs: Optional[Dict[str, Any]] = None,
        engine: Optional[AsyncEngine] = None,
        schema: Optional[str] = None,
        serializer: Optional[Serializer] = None,
    ) -> "AsyncDatasource":
        if serializer is None:
            serializer = DBOSDefaultSerializer
        if engine_kwargs is None:
            engine_kwargs = {}
        if database_url.startswith("sqlite"):
            from ._datasource_sqlite import SqliteAsyncDatasource

            instance: AsyncDatasource = SqliteAsyncDatasource(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
            )
        else:
            from ._datasource_postgres import PostgresAsyncDatasource

            instance = PostgresAsyncDatasource(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
            )
        await instance.run_migrations()
        return instance

    @abstractmethod
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> AsyncEngine:
        pass

    @abstractmethod
    async def run_migrations(self) -> None:
        pass

    def sql_session(self) -> AsyncSession:
        ctx = get_local_dbos_context()
        assert (
            ctx is not None and ctx.async_ds_session is not None
        ), "sql_session() must be called within an async datasource transaction"
        return ctx.async_ds_session

    async def _check_execution(
        self, workflow_id: str, step_id: int
    ) -> Optional[RecordedResult]:
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
            return _row_to_result(result.first())

    async def _record_error(
        self,
        workflow_id: str,
        step_id: int,
        error: str,
        serialization: Optional[str],
    ) -> None:
        async with self.engine.begin() as conn:
            await self._record_result(
                conn, workflow_id, step_id, None, error, serialization
            )

    async def _record_result(
        self,
        conn: Union[AsyncConnection, AsyncSession],
        workflow_id: str,
        step_id: int,
        output: Optional[str],
        error: Optional[str],
        serialization: Optional[str],
    ) -> None:
        await conn.execute(
            sa.insert(DatasourceSchema.datasource_outputs).values(
                workflow_id=workflow_id,
                step_id=step_id,
                output=output,
                error=error,
                serialization=serialization,
            )
        )

    async def run_tx_step_async(
        self,
        ds_options: Optional[DatasourceOptions],
        func: Callable[P, Coroutine[Any, Any, R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        name, isolation_level = _parse_ds_options(ds_options, func)

        if not inspect.iscoroutinefunction(func):
            raise DBOSException(
                f"Function {name} must be a coroutine function for AsyncDatasource"
            )

        ctx = get_local_dbos_context()
        in_wf = ctx is not None and ctx.is_workflow()

        async def _body() -> R:
            workflow_id: str = ""
            step_id: int = -1

            if in_wf:
                inner_ctx = get_local_dbos_context()
                assert inner_ctx is not None
                workflow_id = inner_ctx.workflow_id
                step_id = inner_ctx.curr_step_function_id
                recorded = await self._check_execution(workflow_id, step_id)
                if recorded is not None:
                    return cast(R, _replay_recorded(recorded, self.serializer))

            output: R
            with DBOSContextEnsure() as exec_ctx:
                async with self.sessionmaker() as session:
                    exec_ctx.start_async_ds_transaction(session)
                    try:
                        async with session.begin():
                            await session.connection(
                                execution_options={"isolation_level": isolation_level}
                            )
                            output = await func(*args, **kwargs)
                            if in_wf:
                                serialized, serialization = serialize_value(
                                    output, None, self.serializer
                                )
                                await self._record_result(
                                    session,
                                    workflow_id,
                                    step_id,
                                    serialized,
                                    None,
                                    serialization,
                                )
                    except Exception as e:
                        if in_wf:
                            serialized_e, serialization = serialize_exception(
                                e, None, self.serializer
                            )
                            await self._record_error(
                                workflow_id, step_id, serialized_e, serialization
                            )
                        raise
                    finally:
                        exec_ctx.end_async_ds_transaction()

            return output

        if in_wf:
            from dbos._core import StepOptions, run_step_async
            from dbos._dbos import _get_dbos_instance

            assert ctx is not None
            step_options: StepOptions = {"name": name}
            return await run_step_async(
                _get_dbos_instance(),
                ctx.snapshot_step_ctx(),
                _body,
                step_options,
                (),
                {},
            )
        return await _body()

    @overload
    def transaction(
        self, func: Callable[P, Coroutine[Any, Any, R]]
    ) -> Callable[P, Coroutine[Any, Any, R]]: ...

    @overload
    def transaction(
        self,
        func: None = None,
        *,
        name: Optional[str] = None,
        isolation_level: IsolationLevel = "SERIALIZABLE",
    ) -> Callable[
        [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
    ]: ...

    def transaction(
        self,
        func: Optional[Callable[..., Any]] = None,
        *,
        name: Optional[str] = None,
        isolation_level: IsolationLevel = "SERIALIZABLE",
    ) -> Any:
        def decorator(
            f: Callable[..., Coroutine[Any, Any, Any]],
        ) -> Callable[..., Coroutine[Any, Any, Any]]:
            if not inspect.iscoroutinefunction(f):
                raise DBOSException(
                    f"AsyncDatasource.transaction requires a coroutine function, "
                    f"but {f.__qualname__} is not"
                )
            ds_options: DatasourceOptions = {"isolation_level": isolation_level}
            if name is not None:
                ds_options["name"] = name

            @wraps(f)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                return await self.run_tx_step_async(ds_options, f, *args, **kwargs)

            return wrapper

        if func is not None:
            return decorator(func)
        return decorator


class SyncDatasource(ABC):

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

        _log_datasource_init(
            "SyncDatasource", database_url, engine_kwargs, bool(engine)
        )
        self.dialect = sq if database_url.startswith("sqlite") else pg
        self.schema = _resolve_schema(database_url, schema)
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

    @staticmethod
    def create(
        database_url: str,
        engine_kwargs: Optional[Dict[str, Any]] = None,
        engine: Optional[sa.Engine] = None,
        schema: Optional[str] = None,
        serializer: Optional[Serializer] = None,
    ) -> "SyncDatasource":
        if serializer is None:
            serializer = DBOSDefaultSerializer
        if engine_kwargs is None:
            engine_kwargs = {}
        if database_url.startswith("sqlite"):
            from ._datasource_sqlite import SqliteSyncDatasource

            instance: SyncDatasource = SqliteSyncDatasource(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
            )
        else:
            from ._datasource_postgres import PostgresSyncDatasource

            instance = PostgresSyncDatasource(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
            )
        instance.run_migrations()
        return instance

    @abstractmethod
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        pass

    @abstractmethod
    def run_migrations(self) -> None:
        pass

    def sql_session(self) -> Session:
        ctx = get_local_dbos_context()
        assert (
            ctx is not None and ctx.sync_ds_session is not None
        ), "sql_session() must be called within a sync datasource transaction"
        return ctx.sync_ds_session

    def _check_execution(
        self, workflow_id: str, step_id: int
    ) -> Optional[RecordedResult]:
        with self.engine.connect() as conn:
            result = conn.execute(
                sa.select(
                    DatasourceSchema.datasource_outputs.c.output,
                    DatasourceSchema.datasource_outputs.c.error,
                    DatasourceSchema.datasource_outputs.c.serialization,
                ).where(
                    DatasourceSchema.datasource_outputs.c.workflow_id == workflow_id,
                    DatasourceSchema.datasource_outputs.c.step_id == step_id,
                )
            )
            return _row_to_result(result.first())

    def _record_error(
        self,
        workflow_id: str,
        step_id: int,
        error: str,
        serialization: Optional[str],
    ) -> None:
        with self.engine.begin() as conn:
            self._record_result(conn, workflow_id, step_id, None, error, serialization)

    def _record_result(
        self,
        conn: Union[sa.Connection, Session],
        workflow_id: str,
        step_id: int,
        output: Optional[str],
        error: Optional[str],
        serialization: Optional[str],
    ) -> None:
        conn.execute(
            sa.insert(DatasourceSchema.datasource_outputs).values(
                workflow_id=workflow_id,
                step_id=step_id,
                output=output,
                error=error,
                serialization=serialization,
            )
        )

    def run_tx_step(
        self,
        ds_options: Optional[DatasourceOptions],
        func: Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        name, isolation_level = _parse_ds_options(ds_options, func)

        if inspect.iscoroutinefunction(func):
            raise DBOSException(
                f"Function {name} is a coroutine; use AsyncDatasource.run_tx_step_async instead"
            )

        ctx = get_local_dbos_context()
        in_wf = ctx is not None and ctx.is_workflow()

        def _body() -> R:
            workflow_id: str = ""
            step_id: int = -1

            if in_wf:
                inner_ctx = get_local_dbos_context()
                assert inner_ctx is not None
                workflow_id = inner_ctx.workflow_id
                step_id = inner_ctx.curr_step_function_id
                recorded = self._check_execution(workflow_id, step_id)
                if recorded is not None:
                    return cast(R, _replay_recorded(recorded, self.serializer))

            output: R
            with DBOSContextEnsure() as exec_ctx:
                with self.sessionmaker() as session:
                    exec_ctx.start_sync_ds_transaction(session)
                    try:
                        with session.begin():
                            session.connection(
                                execution_options={"isolation_level": isolation_level}
                            )
                            output = func(*args, **kwargs)
                            if in_wf:
                                serialized, serialization = serialize_value(
                                    output, None, self.serializer
                                )
                                self._record_result(
                                    session,
                                    workflow_id,
                                    step_id,
                                    serialized,
                                    None,
                                    serialization,
                                )
                    except Exception as e:
                        if in_wf:
                            serialized_e, serialization = serialize_exception(
                                e, None, self.serializer
                            )
                            self._record_error(
                                workflow_id, step_id, serialized_e, serialization
                            )
                        raise
                    finally:
                        exec_ctx.end_sync_ds_transaction()

            return output

        if in_wf:
            from dbos._core import StepOptions, run_step
            from dbos._dbos import _get_dbos_instance

            step_options: StepOptions = {"name": name}
            return run_step(_get_dbos_instance(), _body, step_options, (), {})
        return _body()

    @overload
    def transaction(self, func: Callable[P, R]) -> Callable[P, R]: ...

    @overload
    def transaction(
        self,
        func: None = None,
        *,
        name: Optional[str] = None,
        isolation_level: IsolationLevel = "SERIALIZABLE",
    ) -> Callable[[Callable[P, R]], Callable[P, R]]: ...

    def transaction(
        self,
        func: Optional[Callable[..., Any]] = None,
        *,
        name: Optional[str] = None,
        isolation_level: IsolationLevel = "SERIALIZABLE",
    ) -> Any:
        def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
            if inspect.iscoroutinefunction(f):
                raise DBOSException(
                    f"SyncDatasource.transaction requires a non-coroutine function, "
                    f"but {f.__qualname__} is a coroutine"
                )
            ds_options: DatasourceOptions = {"isolation_level": isolation_level}
            if name is not None:
                ds_options["name"] = name

            @wraps(f)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                return self.run_tx_step(ds_options, f, *args, **kwargs)

            return wrapper

        if func is not None:
            return decorator(func)
        return decorator
