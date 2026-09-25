import asyncio
import inspect
import time
from abc import ABC, abstractmethod
from functools import wraps
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Literal,
    Optional,
    ParamSpec,
    TypedDict,
    TypeVar,
    Union,
    cast,
    overload,
)

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker as SyncSessionmaker

from dbos._context import DBOSContextEnsure, current_owner_xid, get_local_dbos_context
from dbos._error import DBOSException, DBOSWorkflowConflictIDError
from dbos._schemas.datasource_database import datasource_outputs_table
from dbos._serialization import (
    DBOSDefaultSerializer,
    Serializer,
    deserialize_exception,
    deserialize_value,
    serialize_exception,
    serialize_value,
)
from dbos._utils import retriable_postgres_exception

from ._logger import dbos_logger

_INITIAL_RETRY_WAIT_SECONDS = 0.001
_RETRY_BACKOFF_FACTOR = 1.5
_MAX_RETRY_WAIT_SECONDS = 2.0

P = ParamSpec("P")
R = TypeVar("R")

IsolationLevel = Literal[
    "SERIALIZABLE",
    "REPEATABLE READ",
    "READ COMMITTED",
]


class RecordedResult(TypedDict):
    output: Optional[str]  # determined by `serialization`
    error: Optional[str]  # determined by `serialization`
    serialization: Optional[str]


class _StepAlreadyRecorded(Exception):
    """Internal signal: a concurrent execution recorded this step's result first.

    Raised by _record_result, so the user transaction is always rolled back by the
    time it is handled — either by this raise unwinding it, or by an earlier failure.
    """


def _is_retriable_db_error(
    error: Exception,
    is_serialization_error: Callable[[Exception], bool],
) -> bool:
    """Transient database errors worth re-running the attempt for."""
    return isinstance(error, DBAPIError) and (
        retriable_postgres_exception(error) or is_serialization_error(error)
    )


def _parse_ds_options(
    ds_options: Optional["DatasourceOptions"], func: Callable[..., Any]
) -> "tuple[str, str]":
    name = (ds_options.get("name") if ds_options else None) or func.__qualname__
    isolation_level: str = (
        ds_options.get("isolation_level") if ds_options else None
    ) or "SERIALIZABLE"
    return name, isolation_level


def _still_owns(workflow_id: str, owner_xid: Optional[str] = None) -> bool:
    """Whether owner_xid (default: the caller's token) still owns the workflow.
    With no token to check, it assumes so."""
    if owner_xid is None:
        owner_xid = current_owner_xid(workflow_id)
    if owner_xid is None:
        return True
    from dbos._dbos import _get_dbos_instance

    return _get_dbos_instance()._sys_db.get_workflow_owner(workflow_id) == owner_xid


def _reject_session_binds(session_kw: Dict[str, Any]) -> None:
    # Per-mapper binds outrank bind=self.engine, which would split user writes from the checkpoint.
    if session_kw.get("binds"):
        raise DBOSException(
            "A datasource sessionmaker must not set binds=: every statement in a datasource "
            "transaction must use the datasource's engine so it commits atomically with the "
            "step checkpoint"
        )


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


def _register_datasource(
    ds: Union["SQLAlchemyDatasource", "AsyncSQLAlchemyDatasource"],
) -> None:
    from dbos._dbos import _get_or_create_dbos_registry

    _get_or_create_dbos_registry().register_datasource(ds)


def _delete_checkpoints_sql(t: sa.Table, workflow_id: str, start_step: int) -> Any:
    return sa.delete(t).where(
        (t.c.workflow_id == workflow_id) & (t.c.step_id >= start_step)
    )


class DatasourceOptions(TypedDict, total=False):
    name: Optional[str]
    isolation_level: Optional[IsolationLevel]


class AsyncSQLAlchemyDatasource(ABC):

    def __init__(
        self,
        *,
        database_url: str,
        engine_kwargs: Dict[str, Any],
        engine: Optional[AsyncEngine],
        schema: Optional[str],
        serializer: Serializer,
        sessionmaker: Optional[async_sessionmaker[Any]] = None,
    ):
        import sqlalchemy.dialects.postgresql as pg
        import sqlalchemy.dialects.sqlite as sq

        if sessionmaker is not None and not isinstance(
            sessionmaker, async_sessionmaker
        ):
            raise DBOSException(
                "AsyncSQLAlchemyDatasource requires an async_sessionmaker"
            )
        if sessionmaker is not None:
            _reject_session_binds(sessionmaker.kw)
        _log_datasource_init(
            "AsyncDatasource", database_url, engine_kwargs, bool(engine)
        )
        self.dialect = sq if database_url.startswith("sqlite") else pg
        self.schema = _resolve_schema(database_url, schema)
        if engine:
            self.engine = engine
            self.created_engine = False
        else:
            self.engine = self._create_engine(database_url, engine_kwargs)
            self.created_engine = True
        self._outputs_table = datasource_outputs_table(self.schema)
        # Pinned on DBOS's own statements, so a caller engine's schema_translate_map can't move them.
        self._pin: Dict[str, Any] = {"schema_translate_map": {self.schema: self.schema}}
        # Sessions are always opened with bind=self.engine, so checkpoints share the engine that reads them.
        # No expiry by default: returned ORM objects outlive the session and are checkpointed after commit.
        self.sessionmaker: async_sessionmaker[Any] = (
            sessionmaker
            if sessionmaker is not None
            else async_sessionmaker(expire_on_commit=False)
        )
        self.serializer = serializer
        _register_datasource(self)

    @staticmethod
    async def create(
        database_url: str,
        engine_kwargs: Optional[Dict[str, Any]] = None,
        engine: Optional[AsyncEngine] = None,
        schema: Optional[str] = None,
        serializer: Optional[Serializer] = None,
        sessionmaker: Optional[async_sessionmaker[Any]] = None,
        run_migrations: bool = True,
    ) -> "AsyncSQLAlchemyDatasource ":
        if serializer is None:
            serializer = DBOSDefaultSerializer
        if engine_kwargs is None:
            engine_kwargs = {}
        if database_url.startswith("sqlite"):
            from ._datasource_sqlite import SqliteAsyncDatasource

            instance: AsyncSQLAlchemyDatasource = SqliteAsyncDatasource(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
                sessionmaker=sessionmaker,
            )
        else:
            from ._datasource_postgres import PostgresAsyncDatasource

            instance = PostgresAsyncDatasource(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
                sessionmaker=sessionmaker,
            )
        if run_migrations:
            await instance.run_migrations()
        else:
            # This role may not be allowed to run DDL, but it still requires an up-to-date schema.
            await instance.verify_migrations()
        return instance

    @abstractmethod
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> AsyncEngine:
        pass

    @abstractmethod
    async def run_migrations(self) -> None:
        pass

    @abstractmethod
    async def verify_migrations(self) -> None:
        pass

    @abstractmethod
    def _is_serialization_error(self, error: Exception) -> bool:
        """Return True if the error is a retryable serialization/concurrency error."""
        pass

    async def _delete_checkpoints(self, workflow_id: str, start_step: int) -> None:
        """Delete this workflow's checkpoints from start_step on."""
        async with self.engine.begin() as conn:
            await conn.execute(
                _delete_checkpoints_sql(self._outputs_table, workflow_id, start_step),
                execution_options=self._pin,
            )

    async def _delete_checkpoints_if_owner(
        self, workflow_id: str, owner_xid: str
    ) -> bool:
        """Delete the workflow's checkpoints; commit only while owner_xid owns it."""
        async with self.engine.connect() as conn:
            async with conn.begin() as txn:
                await conn.execute(
                    _delete_checkpoints_sql(self._outputs_table, workflow_id, 1),
                    execution_options=self._pin,
                )
                # Delete, then check: every removed row predates any later owner's.
                if not await asyncio.to_thread(_still_owns, workflow_id, owner_xid):
                    await txn.rollback()
                    return False
        return True

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
                    self._outputs_table.c.output,
                    self._outputs_table.c.error,
                    self._outputs_table.c.serialization,
                ).where(
                    self._outputs_table.c.workflow_id == workflow_id,
                    self._outputs_table.c.step_id == step_id,
                ),
                execution_options=self._pin,
            )
            return _row_to_result(result.first())

    async def _check_execution_with_retry(
        self, workflow_id: str, step_id: int
    ) -> Optional[RecordedResult]:
        # Retry the OAOO pre-check read on transient connection and serialization errors.
        retry_wait_seconds = _INITIAL_RETRY_WAIT_SECONDS
        while True:
            try:
                return await self._check_execution(workflow_id, step_id)
            except DBAPIError as dbapi_error:
                if not _is_retriable_db_error(
                    dbapi_error, self._is_serialization_error
                ):
                    raise
                await asyncio.sleep(retry_wait_seconds)
                retry_wait_seconds = min(
                    retry_wait_seconds * _RETRY_BACKOFF_FACTOR,
                    _MAX_RETRY_WAIT_SECONDS,
                )

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
        """Record this step's outcome, raising _StepAlreadyRecorded if a concurrent execution beat us to it."""
        result = await conn.execute(
            self.dialect.insert(self._outputs_table)
            .values(
                workflow_id=workflow_id,
                step_id=step_id,
                output=output,
                error=error,
                serialization=serialization,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    self._outputs_table.c.workflow_id,
                    self._outputs_table.c.step_id,
                ]
            )
            # No row back means a concurrent execution's row was already visible to this
            # snapshot. Above READ COMMITTED it can instead surface as a serialization
            # error, which the caller's retry loop converges to this case.
            .returning(self._outputs_table.c.workflow_id),
            execution_options=self._pin,
        )
        if result.first() is None:
            raise _StepAlreadyRecorded()

    async def _replay_conflicting_step(self, workflow_id: str, step_id: int) -> Any:
        # The recorded row is this step's durable result, whichever execution committed it.
        recorded = await self._check_execution_with_retry(workflow_id, step_id)
        if recorded is None:
            raise DBOSException(
                f"Datasource step {step_id} of workflow {workflow_id} conflicted with a "
                "concurrent execution, but no recorded result was found"
            )
        return _replay_recorded(recorded, self.serializer)

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
        if ctx is not None and in_wf and self not in ctx.used_datasources:
            # Recorded even on replay, so completion also clears an earlier execution's rows.
            ctx.used_datasources.append(self)

        async def _body() -> R:
            workflow_id: str = ""
            step_id: int = -1

            if in_wf:
                inner_ctx = get_local_dbos_context()
                assert inner_ctx is not None
                workflow_id = inner_ctx.workflow_id
                step_id = inner_ctx.curr_step_function_id
                recorded = await self._check_execution_with_retry(workflow_id, step_id)
                if recorded is not None:
                    return cast(R, _replay_recorded(recorded, self.serializer))

            output: R
            conflicted = False
            retry_wait_seconds = _INITIAL_RETRY_WAIT_SECONDS
            try:
                with DBOSContextEnsure() as exec_ctx:
                    while True:
                        async with self.sessionmaker(bind=self.engine) as session:
                            exec_ctx.start_async_ds_transaction(session)
                            try:
                                async with session.begin():
                                    await session.connection(
                                        execution_options={
                                            "isolation_level": isolation_level
                                        }
                                    )
                                    output = await func(*args, **kwargs)
                                    if in_wf:
                                        # Flush first, so the checkpoint holds generated keys and defaults, as the caller sees them.
                                        await session.flush()
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
                                        # Holding this step's row, so a later owner's insert waits on our commit.
                                        if not await asyncio.to_thread(
                                            _still_owns, workflow_id
                                        ):
                                            raise DBOSWorkflowConflictIDError(
                                                workflow_id
                                            )
                                break
                            except _StepAlreadyRecorded:
                                raise  # the recorded result wins; don't record an error over it
                            except Exception as e:
                                if _is_retriable_db_error(
                                    e, self._is_serialization_error
                                ):
                                    inner_ctx = get_local_dbos_context()
                                    span = (
                                        inner_ctx.get_current_dbos_span()
                                        if inner_ctx is not None
                                        else None
                                    )
                                    if span:
                                        span.add_event(
                                            "Transaction Failure",
                                            {"retry_wait_seconds": retry_wait_seconds},
                                        )
                                    await asyncio.sleep(retry_wait_seconds)
                                    retry_wait_seconds = min(
                                        retry_wait_seconds * _RETRY_BACKOFF_FACTOR,
                                        _MAX_RETRY_WAIT_SECONDS,
                                    )
                                    continue
                                if in_wf:
                                    serialized_e, serialization = serialize_exception(
                                        e, None, self.serializer
                                    )
                                    await self._record_error(
                                        workflow_id,
                                        step_id,
                                        serialized_e,
                                        serialization,
                                    )
                                raise
                            finally:
                                exec_ctx.end_async_ds_transaction()
            except _StepAlreadyRecorded:
                conflicted = True

            # Outside the except block, so the internal signal stays out of the traceback chain.
            if conflicted:
                if not await asyncio.to_thread(_still_owns, workflow_id):
                    # Another execution owns the workflow: stop, as an ordinary step's loser does.
                    raise DBOSWorkflowConflictIDError(workflow_id)
                # Still the owner: the recorded row is our own ambiguous commit or a stale execution's, and either is this step's result.
                return cast(
                    R, await self._replay_conflicting_step(workflow_id, step_id)
                )

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


class SQLAlchemyDatasource(ABC):

    def __init__(
        self,
        *,
        database_url: str,
        engine_kwargs: Dict[str, Any],
        engine: Optional[sa.Engine],
        schema: Optional[str],
        serializer: Serializer,
        sessionmaker: Optional[SyncSessionmaker[Any]] = None,
    ):
        import sqlalchemy.dialects.postgresql as pg
        import sqlalchemy.dialects.sqlite as sq

        if sessionmaker is not None and not isinstance(sessionmaker, SyncSessionmaker):
            raise DBOSException(
                "SQLAlchemyDatasource requires a sqlalchemy.orm.sessionmaker"
            )
        if sessionmaker is not None:
            _reject_session_binds(sessionmaker.kw)
        _log_datasource_init(
            "SyncDatasource", database_url, engine_kwargs, bool(engine)
        )
        self.dialect = sq if database_url.startswith("sqlite") else pg
        self.schema = _resolve_schema(database_url, schema)
        if engine:
            self.engine = engine
            self.created_engine = False
        else:
            self.engine = self._create_engine(database_url, engine_kwargs)
            self.created_engine = True
        self._outputs_table = datasource_outputs_table(self.schema)
        # Pinned on DBOS's own statements, so a caller engine's schema_translate_map can't move them.
        self._pin: Dict[str, Any] = {"schema_translate_map": {self.schema: self.schema}}
        # Sessions are always opened with bind=self.engine, so checkpoints share the engine that reads them.
        # No expiry by default: returned ORM objects outlive the session and are checkpointed after commit.
        self.sessionmaker: SyncSessionmaker[Any] = (
            sessionmaker
            if sessionmaker is not None
            else SyncSessionmaker(expire_on_commit=False)
        )
        self.serializer = serializer
        _register_datasource(self)

    @staticmethod
    def create(
        database_url: str,
        engine_kwargs: Optional[Dict[str, Any]] = None,
        engine: Optional[sa.Engine] = None,
        schema: Optional[str] = None,
        serializer: Optional[Serializer] = None,
        sessionmaker: Optional[SyncSessionmaker[Any]] = None,
        run_migrations: bool = True,
    ) -> "SQLAlchemyDatasource ":
        if serializer is None:
            serializer = DBOSDefaultSerializer
        if engine_kwargs is None:
            engine_kwargs = {}
        if database_url.startswith("sqlite"):
            from ._datasource_sqlite import SqliteSyncDatasource

            instance: SQLAlchemyDatasource = SqliteSyncDatasource(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
                sessionmaker=sessionmaker,
            )
        else:
            from ._datasource_postgres import PostgresSyncDatasource

            instance = PostgresSyncDatasource(
                database_url=database_url,
                engine_kwargs=engine_kwargs,
                engine=engine,
                schema=schema,
                serializer=serializer,
                sessionmaker=sessionmaker,
            )
        if run_migrations:
            instance.run_migrations()
        else:
            # This role may not be allowed to run DDL, but it still requires an up-to-date schema.
            instance.verify_migrations()
        return instance

    @abstractmethod
    def _create_engine(
        self, database_url: str, engine_kwargs: Dict[str, Any]
    ) -> sa.Engine:
        pass

    @abstractmethod
    def run_migrations(self) -> None:
        pass

    @abstractmethod
    def verify_migrations(self) -> None:
        pass

    @abstractmethod
    def _is_serialization_error(self, error: Exception) -> bool:
        """Return True if the error is a retryable serialization/concurrency error."""
        pass

    def _delete_checkpoints(self, workflow_id: str, start_step: int) -> None:
        """Delete this workflow's checkpoints from start_step on."""
        with self.engine.begin() as conn:
            conn.execute(
                _delete_checkpoints_sql(self._outputs_table, workflow_id, start_step),
                execution_options=self._pin,
            )

    def _delete_checkpoints_if_owner(self, workflow_id: str, owner_xid: str) -> bool:
        """Delete the workflow's checkpoints; commit only while owner_xid owns it."""
        with self.engine.connect() as conn:
            with conn.begin() as txn:
                conn.execute(
                    _delete_checkpoints_sql(self._outputs_table, workflow_id, 1),
                    execution_options=self._pin,
                )
                # Delete, then check: every removed row predates any later owner's.
                if not _still_owns(workflow_id, owner_xid):
                    txn.rollback()
                    return False
        return True

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
                    self._outputs_table.c.output,
                    self._outputs_table.c.error,
                    self._outputs_table.c.serialization,
                ).where(
                    self._outputs_table.c.workflow_id == workflow_id,
                    self._outputs_table.c.step_id == step_id,
                ),
                execution_options=self._pin,
            )
            return _row_to_result(result.first())

    def _check_execution_with_retry(
        self, workflow_id: str, step_id: int
    ) -> Optional[RecordedResult]:
        # Retry the OAOO pre-check read on transient connection and serialization errors.
        retry_wait_seconds = _INITIAL_RETRY_WAIT_SECONDS
        while True:
            try:
                return self._check_execution(workflow_id, step_id)
            except DBAPIError as dbapi_error:
                if not _is_retriable_db_error(
                    dbapi_error, self._is_serialization_error
                ):
                    raise
                time.sleep(retry_wait_seconds)
                retry_wait_seconds = min(
                    retry_wait_seconds * _RETRY_BACKOFF_FACTOR,
                    _MAX_RETRY_WAIT_SECONDS,
                )

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
        """Record this step's outcome, raising _StepAlreadyRecorded if a concurrent execution beat us to it."""
        result = conn.execute(
            self.dialect.insert(self._outputs_table)
            .values(
                workflow_id=workflow_id,
                step_id=step_id,
                output=output,
                error=error,
                serialization=serialization,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    self._outputs_table.c.workflow_id,
                    self._outputs_table.c.step_id,
                ]
            )
            # No row back means a concurrent execution's row was already visible to this
            # snapshot. Above READ COMMITTED it can instead surface as a serialization
            # error, which the caller's retry loop converges to this case.
            .returning(self._outputs_table.c.workflow_id),
            execution_options=self._pin,
        )
        if result.first() is None:
            raise _StepAlreadyRecorded()

    def _replay_conflicting_step(self, workflow_id: str, step_id: int) -> Any:
        # The recorded row is this step's durable result, whichever execution committed it.
        recorded = self._check_execution_with_retry(workflow_id, step_id)
        if recorded is None:
            raise DBOSException(
                f"Datasource step {step_id} of workflow {workflow_id} conflicted with a "
                "concurrent execution, but no recorded result was found"
            )
        return _replay_recorded(recorded, self.serializer)

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
        if ctx is not None and in_wf and self not in ctx.used_datasources:
            # Recorded even on replay, so completion also clears an earlier execution's rows.
            ctx.used_datasources.append(self)

        def _body() -> R:
            workflow_id: str = ""
            step_id: int = -1

            if in_wf:
                inner_ctx = get_local_dbos_context()
                assert inner_ctx is not None
                workflow_id = inner_ctx.workflow_id
                step_id = inner_ctx.curr_step_function_id
                recorded = self._check_execution_with_retry(workflow_id, step_id)
                if recorded is not None:
                    return cast(R, _replay_recorded(recorded, self.serializer))

            output: R
            conflicted = False
            retry_wait_seconds = _INITIAL_RETRY_WAIT_SECONDS
            try:
                with DBOSContextEnsure() as exec_ctx:
                    while True:
                        with self.sessionmaker(bind=self.engine) as session:
                            exec_ctx.start_sync_ds_transaction(session)
                            try:
                                with session.begin():
                                    session.connection(
                                        execution_options={
                                            "isolation_level": isolation_level
                                        }
                                    )
                                    output = func(*args, **kwargs)
                                    if in_wf:
                                        # Flush first, so the checkpoint holds generated keys and defaults, as the caller sees them.
                                        session.flush()
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
                                        # Holding this step's row, so a later owner's insert waits on our commit.
                                        if not _still_owns(workflow_id):
                                            raise DBOSWorkflowConflictIDError(
                                                workflow_id
                                            )
                                break
                            except _StepAlreadyRecorded:
                                raise  # the recorded result wins; don't record an error over it
                            except Exception as e:
                                if _is_retriable_db_error(
                                    e, self._is_serialization_error
                                ):
                                    inner_ctx = get_local_dbos_context()
                                    span = (
                                        inner_ctx.get_current_dbos_span()
                                        if inner_ctx is not None
                                        else None
                                    )
                                    if span:
                                        span.add_event(
                                            "Transaction Failure",
                                            {"retry_wait_seconds": retry_wait_seconds},
                                        )
                                    time.sleep(retry_wait_seconds)
                                    retry_wait_seconds = min(
                                        retry_wait_seconds * _RETRY_BACKOFF_FACTOR,
                                        _MAX_RETRY_WAIT_SECONDS,
                                    )
                                    continue
                                if in_wf:
                                    serialized_e, serialization = serialize_exception(
                                        e, None, self.serializer
                                    )
                                    self._record_error(
                                        workflow_id,
                                        step_id,
                                        serialized_e,
                                        serialization,
                                    )
                                raise
                            finally:
                                exec_ctx.end_sync_ds_transaction()
            except _StepAlreadyRecorded:
                conflicted = True

            # Outside the except block, so the internal signal stays out of the traceback chain.
            if conflicted:
                if not _still_owns(workflow_id):
                    # Another execution owns the workflow: stop, as an ordinary step's loser does.
                    raise DBOSWorkflowConflictIDError(workflow_id)
                # Still the owner: the recorded row is our own ambiguous commit or a stale execution's, and either is this step's result.
                return cast(R, self._replay_conflicting_step(workflow_id, step_id))

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
