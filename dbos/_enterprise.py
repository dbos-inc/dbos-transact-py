"""The contract between DBOS Transact and the dbos-enterprise package.

Core declares here exactly what it uses from enterprise, and loads the package by
name only when a feature needs it. Enterprise imports these types and checks that
it satisfies them, so each side is verified against the contract on its own.
"""

from __future__ import annotations

import importlib
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    cast,
)

from ._error import DBOSInitializationError
from ._utils import GlobalParams

if TYPE_CHECKING:
    from ._dbos import DBOS
    from ._serialization import Serializer
    from ._sys_db import SystemDatabase

# Receives a Conductor alert as (name, message, metadata).
AlertHandler = Callable[[str, str, Dict[str, str]], None]


class ConductorThread(Protocol):
    """The Conductor client, run as a background thread for the life of a DBOS instance."""

    def start(self) -> None: ...

    def stop(self) -> None: ...


class ConductorFactory(Protocol):
    def __call__(
        self,
        dbos: DBOS,
        *,
        app_name: str,
        conductor_url: str,
        conductor_key: str,
        executor_metadata: Optional[Dict[str, Any]],
        metadata_only_mode: bool,
        alert_handler: Optional[AlertHandler],
    ) -> ConductorThread: ...


class ShardedSystemDatabaseFactory(Protocol):
    """Builds the system database a sharded deployment uses, in place of core's Postgres one."""

    def __call__(
        self,
        *,
        system_database_url: str,
        engine_kwargs: Dict[str, Any],
        engine: Optional[Any],
        schema: Optional[str],
        serializer: "Serializer",
        executor_id: Optional[str],
        notification_listener_polling_interval_sec: float,
        polling_concurrency: Optional[int],
        app_name: Optional[str],
        retry_connection_errors: bool,
        observability_query_timeout_sec: Optional[float],
        idle_transaction_timeout_sec: Optional[float],
    ) -> "SystemDatabase": ...


class Enterprise(Protocol):
    """What the top-level dbos_enterprise module must provide."""

    # A property rather than an attribute, so implementations may provide any compatible callable.
    @property
    def ConductorWebsocket(self) -> ConductorFactory: ...

    @property
    def ShardedSystemDatabase(self) -> ShardedSystemDatabaseFactory: ...

    # The statements creating a sharded system database in a schema, for printing.
    @property
    def sharded_schema_statements(self) -> Callable[[str], List[str]]: ...


CONDUCTOR = "Connecting to DBOS Conductor"
SHARDING = "Using a sharded system database"


# Config keys that turn on an enterprise feature, each with the name its errors use.
CONFIG_FEATURES: Dict[str, str] = {"sharding": SHARDING}


def enabled_features(config: Mapping[str, object]) -> List[str]:
    """The enterprise features a config turns on."""
    return [feature for key, feature in CONFIG_FEATURES.items() if config.get(key)]


def require_conductor(feature: str, conductor_key: Optional[str]) -> None:
    """Enterprise features are for executors connected to DBOS Conductor; DBOS Cloud always connects."""
    if GlobalParams.dbos_cloud or conductor_key is not None:
        return
    raise DBOSInitializationError(
        f"{feature} requires a connection to DBOS Conductor. Pass a conductor_key to DBOS."
    )


def load(feature: str = CONDUCTOR) -> Enterprise:
    """Import dbos_enterprise, telling an absent package apart from one that fails to import."""
    try:
        return cast(Enterprise, importlib.import_module("dbos_enterprise"))
    except ImportError as e:
        if isinstance(e, ModuleNotFoundError) and e.name == "dbos_enterprise":
            raise DBOSInitializationError(
                f'{feature} requires the dbos-enterprise package. Install it with `pip install "dbos[enterprise]"`.'
            ) from e
        # The package is present; it imports dbos internals, so this is almost always a version mismatch.
        raise DBOSInitializationError(
            f"The dbos-enterprise package is installed but could not be imported by dbos {GlobalParams.dbos_version}. "
            f"The two must be the same version; upgrade both together. Cause: {e}"
        ) from e
