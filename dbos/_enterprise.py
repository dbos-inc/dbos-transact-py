"""The contract between DBOS Transact and the dbos-enterprise package.

Core declares here exactly what it uses from enterprise, and loads the package by
name only when a feature needs it. Enterprise imports these types and checks that
it satisfies them, so each side is verified against the contract on its own.
"""

from __future__ import annotations

import importlib
import json
import os
import urllib.error
import urllib.request
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
from urllib.parse import urlsplit, urlunsplit

from ._error import DBOSInitializationError
from ._logger import dbos_logger
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


ENTITLEMENT_PATH = "/v2/entitlements/enterprise"
ENTITLEMENT_TIMEOUT_SEC = 10.0


# DBOS's hosted Conductor. An outage there must not keep applications from starting.
HOSTED_CONDUCTOR_URL = "wss://cloud.dbos.dev/conductor/v1alpha1"


def default_conductor_url() -> str:
    """The DBOS-hosted Conductor, on the domain DBOS_DOMAIN names."""
    return f"wss://{os.environ.get('DBOS_DOMAIN', 'cloud.dbos.dev')}/conductor/v1alpha1"


def entitlement_url(conductor_url: str) -> str:
    """The entitlement route of the Conductor at conductor_url, over HTTP(S)."""
    parts = urlsplit(conductor_url)
    scheme = {"wss": "https", "ws": "http"}.get(parts.scheme, parts.scheme)
    # The hosted websocket URL carries an API version; routes sit beside it.
    path = parts.path.rstrip("/")
    if path.endswith("/v1alpha1"):
        path = path[: -len("/v1alpha1")]
    return urlunsplit((scheme, parts.netloc, path + ENTITLEMENT_PATH, "", ""))


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Refuse redirects: following one would carry the key to whatever host it names."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def _fetch_entitlement(request: urllib.request.Request) -> None:
    """GET the entitlement route; a refused redirect surfaces as an HTTPError."""
    # Built per call, so proxy settings are read when the check runs, not at import.
    opener = urllib.request.build_opener(_NoRedirects)
    with opener.open(request, timeout=ENTITLEMENT_TIMEOUT_SEC):
        pass


def _error_detail(error: urllib.error.HTTPError) -> str:
    """The reason Conductor gave, from its JSON error body when there is one."""
    try:
        body = json.loads(error.read())
        return str(body.get("detail") or body.get("message") or error.reason)
    except Exception:
        return str(error.reason)


def require_enterprise(
    features: List[str],
    *,
    enterprise_key: Optional[str],
    conductor_key: Optional[str],
    conductor_url: Optional[str],
) -> None:
    """Enterprise features need a DBOS key in good standing, checked with Conductor.
    DBOS Cloud passes without a check."""
    if not features or GlobalParams.dbos_cloud:
        return
    names = " and ".join(features)
    key = enterprise_key or conductor_key
    if key is None:
        raise DBOSInitializationError(
            f"{names} requires a DBOS key. Set transact_enterprise_key in your DBOS "
            "configuration, or pass a conductor_key to DBOS."
        )
    target = conductor_url or default_conductor_url()
    url = entitlement_url(target)
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {key}",
            "User-Agent": f"dbos-transact-py/{GlobalParams.dbos_version}",
        },
    )
    try:
        _fetch_entitlement(request)
        return
    except urllib.error.HTTPError as e:
        if e.code < 500:
            # Rejected: an unknown key, an org not in good standing, no such route, or a redirect.
            raise DBOSInitializationError(
                f"{names} requires a DBOS key in good standing. Conductor at {url} "
                f"rejected the key (HTTP {e.code}): {_error_detail(e)}"
            ) from e
        failure = f"HTTP {e.code}"
        cause: Exception = e
    except Exception as e:
        failure = str(e)
        cause = e
    if target == HOSTED_CONDUCTOR_URL:
        dbos_logger.warning(
            f"Could not verify the DBOS key with Conductor at {url} ({failure}); "
            "starting anyway."
        )
        return
    # Any other Conductor fails closed, so an unreachable URL cannot stand in for a key.
    raise DBOSInitializationError(
        f"{names} requires a DBOS key in good standing, but checking it with Conductor "
        f"at {url} failed ({failure}). Retry once Conductor is reachable."
    ) from cause


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
