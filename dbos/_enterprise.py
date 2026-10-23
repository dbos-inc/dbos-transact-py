"""The contract between DBOS Transact and the dbos-enterprise package.

Core declares here exactly what it uses from enterprise, and loads the package by
name only when a feature needs it. Enterprise imports these types and checks that
it satisfies them, so each side is verified against the contract on its own.

Transact Enterprise (the dbos-enterprise package) is proprietary, not MIT-licensed like
this project. Its license (https://www.dbos.dev/conductor-license) forbids modifying it, and
makes disabling or circumventing the entitlement checks here a material breach.
"""

from __future__ import annotations

import importlib
import json
import os
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
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


class AutoUpgradePass(Protocol):
    """One tick of auto-upgrade"""

    def run(self, stop_event: threading.Event) -> int: ...

    def close(self) -> None: ...


class AutoUpgradePassFactory(Protocol):
    def __call__(self, dbos: DBOS, *, grace_sec: float) -> AutoUpgradePass: ...


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

    # Properties rather than attributes, so implementations may provide any compatible callable.
    @property
    def ConductorWebsocket(self) -> ConductorFactory: ...

    @property
    def AutoUpgradePass(self) -> AutoUpgradePassFactory: ...

    @property
    def ShardedSystemDatabase(self) -> ShardedSystemDatabaseFactory: ...


# What needs the package without being an entitlement, for load()'s messages.
CONDUCTOR = "Connecting to DBOS Conductor"


@dataclass(frozen=True)
class Feature:
    """A Transact Enterprise feature: the entitlement Conductor reports it under, how
    messages name it, and the DBOSConfig key that turns it on, if one does."""

    entitlement: str
    description: str
    config_key: Optional[str] = None


AUTO_UPGRADE = Feature(
    "autoUpgrade", "Auto-upgrading workflows", config_key="auto_upgrade"
)
SHARDING = Feature("sharding", "Using a sharded system database", config_key="sharding")

# Every entitled feature; a new one is a Feature here, named by Conductor's camelCase key.
FEATURES: Tuple[Feature, ...] = (AUTO_UPGRADE, SHARDING)


def enabled_features(config: Mapping[str, object]) -> List[Feature]:
    """The enterprise features a config turns on."""
    return [
        f for f in FEATURES if f.config_key is not None and config.get(f.config_key)
    ]


ENTITLEMENTS_PATH = "/v2/entitlements"
ENTITLEMENT_TIMEOUT_SEC = 10.0
# Conductor's machine-readable error code, in this header, for an org past its plan limit.
ERROR_CODE_HEADER = "X-DBOS-Error"
PAST_LIMIT = "past_limit"
HOSTED_UNAVAILABLE = frozenset({403, 407, 408, 425, 429})
LICENSE_URL = "https://www.dbos.dev/conductor-license"
LICENSE_NOTICE = (
    f"Transact Enterprise is licensed under the DBOS Conductor Software License ({LICENSE_URL}), "
    "which makes disabling or circumventing this check a material breach."
)


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
    # Without any user:password@, which would otherwise be sent and quoted in errors.
    host = parts.netloc.rpartition("@")[2]
    return urlunsplit((scheme, host, path + ENTITLEMENTS_PATH, "", ""))


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Refuse redirects: following one would carry the key to whatever host it names."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def _fetch_entitlements(request: urllib.request.Request) -> Dict[str, bool]:
    """GET the key's org's entitlements, by entitlement name; a refused redirect surfaces
    as an HTTPError, and a malformed body as a ValueError."""
    # Built per call, so proxy settings are read when the check runs, not at import.
    opener = urllib.request.build_opener(_NoRedirects)
    with opener.open(request, timeout=ENTITLEMENT_TIMEOUT_SEC) as response:
        body = json.loads(response.read())
    entitlements = body.get("entitlements") if isinstance(body, dict) else None
    if not isinstance(entitlements, dict):
        raise ValueError("the response has no entitlements")
    return {name: value is True for name, value in entitlements.items()}


def _problem_detail(error: urllib.error.HTTPError) -> Optional[str]:
    """The reason Conductor gave in its JSON error body, or None if it sent none."""
    try:
        body = json.loads(error.read())
    except Exception:
        return None
    detail = (
        body.get("detail") or body.get("message") if isinstance(body, dict) else None
    )
    return str(detail) if detail else None


def _refusal(message: str) -> DBOSInitializationError:
    return DBOSInitializationError(f"{message} {LICENSE_NOTICE}")


# A license control: the Transact Enterprise license makes disabling or circumventing it a material breach.
def require_enterprise(
    features: Sequence[Feature],
    *,
    enterprise_key: Optional[str],
    conductor_key: Optional[str],
    conductor_url: Optional[str],
) -> None:
    """Enterprise features need Conductor to report the org entitled to each of them. A
    self-hosted Conductor may answer without a key; DBOS Cloud passes without a check.
    """
    if not features or GlobalParams.dbos_cloud:
        return
    names = " and ".join(f.description for f in features)
    entitlements = " and ".join(f.entitlement for f in features)
    key = enterprise_key or conductor_key
    target = conductor_url or default_conductor_url()
    key_hint = (
        "Set transact_enterprise_key in your DBOS configuration, or pass a "
        "conductor_key to DBOS."
    )
    # Only a Conductor named explicitly is asked without a key, and never DBOS's hosted one.
    if key is None and (not conductor_url or target == HOSTED_CONDUCTOR_URL):
        raise DBOSInitializationError(f"{names} requires a DBOS key. {key_hint}")
    url = entitlement_url(target)
    headers = {"User-Agent": f"dbos-transact-py/{GlobalParams.dbos_version}"}
    if key is not None:
        headers["Authorization"] = f"Bearer {key}"
    try:
        granted = _fetch_entitlements(urllib.request.Request(url, headers=headers))
    except urllib.error.HTTPError as e:
        detail = _problem_detail(e)
        if e.code == 403 and e.headers.get(ERROR_CODE_HEADER) == PAST_LIMIT:
            raise _refusal(
                f"Your DBOS organization is past its plan limit, so it is not entitled "
                f"to {entitlements} ({names}). Upgrade its plan to continue."
            ) from e
        reason = detail or str(e.reason)
        if target == HOSTED_CONDUCTOR_URL and e.code in HOSTED_UNAVAILABLE:
            pass  # An outage, handled below.
        elif e.code == 404 and detail is None:
            # A bare 404 is a missing route: a Conductor predating per-feature entitlements.
            raise _refusal(
                f"{names} requires an entitled DBOS key, but Conductor at {url} has no "
                "entitlements route (HTTP 404). Upgrade Conductor, or check its URL."
            ) from e
        elif e.code == 401 and key is None:
            raise DBOSInitializationError(
                f"{names} requires a DBOS key: Conductor at {url} refused to check "
                f"without one (HTTP 401): {reason}. {key_hint}"
            ) from e
        elif e.code == 401:
            # An unknown key, or a user credential instead of an API key.
            raise _refusal(
                f"{names} requires a valid DBOS key. Conductor at {url} rejected the "
                f"key (HTTP 401): {reason}"
            ) from e
        elif e.code < 500:
            # Any other refusal, such as the key's organization being gone, or a redirect.
            raise _refusal(
                f"{names} requires an entitled DBOS key, but Conductor at {url} refused "
                f"the check (HTTP {e.code}): {reason}"
            ) from e
        failure = f"HTTP {e.code}: {detail}" if detail else f"HTTP {e.code}"
        cause: Exception = e
    except Exception as e:
        failure = str(e)
        cause = e
    else:
        missing = [f for f in features if not granted.get(f.entitlement, False)]
        if missing:
            raise _refusal(
                f"Your DBOS organization is not entitled to "
                f"{' and '.join(f.entitlement for f in missing)} "
                f"({' and '.join(f.description for f in missing)}): its plan does not "
                "include it. Contact DBOS to enable it."
            )
        return
    if target == HOSTED_CONDUCTOR_URL:
        dbos_logger.warning(
            f"Could not verify the DBOS key's entitlements with Conductor at {url} "
            f"({failure}); starting anyway."
        )
        return
    # Any other Conductor fails closed, so an unreachable URL cannot stand in for a key.
    raise _refusal(
        f"{names} requires an entitled DBOS key, but checking it with Conductor at {url} "
        f"failed ({failure}). Retry once Conductor is reachable."
    ) from cause


def load(feature: str = CONDUCTOR) -> Enterprise:
    """Import dbos_enterprise, telling an absent package apart from one that fails to import."""
    # dbos_enterprise is proprietary: its license forbids modifying it or copying it beyond authorized use.
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
