from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Dict, Optional

from ._error import DBOSInitializationError

ENTITLEMENT_TIMEOUT_SEC = 10.0

_WS_TO_HTTP = {"ws://": "http://", "wss://": "https://"}

_DBOS_ERROR_HEADER = "X-DBOS-Error"
_PAST_LIMIT = "past_limit"


def enterprise_entitlements_url(conductor_url: Optional[str] = None) -> str:
    """Where to ask about the entitlements.

    A self-hosted Conductor is configured by its root URL (the websocket lives under
    /websocket, the v2 API under /v2).

    Fall back to DBOS-managed Conductor URL.
    """
    if conductor_url:
        root = conductor_url.rstrip("/")
        for ws_scheme, http_scheme in _WS_TO_HTTP.items():
            if root.startswith(ws_scheme):
                root = http_scheme + root[len(ws_scheme) :]
                break
        return f"{root}/v2/entitlements"
    dbos_domain = os.environ.get("DBOS_DOMAIN", "cloud.dbos.dev")
    return f"https://{dbos_domain}/conductor/v2/entitlements"


def conductor_entitlement_name(feature: str) -> str:
    head, *rest = feature.split("_")
    return head + "".join(part.capitalize() for part in rest)


def check_enterprise_entitlement(
    key: str, *, feature: str, conductor_url: Optional[str] = None
) -> None:
    """Raise DBOSInitializationError unless `key` is entitled to `feature`."""
    url = enterprise_entitlements_url(conductor_url)
    entitlements = get_enterprise_entitlements(key, url=url, feature=feature)
    if not entitlements.get(conductor_entitlement_name(feature), False):
        raise DBOSInitializationError(
            f"{feature} requires a Transact Enterprise key, but this key's organization's plan does not include it. "
            "Contact DBOS to add it to the plan."
        )


def get_enterprise_entitlements(key: str, *, url: str, feature: str) -> Dict[str, bool]:
    """The features `key`'s organization may use, by name, as Conductor reports them.

    Raises DBOSInitializationError when Conductor rejects the key, reports the
    organization past its plan limit, or cannot answer.
    """
    request = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {key}", "Accept": "application/json"}
    )
    try:
        with urllib.request.urlopen(
            request, timeout=ENTITLEMENT_TIMEOUT_SEC
        ) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        detail = _error_detail(e)
        if e.code == 401:
            raise DBOSInitializationError(
                f"{feature} requires a Transact Enterprise key, but DBOS rejected the key given: {detail}"
            ) from e
        if e.code == 403 and e.headers.get(_DBOS_ERROR_HEADER) == _PAST_LIMIT:
            raise DBOSInitializationError(
                f"{feature} requires a Transact Enterprise key, but this key's organization is past its plan limit "
                f"and may use no Enterprise feature until its plan is upgraded: {detail}"
            ) from e
        raise DBOSInitializationError(
            f"Could not verify the Transact Enterprise entitlement for {feature}: {url} answered HTTP {e.code}: {detail}"
        ) from e
    except OSError as e:
        # urllib.error.URLError, socket timeouts, connection refused, TLS failures.
        raise DBOSInitializationError(
            f"Could not verify the Transact Enterprise entitlement for {feature}: could not reach {url}: {e}"
        ) from e
    try:
        parsed = json.loads(body)
        entitlements = parsed["entitlements"]
    except (ValueError, KeyError, TypeError) as e:
        raise DBOSInitializationError(
            f"Could not verify the Transact Enterprise entitlement for {feature}: {url} answered with an unexpected body: {body.strip()[:200]}"
        ) from e
    if not isinstance(entitlements, dict):
        raise DBOSInitializationError(
            f"Could not verify the Transact Enterprise entitlement for {feature}: {url} answered with an unexpected body: {body.strip()[:200]}"
        )
    return {name: bool(value) for name, value in entitlements.items()}


def _error_detail(e: urllib.error.HTTPError) -> str:
    try:
        body = e.read().decode("utf-8", errors="replace")
    except Exception:
        return e.reason if isinstance(e.reason, str) else str(e.reason)
    try:
        parsed = json.loads(body)
    except ValueError:
        return body.strip() or str(e.reason)
    if isinstance(parsed, dict):
        # Huma problem+json ("detail") or a plain {"error": ...} / {"message": ...} body.
        for field in ("detail", "error", "message", "title"):
            value = parsed.get(field)
            if isinstance(value, str) and value:
                return value
    return body.strip() or str(e.reason)
