"""Transact Enterprise entitlements.

Enterprise features are gated on a key (a Conductor API key) belonging to an
organization in good standing. The check is one HTTP call to the Conductor the
application talks to, made at launch, before the feature starts.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Optional

from ._error import DBOSInitializationError

ENTITLEMENT_TIMEOUT_SEC = 10.0

_WS_TO_HTTP = {"ws://": "http://", "wss://": "https://"}


def enterprise_entitlements_url(conductor_url: Optional[str] = None) -> str:
    """Where to ask about the entitlement.

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
        return f"{root}/v2/entitlements/enterprise"
    dbos_domain = os.environ.get("DBOS_DOMAIN", "cloud.dbos.dev")
    return f"https://{dbos_domain}/conductor/v2/entitlements/enterprise"


def check_enterprise_entitlement(
    key: str, *, feature: str, conductor_url: Optional[str] = None
) -> None:
    """Raise DBOSInitializationError unless `key` is entitled to Transact Enterprise.

    `feature` names what is being gated, for the error message. `conductor_url` is
    the self-hosted Conductor to ask, if any; see enterprise_entitlements_url.
    """
    url = enterprise_entitlements_url(conductor_url)
    request = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {key}", "Accept": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=ENTITLEMENT_TIMEOUT_SEC):
            return
    except urllib.error.HTTPError as e:
        code = e.headers.get("X-DBOS-Error") if e.headers is not None else None
        detail = _error_detail(e)
        if e.code == 401:
            raise DBOSInitializationError(
                f"{feature} requires a Transact Enterprise key, but DBOS rejected the key given: {detail}"
            ) from e
        if e.code == 403:
            raise DBOSInitializationError(
                f"{feature} requires a Transact Enterprise key, but this key's organization is not entitled"
                f"{f' ({code})' if code else ''}: {detail}"
            ) from e
        raise DBOSInitializationError(
            f"Could not verify the Transact Enterprise entitlement for {feature}: {url} answered HTTP {e.code}: {detail}"
        ) from e
    except OSError as e:
        # urllib.error.URLError, socket timeouts, connection refused, TLS failures.
        raise DBOSInitializationError(
            f"Could not verify the Transact Enterprise entitlement for {feature}: could not reach {url}: {e}"
        ) from e


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
