"""The Transact Enterprise entitlement call, against a local HTTP stand-in for Conductor."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Generator, List, Optional, Tuple

import pytest

from dbos import _entitlements
from dbos._entitlements import (
    check_enterprise_entitlement,
    conductor_entitlement_name,
)
from dbos._error import DBOSInitializationError

# Conductor spells the entitlements in camelCase.
ALL_ON = {"sharding": True, "autoUpgrade": True, "suspendWorkflows": True}


class _ConductorStandIn:
    """Answers GET /conductor/v2/entitlements the way DBOS (Conductor behind Cloud) does."""

    def __init__(self) -> None:
        self.status = 200
        self.headers: Dict[str, str] = {}
        self.body: Any = {"entitlements": ALL_ON}
        self.requests: List[Tuple[str, Optional[str]]] = []
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                stand_in.requests.append((self.path, self.headers.get("Authorization")))
                raw = stand_in.body
                payload = (
                    raw.encode() if isinstance(raw, str) else json.dumps(raw).encode()
                )
                self.send_response(stand_in.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                for name, value in stand_in.headers.items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True, name="conductor-stand-in"
        )

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}/conductor/v2/entitlements"

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


@pytest.fixture()
def conductor_stand_in(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[_ConductorStandIn, Any, None]:
    stand_in = _ConductorStandIn()
    stand_in.start()
    monkeypatch.setattr(
        _entitlements,
        "enterprise_entitlements_url",
        lambda conductor_url=None: stand_in.url,
    )
    yield stand_in
    stand_in.stop()


def test_url_follows_the_dbos_domain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DBOS_DOMAIN", raising=False)
    assert (
        _entitlements.enterprise_entitlements_url()
        == "https://cloud.dbos.dev/conductor/v2/entitlements"
    )
    monkeypatch.setenv("DBOS_DOMAIN", "staging.dbos.dev")
    assert (
        _entitlements.enterprise_entitlements_url()
        == "https://staging.dbos.dev/conductor/v2/entitlements"
    )


def test_url_follows_a_self_hosted_conductor(monkeypatch: pytest.MonkeyPatch) -> None:
    # A self-hosted Conductor is configured by its root URL, and DBOS_DOMAIN plays no part.
    monkeypatch.setenv("DBOS_DOMAIN", "staging.dbos.dev")
    assert (
        _entitlements.enterprise_entitlements_url("ws://localhost:8090/")
        == "http://localhost:8090/v2/entitlements"
    )
    assert (
        _entitlements.enterprise_entitlements_url(
            "wss://dbos.example.com/conductor-api"
        )
        == "https://dbos.example.com/conductor-api/v2/entitlements"
    )
    assert (
        _entitlements.enterprise_entitlements_url("http://conductor:8090")
        == "http://conductor:8090/v2/entitlements"
    )


def test_features_are_looked_up_under_their_conductor_name() -> None:
    assert conductor_entitlement_name("auto_upgrade") == "autoUpgrade"
    assert conductor_entitlement_name("suspend_workflows") == "suspendWorkflows"
    assert conductor_entitlement_name("sharding") == "sharding"


def test_entitled_key_passes(conductor_stand_in: _ConductorStandIn) -> None:
    check_enterprise_entitlement("dbos_test_key", feature="auto_upgrade")
    # The key travels as a bearer token, which is what both DBOS and Conductor read.
    assert conductor_stand_in.requests == [
        ("/conductor/v2/entitlements", "Bearer dbos_test_key")
    ]


def test_only_the_feature_asked_for_matters(
    conductor_stand_in: _ConductorStandIn,
) -> None:
    conductor_stand_in.body = {"entitlements": {"sharding": False, "autoUpgrade": True}}
    check_enterprise_entitlement("dbos_test_key", feature="auto_upgrade")
    with pytest.raises(DBOSInitializationError) as exc:
        check_enterprise_entitlement("dbos_test_key", feature="sharding")
    assert "sharding requires a Transact Enterprise key" in str(exc.value)
    assert "plan does not include it" in str(exc.value)


def test_feature_switched_off_for_the_org(
    conductor_stand_in: _ConductorStandIn,
) -> None:
    conductor_stand_in.body = {"entitlements": {**ALL_ON, "autoUpgrade": False}}
    with pytest.raises(DBOSInitializationError) as exc:
        check_enterprise_entitlement("dbos_test_key", feature="auto_upgrade")
    message = str(exc.value)
    # The message names the feature as the configuration spells it, not as Conductor does.
    assert "auto_upgrade requires a Transact Enterprise key" in message
    assert "plan does not include it" in message


def test_org_past_its_limit(conductor_stand_in: _ConductorStandIn) -> None:
    # A past-limit org gets a 403 tagged past_limit: the fix is a paid plan, not a
    # plan with the feature, and the message says so.
    conductor_stand_in.status = 403
    conductor_stand_in.headers = {"X-DBOS-Error": "past_limit"}
    conductor_stand_in.body = {
        "detail": "your DBOS account is past the free trial limit, please upgrade to Pro to continue"
    }
    with pytest.raises(DBOSInitializationError) as exc:
        check_enterprise_entitlement("dbos_test_key", feature="auto_upgrade")
    message = str(exc.value)
    assert "auto_upgrade requires a Transact Enterprise key" in message
    assert "past its plan limit" in message
    assert "please upgrade to Pro" in message


def test_any_other_forbidden_is_not_a_verdict(
    conductor_stand_in: _ConductorStandIn,
) -> None:
    # A 403 without the past_limit tag is not Conductor's answer about the plan.
    conductor_stand_in.status = 403
    conductor_stand_in.body = {"detail": "forbidden"}
    with pytest.raises(DBOSInitializationError) as exc:
        check_enterprise_entitlement("dbos_test_key", feature="auto_upgrade")
    message = str(exc.value)
    assert "Could not verify the Transact Enterprise entitlement" in message
    assert "HTTP 403" in message
    assert "past its plan limit" not in message


def test_unknown_feature_is_not_entitled(
    conductor_stand_in: _ConductorStandIn,
) -> None:
    # A Conductor too old to know the feature cannot have entitled it.
    with pytest.raises(DBOSInitializationError) as exc:
        check_enterprise_entitlement("dbos_test_key", feature="time_travel")
    assert "time_travel requires a Transact Enterprise key" in str(exc.value)


def test_rejected_key(conductor_stand_in: _ConductorStandIn) -> None:
    conductor_stand_in.status = 401
    conductor_stand_in.body = {"detail": "this endpoint requires a DBOS API key"}
    with pytest.raises(DBOSInitializationError) as exc:
        check_enterprise_entitlement("not-a-key", feature="auto_upgrade")
    message = str(exc.value)
    assert "auto_upgrade requires a Transact Enterprise key" in message
    assert "rejected the key" in message
    assert "this endpoint requires a DBOS API key" in message


def test_other_server_errors(conductor_stand_in: _ConductorStandIn) -> None:
    conductor_stand_in.status = 503
    conductor_stand_in.body = "conductor not found"
    with pytest.raises(DBOSInitializationError) as exc:
        check_enterprise_entitlement("dbos_test_key", feature="auto_upgrade")
    message = str(exc.value)
    assert "Could not verify the Transact Enterprise entitlement" in message
    assert "HTTP 503" in message
    assert "conductor not found" in message


def test_unexpected_body(conductor_stand_in: _ConductorStandIn) -> None:
    for body in ("not json", {"entitled": True}, {"entitlements": "yes"}):
        conductor_stand_in.body = body
        with pytest.raises(DBOSInitializationError) as exc:
            check_enterprise_entitlement("dbos_test_key", feature="auto_upgrade")
        assert "unexpected body" in str(exc.value), body


def test_unreachable_conductor(
    conductor_stand_in: _ConductorStandIn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conductor_stand_in.stop()
    with pytest.raises(DBOSInitializationError) as exc:
        check_enterprise_entitlement("dbos_test_key", feature="auto_upgrade")
    message = str(exc.value)
    assert "Could not verify the Transact Enterprise entitlement" in message
    assert "could not reach" in message
