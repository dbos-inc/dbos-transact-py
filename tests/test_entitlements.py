"""The Transact Enterprise entitlement call, against a local HTTP stand-in for DBOS."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Generator, List, Optional, Tuple

import pytest

from dbos import _entitlements
from dbos._entitlements import check_enterprise_entitlement
from dbos._error import DBOSInitializationError


class _DBOSStandIn:
    """Answers GET /conductor/v2/entitlements/enterprise the way DBOS does."""

    def __init__(self) -> None:
        self.status = 200
        self.headers: Dict[str, str] = {}
        self.body: Any = {"entitled": True}
        self.requests: List[Tuple[str, Optional[str]]] = []
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                stand_in.requests.append((self.path, self.headers.get("Authorization")))
                payload = json.dumps(stand_in.body).encode()
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
            target=self.server.serve_forever, daemon=True, name="dbos-stand-in"
        )

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}/conductor/v2/entitlements/enterprise"

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


@pytest.fixture()
def dbos_stand_in(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[_DBOSStandIn, Any, None]:
    stand_in = _DBOSStandIn()
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
        == "https://cloud.dbos.dev/conductor/v2/entitlements/enterprise"
    )
    monkeypatch.setenv("DBOS_DOMAIN", "staging.dbos.dev")
    assert (
        _entitlements.enterprise_entitlements_url()
        == "https://staging.dbos.dev/conductor/v2/entitlements/enterprise"
    )


def test_url_follows_a_self_hosted_conductor(monkeypatch: pytest.MonkeyPatch) -> None:
    # A self-hosted Conductor is configured by its root URL, and DBOS_DOMAIN plays no part.
    monkeypatch.setenv("DBOS_DOMAIN", "staging.dbos.dev")
    assert (
        _entitlements.enterprise_entitlements_url("ws://localhost:8090/")
        == "http://localhost:8090/v2/entitlements/enterprise"
    )
    assert (
        _entitlements.enterprise_entitlements_url(
            "wss://dbos.example.com/conductor-api"
        )
        == "https://dbos.example.com/conductor-api/v2/entitlements/enterprise"
    )
    assert (
        _entitlements.enterprise_entitlements_url("http://conductor:8090")
        == "http://conductor:8090/v2/entitlements/enterprise"
    )


def test_entitled_key_passes(dbos_stand_in: _DBOSStandIn) -> None:
    check_enterprise_entitlement("dbos_test_key", feature="auto_upgrade")
    # The key travels as a bearer token, which is what both DBOS and Conductor read.
    assert dbos_stand_in.requests == [
        ("/conductor/v2/entitlements/enterprise", "Bearer dbos_test_key")
    ]


def test_rejected_key(dbos_stand_in: _DBOSStandIn) -> None:
    dbos_stand_in.status = 401
    dbos_stand_in.body = {"detail": "this endpoint requires a DBOS API key"}
    with pytest.raises(DBOSInitializationError) as exc:
        check_enterprise_entitlement("not-a-key", feature="auto_upgrade")
    message = str(exc.value)
    assert "auto_upgrade requires a Transact Enterprise key" in message
    assert "rejected the key" in message
    assert "this endpoint requires a DBOS API key" in message


def test_org_past_its_limit(dbos_stand_in: _DBOSStandIn) -> None:
    dbos_stand_in.status = 403
    dbos_stand_in.headers = {"X-DBOS-Error": "past_limit"}
    dbos_stand_in.body = {
        "title": "Forbidden",
        "detail": "your DBOS account is past the free trial limit",
    }
    with pytest.raises(DBOSInitializationError) as exc:
        check_enterprise_entitlement("dbos_test_key", feature="auto_upgrade")
    message = str(exc.value)
    assert "not entitled (past_limit)" in message
    assert "past the free trial limit" in message


def test_other_server_errors(dbos_stand_in: _DBOSStandIn) -> None:
    dbos_stand_in.status = 503
    dbos_stand_in.body = "conductor not found"
    with pytest.raises(DBOSInitializationError) as exc:
        check_enterprise_entitlement("dbos_test_key", feature="auto_upgrade")
    message = str(exc.value)
    assert "Could not verify the Transact Enterprise entitlement" in message
    assert "HTTP 503" in message
    assert "conductor not found" in message


def test_unreachable_dbos(
    dbos_stand_in: _DBOSStandIn, monkeypatch: pytest.MonkeyPatch
) -> None:
    dbos_stand_in.stop()
    with pytest.raises(DBOSInitializationError) as exc:
        check_enterprise_entitlement("dbos_test_key", feature="auto_upgrade")
    message = str(exc.value)
    assert "Could not verify the Transact Enterprise entitlement" in message
    assert "could not reach" in message
