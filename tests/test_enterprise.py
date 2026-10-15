"""The seam to dbos-enterprise: what core reports when the package is absent or
broken, and that a refused construction leaves nothing behind."""

import json
import socket
import threading
import types
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Generator, List, Optional, Tuple

import pytest

from dbos import DBOS, DBOSConfig
from dbos import _enterprise as enterprise_module
from dbos._error import DBOSInitializationError
from dbos._logger import dbos_logger
from dbos._utils import GlobalParams

# Captured at import, before the sharded test harness replaces it.
_require_enterprise = enterprise_module.require_enterprise


def _import_raises(monkeypatch: pytest.MonkeyPatch, error: ImportError) -> None:
    def import_module(name: str) -> Any:
        raise error

    monkeypatch.setattr(
        enterprise_module,
        "importlib",
        types.SimpleNamespace(import_module=import_module),
    )


def test_absent_package_gets_the_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _import_raises(
        monkeypatch,
        ModuleNotFoundError(
            "No module named 'dbos_enterprise'", name="dbos_enterprise"
        ),
    )
    with pytest.raises(
        DBOSInitializationError, match=r"pip install \"dbos\[enterprise\]\""
    ):
        enterprise_module.load()


@pytest.mark.parametrize(
    "error",
    [
        ImportError("cannot import name 'get_workflow' from 'dbos._workflow_commands'"),
        ModuleNotFoundError("No module named 'websockets'", name="websockets"),
    ],
)
def test_broken_package_is_not_reported_as_absent(
    monkeypatch: pytest.MonkeyPatch, error: ImportError
) -> None:
    _import_raises(monkeypatch, error)
    with pytest.raises(DBOSInitializationError) as raised:
        enterprise_module.load()
    message = str(raised.value)
    assert "is installed but could not be imported" in message
    assert str(error) in message
    assert "pip install" not in message


def _load_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    def load(*args: Any) -> Any:
        raise DBOSInitializationError("refused")

    monkeypatch.setattr(enterprise_module, "load", load)


def test_refused_construction_leaves_nothing_behind(
    config: DBOSConfig,
    cleanup_test_databases: None,
    monkeypatch: pytest.MonkeyPatch,
    skip_with_sharding: None,
) -> None:
    _load_refuses(monkeypatch)
    with pytest.raises(DBOSInitializationError, match="refused"):
        DBOS(config=config, conductor_key="test-key")

    # Neither teardown nor a fresh instance trips over the refused one.
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.launch()
    assert DBOS.executor_id == "local"
    DBOS.destroy(destroy_registry=True)


def test_cloud_always_requires_the_package(
    config: DBOSConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    _load_refuses(monkeypatch)
    monkeypatch.setattr(GlobalParams, "dbos_cloud", True)
    DBOS.destroy(destroy_registry=True)

    # DBOS Cloud always connects to Conductor, whether or not a key is passed in code.
    with pytest.raises(DBOSInitializationError, match="refused"):
        DBOS(config=config)
    DBOS.destroy(destroy_registry=True)


# A feature for exercising the check itself, entitled under a name Conductor may not know.
SOME_FEATURE = enterprise_module.Feature("some_feature", "Some feature")
ALL_ENTITLED = {"sharding": True, "auto_upgrade": True, "suspend_workflows": True}


class FakeConductor:
    """A local stand-in for Conductor's entitlements route."""

    def __init__(self) -> None:
        self.status = 200
        self.body: Any = {"entitlements": {**ALL_ENTITLED, "some_feature": True}}
        self.headers: Dict[str, str] = {}
        self.requests: List[Tuple[str, Optional[str]]] = []
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                fake.requests.append((self.path, self.headers.get("Authorization")))
                payload = json.dumps(fake.body).encode()
                self.send_response(fake.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                for name, value in fake.headers.items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"ws://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def reject(self, status: int, detail: str) -> None:
        self.status = status
        self.body = {"title": "Rejected", "status": status, "detail": detail}

    def entitle(self, **entitlements: bool) -> None:
        self.status = 200
        self.body = {"entitlements": entitlements}


@pytest.fixture()
def conductor(monkeypatch: pytest.MonkeyPatch) -> Generator[FakeConductor, Any, None]:
    monkeypatch.setattr(GlobalParams, "dbos_cloud", False)
    fake = FakeConductor()
    yield fake
    fake.server.shutdown()
    fake.server.server_close()


def check(
    conductor: FakeConductor,
    *,
    enterprise_key: Optional[str] = None,
    conductor_key: Optional[str] = None,
    features: Optional[List[enterprise_module.Feature]] = None,
) -> None:
    _require_enterprise(
        features if features is not None else [SOME_FEATURE],
        enterprise_key=enterprise_key,
        conductor_key=conductor_key,
        conductor_url=conductor.url,
    )


def test_entitlement_url_sits_beside_the_conductor_url() -> None:
    url = enterprise_module.entitlement_url
    assert (
        url("wss://cloud.dbos.dev/conductor/v1alpha1")
        == "https://cloud.dbos.dev/conductor/v2/entitlements"
    )
    assert (
        url("wss://cloud.dbos.dev/conductor/v1alpha1/")
        == "https://cloud.dbos.dev/conductor/v2/entitlements"
    )
    # Credentials in the URL are dropped, so they are never sent or quoted in errors.
    assert url("wss://user:secret@conductor.example.com:8443/conductor/v1alpha1") == (
        "https://conductor.example.com:8443/conductor/v2/entitlements"
    )
    assert url("ws://user:secret@[::1]:8090") == "http://[::1]:8090/v2/entitlements"
    # A self-hosted Conductor serves its routes from the root.
    assert url("ws://localhost:8090") == "http://localhost:8090/v2/entitlements"
    assert url("https://conductor.example.com/") == (
        "https://conductor.example.com/v2/entitlements"
    )


def test_an_entitled_key_unlocks_enterprise_features(
    conductor: FakeConductor,
) -> None:
    check(conductor, enterprise_key="enterprise-key")
    check(conductor, conductor_key="conductor-key")
    # The enterprise key is checked when both are set.
    check(conductor, enterprise_key="enterprise-key", conductor_key="conductor-key")
    assert conductor.requests == [
        ("/v2/entitlements", "Bearer enterprise-key"),
        ("/v2/entitlements", "Bearer conductor-key"),
        ("/v2/entitlements", "Bearer enterprise-key"),
    ]


def test_each_feature_needs_its_own_entitlement(conductor: FakeConductor) -> None:
    conductor.entitle(sharding=False, auto_upgrade=True, some_feature=True)
    check(conductor, enterprise_key="enterprise-key")
    with pytest.raises(DBOSInitializationError) as raised:
        check(
            conductor,
            enterprise_key="enterprise-key",
            features=[SOME_FEATURE, enterprise_module.SHARDING],
        )
    message = str(raised.value)
    # Names only what the org lacks, by entitlement and by what it does.
    assert "not entitled to sharding (Using a sharded system database)" in message
    assert "some_feature" not in message and "enterprise-key" not in message
    # One request answers for every feature.
    assert len(conductor.requests) == 2


def test_a_past_limit_org_is_entitled_to_nothing(conductor: FakeConductor) -> None:
    # Conductor reports every entitlement off while the org is past its plan limit.
    conductor.entitle(**{name: False for name in ALL_ENTITLED}, some_feature=False)
    with pytest.raises(DBOSInitializationError, match="past its plan limit") as raised:
        check(conductor, enterprise_key="enterprise-key")
    assert "not entitled to some_feature" in str(raised.value)


def test_an_entitlement_conductor_does_not_report_is_not_granted(
    conductor: FakeConductor,
) -> None:
    # A Conductor older than the feature doesn't list it, which grants nothing.
    conductor.entitle(**ALL_ENTITLED)
    with pytest.raises(DBOSInitializationError, match="not entitled to some_feature"):
        check(conductor, enterprise_key="enterprise-key")
    # Anything but true grants nothing either.
    conductor.entitle(some_feature="yes")  # type: ignore[arg-type]
    with pytest.raises(DBOSInitializationError, match="not entitled to some_feature"):
        check(conductor, enterprise_key="enterprise-key")


def test_enterprise_features_require_a_key(conductor: FakeConductor) -> None:
    with pytest.raises(
        DBOSInitializationError,
        match=": Some feature requires a DBOS key. Set transact_enterprise_key",
    ):
        check(conductor)
    assert conductor.requests == []


def test_conductor_credentials_never_reach_errors(conductor: FakeConductor) -> None:
    conductor.reject(401, "invalid API key")
    with_credentials = conductor.url.replace("ws://", "ws://user:secret@")
    with pytest.raises(DBOSInitializationError) as raised:
        _require_enterprise(
            [SOME_FEATURE],
            enterprise_key="enterprise-key",
            conductor_key=None,
            conductor_url=with_credentials,
        )
    assert "rejected the key (HTTP 401)" in str(raised.value)
    assert "secret" not in str(raised.value) and "user" not in str(raised.value)


@pytest.mark.parametrize(
    "status, detail",
    [
        (401, "invalid API key"),
        # A user credential, which the route refuses in favor of an API key.
        (401, "this endpoint requires a DBOS API key"),
        (403, "forbidden"),
    ],
)
def test_a_rejected_key_is_refused(
    conductor: FakeConductor, status: int, detail: str
) -> None:
    conductor.reject(status, detail)
    with pytest.raises(DBOSInitializationError) as raised:
        check(conductor, enterprise_key="enterprise-key")
    message = str(raised.value)
    assert f"rejected the key (HTTP {status}): {detail}" in message
    assert "enterprise-key" not in message


def test_a_conductor_without_the_route_is_refused(conductor: FakeConductor) -> None:
    # An older Conductor, or a self-hosted one without OAuth.
    conductor.reject(404, "Not Found")
    with pytest.raises(
        DBOSInitializationError, match=r"has no entitlements route \(HTTP 404\)"
    ):
        check(conductor, enterprise_key="enterprise-key")


def test_a_redirect_is_refused_and_never_carries_the_key(
    conductor: FakeConductor,
) -> None:
    elsewhere = FakeConductor()
    try:
        target = elsewhere.url.replace("ws://", "http://")
        conductor.status = 302
        conductor.headers = {"Location": f"{target}/v2/entitlements"}
        with pytest.raises(
            DBOSInitializationError, match=r"rejected the key \(HTTP 302\)"
        ):
            check(conductor, enterprise_key="enterprise-key")
        # The redirect target never saw the key, or anything at all.
        assert elsewhere.requests == []
    finally:
        elsewhere.server.shutdown()
        elsewhere.server.server_close()


@pytest.mark.parametrize(
    "body", [[], {"entitlements": None}, {"entitled": True}, "not json"]
)
def test_a_malformed_answer_counts_as_an_outage(
    conductor: FakeConductor, body: Any
) -> None:
    conductor.body = body
    with pytest.raises(DBOSInitializationError, match="Retry once Conductor"):
        check(conductor, enterprise_key="enterprise-key")


def test_an_unreachable_conductor_blocks_startup(conductor: FakeConductor) -> None:
    conductor.reject(503, "unavailable")
    with pytest.raises(
        DBOSInitializationError,
        match=r"checking it with Conductor .* failed \(HTTP 503\)",
    ):
        check(conductor, enterprise_key="enterprise-key")
    # A port nothing listens on.
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        closed = s.getsockname()[1]
    with pytest.raises(DBOSInitializationError) as raised:
        _require_enterprise(
            [SOME_FEATURE],
            enterprise_key="enterprise-key",
            conductor_key=None,
            conductor_url=f"ws://127.0.0.1:{closed}",
        )
    message = str(raised.value)
    assert "checking it with Conductor" in message and "failed" in message
    assert "enterprise-key" not in message


def test_a_hosted_conductor_outage_does_not_block_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(GlobalParams, "dbos_cloud", False)
    monkeypatch.delenv("DBOS_DOMAIN", raising=False)
    requested: List[str] = []

    def unreachable(request: urllib.request.Request) -> Dict[str, bool]:
        requested.append(request.full_url)
        raise urllib.error.URLError("connection refused")

    warnings: List[str] = []
    monkeypatch.setattr(enterprise_module, "_fetch_entitlements", unreachable)
    monkeypatch.setattr(dbos_logger, "warning", lambda msg: warnings.append(msg))
    # No Conductor URL means DBOS's hosted one, whose outage only warns.
    _require_enterprise(
        [SOME_FEATURE],
        enterprise_key="enterprise-key",
        conductor_key=None,
        conductor_url=None,
    )
    assert requested == ["https://cloud.dbos.dev/conductor/v2/entitlements"]
    assert len(warnings) == 1 and "Could not verify the DBOS key" in warnings[0]
    assert "enterprise-key" not in warnings[0]
    # Another domain is not DBOS's hosted Conductor, so its outage still refuses.
    monkeypatch.setenv("DBOS_DOMAIN", "unreachable.example.com")
    with pytest.raises(
        DBOSInitializationError, match="Retry once Conductor is reachable"
    ):
        _require_enterprise(
            [SOME_FEATURE],
            enterprise_key="enterprise-key",
            conductor_key=None,
            conductor_url=None,
        )


def test_dbos_cloud_passes_without_a_check(
    conductor: FakeConductor, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(GlobalParams, "dbos_cloud", True)
    check(conductor)
    assert conductor.requests == []


def test_config_turns_on_enterprise_features() -> None:
    assert enterprise_module.enabled_features({}) == []
    assert enterprise_module.enabled_features({"sharding": False}) == []
    assert enterprise_module.enabled_features({"sharding": True}) == [
        enterprise_module.SHARDING
    ]
    # Every registered feature has a distinct entitlement name.
    names = [f.entitlement for f in enterprise_module.FEATURES]
    assert len(names) == len(set(names))


def test_sharded_executor_checks_its_entitlement(
    config: DBOSConfig,
    conductor: FakeConductor,
    skip_with_sharding: None,
) -> None:
    DBOS.destroy(destroy_registry=True)
    # Refused before the package is even loaded, so this holds without dbos-enterprise.
    with pytest.raises(
        DBOSInitializationError,
        match=f": {enterprise_module.SHARDING.description} requires a DBOS key",
    ):
        DBOS(config={**config, "sharding": True})
    DBOS.destroy(destroy_registry=True)
    sharded = {
        **config,
        "sharding": True,
        "transact_enterprise_key": "enterprise-key",
        "conductor_url": conductor.url,
    }
    conductor.entitle(**{**ALL_ENTITLED, "sharding": False})
    with pytest.raises(DBOSInitializationError, match="not entitled to sharding"):
        DBOS(config=sharded)  # type: ignore[arg-type]
    DBOS.destroy(destroy_registry=True)
    assert conductor.requests == [("/v2/entitlements", "Bearer enterprise-key")]
