"""The seam to dbos-enterprise: what core reports when the package is absent or
broken, that a refused construction leaves nothing behind, and the entitlement check
every enterprise feature goes through, against a local stand-in for Conductor."""

import email.message
import io
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

# Captured at import, before any test harness replaces it.
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
    # The message names what needed the package.
    with pytest.raises(
        DBOSInitializationError, match="Auto-upgrading workflows requires the"
    ):
        enterprise_module.load(enterprise_module.AUTO_UPGRADE.description)


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


# --- Entitlements -------------------------------------------------------------

# A feature for exercising the check itself, entitled under a name Conductor may not know.
SOME_FEATURE = enterprise_module.Feature("someFeature", "Some feature")
ALL_ENTITLED = {"sharding": True, "autoUpgrade": True, "suspendWorkflows": True}


class FakeConductor:
    """A local stand-in for Conductor's entitlements route."""

    def __init__(self) -> None:
        self.status = 200
        self.body: Any = {"entitlements": {**ALL_ENTITLED, "someFeature": True}}
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

    def reject(
        self, status: int, detail: str, headers: Optional[Dict[str, str]] = None
    ) -> None:
        self.status = status
        self.body = {"title": "Rejected", "status": status, "detail": detail}
        self.headers = headers or {}

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


def test_the_default_conductor_follows_the_dbos_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DBOS_DOMAIN", raising=False)
    assert (
        enterprise_module.default_conductor_url()
        == enterprise_module.HOSTED_CONDUCTOR_URL
    )
    monkeypatch.setenv("DBOS_DOMAIN", "staging.dbos.dev")
    assert (
        enterprise_module.default_conductor_url()
        == "wss://staging.dbos.dev/conductor/v1alpha1"
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
    conductor.entitle(sharding=True, autoUpgrade=False, someFeature=True)
    check(conductor, enterprise_key="enterprise-key")
    with pytest.raises(DBOSInitializationError) as raised:
        check(
            conductor,
            enterprise_key="enterprise-key",
            features=[SOME_FEATURE, enterprise_module.AUTO_UPGRADE],
        )
    message = str(raised.value)
    # Names only what the org lacks, by entitlement and by what it does.
    assert "not entitled to autoUpgrade (Auto-upgrading workflows)" in message
    assert "plan does not include it" in message
    assert "someFeature" not in message and "enterprise-key" not in message
    # One request answers for every feature.
    assert len(conductor.requests) == 2


def test_a_past_limit_org_is_told_to_upgrade(conductor: FakeConductor) -> None:
    # Conductor refuses a past-limit org outright, tagging the 403 with a code.
    conductor.reject(
        403,
        "your DBOS account is past the free trial limit",
        headers={"X-DBOS-Error": "past_limit"},
    )
    with pytest.raises(DBOSInitializationError) as raised:
        check(conductor, enterprise_key="enterprise-key")
    message = str(raised.value)
    assert "past its plan limit, so it is not entitled to someFeature" in message
    assert "Upgrade its plan" in message
    # The code, not the status or the prose, marks it: a bare 403 is another refusal.
    conductor.reject(403, "your DBOS account is past the free trial limit")
    with pytest.raises(DBOSInitializationError) as raised:
        check(conductor, enterprise_key="enterprise-key")
    assert "refused the check (HTTP 403)" in str(raised.value)
    assert "Upgrade its plan" not in str(raised.value)


def test_an_entitlement_conductor_does_not_report_is_not_granted(
    conductor: FakeConductor,
) -> None:
    # A Conductor older than the feature doesn't list it, which grants nothing.
    conductor.entitle(**ALL_ENTITLED)
    with pytest.raises(DBOSInitializationError, match="not entitled to someFeature"):
        check(conductor, enterprise_key="enterprise-key")
    # Anything but true grants nothing either.
    conductor.entitle(someFeature="yes")  # type: ignore[arg-type]
    with pytest.raises(DBOSInitializationError, match="not entitled to someFeature"):
        check(conductor, enterprise_key="enterprise-key")


def test_enterprise_features_require_a_key(
    conductor: FakeConductor, monkeypatch: pytest.MonkeyPatch
) -> None:
    # DBOS's hosted Conductor, implicit or named, is never asked without a key.
    monkeypatch.delenv("DBOS_DOMAIN", raising=False)
    for url in [None, enterprise_module.HOSTED_CONDUCTOR_URL]:
        with pytest.raises(
            DBOSInitializationError,
            match=": Some feature requires a DBOS key. Set transact_enterprise_key",
        ):
            _require_enterprise(
                [SOME_FEATURE],
                enterprise_key=None,
                conductor_key=None,
                conductor_url=url,
            )
    assert conductor.requests == []


def test_a_self_hosted_conductor_is_asked_without_a_key(
    conductor: FakeConductor,
) -> None:
    # Without OAuth, a self-hosted Conductor grants every feature to any caller.
    check(conductor)
    assert conductor.requests == [("/v2/entitlements", None)]
    # With OAuth, it wants a key.
    conductor.reject(401, "this endpoint requires a DBOS API key")
    with pytest.raises(DBOSInitializationError) as raised:
        check(conductor)
    message = str(raised.value)
    assert "requires a DBOS key: Conductor at" in message
    assert "(HTTP 401): this endpoint requires a DBOS API key" in message
    assert "Set transact_enterprise_key" in message


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
    "detail",
    [
        "invalid API key",
        # A user credential, which the route refuses in favor of an API key.
        "this endpoint requires a DBOS API key",
    ],
)
def test_a_rejected_key_is_refused(conductor: FakeConductor, detail: str) -> None:
    conductor.reject(401, detail)
    with pytest.raises(DBOSInitializationError) as raised:
        check(conductor, enterprise_key="enterprise-key")
    message = str(raised.value)
    assert "requires a valid DBOS key." in message
    assert f"rejected the key (HTTP 401): {detail}" in message
    assert "enterprise-key" not in message


@pytest.mark.parametrize(
    "status, detail",
    [(403, "forbidden"), (404, "organization not found"), (409, "conflict")],
)
def test_any_other_refusal_quotes_conductor(
    conductor: FakeConductor, status: int, detail: str
) -> None:
    conductor.reject(status, detail)
    with pytest.raises(DBOSInitializationError) as raised:
        check(conductor, enterprise_key="enterprise-key")
    message = str(raised.value)
    assert f"refused the check (HTTP {status}): {detail}" in message
    assert "rejected the key" not in message and "enterprise-key" not in message


def test_a_conductor_without_the_route_is_refused(conductor: FakeConductor) -> None:
    # An older Conductor's router answers an unknown route with no problem detail.
    conductor.status = 404
    conductor.body = "404 page not found"
    with pytest.raises(
        DBOSInitializationError, match=r"has no entitlements route \(HTTP 404\)"
    ):
        check(conductor, enterprise_key="enterprise-key")


def test_a_redirect_is_refused_and_never_carries_the_key(
    conductor: FakeConductor,
) -> None:
    # urllib follows a redirect by re-sending the request, headers included, to the
    # host the Location names. That would hand the key to whoever answered.
    elsewhere = FakeConductor()
    try:
        target = elsewhere.url.replace("ws://", "http://")
        conductor.status = 302
        conductor.headers = {"Location": f"{target}/v2/entitlements"}
        with pytest.raises(
            DBOSInitializationError, match=r"refused the check \(HTTP 302\)"
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
        match=r"checking it with Conductor .* failed \(HTTP 503: unavailable\)",
    ):
        check(conductor, enterprise_key="enterprise-key")
    # A failure without a problem detail is reported by status alone.
    conductor.body = "Bad Gateway"
    conductor.status = 502
    with pytest.raises(DBOSInitializationError, match=r"failed \(HTTP 502\)\."):
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


def _hosted_answers(
    monkeypatch: pytest.MonkeyPatch, status: int, headers: Dict[str, str]
) -> List[str]:
    """Make the hosted Conductor answer with this error; returns the warnings logged."""
    monkeypatch.setattr(GlobalParams, "dbos_cloud", False)
    monkeypatch.delenv("DBOS_DOMAIN", raising=False)

    def answer(request: urllib.request.Request) -> Dict[str, bool]:
        hdrs = email.message.Message()
        for name, value in headers.items():
            hdrs[name] = value
        raise urllib.error.HTTPError(
            request.full_url, status, "error", hdrs, io.BytesIO(b"")
        )

    warnings: List[str] = []
    monkeypatch.setattr(enterprise_module, "_fetch_entitlements", answer)
    monkeypatch.setattr(dbos_logger, "warning", lambda msg: warnings.append(msg))
    return warnings


def _check_hosted() -> None:
    _require_enterprise(
        [SOME_FEATURE],
        enterprise_key="enterprise-key",
        conductor_key=None,
        conductor_url=None,
    )


@pytest.mark.parametrize("status", [403, 407, 408, 425, 429])
def test_hosted_throttling_and_proxies_count_as_outages(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    """From the hosted Conductor these mean it couldn't answer, not that it refused the
    key, so startup warns and goes on as for any hosted outage."""
    warnings = _hosted_answers(monkeypatch, status, {})
    _check_hosted()
    assert len(warnings) == 1 and f"(HTTP {status})" in warnings[0]


def test_hosted_refusals_still_refuse(monkeypatch: pytest.MonkeyPatch) -> None:
    _hosted_answers(monkeypatch, 403, {"X-DBOS-Error": "past_limit"})
    with pytest.raises(DBOSInitializationError, match="past its plan limit"):
        _check_hosted()
    _hosted_answers(monkeypatch, 401, {})
    with pytest.raises(DBOSInitializationError, match=r"rejected the key \(HTTP 401\)"):
        _check_hosted()


def test_another_conductors_throttling_still_refuses(conductor: FakeConductor) -> None:
    conductor.reject(429, "slow down")
    with pytest.raises(
        DBOSInitializationError, match=r"refused the check \(HTTP 429\)"
    ):
        check(conductor, enterprise_key="enterprise-key")


def test_dbos_cloud_passes_without_a_check(
    conductor: FakeConductor, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(GlobalParams, "dbos_cloud", True)
    check(conductor)
    assert conductor.requests == []


def test_config_turns_on_enterprise_features() -> None:
    assert enterprise_module.enabled_features({}) == []
    assert enterprise_module.enabled_features({"auto_upgrade": False}) == []
    assert enterprise_module.enabled_features({"auto_upgrade": True}) == [
        enterprise_module.AUTO_UPGRADE
    ]
    assert enterprise_module.enabled_features({"sharding": False}) == []
    assert enterprise_module.enabled_features({"sharding": True}) == [
        enterprise_module.SHARDING
    ]
    # Every registered feature has a distinct entitlement name.
    names = [f.entitlement for f in enterprise_module.FEATURES]
    assert len(names) == len(set(names))


def test_an_auto_upgrading_executor_checks_its_entitlement(
    config: DBOSConfig, conductor: FakeConductor, skip_with_sharding: None
) -> None:
    DBOS.destroy(destroy_registry=True)
    # Refused before the package is even loaded, so this holds without dbos-enterprise.
    with pytest.raises(
        DBOSInitializationError,
        match=f": {enterprise_module.AUTO_UPGRADE.description} requires a DBOS key",
    ):
        DBOS(config={**config, "auto_upgrade": True})
    DBOS.destroy(destroy_registry=True)
    upgrading = {
        **config,
        "auto_upgrade": True,
        "transact_enterprise_key": "enterprise-key",
        "conductor_url": conductor.url,
    }
    conductor.entitle(**{**ALL_ENTITLED, "autoUpgrade": False})
    with pytest.raises(DBOSInitializationError, match="not entitled to autoUpgrade"):
        DBOS(config=upgrading)  # type: ignore[arg-type]
    DBOS.destroy(destroy_registry=True)
    assert conductor.requests == [("/v2/entitlements", "Bearer enterprise-key")]


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
