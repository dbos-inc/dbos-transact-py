"""Auto-upgrade wiring in core: who enrolls, when launch refuses, which key the
entitlement is checked with, and the thread's lifecycle. The enterprise package and
the entitlement call are stood in for, so this runs without either; the pass itself
(the queries) is tested in dbos-enterprise and the HTTP call in test_entitlements."""

import threading
import types
from typing import Any, Generator, List, Optional, cast

import pytest

from dbos import DBOS, DBOSConfig
from dbos import _dbos as dbos_module
from dbos import _enterprise as enterprise_module
from dbos._auto_upgrade import AutoUpgradeThread
from dbos._error import DBOSInitializationError

from .conftest import retry_until_success


class _NoopConductor:
    def __init__(self, *args: object, **kwargs: object) -> None:
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


class _RecordingPass:
    """Satisfies dbos._enterprise.AutoUpgradePass, recording every call."""

    instances: List["_RecordingPass"] = []

    def __init__(self, dbos: DBOS) -> None:
        self.dbos = dbos
        self.runs: List[threading.Event] = []
        self.closes = 0
        self.events: List[str] = []
        self.run_error: Optional[Exception] = None
        self.close_error: Optional[Exception] = None
        self.block_until_stopped = False
        _RecordingPass.instances.append(self)

    def run(self, stop_event: threading.Event) -> int:
        self.events.append("run-start")
        self.runs.append(stop_event)
        if self.run_error is not None:
            raise self.run_error
        if self.block_until_stopped:
            stop_event.wait()
        self.events.append("run-end")
        return 0

    def close(self) -> None:
        self.events.append("close")
        self.closes += 1
        if self.close_error is not None:
            raise self.close_error


class _EntitlementStandIn:
    """Stands in for the entitlement call: records the keys checked, fails on demand."""

    def __init__(self) -> None:
        self.checked: List[str] = []
        self.error: Optional[DBOSInitializationError] = None

    def __call__(self, key: str, *, feature: str) -> None:
        assert feature == "auto_upgrade"
        self.checked.append(key)
        if self.error is not None:
            raise self.error


@pytest.fixture()
def entitlement(monkeypatch: pytest.MonkeyPatch) -> _EntitlementStandIn:
    stand_in = _EntitlementStandIn()
    monkeypatch.setattr(dbos_module, "check_enterprise_entitlement", stand_in)
    return stand_in


@pytest.fixture()
def enterprise_stand_in(
    monkeypatch: pytest.MonkeyPatch, entitlement: _EntitlementStandIn
) -> Generator[None, Any, None]:
    _RecordingPass.instances = []
    monkeypatch.setattr(
        enterprise_module,
        "load",
        lambda: types.SimpleNamespace(
            ConductorWebsocket=_NoopConductor, AutoUpgradePass=_RecordingPass
        ),
    )
    DBOS.destroy(destroy_registry=True)
    yield
    DBOS.destroy(destroy_registry=True)


def _conductor_config(config: DBOSConfig, **overrides: Any) -> DBOSConfig:
    return cast(
        DBOSConfig,
        {
            **config,
            "conductor_key": "test-key",
            "conductor_url": "ws://127.0.0.1:1",
            "auto_upgrade_interval_sec": 0.05,
            **overrides,
        },
    )


def _wait_for_runs(upgrade_pass: _RecordingPass, count: int) -> None:
    def enough() -> None:
        assert len(upgrade_pass.runs) >= count

    retry_until_success(enough, interval=0.05, max_attempts=100)


# --- Gating -------------------------------------------------------------------


def test_auto_upgrade_requires_a_key(
    config: DBOSConfig, cleanup_test_databases: None, enterprise_stand_in: None
) -> None:
    with pytest.raises(
        DBOSInitializationError, match="requires a Transact Enterprise key"
    ):
        DBOS(config={**config, "auto_upgrade": True})

    # The refused instance leaves nothing behind.
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.launch()


def test_opted_in_workflow_requires_a_key_at_launch(
    config: DBOSConfig,
    cleanup_test_databases: None,
    enterprise_stand_in: None,
    entitlement: _EntitlementStandIn,
) -> None:
    # The config is off and no key is given, so construction passes...
    DBOS(config=config)

    @DBOS.workflow(auto_upgrade=True)
    def wants_upgrade() -> None:
        pass

    # ...but the workflow's opt-in needs a key, which only launch can tell.
    with pytest.raises(
        DBOSInitializationError, match="requires a Transact Enterprise key"
    ):
        DBOS.launch()
    assert entitlement.checked == []
    assert _RecordingPass.instances == []


def test_launch_refuses_when_the_key_is_not_entitled(
    config: DBOSConfig,
    cleanup_test_databases: None,
    enterprise_stand_in: None,
    entitlement: _EntitlementStandIn,
) -> None:
    entitlement.error = DBOSInitializationError("not entitled (past_limit)")
    dbos = DBOS(config=_conductor_config(config, auto_upgrade=True))

    @DBOS.workflow()
    def inherits() -> None:
        pass

    with pytest.raises(DBOSInitializationError, match="past_limit"):
        DBOS.launch()
    assert entitlement.checked == ["test-key"]
    assert dbos.auto_upgrader is None
    assert _RecordingPass.instances == []


def test_entitlement_is_checked_with_the_enterprise_key_first(
    config: DBOSConfig,
    cleanup_test_databases: None,
    enterprise_stand_in: None,
    entitlement: _EntitlementStandIn,
) -> None:
    # Both keys given: the enterprise key is the one checked.
    dbos = DBOS(
        config=_conductor_config(
            config, auto_upgrade=True, transact_enterprise_key="enterprise-key"
        )
    )

    @DBOS.workflow()
    def inherits() -> None:
        pass

    DBOS.launch()
    assert entitlement.checked == ["enterprise-key"]
    assert dbos.auto_upgrader is not None
    DBOS.destroy(destroy_registry=True)

    # The enterprise key alone is enough: no Conductor connection is needed.
    dbos = DBOS(
        config={
            **config,
            "auto_upgrade": True,
            "transact_enterprise_key": "enterprise-key",
            "auto_upgrade_interval_sec": 0.05,
        }
    )

    @DBOS.workflow()
    def standalone() -> None:
        pass

    DBOS.launch()
    assert entitlement.checked == ["enterprise-key", "enterprise-key"]
    assert dbos.conductor_websocket is None
    assert dbos.auto_upgrader is not None
    assert dbos.auto_upgrader.is_alive()


# --- Enrollment ---------------------------------------------------------------


def test_workflows_enroll_by_flag_then_by_config(
    config: DBOSConfig, cleanup_test_databases: None, enterprise_stand_in: None
) -> None:
    dbos = DBOS(config=_conductor_config(config))

    @DBOS.workflow(auto_upgrade=True)
    def opted_in() -> None:
        pass

    @DBOS.workflow(auto_upgrade=False)
    def opted_out() -> None:
        pass

    @DBOS.workflow()
    def inherits() -> None:
        pass

    class Jobs:
        @staticmethod
        @DBOS.workflow()
        def method() -> None:
            pass

    # Names are the qualnames stored in workflow_status.name.
    assert dbos._registry.auto_upgradable_workflows(default=False) == [
        opted_in.__qualname__
    ]
    assert sorted(dbos._registry.auto_upgradable_workflows(default=True)) == sorted(
        [Jobs.method.__qualname__, inherits.__qualname__, opted_in.__qualname__]
    )
    # The config is off, so only the explicit opt-in enrolls.
    assert dbos._auto_upgradable_workflows() == [opted_in.__qualname__]


def test_no_thread_without_enrolled_workflows(
    config: DBOSConfig,
    cleanup_test_databases: None,
    enterprise_stand_in: None,
    entitlement: _EntitlementStandIn,
) -> None:
    # A key is given, but nothing enrolls: the config is off and the one workflow opts out.
    dbos = DBOS(config=_conductor_config(config))

    @DBOS.workflow(auto_upgrade=False)
    def opted_out() -> None:
        pass

    DBOS.launch()
    assert dbos.auto_upgrader is None
    assert _RecordingPass.instances == []
    # So no entitlement is checked either.
    assert entitlement.checked == []


# --- Thread lifecycle through DBOS ---------------------------------------------


def test_thread_runs_the_pass_until_destroy(
    config: DBOSConfig,
    cleanup_test_databases: None,
    enterprise_stand_in: None,
    entitlement: _EntitlementStandIn,
) -> None:
    dbos = DBOS(config=_conductor_config(config, auto_upgrade=True))

    @DBOS.workflow()
    def inherits() -> None:
        pass

    DBOS.launch()
    # The config's flag reaches the instance: the unflagged workflow enrolls, and the
    # Conductor key stands in for the enterprise key.
    assert dbos._auto_upgradable_workflows() == [inherits.__qualname__]
    assert entitlement.checked == ["test-key"]
    upgrader = dbos.auto_upgrader
    assert upgrader is not None
    assert upgrader.is_alive()
    assert upgrader.interval_sec == 0.05

    # The pass was built from this instance and ticks on the configured interval.
    [upgrade_pass] = _RecordingPass.instances
    assert upgrade_pass.dbos is dbos
    _wait_for_runs(upgrade_pass, 3)
    assert all(evt is upgrader.stop_event for evt in upgrade_pass.runs)

    DBOS.destroy(destroy_registry=True)
    assert not upgrader.is_alive()
    assert upgrade_pass.closes == 1
    # Nothing ticks after destroy.
    runs = len(upgrade_pass.runs)
    threading.Event().wait(0.2)
    assert len(upgrade_pass.runs) == runs


def test_default_interval(
    config: DBOSConfig, cleanup_test_databases: None, enterprise_stand_in: None
) -> None:
    dbos = DBOS(
        config=_conductor_config(
            config, auto_upgrade=True, auto_upgrade_interval_sec=None
        )
    )

    @DBOS.workflow()
    def inherits() -> None:
        pass

    DBOS.launch()
    assert dbos.auto_upgrader is not None
    assert dbos.auto_upgrader.interval_sec == 30.0


# --- Thread lifecycle in isolation --------------------------------------------


def _thread(
    upgrade_pass: _RecordingPass, interval_sec: float = 0.01
) -> AutoUpgradeThread:
    return AutoUpgradeThread(cast(DBOS, None), upgrade_pass, interval_sec=interval_sec)


def test_pass_failure_does_not_stop_the_thread(enterprise_stand_in: None) -> None:
    upgrade_pass = _RecordingPass(cast(DBOS, None))
    upgrade_pass.run_error = RuntimeError("boom")
    thread = _thread(upgrade_pass)
    thread.start()
    try:
        _wait_for_runs(upgrade_pass, 3)
    finally:
        thread.stop()
    assert not thread.is_alive()


def test_stop_lets_a_pass_in_flight_finish_before_closing(
    enterprise_stand_in: None,
) -> None:
    upgrade_pass = _RecordingPass(cast(DBOS, None))
    upgrade_pass.block_until_stopped = True
    thread = _thread(upgrade_pass)
    thread.start()
    _wait_for_runs(upgrade_pass, 1)

    thread.stop()
    assert not thread.is_alive()
    # The pass saw the stop event, returned, and only then was closed.
    assert upgrade_pass.events == ["run-start", "run-end", "close"]
