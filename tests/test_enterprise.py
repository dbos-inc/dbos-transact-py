"""The seam to dbos-enterprise: what core reports when the package is absent or
broken, and that a refused construction leaves nothing behind."""

import types
from typing import Any

import pytest

from dbos import DBOS, DBOSConfig
from dbos import _enterprise as enterprise_module
from dbos._error import DBOSInitializationError
from dbos._utils import GlobalParams


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
    with pytest.raises(DBOSInitializationError, match="pip install dbos-enterprise"):
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
    def load() -> Any:
        raise DBOSInitializationError("refused")

    monkeypatch.setattr(enterprise_module, "load", load)


def test_refused_construction_leaves_nothing_behind(
    config: DBOSConfig, cleanup_test_databases: None, monkeypatch: pytest.MonkeyPatch
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
