"""The contract between DBOS Transact and the dbos-enterprise package.

Core declares here exactly what it uses from enterprise, and loads the package by
name only when a feature needs it. Enterprise imports these types and checks that
it satisfies them, so each side is verified against the contract on its own.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Protocol, cast

from ._error import DBOSInitializationError
from ._utils import GlobalParams

if TYPE_CHECKING:
    from ._dbos import DBOS


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
    ) -> ConductorThread: ...


class Enterprise(Protocol):
    """What the top-level dbos_enterprise module must provide."""

    # A property rather than an attribute, so implementations may provide any compatible callable.
    @property
    def ConductorWebsocket(self) -> ConductorFactory: ...


def load() -> Enterprise:
    """Import dbos_enterprise, telling an absent package apart from one that fails to import."""
    try:
        return cast(Enterprise, importlib.import_module("dbos_enterprise"))
    except ImportError as e:
        if isinstance(e, ModuleNotFoundError) and e.name == "dbos_enterprise":
            raise DBOSInitializationError(
                'Connecting to DBOS Conductor requires the dbos-enterprise package. Install it with `pip install "dbos[enterprise]"`.'
            ) from e
        # The package is present; it imports dbos internals, so this is almost always a version mismatch.
        raise DBOSInitializationError(
            f"The dbos-enterprise package is installed but could not be imported by dbos {GlobalParams.dbos_version}. "
            f"The two must be the same version; upgrade both together. Cause: {e}"
        ) from e
