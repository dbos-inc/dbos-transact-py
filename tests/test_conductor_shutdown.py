"""The Conductor connection must outlive the shutdown completion wait.

`DBOS.destroy(workflow_completion_timeout_sec=N)` drains: it stops queue polling
and waits for locally running workflows to finish. Conductor treats the websocket
as the executor's liveness signal, so dropping it at the start of the wait lets
Conductor declare this executor dead mid-drain and have a peer re-enqueue
workflows that are still running here. The connection must therefore stay up for
the whole wait and only be torn down just before the system database is.
"""

import threading
from typing import Any, Generator

import pytest
from websockets.sync.server import ServerConnection, serve

from dbos import DBOS, DBOSConfig

from .conftest import retry_until_success


class _ConductorStandIn:
    """A loopback stand-in for Conductor that only tracks connection liveness."""

    def __init__(self) -> None:
        self.connected = threading.Event()
        self._lock = threading.Lock()
        self._open = 0
        self._server = serve(self._handle, "127.0.0.1", 0)
        self.port: int = self._server.socket.getsockname()[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def open_connections(self) -> int:
        with self._lock:
            return self._open

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()

    def _handle(self, websocket: ServerConnection) -> None:
        with self._lock:
            self._open += 1
        self.connected.set()
        try:
            # Send nothing; hold the connection open until the client closes it.
            for _ in websocket:
                pass
        except Exception:
            pass
        finally:
            with self._lock:
                self._open -= 1


@pytest.fixture()
def conductor_stand_in() -> Generator[_ConductorStandIn, Any, None]:
    server = _ConductorStandIn()
    server.start()
    yield server
    server.stop()


def test_conductor_connection_outlives_shutdown_drain(
    config: DBOSConfig,
    cleanup_test_databases: None,
    conductor_stand_in: _ConductorStandIn,
) -> None:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(
        config=config,
        conductor_key="test-key",
        conductor_url=f"ws://127.0.0.1:{conductor_stand_in.port}",
    )

    workflow_started = threading.Event()
    blocking_event = threading.Event()

    @DBOS.workflow()
    def blocked_workflow() -> None:
        workflow_started.set()
        blocking_event.wait()

    DBOS.launch()
    try:
        assert conductor_stand_in.connected.wait(timeout=10)
        conductor = dbos.conductor_websocket
        assert conductor is not None

        DBOS.start_workflow(blocked_workflow)
        assert workflow_started.wait(timeout=10)

        destroy_thread = threading.Thread(
            target=lambda: DBOS.destroy(workflow_completion_timeout_sec=30)
        )
        destroy_thread.start()

        # The background stop events are set immediately before the completion
        # wait begins, so all of them being set means the drain is underway.
        def drain_started() -> None:
            assert dbos.background_thread_stop_events
            assert all(e.is_set() for e in dbos.background_thread_stop_events)

        retry_until_success(drain_started, interval=0.1, max_attempts=100)

        # Mid-drain: Conductor must still see us as a live executor.
        assert not conductor.evt.is_set()
        assert conductor.is_alive()
        assert conductor_stand_in.open_connections == 1

        blocking_event.set()
        destroy_thread.join(timeout=30)
        assert not destroy_thread.is_alive()

        # Only now, with the drain finished, does the connection go away.
        assert conductor.evt.is_set()
        assert not conductor.is_alive()

        def connection_closed() -> None:
            assert conductor_stand_in.open_connections == 0

        retry_until_success(connection_closed, interval=0.1, max_attempts=100)
    finally:
        blocking_event.set()
        DBOS.destroy(destroy_registry=True)
