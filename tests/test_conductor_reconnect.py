"""
Test suite for ConductorWebsocket reconnection behavior.

Key regression: with websockets >= 15.0, the custom keepalive thread is disabled
and recv() has no timeout, so a silently dead connection causes recv() to hang
forever and reconnection never triggers.
"""

import json
import selectors
import socket
import threading
import time
from unittest.mock import MagicMock

import pytest
from websockets.sync.server import ServerConnection, serve

from dbos._conductor import protocol as p
from dbos._conductor.conductor import ConductorWebsocket
from dbos._utils import GlobalParams


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _executor_info_request(request_id: str = "req-1") -> str:
    return json.dumps({"type": p.MessageType.EXECUTOR_INFO.value, "request_id": request_id})


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_mock_dbos(app_name: str = "test-app") -> MagicMock:
    mock = MagicMock()
    mock._config = {"name": app_name}
    mock.logger = MagicMock()
    mock.logger.info.side_effect = lambda msg, *a: print(f"  [INFO] {msg}")
    mock.logger.warning.side_effect = lambda msg, *a: print(f"  [WARN] {msg}")
    mock.logger.error.side_effect = lambda msg, *a: print(f"  [ERROR] {msg}")
    mock.logger.debug.side_effect = lambda msg, *a: None
    mock._conductor_executor_metadata = {}
    mock._recover_pending_workflows = MagicMock()
    return mock


def _make_conductor(port: int):
    """Create a ConductorWebsocket pointed at localhost:port. Returns (ws_thread, evt)."""
    GlobalParams.executor_id = "test-executor-id"
    GlobalParams.app_version = "test-version"
    GlobalParams.dbos_version = "0.0.0-test"

    mock_dbos = _make_mock_dbos()
    evt = threading.Event()
    ws = ConductorWebsocket(
        dbos=mock_dbos,
        conductor_url=f"ws://127.0.0.1:{port}",
        conductor_key="fake-key",
        evt=evt,
    )
    ws.url = f"ws://127.0.0.1:{port}"
    return ws, evt


# ---------------------------------------------------------------------------
# Mock WebSocket Server
# ---------------------------------------------------------------------------

class MockWebSocketServer:
    """Minimal WebSocket server that sends EXECUTOR_INFO on connect."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.host = host
        self.port = port
        self.connections: int = 0
        self._lock = threading.Lock()
        self._server = None
        self._thread = None
        self._stop = threading.Event()

    def _handler(self, websocket: ServerConnection) -> None:
        with self._lock:
            self.connections += 1
            conn_num = self.connections
        try:
            websocket.send(_executor_info_request(f"req-{conn_num}"))
        except Exception:
            return
        try:
            while not self._stop.is_set():
                try:
                    websocket.recv(timeout=0.5)
                except TimeoutError:
                    continue
        except Exception:
            pass

    def start(self) -> None:
        self._stop.clear()
        self._server = serve(self._handler, self.host, self.port)
        if self.port == 0:
            self.port = self._server.socket.getsockname()[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._server:
            self._server.shutdown()
            if self._thread:
                self._thread.join(timeout=5)
            self._server = None
            self._thread = None

    def wait_for_connections(self, n: int, timeout: float = 10.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if self.connections >= n:
                    return True
            time.sleep(0.05)
        return False


# ---------------------------------------------------------------------------
# TCP Proxy — can be "cut" to simulate a network black hole
# ---------------------------------------------------------------------------

class TCPProxy:
    """Bidirectional TCP proxy that can stop forwarding on demand.

    When cut() is called, the proxy stops relaying bytes between the client
    and server. Both TCP connections stay open — no RST, no FIN — perfectly
    simulating a network black hole (firewall drop, NAT timeout, etc.).
    """

    def __init__(self, listen_port: int, target_host: str, target_port: int):
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self._cut = threading.Event()
        self._stop = threading.Event()
        self._listener = None
        self._thread = None

    def start(self) -> None:
        self._stop.clear()
        self._cut.clear()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", self.listen_port))
        self._listener.listen(5)
        self._listener.settimeout(0.5)
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                client_sock, _ = self._listener.accept()
            except (socket.timeout, OSError):
                continue
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                server_sock.connect((self.target_host, self.target_port))
            except OSError:
                client_sock.close()
                continue
            t = threading.Thread(
                target=self._relay, args=(client_sock, server_sock), daemon=True
            )
            t.start()

    def _relay(self, client_sock: socket.socket, server_sock: socket.socket) -> None:
        sel = selectors.DefaultSelector()
        try:
            client_sock.setblocking(False)
            server_sock.setblocking(False)
            sel.register(client_sock, selectors.EVENT_READ, server_sock)
            sel.register(server_sock, selectors.EVENT_READ, client_sock)
            while not self._stop.is_set():
                if self._cut.is_set():
                    # Black hole: keep sockets open, stop relaying.
                    self._stop.wait(timeout=1)
                    continue
                events = sel.select(timeout=0.5)
                for key, _ in events:
                    src = key.fileobj
                    dst = key.data
                    try:
                        data = src.recv(65536)
                        if not data:
                            return
                        dst.sendall(data)
                    except (BlockingIOError, ConnectionError, OSError):
                        return
        finally:
            sel.close()

    def cut(self) -> None:
        """Stop forwarding. Both sides keep sockets open — a true black hole."""
        self._cut.set()

    def stop(self) -> None:
        self._stop.set()
        if self._listener:
            self._listener.close()
        if self._thread:
            self._thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestConductorReconnection:

    def test_server_restart(self):
        """Client reconnects after the server disappears and comes back."""
        port = _find_free_port()
        server = MockWebSocketServer(port=port)
        server.start()

        ws, evt = _make_conductor(port)
        ws.start()
        try:
            assert server.wait_for_connections(1), "Client never connected"
            server.stop()
            time.sleep(1)

            server2 = MockWebSocketServer(port=port)
            server2.start()
            try:
                assert server2.wait_for_connections(1, timeout=15), (
                    "Client did NOT reconnect after server restart"
                )
            finally:
                server2.stop()
        finally:
            evt.set()
            ws.join(timeout=5)

    def test_silent_connection_death(self):
        """Client reconnects when the connection goes silently dead.

        Uses a TCP proxy to create a true black hole: after cutting the proxy,
        both TCP connections stay open but no bytes flow. The client's
        websockets keepalive sends pings that disappear into the void.
        The client must detect this via keepalive timeout and reconnect.
        """
        server = MockWebSocketServer()
        server.start()

        proxy_port = _find_free_port()
        proxy = TCPProxy(proxy_port, "127.0.0.1", server.port)
        proxy.start()

        ws, evt = _make_conductor(proxy_port)
        ws.start()
        try:
            assert server.wait_for_connections(1), "Client never connected"
            time.sleep(0.5)

            # Cut the proxy — true network black hole.
            proxy.cut()
            print("[TEST] Proxy cut — connection is a black hole")

            # Wait for the client to detect the dead connection.
            # websockets default keepalive: ping_interval=20, ping_timeout=20
            # Detection takes up to ~40s, plus up to 30s for recv timeout.
            # Poll until the client starts reconnecting (we'll see connection
            # attempts hitting the cut proxy).
            time.sleep(50)

            # "Restore" the network: stop the cut proxy and start a fresh one.
            proxy.stop()
            proxy2 = TCPProxy(proxy_port, "127.0.0.1", server.port)
            proxy2.start()
            print("[TEST] Proxy restored — network is back")

            try:
                assert server.wait_for_connections(2, timeout=30), (
                    "Client did NOT reconnect after silent connection death. "
                    "recv() likely hung without a timeout, preventing the "
                    "reconnection loop from running."
                )
                print("[TEST] PASS — client reconnected")
            finally:
                proxy2.stop()
        finally:
            evt.set()
            proxy.stop()
            server.stop()
            ws.join(timeout=5)
