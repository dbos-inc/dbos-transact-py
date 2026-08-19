"""Conductor websocket lifecycle: staying connected, and disconnecting on time.

Two regressions live here, at opposite ends of the connection's life.

Reconnect (test_conductor_reconnects_after_keepalive_timeout): when the
websockets library's built-in keepalive fires (websockets >= 15.0) and the close
handshake also times out - the network is wedged so the kernel never sees a
FIN/RST, and the library's forced socket close fails to unblock the recv_events
thread - the user-level `websocket.recv()` in `ConductorWebsocket.run()` stays
blocked indefinitely and the reconnect loop never iterates. Reproduced against a
hand-rolled black-hole server with `Connection.close_socket` neutralised, and
asserted via the "Reconnecting" warning that a healthy run loop emits.

Shutdown (test_conductor_connection_outlives_shutdown_drain):
`DBOS.destroy(workflow_completion_timeout_sec=N)` drains - it stops queue polling
and waits for locally running workflows to finish. Conductor treats the websocket
as the executor's liveness signal, so dropping it at the start of the wait lets
Conductor declare this executor dead mid-drain and have a peer re-enqueue
workflows that are still running here. The connection must stay up for the whole
wait and only be torn down just before the system database is.
"""

import base64
import hashlib
import logging
import socket
import struct
import threading
from importlib.metadata import version
from typing import Any, Generator, List, Optional

import pytest
from websockets.sync import connection as ws_connection
from websockets.sync.client import connect as _real_connect
from websockets.sync.server import ServerConnection, serve

from dbos import DBOS, DBOSConfig
from dbos._conductor import conductor as conductor_module
from dbos._conductor import protocol as p

from .conftest import retry_until_success

WS_VERSION = version("websockets")

_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _ws_accept(key: str) -> str:
    digest = hashlib.sha1((key + _WS_GUID).encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


def _encode_text_frame(payload: str) -> bytes:
    """Encode a single unmasked text frame (server -> client)."""
    data = payload.encode("utf-8")
    header = bytearray([0x81])  # FIN=1, opcode=0x1 (text)
    n = len(data)
    if n < 126:
        header.append(n)
    elif n < 1 << 16:
        header.append(126)
        header += struct.pack(">H", n)
    else:
        header.append(127)
        header += struct.pack(">Q", n)
    return bytes(header) + data


class _BlackHoleWSServer:
    """A loopback WebSocket server that completes the EXECUTOR_INFO handshake
    and then stops responding to anything. The TCP socket is held open with no
    bytes flowing in either direction, simulating a wedged network."""

    def __init__(self) -> None:
        self._listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listen.bind(("127.0.0.1", 0))
        self._listen.listen(4)
        self.port: int = self._listen.getsockname()[1]
        self.handshake_done = threading.Event()
        self.connection_count = 0
        self._stop = threading.Event()
        self._conns: List[socket.socket] = []
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._listen.close()
        except OSError:
            pass
        for c in list(self._conns):
            try:
                c.close()
            except OSError:
                pass

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._listen.accept()
            except OSError:
                return
            self._conns.append(conn)
            self.connection_count += 1
            t = threading.Thread(target=self._handle, args=(conn,), daemon=True)
            t.start()

    def _handle(self, conn: socket.socket) -> None:
        try:
            # 1) Read HTTP upgrade request.
            buf = b""
            conn.settimeout(5)
            while b"\r\n\r\n" not in buf:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                buf += chunk
            key = ""
            for line in buf.split(b"\r\n"):
                if line.lower().startswith(b"sec-websocket-key:"):
                    key = line.split(b":", 1)[1].strip().decode("ascii")
                    break
            if not key:
                return
            resp = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {_ws_accept(key)}\r\n\r\n"
            )
            conn.sendall(resp.encode("ascii"))

            # 2) Send an EXECUTOR_INFO request to drive the conductor through
            # its handshake branch, exactly like real conductor would.
            req = p.ExecutorInfoRequest(
                type=p.MessageType.EXECUTOR_INFO,
                request_id="test-executor-info",
            ).to_json()
            conn.sendall(_encode_text_frame(req))

            # 3) Drain the EXECUTOR_INFO response from the client (best-effort
            # — we don't decode it; we just want the recv to unblock once and
            # confirm the conductor reached the steady-state recv() loop).
            conn.settimeout(5)
            try:
                conn.recv(65536)
            except (socket.timeout, OSError):
                pass

            self.handshake_done.set()

            # 4) Black-hole. Never read, never write, never close. The TCP
            # socket stays open at the kernel level — no FIN, no RST, no
            # pongs — exactly the wedge the production incident hit.
            conn.settimeout(None)
            self._stop.wait()
        except Exception:
            return


class _StubDBOS:
    """Minimum surface ConductorWebsocket touches before the bug fires.

    We only ever exercise the EXECUTOR_INFO branch + the run-loop boilerplate,
    so we don't need a real DBOS / sysdb / queues."""

    def __init__(self, logger: logging.Logger) -> None:
        self._config = {"name": "regression-test-app"}
        self.logger = logger
        self._conductor_executor_metadata: Optional[dict[str, Any]] = None


class _ReconnectLogProbe(logging.Handler):
    """Captures the log record we expect the conductor to emit when the
    library-level keepalive tears the connection down. The conductor uses two
    code paths — `ConnectionClosedOK` and `ConnectionClosed` — both of which
    log the substring 'Reconnecting'. We watch for either."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.reconnecting = threading.Event()
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
        except Exception:
            msg = ""
        self.records.append(record)
        if "Reconnecting" in msg:
            self.reconnecting.set()


@pytest.mark.skipif(
    WS_VERSION < "15.0",
    reason=(
        "The deadlock is only reachable on websockets>=15.0, where DBOS "
        "relies on the library's built-in keepalive (use_keepalive=False)."
    ),
)
def test_conductor_reconnects_after_keepalive_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the websockets library's keepalive fires and the close handshake
    times out, the conductor's run() loop must still escape recv() and try
    to reconnect. On main this fails because recv() stays blocked."""

    # Force the keepalive window to be small so the test runs in seconds.
    def fast_connect(*args: Any, **kwargs: Any) -> Any:
        kwargs["ping_interval"] = 0.5
        kwargs["ping_timeout"] = 0.5
        kwargs["close_timeout"] = 1.0
        return _real_connect(*args, **kwargs)

    monkeypatch.setattr(conductor_module, "connect", fast_connect)

    # Simulate the production wedge: the OS-level shutdown(SHUT_RDWR) +
    # socket.close() in `Connection.close_socket` fail to wake the
    # recv_events thread (kernel doesn't propagate the close to other
    # threads' blocked recv()), so the user-level `websocket.recv()` in
    # ConductorWebsocket.run() stays blocked. The pure-Python state
    # transition that follows still runs, so `websocket.close_code`
    # becomes observable — that's the signal a robust conductor must
    # poll for. We patch `close_socket` to model exactly that: skip the
    # OS calls, run the protocol state transition.
    def wedged_close_socket(self: Any) -> None:
        with self.protocol_mutex:
            self.protocol.receive_eof()
            # terminate_pending_pings was added in websockets 16.0; older versions
            # don't have it. Be tolerant so the test runs against either.
            terminate = getattr(self, "terminate_pending_pings", None)
            if terminate is not None:
                terminate()

    monkeypatch.setattr(ws_connection.Connection, "close_socket", wedged_close_socket)

    server = _BlackHoleWSServer()
    server.start()

    logger = logging.getLogger("test_conductor_reconnect")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    probe = _ReconnectLogProbe()
    logger.addHandler(probe)

    stub = _StubDBOS(logger)
    evt = threading.Event()

    cw = conductor_module.ConductorWebsocket(
        dbos=stub,  # type: ignore[arg-type]
        app_name="test-app",
        conductor_url=f"ws://127.0.0.1:{server.port}",
        conductor_key="test-key",
        evt=evt,
    )
    cw.start()

    try:
        # Wait for the executor handshake — ensures the run loop is parked
        # in recv() (the exact spot the bug freezes).
        assert server.handshake_done.wait(timeout=10), (
            "Server never completed the EXECUTOR_INFO handshake — the "
            "conductor never reached the steady-state recv() loop."
        )

        # Allow ample time for the websockets keepalive to:
        #   (a) send a ping (every 0.5s),
        #   (b) time out waiting for pong (0.5s),
        #   (c) attempt close handshake (1s close_timeout),
        # and for the conductor's `except ConnectionClosed` branch to run.
        # 8s is well over the worst-case ~2s budget.
        deadline_s = 8.0

        # The fix-state behavior we assert: a "Reconnecting" warning is
        # emitted from conductor.py:run() within `deadline_s`. On main the
        # recv() stays blocked and this never fires.
        assert probe.reconnecting.wait(timeout=deadline_s), (
            f"Conductor did not log a 'Reconnecting' warning within "
            f"{deadline_s:.1f}s after the websockets keepalive must have "
            f"fired (ping_interval=0.5s, ping_timeout=0.5s, "
            f"close_timeout=1.0s). The run() loop is deadlocked inside "
            f"websocket.recv() — see "
            f"dbos/_conductor/conductor.py:108 / :939. "
            f"Recent log records: "
            f"{[r.getMessage() for r in probe.records[-10:]]}"
        )
    finally:
        evt.set()
        server.stop()
        # cw.run() may still be parked in recv(); it's a daemon thread so we
        # don't block test teardown on it. Best-effort join.
        cw.join(timeout=2)


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
