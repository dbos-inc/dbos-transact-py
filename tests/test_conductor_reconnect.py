"""
Regression test for the conductor websocket reconnect deadlock.

Bug: when the websockets library's built-in keepalive fires (websockets >= 15.0)
and the close handshake also times out (network is wedged so the kernel never
sees a FIN/RST, and the library's forced socket close fails to unblock the
recv_events thread), the user-level `websocket.recv()` in
`ConductorWebsocket.run()` stays blocked indefinitely and the reconnect loop
never iterates.

The test reproduces this deterministically by:
  1. Standing up a raw-TCP server on localhost that performs the WebSocket
     upgrade handshake by hand, sends an EXECUTOR_INFO request, reads the
     response, and then black-holes — never reads or writes again, never
     replies to pings, never sends a close frame.
  2. Neutralising `websockets.sync.connection.Connection.close_socket` so
     that, after the keepalive ping timeout fires and the close handshake
     times out, the library's forced socket teardown is a no-op. This
     simulates the wedged-TCP condition the production incident hit, where
     `socket.shutdown(SHUT_RDWR)` + `socket.close()` do not propagate as
     a clean close to the recv_events thread.
  3. Driving `ConductorWebsocket.run()` against that server with a stub
     `dbos` and small `ping_interval`/`ping_timeout`/`close_timeout` so the
     keepalive sequence completes well within the test window.
  4. Asserting that the conductor logs the "Connection to conductor lost.
     Reconnecting" warning within a generous window. On main the assertion
     fails because `websocket.recv()` at conductor.py:108 stays blocked.
     Once the bug is fixed (e.g. DBOS runs its own keepalive thread, or
     uses `recv(timeout=…)` so it can poll `websocket.close_code`), the
     warning fires and the test passes.
"""

import base64
import hashlib
import logging
import socket
import struct
import threading
import time
from importlib.metadata import version
from typing import Any, List, Optional

import pytest
from websockets.sync import connection as ws_connection
from websockets.sync.client import connect as _real_connect

from dbos._conductor import conductor as conductor_module
from dbos._conductor import protocol as p

WS_VERSION = version("websockets")

pytestmark = pytest.mark.skipif(
    WS_VERSION < "15.0",
    reason=(
        "The deadlock is only reachable on websockets>=15.0, where DBOS "
        "relies on the library's built-in keepalive (use_keepalive=False)."
    ),
)


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
            t = threading.Thread(
                target=self._handle, args=(conn,), daemon=True
            )
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
        self._conductor_executor_metadata: Optional[dict] = None


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


def test_conductor_reconnects_after_keepalive_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the websockets library's keepalive fires and the close handshake
    times out, the conductor's run() loop must still escape recv() and try
    to reconnect. On main this fails because recv() stays blocked."""

    # Force the close window to be small so the test is quick. We do NOT
    # touch ping_interval here — the conductor (post-fix) disables the
    # library's keepalive by passing ping_interval=None, and we want to
    # exercise that path. DBOS's own keepalive is shortened via
    # cw.ping_interval / cw.ping_timeout below.
    def fast_connect(*args: Any, **kwargs: Any) -> Any:
        kwargs["close_timeout"] = 1.0
        return _real_connect(*args, **kwargs)

    monkeypatch.setattr(conductor_module, "connect", fast_connect)

    # Simulate the production wedge: when the keepalive's close handshake
    # times out, the websockets library's last resort is `close_socket()`,
    # which does shutdown(SHUT_RDWR) + close() to unblock recv_events. In
    # the prod incident the underlying socket was not cleanly torn down,
    # so that path failed to wake recv_events. We force the same condition
    # by neutralising `close_socket` — exposing the conductor's reliance
    # on the library's forced-close working.
    monkeypatch.setattr(
        ws_connection.Connection, "close_socket", lambda self: None
    )

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
        conductor_url=f"ws://127.0.0.1:{server.port}",
        conductor_key="test-key",
        evt=evt,
    )
    # Shrink DBOS's own keepalive intervals so the pong-timeout warning fires
    # well within the test window. Defaults are 20s / 15s.
    cw.ping_interval = 0.5
    cw.ping_timeout = 0.5
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
