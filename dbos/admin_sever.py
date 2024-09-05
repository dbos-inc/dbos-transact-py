from __future__ import annotations

import json
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, List, TypedDict

import psutil

from dbos.recovery import _recover_pending_workflows

from .logger import dbos_logger

if TYPE_CHECKING:
    from .dbos import DBOS

health_check_path = "/dbos-healthz"
workflow_recovery_path = "/dbos-workflow-recovery"
perf_path = "/dbos-perf"


class AdminServer:
    def __init__(self, dbos: DBOS, port: int = 3001) -> None:
        self.port = port
        handler = partial(AdminRequestHandler, dbos)
        self.server = ThreadingHTTPServer(("0.0.0.0", port), handler)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True

        dbos_logger.debug("Starting DBOS admin server on port %d", self.port)
        self.server_thread.start()

    def stop(self) -> None:
        dbos_logger.debug("Stopping DBOS admin server")
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join()


class AdminRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, dbos: DBOS, *args: Any, **kwargs: Any) -> None:
        self.dbos = dbos
        super().__init__(*args, **kwargs)

    def _end_headers(self) -> None:
        self.send_header("Content-type", "application/json")
        self.end_headers()

    def do_HEAD(self) -> None:
        self._end_headers()

    def do_GET(self) -> None:
        if self.path == health_check_path:
            self.send_response(200)
            self._end_headers()
            self.wfile.write("healthy".encode("utf-8"))
        elif self.path == perf_path:
            # Compares system CPU times elapsed since last call or module import, returning immediately (non blocking).
            cpu_percent = psutil.cpu_percent(interval=None) / 100.0
            perf_util: PerfUtilization = {
                "idle": 1.0 - cpu_percent,
                "active": cpu_percent,
                "utilization": cpu_percent,
            }
            self.send_response(200)
            self._end_headers()
            self.wfile.write(json.dumps(perf_util).encode("utf-8"))
        else:
            self.send_response(404)
            self._end_headers()

    def do_POST(self) -> None:
        content_length = int(
            self.headers["Content-Length"]
        )  # <--- Gets the size of data
        post_data = self.rfile.read(content_length)  # <--- Gets the data itself

        if self.path == workflow_recovery_path:
            executor_ids: List[str] = json.loads(post_data.decode("utf-8"))
            dbos_logger.info("Recovering workflows for executors: %s", executor_ids)
            workflow_handles = _recover_pending_workflows(self.dbos, executor_ids)
            workflow_ids = [handle.workflow_id for handle in workflow_handles]
            self.send_response(200)
            self._end_headers()
            self.wfile.write(json.dumps(workflow_ids).encode("utf-8"))
        else:
            self.send_response(404)
            self._end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        return  # Disable admin server request logging


# Be consistent with DBOS-TS response.
class PerfUtilization(TypedDict):
    idle: float
    active: float
    utilization: float
