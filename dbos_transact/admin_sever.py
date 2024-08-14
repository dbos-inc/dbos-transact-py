from __future__ import annotations

import json
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, List

from .logger import dbos_logger

if TYPE_CHECKING:
    from .dbos import DBOS

health_check_path = "/dbos-healthz"
workflow_recovery_path = "/dbos-workflow-recovery"


class AdminServer:
    def __init__(self, dbos: DBOS, port: int = 3001) -> None:
        self.port = port
        handler = partial(AdminRequestHandler, dbos)
        self.server = ThreadingHTTPServer(("0.0.0.0", port), handler)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True

        dbos_logger.info("Starting DBOS admin server on port %d", self.port)
        self.server_thread.start()

    def stop(self) -> None:
        dbos_logger.info("Stopping DBOS admin server")
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
            workflow_handles = self.dbos.recover_pending_workflows(executor_ids)
            workflow_uuids = [handle.workflow_uuid for handle in workflow_handles]
            self.send_response(200)
            self._end_headers()
            self.wfile.write(json.dumps(workflow_uuids).encode("utf-8"))
        else:
            self.send_response(404)
            self._end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        return  # Disable admin server request logging
