from __future__ import annotations

import json
import re
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, List, TypedDict

from ._logger import dbos_logger
from ._recovery import recover_pending_workflows

if TYPE_CHECKING:
    from ._dbos import DBOS

_health_check_path = "/dbos-healthz"
_workflow_recovery_path = "/dbos-workflow-recovery"
_deactivate_path = "/deactivate"
_workflow_queues_metadata_path = "/dbos-workflow-queues-metadata"
# /workflows/:workflow_id/cancel
# /workflows/:workflow_id/resume
# /workflows/:workflow_id/restart


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
        if self.path == _health_check_path:
            self.send_response(200)
            self._end_headers()
            self.wfile.write("healthy".encode("utf-8"))
        elif self.path == _deactivate_path:
            # Stop all scheduled workflows, queues, and kafka loops
            for event in self.dbos.stop_events:
                event.set()

            self.send_response(200)
            self._end_headers()
            self.wfile.write("deactivated".encode("utf-8"))
        elif self.path == _workflow_queues_metadata_path:
            queue_metadata_array = []
            from ._dbos import _get_or_create_dbos_registry

            registry = _get_or_create_dbos_registry()
            for queue in registry.queue_info_map.values():
                queue_metadata = {
                    "name": queue.name,
                    "concurrency": queue.concurrency,
                    "worker_concurrency": queue.worker_concurrency,
                    "limiter": queue.limiter,
                }
                # Remove keys with None values
                queue_metadata = {
                    k: v for k, v in queue_metadata.items() if v is not None
                }
                queue_metadata_array.append(queue_metadata)
            self.send_response(200)
            self._end_headers()
            self.wfile.write(json.dumps(queue_metadata_array).encode("utf-8"))
        else:
            self.send_response(404)
            self._end_headers()

    def do_POST(self) -> None:
        content_length = int(
            self.headers["Content-Length"]
        )  # <--- Gets the size of data
        post_data = self.rfile.read(content_length)  # <--- Gets the data itself

        if self.path == _workflow_recovery_path:
            executor_ids: List[str] = json.loads(post_data.decode("utf-8"))
            dbos_logger.info("Recovering workflows for executors: %s", executor_ids)
            workflow_handles = recover_pending_workflows(self.dbos, executor_ids)
            workflow_ids = [handle.workflow_id for handle in workflow_handles]
            self.send_response(200)
            self._end_headers()
            self.wfile.write(json.dumps(workflow_ids).encode("utf-8"))
        else:

            restart_match = re.match(
                r"^/workflows/(?P<workflow_id>[^/]+)/restart$", self.path
            )
            resume_match = re.match(
                r"^/workflows/(?P<workflow_id>[^/]+)/resume$", self.path
            )
            cancel_match = re.match(
                r"^/workflows/(?P<workflow_id>[^/]+)/cancel$", self.path
            )

            if restart_match:
                workflow_id = restart_match.group("workflow_id")
                self._handle_restart(workflow_id)
            elif resume_match:
                workflow_id = resume_match.group("workflow_id")
                self._handle_resume(workflow_id)
            elif cancel_match:
                workflow_id = cancel_match.group("workflow_id")
                self._handle_cancel(workflow_id)
            else:
                self.send_response(404)
                self._end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        return  # Disable admin server request logging

    def _handle_restart(self, workflow_id: str) -> None:
        self.dbos.restart_workflow(workflow_id)
        print("Restarting workflow", workflow_id)
        self.send_response(204)
        self._end_headers()

    def _handle_resume(self, workflow_id: str) -> None:
        print("Resuming workflow", workflow_id)
        self.dbos.resume_workflow(workflow_id)
        self.send_response(204)
        self._end_headers()

    def _handle_cancel(self, workflow_id: str) -> None:
        print("Cancelling workflow", workflow_id)
        self.dbos.cancel_workflow(workflow_id)
        self.send_response(204)
        self._end_headers()


# Be consistent with DBOS-TS response.
class PerfUtilization(TypedDict):
    idle: float
    active: float
    utilization: float
