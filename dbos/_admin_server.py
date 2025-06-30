from __future__ import annotations

import json
import re
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypedDict

from dbos._workflow_commands import garbage_collect, global_timeout

from ._conductor import protocol as conductor_protocol
from ._context import SetWorkflowID
from ._error import DBOSException
from ._logger import dbos_logger
from ._recovery import recover_pending_workflows
from ._utils import GlobalParams

if TYPE_CHECKING:
    from ._dbos import DBOS

_health_check_path = "/dbos-healthz"
_workflow_recovery_path = "/dbos-workflow-recovery"
_deactivate_path = "/deactivate"
_workflow_queues_metadata_path = "/dbos-workflow-queues-metadata"
_garbage_collect_path = "/dbos-garbage-collect"
_global_timeout_path = "/dbos-global-timeout"
_queued_workflows_path = "/queues"
_workflows_path = "/workflows"
# /workflows/:workflow_id/cancel
# /workflows/:workflow_id/resume
# /workflows/:workflow_id/restart
# /workflows/:workflow_id/steps
# /workflows/:workflow_id/fork


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
    is_deactivated = False

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
            if not AdminRequestHandler.is_deactivated:
                dbos_logger.info(
                    f"Deactivating DBOS executor {GlobalParams.executor_id} with version {GlobalParams.app_version}. This executor will complete existing workflows but will not create new workflows."
                )
                AdminRequestHandler.is_deactivated = True
            # Stop all event receivers (scheduler and Kafka threads)
            for event in self.dbos.poller_stop_events:
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
                    "workerConcurrency": queue.worker_concurrency,
                    "rateLimit": queue.limiter,
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
            steps_match = re.match(
                r"^/workflows/(?P<workflow_id>[^/]+)/steps$", self.path
            )
            workflow_match = re.match(r"^/workflows/(?P<workflow_id>[^/]+)$", self.path)

            if steps_match:
                workflow_id = steps_match.group("workflow_id")
                self._handle_steps(workflow_id)
            elif workflow_match:
                workflow_id = workflow_match.group("workflow_id")
                workflows = self.dbos.list_workflows(workflow_ids=[workflow_id])
                if not workflows:
                    self.send_response(404)
                    self._end_headers()
                    return
                workflow_output = (
                    conductor_protocol.WorkflowsOutput.from_workflow_information(
                        workflows[0]
                    )
                )
                response_body = json.dumps(workflow_output.__dict__).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response_body)))
                self._end_headers()
                self.wfile.write(response_body)
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
        elif self.path == _workflows_path:
            try:
                filters = json.loads(post_data.decode("utf-8")) if post_data else {}
                self._handle_workflows(filters)
            except (json.JSONDecodeError, AttributeError) as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"error": f"Invalid JSON input: {str(e)}"}).encode(
                        "utf-8"
                    )
                )
        elif self.path == _queued_workflows_path:
            try:
                filters = json.loads(post_data.decode("utf-8")) if post_data else {}
                self._handle_queued_workflows(filters)
            except (json.JSONDecodeError, AttributeError) as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"error": f"Invalid JSON input: {str(e)}"}).encode(
                        "utf-8"
                    )
                )
        elif self.path == _garbage_collect_path:
            inputs = json.loads(post_data.decode("utf-8"))
            cutoff_epoch_timestamp_ms = inputs.get("cutoff_epoch_timestamp_ms", None)
            rows_threshold = inputs.get("rows_threshold", None)
            garbage_collect(
                self.dbos,
                cutoff_epoch_timestamp_ms=cutoff_epoch_timestamp_ms,
                rows_threshold=rows_threshold,
            )
            self.send_response(204)
            self._end_headers()
        elif self.path == _global_timeout_path:
            inputs = json.loads(post_data.decode("utf-8"))
            cutoff_epoch_timestamp_ms = inputs.get("cutoff_epoch_timestamp_ms", None)
            global_timeout(self.dbos, cutoff_epoch_timestamp_ms)
            self.send_response(204)
            self._end_headers()
        else:
            restart_match = re.match(
                r"^/workflows/(?P<workflow_id>[^/]+)/restart$", self.path
            )
            fork_match = re.match(
                r"^/workflows/(?P<workflow_id>[^/]+)/fork$", self.path
            )
            resume_match = re.match(
                r"^/workflows/(?P<workflow_id>[^/]+)/resume$", self.path
            )
            cancel_match = re.match(
                r"^/workflows/(?P<workflow_id>[^/]+)/cancel$", self.path
            )

            if fork_match:
                workflow_id = fork_match.group("workflow_id")
                try:
                    data = json.loads(post_data.decode("utf-8"))
                    start_step: int = data.get("start_step", 1)
                    new_workflow_id: Optional[str] = data.get("new_workflow_id")
                    application_version: Optional[str] = data.get("application_version")
                    self._handle_fork(
                        workflow_id, start_step, new_workflow_id, application_version
                    )
                except (json.JSONDecodeError, AttributeError) as e:
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps({"error": f"Invalid JSON input: {str(e)}"}).encode(
                            "utf-8"
                        )
                    )
                    return
            elif restart_match:
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
        try:
            print(f"Restarting workflow {workflow_id}")
            handle = self.dbos.restart_workflow(workflow_id)
            response_body = json.dumps(
                {
                    "workflow_id": handle.workflow_id,
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response_body)))
            self._end_headers()
            self.wfile.write(response_body)
        except DBOSException as e:
            print(f"Error restarting workflow: {e}")
            self.send_response(500)
            response_body = json.dumps({"error": str(e)}).encode("utf-8")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)

    def _handle_fork(
        self,
        workflow_id: str,
        start_step: int,
        new_workflow_id: Optional[str],
        application_version: Optional[str],
    ) -> None:
        try:
            print(f"Forking workflow {workflow_id} from step {start_step}")
            if new_workflow_id is not None:
                with SetWorkflowID(new_workflow_id):
                    handle = self.dbos.fork_workflow(
                        workflow_id, start_step, application_version=application_version
                    )
            else:
                handle = self.dbos.fork_workflow(
                    workflow_id, start_step, application_version=application_version
                )
            response_body = json.dumps(
                {
                    "workflow_id": handle.workflow_id,
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response_body)))
            self._end_headers()
            self.wfile.write(response_body)
        except DBOSException as e:
            print(f"Error forking workflow: {e}")
            self.send_response(500)
            response_body = json.dumps({"error": str(e)}).encode("utf-8")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)

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

    def _handle_steps(self, workflow_id: str) -> None:
        steps = self.dbos.list_workflow_steps(workflow_id)

        updated_steps = [
            {
                **step,
                "output": str(step["output"]) if step["output"] is not None else None,
                "error": str(step["error"]) if step["error"] is not None else None,
            }
            for step in steps
        ]

        json_steps = json.dumps(updated_steps).encode("utf-8")
        self.send_response(200)
        self._end_headers()
        self.wfile.write(json_steps)

    def _handle_workflows(self, filters: Dict[str, Any]) -> None:
        workflows = self.dbos.list_workflows(
            workflow_ids=filters.get("workflow_uuids"),
            user=filters.get("authenticated_user"),
            start_time=filters.get("start_time"),
            end_time=filters.get("end_time"),
            status=filters.get("status"),
            app_version=filters.get("application_version"),
            name=filters.get("workflow_name"),
            limit=filters.get("limit"),
            offset=filters.get("offset"),
            sort_desc=filters.get("sort_desc", False),
            workflow_id_prefix=filters.get("workflow_id_prefix"),
            load_input=filters.get("load_input", False),
            load_output=filters.get("load_output", False),
        )
        workflows_output = [
            conductor_protocol.WorkflowsOutput.from_workflow_information(i)
            for i in workflows
        ]
        response_body = json.dumps(
            [workflow.__dict__ for workflow in workflows_output]
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_body)))
        self._end_headers()
        self.wfile.write(response_body)

    def _handle_queued_workflows(self, filters: Dict[str, Any]) -> None:
        workflows = self.dbos.list_queued_workflows(
            start_time=filters.get("start_time"),
            end_time=filters.get("end_time"),
            status=filters.get("status"),
            name=filters.get("workflow_name"),
            limit=filters.get("limit"),
            offset=filters.get("offset"),
            queue_name=filters.get("queue_name"),
            sort_desc=filters.get("sort_desc", False),
            load_input=filters.get("load_input", False),
        )
        workflows_output = [
            conductor_protocol.WorkflowsOutput.from_workflow_information(i)
            for i in workflows
        ]
        response_body = json.dumps(
            [workflow.__dict__ for workflow in workflows_output]
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_body)))
        self._end_headers()
        self.wfile.write(response_body)


# Be consistent with DBOS-TS response.
class PerfUtilization(TypedDict):
    idle: float
    active: float
    utilization: float
