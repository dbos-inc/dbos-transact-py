import socket
import threading
import time
import traceback
import uuid
from importlib.metadata import version
from typing import TYPE_CHECKING, Optional

from websockets import ConnectionClosed, ConnectionClosedOK, InvalidStatus
from websockets.sync.client import connect
from websockets.sync.connection import Connection

from dbos._context import SetWorkflowID
from dbos._utils import GlobalParams
from dbos._workflow_commands import (
    garbage_collect,
    get_workflow,
    global_timeout,
    list_queued_workflows,
    list_workflow_steps,
    list_workflows,
)

from . import protocol as p

if TYPE_CHECKING:
    from dbos import DBOS

ws_version = version("websockets")
use_keepalive = ws_version < "15.0"


class ConductorWebsocket(threading.Thread):

    def __init__(
        self, dbos: "DBOS", conductor_url: str, conductor_key: str, evt: threading.Event
    ):
        super().__init__(daemon=True)
        self.websocket: Optional[Connection] = None
        self.evt = evt
        self.dbos = dbos
        self.app_name = dbos._config["name"]
        self.url = (
            conductor_url.rstrip("/") + f"/websocket/{self.app_name}/{conductor_key}"
        )
        # TODO: once we can upgrade to websockets>=15.0, we can always use the built-in keepalive
        self.ping_interval = 20  # Time between pings in seconds
        self.ping_timeout = 15  # Time to wait for a pong response in seconds
        self.keepalive_thread: Optional[threading.Thread] = None
        self.pong_event: Optional[threading.Event] = None
        self.last_ping_time = -1.0

        self.dbos.logger.debug(
            f"Connecting to conductor at {self.url} using websockets version {ws_version}"
        )

    def keepalive(self) -> None:
        self.dbos.logger.debug("Starting keepalive thread")
        while not self.evt.is_set():
            if self.websocket is None or self.websocket.close_code is not None:
                time.sleep(1)
                continue
            try:
                self.last_ping_time = time.time()
                self.pong_event = self.websocket.ping()
                self.dbos.logger.debug("> Sent ping to conductor")
                pong_result = self.pong_event.wait(self.ping_timeout)
                elapsed_time = time.time() - self.last_ping_time
                if not pong_result:
                    self.dbos.logger.warning(
                        f"Failed to receive pong from conductor after {elapsed_time:.2f} seconds. Reconnecting."
                    )
                    self.websocket.close()
                    continue
                # Wait for the next ping interval
                self.dbos.logger.debug(
                    f"< Received pong from conductor after {elapsed_time:.2f} seconds"
                )
                wait_time = self.ping_interval - elapsed_time
                self.evt.wait(max(0, wait_time))
            except ConnectionClosed:
                # The main loop will try to reconnect
                self.dbos.logger.debug("Connection to conductor closed.")
            except Exception as e:
                self.dbos.logger.warning(f"Failed to send ping to conductor: {e}.")
                self.websocket.close()

    def run(self) -> None:
        while not self.evt.is_set():
            try:
                with connect(
                    self.url,
                    open_timeout=5,
                    close_timeout=5,
                    logger=self.dbos.logger,
                ) as websocket:
                    self.websocket = websocket
                    if use_keepalive and self.keepalive_thread is None:
                        self.keepalive_thread = threading.Thread(
                            target=self.keepalive,
                            daemon=True,
                        )
                        self.keepalive_thread.start()
                    while not self.evt.is_set():
                        message = websocket.recv()
                        if not isinstance(message, str):
                            self.dbos.logger.warning(
                                "Received unexpected non-str message"
                            )
                            continue
                        base_message = p.BaseMessage.from_json(message)
                        msg_type = base_message.type
                        error_message = None
                        if msg_type == p.MessageType.EXECUTOR_INFO:
                            info_response = p.ExecutorInfoResponse(
                                type=p.MessageType.EXECUTOR_INFO,
                                request_id=base_message.request_id,
                                executor_id=GlobalParams.executor_id,
                                application_version=GlobalParams.app_version,
                                hostname=socket.gethostname(),
                            )
                            websocket.send(info_response.to_json())
                            self.dbos.logger.info("Connected to DBOS conductor")
                        elif msg_type == p.MessageType.RECOVERY:
                            recovery_message = p.RecoveryRequest.from_json(message)
                            success = True
                            try:
                                self.dbos._recover_pending_workflows(
                                    recovery_message.executor_ids
                                )
                            except Exception as e:
                                error_message = f"Exception encountered when recovering workflows: {traceback.format_exc()}"
                                self.dbos.logger.error(error_message)
                                success = False
                            recovery_response = p.RecoveryResponse(
                                type=p.MessageType.RECOVERY,
                                request_id=base_message.request_id,
                                success=success,
                                error_message=error_message,
                            )
                            websocket.send(recovery_response.to_json())
                        elif msg_type == p.MessageType.CANCEL:
                            cancel_message = p.CancelRequest.from_json(message)
                            success = True
                            try:
                                self.dbos.cancel_workflow(cancel_message.workflow_id)
                            except Exception as e:
                                error_message = f"Exception encountered when cancelling workflow {cancel_message.workflow_id}: {traceback.format_exc()}"
                                self.dbos.logger.error(error_message)
                                success = False
                            cancel_response = p.CancelResponse(
                                type=p.MessageType.CANCEL,
                                request_id=base_message.request_id,
                                success=success,
                                error_message=error_message,
                            )
                            websocket.send(cancel_response.to_json())
                        elif msg_type == p.MessageType.RESUME:
                            resume_message = p.ResumeRequest.from_json(message)
                            success = True
                            try:
                                self.dbos.resume_workflow(resume_message.workflow_id)
                            except Exception as e:
                                error_message = f"Exception encountered when resuming workflow {resume_message.workflow_id}: {traceback.format_exc()}"
                                self.dbos.logger.error(error_message)
                                success = False
                            resume_response = p.ResumeResponse(
                                type=p.MessageType.RESUME,
                                request_id=base_message.request_id,
                                success=success,
                                error_message=error_message,
                            )
                            websocket.send(resume_response.to_json())
                        elif msg_type == p.MessageType.RESTART:
                            # TODO: deprecate this message type in favor of Fork
                            restart_message = p.RestartRequest.from_json(message)
                            success = True
                            try:
                                self.dbos.fork_workflow(restart_message.workflow_id, 1)
                            except Exception as e:
                                error_message = f"Exception encountered when restarting workflow {restart_message.workflow_id}: {traceback.format_exc()}"
                                self.dbos.logger.error(error_message)
                                success = False
                            restart_response = p.RestartResponse(
                                type=p.MessageType.RESTART,
                                request_id=base_message.request_id,
                                success=success,
                                error_message=error_message,
                            )
                            websocket.send(restart_response.to_json())
                        elif msg_type == p.MessageType.FORK_WORKFLOW:
                            fork_message = p.ForkWorkflowRequest.from_json(message)
                            new_workflow_id = fork_message.body["new_workflow_id"]
                            if new_workflow_id is None:
                                new_workflow_id = str(uuid.uuid4())
                            workflow_id = fork_message.body["workflow_id"]
                            start_step = fork_message.body["start_step"]
                            app_version = fork_message.body["application_version"]
                            try:
                                with SetWorkflowID(new_workflow_id):
                                    new_handle = self.dbos.fork_workflow(
                                        workflow_id,
                                        start_step,
                                        application_version=app_version,
                                    )
                                new_workflow_id = new_handle.workflow_id
                            except Exception as e:
                                error_message = f"Exception encountered when forking workflow {workflow_id} to new workflow {new_workflow_id} on step {start_step}, app version {app_version}: {traceback.format_exc()}"
                                self.dbos.logger.error(error_message)
                                new_workflow_id = None

                            fork_response = p.ForkWorkflowResponse(
                                type=p.MessageType.FORK_WORKFLOW,
                                request_id=base_message.request_id,
                                new_workflow_id=new_workflow_id,
                                error_message=error_message,
                            )
                            websocket.send(fork_response.to_json())
                        elif msg_type == p.MessageType.LIST_WORKFLOWS:
                            list_workflows_message = p.ListWorkflowsRequest.from_json(
                                message
                            )
                            body = list_workflows_message.body
                            infos = []
                            try:
                                load_input = body.get("load_input", False)
                                load_output = body.get("load_output", False)
                                infos = list_workflows(
                                    self.dbos._sys_db,
                                    workflow_ids=body["workflow_uuids"],
                                    user=body["authenticated_user"],
                                    start_time=body["start_time"],
                                    end_time=body["end_time"],
                                    status=body["status"],
                                    app_version=body["application_version"],
                                    name=body["workflow_name"],
                                    limit=body["limit"],
                                    offset=body["offset"],
                                    sort_desc=body["sort_desc"],
                                    load_input=load_input,
                                    load_output=load_output,
                                )
                            except Exception as e:
                                error_message = f"Exception encountered when listing workflows: {traceback.format_exc()}"
                                self.dbos.logger.error(error_message)

                            list_workflows_response = p.ListWorkflowsResponse(
                                type=p.MessageType.LIST_WORKFLOWS,
                                request_id=base_message.request_id,
                                output=[
                                    p.WorkflowsOutput.from_workflow_information(i)
                                    for i in infos
                                ],
                                error_message=error_message,
                            )
                            websocket.send(list_workflows_response.to_json())
                        elif msg_type == p.MessageType.LIST_QUEUED_WORKFLOWS:
                            list_queued_workflows_message = (
                                p.ListQueuedWorkflowsRequest.from_json(message)
                            )
                            q_body = list_queued_workflows_message.body
                            infos = []
                            try:
                                q_load_input = q_body.get("load_input", False)
                                infos = list_queued_workflows(
                                    self.dbos._sys_db,
                                    start_time=q_body["start_time"],
                                    end_time=q_body["end_time"],
                                    status=q_body["status"],
                                    name=q_body["workflow_name"],
                                    limit=q_body["limit"],
                                    offset=q_body["offset"],
                                    queue_name=q_body["queue_name"],
                                    sort_desc=q_body["sort_desc"],
                                    load_input=q_load_input,
                                )
                            except Exception as e:
                                error_message = f"Exception encountered when listing queued workflows: {traceback.format_exc()}"
                                self.dbos.logger.error(error_message)

                            list_queued_workflows_response = (
                                p.ListQueuedWorkflowsResponse(
                                    type=p.MessageType.LIST_QUEUED_WORKFLOWS,
                                    request_id=base_message.request_id,
                                    output=[
                                        p.WorkflowsOutput.from_workflow_information(i)
                                        for i in infos
                                    ],
                                    error_message=error_message,
                                )
                            )
                            websocket.send(list_queued_workflows_response.to_json())
                        elif msg_type == p.MessageType.GET_WORKFLOW:
                            get_workflow_message = p.GetWorkflowRequest.from_json(
                                message
                            )
                            info = None
                            try:
                                info = get_workflow(
                                    self.dbos._sys_db, get_workflow_message.workflow_id
                                )
                            except Exception as e:
                                error_message = f"Exception encountered when getting workflow {get_workflow_message.workflow_id}: {traceback.format_exc()}"
                                self.dbos.logger.error(error_message)

                            get_workflow_response = p.GetWorkflowResponse(
                                type=p.MessageType.GET_WORKFLOW,
                                request_id=base_message.request_id,
                                output=(
                                    p.WorkflowsOutput.from_workflow_information(info)
                                    if info is not None
                                    else None
                                ),
                                error_message=error_message,
                            )
                            websocket.send(get_workflow_response.to_json())
                        elif msg_type == p.MessageType.EXIST_PENDING_WORKFLOWS:
                            exist_pending_workflows_message = (
                                p.ExistPendingWorkflowsRequest.from_json(message)
                            )
                            pending_wfs = []
                            try:
                                pending_wfs = self.dbos._sys_db.get_pending_workflows(
                                    exist_pending_workflows_message.executor_id,
                                    exist_pending_workflows_message.application_version,
                                )
                            except Exception as e:
                                error_message = f"Exception encountered when checking for pending workflows: {traceback.format_exc()}"
                                self.dbos.logger.error(error_message)

                            exist_pending_workflows_response = (
                                p.ExistPendingWorkflowsResponse(
                                    type=p.MessageType.EXIST_PENDING_WORKFLOWS,
                                    request_id=base_message.request_id,
                                    exist=len(pending_wfs) > 0,
                                    error_message=error_message,
                                )
                            )
                            websocket.send(exist_pending_workflows_response.to_json())
                        elif msg_type == p.MessageType.LIST_STEPS:
                            list_steps_message = p.ListStepsRequest.from_json(message)
                            step_info = None
                            try:
                                step_info = list_workflow_steps(
                                    self.dbos._sys_db,
                                    self.dbos._app_db,
                                    list_steps_message.workflow_id,
                                )
                            except Exception as e:
                                error_message = f"Exception encountered when getting workflow {list_steps_message.workflow_id}: {traceback.format_exc()}"
                                self.dbos.logger.error(error_message)

                            list_steps_response = p.ListStepsResponse(
                                type=p.MessageType.LIST_STEPS,
                                request_id=base_message.request_id,
                                output=(
                                    [
                                        p.WorkflowSteps.from_step_info(i)
                                        for i in step_info
                                    ]
                                    if step_info is not None
                                    else None
                                ),
                                error_message=error_message,
                            )
                            websocket.send(list_steps_response.to_json())
                        elif msg_type == p.MessageType.RETENTION:
                            retention_message = p.RetentionRequest.from_json(message)
                            success = True
                            try:
                                garbage_collect(
                                    self.dbos,
                                    cutoff_epoch_timestamp_ms=retention_message.body[
                                        "gc_cutoff_epoch_ms"
                                    ],
                                    rows_threshold=retention_message.body[
                                        "gc_rows_threshold"
                                    ],
                                )
                                if (
                                    retention_message.body["timeout_cutoff_epoch_ms"]
                                    is not None
                                ):
                                    global_timeout(
                                        self.dbos,
                                        retention_message.body[
                                            "timeout_cutoff_epoch_ms"
                                        ],
                                    )
                            except Exception as e:
                                error_message = f"Exception encountered during enforcing retention policy: {traceback.format_exc()}"
                                self.dbos.logger.error(error_message)
                                success = False

                            retention_response = p.RetentionResponse(
                                type=p.MessageType.RETENTION,
                                request_id=base_message.request_id,
                                success=success,
                                error_message=error_message,
                            )
                            websocket.send(retention_response.to_json())
                        else:
                            self.dbos.logger.warning(
                                f"Unexpected message type: {msg_type}"
                            )
                            unknown_message = p.BaseResponse(
                                request_id=base_message.request_id,
                                type=msg_type,
                                error_message="Unknown message type",
                            )
                            # Still need to send a response to the conductor
                            websocket.send(unknown_message.to_json())
            except ConnectionClosedOK:
                if self.evt.is_set():
                    self.dbos.logger.info("Conductor connection terminated")
                    break
                # Otherwise, we are trying to reconnect
                self.dbos.logger.warning(
                    "Connection to conductor lost. Reconnecting..."
                )
                time.sleep(1)
                continue
            except ConnectionClosed as e:
                self.dbos.logger.warning(
                    f"Connection to conductor lost. Reconnecting: {e}"
                )
                time.sleep(1)
                continue
            except InvalidStatus as e:
                # This happens when it cannot open a connection to the conductor. E.g., the conductor rejects the request
                json_data = e.response.body.decode("utf-8")
                self.dbos.logger.error(
                    f"Failed to connect to conductor. Retrying: {str(e) }. Details: {json_data}"
                )
                time.sleep(1)
                continue
            except Exception as e:
                self.dbos.logger.error(
                    f"Unexpected exception in connection to conductor. Reconnecting: {e}"
                )
                time.sleep(1)
                continue

        # Wait for the keepalive thread to finish
        if self.keepalive_thread is not None:
            if self.pong_event is not None:
                self.pong_event.set()
            self.keepalive_thread.join()
