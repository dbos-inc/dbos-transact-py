import threading
import time
import traceback
import urllib.parse
from typing import TYPE_CHECKING, Optional

from websockets import ConnectionClosed, ConnectionClosedOK
from websockets.sync.client import ClientConnection, connect

from dbos._utils import GlobalParams
from dbos._workflow_commands import list_queued_workflows, list_workflows

from . import protocol as p

if TYPE_CHECKING:
    from dbos import DBOS


class ConductorWebsocket(threading.Thread):

    def __init__(
        self, dbos: "DBOS", conductor_url: str, token: str, evt: threading.Event
    ):
        super().__init__(daemon=True)
        self.websocket: Optional[ClientConnection] = None
        self.evt = evt
        self.dbos = dbos
        self.app_name = dbos.config["name"]
        self.url = conductor_url.rstrip("/") + f"/websocket/{self.app_name}/{token}"

    def run(self) -> None:
        while not self.evt.is_set():
            try:
                with connect(self.url) as self.websocket:
                    message = self.websocket.recv()
                    if not isinstance(message, str):
                        self.dbos.logger.warning("Receieved unexpected non-str message")
                        continue
                    base_message = p.BaseMessage.from_json(message)
                    type = base_message.type
                    if type == p.MessageType.EXECUTOR_INFO.value:
                        info_response = p.ExecutorInfoResponse(
                            type=p.MessageType.EXECUTOR_INFO,
                            request_id=base_message.request_id,
                            executor_id=GlobalParams.executor_id,
                            application_version=GlobalParams.app_version,
                        )
                        self.websocket.send(info_response.to_json())
                        self.dbos.logger.info("Connected to DBOS conductor")
                    elif type == p.MessageType.RECOVERY:
                        recovery_message = p.RecoveryRequest.from_json(message)
                        success = True
                        try:
                            self.dbos.recover_pending_workflows(
                                recovery_message.executor_ids
                            )
                        except Exception as e:
                            self.dbos.logger.error(
                                f"Exception encountered when recovering workflows: {traceback.format_exc()}"
                            )
                            success = False
                        recovery_response = p.RecoveryResponse(
                            type=p.MessageType.RECOVERY,
                            request_id=base_message.request_id,
                            success=success,
                        )
                        self.websocket.send(recovery_response.to_json())
                    elif type == p.MessageType.CANCEL:
                        cancel_message = p.CancelRequest.from_json(message)
                        success = True
                        try:
                            self.dbos.cancel_workflow(cancel_message.workflow_id)
                        except Exception as e:
                            self.dbos.logger.error(
                                f"Exception encountered when cancelling workflow {cancel_message.workflow_id}: {traceback.format_exc()}"
                            )
                            success = False
                        cancel_response = p.CancelResponse(
                            type=p.MessageType.CANCEL,
                            request_id=base_message.request_id,
                            success=success,
                        )
                        self.websocket.send(cancel_response.to_json())
                    elif type == p.MessageType.RESUME:
                        resume_message = p.ResumeRequest.from_json(message)
                        success = True
                        try:
                            self.dbos.resume_workflow(resume_message.workflow_id)
                        except Exception as e:
                            self.dbos.logger.error(
                                f"Exception encountered when resuming workflow {resume_message.workflow_id}: {traceback.format_exc()}"
                            )
                            success = False
                        resume_response = p.ResumeResponse(
                            type=p.MessageType.RESUME,
                            request_id=base_message.request_id,
                            success=success,
                        )
                        self.websocket.send(resume_response.to_json())
                    elif type == p.MessageType.RESTART:
                        restart_message = p.RestartRequest.from_json(message)
                        success = True
                        try:
                            self.dbos.restart_workflow(restart_message.workflow_id)
                        except Exception as e:
                            self.dbos.logger.error(
                                f"Exception encountered when restarting workflow {restart_message.workflow_id}: {traceback.format_exc()}"
                            )
                            success = False
                        restart_response = p.RestartResponse(
                            type=p.MessageType.RESTART,
                            request_id=base_message.request_id,
                            success=success,
                        )
                        self.websocket.send(restart_response.to_json())
                    elif type == p.MessageType.LIST_WORKFLOWS:
                        list_workflows_message = p.ListWorkflowsRequest.from_json(
                            message
                        )
                        b = list_workflows_message.body
                        infos = list_workflows(
                            self.dbos._sys_db,
                            workflow_ids=b["workflow_uuids"],
                            user=b["authenticated_user"],
                            start_time=b["start_time"],
                            end_time=b["end_time"],
                            status=b["status"],
                            request=False,
                            app_version=b["application_version"],
                            name=b["workflow_name"],
                            limit=b["limit"],
                            offset=b["offset"],
                            sort_desc=b["sort_desc"],
                        )
                        list_workflows_response = p.ListWorkflowsResponse(
                            type=p.MessageType.LIST_WORKFLOWS,
                            request_id=base_message.request_id,
                            output=[
                                p.WorkflowsOutput.from_workflow_information(i)
                                for i in infos
                            ],
                        )
                        self.websocket.send(list_workflows_response.to_json())
                    elif type == p.MessageType.LIST_QUEUED_WORKFLOWS:
                        list_queued_workflows_message = (
                            p.ListQueuedWorkflowsRequest.from_json(message)
                        )
                        q = list_queued_workflows_message.body
                        infos = list_queued_workflows(
                            self.dbos._sys_db,
                            start_time=q["start_time"],
                            end_time=q["end_time"],
                            status=q["status"],
                            request=False,
                            name=q["workflow_name"],
                            limit=q["limit"],
                            offset=q["offset"],
                            queue_name=q["queue_name"],
                            sort_desc=q["sort_desc"],
                        )
                        list_queued_workflows_response = p.ListQueuedWorkflowsResponse(
                            type=p.MessageType.LIST_QUEUED_WORKFLOWS,
                            request_id=base_message.request_id,
                            output=[
                                p.WorkflowsOutput.from_workflow_information(i)
                                for i in infos
                            ],
                        )
                        self.websocket.send(list_queued_workflows_response.to_json())
                    else:
                        self.dbos.logger.warning(f"Unexpected message type: {type}")
            except ConnectionClosedOK:
                self.dbos.logger.info("Conductor connection terminated")
                break
            except ConnectionClosed as e:
                self.dbos.logger.warning(
                    f"Connection to conductor lost. Reconnecting: {e}"
                )
                time.sleep(1)
                continue
            except Exception as e:
                self.dbos.logger.error(
                    f"Unexpected exception in connection to conductor: {e}"
                )
                time.sleep(1)
                continue
