import threading
import time
import urllib.parse
from typing import TYPE_CHECKING, Optional

from websockets import ConnectionClosed, ConnectionClosedOK
from websockets.sync.client import ClientConnection, connect

from dbos._utils import GlobalParams

from . import protocol as p

if TYPE_CHECKING:
    from dbos import DBOS


class ConductorWebsocket(threading.Thread):

    def __init__(self, dbos: "DBOS", host: str, evt: threading.Event):
        super().__init__(daemon=True)
        self.websocket: Optional[ClientConnection] = None
        self.evt = evt
        self.dbos = dbos
        self.org_id = "default"
        self.app_name = dbos.config["name"]
        self.url = urllib.parse.urljoin(
            host, f"/{self.org_id}/applications/{self.app_name}/websocket"
        )

    def run(self) -> None:
        while not self.evt.is_set():
            try:
                self.websocket = connect(self.url)
                while not self.evt.is_set():
                    message = self.websocket.recv()
                    if not isinstance(message, str):
                        self.dbos.logger.warning("Receieved unexpected non-str message")
                        continue
                    base_message = p.BaseMessage.from_json(message)
                    type = base_message.type
                    if type == p.MessageType.EXECUTOR_INFO.value:
                        info_response = p.ExecutorInfoResponse(
                            type=p.MessageType.EXECUTOR_INFO,
                            executor_id=GlobalParams.executor_id,
                            application_version=GlobalParams.app_version,
                        )
                        self.websocket.send(info_response.to_json())
                        self.dbos.logger.info("Connected to DBOS conductor")
                    elif type == p.MessageType.RECOVERY:
                        recovery_message = p.RecoveryRequest.from_json(message)
                        print(recovery_message)
                        recovery_response = p.RecoveryResponse(
                            type=p.MessageType.RECOVERY, success=True
                        )
                        self.websocket.send(recovery_response.to_json())
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
