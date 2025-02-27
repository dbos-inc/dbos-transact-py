import threading
import time
import urllib.parse
from typing import TYPE_CHECKING, Optional

from websockets import ConnectionClosed, ConnectionClosedOK
from websockets.sync.client import ClientConnection, connect

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
            host, f"/{self.org_id}/applications/{self.app_name}"
        )

    def run(self) -> None:
        while not self.evt.is_set():
            try:
                self.websocket = connect(self.url)
                while not self.evt.is_set():
                    message = self.websocket.recv()
                    base_message = p.BaseMessage.from_json(message)
                    type = base_message.type
                    if type == p.MessageType.EXECUTOR_INFO.value:
                        pass
                    else:
                        self.dbos.logger.warning(f"Unexpected message type: {type}")
                    print(message)
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
