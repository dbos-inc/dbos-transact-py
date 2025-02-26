import threading
import time
from typing import TYPE_CHECKING, Optional

from websockets import ConnectionClosed, ConnectionClosedOK
from websockets.sync.client import ClientConnection, connect

if TYPE_CHECKING:
    from dbos import DBOS


class ConductorWebsocket(threading.Thread):

    def __init__(self, dbos: "DBOS", url: str, evt: threading.Event):
        super().__init__(daemon=True)
        self.websocket: Optional[ClientConnection] = None
        self.url = url
        self.evt = evt
        self.dbos = dbos

    def run(self) -> None:
        while not self.evt.is_set():
            try:
                self.websocket = connect(self.url)
                self.websocket.send("Hello world!")
                while not self.evt.is_set():
                    message = self.websocket.recv()
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
                break
