import threading
from typing import Optional

from websockets.sync.client import ClientConnection, connect


class ConductorWebsocket(threading.Thread):

    def __init__(self, url: str, evt: threading.Event):
        super().__init__(daemon=True)
        self.websocket: Optional[ClientConnection] = None
        self.url = url
        self.evt = evt

    def run(self) -> None:
        self.websocket = connect(self.url)
        self.websocket.send("Hello world!")
        while not self.evt.is_set():
            message = self.websocket.recv()
            print(message)
