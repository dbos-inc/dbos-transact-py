import threading

from websockets.sync.client import connect


def conductor_websocket(url: str, evt: threading.Event) -> None:
    with connect(url) as websocket:
        websocket.send("Hello world!")
        while not evt.is_set():
            message = websocket.recv()
            print(message)
