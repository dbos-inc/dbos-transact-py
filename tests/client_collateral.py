import json
from typing import Optional, TypedDict, cast

from dbos import DBOS, Queue


class Person(TypedDict):
    first: str
    last: str
    age: int


queue = Queue("test_queue")


@DBOS.workflow()
def enqueue_test(numVal: int, strVal: str, person: Person) -> str:
    return f"{numVal}-{strVal}-{json.dumps(person)}"


@DBOS.workflow()
def send_test(topic: Optional[str] = None) -> str:
    return cast(str, DBOS.recv(topic, 60))


@DBOS.workflow()
def retrieve_test(value: str) -> str:
    DBOS.sleep(5)
    return value


@DBOS.workflow()
def event_test(key: str, value: str, update: Optional[int] = None) -> str:
    DBOS.set_event(key, value)
    if update is not None:
        DBOS.sleep(update)
        DBOS.set_event(key, f"updated-{value}")
    return f"{key}-{value}"
