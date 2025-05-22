import json
from typing import List, Optional, TypedDict, cast

from dbos import DBOS, Queue


class Person(TypedDict):
    first: str
    last: str
    age: int


queue = Queue("test_queue")
inorder_queue = Queue("inorder_queue", 1, priority_enabled=True)
inorder_results: List[str] = []


@DBOS.workflow()
def enqueue_test(numVal: int, strVal: str, person: Person) -> str:
    return f"{numVal}-{strVal}-{json.dumps(person)}"


@DBOS.workflow()
def send_test(topic: Optional[str] = None) -> str:
    return cast(str, DBOS.recv(topic, 60))


@DBOS.workflow()
def retrieve_test(value: str) -> str:
    DBOS.sleep(5)
    inorder_results.append(value)
    return value


@DBOS.workflow()
def event_test(key: str, value: str, update: Optional[int] = None) -> str:
    DBOS.set_event(key, value)
    if update is not None:
        DBOS.sleep(update)
        DBOS.set_event(key, f"updated-{value}")
    return f"{key}-{value}"


@DBOS.workflow()
def blocked_workflow() -> None:
    while True:
        DBOS.sleep(0.1)


@DBOS.transaction()
def test_txn(x: int) -> int:
    return x


@DBOS.step()
def test_step(x: int) -> int:
    return x


@DBOS.workflow()
def fork_test(x: int) -> int:
    return test_txn(x) + test_step(x)
