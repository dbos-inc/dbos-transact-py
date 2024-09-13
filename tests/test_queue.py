import sqlalchemy as sa

from dbos import DBOS, Queue


def test_simple_workflow(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        return var1 + var2

    assert test_workflow("abc", "123") == "abc123"
    assert wf_counter == 1

    queue = Queue("test_queue", None)

    queue.enqueue(test_workflow, "abc", "123")
