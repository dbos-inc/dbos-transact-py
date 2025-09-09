import pytest

from dbos import DBOS, Debouncer


def workflow(x: int) -> int:
    return x


async def workflow_async(x: int) -> int:
    return x


def test_debouncer(dbos: DBOS) -> None:

    DBOS.workflow()(workflow)
    first_value = 4
    second_value = 5

    debouncer = Debouncer("key", 2)
    first_handle = debouncer.debounce(workflow, first_value)
    second_handle = debouncer.debounce(workflow, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value


@pytest.mark.asyncio
async def test_debouncer_async(dbos: DBOS) -> None:

    DBOS.workflow()(workflow_async)
    first_value = 4
    second_value = 5

    debouncer = Debouncer("key", 2)
    first_handle = await debouncer.debounce_async(workflow_async, first_value)
    second_handle = await debouncer.debounce_async(workflow_async, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert await first_handle.get_result() == second_value
    assert await second_handle.get_result() == second_value
