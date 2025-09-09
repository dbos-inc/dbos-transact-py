import pytest

from dbos import DBOS, Debouncer


def workflow(x: int) -> int:
    return x


async def workflow_async(x: int) -> int:
    return x


def test_debouncer(dbos: DBOS) -> None:

    DBOS.workflow()(workflow)
    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    debouncer = Debouncer(debounce_key="key", debounce_period_sec=2)

    first_handle = debouncer.debounce(workflow, first_value)
    second_handle = debouncer.debounce(workflow, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert first_handle.get_result() == second_value
    assert second_handle.get_result() == second_value

    third_handle = debouncer.debounce(workflow, third_value)
    fourth_handle = debouncer.debounce(workflow, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert third_handle.get_result() == fourth_value
    assert fourth_handle.get_result() == fourth_value


@pytest.mark.asyncio
async def test_debouncer_async(dbos: DBOS) -> None:

    DBOS.workflow()(workflow_async)
    first_value, second_value, third_value, fourth_value = 0, 1, 2, 3

    debouncer = Debouncer(debounce_key="key", debounce_period_sec=2)

    first_handle = await debouncer.debounce_async(workflow_async, first_value)
    second_handle = await debouncer.debounce_async(workflow_async, second_value)
    assert first_handle.workflow_id == second_handle.workflow_id
    assert await first_handle.get_result() == second_value
    assert await second_handle.get_result() == second_value

    third_handle = await debouncer.debounce_async(workflow_async, third_value)
    fourth_handle = await debouncer.debounce_async(workflow_async, fourth_value)
    assert third_handle.workflow_id != first_handle.workflow_id
    assert third_handle.workflow_id == fourth_handle.workflow_id
    assert await third_handle.get_result() == fourth_value
    assert await fourth_handle.get_result() == fourth_value
