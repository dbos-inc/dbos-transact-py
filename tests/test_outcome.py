import asyncio
import functools
from typing import Callable

import pytest

from dbos._outcome import Immediate, Outcome, Pending


def after(result: Callable[[], int]) -> str:
    return f"Result: {result()}"


def before(func: Callable[[], None]) -> Callable[[Callable[[], int]], str]:
    func()
    return after


def test_immediate() -> None:

    func_called = False

    def adder(a: int, b: int) -> int:
        assert func_called  # ensure func called before adder
        return a + b

    def func() -> None:
        nonlocal func_called
        func_called = True

    o1 = Outcome[int].make(lambda: adder(10, 20)).wrap(lambda: before(func))

    assert isinstance(o1, Immediate)

    output1 = o1()
    assert func_called
    assert output1 == "Result: 30"

    o3 = Outcome[int].make(lambda: adder(30, 40)).then(after)
    assert isinstance(o3, Immediate)
    out2 = o3()
    assert out2 == "Result: 70"


@pytest.mark.asyncio
async def test_pending() -> None:
    func_called = False

    async def adder(a: int, b: int) -> int:
        assert func_called  # ensure func called before adder
        await asyncio.sleep(0.1)  # simulate async operation
        return a + b

    def func() -> None:
        nonlocal func_called
        func_called = True

    o1 = Outcome[int].make(functools.partial(adder, 10, 20))
    o2 = o1.wrap(lambda: before(func))

    assert isinstance(o1, Pending)
    assert isinstance(o2, Pending)

    output = await o2()
    assert func_called
    assert output == "Result: 30"

    o3 = Outcome[int].make(functools.partial(adder, 30, 40)).then(after)
    assert isinstance(o3, Pending)
    out2 = await o3()
    assert out2 == "Result: 70"
