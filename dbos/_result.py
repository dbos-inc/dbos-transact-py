import contextlib
import inspect
from typing import Any, Callable, Coroutine, Protocol, TypeVar, Union, cast

T = TypeVar("T")
R = TypeVar("R")


# define Result protocol w/ common composition methods
class Result(Protocol[T]):
    def then(self, next: Callable[[Callable[[], T]], R]) -> "Result[R]": ...

    def also(self, cm: contextlib.AbstractContextManager[Any, bool]) -> "Result[T]": ...

    def __call__(self) -> Union[T, Coroutine[Any, Any, T]]: ...


# Immediate Result - for composing non-async functions
class Immediate(Result[T]):
    __slots__ = "_func"

    def __init__(self, func: Callable[[], T]):
        self._func = func

    def then(self, next: Callable[[Callable[[], T]], R]) -> "Immediate[R]":
        return Immediate(lambda: next(self._func))

    @staticmethod
    def _also(func: Callable[[], T], cm: contextlib.AbstractContextManager[Any, bool]) -> T:  # type: ignore
        with cm:
            return func()

    def also(self, cm: contextlib.AbstractContextManager[Any, bool]) -> "Immediate[T]":
        return Immediate[T](lambda: Immediate._also(self._func, cm))

    def __call__(self) -> T:
        return self._func()


# Pending Result - for composing async functions
class Pending(Result[T]):
    __slots__ = "_func"

    def __init__(self, func: Callable[[], Coroutine[Any, Any, T]]):
        self._func = func

    # Helper method in order to raise an exception in a lambda
    @staticmethod
    def _raise(ex: BaseException) -> T:
        raise ex

    # helper method to invoke  _func with await, then pass the resulting value
    # or exception in a lambda to next. This is needed since _func is async.
    @staticmethod
    async def _do(
        func: Callable[[], Coroutine[Any, Any, T]], next: Callable[[Callable[[], T]], R]
    ) -> R:
        try:
            value = await func()
            return next(lambda: value)
        except BaseException as exp:
            return next(lambda: Pending._raise(exp))

    def then(self, next: Callable[[Callable[[], T]], R]) -> "Pending[R]":
        return Pending[R](lambda: Pending._do(self._func, next))

    @staticmethod
    async def _also(  # type: ignore
        func: Callable[[], Coroutine[Any, Any, T]],
        cm: contextlib.AbstractContextManager[Any, bool],
    ) -> T:
        with cm:
            return await func()

    def also(self, cm: contextlib.AbstractContextManager[Any, bool]) -> "Pending[T]":
        return Pending[T](lambda: Pending._also(self._func, cm))

    async def __call__(self) -> T:
        return await self._func()


# Helper function to create an Immediate or Pending Result, depending on if func is a coroutine function or not
def make_result(func: Callable[[], Union[T, Coroutine[Any, Any, T]]]) -> Result[T]:
    return (
        Pending(cast(Callable[[], Coroutine[Any, Any, T]], func))
        if inspect.iscoroutinefunction(func)
        else Immediate(cast(Callable[[], T], func))
    )
