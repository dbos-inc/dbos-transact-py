import asyncio
import contextlib
import inspect
import time
from typing import Any, Callable, Coroutine, Optional, Protocol, TypeVar, Union, cast

T = TypeVar("T")
R = TypeVar("R")


# define Outcome protocol w/ common composition methods
class Outcome(Protocol[T]):

    def wrap(
        self, before: Callable[[], Callable[[Callable[[], T]], R]]
    ) -> "Outcome[R]": ...

    def then(self, next: Callable[[Callable[[], T]], R]) -> "Outcome[R]": ...

    def also(
        self, cm: contextlib.AbstractContextManager[Any, bool]
    ) -> "Outcome[T]": ...

    def retry(
        self,
        attempts: int,
        on_exception: Callable[[int, BaseException], float],
        exceeded_retries: Callable[[int], BaseException],
    ) -> "Outcome[T]": ...

    def intercept(self, interceptor: Callable[[], Optional[T]]) -> "Outcome[T]": ...

    def __call__(self) -> Union[T, Coroutine[Any, Any, T]]: ...

    # Helper function to create an Immediate or Pending Result, depending on if func is a coroutine function or not
    @staticmethod
    def make(func: Callable[[], Union[T, Coroutine[Any, Any, T]]]) -> "Outcome[T]":
        return (
            Pending(cast(Callable[[], Coroutine[Any, Any, T]], func))
            if inspect.iscoroutinefunction(func)
            else Immediate(cast(Callable[[], T], func))
        )


# Immediate Outcome - for composing non-async functions
class Immediate(Outcome[T]):
    __slots__ = "_func"

    def __init__(self, func: Callable[[], T]):
        self._func = func

    def then(self, next: Callable[[Callable[[], T]], R]) -> "Immediate[R]":
        return Immediate(lambda: next(self._func))

    def wrap(
        self, before: Callable[[], Callable[[Callable[[], T]], R]]
    ) -> "Immediate[R]":
        return Immediate(lambda: before()(self._func))

    @staticmethod
    def _intercept(func: Callable[[], T], interceptor: Callable[[], Optional[T]]) -> T:
        intercepted = interceptor()
        return intercepted if intercepted else func()

    def intercept(self, interceptor: Callable[[], Optional[T]]) -> "Immediate[T]":
        return Immediate[T](lambda: Immediate._intercept(self._func, interceptor))

    @staticmethod
    def _also(func: Callable[[], T], cm: contextlib.AbstractContextManager[Any, bool]) -> T:  # type: ignore
        with cm:
            return func()

    def also(self, cm: contextlib.AbstractContextManager[Any, bool]) -> "Immediate[T]":
        return Immediate[T](lambda: Immediate._also(self._func, cm))

    @staticmethod
    def _retry(
        func: Callable[[], T],
        attempts: int,
        on_exception: Callable[[int, BaseException], float],
        exceeded_retries: Callable[[int], BaseException],
    ) -> T:
        for i in range(attempts):
            try:
                return func()
            except Exception as exp:
                wait_time = on_exception(i, exp)
                time.sleep(wait_time)

        raise exceeded_retries(attempts)

    def retry(
        self,
        attempts: int,
        on_exception: Callable[[int, BaseException], float],
        exceeded_retries: Callable[[int], BaseException],
    ) -> "Immediate[T]":
        assert attempts > 0
        return Immediate[T](
            lambda: Immediate._retry(
                self._func, attempts, on_exception, exceeded_retries
            )
        )

    def __call__(self) -> T:
        return self._func()


# Pending Outcome - for composing async functions
class Pending(Outcome[T]):
    __slots__ = "_func"

    def __init__(self, func: Callable[[], Coroutine[Any, Any, T]]):
        self._func = func

    # Helper method in order to raise an exception in a lambda
    @staticmethod
    def _raise(ex: BaseException) -> T:
        raise ex

    async def _wrap(
        func: Callable[[], Coroutine[Any, Any, T]],
        before: Callable[[], Callable[[Callable[[], T]], R]],
    ) -> R:
        after = before()
        try:
            value = await func()
            return after(lambda: value)
        except BaseException as exp:
            return after(lambda: Pending._raise(exp))

    def wrap(
        self, before: Callable[[], Callable[[Callable[[], T]], R]]
    ) -> "Pending[R]":
        return Pending[R](lambda: Pending._wrap(self._func, before))

    def then(self, next: Callable[[Callable[[], T]], R]) -> "Pending[R]":
        return Pending[R](lambda: Pending._wrap(self._func, lambda: next))

    @staticmethod
    async def _also(  # type: ignore
        func: Callable[[], Coroutine[Any, Any, T]],
        cm: contextlib.AbstractContextManager[Any, bool],
    ) -> T:
        with cm:
            return await func()

    def also(self, cm: contextlib.AbstractContextManager[Any, bool]) -> "Pending[T]":
        return Pending[T](lambda: Pending._also(self._func, cm))

    @staticmethod
    async def _intercept(
        func: Callable[[], Coroutine[Any, Any, T]],
        interceptor: Callable[[], Optional[T]],
    ) -> T:
        intercepted = interceptor()
        return intercepted if intercepted else await func()

    def intercept(self, interceptor: Callable[[], Optional[T]]) -> "Pending[T]":
        return Pending[T](lambda: Pending._intercept(self._func, interceptor))

    @staticmethod
    async def _retry(
        func: Callable[[], Coroutine[Any, Any, T]],
        attempts: int,
        on_exception: Callable[[int, BaseException], float],
        exceeded_retries: Callable[[int], BaseException],
    ) -> T:
        for i in range(attempts):
            try:
                return await func()
            except Exception as exp:
                wait_time = on_exception(i, exp)
                await asyncio.sleep(wait_time)

        raise exceeded_retries(attempts)

    def retry(
        self,
        attempts: int,
        on_exception: Callable[[int, BaseException], float],
        exceeded_retries: Callable[[int], BaseException],
    ) -> "Pending[T]":
        assert attempts > 0
        return Pending[T](
            lambda: Pending._retry(self._func, attempts, on_exception, exceeded_retries)
        )

    async def __call__(self) -> T:
        return await self._func()
