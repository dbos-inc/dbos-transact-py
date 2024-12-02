import inspect
import sys
from contextlib import AbstractContextManager
from typing import Any, Callable, Coroutine, Generic, TypeAlias, TypeVar, Union, cast

T = TypeVar("T", covariant=True)  # A generic type for OK Result values
R = TypeVar("R", covariant=True)  # A generic type for OK Result values


# OK branch of functional Result type
class Ok(Generic[T]):
    __slots__ = "_value"

    def __init__(self, value: T) -> None:
        self._value = value

    def is_ok(self) -> bool:
        return True

    def is_err(self) -> bool:
        return False

    def __call__(self) -> T:
        return self._value


# Err branch of functional Result type
class Err:
    __slots__ = "_value"

    def __init__(self, value: BaseException) -> None:
        self._value = value

    def is_ok(self) -> bool:
        return False

    def is_err(self) -> bool:
        return True

    def __call__(self) -> Any:
        raise self._value


Result: TypeAlias = Union[Ok[T], Err]


def _to_result_sync(func: Callable[[], T]) -> Result[T]:
    try:
        result = func()
        return Ok(result)
    except Exception as e:
        return Err(e)


async def _to_result_async(func: Callable[[], Coroutine[Any, Any, T]]) -> Result[T]:
    try:
        result = await func()
        return Ok(result)
    except Exception as e:
        return Err(e)


def to_result(
    func: Callable[[], Union[T, Coroutine[Any, Any, T]]]
) -> Union[Result[T], Coroutine[Any, Any, Result[T]]]:

    return (
        _to_result_async(func)
        if inspect.iscoroutinefunction(func)
        else _to_result_sync(cast(Callable[[], T], func))
    )


# def chain_result(
#     result: Union[Result[T], Coroutine[Any, Any, Result[T]]],
#     next_func: Callable[[Result[T]], R],
# ) -> Union[R, Coroutine[Any, Any, R]]:

#     def chain_result_sync(result: Result[T]) -> R:
#         return next_func(result)

#     async def chain_result_async(coro: Coroutine[Any, Any, Result[T]]) -> R:
#         result = await coro
#         return next_func(result)

#     return (
#         chain_result_async(result)
#         if inspect.iscoroutine(result)
#         else chain_result_sync(cast(Result[T], result))
#     )


def chain_result(
    func: Callable[[], Union[T, Coroutine[Any, Any, T]]],
    next_func: Callable[[Result[T]], R],
) -> Union[R, Coroutine[Any, Any, R]]:

    def chain_result_sync(
        func: Callable[[], T],
    ) -> R:
        result = _to_result_sync(func)
        return next_func(result)

    async def chain_result_async(
        func: Callable[[], Coroutine[Any, Any, T]],
    ) -> R:
        result = await _to_result_async(func)
        return next_func(result)

    return (
        chain_result_async()
        if inspect.iscoroutinefunction(func)
        else chain_result_sync(cast(Callable[[], T], func))
    )


# def chain_ctx_mgr(
#     result: Union[Result[T], Coroutine[Any, Any, Result[T]]],
#     acm: AbstractContextManager,
# ) -> Union[T, Coroutine[Any, Any, T]]:
#     exc = True
#     try:
#         try:
#             return result()
#         except:
#             exc = False
#             if not acm.__exit__(*sys.exc_info()):
#                 raise

#     finally:
#         if exc:
#             acm.__exit__(None, None, None)
