from typing import Any, Callable, Generic, Optional, TypeVar

G = TypeVar("G")  # A generic type for ClassPropertyDescriptor getters


class ClassPropertyDescriptor(Generic[G]):
    def __init__(self, fget: Callable[..., G]) -> None:
        self.fget = fget

    def __get__(self, obj: Any, objtype: Optional[Any] = None) -> G:
        if objtype is None:
            objtype = type(obj)
        if self.fget is None:
            raise AttributeError("unreadable attribute")
        return self.fget(objtype)


def classproperty(func: Callable[..., G]) -> ClassPropertyDescriptor[G]:
    return ClassPropertyDescriptor(func)
