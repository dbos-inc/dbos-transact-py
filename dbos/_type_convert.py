"""Rebuild typed Python values from portable JSON using type hints.

Portable JSON stores plain objects, arrays and primitives with no type tags, so
the reader's annotations decide what a stored value becomes. Conversion only
rebuilds structure; it does not validate primitives (see ``validate_args``).
"""

from __future__ import annotations

import base64
import collections.abc
import dataclasses
import functools
import types
import typing
import uuid
from collections import deque
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Tuple, TypeVar, Union

from dateutil.parser import isoparse

from ._error import DBOSSerializationError

_NoneType = type(None)

_SEQUENCE_ORIGINS = (
    list,
    collections.abc.Sequence,
    collections.abc.MutableSequence,
    collections.abc.Collection,
    collections.abc.Iterable,
)
_SET_ORIGINS = (set, collections.abc.Set, collections.abc.MutableSet)
_MAPPING_ORIGINS = (
    dict,
    collections.abc.Mapping,
    collections.abc.MutableMapping,
)


def value_to_type(hint: Any, value: Any, path: str = "$") -> Any:
    """Convert a value loaded from portable JSON to the type ``hint`` describes.

    ``path`` names the value in error messages."""
    return _convert(hint, value, path)


def _fail(hint: Any, path: str, reason: str) -> DBOSSerializationError:
    return DBOSSerializationError(
        f"Cannot convert stored value at {path} to {_hint_name(hint)}: {reason}"
    )


def _hint_name(hint: Any) -> str:
    if isinstance(hint, type):
        return hint.__qualname__
    return repr(hint)


def _is_union(origin: Any) -> bool:
    return origin is Union or origin is types.UnionType


def _is_pydantic_model(hint: Any) -> bool:
    return (
        isinstance(hint, type)
        and hasattr(hint, "model_validate")
        and hasattr(hint, "model_fields")
    )


def _is_namedtuple(hint: Any) -> bool:
    return (
        isinstance(hint, type) and issubclass(hint, tuple) and hasattr(hint, "_fields")
    )


@functools.lru_cache(maxsize=1024)
def _field_hints(cls: Any) -> Dict[str, Any]:
    """A record type's field hints, or none if they cannot be resolved."""
    try:
        return typing.get_type_hints(cls)
    except Exception:
        return {}


def _convert(hint: Any, value: Any, path: str) -> Any:
    # A null is never converted, so a sloppily annotated None result still loads.
    if value is None or hint is None or hint is Any or hint is object:
        return value
    if isinstance(hint, TypeVar):
        return value

    # NewType and Annotated wrap another hint
    supertype = getattr(hint, "__supertype__", None)
    if supertype is not None:
        return _convert(supertype, value, path)
    origin = typing.get_origin(hint)
    args = typing.get_args(hint)
    if origin is typing.Annotated:
        return _convert(args[0], value, path)

    if origin is Literal:
        return value
    if _is_union(origin):
        return _convert_union(hint, args, value, path)

    if hint is _NoneType or hint in (str, int, float, bool):
        # Primitives are not coerced; validation is validate_args's job.
        return value
    if hint in (bytes, bytearray):
        if not isinstance(value, str):
            return value
        try:
            return hint(base64.b64decode(value, validate=True))
        except ValueError as e:
            raise _fail(hint, path, f"invalid base64: {e}") from e
    if hint is datetime:
        return _convert_datetime(value, path)
    if hint is date:
        if isinstance(value, date) or not isinstance(value, str):
            return value
        try:
            return isoparse(value).date()
        except (ValueError, OverflowError) as e:
            raise _fail(hint, path, f"invalid ISO date {value!r}") from e
    if hint is Decimal:
        if isinstance(value, Decimal) or isinstance(value, bool):
            return value
        if not isinstance(value, (str, int, float)):
            return value
        try:
            return Decimal(str(value))
        except InvalidOperation as e:
            raise _fail(hint, path, f"invalid decimal {value!r}") from e
    if hint is uuid.UUID:
        if not isinstance(value, str):
            return value
        try:
            return uuid.UUID(value)
        except ValueError as e:
            raise _fail(hint, path, f"invalid UUID {value!r}") from e

    if isinstance(hint, type) and issubclass(hint, Enum):
        if isinstance(value, hint):
            return value
        try:
            return hint(value)
        except ValueError as e:
            raise _fail(hint, path, f"{value!r} is not a member") from e

    if _is_pydantic_model(hint):
        if isinstance(value, hint):
            return value
        try:
            return hint.model_validate(value)
        except Exception as e:
            raise _fail(hint, path, str(e)) from e

    dataclass_type = origin if dataclasses.is_dataclass(origin) else hint
    if isinstance(dataclass_type, type) and dataclasses.is_dataclass(dataclass_type):
        return _convert_dataclass(dataclass_type, value, path)

    if _is_namedtuple(hint):
        return _convert_namedtuple(hint, value, path)

    if typing.is_typeddict(hint):
        return _convert_typeddict(hint, value, path)

    container = origin if origin is not None else hint
    if container is tuple:
        return _convert_tuple(hint, args, value, path)
    if container in _MAPPING_ORIGINS:
        return _convert_mapping(hint, args, value, path)
    if container in (deque, frozenset) or container in _SET_ORIGINS:
        items = _convert_items(hint, args, value, path)
        if container is deque:
            return deque(items)
        return frozenset(items) if container is frozenset else set(items)
    if container in _SEQUENCE_ORIGINS:
        return _convert_items(hint, args, value, path)

    # Types portable JSON has no encoding for pass through unchanged.
    return value


def _convert_datetime(value: Any, path: str) -> Any:
    if isinstance(value, datetime) or not isinstance(value, str):
        return value
    try:
        return isoparse(value)
    except (ValueError, OverflowError) as e:
        raise _fail(datetime, path, f"invalid ISO datetime {value!r}") from e


def _literal_fields(cls: Any) -> Dict[str, Tuple[Any, ...]]:
    """The fields of a record type annotated with a Literal, as name -> allowed values."""
    if not (
        (isinstance(cls, type) and dataclasses.is_dataclass(cls))
        or _is_pydantic_model(cls)
        or typing.is_typeddict(cls)
    ):
        return {}
    return {
        name: typing.get_args(h)
        for name, h in _field_hints(cls).items()
        if typing.get_origin(h) is Literal
    }


def _convert_union(hint: Any, args: Tuple[Any, ...], value: Any, path: str) -> Any:
    members = [a for a in args if a is not _NoneType]
    # An exact primitive match needs no conversion.
    if type(value) in members:
        return value
    # A matching Literal discriminator picks the member outright.
    if isinstance(value, dict):
        for member in members:
            literals = _literal_fields(member)
            if literals and all(
                name in value and value[name] in allowed
                for name, allowed in literals.items()
            ):
                return _convert(member, value, path)
    last_error: Optional[DBOSSerializationError] = None
    for member in members:
        try:
            return _convert(member, value, path)
        except DBOSSerializationError as e:
            last_error = e
    raise _fail(hint, path, f"no union member accepts it ({last_error})")


def _convert_dataclass(cls: Any, value: Any, path: str) -> Any:
    if isinstance(value, cls):
        return value
    if not isinstance(value, dict):
        raise _fail(cls, path, f"expected an object, got {type(value).__name__}")
    hints = _field_hints(cls)
    kwargs: Dict[str, Any] = {}
    # Keys that name no field are ignored, so removing a field stays compatible.
    for field in dataclasses.fields(cls):
        if field.init and field.name in value:
            kwargs[field.name] = _convert(
                hints.get(field.name), value[field.name], f"{path}.{field.name}"
            )
    try:
        return cls(**kwargs)
    except TypeError as e:
        raise _fail(cls, path, str(e)) from e


def _convert_namedtuple(cls: Any, value: Any, path: str) -> Any:
    if isinstance(value, cls):
        return value
    if not isinstance(value, list):
        raise _fail(cls, path, f"expected an array, got {type(value).__name__}")
    hints = _field_hints(cls)
    items = [
        _convert(hints.get(name), item, f"{path}[{i}]")
        for i, (name, item) in enumerate(zip(cls._fields, value))
    ]
    try:
        return cls(*items)
    except TypeError as e:
        raise _fail(cls, path, str(e)) from e


def _convert_typeddict(cls: Any, value: Any, path: str) -> Any:
    if not isinstance(value, dict):
        raise _fail(cls, path, f"expected an object, got {type(value).__name__}")
    hints = _field_hints(cls)
    return {k: _convert(hints.get(k), v, f"{path}.{k}") for k, v in value.items()}


def _convert_items(
    hint: Any, args: Tuple[Any, ...], value: Any, path: str
) -> List[Any]:
    if not isinstance(value, list):
        raise _fail(hint, path, f"expected an array, got {type(value).__name__}")
    item_hint = args[0] if args else None
    return [_convert(item_hint, item, f"{path}[{i}]") for i, item in enumerate(value)]


def _convert_tuple(hint: Any, args: Tuple[Any, ...], value: Any, path: str) -> Any:
    if not isinstance(value, list):
        raise _fail(hint, path, f"expected an array, got {type(value).__name__}")
    if not args or (len(args) == 2 and args[1] is Ellipsis):
        item_hint = args[0] if args else None
        return tuple(
            _convert(item_hint, item, f"{path}[{i}]") for i, item in enumerate(value)
        )
    if args == ((),):
        args = ()
    if len(value) != len(args):
        raise _fail(hint, path, f"expected {len(args)} items, got {len(value)}")
    return tuple(
        _convert(h, item, f"{path}[{i}]")
        for i, (h, item) in enumerate(zip(args, value))
    )


def _convert_mapping(hint: Any, args: Tuple[Any, ...], value: Any, path: str) -> Any:
    if not isinstance(value, dict):
        raise _fail(hint, path, f"expected an object, got {type(value).__name__}")
    key_hint = args[0] if len(args) > 0 else None
    value_hint = args[1] if len(args) > 1 else None
    return {
        _convert_key(key_hint, k, path): _convert(value_hint, v, f"{path}[{k!r}]")
        for k, v in value.items()
    }


_BOOL_KEYS = {"true": True, "false": False}


def _convert_key(hint: Any, key: Any, path: str) -> Any:
    """Rebuild a dict key, which portable JSON always stores as a string."""
    while True:
        supertype = getattr(hint, "__supertype__", None)
        if supertype is not None:
            hint = supertype
        elif typing.get_origin(hint) is typing.Annotated:
            hint = typing.get_args(hint)[0]
        else:
            break
    if not isinstance(key, str) or hint in (None, Any, str):
        return key
    try:
        if hint is bool:
            return _BOOL_KEYS[key]
        if hint is int:
            return int(key)
        if hint is float:
            return float(key)
        if hint is _NoneType:
            if key != "null":
                raise ValueError(key)
            return None
    except (KeyError, ValueError) as e:
        raise _fail(hint, f"{path} key {key!r}", "invalid key") from e
    return _convert(hint, key, f"{path} key {key!r}")
