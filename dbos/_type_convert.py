"""Rebuild typed Python values from portable JSON using type hints.

Portable JSON stores plain objects, arrays and primitives with no type tags, so
the reader's annotations decide what a stored value becomes. Conversion only
rebuilds structure; it does not validate primitives (see ``validate_args``).
"""

from __future__ import annotations

import base64
import collections
import collections.abc
import dataclasses
import functools
import json
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
_PRIMITIVES = (str, int, float, bool)
_TypeAliasType = getattr(typing, "TypeAliasType", None)

_ABSTRACT_SEQUENCE_ORIGINS = (
    collections.abc.Sequence,
    collections.abc.MutableSequence,
    collections.abc.Collection,
    collections.abc.Iterable,
)
_SET_ORIGINS = (set, frozenset, collections.abc.Set, collections.abc.MutableSet)
_MAPPING_ORIGINS = (
    dict,
    collections.abc.Mapping,
    collections.abc.MutableMapping,
    collections.OrderedDict,
    collections.Counter,
    collections.defaultdict,
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


def _is_passthrough(hint: Any) -> bool:
    return hint is None or hint is Any or hint is object or isinstance(hint, TypeVar)


_SCALAR_TYPES = (bytes, bytearray, datetime, date, Decimal, uuid.UUID)


def _is_opaque(hint: Any) -> bool:
    """A class portable JSON has no encoding for, whose values conversion passes through."""
    if not isinstance(hint, type) or typing.get_origin(hint) is not None:
        return False
    return not (
        hint in _PRIMITIVES
        or hint is _NoneType
        or hint in _SCALAR_TYPES
        or hint in (tuple, list, deque)
        or hint in _SET_ORIGINS
        or hint in _MAPPING_ORIGINS
        or hint in _ABSTRACT_SEQUENCE_ORIGINS
        or issubclass(hint, Enum)
        or _is_pydantic_model(hint)
        or dataclasses.is_dataclass(hint)
        or _is_namedtuple(hint)
        or typing.is_typeddict(hint)
    )


def _unwrap(hint: Any) -> Any:
    """Strip NewType, Annotated and type-alias wrappers down to the hint they name."""
    while True:
        supertype = getattr(hint, "__supertype__", None)
        if supertype is not None:
            hint = supertype
        elif typing.get_origin(hint) is typing.Annotated:
            hint = typing.get_args(hint)[0]
        elif _TypeAliasType is not None and isinstance(hint, _TypeAliasType):
            hint = hint.__value__
        else:
            return hint


@functools.lru_cache(maxsize=1024)
def _field_hints(cls: Any) -> Dict[str, Any]:
    """A record type's field hints, or none if they cannot be resolved."""
    try:
        return typing.get_type_hints(cls)
    except Exception:
        return {}


def _plain(value: Any) -> Any:
    """The JSON form portable serialization gives a scalar, for matching stored values."""
    if isinstance(value, Enum):
        return _plain(value.value)
    if isinstance(value, (tuple, list)):
        return [_plain(v) for v in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _same_plain(expected: Any, value: Any) -> bool:
    plain = _plain(expected)
    # 1 == True in Python, but not in JSON.
    return type(plain) is type(value) and plain == value


def _convert(hint: Any, value: Any, path: str) -> Any:
    # A null is never converted, so a sloppily annotated None result still loads.
    if value is None or _is_passthrough(hint):
        return value
    hint = _unwrap(hint)
    if _is_passthrough(hint):
        return value
    origin = typing.get_origin(hint)
    args = typing.get_args(hint)

    if origin is Literal:
        return next((a for a in args if _same_plain(a, value)), value)
    if _is_union(origin):
        return _convert_union(hint, args, value, path)

    if hint is _NoneType or hint in _PRIMITIVES:
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
        return _convert_enum(hint, value, path)

    if _is_pydantic_model(hint):
        if isinstance(value, hint):
            return value
        try:
            return hint.model_validate(value)
        except Exception as e:
            raise _fail(hint, path, str(e)) from e

    if dataclasses.is_dataclass(origin) and isinstance(origin, type):
        return _convert_dataclass(
            origin, _generic_field_hints(origin, args), value, path
        )
    if isinstance(hint, type) and dataclasses.is_dataclass(hint):
        return _convert_dataclass(hint, _field_hints(hint), value, path)

    if _is_namedtuple(hint):
        return _convert_namedtuple(hint, value, path)

    if typing.is_typeddict(hint):
        return _convert_typeddict(hint, value, path)

    container = origin if origin is not None else hint
    if container is tuple:
        # A bare `tuple` has no origin; `Tuple[()]` has one and no arguments.
        return _convert_tuple(hint, args, origin is None, value, path)
    if container in _MAPPING_ORIGINS:
        return _convert_mapping(hint, container, args, value, path)
    if container in _SET_ORIGINS:
        items = _convert_items(hint, args, value, path)
        return frozenset(items) if container is frozenset else set(items)
    if container is deque:
        return deque(_convert_items(hint, args, value, path))
    if container is list:
        return _convert_items(hint, args, value, path)
    if container in _ABSTRACT_SEQUENCE_ORIGINS:
        # A str or dict is a valid Sequence or Iterable, so only arrays are rebuilt.
        if not isinstance(value, list):
            return value
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


def _convert_enum(hint: Any, value: Any, path: str) -> Any:
    if isinstance(value, hint):
        return value
    try:
        return hint(value)
    except Exception:
        pass
    # Values JSON cannot hold natively, like tuples or Decimals, are stored in another form.
    for member in hint:
        if _plain(member.value) == value:
            return member
    raise _fail(hint, path, f"{value!r} is not a member")


def _record_fields(cls: Any) -> Optional[set[str]]:
    """The keys a record type stores, or None for a hint that is not a record type."""
    if _is_pydantic_model(cls):
        names: set[str] = set()
        for name, field in cls.model_fields.items():
            names.add(name)
            if field.alias:
                names.add(field.alias)
        return names
    base = typing.get_origin(cls) or cls
    if isinstance(base, type) and dataclasses.is_dataclass(base):
        return {f.name for f in dataclasses.fields(base)}
    if typing.is_typeddict(cls):
        return set(_field_hints(cls))
    return None


def _literal_fields(cls: Any) -> Dict[str, Tuple[Any, ...]]:
    """A record type's fields annotated with a Literal, as name -> allowed values."""
    if _record_fields(cls) is None:
        return {}
    base = typing.get_origin(cls) or cls
    literals: Dict[str, Tuple[Any, ...]] = {}
    for name, h in _field_hints(base).items():
        h = _unwrap(h)
        if typing.get_origin(h) is Literal:
            literals[name] = typing.get_args(h)
    return literals


def _convert_union(hint: Any, args: Tuple[Any, ...], value: Any, path: str) -> Any:
    members = [_unwrap(a) for a in args if a is not _NoneType]
    # A primitive of the stored JSON type needs no conversion.
    if type(value) in _PRIMITIVES and type(value) in members:
        return value
    for member in members:
        if typing.get_origin(member) is Literal and any(
            _same_plain(a, value) for a in typing.get_args(member)
        ):
            return _convert(member, value, path)
    if isinstance(value, dict):
        # A matching Literal discriminator picks the member outright.
        for member in members:
            literals = _literal_fields(member)
            if literals and all(
                name in value and any(_same_plain(a, value[name]) for a in allowed)
                for name, allowed in literals.items()
            ):
                return _convert(member, value, path)

    structured = [
        m
        for m in members
        if not _is_passthrough(m)
        and not _is_opaque(m)
        and m not in _PRIMITIVES
        and typing.get_origin(m) is not Literal
    ]
    best: Optional[Tuple[Tuple[bool, int], Any]] = None
    last_error: Optional[Exception] = None
    for member in structured:
        try:
            converted = _convert(member, value, path)
        except Exception as e:
            last_error = e
            continue
        if not isinstance(value, dict):
            return converted
        # Of the record types that accept an object, prefer the one whose fields fit its keys.
        fit = _record_fit(member, value)
        if best is None or fit > best[0]:
            best = (fit, converted)
    if best is not None:
        return best[1]

    # A primitive member accepts only a value already of its type.
    for member in members:
        if member in _PRIMITIVES and (
            isinstance(value, member) or (member is float and type(value) is int)
        ):
            return value
    if any(_is_passthrough(m) or _is_opaque(m) for m in members):
        return value
    raise _fail(hint, path, f"no union member accepts it ({last_error})")


def _record_fit(member: Any, value: Dict[str, Any]) -> Tuple[bool, int]:
    """Rank how well a union member fits a stored object: no unknown keys, then most keys used."""
    fields = _record_fields(member)
    if fields is None:
        # A mapping accepts any object, so it loses to a record type that fits exactly.
        return (True, 0)
    return (set(value) <= fields, len(set(value) & fields))


def _generic_field_hints(cls: Any, args: Tuple[Any, ...]) -> Dict[str, Any]:
    """Field hints of a parameterized generic dataclass, with its type arguments substituted."""
    hints = _field_hints(cls)
    params = getattr(cls, "__parameters__", ())
    if not params or len(params) != len(args):
        return hints
    mapping = dict(zip(params, args))
    return {name: _substitute(h, mapping) for name, h in hints.items()}


def _substitute(hint: Any, mapping: Dict[Any, Any]) -> Any:
    if isinstance(hint, TypeVar):
        return mapping.get(hint, hint)
    params = getattr(hint, "__parameters__", ())
    if not params:
        return hint
    try:
        return hint[tuple(mapping.get(p, p) for p in params)]
    except TypeError:
        return hint


def _convert_dataclass(cls: Any, hints: Dict[str, Any], value: Any, path: str) -> Any:
    if isinstance(value, cls):
        return value
    if not isinstance(value, dict):
        raise _fail(cls, path, f"expected an object, got {type(value).__name__}")
    # Keys that name no field are ignored, so removing a field stays compatible.
    stored: Dict[str, Any] = {}
    for field in dataclasses.fields(cls):
        if field.name in value:
            stored[field.name] = _convert(
                hints.get(field.name), value[field.name], f"{path}.{field.name}"
            )
    init_args = {
        f.name: stored[f.name]
        for f in dataclasses.fields(cls)
        if f.init and f.name in stored
    }
    try:
        instance = cls(**init_args)
    except Exception as e:
        raise _fail(cls, path, str(e)) from e
    # Restore the stored state over whatever __post_init__ derived, including init=False fields.
    for name, field_value in stored.items():
        object.__setattr__(instance, name, field_value)
    return instance


def _convert_namedtuple(cls: Any, value: Any, path: str) -> Any:
    if isinstance(value, cls):
        return value
    if not isinstance(value, list):
        raise _fail(cls, path, f"expected an array, got {type(value).__name__}")
    if len(value) > len(cls._fields):
        raise _fail(cls, path, f"expected at most {len(cls._fields)} items")
    hints = _field_hints(cls)
    items = [
        _convert(hints.get(name), item, f"{path}[{i}]")
        for i, (name, item) in enumerate(zip(cls._fields, value))
    ]
    try:
        return cls(*items)
    except Exception as e:
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


def _convert_tuple(
    hint: Any, args: Tuple[Any, ...], bare: bool, value: Any, path: str
) -> Any:
    if not isinstance(value, list):
        raise _fail(hint, path, f"expected an array, got {type(value).__name__}")
    if bare or (len(args) == 2 and args[1] is Ellipsis):
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


def _convert_mapping(
    hint: Any, container: Any, args: Tuple[Any, ...], value: Any, path: str
) -> Any:
    if not isinstance(value, dict):
        raise _fail(hint, path, f"expected an object, got {type(value).__name__}")
    key_hint = args[0] if len(args) > 0 else None
    value_hint = args[1] if len(args) > 1 else None
    if container is collections.Counter:
        value_hint = int
    converted = {
        _convert_key(key_hint, k, path): _convert(value_hint, v, f"{path}[{k!r}]")
        for k, v in value.items()
    }
    if container is collections.Counter:
        return collections.Counter(converted)
    if container is collections.OrderedDict:
        return collections.OrderedDict(converted)
    if container is collections.defaultdict:
        return collections.defaultdict(None, converted)
    return converted


_BOOL_KEYS = {"true": True, "false": False}


def _key_form(value: Any) -> Any:
    """The string portable serialization writes for a dict key."""
    plain = _plain(value)
    if isinstance(plain, str):
        return plain
    if plain is None or isinstance(plain, (bool, int, float)):
        return json.dumps(plain)
    return None


def _convert_key(hint: Any, key: Any, path: str) -> Any:
    """Rebuild a dict key, which portable JSON always stores as a string."""
    hint = _unwrap(hint)
    if not isinstance(key, str) or _is_passthrough(hint) or hint is str:
        return key
    key_path = f"{path} key {key!r}"
    origin = typing.get_origin(hint)
    if _is_union(origin):
        # Keys are always strings, so a str member is the last resort.
        members = sorted(typing.get_args(hint), key=lambda m: m is str)
        for member in members:
            try:
                return _convert_key(member, key, path)
            except DBOSSerializationError:
                continue
        raise _fail(hint, key_path, "invalid key")
    if origin is Literal:
        for allowed in typing.get_args(hint):
            if _key_form(allowed) == key:
                return allowed
        return key
    if isinstance(hint, type) and issubclass(hint, Enum):
        for member in hint:
            if _key_form(member) == key:
                return member
        raise _fail(hint, key_path, "not a member")
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
        raise _fail(hint, key_path, "invalid key") from e
    return _convert(hint, key, key_path)
