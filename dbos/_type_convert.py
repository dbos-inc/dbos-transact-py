"""Convert Python values to and from portable JSON, using type hints to rebuild types.

Portable JSON stores plain objects, arrays and primitives with no type tags, so
the reader's annotations decide what a stored value becomes. Conversion only
rebuilds structure; it does not validate primitives (see ``validate_args``).
"""

from __future__ import annotations

import base64
import collections
import collections.abc
import contextvars
import dataclasses
import functools
import inspect
import json
import sys
import types
import typing
import uuid
from collections import deque
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Dict, Iterable, List, Literal, Optional, Tuple, TypeVar, Union

from dateutil.parser import isoparse

try:
    import typing_extensions
except ImportError:  # pragma: no cover
    typing_extensions = None  # type: ignore[assignment]

from ._error import DBOSSerializationError
from ._logger import dbos_logger

_NoneType = type(None)
_PRIMITIVES = (str, int, float, bool)
# typing's own and typing_extensions' versions are distinct classes, even on 3.14.
_TYPE_ALIAS_TYPES = tuple(
    t
    for t in (
        getattr(typing, "TypeAliasType", None),
        getattr(typing_extensions, "TypeAliasType", None),
    )
    if t is not None
)


def _is_typeddict(hint: Any) -> bool:
    if typing.is_typeddict(hint):
        return True
    return typing_extensions is not None and typing_extensions.is_typeddict(hint)


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

# ---------- Encoding ----------


def to_rfc3339_utc(dt: datetime) -> str:
    """
    Canonical timestamp string: UTC RFC3339.
    - Always emits Z, with milliseconds when present.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    # Python emits "+00:00"; canonicalize to "Z".
    s = dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return s


def to_portable(value: Any) -> Any:
    """
    Convert common Python objects into plain JSON-serializable structures
    """
    if isinstance(value, Enum):
        return to_portable(value.value)

    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if isinstance(value, datetime):
        return to_rfc3339_utc(value)

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return format(value, "f")

    if isinstance(value, uuid.UUID):
        return str(value)

    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode("ascii")

    if isinstance(value, (list, tuple, deque, set, frozenset)):
        return [to_portable(v) for v in value]

    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            f.name: to_portable(getattr(value, f.name))
            for f in dataclasses.fields(value)
        }

    # A pydantic model, detected without importing pydantic.
    if hasattr(type(value), "model_fields") and hasattr(value, "model_dump"):
        return _portable_model(value)

    if isinstance(value, collections.abc.Mapping):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            key = portable_key(k)
            if key in out:
                raise TypeError(
                    f"Attempt to do portable JSON serialization of a map whose keys {k!r} and another both encode as {key!r}"
                )
            out[key] = to_portable(v)
        return out

    raise TypeError(
        f"Object of type {type(value).__name__} is not portable JSON serializable"
    )


def portable_key(key: Any) -> str:
    """Encode a dict key as JSON's string key, the way Python's json writes scalars."""
    if isinstance(key, Enum):
        key = key.value
    if isinstance(key, str):
        return key
    if key is None or isinstance(key, (bool, int, float)):
        return json.dumps(key)
    if isinstance(key, (datetime, date, Decimal, uuid.UUID)):
        return typing.cast(str, to_portable(key))
    raise TypeError(
        f"Attempt to do portable JSON serialization of a map with a key of type {type(key).__name__}"
    )


# Pydantic writes this in place of a secret's value.
_SECRET_MASK = "**********"

# Whether to_portable writes secrets' values, which a checkpoint needs but an error report must not show.
_unmask_secrets: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "dbos_unmask_secrets", default=True
)


def to_portable_masked(value: Any) -> Any:
    """to_portable, leaving pydantic secrets masked."""
    token = _unmask_secrets.set(False)
    try:
        return to_portable(value)
    finally:
        _unmask_secrets.reset(token)


def _portable_model(model: Any) -> Any:
    # Field names, not aliases, since a serialization-only alias is not read back; reading
    # accepts names and input aliases. round_trip leaves out computed fields.
    dumped = model.model_dump(mode="json", by_alias=False, round_trip=True)
    if _unmask_secrets.get() and _contains(dumped, _SECRET_MASK):
        # A checkpoint must hold a secret's value, as a pickled one does, or replay loses it.
        secrets = model.model_dump(mode="python", by_alias=False, round_trip=True)
        dumped = _unmask(secrets, dumped)
    return to_portable(dumped)


def _contains(value: Any, target: str) -> bool:
    if isinstance(value, dict):
        return any(_contains(v, target) for v in value.values())
    if isinstance(value, list):
        return any(_contains(v, target) for v in value)
    return bool(value == target)


def _unmask(python_dump: Any, json_dump: Any) -> Any:
    """Put secret values from a python-mode dump into the matching places of a JSON dump."""
    get_secret_value = getattr(python_dump, "get_secret_value", None)
    if callable(get_secret_value):
        secret = get_secret_value()
        # Pydantic reads bytes from JSON as UTF-8 text.
        return secret.decode("utf-8") if isinstance(secret, bytes) else secret
    if (
        isinstance(json_dump, dict)
        and isinstance(python_dump, dict)
        and len(json_dump) == len(python_dump)
    ):
        return {
            key: _unmask(python_value, json_value)
            for (key, json_value), python_value in zip(
                json_dump.items(), python_dump.values()
            )
        }
    if (
        isinstance(json_dump, list)
        and isinstance(python_dump, (list, tuple, set, frozenset, deque))
        and len(json_dump) == len(python_dump)
    ):
        return [_unmask(p, j) for p, j in zip(python_dump, json_dump)]
    return json_dump


# ---------- Decoding ----------


# Set while a union is choosing a member, which needs a scalar that does not parse to fail.
_strict_scalars: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "dbos_strict_scalars", default=False
)


def _is_primitive_of(hint: Any, value: Any) -> bool:
    """Whether a stored JSON value is of a primitive annotation's own type (an int is a float)."""
    if hint is bool:
        return isinstance(value, bool)
    if hint is int:
        return isinstance(value, int) and not isinstance(value, bool)
    if hint is float:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if hint is str:
        return isinstance(value, str)
    return value is None


def _scalar_mismatch(hint: Any, value: Any, path: str, reason: str) -> Any:
    """A stored scalar that does not parse as its annotation passes through, as a primitive does."""
    if _strict_scalars.get():
        raise _fail(hint, path, reason)
    return value


# Failed conversions of each object or array in this decode, and fits computed, so unions
# retrying members through nested unions stay polynomial rather than exponential. Successes
# aren't kept, as a result may be mutable and must not be shared between union candidates.
_memo: contextvars.ContextVar[Optional[Dict[Any, Tuple[Any, Any]]]] = (
    contextvars.ContextVar("dbos_convert_memo", default=None)
)


def value_to_type(hint: Any, value: Any, path: str = "$") -> Any:
    """Convert a value loaded from portable JSON to the type ``hint`` describes.

    ``path`` names the value in error messages."""
    # A fresh decode, even when user code starts one inside another (say from __post_init__).
    memo_token = _memo.set({})
    strict_token = _strict_scalars.set(False)
    try:
        return _convert(hint, value, path)
    finally:
        _strict_scalars.reset(strict_token)
        _memo.reset(memo_token)


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
        or _is_typeddict(hint)
    )


def _is_lenient(hint: Any) -> bool:
    """A hint whose mismatched values pass through rather than fail, as primitives do."""
    return (
        _is_passthrough(hint)
        or _is_opaque(hint)
        or hint in _PRIMITIVES
        or typing.get_origin(hint) is Literal
    )


def _unwrap(hint: Any) -> Any:
    """Strip NewType, Annotated and type-alias wrappers down to the hint they name."""
    while True:
        supertype = getattr(hint, "__supertype__", None)
        origin = typing.get_origin(hint)
        if supertype is not None:
            hint = supertype
        elif origin is typing.Annotated or origin is typing.Final:
            hint = typing.get_args(hint)[0]
        elif isinstance(origin, _TYPE_ALIAS_TYPES):
            # A generic alias, like Pair[date] for `type Pair[T] = tuple[T, T]`. Checked
            # first, as on 3.10 a parameterized alias also passes isinstance for its origin.
            hint = _expand_generic_alias(hint)
        elif isinstance(hint, _TYPE_ALIAS_TYPES):
            hint = _alias_value(hint)
        else:
            return hint


_UNPACKS = tuple(
    u
    for u in (
        getattr(typing, "Unpack", None),
        getattr(typing_extensions, "Unpack", None),
    )
    if u is not None
)


def _is_type_var_tuple(param: Any) -> bool:
    return type(param).__name__ == "TypeVarTuple"


def _is_unpacked(arg: Any) -> bool:
    """Whether a tuple argument is a variadic `*Ts`, which stands for any number of items."""
    return typing.get_origin(arg) in _UNPACKS or (
        getattr(arg, "__unpacked__", False) is True
    )


def resolve_nested_strings(hint: Any, namespace: Dict[str, Any]) -> Any:
    """Evaluate quoted names left as arguments of builtin generics, like `list["Item"]`.

    get_type_hints leaves these on 3.10 and inside type-alias values. Literal and
    Annotated contents are data, so they are never evaluated."""
    if isinstance(hint, typing.ForwardRef):
        return eval(hint.__forward_arg__, namespace)
    origin = typing.get_origin(hint)
    args = typing.get_args(hint)
    if not args or origin is Literal:
        return hint
    if origin is typing.Annotated:
        # Only the annotated type is a type; the metadata after it is data.
        annotated = resolve_nested_strings(args[0], namespace)
        if annotated is args[0]:
            return hint
        return typing.Annotated[(annotated, *hint.__metadata__)]
    if isinstance(hint, types.GenericAlias):
        resolved = tuple(_resolve_generic_arg(a, namespace) for a in args)
    else:
        resolved = tuple(resolve_nested_strings(a, namespace) for a in args)
    if all(r is a for r, a in zip(resolved, args)):
        return hint
    if isinstance(hint, types.GenericAlias):
        return types.GenericAlias(origin, resolved)
    if _is_union(origin):
        return Union[resolved]
    copy_with = getattr(hint, "copy_with", None)
    return copy_with(resolved) if callable(copy_with) else hint


def _resolve_generic_arg(arg: Any, namespace: Dict[str, Any]) -> Any:
    # A builtin generic keeps a quoted argument as a plain string.
    if isinstance(arg, str):
        return eval(arg, namespace)
    return resolve_nested_strings(arg, namespace)


def _expand_generic_alias(hint: Any) -> Any:
    """A parameterized alias's value with its arguments bound, cached so that it keeps one
    identity for the memo; 3.14 builds a new object for every substitution."""
    try:
        hash(hint)
    except TypeError:
        # Unhashable arguments, like dict metadata in an Annotated.
        return _bind_generic_alias(hint)
    # Keyed on the repr too, as typing treats unions differing only in member order as equal.
    return _bind_generic_alias_cached(hint, repr(hint))


@functools.lru_cache(maxsize=1024)
def _bind_generic_alias_cached(hint: Any, _: str) -> Any:
    return _bind_generic_alias(hint)


def _bind_generic_alias(hint: Any) -> Any:
    origin = typing.get_origin(hint)
    params = getattr(origin, "__type_params__", ())
    if any(_is_type_var_tuple(p) for p in params):
        return Any
    return _substitute(_alias_value(origin), dict(zip(params, typing.get_args(hint))))


@functools.lru_cache(maxsize=1024)
def _alias_value(alias: Any) -> Any:
    """A type alias's value, evaluated lazily; one that cannot be evaluated converts nothing."""
    try:
        module = sys.modules.get(getattr(alias, "__module__", ""), typing)
        return resolve_nested_strings(alias.__value__, dict(vars(module)))
    except Exception as e:
        # Cached, so this warns once per alias.
        dbos_logger.warning(
            f"DBOS cannot resolve the type alias {alias!r}, so values are not converted to it: {e}"
        )
        return Any


_get_type_hints: Any = (
    typing_extensions.get_type_hints
    if typing_extensions is not None
    else typing.get_type_hints
)


@functools.lru_cache(maxsize=1024)
def _field_hints(cls: Any) -> Dict[str, Any]:
    """A record type's field hints; any that cannot be resolved are left out."""
    try:
        if getattr(cls, "__type_params__", ()):
            hints = _get_type_hints(cls, localns=_class_locals(cls))
        else:
            # Without localns, get_type_hints also resolves names from each class body.
            hints = _get_type_hints(cls)
        return _resolve_field_strings(cls, typing.cast(Dict[str, Any], hints))
    except Exception:
        pass
    hints = {}
    failed: Dict[str, str] = {}
    for base in reversed(getattr(cls, "__mro__", (cls,))):
        namespace = dict(vars(sys.modules.get(base.__module__, typing)))
        localns = _class_locals(base)
        for name, annotation in _own_annotations(base).items():
            probe = types.new_class(f"_Probe_{name}")
            probe.__annotations__ = {name: annotation}
            probe.__module__ = base.__module__
            try:
                hints.update(
                    _get_type_hints(probe, globalns=namespace, localns=localns)
                )
                failed.pop(name, None)
            except Exception as e:
                failed[name] = str(e)
    hints = _resolve_field_strings(cls, hints)
    if failed:
        # Cached, so this warns once per type.
        dbos_logger.warning(
            f"DBOS cannot resolve the types of {_hint_name(cls)} fields {', '.join(failed)}, so they are not converted to them: {next(iter(failed.values()))}"
        )
    return hints


def _class_locals(cls: Any) -> Dict[str, Any]:
    """Names a class's annotations may use beyond its module: types nested in its body, and its PEP 695 parameters.

    Only nested types, so a field's default never shadows a module name its annotation uses.
    """
    localns = {
        name: value
        for name, value in (vars(cls).items() if isinstance(cls, type) else ())
        if isinstance(value, type)
    }
    localns.update({p.__name__: p for p in getattr(cls, "__type_params__", ())})
    return localns


def _resolve_field_strings(cls: Any, hints: Dict[str, Any]) -> Dict[str, Any]:
    if sys.version_info >= (3, 11):
        return hints
    # Each field resolves where it was declared: its class's module and body.
    namespaces: Dict[str, Dict[str, Any]] = {}
    for base in reversed(getattr(cls, "__mro__", (cls,))):
        # Module names win over the class body's, as get_type_hints resolves them.
        namespace = {
            **_class_locals(base),
            **vars(sys.modules.get(getattr(base, "__module__", ""), typing)),
        }
        for name in vars(base).get("__annotations__", {}):
            namespaces[name] = namespace
    resolved: Dict[str, Any] = {}
    for name, hint in hints.items():
        try:
            resolved[name] = resolve_nested_strings(hint, namespaces.get(name, {}))
        except Exception:
            resolved[name] = hint
    return resolved


def _own_annotations(cls: Any) -> Dict[str, Any]:
    if sys.version_info >= (3, 14):
        import annotationlib

        try:
            return dict(
                annotationlib.get_annotations(
                    cls, format=annotationlib.Format.FORWARDREF
                )
            )
        except Exception:
            return {}
    return dict(vars(cls).get("__annotations__", {}))


def _plain(value: Any) -> Any:
    """The JSON form portable serialization gives a value, or None if it has none."""
    try:
        return to_portable(value)
    except TypeError:
        return None


def _same_plain(expected: Any, value: Any) -> bool:
    plain = _plain(expected)
    # 1 == True in Python, but not in JSON.
    return type(plain) is type(value) and plain == value


def _convert(hint: Any, value: Any, path: str) -> Any:
    memo = _memo.get()
    if memo is None or not isinstance(value, (dict, list)):
        return _convert_value(hint, value, path)
    try:
        # By identity, as stored JSON is a tree whose nodes outlive the decode, and as
        # typing counts unions that differ only in member order as equal. The hint itself
        # is part of the key to keep it alive, so its id is not reused.
        key = (id(hint), hint, id(value), _strict_scalars.get())
        hit = memo.get(key)
    except TypeError:
        return _convert_value(hint, value, path)
    if hit is not None:
        raise hit[0]
    try:
        return _convert_value(hint, value, path)
    except DBOSSerializationError as e:
        memo[key] = (e, value)
        raise


def _convert_value(hint: Any, value: Any, path: str) -> Any:
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
        if not _is_primitive_of(hint, value):
            return _scalar_mismatch(hint, value, path, f"not a {_hint_name(hint)}")
        return value
    if hint in (bytes, bytearray):
        if not isinstance(value, str):
            return _scalar_mismatch(hint, value, path, "not a base64 string")
        try:
            return hint(base64.b64decode(value, validate=True))
        except ValueError as e:
            return _scalar_mismatch(hint, value, path, f"invalid base64: {e}")
    if hint is datetime:
        return _convert_datetime(value, path)
    if hint is date:
        if isinstance(value, date):
            return value
        if not isinstance(value, str):
            return _scalar_mismatch(hint, value, path, "not an ISO date string")
        try:
            # Languages without a date type send a full timestamp for one.
            return isoparse(value).date()
        except (ValueError, OverflowError):
            return _scalar_mismatch(hint, value, path, f"invalid ISO date {value!r}")
    if hint is Decimal:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            return _scalar_mismatch(hint, value, path, "not a decimal")
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return _scalar_mismatch(hint, value, path, f"invalid decimal {value!r}")
    if hint is uuid.UUID:
        if not isinstance(value, str):
            return _scalar_mismatch(hint, value, path, "not a UUID string")
        try:
            return uuid.UUID(value)
        except ValueError:
            return _scalar_mismatch(hint, value, path, f"invalid UUID {value!r}")

    if isinstance(hint, type) and issubclass(hint, Enum):
        return _convert_enum(hint, value, path)

    if _is_pydantic_model(hint):
        if isinstance(value, hint):
            return value
        try:
            # JSON-mode validation reads back the JSON forms model_dump wrote, even in strict mode.
            return _validate_model_json(hint, json.dumps(value))
        except Exception as e:
            error = e
        pruned = _without_forbidden_extras(value, error)
        if pruned is not None:
            try:
                return _validate_model_json(hint, json.dumps(pruned))
            except Exception:
                pass
        raise _fail(hint, path, str(error)) from error

    if dataclasses.is_dataclass(origin) and isinstance(origin, type):
        return _convert_dataclass(origin, _dataclass_hints(origin, args), value, path)
    if isinstance(hint, type) and dataclasses.is_dataclass(hint):
        return _convert_dataclass(hint, _dataclass_hints(hint, ()), value, path)

    record = origin if origin is not None else hint
    if _is_namedtuple(record):
        return _convert_namedtuple(record, _dataclass_hints(record, args), value, path)

    if _is_typeddict(record):
        return _convert_typeddict(record, _dataclass_hints(record, args), value, path)

    container = origin if origin is not None else hint
    if container is tuple:
        # A bare tuple has no arguments; `Tuple[()]` has none either but holds no items.
        bare = origin is None or hint is typing.Tuple
        return _convert_tuple(hint, args, bare, value, path)
    if container in _MAPPING_ORIGINS:
        return _convert_mapping(hint, container, args, value, path)
    if container in _SET_ORIGINS:
        return _convert_set(hint, container, args, value, path)
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


def _without_forbidden_extras(value: Any, error: Exception) -> Any:
    """The value without the keys pydantic rejected as extra, at any depth, or None if it rejected none.

    A stored subclass, or a field since removed, leaves keys a forbid-extra model doesn't
    declare; they are ignored, as unknown dataclass fields are."""
    errors = getattr(error, "errors", None)
    if not callable(errors):
        return None
    locations = [e["loc"] for e in errors() if e.get("type") == "extra_forbidden"]
    if not locations:
        return None
    pruned = json.loads(json.dumps(value))
    targets = []
    for location in locations:
        node = pruned
        for step in location[:-1]:
            if isinstance(node, dict) and step in node:
                node = node[step]
            elif isinstance(node, list) and isinstance(step, int) and step < len(node):
                node = node[step]
            else:
                # A step that names no stored key, like a union member's tag: the key may be
                # real for another member, so pruning could pick the wrong one.
                return None
        if not isinstance(node, dict) or location[-1] not in node:
            return None
        targets.append((node, location[-1]))
    for node, key in targets:
        node.pop(key, None)
    return pruned


def _field_aliases(field: Any) -> set[str]:
    """The keys besides its name a pydantic field may be read from."""
    aliases = {field.alias} if field.alias else set()
    validation_alias = field.validation_alias
    for choice in [validation_alias, *getattr(validation_alias, "choices", ())]:
        if isinstance(choice, str):
            aliases.add(choice)
        path = getattr(choice, "path", None)
        if path and isinstance(path[0], str):
            aliases.add(path[0])
    return aliases


def _validate_model_json(model: Any, data: str) -> Any:
    # While a union is choosing, a member only accepts values of its own JSON types.
    strict = True if _strict_scalars.get() else None
    if _validates_by_name(model):
        # Dumped under field names, so read names; aliases too, for other writers, unless
        # an alias is another field's name, which would then be read into the wrong field.
        by_alias = not _alias_names_a_field(model)
        return model.model_validate_json(
            data, by_alias=by_alias, by_name=True, strict=strict
        )
    return model.model_validate_json(data, strict=strict)


@functools.lru_cache(maxsize=1024)
def _alias_names_a_field(model: Any) -> bool:
    """Whether a model, or any record nested in its fields, has an alias naming another
    field, since pydantic reads every nested record by alias too."""
    try:
        return _alias_collision(model, {})
    except Exception:
        # Reading by name alone is right for anything DBOS wrote, so it's the safe answer.
        return True


def _alias_collision(hint: Any, visited: Dict[int, Any]) -> bool:
    # By identity, as a recursive alias expands to itself again; kept alive so ids aren't reused.
    if id(hint) in visited:
        return False
    visited[id(hint)] = hint
    unwrapped = _unwrap(hint)
    record = typing.get_origin(unwrapped) or unwrapped
    aliases = _record_aliases(record)
    if aliases is None:
        return any(_alias_collision(a, visited) for a in typing.get_args(unwrapped))
    names = set(aliases)
    if any(
        alias != name and alias in names
        for name, field_aliases in aliases.items()
        for alias in field_aliases
    ):
        return True
    # A model keeping extra keys may store one under any alias's name.
    if _keeps_extra_keys(record) and any(
        alias != name
        for name, field_aliases in aliases.items()
        for alias in field_aliases
    ):
        return True
    return any(
        _alias_collision(field_hint, visited)
        for field_hint in _record_field_hints(record, typing.get_args(unwrapped))
    )


def _pydantic_fields(cls: Any) -> Optional[Dict[str, Any]]:
    """A pydantic model's or pydantic dataclass's fields, or None for another type."""
    if not isinstance(cls, type):
        return None
    fields = getattr(cls, "model_fields", None)
    if fields is None:
        fields = getattr(cls, "__pydantic_fields__", None)
    return typing.cast(Optional[Dict[str, Any]], fields)


def _record_aliases(record: Any) -> Optional[Dict[str, set[str]]]:
    """A record type's field names with the keys besides each name pydantic reads it from,
    or None for a type that isn't a record."""
    fields = _pydantic_fields(record)
    if fields is not None:
        return {name: _field_aliases(field) for name, field in fields.items()}
    if not (
        (isinstance(record, type) and dataclasses.is_dataclass(record))
        or _is_typeddict(record)
    ):
        return None
    # Plain dataclasses and TypedDicts take aliases from Annotated[..., Field(alias=...)].
    try:
        hints = _get_type_hints(record, include_extras=True)
    except Exception:
        hints = {}
    aliases: Dict[str, set[str]] = {}
    for name, hint in hints.items():
        aliases[name] = set()
        # TypedDict qualifiers like NotRequired may wrap the Annotated.
        while typing.get_origin(hint) in _TYPED_DICT_QUALIFIERS:
            hint = typing.get_args(hint)[0]
        for metadata in getattr(hint, "__metadata__", ()):
            if hasattr(metadata, "validation_alias"):
                aliases[name] |= _field_aliases(metadata)
    if isinstance(record, type) and dataclasses.is_dataclass(record):
        # Or a dataclass field's default may be a pydantic Field.
        for field in dataclasses.fields(record):
            if hasattr(field.default, "validation_alias"):
                aliases.setdefault(field.name, set()).update(
                    _field_aliases(field.default)
                )
    return aliases


_TYPED_DICT_QUALIFIERS = tuple(
    q
    for module in (typing, typing_extensions)
    for q in (
        getattr(module, "Required", None),
        getattr(module, "NotRequired", None),
        getattr(module, "ReadOnly", None),
    )
    if q is not None
)


def _record_field_hints(record: Any, args: Tuple[Any, ...]) -> List[Any]:
    fields = _pydantic_fields(record)
    if fields is not None and not dataclasses.is_dataclass(record):
        return [field.annotation for field in fields.values()]
    # Dataclasses, pydantic ones included, and TypedDicts, with their type arguments bound.
    return list(_dataclass_hints(record, args).values())


@functools.lru_cache(maxsize=1024)
def _validates_by_name(model: Any) -> bool:
    """Whether this pydantic version takes by_name, added in 2.11."""
    try:
        return "by_name" in inspect.signature(model.model_validate_json).parameters
    except Exception:
        return False


def _convert_datetime(value: Any, path: str) -> Any:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return _scalar_mismatch(datetime, value, path, "not an ISO datetime string")
    try:
        return isoparse(value)
    except (ValueError, OverflowError):
        return _scalar_mismatch(
            datetime, value, path, f"invalid ISO datetime {value!r}"
        )


def _convert_enum(hint: Any, value: Any, path: str) -> Any:
    if isinstance(value, hint):
        return value
    try:
        return hint(value)
    except Exception:
        pass
    # Values JSON cannot hold natively, like dates or tuples, are stored in another form.
    for member in hint:
        if _plain(member.value) == value:
            return member
    return _scalar_mismatch(hint, value, path, f"{value!r} is not a member")


def _record_fields(cls: Any) -> Optional[List[set[str]]]:
    """The keys each field of a record type may be stored under, or None for a non-record hint."""
    cls = _unwrap(cls)
    if _is_pydantic_model(cls):
        if getattr(cls, "__pydantic_root_model__", False):
            return None
        return [
            {name} | ({field.alias} if field.alias else set())
            for name, field in cls.model_fields.items()
        ]
    base = typing.get_origin(cls) or cls
    if isinstance(base, type) and dataclasses.is_dataclass(base):
        return [{f.name} for f in dataclasses.fields(base)]
    if _is_typeddict(base):
        return [{name} for name in _field_hints(base)]
    return None


def _keeps_extra_keys(cls: Any) -> bool:
    """Whether a record type keeps keys it does not declare, so they are no misfit."""
    cls = _unwrap(cls)
    return _is_pydantic_model(cls) and cls.model_config.get("extra") == "allow"


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


# How well a hint's shape fits a stored value: no unknown keys, then fields used, then minus fields missing.
_Fit = Tuple[bool, int, int]
_NEUTRAL_FIT: _Fit = (True, 0, 0)
# A member that cannot hold the stored shape at all ranks below any that can.
_MISFIT: _Fit = (False, -1, -(2**31))


def _combine(fits: Iterable[_Fit]) -> _Fit:
    no_extra, used, missing = True, 0, 0
    for fit in fits:
        no_extra = no_extra and fit[0]
        used += fit[1]
        missing += fit[2]
    return (no_extra, used, missing)


def _fit(hint: Any, value: Any) -> _Fit:
    memo = _memo.get()
    if memo is None or not isinstance(value, (dict, list)):
        return _compute_fit(hint, value)
    try:
        key = ("fit", id(hint), hint, id(value))
        hit = memo.get(key)
    except TypeError:
        return _compute_fit(hint, value)
    if hit is not None:
        return typing.cast(_Fit, hit[0])
    fit = _compute_fit(hint, value)
    memo[key] = (fit, value)
    return fit


@functools.lru_cache(maxsize=1024)
def _root_hint(model: Any) -> Any:
    # Pydantic may leave quoted names in the root, as on 3.10, which resolve in the model's module.
    root = model.model_fields["root"].annotation
    namespace = {
        **_class_locals(model),
        **vars(sys.modules.get(getattr(model, "__module__", ""), typing)),
    }
    try:
        return resolve_nested_strings(root, namespace)
    except Exception:
        return root


@functools.lru_cache(maxsize=1024)
def _enum_shapes(enum: Any) -> frozenset[type]:
    return frozenset(type(_plain(member.value)) for member in enum)


def _compute_fit(hint: Any, value: Any) -> _Fit:
    if value is None:
        return _NEUTRAL_FIT
    hint = _unwrap(hint)
    if isinstance(hint, (str, typing.ForwardRef)):
        # A name left unresolved, like a quoted one inside a RootModel's root on 3.10.
        return _NEUTRAL_FIT
    origin = typing.get_origin(hint)
    args = typing.get_args(hint)
    if _is_union(origin):
        return max(_fit(a, value) for a in args if a is not _NoneType)
    if _is_passthrough(hint):
        return _NEUTRAL_FIT
    if _is_pydantic_model(hint) and getattr(hint, "__pydantic_root_model__", False):
        return _fit(_root_hint(hint), value)
    if isinstance(hint, type) and issubclass(hint, Enum):
        # An enum holds an object or array only if a member's value is stored as one.
        if isinstance(value, (dict, list)) and type(value) not in _enum_shapes(hint):
            return _MISFIT
        return _NEUTRAL_FIT
    container = origin if origin is not None else hint
    if isinstance(value, dict):
        fields = _record_fields(hint)
        if fields is not None:
            keys = set(value)
            known = set().union(*fields) if fields else set()
            used = sum(1 for names in fields if names & keys)
            no_extra = keys <= known or _keeps_extra_keys(hint)
            return (no_extra, used, used - len(fields))
        if container in _MAPPING_ORIGINS:
            if len(args) > 1:
                return _combine(_fit(args[1], v) for v in value.values())
            return _NEUTRAL_FIT
        # Anything else, from an array to an enum, cannot hold an object.
        return _MISFIT
    if isinstance(value, list):
        if _is_namedtuple(container) or _is_pydantic_model(container):
            return _NEUTRAL_FIT
        if not (
            container in (tuple, list, deque)
            or container in _SET_ORIGINS
            or container in _ABSTRACT_SEQUENCE_ORIGINS
        ):
            return _MISFIT
        if not args:
            return _NEUTRAL_FIT
        if origin is tuple and not (len(args) == 2 and args[1] is Ellipsis):
            return _combine(_fit(h, v) for h, v in zip(args, value))
        return _combine(_fit(args[0], v) for v in value)
    return _NEUTRAL_FIT


def _convert_union(hint: Any, args: Tuple[Any, ...], value: Any, path: str) -> Any:
    members = [a for a in args if a is not _NoneType]
    if len(members) == 1:
        # Optional[X] chooses nothing, so it converts exactly as X does.
        return _convert(members[0], value, path)
    # Whether an enclosing union is choosing a member, and so needs this one to fail strictly.
    nested = _strict_scalars.get()
    token = _strict_scalars.set(True)
    try:
        return _choose_union_member(hint, args, value, path, nested)
    finally:
        _strict_scalars.reset(token)


def _convert_chosen(member: Any, value: Any, path: str, nested: bool) -> Any:
    """Convert a union member once chosen, as it converts outside a union unless an enclosing union is still choosing."""
    token = _strict_scalars.set(nested)
    try:
        return _convert(member, value, path)
    finally:
        _strict_scalars.reset(token)


def _choose_union_member(
    hint: Any, args: Tuple[Any, ...], value: Any, path: str, nested: bool
) -> Any:
    members = [_unwrap(a) for a in args if a is not _NoneType]
    # A primitive of the stored JSON type needs no conversion.
    if type(value) in _PRIMITIVES and type(value) in members:
        return value
    for member in members:
        if typing.get_origin(member) is Literal and any(
            _same_plain(a, value) for a in typing.get_args(member)
        ):
            return _convert_chosen(member, value, path, nested)
    if isinstance(value, dict):
        # A matching Literal discriminator picks the member outright.
        for member in members:
            literals = _literal_fields(member)
            if literals and all(
                name in value and any(_same_plain(a, value[name]) for a in allowed)
                for name, allowed in literals.items()
            ):
                return _convert_chosen(member, value, path, nested)

    # Members are tried best fit first, ties in declared order, so each success is converted
    # once and the fit decides; converting every member would repeat at each level of nesting.
    candidates: List[Tuple[_Fit, int, Any]] = []
    for index, member in enumerate(members):
        if _is_lenient(member):
            continue
        if (
            typing.get_origin(member) in _ABSTRACT_SEQUENCE_ORIGINS
            or member in _ABSTRACT_SEQUENCE_ORIGINS
        ) and not isinstance(value, list):
            # It would accept a str or dict unchanged, which says nothing about a fit.
            continue
        if member in _SCALAR_TYPES and isinstance(value, (dict, list)):
            # Stored as strings or numbers, so it passes an object or array through unconverted.
            continue
        fit = _fit(member, value) if isinstance(value, (dict, list)) else _NEUTRAL_FIT
        candidates.append((fit, -index, member))
    candidates.sort(key=lambda c: (c[0], c[1]), reverse=True)
    # Members that failed, which may have failed only on a scalar deep inside.
    failed: List[Tuple[_Fit, int, Any]] = []
    last_error: Optional[Exception] = None
    for fit, order, member in candidates:
        try:
            converted = _convert(member, value, path)
        except RecursionError:
            raise
        except Exception as e:
            last_error = e
            if isinstance(value, (dict, list)):
                failed.append((fit, order, member))
            continue
        better_failed = [f for f in failed if f[0] > fit]
        if not better_failed:
            return converted
        # A member that fits the stored shape better, failing only on a value that does not
        # parse, converts as it would outside a union rather than lose to a worse fit. While an
        # enclosing union is choosing, fail instead, so it retries its whole member that way.
        if nested:
            raise _fail(
                hint, path, f"no union member accepts it strictly ({last_error})"
            )
        for _, _, better in better_failed:
            try:
                return _convert_chosen(better, value, path, nested)
            except DBOSSerializationError:
                continue
        return converted
    if nested and failed:
        raise _fail(hint, path, f"no union member accepts it strictly ({last_error})")
    for _, _, member in failed:
        try:
            return _convert_chosen(member, value, path, nested)
        except DBOSSerializationError:
            continue
    # As with a bare primitive, a mismatched value passes through rather than fails.
    if any(_is_lenient(m) for m in members):
        return value
    raise _fail(hint, path, f"no union member accepts it ({last_error})")


def _class_bindings(cls: Any) -> Dict[Any, Any]:
    """The type arguments a class's generic bases bind, like T -> Cat for `CatBox(Box[Cat])`."""
    bindings: Dict[Any, Any] = {}
    for base in getattr(cls, "__orig_bases__", ()):
        origin = typing.get_origin(base)
        if origin is None:
            continue
        bindings.update(_class_bindings(origin))
        params = getattr(origin, "__parameters__", ())
        bindings.update(zip(params, typing.get_args(base)))
    return bindings


def _dataclass_hints(cls: Any, args: Tuple[Any, ...]) -> Dict[str, Any]:
    """Field hints of a dataclass, with the type arguments it or its bases bind substituted."""
    hints = _field_hints(cls)
    bindings = _class_bindings(cls)
    params = getattr(cls, "__parameters__", ())
    if params and len(params) == len(args):
        bindings.update(zip(params, args))
    if not bindings:
        return hints
    # A base's parameter may be bound to another parameter, so resolve chains.
    for _ in range(len(bindings)):
        bindings = {k: _substitute(v, bindings) for k, v in bindings.items()}
    return {name: _substitute(h, bindings) for name, h in hints.items()}


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
        if _takes_fields(cls, init_args):
            instance = cls(**init_args)
        else:
            # A class with its own __init__ may not take its fields; build it from them as pickle would.
            instance = cls.__new__(cls)
            for field in dataclasses.fields(cls):
                if field.name in stored:
                    continue
                if field.default is not dataclasses.MISSING:
                    stored[field.name] = field.default
                elif field.default_factory is not dataclasses.MISSING:
                    stored[field.name] = field.default_factory()
                else:
                    raise TypeError(f"missing field {field.name!r}")
    except Exception as e:
        raise _fail(cls, path, str(e)) from e
    # Restore the stored state over whatever __post_init__ derived, including init=False fields.
    for name, field_value in stored.items():
        object.__setattr__(instance, name, field_value)
    return instance


def _lazy_signature(cls: Any) -> inspect.Signature:
    try:
        return inspect.signature(cls)
    except Exception:
        # On 3.14 a signature evaluates annotations, which may name a TYPE_CHECKING-only import.
        if sys.version_info >= (3, 14):
            import annotationlib

            return inspect.signature(
                cls, annotation_format=annotationlib.Format.FORWARDREF
            )
        raise


def _takes_fields(cls: Any, init_args: Dict[str, Any]) -> bool:
    """Whether a dataclass's __init__ accepts its stored fields, as one dataclass generates does."""
    try:
        signature = _lazy_signature(cls)
    except Exception:
        return True
    try:
        signature.bind(**init_args)
        return True
    except TypeError:
        pass
    if not all(name in signature.parameters for name in init_args):
        return False
    # An InitVar is never stored, so a required one makes __init__ unusable for decoding;
    # a missing required field fails here too, and the constructor reports it better.
    init_var = getattr(dataclasses, "_FIELD_INITVAR", None)
    init_vars = {
        name
        for name, f in getattr(cls, "__dataclass_fields__", {}).items()
        if init_var is not None and getattr(f, "_field_type", None) is init_var
    }
    missing = {
        name
        for name, p in signature.parameters.items()
        if p.default is inspect.Parameter.empty
        and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
        and name not in init_args
    }
    return not (missing and missing <= init_vars)


def _convert_namedtuple(cls: Any, hints: Dict[str, Any], value: Any, path: str) -> Any:
    if isinstance(value, cls):
        return value
    if not isinstance(value, list):
        raise _fail(cls, path, f"expected an array, got {type(value).__name__}")
    if len(value) > len(cls._fields):
        raise _fail(cls, path, f"expected at most {len(cls._fields)} items")
    items = [
        _convert(hints.get(name), item, f"{path}[{i}]")
        for i, (name, item) in enumerate(zip(cls._fields, value))
    ]
    try:
        return cls(*items)
    except Exception as e:
        raise _fail(cls, path, str(e)) from e


def _convert_typeddict(cls: Any, hints: Dict[str, Any], value: Any, path: str) -> Any:
    if not isinstance(value, dict):
        raise _fail(cls, path, f"expected an object, got {type(value).__name__}")
    return {k: _convert(hints.get(k), v, f"{path}.{k}") for k, v in value.items()}


def _convert_items(
    hint: Any, args: Tuple[Any, ...], value: Any, path: str
) -> List[Any]:
    if not isinstance(value, list):
        raise _fail(hint, path, f"expected an array, got {type(value).__name__}")
    item_hint = args[0] if args else None
    return [_convert(item_hint, item, f"{path}[{i}]") for i, item in enumerate(value)]


def _hashable(value: Any) -> Any:
    """A set element stored as an array was a tuple, the only array type a set can hold."""
    if isinstance(value, list):
        return tuple(_hashable(v) for v in value)
    return value


def _convert_set(
    hint: Any, container: Any, args: Tuple[Any, ...], value: Any, path: str
) -> Any:
    items = [_hashable(item) for item in _convert_items(hint, args, value, path)]
    try:
        return frozenset(items) if container is frozenset else set(items)
    except TypeError as e:
        raise _fail(hint, path, str(e)) from e


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
    if any(_is_unpacked(a) for a in args):
        # A variadic tuple says nothing about how many items each type covers.
        return tuple(value)
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


def _key_form(value: Any) -> Optional[str]:
    """The string portable serialization writes for a dict key, or None if it writes none."""
    try:
        return portable_key(value)
    except TypeError:
        return None


def _convert_key(hint: Any, key: Any, path: str) -> Any:
    """Rebuild a dict key, which portable JSON always stores as a string."""
    hint = _unwrap(hint)
    if not isinstance(key, str) or _is_passthrough(hint) or hint is str:
        return key
    key_path = f"{path} key {key!r}"
    origin = typing.get_origin(hint)
    if _is_union(origin):
        # Keys are always strings, so a str member is the last resort, and another
        # member claims a key only if it writes exactly that key back.
        members = sorted(
            (_unwrap(m) for m in typing.get_args(hint)), key=lambda m: m is str
        )
        for member in members:
            if typing.get_origin(member) is Literal and not any(
                _key_form(allowed) == key for allowed in typing.get_args(member)
            ):
                continue
            token = _strict_scalars.set(True)
            try:
                converted = _convert_key(member, key, path)
            except DBOSSerializationError:
                continue
            finally:
                _strict_scalars.reset(token)
            if member is str or _key_form(converted) == key:
                return converted
        # As with a bare Literal key, an unclaimed key passes a union with a lenient member.
        if any(_is_lenient(m) for m in members):
            return key
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
        return _scalar_mismatch(hint, key, key_path, "not a member")
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
    except (KeyError, ValueError):
        return _scalar_mismatch(hint, key, key_path, "invalid key")
    return _convert(hint, key, key_path)
