import functools
import inspect
import sys
import typing
import weakref
from dataclasses import dataclass, field
from enum import Enum
from types import FunctionType
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type

from dbos._error import DBOSWorkflowFunctionNotFoundError
from dbos._logger import dbos_logger
from dbos._serialization import WorkflowSerializationFormat
from dbos._type_convert import resolve_nested_strings

DEFAULT_MAX_RECOVERY_ATTEMPTS = 100


def get_dbos_func_name(f: Any) -> str:
    if hasattr(f, "dbos_function_name"):
        return str(getattr(f, "dbos_function_name"))
    raise DBOSWorkflowFunctionNotFoundError(
        "<NONE>", f"{f.__name__} is not a registered workflow function"
    )


def set_dbos_func_name(f: Any, name: str) -> None:
    setattr(f, "dbos_function_name", name)


@dataclass
class DBOSClassInfo:
    registered_name: str
    def_required_roles: Optional[List[str]] = None


class DBOSFuncType(Enum):
    Unknown = 0
    Bare = 1
    Static = 2
    Class = 3
    Instance = 4


ValidateArgsCallable = Callable[
    [Tuple[Any, ...], dict[str, Any]], Tuple[Tuple[Any, ...], dict[str, Any]]
]


@dataclass
class DBOSFuncInfo:
    class_info: Optional[DBOSClassInfo] = None
    func_type: DBOSFuncType = DBOSFuncType.Unknown
    required_roles: Optional[List[str]] = None
    max_recovery_attempts: Optional[int] = DEFAULT_MAX_RECOVERY_ATTEMPTS
    auto_upgrade: Optional[bool] = None
    serialization_type: Optional["WorkflowSerializationFormat"] = None
    validate_args: Optional[ValidateArgsCallable] = None


def get_or_create_class_info(
    cls: Type[Any], provided_name: Optional[str] = None
) -> DBOSClassInfo:
    if hasattr(cls, "dbos_class_decorator_info"):
        ci: DBOSClassInfo = getattr(cls, "dbos_class_decorator_info")
        return ci
    class_name = _class_fqn(cls)
    # Use the provided name instead of the class name if it is not None
    if provided_name is not None:
        class_name = provided_name
    ci = DBOSClassInfo(registered_name=class_name)
    setattr(cls, "dbos_class_decorator_info", ci)

    # Tell all DBOS functions about this
    for name, attribute in cls.__dict__.items():
        # Check if the attribute is a function or method
        if isinstance(attribute, (FunctionType, staticmethod, classmethod)):
            dbft = DBOSFuncType.Unknown
            if isinstance(attribute, staticmethod):
                attribute = attribute.__func__
                dbft = DBOSFuncType.Static
            elif isinstance(attribute, classmethod):
                attribute = attribute.__func__
                dbft = DBOSFuncType.Class
            elif isinstance(attribute, FunctionType):
                dbft = DBOSFuncType.Instance

            # Walk down the __wrapped__ chain
            wrapped = attribute
            while True:
                # Annotate based on the type
                if hasattr(wrapped, "dbos_func_decorator_info"):
                    fi: DBOSFuncInfo = getattr(wrapped, "dbos_func_decorator_info")
                    fi.class_info = ci
                    fi.func_type = dbft

                if not hasattr(wrapped, "__wrapped__"):
                    break
                wrapped = wrapped.__wrapped__

    return ci


def get_func_info(func: Callable[..., Any]) -> Optional[DBOSFuncInfo]:
    while True:
        if hasattr(func, "dbos_func_decorator_info"):
            fi: DBOSFuncInfo = getattr(func, "dbos_func_decorator_info")
            return fi
        if not hasattr(func, "__wrapped__"):
            break
        func = func.__wrapped__
    return None


@dataclass(frozen=True)
class FuncTypeHints:
    # The parameters a call's stored arguments bind to, without a leading self/cls.
    params: List[inspect.Parameter]
    hints: Dict[str, Any]
    # Annotations that could not be resolved, by name ("return" for the return type), with why.
    unresolved: Dict[str, str] = field(default_factory=dict)

    @property
    def return_hint(self) -> Any:
        return self.hints.get("return")


# Keyed by function, then by whether it was reached bound (its signature then drops self) and
# the func_type the entry was built for, as a later class decorator changes the self/cls skip.
_type_hints_cache: (
    "weakref.WeakKeyDictionary[Any, Dict[Tuple[bool, DBOSFuncType], FuncTypeHints]]"
) = weakref.WeakKeyDictionary()

# Qualified names already warned about, so a function warns once, however often it is rebound.
_unresolved_warned: Set[str] = set()

if sys.version_info >= (3, 14):
    import annotationlib

    _FORWARDREF: Any = annotationlib.Format.FORWARDREF
else:
    _FORWARDREF = None


def _signature_params(func: Callable[..., Any]) -> List[inspect.Parameter]:
    try:
        return list(inspect.signature(func).parameters.values())
    except Exception:
        # On 3.14 a signature evaluates its annotations, any of which may fail to.
        if _FORWARDREF is None:
            return []
        try:
            return list(
                inspect.signature(
                    func, annotation_format=_FORWARDREF
                ).parameters.values()
            )
        except Exception:
            return []


def _hint_namespace(target: Any) -> Dict[str, Any]:
    namespace = getattr(target, "__globals__", None)
    if namespace is None:
        namespace = getattr(getattr(target, "__func__", None), "__globals__", {})
    return typing.cast(Dict[str, Any], namespace)


def _safe_unwrap(func: Any) -> Any:
    try:
        return inspect.unwrap(func)
    except ValueError:
        # A __wrapped__ cycle.
        return func


def _hint_target(func: Callable[..., Any]) -> Any:
    """The function whose annotations describe a callable's parameters and result."""
    target = _safe_unwrap(func)
    while isinstance(target, functools.partial):
        target = _safe_unwrap(target.func)
    if not (
        inspect.isfunction(target)
        or inspect.ismethod(target)
        or inspect.isbuiltin(target)
        or inspect.isclass(target)
    ) and callable(target):
        # A callable object's signature is its __call__'s, not its attributes'.
        target = target.__call__
    return target


def _resolve_each(target: Any) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Resolve a function's annotations one at a time, so one that fails spoils no other."""
    try:
        if sys.version_info >= (3, 14):
            annotations = dict(
                annotationlib.get_annotations(target, format=_FORWARDREF)
            )
        else:
            annotations = dict(getattr(target, "__annotations__", None) or {})
    except Exception as e:
        return {}, {"*": str(e)}
    namespace = _hint_namespace(target)
    hints: Dict[str, Any] = {}
    unresolved: Dict[str, str] = {}
    for name, annotation in annotations.items():
        probe = FunctionType(_resolve_each.__code__, namespace)
        probe.__annotations__ = {name: annotation}
        try:
            hints.update(typing.get_type_hints(probe))
        except Exception as e:
            unresolved[name] = str(e)
    return hints, unresolved


def get_func_type_hints(func: Callable[..., Any]) -> FuncTypeHints:
    """A function's resolved type hints, cached; any that cannot be resolved are left out."""
    fi = get_func_info(func)
    func_type = fi.func_type if fi is not None else DBOSFuncType.Unknown
    # Keyed by the wrapped function, which outlives a wrapper made per call.
    key = _safe_unwrap(func)
    variant = (inspect.ismethod(func), func_type)
    try:
        cached = _type_hints_cache.get(key, {}).get(variant)
    except TypeError:
        cached = None
    if cached is not None:
        return cached
    target = _hint_target(func)
    unresolved: Dict[str, str] = {}
    if inspect.isclass(target):
        hints: Dict[str, Any] = {}
    else:
        try:
            hints = typing.get_type_hints(target)
        except Exception:
            hints, unresolved = _resolve_each(target)
        if sys.version_info < (3, 11):
            for name, hint in list(hints.items()):
                try:
                    hints[name] = resolve_nested_strings(hint, _hint_namespace(target))
                except Exception as e:
                    del hints[name]
                    unresolved[name] = str(e)
    if unresolved:
        module = getattr(target, "__module__", None)
        qualname = getattr(target, "__qualname__", str(target))
        if module:
            qualname = f"{module}.{qualname}"
        if qualname not in _unresolved_warned:
            _unresolved_warned.add(qualname)
            dbos_logger.warning(
                f"DBOS cannot resolve the type hints of {qualname} ({', '.join(unresolved)}): "
                f"{next(iter(unresolved.values()))}; values read back for them are not converted to their annotated types"
            )
    params = _signature_params(func)
    # A bound method's signature already leaves out self/cls.
    if func_type in (
        DBOSFuncType.Class,
        DBOSFuncType.Instance,
    ) and not inspect.ismethod(func):
        params = params[1:]
    result = FuncTypeHints(params, hints, unresolved)
    try:
        _type_hints_cache.setdefault(key, {})[variant] = result
    except TypeError:
        pass
    return result


def get_or_create_func_info(func: Callable[..., Any]) -> DBOSFuncInfo:
    fi = get_func_info(func)
    if fi is not None:
        return fi

    fi = DBOSFuncInfo()
    setattr(func, "dbos_func_decorator_info", fi)
    return fi


def set_func_info(func: Callable[..., Any], fi: DBOSFuncInfo) -> None:
    setattr(func, "dbos_func_decorator_info", fi)


def get_class_info(cls: Type[Any]) -> Optional[DBOSClassInfo]:
    if hasattr(cls, "dbos_class_decorator_info"):
        ci: DBOSClassInfo = getattr(cls, "dbos_class_decorator_info")
        return ci
    return None


def get_class_info_for_func(fi: Optional[DBOSFuncInfo]) -> Optional[DBOSClassInfo]:
    if fi and fi.class_info:
        return fi.class_info

    # Bare function or function on something else
    return None


def get_config_name(
    fi: Optional[DBOSFuncInfo], func: Callable[..., Any], args: Tuple[Any, ...]
) -> Optional[str]:
    if fi and fi.func_type != DBOSFuncType.Unknown and len(args) > 0:
        if fi.func_type == DBOSFuncType.Instance:
            first_arg = args[0]
            if hasattr(first_arg, "config_name"):
                iname: str = getattr(first_arg, "config_name")
                return str(iname)
            else:
                raise Exception(
                    "Function target appears to be a class instance, but does not have `config_name` set"
                )
        return None

    # Check for improperly-registered functions
    if len(args) > 0:
        first_arg = args[0]
        if isinstance(first_arg, type):
            raise Exception(
                "Function target appears to be a class, but is not properly registered"
            )
        else:
            # Check if the function signature has "self" as the first parameter name
            #   This is not 100% reliable but it is better than nothing for detecting footguns
            sig = inspect.signature(func)
            parameters = list(sig.parameters.values())
            if parameters and parameters[0].name == "self":
                raise Exception(
                    "Function target appears to be a class instance, but is not properly registered"
                )

    # Bare function or function on something else
    return None


def _class_fqn(cls: type) -> str:
    """Returns the registered name of the given class. If the class name was not overridden at registration time, it returns the qualified name of the class."""
    ci = get_class_info(cls)
    if ci is not None:
        return ci.registered_name
    return cls.__qualname__


def get_dbos_class_name(
    fi: Optional[DBOSFuncInfo], func: Callable[..., Any], args: Tuple[Any, ...]
) -> Optional[str]:
    if fi and fi.func_type != DBOSFuncType.Unknown and len(args) > 0:
        if fi.func_type == DBOSFuncType.Instance:
            first_arg = args[0]
            return _class_fqn(first_arg.__class__)
        if fi.func_type == DBOSFuncType.Class:
            first_arg = args[0]
            return _class_fqn(first_arg)
        return None

    # Check for improperly-registered functions
    if len(args) > 0:
        first_arg = args[0]
        if isinstance(first_arg, type):
            raise Exception(
                "Function target appears to be a class, but is not properly registered"
            )
        else:
            # Check if the function signature has "self" as the first parameter name
            #   This is not 100% reliable but it is better than nothing for detecting footguns
            sig = inspect.signature(func)
            parameters = list(sig.parameters.values())
            if parameters and parameters[0].name == "self":
                raise Exception(
                    "Function target appears to be a class instance, but is not properly registered"
                )

    # Bare function or function on something else
    return None
