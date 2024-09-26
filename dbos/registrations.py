import inspect
from enum import Enum
from types import FunctionType
from typing import Any, Callable, List, Literal, Optional, Tuple, Type, cast

DEFAULT_MAX_RECOVERY_ATTEMPTS = 50


def get_dbos_func_name(f: Any) -> str:
    if hasattr(f, "dbos_function_name"):
        return str(getattr(f, "dbos_function_name"))
    if hasattr(f, "__qualname__"):
        return str(getattr(f, "__qualname__"))
    return "<unknown>"


def set_dbos_func_name(f: Any, name: str) -> None:
    setattr(f, "dbos_function_name", name)


TempWorkflowType = Literal["transaction", "step", "send", None]


def get_temp_workflow_type(f: Any) -> TempWorkflowType:
    if hasattr(f, "dbos_temp_workflow_type"):
        return cast(TempWorkflowType, str(getattr(f, "dbos_temp_workflow_type")))
    return None


def set_temp_workflow_type(f: Any, name: TempWorkflowType) -> None:
    setattr(f, "dbos_temp_workflow_type", name)


class DBOSClassInfo:
    def __init__(self) -> None:
        self.def_required_roles: Optional[List[str]] = None


class DBOSFuncType(Enum):
    Unknown = 0
    Bare = 1
    Static = 2
    Class = 3
    Instance = 4


class DBOSFuncInfo:
    def __init__(self) -> None:
        self.class_info: Optional[DBOSClassInfo] = None
        self.func_type: DBOSFuncType = DBOSFuncType.Unknown
        self.required_roles: Optional[List[str]] = None
        self.max_recovery_attempts = DEFAULT_MAX_RECOVERY_ATTEMPTS


def get_or_create_class_info(cls: Type[Any]) -> DBOSClassInfo:
    if hasattr(cls, "dbos_class_decorator_info"):
        ci: DBOSClassInfo = getattr(cls, "dbos_class_decorator_info")
        return ci
    ci = DBOSClassInfo()
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


def get_or_create_func_info(func: Callable[..., Any]) -> DBOSFuncInfo:
    fi = get_func_info(func)
    if fi is not None:
        return fi

    fi = DBOSFuncInfo()
    setattr(func, "dbos_func_decorator_info", fi)
    return fi


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


def get_dbos_class_name(
    fi: Optional[DBOSFuncInfo], func: Callable[..., Any], args: Tuple[Any, ...]
) -> Optional[str]:
    if fi and fi.func_type != DBOSFuncType.Unknown and len(args) > 0:
        if fi.func_type == DBOSFuncType.Instance:
            first_arg = args[0]
            return str(first_arg.__class__.__name__)
        if fi.func_type == DBOSFuncType.Class:
            first_arg = args[0]
            return str(first_arg.__name__)
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
