from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Type, TypeVar, cast

from ._error import DBOSNotAuthorizedError

if TYPE_CHECKING:
    from ._dbos import DBOSRegistry

from ._context import DBOSAssumeRole, get_local_dbos_context
from ._registrations import (
    DBOSFuncInfo,
    get_class_info_for_func,
    get_dbos_func_name,
    get_or_create_class_info,
    get_or_create_func_info,
)

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")


def check_required_roles(
    func: Callable[..., Any], fi: Optional[DBOSFuncInfo]
) -> Optional[str]:
    # Check required roles
    required_roles: Optional[List[str]] = None
    # First, we need to know if this has class info and func info
    ci = get_class_info_for_func(fi)
    if ci is not None:
        required_roles = ci.def_required_roles
    if fi and fi.required_roles is not None:
        required_roles = fi.required_roles

    if required_roles is None or len(required_roles) == 0:
        return None  # Nothing to check

    ctx = get_local_dbos_context()
    if ctx is None or ctx.authenticated_roles is None:
        raise DBOSNotAuthorizedError(
            f"Function {get_dbos_func_name(func)} requires a role, but was called in a context without authentication information"
        )

    for r in required_roles:
        if r in ctx.authenticated_roles:
            return r

    raise DBOSNotAuthorizedError(
        f"Function {get_dbos_func_name(func)} has required roles, but user is not authenticated for any of them"
    )


def required_roles(roles: List[str]) -> Callable[[F], F]:
    def set_roles(func: F) -> F:
        fi = get_or_create_func_info(func)
        fi.required_roles = roles

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            rr: Optional[str] = check_required_roles(func, fi)
            with DBOSAssumeRole(rr):
                return func(*args, **kwargs)

        return cast(F, wrapper)

    return set_roles


def default_required_roles(
    dbosreg: "DBOSRegistry", roles: List[str]
) -> Callable[[Type[T]], Type[T]]:
    def set_roles(cls: Type[T]) -> Type[T]:
        ci = get_or_create_class_info(cls)
        dbosreg.register_class(cls, ci)
        ci.def_required_roles = roles
        return cls

    return set_roles
