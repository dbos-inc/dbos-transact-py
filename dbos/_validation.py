"""Argument validation helpers for DBOS workflows.

Provides ``pydantic_args_validator`` — a ready-made ``validate_args`` hook that
uses `pydantic <https://docs.pydantic.dev/>`_ to coerce and validate deserialized
workflow arguments against the function's type hints.

Usage::

    from dbos import DBOS
    from dbos._validation import pydantic_args_validator

    @DBOS.workflow(validate_args=pydantic_args_validator)
    def my_workflow(name: str, count: int) -> str:
        ...
"""

from __future__ import annotations

import inspect
from typing import Any, Callable, Dict, Tuple

from ._registrations import DBOSFuncType, ValidateArgsCallable, get_func_info


def pydantic_args_validator(
    args: Tuple[Any, ...], kwargs: Dict[str, Any]
) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
    """Sentinel for pydantic-based argument validation.

    Pass this as ``validate_args`` on ``@DBOS.workflow``.  The decorator will
    replace it with a real validator built from the function's type hints.

    Should never be called directly at runtime.
    """
    raise NotImplementedError(
        "pydantic_args_validator should not be called directly. "
        "Use make_pydantic_args_validator(func) to create a validator for a specific function."
    )


def make_pydantic_args_validator(
    func: Callable[..., Any],
) -> ValidateArgsCallable:
    """Create a ``validate_args`` callable that validates arguments against *func*'s type hints.

    Returns a callable ``(args, kwargs) -> (args, kwargs)`` suitable for the
    ``validate_args`` parameter of ``@DBOS.workflow``.

    Adapters are built lazily on the first validation call so that
    ``DBOSFuncInfo.func_type`` (set by ``@DBOS.dbos_class``) is available to
    determine whether to skip the leading ``self``/``cls`` parameter.

    Requires pydantic >= 2.0.
    """
    try:
        from pydantic import TypeAdapter, ValidationError
    except ImportError:
        raise ImportError(
            "pydantic is required for pydantic_args_validator. "
            "Install it with: pip install pydantic  (or pip install dbos[validation])"
        )

    # Cache for lazily-built adapters.  Empty list means "not yet built".
    _cached_adapters: list[tuple[str, Any, Any]] = []
    _initialized = False

    def _build_adapters() -> list[tuple[str, Any, Any]]:
        sig = inspect.signature(func)
        hints = _get_type_hints_safe(func)
        params = list(sig.parameters.values())

        # Determine whether to skip the first parameter (self/cls) using the
        # function's registration info rather than sniffing parameter names.
        fi = get_func_info(func)
        if fi is not None and fi.func_type in (
            DBOSFuncType.Class,
            DBOSFuncType.Instance,
        ):
            params = params[1:]

        adapters: list[tuple[str, Any, Any]] = []
        for p in params:
            hint = hints.get(p.name)
            if hint is not None:
                adapters.append((p.name, TypeAdapter(hint), p.default))
            else:
                adapters.append((p.name, None, p.default))
        return adapters

    def _validate(
        args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
        nonlocal _initialized
        # Lazy init — first call builds the TypeAdapters.
        if not _initialized:
            _cached_adapters.extend(_build_adapters())
            _initialized = True
        adapters = _cached_adapters

        validated_args = list(args)
        validated_kwargs = dict(kwargs)

        errors: list[str] = []

        # Validate positional args
        for i, val in enumerate(validated_args):
            if i < len(adapters):
                name, adapter, _default = adapters[i]
                if adapter is not None:
                    try:
                        validated_args[i] = adapter.validate_python(val)
                    except ValidationError as e:
                        errors.append(f"Argument '{name}' (position {i}): {e}")

        # Validate keyword args
        adapter_by_name = {name: adapter for name, adapter, _ in adapters}
        for key, val in validated_kwargs.items():
            adapter = adapter_by_name.get(key)
            if adapter is not None:
                try:
                    validated_kwargs[key] = adapter.validate_python(val)
                except ValidationError as e:
                    errors.append(f"Argument '{key}': {e}")

        # Check for missing required arguments
        provided_names = set()
        for i in range(len(args)):
            if i < len(adapters):
                provided_names.add(adapters[i][0])
        provided_names.update(kwargs.keys())

        for name, _adapter, default in adapters:
            if name not in provided_names and default is inspect.Parameter.empty:
                errors.append(f"Missing required argument: '{name}'")

        if errors:
            raise ValueError(
                f"Argument validation failed for workflow:\n" + "\n".join(errors)
            )

        return tuple(validated_args), validated_kwargs

    return _validate


def _get_type_hints_safe(func: Callable[..., Any]) -> Dict[str, Any]:
    """Get type hints for a function, falling back to __annotations__ on failure."""
    try:
        import typing

        return typing.get_type_hints(func)
    except Exception:
        return getattr(func, "__annotations__", {})
