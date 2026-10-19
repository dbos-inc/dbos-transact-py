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
from typing import Any, Callable, Dict, Optional, Tuple

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

    # Lazily-built adapters, None until the first call.  Holds the named
    # parameters as (name, adapter, default, kind), then the (name, adapter) of
    # *args and of **kwargs.  It is published in a single assignment so that
    # concurrent first calls can never see a partially built or duplicated cache.
    _cached: Optional[
        tuple[list[tuple[str, Any, Any, Any]], tuple[str, Any], tuple[str, Any]]
    ] = None

    def _build_adapters() -> (
        tuple[list[tuple[str, Any, Any, Any]], tuple[str, Any], tuple[str, Any]]
    ):
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

        adapters: list[tuple[str, Any, Any, Any]] = []
        var_args: tuple[str, Any] = ("", None)
        var_kwargs: tuple[str, Any] = ("", None)
        for p in params:
            hint = hints.get(p.name)
            adapter: Any = TypeAdapter(hint) if hint is not None else None
            # *args and **kwargs accept any number of arguments, so none is required.
            if p.kind is p.VAR_POSITIONAL:
                var_args = (p.name, adapter)
            elif p.kind is p.VAR_KEYWORD:
                var_kwargs = (p.name, adapter)
            else:
                adapters.append((p.name, adapter, p.default, p.kind))
        return adapters, var_args, var_kwargs

    def _validate(
        args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
        nonlocal _cached
        # Lazy init — first call builds the TypeAdapters.
        cached = _cached
        if cached is None:
            cached = _cached = _build_adapters()
        adapters, var_args, var_kwargs = cached
        by_position = (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )

        validated_args = list(args)
        validated_kwargs = dict(kwargs)

        errors: list[str] = []

        # Validate positional args; those past the last parameter that can be
        # passed by position are collected by *args.
        for i, val in enumerate(validated_args):
            if i < len(adapters) and adapters[i][3] in by_position:
                name, adapter, _default, _kind = adapters[i]
            else:
                name, adapter = var_args
            if adapter is not None:
                try:
                    validated_args[i] = adapter.validate_python(val)
                except ValidationError as e:
                    errors.append(f"Argument '{name}' (position {i}): {e}")

        # Validate keyword args; those naming no parameter, or a positional-only
        # one, are collected by **kwargs.
        adapter_by_name = {
            name: adapter
            for name, adapter, _, kind in adapters
            if kind is not inspect.Parameter.POSITIONAL_ONLY
        }
        for key, val in validated_kwargs.items():
            adapter = adapter_by_name.get(key, var_kwargs[1])
            if adapter is not None:
                try:
                    validated_kwargs[key] = adapter.validate_python(val)
                except ValidationError as e:
                    errors.append(f"Argument '{key}': {e}")

        # Check for missing required arguments
        provided_names = set()
        for i in range(len(args)):
            if i < len(adapters) and adapters[i][3] in by_position:
                provided_names.add(adapters[i][0])
        provided_names.update(key for key in kwargs if key in adapter_by_name)

        for name, _adapter, default, _kind in adapters:
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
