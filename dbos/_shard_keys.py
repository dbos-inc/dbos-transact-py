"""Shard keys a caller supplies for the workflows it addresses.

Core only records them; a sharded system database reads them to route each
statement. On an unsharded database nothing here is consulted.
"""

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Dict, Generator, Mapping, Optional, Tuple

_bindings: ContextVar[Optional[Dict[str, str]]] = ContextVar(
    "dbos_shard_key_bindings", default=None
)
_scope: ContextVar[Optional[str]] = ContextVar("dbos_shard_key_scope", default=None)


@contextmanager
def bind_shard_keys(keys: Mapping[str, Optional[str]]) -> Generator[None, None, None]:
    """Within the block, each workflow ID in keys lives under its shard key. None values are ignored."""
    given = {wfid: key for wfid, key in keys.items() if key is not None}
    if not given:
        yield
        return
    token = _bindings.set({**(_bindings.get() or {}), **given})
    try:
        yield
    finally:
        _bindings.reset(token)


def bound_shard_key(workflow_id: str) -> Optional[str]:
    """The shard key bound for this workflow ID, if any."""
    bindings = _bindings.get()
    return bindings.get(workflow_id) if bindings is not None else None


@contextmanager
def shard_key_scope(key: Optional[str]) -> Generator[None, None, None]:
    """Within the block, statements not addressing a specific workflow run under this shard key."""
    token = _scope.set(key)
    try:
        yield
    finally:
        _scope.reset(token)


def scoped_shard_key() -> Optional[str]:
    """The shard key of the enclosing scope, if any."""
    return _scope.get()


_Tokens = Tuple["Token[Optional[Dict[str, str]]]", "Token[Optional[str]]"]


def clear_shard_key_state() -> _Tokens:
    """Empty every binding and scope, so a workflow body inherits none from its caller."""
    return (_bindings.set(None), _scope.set(None))


def restore_shard_key_state(tokens: _Tokens) -> None:
    bindings, scope = tokens
    _scope.reset(scope)
    _bindings.reset(bindings)
