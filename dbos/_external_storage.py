"""External storage: values kept in an external store, checkpointed as small refs."""

import collections.abc
from typing import Generic, List, Optional, Protocol, Sequence, TypeVar

# typing.TypedDict can't be generic before Python 3.11; SQLAlchemy already requires typing_extensions.
from typing_extensions import TypedDict

from ._error import DBOSInitializationError

T = TypeVar("T")


class ExternalObjectRef(TypedDict, Generic[T]):
    """A reference to a value stored with DBOS.store_external_object."""

    version: int
    store: str
    key: str
    size: int
    sha256: str
    serialization: str
    encoding: str


class ExternalStore(Protocol):
    """Where external objects' bytes live; each call retries its own transient failures.
    Its store_id names the location, so it must not change while refs to it exist."""

    @property
    def store_id(self) -> str: ...

    # How long garbage collection keeps an object whose owner workflow doesn't exist.
    @property
    def orphan_ttl_sec(self) -> float: ...

    def put(self, key: str, data: bytes) -> None: ...

    # Raises KeyError if no object has this key.
    def get(self, key: str) -> bytes: ...

    # Keys with no object are ignored.
    def delete(self, keys: Sequence[str]) -> None: ...


def check_external_stores(
    stores: Optional[Sequence[ExternalStore]],
) -> List[ExternalStore]:
    """The configured stores, each with its own store_id; empty means none."""
    if stores is not None and not isinstance(stores, collections.abc.Sequence):
        raise DBOSInitializationError(
            f"external_stores must be a list of stores, not a single {type(stores).__name__}; pass [store]"
        )
    configured = list(stores or [])
    ids = [store.store_id for store in configured]
    repeated = sorted({store_id for store_id in ids if ids.count(store_id) > 1})
    if repeated:
        raise DBOSInitializationError(
            f"Each external store needs its own store_id, but these repeat: {', '.join(repeated)}"
        )
    return configured
