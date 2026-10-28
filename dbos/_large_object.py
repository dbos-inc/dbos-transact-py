"""Large objects: values kept in an external object store, checkpointed as small refs."""

from typing import Generic, Protocol, Sequence, TypeVar

# typing.TypedDict can't be generic before Python 3.11; SQLAlchemy already requires typing_extensions.
from typing_extensions import TypedDict

T = TypeVar("T")


class LargeObjectRef(TypedDict, Generic[T]):
    """A reference to a value stored with DBOS.store_large_object.
    Plain data, so every serializer and SDK carries it unchanged."""

    version: int
    store: str
    key: str
    size: int
    sha256: str
    serialization: str
    encoding: str


class LargeObjectStore(Protocol):
    """Where large objects' bytes live. Its store_id names the location, so it must
    not change while refs to it exist."""

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
