import collections
import functools
import json
import sys
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum, IntEnum
from typing import (
    Annotated,
    Any,
    Counter,
    Dict,
    FrozenSet,
    Generic,
    List,
    Literal,
    NamedTuple,
    NewType,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

import pytest
from pydantic import BaseModel, Field

from dbos import KafkaMessage, make_pydantic_args_validator
from dbos._error import DBOSSerializationError
from dbos._registrations import get_func_type_hints
from dbos._serialization import (
    DBOSDefaultSerializer,
    DBOSPortableJSON,
    Serializer,
    deserialize_value,
)
from dbos._type_convert import value_to_type


class Status(Enum):
    PAID = "paid"
    SHIPPED = "shipped"


class Priority(IntEnum):
    LOW = 1
    HIGH = 2


@dataclass
class Item:
    sku: str
    qty: int


@dataclass
class Order:
    id: str
    placed: datetime
    total: Decimal
    status: Status
    items: List[Item]
    ref: uuid.UUID
    note: Optional[str] = None
    tags: Set[str] = field(default_factory=set)


class Point(NamedTuple):
    x: int
    when: date


class Movie(TypedDict):
    title: str
    released: date


class Customer(BaseModel):
    name: str
    since: datetime
    orders: List[Item]


@dataclass
class Cat:
    kind: Literal["cat"]
    lives: int


@dataclass
class Dog:
    kind: Literal["dog"]
    lives: int


UserId = NewType("UserId", uuid.UUID)


def roundtrip(value: Any, hint: Any) -> Any:
    return DBOSPortableJSON.deserialize_typed(DBOSPortableJSON.serialize(value), hint)


def test_dataclass_roundtrip() -> None:
    order = Order(
        id="A1",
        placed=datetime(2026, 10, 27, 14, 0, tzinfo=timezone.utc),
        total=Decimal("19.99"),
        status=Status.PAID,
        items=[Item("x", 2), Item("y", 1)],
        ref=uuid.uuid4(),
        tags={"rush"},
    )
    stored = json.loads(DBOSPortableJSON.serialize(order))
    # Stored as plain portable JSON, with no type information.
    assert stored["status"] == "paid"
    assert stored["total"] == "19.99"
    assert stored["items"] == [{"sku": "x", "qty": 2}, {"sku": "y", "qty": 1}]
    assert roundtrip(order, Order) == order
    # Without a hint the plain JSON comes back.
    assert DBOSPortableJSON.deserialize(DBOSPortableJSON.serialize(order)) == stored


def test_pydantic_namedtuple_typeddict() -> None:
    customer = Customer(
        name="ann",
        since=datetime(2025, 1, 2, tzinfo=timezone.utc),
        orders=[Item("x", 1)],
    )
    assert roundtrip(customer, Customer) == customer
    assert roundtrip(Point(1, date(2026, 1, 2)), Point) == Point(1, date(2026, 1, 2))
    movie: Movie = {"title": "M", "released": date(1999, 3, 31)}
    assert roundtrip(movie, Movie) == movie


def test_scalars() -> None:
    assert roundtrip(Priority.HIGH, Priority) is Priority.HIGH
    assert roundtrip(Status.SHIPPED, Status) is Status.SHIPPED
    assert roundtrip(Decimal("1.50"), Decimal) == Decimal("1.50")
    ref = uuid.uuid4()
    assert roundtrip(ref, uuid.UUID) == ref
    assert roundtrip(ref, UserId) == ref
    assert roundtrip(date(2026, 2, 3), date) == date(2026, 2, 3)
    assert roundtrip(b"\x00\xffbytes", bytes) == b"\x00\xffbytes"
    assert roundtrip(bytearray(b"ab"), bytearray) == bytearray(b"ab")
    assert roundtrip(5, Annotated[int, "meta"]) == 5
    # Bytes are stored as base64 so other languages read a plain string.
    assert json.loads(DBOSPortableJSON.serialize(b"hi")) == "aGk="


def test_containers() -> None:
    assert roundtrip(deque([Item("a", 1)]), deque[Item]) == deque([Item("a", 1)])
    assert roundtrip(frozenset({1, 2}), FrozenSet[int]) == frozenset({1, 2})
    assert roundtrip((1, "a", Status.PAID), Tuple[int, str, Status]) == (
        1,
        "a",
        Status.PAID,
    )
    assert roundtrip((1, 2, 3), Tuple[int, ...]) == (1, 2, 3)
    assert roundtrip([Item("a", 1)], list[Item]) == [Item("a", 1)]


def test_non_string_dict_keys() -> None:
    ref = uuid.uuid4()
    assert roundtrip({1: Item("a", 1)}, Dict[int, Item]) == {1: Item("a", 1)}
    assert roundtrip({1.5: "x"}, Dict[float, str]) == {1.5: "x"}
    assert roundtrip({True: 1, False: 2}, Dict[bool, int]) == {True: 1, False: 2}
    assert roundtrip({None: 1}, Dict[None, int]) == {None: 1}
    assert roundtrip({ref: 1}, Dict[uuid.UUID, int]) == {ref: 1}
    assert roundtrip({Status.PAID: 1}, Dict[Status, int]) == {Status.PAID: 1}
    # Without a key hint the keys stay strings.
    assert roundtrip({1: "a"}, dict) == {"1": "a"}
    with pytest.raises(TypeError):
        DBOSPortableJSON.serialize({(1, 2): "tuple key"})


def test_unions() -> None:
    assert roundtrip(None, Optional[Order]) is None
    assert roundtrip("2026-01-01T00:00:00Z", Optional[datetime]) == datetime(
        2026, 1, 1, tzinfo=timezone.utc
    )
    assert roundtrip(3, Union[int, str]) == 3
    # The Literal discriminator picks the member even though both shapes match.
    assert roundtrip(Dog("dog", 1), Union[Cat, Dog]) == Dog("dog", 1)
    assert roundtrip([Cat("cat", 9), Dog("dog", 1)], List[Cat | Dog]) == [
        Cat("cat", 9),
        Dog("dog", 1),
    ]


def test_lenient_cases() -> None:
    # Unknown fields are ignored, so removing a field stays compatible.
    assert value_to_type(Item, {"sku": "a", "qty": 1, "gone": True}) == Item("a", 1)
    # Primitives are not coerced, and a null is never converted.
    assert value_to_type(int, "3") == "3"
    assert value_to_type(Order, None) is None
    # Hints with no portable encoding pass the value through.
    assert value_to_type(Any, {"a": 1}) == {"a": 1}


def test_shape_mismatch_raises() -> None:
    with pytest.raises(DBOSSerializationError, match=r"\$\.items\[0\]"):
        value_to_type(Order, {"id": "a", "items": ["not an item"]})
    with pytest.raises(DBOSSerializationError, match="qty"):
        value_to_type(Item, {"sku": "a"})
    with pytest.raises(DBOSSerializationError):
        value_to_type(datetime, "not a date")
    with pytest.raises(DBOSSerializationError):
        value_to_type(Status, "refunded")
    with pytest.raises(DBOSSerializationError):
        value_to_type(Customer, {"name": "x"})
    with pytest.raises(DBOSSerializationError):
        value_to_type(Union[Cat, Item], {"kind": "fish"})


def test_serializer_dispatch() -> None:
    class PlainSerializer(Serializer):
        def serialize(self, data: Any) -> str:
            return json.dumps(data)

        def deserialize(self, serialized_data: str) -> Any:
            return json.loads(serialized_data)

        def name(self) -> str:
            return "plain"

    class TypedSerializer(PlainSerializer):
        def deserialize_typed(self, serialized_data: str, type_hint: Any) -> Any:
            return (type_hint, self.deserialize(serialized_data))

        def name(self) -> str:
            return "typed"

    # A serializer that predates deserialize_typed ignores the hint.
    plain = PlainSerializer()
    assert deserialize_value('{"sku":"a","qty":1}', "plain", plain, Item) == {
        "sku": "a",
        "qty": 1,
    }
    typed = TypedSerializer()
    assert deserialize_value("1", "typed", typed, int) == (int, 1)
    assert deserialize_value("1", "typed", typed) == 1
    # Pickle stores types itself, so the hint is ignored.
    pickled = DBOSDefaultSerializer.serialize({"sku": "a", "qty": 1})
    assert deserialize_value(pickled, "py_pickle", plain, Item) == {
        "sku": "a",
        "qty": 1,
    }
    assert deserialize_value('{"sku":"a","qty":1}', "portable_json", plain, Item) == (
        Item("a", 1)
    )


def test_function_hints() -> None:
    def priced(order: Order, rush: bool) -> Item:
        return Item("a", 1)

    hints = get_func_type_hints(priced)
    assert hints.return_hint is Item
    assert [p.name for p in hints.params] == ["order", "rush"]
    # A partial reports its function's hints, without the bound arguments.
    partial_hints = get_func_type_hints(functools.partial(priced, cast(Order, None)))
    assert partial_hints.return_hint is Item
    assert [p.name for p in partial_hints.params] == ["rush"]

    # An unresolvable annotation falls back to no conversion.
    def unresolved(x: "NoSuchType") -> "NoSuchType":  # type: ignore[name-defined] # noqa: F821
        return x

    unresolved_hints = get_func_type_hints(unresolved)
    assert unresolved_hints.hints == {}
    assert unresolved_hints.unresolved is not None and (
        "NoSuchType" in unresolved_hints.unresolved
    )
    # The pydantic validator refuses to run without hints instead of accepting anything.
    with pytest.raises(ValueError, match="NoSuchType"):
        make_pydantic_args_validator(unresolved)(("x",), {})


@pytest.mark.skipif(sys.version_info < (3, 14), reason="needs lazy annotations")
def test_function_hints_type_checking_only_name() -> None:
    # On 3.14 an unquoted TYPE_CHECKING-only name is legal until something evaluates it.
    namespace: Dict[str, Any] = {}
    exec("def step(x: int) -> MissingAtRuntime:\n    return x\n", namespace)
    hints = get_func_type_hints(namespace["step"])
    assert hints.hints == {} and hints.unresolved is not None
    assert [p.name for p in hints.params] == ["x"]


def test_union_members_convert_before_primitives() -> None:
    order_item = Item("a", 1)
    # A primitive member accepts only its own JSON type, so it never swallows another member's value.
    assert roundtrip(order_item, Union[str, Item]) == order_item
    assert roundtrip(order_item, Union[Literal["auto"], Item]) == order_item
    assert roundtrip([order_item], Union[int, List[Item]]) == [order_item]
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert roundtrip(when, Union[int, datetime]) == when
    assert roundtrip(Decimal("1.5"), Union[float, Decimal]) == Decimal("1.5")
    assert roundtrip("auto", Union[Literal["auto"], Item]) == "auto"
    assert roundtrip(2.5, Union[int, float]) == 2.5
    # A str stored for a union that also holds str stays a str: JSON cannot tell them apart.
    assert roundtrip("aGk=", Union[str, bytes]) == "aGk="
    with pytest.raises(DBOSSerializationError):
        value_to_type(Union[int, datetime], "not a date")


@dataclass
class Plain:
    name: str


@dataclass
class Tagged:
    name: str
    tags: List[str]


class Kind(Enum):
    A = "a"
    B = "b"


@dataclass
class KindA:
    kind: Literal[Kind.A]
    x: int


@dataclass
class KindB:
    kind: Literal[Kind.B]
    x: int
    when: datetime


def test_union_picks_best_fitting_record() -> None:
    tagged = Tagged("n", ["t"])
    assert roundtrip(tagged, Union[Plain, Tagged]) == tagged
    assert roundtrip(Plain("n"), Union[Tagged, Plain]) == Plain("n")
    # A Literal discriminator of a plain Enum matches its stored value.
    kind_b = KindB(Kind.B, 1, datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert roundtrip(kind_b, Union[KindA, KindB]) == kind_b
    assert roundtrip(KindA(Kind.A, 2), Union[KindA, KindB]) == KindA(Kind.A, 2)


@dataclass
class NonNegative:
    n: int

    def __post_init__(self) -> None:
        if self.n < 0:
            raise ValueError("negative")


@dataclass
class Signed:
    n: int


def test_union_member_constructor_errors_fall_through() -> None:
    assert roundtrip(Signed(-1), Union[NonNegative, Signed]) == Signed(-1)
    with pytest.raises(DBOSSerializationError, match="negative"):
        value_to_type(NonNegative, {"n": -1})


@dataclass
class Cents:
    amount: int

    def __post_init__(self) -> None:
        self.amount *= 100


@dataclass
class Job:
    id: str
    status: str = field(default="new", init=False)


def test_dataclass_restores_stored_state() -> None:
    # A __post_init__ that changes a field, and an init=False field, keep their stored values.
    cents = Cents(2)
    assert cents.amount == 200
    assert roundtrip(cents, Cents).amount == 200
    job = Job("a")
    job.status = "done"
    assert roundtrip(job, Job).status == "done"


class Aliased(BaseModel):
    user_name: str = Field(alias="userName")


def test_pydantic_alias_roundtrip() -> None:
    model = Aliased(userName="bob")
    assert json.loads(DBOSPortableJSON.serialize(model)) == {"userName": "bob"}
    assert roundtrip(model, Aliased) == model


class Colour(Enum):
    RED = 1
    PAIR = (1, 2)


def test_enum_keys_and_values() -> None:
    assert roundtrip({Priority.LOW: "x"}, Dict[Priority, str]) == {Priority.LOW: "x"}
    assert roundtrip({Colour.RED: "x"}, Dict[Colour, str]) == {Colour.RED: "x"}
    # Enum values with no native JSON form decode back to their member.
    assert roundtrip(Colour.PAIR, Colour) is Colour.PAIR
    assert roundtrip({1: "a", None: "b"}, Dict[Optional[int], str]) == {
        1: "a",
        None: "b",
    }
    assert roundtrip({1: "a"}, Dict[Literal[1, 2], str]) == {1: "a"}
    counts = roundtrip(collections.Counter({3: 2}), Counter[int])
    assert counts == collections.Counter({3: 2}) and isinstance(
        counts, collections.Counter
    )
    # Keys that would encode alike are refused rather than one silently dropped.
    with pytest.raises(TypeError, match="encode as"):
        DBOSPortableJSON.serialize({1: "a", "1": "b"})


T = TypeVar("T")


@dataclass
class Box(Generic[T]):
    item: T
    items: List[T]


class Pair(NamedTuple):
    a: int
    b: int


def test_generic_and_structural_edges() -> None:
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert roundtrip(Box(when, [when]), Box[datetime]) == Box(when, [when])
    # A str is a valid Sequence, so only arrays are rebuilt.
    assert value_to_type(Sequence[str], "abc") == "abc"
    assert value_to_type(Tuple[()], []) == ()
    with pytest.raises(DBOSSerializationError):
        value_to_type(Tuple[()], [1])
    with pytest.raises(DBOSSerializationError):
        value_to_type(Pair, [1, 2, 3])


def test_kafka_message_bytes_roundtrip() -> None:
    msg = KafkaMessage(
        headers=[("h", b"\x00\xff")],
        key=b"k",
        latency=None,
        leader_epoch=None,
        offset=1,
        partition=0,
        timestamp=(0, 0),
        topic="t",
        value=b"hello",
    )
    assert roundtrip(msg, KafkaMessage) == msg


def test_duck_typed_serializer_without_deserialize_typed() -> None:
    class DuckSerializer:
        def serialize(self, data: Any) -> str:
            return json.dumps(data)

        def deserialize(self, serialized_data: str) -> Any:
            return json.loads(serialized_data)

        def name(self) -> str:
            return "duck"

    duck: Any = DuckSerializer()
    assert deserialize_value("[1]", "duck", duck, List[int]) == [1]
