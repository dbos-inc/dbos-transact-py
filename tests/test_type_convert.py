import collections
import dataclasses
import functools
import json
import sys
import types
import typing
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
import typing_extensions
from pydantic import (
    AliasChoices,
    BaseModel,
    Field,
    RootModel,
    SecretBytes,
    SecretStr,
    computed_field,
)

from dbos import KafkaMessage, make_pydantic_args_validator
from dbos._error import DBOSSerializationError
from dbos._registrations import get_func_type_hints
from dbos._serialization import (
    DBOSDefaultSerializer,
    DBOSPortableJSON,
    Serializer,
    deserialize_value,
    exception_to_workflow_error_data,
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
    # A scalar that does not parse passes through, as a primitive does, so it never strands a workflow.
    assert value_to_type(datetime, "not a date") == "not a date"
    assert value_to_type(Status, "refunded") == "refunded"
    assert value_to_type(Dict[int, str], {"abc": "x"}) == {"abc": "x"}
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
    assert set(unresolved_hints.unresolved) == {"x", "return"}
    # The pydantic validator refuses to run without a parameter's hint instead of accepting anything.
    with pytest.raises(ValueError, match="NoSuchType"):
        make_pydantic_args_validator(unresolved)(("x",), {})

    # One unresolvable annotation spoils none of the others.
    def return_only(x: int, when: datetime) -> "NoSuchType":  # type: ignore[name-defined] # noqa: F821
        return x

    partly = get_func_type_hints(return_only)
    assert partly.hints == {"x": int, "when": datetime}
    assert set(partly.unresolved) == {"return"}
    assert make_pydantic_args_validator(return_only)(
        ("5", "2026-01-01T00:00:00Z"), {}
    ) == ((5, datetime(2026, 1, 1, tzinfo=timezone.utc)), {})

    # A failure other than a missing name counts as unresolved too.
    def bad_attribute(x: "collections.NoSuchThing") -> int:  # type: ignore[name-defined]
        return 0

    assert set(get_func_type_hints(bad_attribute).unresolved) == {"x"}

    # A callable object is described by its __call__.
    class Pricer:
        def __call__(self, order: Order) -> Item:
            return Item("a", 1)

    assert get_func_type_hints(Pricer()).hints == {"order": Order, "return": Item}


@pytest.mark.skipif(sys.version_info < (3, 14), reason="needs lazy annotations")
def test_function_hints_type_checking_only_name() -> None:
    # On 3.14 an unquoted TYPE_CHECKING-only name is legal until something evaluates it.
    namespace: Dict[str, Any] = {}
    exec("def step(x: int) -> MissingAtRuntime:\n    return x\n", namespace)
    hints = get_func_type_hints(namespace["step"])
    assert hints.hints == {"x": int} and set(hints.unresolved) == {"return"}
    assert [p.name for p in hints.params] == ["x"]
    # An annotation that fails to evaluate for another reason keeps the parameters too.
    exec('def wf(a: int, b: int) -> int | "Foo":\n    return a\n', namespace)
    wf_hints = get_func_type_hints(namespace["wf"])
    assert [p.name for p in wf_hints.params] == ["a", "b"]
    assert wf_hints.hints == {"a": int, "b": int}


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
    # As with a bare primitive, a mismatched value passes a union with a primitive member.
    assert value_to_type(Union[int, datetime], "not a date") == "not a date"
    assert value_to_type(Optional[int], "3") == "3"
    assert value_to_type(Optional[Literal["a", "b"]], "c") == "c"
    with pytest.raises(DBOSSerializationError):
        value_to_type(Union[datetime, Item], "not a date")


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
    # Stored under field names, which reading accepts alongside input aliases.
    assert json.loads(DBOSPortableJSON.serialize(model)) == {"user_name": "bob"}
    assert value_to_type(Aliased, {"userName": "bob"}) == model
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


@dataclass
class Animal:
    name: str


@dataclass
class Hound(Animal):
    breed: str = "unknown"


@dataclass
class Leaf:
    kids: "List[Tree]"
    a: int = 0


@dataclass
class Branch:
    kids: "List[Tree]"
    b: int = 0


Tree = Union[Leaf, Branch]


def test_union_fit_through_containers() -> None:
    hound = Hound("rex", "lab")
    assert roundtrip([hound], Union[List[Animal], List[Hound]]) == [hound]
    assert roundtrip({"k": hound}, Union[Dict[str, Animal], Dict[str, Hound]]) == {
        "k": hound
    }
    # On a tie, the member with no fields missing from the stored object wins.
    assert type(roundtrip(Animal("x"), Union[Hound, Animal])) is Animal
    # A recursive union stops at the first exact fit instead of converting every member.
    tree: Any = Leaf([])
    for _ in range(40):
        tree = Leaf([tree])
    # Converting every member would take 2**40 conversions, far past the test timeout.
    assert roundtrip(tree, Tree) == tree


def test_bare_and_untyped_containers() -> None:
    assert roundtrip((1, 2), Tuple) == (1, 2)
    assert roundtrip({(1, 2), (3, 4)}, set) == {(1, 2), (3, 4)}
    assert roundtrip(frozenset({(1, 2)}), FrozenSet[Any]) == frozenset({(1, 2)})
    with pytest.raises(DBOSSerializationError):
        value_to_type(set, [{"a": 1}])


def test_union_keys_round_trip() -> None:
    # A key goes to a non-str member only if that member writes the same key back.
    assert value_to_type(Dict[Union[int, str], int], {"007": 1, "7": 2}) == {
        "007": 1,
        7: 2,
    }
    assert value_to_type(Dict[Union[float, str], int], {"nan": 1}) == {"nan": 1}


@dataclass
class GenericBox(Generic[T]):
    item: T


@dataclass
class AnimalBox(GenericBox[Animal]):
    label: str = ""


def test_inherited_and_aliased_generics() -> None:
    box = AnimalBox(Animal("x"), "l")
    assert roundtrip(box, AnimalBox) == box
    if sys.version_info >= (3, 12):
        namespace: Dict[str, Any] = {}
        exec("type Pair[T] = tuple[T, T]", namespace)
        pair = (date(2026, 1, 1), date(2026, 1, 2))
        assert roundtrip(pair, namespace["Pair"][date]) == pair


class Computed(BaseModel):
    model_config = {"extra": "forbid"}
    a: int

    @computed_field  # type: ignore[prop-decorator]
    @property
    def doubled(self) -> int:
        return self.a * 2


class Credentials(BaseModel):
    key: SecretStr
    raw: SecretBytes
    nested: List[SecretStr]


def test_pydantic_round_trip_edges() -> None:
    assert roundtrip(Computed(a=1), Computed) == Computed(a=1)
    creds = Credentials(
        key=SecretStr("hunter2"), raw=SecretBytes(b"pw"), nested=[SecretStr("s")]
    )
    decoded = roundtrip(creds, Credentials)
    # The checkpoint holds the secrets themselves, as a pickled one does.
    assert decoded.key.get_secret_value() == "hunter2"
    assert decoded.raw.get_secret_value() == b"pw"
    assert decoded.nested[0].get_secret_value() == "s"


class Holiday(Enum):
    NEW_YEAR = date(2026, 1, 1)
    BLOB = b"\x00\xff"


def test_enum_values_follow_their_encoding() -> None:
    assert roundtrip(Holiday.NEW_YEAR, Holiday) is Holiday.NEW_YEAR
    assert roundtrip(Holiday.BLOB, Holiday) is Holiday.BLOB
    assert roundtrip({Holiday.NEW_YEAR: 1}, Dict[Holiday, int]) == {Holiday.NEW_YEAR: 1}


def test_quoted_names_inside_builtin_generics() -> None:
    def takes(items: list["Item"], by_sku: dict[str, "Item"]) -> list["Item"]:
        return items

    hints = get_func_type_hints(takes)
    assert hints.hints == {
        "items": list[Item],
        "by_sku": dict[str, Item],
        "return": list[Item],
    }
    assert value_to_type(hints.hints["items"], [{"sku": "a", "qty": 1}]) == [
        Item("a", 1)
    ]


class StrictModel(BaseModel):
    model_config = {"strict": True}
    when: datetime
    tags: Set[str]
    pair: Tuple[int, int]


class ExtTypedDict(typing_extensions.TypedDict):
    when: datetime
    note: typing_extensions.NotRequired[date]


class GenericPair(NamedTuple):
    a: int
    when: datetime


WhenList = typing_extensions.TypeAliasType("WhenList", List[datetime])


def test_round3_record_types() -> None:
    strict = StrictModel(
        when=datetime(2026, 1, 1, tzinfo=timezone.utc), tags={"a"}, pair=(1, 2)
    )
    assert roundtrip(strict, StrictModel) == strict
    ext: ExtTypedDict = {
        "when": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "note": date(2026, 1, 2),
    }
    assert roundtrip(ext, ExtTypedDict) == ext
    whens = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
    assert roundtrip(whens, WhenList) == whens


def test_round3_scalars_and_keys() -> None:
    # Languages without a date type send a full timestamp for one.
    assert value_to_type(date, "2026-01-01T12:00:00.000Z") == date(2026, 1, 1)
    assert roundtrip(date(2026, 1, 1), date) == date(2026, 1, 1)
    assert roundtrip({(1, 2)}, Set[typing.Hashable]) == {(1, 2)}
    user_id = NewType("user_id", str)
    assert value_to_type(Dict[Union[user_id, int], str], {"1": "a"}) == {1: "a"}


@dataclass
class PartlyResolvable:
    when: datetime
    other: "NoSuchType"  # type: ignore[name-defined] # noqa: F821


def test_field_hints_resolve_per_field() -> None:
    # One unresolvable field annotation leaves the others converted.
    decoded = value_to_type(
        PartlyResolvable, {"when": "2026-01-01T00:00:00.000Z", "other": 1}
    )
    assert decoded.when == datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.mark.skipif(sys.version_info < (3, 12), reason="needs PEP 695")
def test_pep695_generics() -> None:
    # Dataclasses with string annotations need their module registered while defined.
    module = types.ModuleType("pep695_test")
    sys.modules["pep695_test"] = module
    try:
        exec(
            "from __future__ import annotations\n"
            "from dataclasses import dataclass\n"
            "from datetime import datetime\n"
            "@dataclass\n"
            "class Box[T]:\n"
            "    item: T\n"
            "    when: datetime\n"
            "type Missing = list[NoSuchType]\n",
            module.__dict__,
        )
        box_cls: Any = getattr(module, "Box")
        box = box_cls(date(2026, 1, 2), datetime(2026, 1, 1, tzinfo=timezone.utc))
        assert roundtrip(box, box_cls[date]) == box
        # An alias that cannot be evaluated passes values through instead of raising.
        assert value_to_type(getattr(module, "Missing"), [1]) == [1]
    finally:
        del sys.modules["pep695_test"]


@pytest.mark.skipif(sys.version_info < (3, 11), reason="generic NamedTuple is 3.11+")
def test_generic_namedtuple_and_typeddict() -> None:
    namespace: Dict[str, Any] = {}
    exec(
        "from datetime import datetime\n"
        "from typing import Generic, NamedTuple, TypedDict, TypeVar\n"
        "T = TypeVar('T')\n"
        "class GP(NamedTuple, Generic[T]):\n"
        "    a: T\n"
        "    when: datetime\n"
        "class GTD(TypedDict, Generic[T]):\n"
        "    a: T\n"
        "    when: datetime\n",
        namespace,
    )
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    gp = namespace["GP"](date(2026, 1, 2), when)
    assert roundtrip(gp, namespace["GP"][date]) == gp
    gtd = {"a": date(2026, 1, 2), "when": when}
    assert roundtrip(gtd, namespace["GTD"][date]) == gtd


@dataclass
class WithNestedEnum:
    class Shade(Enum):
        LIGHT = "light"

    shade: "WithNestedEnum.Shade"
    tone: "Literal['warm', 'cool']"
    picked: "Union[Literal['none'], Item]"


def test_round4_hint_resolution() -> None:
    value = WithNestedEnum(WithNestedEnum.Shade.LIGHT, "warm", Item("a", 1))
    assert roundtrip(value, WithNestedEnum) == value

    # Literal values are data, never evaluated as names.
    def paint(color: Literal["red", "blue"], fallback: Optional[Literal["x"]]) -> None:
        return None

    hints = get_func_type_hints(paint)
    assert hints.unresolved == {}
    assert hints.hints["color"] == Literal["red", "blue"]
    assert make_pydantic_args_validator(paint)(("red", None), {}) == (
        ("red", None),
        {},
    )

    # Quoted names nested in typing generics and unions resolve on every version.
    def nested(
        by_key: Dict[str, list["Item"]], maybe: "list[Item] | None"
    ) -> None:  # noqa: F821
        return None

    nested_hints = get_func_type_hints(nested).hints
    assert value_to_type(nested_hints["by_key"], {"k": [{"sku": "a", "qty": 1}]}) == {
        "k": [Item("a", 1)]
    }
    assert value_to_type(nested_hints["maybe"], [{"sku": "a", "qty": 1}]) == [
        Item("a", 1)
    ]


class ExtraDoc(BaseModel):
    model_config = {"extra": "allow"}
    title: str


class InputAliased(BaseModel):
    user_name: str = Field(validation_alias="userName")


@dataclass
class Event:
    name: str


@dataclass
class TimedEvent(Event):
    at: datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass
class Frozen:
    when: typing.Final[datetime]  # type: ignore[misc]


@dataclass
class Money:
    cents: int
    currency: str

    def __init__(self, amount: str) -> None:
        self.cents = int(Decimal(amount) * 100)
        self.currency = "USD"


def test_round4_records() -> None:
    # A record whose stored object has extra keys still beats an untouched dict or scalar.
    doc = ExtraDoc(title="t", note="n")  # type: ignore[call-arg]
    assert roundtrip(doc, Union[ExtraDoc, datetime]) == doc
    assert roundtrip(doc, Union[ExtraDoc, Dict[str, Any]]) == doc
    assert type(roundtrip(TimedEvent("e"), Union[Event, datetime])) is Event
    aliased = InputAliased.model_validate({"userName": "bob"})
    assert roundtrip(aliased, InputAliased) == aliased
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert roundtrip(Frozen(when), Frozen) == Frozen(when)
    money = Money("1.50")
    decoded = roundtrip(money, Money)
    assert (decoded.cents, decoded.currency) == (150, "USD")


@dataclass
class Config:
    name: str = ""
    retries: int = 3


@dataclass
class Quantity:
    n: int

    def __post_init__(self) -> None:
        if not isinstance(self.n, int):
            raise TypeError("n must be an int")


@dataclass
class Account:
    id: str
    tags: List[str] = field(default_factory=list)

    def __init__(self, raw: str) -> None:
        self.id = raw
        self.tags = []


def test_round5_unions_and_records() -> None:
    # An object with keys a record doesn't declare stays a dict when the union offers one.
    stored = {"name": "svc", "custom": 5}
    assert value_to_type(Union[Config, Dict[str, Any]], stored) == stored
    assert value_to_type(Union[Dict[str, Any], Config], stored) == stored
    # A scalar member still converts numbers.
    assert value_to_type(Optional[Decimal], 12.5) == Decimal("12.5")
    # A __post_init__ check is never bypassed.
    with pytest.raises(DBOSSerializationError, match="must be an int"):
        value_to_type(Quantity, {"n": "5"})
    # A class whose __init__ takes no fields is built from them, defaults included.
    assert value_to_type(Account, {"id": "a"}).tags == []


@pytest.mark.skipif(sys.version_info < (3, 12), reason="needs PEP 695")
def test_alias_forward_refs() -> None:
    module = types.ModuleType("alias_refs_test")
    sys.modules["alias_refs_test"] = module
    try:
        exec(
            "from dataclasses import dataclass\n"
            "from datetime import date\n"
            "from typing import Annotated, List, Optional\n"
            "type MaybeEntry = Optional['Entry']\n"
            "type Entries = List['Entry']\n"
            "type Tagged = Annotated[list['Entry'], 'meta']\n"
            "@dataclass\n"
            "class Entry:\n"
            "    d: date\n",
            module.__dict__,
        )
        entry = getattr(module, "Entry")(date(2026, 1, 2))
        assert value_to_type(getattr(module, "MaybeEntry"), {"d": "2026-01-02"}) == (
            entry
        )
        assert value_to_type(getattr(module, "Entries"), [{"d": "2026-01-02"}]) == [
            entry
        ]
        assert value_to_type(getattr(module, "Tagged"), [{"d": "2026-01-02"}]) == [
            entry
        ]
    finally:
        del sys.modules["alias_refs_test"]


@dataclass
class Credentialed:
    name: str
    password: dataclasses.InitVar[str]
    pw_hash: str = field(init=False)

    def __post_init__(self, password: str) -> None:
        self.pw_hash = f"h:{password}"


def test_required_init_var_decodes_from_stored_fields() -> None:
    # An InitVar is never stored, so the object is built from its stored fields instead.
    decoded = roundtrip(Credentialed("bob", "pw"), Credentialed)
    assert (decoded.name, decoded.pw_hash) == ("bob", "h:pw")


GenericPairAlias = typing_extensions.TypeAliasType(
    "GenericPairAlias", Tuple[T, T], type_params=(T,)
)


def test_round6_aliases_and_keys() -> None:
    pair = (date(2026, 1, 1), date(2026, 1, 2))
    assert roundtrip(pair, GenericPairAlias[date]) == pair
    # A Literal key member claims only the keys it names.
    assert value_to_type(
        Dict[Union[Literal["all"], int], str], {"all": "a", "5": "b"}
    ) == {"all": "a", 5: "b"}


@pytest.mark.skipif(sys.version_info < (3, 12), reason="needs PEP 695")
def test_variadic_tuples_and_nested_union_aliases() -> None:
    module = types.ModuleType("variadic_test")
    sys.modules["variadic_test"] = module
    try:
        exec(
            "from dataclasses import dataclass\n"
            "type Tup[*Ts] = tuple[*Ts]\n"
            "@dataclass\n"
            "class Circle:\n"
            "    r: float\n"
            "@dataclass\n"
            "class Square:\n"
            "    side: float\n"
            "@dataclass\n"
            "class Labeled:\n"
            "    r: float = 0.0\n"
            "    label: str = ''\n"
            "type Shape = Circle | Square\n",
            module.__dict__,
        )
        # A variadic alias passes its value through rather than rejecting it.
        assert value_to_type(getattr(module, "Tup")[int, date], [1, 2]) == [1, 2]
        # A union nested through an alias is scored by its best-fitting member.
        shape = value_to_type(
            Union[getattr(module, "Shape"), getattr(module, "Labeled")], {"r": 2.0}
        )
        assert type(shape).__name__ == "Circle"
    finally:
        del sys.modules["variadic_test"]


def test_round7_generic_typeddict_union_and_keys() -> None:
    namespace: Dict[str, Any] = {}
    exec(
        "from datetime import date\n"
        "from typing import Generic, Literal, TypeVar\n"
        "from typing_extensions import TypedDict\n"
        "T = TypeVar('T')\n"
        "class TA(TypedDict, Generic[T]):\n"
        "    kind: Literal['a']\n"
        "    when: T\n"
        "class TB(TypedDict, Generic[T]):\n"
        "    kind: Literal['b']\n"
        "    other: T\n",
        namespace,
    )
    hint: Any = Union[namespace["TA"][date], namespace["TB"][date]]
    assert value_to_type(hint, {"kind": "b", "other": "2026-01-01"}) == {
        "kind": "b",
        "other": date(2026, 1, 1),
    }
    # An unclaimed key passes a key union with a Literal member, as it passes a bare Literal.
    assert value_to_type(Dict[Optional[Literal["a", "b"]], int], {"c": 1}) == {"c": 1}


class OutputAliased(BaseModel):
    user_name: str = Field(serialization_alias="userName")


def test_output_alias_and_masked_error_secrets() -> None:
    model = OutputAliased(user_name="bob")
    assert roundtrip(model, OutputAliased) == model

    class Leaky(Exception):
        def __init__(self) -> None:
            super().__init__("boom")
            self.data = Credentials(
                key=SecretStr("hunter2"), raw=SecretBytes(b"pw"), nested=[]
            )

    # An error report keeps secrets masked; only checkpoints of values hold them.
    error_data = exception_to_workflow_error_data(Leaky())
    assert "hunter2" not in json.dumps(error_data)


class Shade(Enum):
    RED = "red"


@dataclass
class Painted:
    id: int
    shade: Shade


@dataclass
class Meta:
    tags: List[str] = field(default_factory=list)


class Posting(BaseModel):
    model_config = {"populate_by_name": True}
    name: str = Field(alias="title")
    title: str = Field(alias="jobTitle")


def test_round8_leniency_inside_unions_and_alias_collisions() -> None:
    # Optional[X] converts exactly as X, passing an unknown enum value through.
    assert value_to_type(Optional[Shade], "green") == "green"
    painted = value_to_type(Optional[Painted], {"id": 1, "shade": "green"})
    assert painted == Painted(1, "green")  # type: ignore[arg-type]
    # A better-fitting member failing only on a nested scalar wins over a worse fit.
    chosen = value_to_type(Union[Painted, Meta], {"id": 1, "shade": "green"})
    assert chosen == Painted(1, "green")  # type: ignore[arg-type]
    # A field whose alias is another field's name reads back into the right field.
    posting = Posting(title="Build", jobTitle="Engineer")
    assert roundtrip(posting, Posting) == posting


@dataclass
class DatedPair:
    x: date
    y: int


@dataclass
class Counted:
    q: int


@dataclass
class Labelled:
    x: str
    y: int


@dataclass
class WrapsChoice:
    item: Union[DatedPair, Counted]


@dataclass
class WrapsLabel:
    item: Labelled


@dataclass
class Kitten:
    kind: Literal["kitten"]
    born: date


@dataclass
class Puppy:
    kind: Literal["puppy"]
    born: date


class SwappedInner(BaseModel):
    model_config = {"populate_by_name": True}
    a: int = Field(alias="b")
    b: int = Field(alias="a")


class SwappedOuter(BaseModel):
    inner: SwappedInner
    items: List[SwappedInner]


def test_round9_nested_choices() -> None:
    # An inner union's fallback never overrides the enclosing union's choice.
    stored = WrapsLabel(Labelled("hello", 1))
    assert roundtrip(stored, Union[WrapsChoice, WrapsLabel]) == stored
    # A discriminator picks the member, which then converts as outside a union.
    kitten: Any = value_to_type(
        Union[Kitten, Puppy], {"kind": "kitten", "born": "unknown"}
    )
    assert type(kitten) is Kitten and kitten.born == "unknown"
    # Nested models whose aliases name each other's fields read back unswapped.
    outer = SwappedOuter(inner=SwappedInner(b=1, a=2), items=[SwappedInner(b=3, a=4)])
    assert roundtrip(outer, SwappedOuter) == outer


def test_hint_cache_separates_bound_methods() -> None:
    class Holder:
        @functools.wraps(lambda self, x: x)
        def method(self, x: int) -> int:
            return x

    holder = Holder()
    assert [p.name for p in get_func_type_hints(holder.method).params] == ["x"]
    assert [p.name for p in get_func_type_hints(Holder.method).params] == [
        "self",
        "x",
    ]


class StrictBase(BaseModel):
    model_config = {"extra": "forbid"}
    x: int


class LooserDerived(StrictBase):
    model_config = {"extra": "ignore"}
    y: int


def test_forbid_extra_base_reads_stored_subclass() -> None:
    # A subclass stored under its base's annotation reads back as the base, as dataclasses do.
    assert roundtrip(LooserDerived(x=1, y=2), StrictBase) == StrictBase(x=1)


@dataclass
class Sized:
    shade: Shade
    size: int


@dataclass
class SizeOnly:
    size: int


@dataclass
class HoldsChoice:
    item: Union[Sized, SizeOnly]


class ForbidAliased(BaseModel):
    model_config = {"extra": "forbid"}
    user_name: str = Field(validation_alias=AliasChoices("user_name", "userName"))
    nick: str = Field("", validation_alias="nickName")


def test_round10_recursion_nesting_and_forbid_aliases() -> None:
    # A recursive alias in a model's field doesn't make the alias check recurse forever.
    module = types.ModuleType("recursive_alias_test")
    sys.modules["recursive_alias_test"] = module
    try:
        exec(
            "from typing import Dict, List, Union\n"
            "from pydantic import BaseModel\n"
            "from typing_extensions import TypeAliasType\n"
            "JsonValue = TypeAliasType('JsonValue', "
            "Union[Dict[str, 'JsonValue'], List['JsonValue'], str, int, None])\n"
            "class JsonDoc(BaseModel):\n"
            "    name: str\n"
            "    data: JsonValue\n",
            module.__dict__,
        )
        doc_cls: Any = getattr(module, "JsonDoc")
        doc = doc_cls(name="x", data={"a": [1, 2]})
        assert roundtrip(doc, doc_cls) == doc
    finally:
        del sys.modules["recursive_alias_test"]
    # An enclosing union doesn't freeze an inner union's worse fit.
    stored = {"item": {"shade": "purple", "size": 3}}
    for hint in (HoldsChoice, Union[HoldsChoice, int]):
        holder: Any = value_to_type(hint, stored)
        assert type(holder.item) is Sized
        assert getattr(holder.item, "shade") == "purple"
    # Keys a forbid-extra model reads through validation aliases are kept.
    assert value_to_type(
        ForbidAliased, {"userName": "a", "nickName": "b"}
    ) == ForbidAliased.model_validate({"user_name": "a", "nickName": "b"})


class Mood(Enum):
    HAPPY = "happy"


@dataclass
class Comment:
    text: str
    mood: Mood
    replies: "List[Thread]"


@dataclass
class Removed:
    replies: "List[Thread]"


Thread = Union[Comment, Removed]


class ForbidInner(BaseModel):
    model_config = {"extra": "forbid"}
    a: int


class HoldsForbidInner(BaseModel):
    inner: ForbidInner
    items: List[ForbidInner]


def test_round11_bounded_unions_and_nested_extras() -> None:
    # A deep thread whose leaf holds a since-removed enum value decodes in linear time.
    stored: Any = {"text": "leaf", "mood": "angry", "replies": []}
    for _ in range(40):
        stored = {"text": "t", "mood": "happy", "replies": [stored]}
    decoded: Any = value_to_type(Thread, stored)
    while decoded.replies:
        decoded = decoded.replies[0]
    assert decoded.mood == "angry"
    # Keys a nested forbid-extra model no longer declares are ignored at any depth.
    holder = value_to_type(
        HoldsForbidInner,
        {"inner": {"a": 1, "old": 2}, "items": [{"a": 3, "old": 4}]},
    )
    assert holder == HoldsForbidInner(inner=ForbidInner(a=1), items=[ForbidInner(a=3)])


class ForbidCat(BaseModel):
    model_config = {"extra": "forbid"}
    name: str


class ForbidDog(BaseModel):
    model_config = {"extra": "forbid"}
    name: str
    bark: int = 0


class PetOwner(BaseModel):
    pet: Union[ForbidCat, ForbidDog]


@dataclass
class DraftPost:
    tags: List[str]

    def __post_init__(self) -> None:
        self.tags.append("draft")


@dataclass
class PublishedPost:
    tags: List[str]
    url: str


def test_round12_pruning_and_memo() -> None:
    # Pruning never drops a key that another union member declares; the decode fails instead.
    with pytest.raises(DBOSSerializationError):
        value_to_type(PetOwner, {"pet": {"name": "rex", "bark": 5, "legacy": True}})
    # A losing union candidate's mutations don't leak into the winner's values.
    post: Any = value_to_type(
        Union[DraftPost, PublishedPost], {"tags": ["a"], "url": "u"}
    )
    assert post == PublishedPost(["a"], "u")
    # Nested arrays under a recursive union decode in linear time.
    nested: Any = 1
    for _ in range(60):
        nested = [nested]
    assert value_to_type(Union[List[Any], Tuple[Any, ...], int], nested) == nested


@dataclass
class PlainStep:
    name: str
    next: "Optional[Union[PlainStep, RetryingStep]]" = None


@dataclass
class RetryingStep:
    name: str
    next: "Optional[Union[PlainStep, RetryingStep]]" = None
    retries: int = 3


@dataclass
class Tag:
    name: str
    color: str


@dataclass
class AnnotatedAliases:
    x: Annotated[int, Field(alias="y")]
    y: int


class HoldsAnnotatedAliases(BaseModel):
    inner: AnnotatedAliases


def test_round13_fit_ordered_unions() -> None:
    # Members are tried best fit first, so a long chain of successful records stays linear.
    chain: Any = None
    for index in range(60):
        chain = RetryingStep(str(index), chain)
    decoded: Any = roundtrip(chain, Optional[Union[PlainStep, RetryingStep]])
    assert decoded == chain
    # An array of records goes to the member whose records fit, wherever it is declared.
    tags = [Tag("a", "red")]
    assert roundtrip(tags, Union[List[str], List[Tag]]) == tags
    # An alias naming another field on a nested plain dataclass is read by name.
    stored = HoldsAnnotatedAliases(inner=AnnotatedAliases(1, 2))
    assert roundtrip(stored, HoldsAnnotatedAliases).inner == AnnotatedAliases(1, 2)


@dataclass
class CountAt:
    at: int


@dataclass
class StampAt:
    at: datetime


class Hue(Enum):
    RED = "red"


class NotRequiredAliases(typing_extensions.TypedDict):
    a: typing_extensions.NotRequired[Annotated[int, Field(alias="b")]]
    b: typing_extensions.NotRequired[int]


class HoldsNotRequiredAliases(BaseModel):
    td: NotRequiredAliases


def test_round14_strict_selection_and_shapes() -> None:
    # While choosing, a member only accepts values of its own JSON types.
    stamp = StampAt(datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert roundtrip(stamp, Union[CountAt, StampAt]) == stamp
    assert roundtrip(CountAt(5), Union[StampAt, CountAt]) == CountAt(5)
    when = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
    assert roundtrip(when, Union[List[int], List[datetime]]) == when
    # A member that can't hold an object never outranks the record that can.
    assert roundtrip(Hound("rex", "lab"), Union[Hue, Animal]) == Animal("rex")
    # TypedDict qualifiers around an aliased field don't hide a collision.
    stored = HoldsNotRequiredAliases(td={"a": 2, "b": 1})
    assert roundtrip(stored, HoldsNotRequiredAliases) == stored


class Planet(Enum):
    MERCURY = (3.303e23, 2.4397e6)


class IntCounts(RootModel[Dict[str, int]]):
    pass


@dataclass
class OptionalX:
    x: int = 0


def test_round15_enums_root_models_and_aliases() -> None:
    # An enum stored as an array still wins over array members that can't hold it.
    assert roundtrip(Planet.MERCURY, Union[Planet, List[str]]) is Planet.MERCURY
    assert roundtrip(Planet.MERCURY, Union[Tuple[str, str], Planet]) is Planet.MERCURY
    # A root model ranks by its root type.
    counts = IntCounts({"a": 1})
    assert roundtrip(counts, Union[IntCounts, Dict[str, str]]) == counts
    assert roundtrip(counts, Union[OptionalX, IntCounts]) == counts
    # Unions differing only in member order expand separately under a generic alias.
    if sys.version_info >= (3, 12):
        namespace: Dict[str, Any] = {}
        exec("type Pair[T] = tuple[T, T]", namespace)
        pair = namespace["Pair"]
        when = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)
        days = (when.date(), when.date())
        assert roundtrip(days, pair[Union[date, datetime]]) == days
        assert roundtrip((when, when), pair[Union[datetime, date]]) == (when, when)
    # Pydantic members validate strictly while a union is choosing.
    assert roundtrip(TextCode(code="42"), Union[NumCode, TextCode]) == TextCode(
        code="42"
    )


class NumCode(BaseModel):
    code: int


class TextCode(BaseModel):
    code: str


class Rate(Enum):
    LOW = 1.0


class NodeName(BaseModel):
    name: str


class Forest(RootModel[List["NodeName"]]):
    pass


def test_round16_enum_fit_and_root_strings() -> None:
    # A null item doesn't make an Optional enum container a misfit.
    assert value_to_type(Union[List[Optional[Hue]], List[int]], [None, "red"]) == [
        None,
        Hue.RED,
    ]
    # An int another writer stored for a float-valued enum still selects the enum.
    assert value_to_type(Union[List[Rate], List[int]], [1]) == [Rate.LOW]
    # A RootModel's quoted root names resolve for ranking, on every version.
    forest = Forest([NodeName(name="x")])
    assert roundtrip(forest, Union[List[Dict[str, Any]], Forest]) == forest
