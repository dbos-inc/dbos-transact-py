import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import List, Type, TypeVar


class MessageType(str, Enum):
    EXECUTOR_INFO = "executor_info"
    RECOVERY = "recovery"


T = TypeVar("T", bound="BaseMessage")


@dataclass
class BaseMessage:
    type: MessageType

    @classmethod
    def from_json(cls: Type[T], json_str: str) -> T:
        """
        Safely load a JSON into a dataclass, loading only the
        attributes specified in the dataclass.
        """
        data = json.loads(json_str)
        all_annotations = {}
        for base_cls in cls.__mro__:
            if hasattr(base_cls, "__annotations__"):
                all_annotations.update(base_cls.__annotations__)
        kwargs = {k: v for k, v in data.items() if k in all_annotations}
        return cls(**kwargs)

    def to_json(self) -> str:
        dict_data = asdict(self)
        return json.dumps(dict_data)


@dataclass
class ExecutorInfoRequest(BaseMessage):
    pass


@dataclass
class ExecutorInfoResponse(BaseMessage):
    executor_id: str
    application_version: str


@dataclass
class RecoveryRequest(BaseMessage):
    executor_ids: List[str]


@dataclass
class RecoveryResponse(BaseMessage):
    success: bool
