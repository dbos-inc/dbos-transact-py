import json
from dataclasses import asdict, dataclass
from enum import Enum


class MessageType(str, Enum):
    EXECUTOR_INFO = "executor_info"


@dataclass
class BaseMessage:
    type: MessageType

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        kwargs = {k: v for k, v in data.items() if k in cls.__annotations__}
        return cls(**kwargs)

    def to_json(self):
        dict_data = asdict(self)
        return json.dumps(dict_data)


@dataclass
class ExecutorInfoRequest(BaseMessage):
    pass


@dataclass
class ExecutorInfoResponse(BaseMessage):
    executor_id: str
    application_version: str

    def __post_init__(self):
        self.type = MessageType.EXECUTOR_INFO
