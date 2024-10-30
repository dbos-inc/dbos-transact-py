from dataclasses import dataclass
from typing import Optional, Union


@dataclass
class KafkaMessage:
    headers: Optional[list[tuple[str, Union[str, bytes]]]]
    key: Optional[Union[str, bytes]]
    latency: Optional[float]
    leader_epoch: Optional[int]
    offset: Optional[int]
    partition: Optional[int]
    timestamp: tuple[int, int]
    topic: Optional[str]
    value: Optional[Union[str, bytes]]
