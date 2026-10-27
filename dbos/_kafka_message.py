from dataclasses import dataclass
from typing import Optional


# The consumer returns raw bytes; a str | bytes union could not tell base64 bytes from a str once stored as portable JSON.
@dataclass
class KafkaMessage:
    headers: Optional[list[tuple[str, Optional[bytes]]]]
    key: Optional[bytes]
    latency: Optional[float]
    leader_epoch: Optional[int]
    offset: Optional[int]
    partition: Optional[int]
    timestamp: tuple[int, int]
    topic: Optional[str]
    value: Optional[bytes]
