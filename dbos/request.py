from dataclasses import dataclass
from typing import Any, Mapping, NamedTuple, Optional


class Address(NamedTuple):
    hostname: str
    port: int


@dataclass
class Request:
    headers: Mapping[str, str]
    path_params: Mapping[str, Any]
    query_params: Mapping[str, str]
    url: str
    base_url: str
    client: Optional[Address]
    cookies: Mapping[str, str]
    method: str
