from dataclasses import dataclass
from typing import Any, Mapping, NamedTuple, Optional

request_id_header = "x-request-id"


class Address(NamedTuple):
    hostname: str
    port: int


@dataclass
class Request:
    """
    Serializable HTTP Request object.

    Attributes:
        base_url(str): Base of URL requested, as in application code
        client(Optional[Address]): HTTP Client
        cookies(Mapping[str, str]): HTTP Cookies
        headers(Mapping[str, str]): HTTP headers
        method(str): HTTP verb
        path_params(Mapping[str, Any]): Parameters extracted from URL path sections
        query_params(Mapping[str, str]): URL query string parameters
        url(str): Full URL accessed
    """

    headers: Mapping[str, str]
    path_params: Mapping[str, Any]
    query_params: Mapping[str, str]
    url: str
    base_url: str
    client: Optional[Address]
    cookies: Mapping[str, str]
    method: str
