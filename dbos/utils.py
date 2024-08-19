from typing import Any

import jsonpickle  # type: ignore


def serialize(data: Any) -> str:
    """Use jsonpickle to serialize an object to a JSON string"""
    encoded_data: str = jsonpickle.encode(data)
    return encoded_data


def deserialize(serialized_data: str) -> Any:
    """Deserialize a JSON string back to a Python object using jsonpickle"""
    return jsonpickle.decode(serialized_data)
