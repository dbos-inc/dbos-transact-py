from typing import Any

import jsonpickle  # type: ignore

jsonpickle.load_backend("simplejson", "dumps", "loads", ValueError)
# jsonpickle.set_preferred_backend('jsonpickle.simplejson')


def serialize(data: Any) -> str:
    """Serialize an object to a JSON string using jsonpickle."""
    encoded_data: str = jsonpickle.encode(data, unpicklable=False)
    return encoded_data


def serialize_error(data: Exception) -> str:
    """Serialize an Exception object to a JSON string using jsonpickle."""
    encoded_data: str = jsonpickle.encode(data, unpicklable=True)
    return encoded_data


def deserialize(serialized_data: str) -> Any:
    """Deserialize a JSON string back to a Python object using jsonpickle."""
    return jsonpickle.decode(serialized_data)


def deserialize_exception(serialized_data: str) -> Exception:
    """Deserialize JSON string back to a Python Exception using jsonpickle."""
    upo: Exception = jsonpickle.decode(serialized_data)
    return upo
