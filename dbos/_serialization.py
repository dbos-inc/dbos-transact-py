import types
from typing import Any, Dict, Tuple, TypedDict

import jsonpickle  # type: ignore


class WorkflowInputs(TypedDict):
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


def _validate_item(data: Any) -> None:
    if isinstance(data, (types.FunctionType, types.MethodType)):
        raise TypeError("Serialized data item should not be a function")


def serialize(data: Any) -> str:
    """Serialize an object to a JSON string using jsonpickle."""
    _validate_item(data)
    encoded_data: str = jsonpickle.encode(data, unpicklable=True)
    return encoded_data


def serialize_args(data: WorkflowInputs) -> str:
    """Serialize args to a JSON string using jsonpickle."""
    arg: Any
    for arg in data["args"]:
        _validate_item(arg)
    for arg in data["kwargs"].values():
        _validate_item(arg)
    encoded_data: str = jsonpickle.encode(data, unpicklable=True)
    return encoded_data


def serialize_exception(data: Exception) -> str:
    """Serialize an Exception object to a JSON string using jsonpickle."""
    encoded_data: str = jsonpickle.encode(data, unpicklable=True)
    return encoded_data


def deserialize(serialized_data: str) -> Any:
    """Deserialize a JSON string back to a Python object using jsonpickle."""
    return jsonpickle.decode(serialized_data)


def deserialize_args(serialized_data: str) -> WorkflowInputs:
    """Deserialize a JSON string back to a Python object list using jsonpickle."""
    args: WorkflowInputs = jsonpickle.decode(serialized_data)
    return args


def deserialize_exception(serialized_data: str) -> Exception:
    """Deserialize JSON string back to a Python Exception using jsonpickle."""
    upo: Exception = jsonpickle.decode(serialized_data)
    return upo
