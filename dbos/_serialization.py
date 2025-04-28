import types
from typing import Any, Dict, Optional, Tuple, TypedDict

import jsonpickle  # type: ignore

from ._logger import dbos_logger


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
    exc: Exception = jsonpickle.decode(serialized_data)
    return exc


def safe_deserialize(
    workflow_id: str,
    *,
    serialized_input: Optional[str],
    serialized_output: Optional[str],
    serialized_exception: Optional[str],
) -> tuple[Optional[WorkflowInputs], Optional[Any], Optional[Exception]]:
    """
    This function safely deserializes a workflow's recorded input and output/exception.
    If any of them is not deserializable, it logs a warning and returns a string instead of throwing an exception.

    This function is used in workflow introspection methods (get_workflows and get_queued_workflow)
    to ensure errors related to nondeserializable objects are observable.
    """
    input: Optional[WorkflowInputs]
    try:
        input = (
            deserialize_args(serialized_input) if serialized_input is not None else None
        )
    except Exception as e:
        dbos_logger.warning(
            f"Warning: input object could not be deserialized for workflow {workflow_id}, returning as string: {e}"
        )
        input = serialized_input  # type: ignore
    output: Optional[Any]
    try:
        output = (
            deserialize(serialized_output) if serialized_output is not None else None
        )
    except Exception as e:
        dbos_logger.warning(
            f"Warning: output object could not be deserialized for workflow {workflow_id}, returning as string: {e}"
        )
        output = serialized_output
    exception: Optional[Exception]
    try:
        exception = (
            deserialize_exception(serialized_exception)
            if serialized_exception is not None
            else None
        )
    except Exception as e:
        dbos_logger.warning(
            f"Warning: exception object could not be deserialized for workflow {workflow_id}, returning as string: {e}"
        )
        exception = serialized_exception  # type: ignore
    return input, output, exception
