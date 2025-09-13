import base64
import pickle
import types
from typing import Any, Dict, Optional, Tuple, TypedDict

from ._logger import dbos_logger


class WorkflowInputs(TypedDict):
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


def serialize(data: Any) -> str:
    pickled_data: bytes = pickle.dumps(data)
    encoded_data: str = base64.b64encode(pickled_data).decode("utf-8")
    return encoded_data


def serialize_args(data: WorkflowInputs) -> str:
    """Serialize args to a base64-encoded string using pickle."""
    pickled_data: bytes = pickle.dumps(data)
    encoded_data: str = base64.b64encode(pickled_data).decode("utf-8")
    return encoded_data


def serialize_exception(data: Exception) -> str:
    """Serialize an Exception object to a base64-encoded string using pickle."""
    pickled_data: bytes = pickle.dumps(data)
    encoded_data: str = base64.b64encode(pickled_data).decode("utf-8")
    return encoded_data


def deserialize(serialized_data: str) -> Any:
    """Deserialize a base64-encoded string back to a Python object using pickle."""
    pickled_data: bytes = base64.b64decode(serialized_data)
    return pickle.loads(pickled_data)


def deserialize_args(serialized_data: str) -> WorkflowInputs:
    """Deserialize a base64-encoded string back to a Python object list using pickle."""
    pickled_data: bytes = base64.b64decode(serialized_data)
    args: WorkflowInputs = pickle.loads(pickled_data)
    return args


def deserialize_exception(serialized_data: str) -> Exception:
    """Deserialize a base64-encoded string back to a Python Exception using pickle."""
    pickled_data: bytes = base64.b64decode(serialized_data)
    exc: Exception = pickle.loads(pickled_data)
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
