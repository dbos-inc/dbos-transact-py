import base64
import pickle
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple, TypedDict

from ._logger import dbos_logger


class WorkflowInputs(TypedDict):
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


class Serializer(ABC):

    @abstractmethod
    def serialize(self, data: Any) -> str:
        pass

    @abstractmethod
    def deserialize(cls, serialized_data: str) -> Any:
        pass


class DefaultSerializer(Serializer):

    def serialize(self, data: Any) -> str:
        try:
            pickled_data: bytes = pickle.dumps(data)
            encoded_data: str = base64.b64encode(pickled_data).decode("utf-8")
            return encoded_data
        except Exception as e:
            dbos_logger.error(f"Error serializing object: {data}", exc_info=e)
            raise

    def deserialize(cls, serialized_data: str) -> Any:
        pickled_data: bytes = base64.b64decode(serialized_data)
        return pickle.loads(pickled_data)


def safe_deserialize(
    serializer: Serializer,
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
            serializer.deserialize(serialized_input)
            if serialized_input is not None
            else None
        )
    except Exception as e:
        dbos_logger.warning(
            f"Warning: input object could not be deserialized for workflow {workflow_id}, returning as string: {e}"
        )
        input = serialized_input  # type: ignore
    output: Optional[Any]
    try:
        output = (
            serializer.deserialize(serialized_output)
            if serialized_output is not None
            else None
        )
    except Exception as e:
        dbos_logger.warning(
            f"Warning: output object could not be deserialized for workflow {workflow_id}, returning as string: {e}"
        )
        output = serialized_output
    exception: Optional[Exception]
    try:
        exception = (
            serializer.deserialize(serialized_exception)
            if serialized_exception is not None
            else None
        )
    except Exception as e:
        dbos_logger.warning(
            f"Warning: exception object could not be deserialized for workflow {workflow_id}, returning as string: {e}"
        )
        exception = serialized_exception  # type: ignore
    return input, output, exception
