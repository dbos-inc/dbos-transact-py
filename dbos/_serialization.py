import base64
import json
import pickle
from abc import ABC, abstractmethod
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Mapping, Optional, Tuple, TypedDict

from dbos._schemas.system_database import JsonValue

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

    @abstractmethod
    def name(self) -> str:
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

    def name(self) -> str:
        return "py_pickle"


############# Portable JSON ###################


# ---------- Portable JSON conversions ----------
def _to_rfc3339_utc(dt: datetime) -> str:
    """
    Canonical timestamp string: UTC RFC3339.
    - Always emits Z, with milliseconds when present.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    # Python emits "+00:00"; canonicalize to "Z".
    s = dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return s


def _portableify(value: Any) -> JsonValue:
    """
    Convert common Python objects into plain JSON-serializable structures
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if isinstance(value, datetime):
        return _to_rfc3339_utc(value)

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return format(value, "f")

    if isinstance(value, (list, tuple)):
        return [_portableify(v) for v in value]

    if isinstance(value, set):
        return [_portableify(v) for v in value]

    if isinstance(value, Mapping):
        out: dict[str, JsonValue] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise TypeError(
                    "Attempt to do portable JSON serialization of a map with non-string keys"
                )
            out[k] = _portableify(v)
        return out

    raise TypeError(
        f"Object of type {type(value).__name__} is not portable JSON serializable"
    )


# ---------- DBOS Portable JSON serializer ----------
class DBOSPortableJSONSerializer(Serializer):
    """
    DBOS Portable JSON serializer:
      - parse: json.loads
      - stringify: portableify + json.dumps
    """

    def name(self) -> str:
        return "portable_json"

    def deserialize(self, text: str | None) -> Any:
        return None if text is None else json.loads(text)

    def serialize(self, value: Any) -> str:
        portable = _portableify(value)
        # separators -> compact encoding like JS default-ish
        return json.dumps(portable, separators=(",", ":"), ensure_ascii=False)


DBOSPortableJSON: Serializer = DBOSPortableJSONSerializer()


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
