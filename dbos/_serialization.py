import base64
import json
import pickle
from abc import ABC, abstractmethod
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Tuple, TypedDict, cast

from dbos._schemas.system_database import (
    JsonValue,
    JsonWorkflowArgs,
    JsonWorkflowErrorData,
    PortableWorkflowError,
)

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
DBOSDefaultSerializer: Serializer = DefaultSerializer()


class WorkflowSerializationFormat(str, Enum):
    PORTABLE = "portable"
    NATIVE = "native"
    DEFAULT = None


def serialization_for_type(
    serialization_type: Optional[WorkflowSerializationFormat], serializer: Serializer
) -> str:
    if serialization_type == WorkflowSerializationFormat.PORTABLE:
        return DBOSPortableJSON.name()
    if serialization_type == WorkflowSerializationFormat.NATIVE:
        return DBOSDefaultSerializer.name()
    return serializer.name()


def serialize_value(
    value: Optional[Any],
    serialization: Optional[str],
    serializer: Serializer,
) -> tuple[Optional[str], str]:
    if serialization == DBOSPortableJSON.name():
        return DBOSPortableJSON.serialize(value), DBOSPortableJSON.name()
    if serialization == DBOSDefaultSerializer.name():
        return DBOSDefaultSerializer.serialize(value), DBOSDefaultSerializer.name()
    if serialization is not None and serialization != serializer.name():
        raise TypeError(f"Serialization {serialization} is not available")
    return serializer.serialize(value), serializer.name()


def deserialize_value(
    serialized_value: Optional[str],
    serialization: Optional[str],
    serializer: Serializer,
) -> Optional[Any]:
    if serialized_value is None:
        return None
    if serialization == DBOSPortableJSON.name():
        return DBOSPortableJSON.deserialize(serialized_value)
    if serialization == DBOSDefaultSerializer.name():
        return DBOSDefaultSerializer.deserialize(serialized_value)
    if serialization is not None and serialization != serializer.name():
        raise TypeError(f"Serialization {serialization} is not available")
    return serializer.deserialize(serialized_value)


def serialize_args(
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    serialization: Optional[str],
    serializer: Serializer,
) -> tuple[str, str]:
    if serialization == DBOSPortableJSON.name():
        a: JsonWorkflowArgs = {"namedArgs": kwargs, "positionalArgs": list(args)}
        serval = DBOSPortableJSON.serialize(a)
        return serval, DBOSPortableJSON.name()
    i: WorkflowInputs = {"args": args, "kwargs": kwargs}
    if serialization == DBOSDefaultSerializer.name():
        return DBOSDefaultSerializer.serialize(i), DBOSDefaultSerializer.name()
    if serialization is not None and serialization != serializer.name():
        raise TypeError(f"Serialization {serialization} is not available")
    return serializer.serialize(i), serializer.name()


def deserialize_args(
    serialized_value: str, serialization: Optional[str], serializer: Serializer
) -> WorkflowInputs:
    if serialized_value is None:
        return None
    if serialization == DBOSPortableJSON.name():
        args: JsonWorkflowArgs = DBOSPortableJSON.deserialize(serialized_value)
        if (not "positionalArgs" in args) or args["positionalArgs"] is None:
            args["positionalArgs"] = []
        if (not "namedArgs" in args) or args["namedArgs"] is None:
            args["namedArgs"] = {}
        return {"args": tuple(args["positionalArgs"]), "kwargs": args["namedArgs"]}
    if serialization == DBOSDefaultSerializer.name():
        return cast(WorkflowInputs, DBOSDefaultSerializer.deserialize(serialized_value))
    if serialization is not None and serialization != serializer.name():
        raise TypeError(f"Serialization {serialization} is not available")
    return cast(WorkflowInputs, serializer.deserialize(serialized_value))


def _safe_str(x: Any) -> str:
    try:
        return str(x)
    except Exception:
        try:
            return repr(x)
        except Exception:
            return "<unprintable>"


def _extract_code(err: Any) -> int | str | None:
    """
    Best-effort extraction of an app-specific error code.
    """
    for attr in ("code", "error_code", "errno", "status", "status_code"):
        try:
            v = getattr(err, attr)
        except Exception:
            continue
        if isinstance(v, (int, str)) or v is None:
            return v

    return None


def exception_to_workflow_error_data(
    err: Any,
) -> JsonWorkflowErrorData:
    """
    Best-effort conversion of an arbitrary exception/error-like object into JsonWorkflowErrorData.

    - name: class/type name if possible, else "Error"
    - message: str(err) best effort
    - code: tries common attributes (code/error_code/errno/status/status_code) + args heuristics
    - data: optional structured extras (cause/context/origin/traceback) converted to JSON-ish values
    """
    try:
        name = type(err).__name__
    except Exception:
        name = "Error"

    message = _safe_str(err)

    code = _extract_code(err)

    out: JsonWorkflowErrorData = {"name": name, "message": message}

    out["code"] = code

    for attr in ("data", "details", "payload", "extra", "meta", "metadata"):
        try:
            v = getattr(err, attr)
        except Exception:
            continue
        if callable(v):
            continue
        out["data"] = v
        break

    return out


def serialize_exception(
    value: Exception,
    serialization: Optional[str],
    serializer: Serializer,
) -> tuple[Optional[str], str]:
    if serialization == DBOSPortableJSON.name():
        return (
            DBOSPortableJSON.serialize(exception_to_workflow_error_data(value)),
            DBOSPortableJSON.name(),
        )
    if serialization == DBOSDefaultSerializer.name():
        return DBOSDefaultSerializer.serialize(value), DBOSDefaultSerializer.name()
    if serialization is not None and serialization != serializer.name():
        raise TypeError(f"Serialization {serialization} is not available")
    return serializer.serialize(value), serializer.name()


def deserialize_exception(
    serialized_value: str, serialization: Optional[str], serializer: Serializer
) -> Exception:
    if serialized_value is None:
        return None
    if serialization == DBOSPortableJSON.name():
        errdata: JsonWorkflowErrorData = DBOSPortableJSON.deserialize(serialized_value)
        return PortableWorkflowError(
            errdata["message"], errdata["name"], errdata["code"], errdata["data"]
        )
    if serialization == DBOSDefaultSerializer.name():
        return cast(Exception, DBOSDefaultSerializer.deserialize(serialized_value))
    if serialization is not None and serialization != serializer.name():
        raise TypeError(f"Serialization {serialization} is not available")
    return cast(Exception, serializer.deserialize(serialized_value))


def safe_deserialize(
    serializer: Serializer,
    serialization: Optional[str],
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
            deserialize_args(serialized_input, serialization, serializer)
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
            deserialize_value(serialized_output, serialization, serializer)
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
            deserialize_exception(serialized_exception, serialization, serializer)
            if serialized_exception is not None
            else None
        )
    except Exception as e:
        dbos_logger.warning(
            f"Warning: exception object could not be deserialized for workflow {workflow_id}, returning as string: {e}"
        )
        exception = serialized_exception  # type: ignore
    return input, output, exception
