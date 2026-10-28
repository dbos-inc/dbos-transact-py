from __future__ import annotations

import base64
import json
import pickle
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    TypeAlias,
    TypedDict,
    cast,
)

from ._logger import dbos_logger
from ._type_convert import to_portable as _portableify
from ._type_convert import to_portable_masked, value_to_type


class WorkflowInputs(TypedDict):
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


# ---------- Canonical JSON value space for use in serialized columns ----------
# Note the absence of datetime/date/etc.
# Canonical Date = RFC 3339 / ISO-8601 UTC string: YYYY-MM-DDTHH:mm:ss(.sss)Z

JsonPrimitive: TypeAlias = None | bool | int | float | str
JsonArray: TypeAlias = List["JsonValue"]
JsonObject: TypeAlias = Dict[str, "JsonValue"]
JsonValue: TypeAlias = JsonPrimitive | JsonObject | JsonArray


# ---------- Workflow args + result ----------
class JsonWorkflowArgs(TypedDict, total=False):
    positionalArgs: JsonArray
    namedArgs: JsonObject


class JsonWorkflowErrorData(TypedDict, total=False):
    """
    Portable error payload to store in DB / exchange across languages.
    """

    name: str  # error name / type
    message: str  # human-readable message
    code: int | str | None  # portable app/code
    data: JsonValue | None  # structured details (retryable, origin, etc.)


@dataclass(eq=False)
class PortableWorkflowError(Exception):
    """
    A Python exception that carries a portable, JSON-serializable payload.

    Mirrors the TS:
      new PortableWorkflowError(message, name, code?, data?)
    """

    message: str
    name: str
    code: int | str | None = None
    data: JsonValue | None = None

    def __str__(self) -> str:
        return self.message


class Serializer(ABC):

    @abstractmethod
    def serialize(self, data: Any) -> str:
        pass

    @abstractmethod
    def deserialize(cls, serialized_data: str) -> Any:
        pass

    def deserialize_typed(self, serialized_data: str, type_hint: Any) -> Any:
        """Deserialize a value the reading code expects to have type `type_hint`.

        Override to build typed values; by default the hint is ignored. The data may encode None.
        """
        return self.deserialize(serialized_data)

    def name(self) -> str:
        return "custom_serializer"


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

    def deserialize_typed(self, text: str, type_hint: Any) -> Any:
        return value_to_type(type_hint, self.deserialize(text))

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
    serialization_type: Optional[WorkflowSerializationFormat],
    serializer: Serializer,
) -> tuple[Optional[str], str]:
    return serialize_value_as(
        value, serialization_for_type(serialization_type, serializer), serializer
    )


def reads_type_hints(serialization: Optional[str], serializer: Serializer) -> bool:
    """Whether decoding this serialization uses a type hint, so a caller can skip resolving one."""
    if serialization is None:
        serialization = serializer.name()
    if serialization == DBOSPortableJSON.name():
        return True
    if serialization == DBOSDefaultSerializer.name():
        return False
    deserialize_typed = getattr(type(serializer), "deserialize_typed", None)
    return (
        deserialize_typed is not None
        and deserialize_typed is not Serializer.deserialize_typed
    )


def serialize_value_or_portable(
    value: Optional[Any], serializer: Serializer
) -> tuple[Optional[str], str]:
    """Serialize a checkpoint in the app's serializer, or in portable JSON when that cannot encode it.

    Typed values decoded from portable JSON, like dataclasses, may be beyond a custom serializer.
    """
    try:
        if type(serializer) is DefaultSerializer:
            # Pickled directly, as DefaultSerializer logs a failure the fallback below recovers from.
            return (
                base64.b64encode(pickle.dumps(value)).decode("utf-8"),
                serializer.name(),
            )
        return serialize_value(value, None, serializer)
    except Exception as e:
        error = e
    try:
        return DBOSPortableJSON.serialize(value), DBOSPortableJSON.name()
    except Exception:
        raise error


def serialize_value_as(
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
    type_hint: Any = None,
) -> Optional[Any]:
    if serialized_value is None:
        return None
    if serialization == DBOSPortableJSON.name():
        if type_hint is not None:
            return DBOSPortableJSON.deserialize_typed(serialized_value, type_hint)
        return DBOSPortableJSON.deserialize(serialized_value)
    if serialization == DBOSDefaultSerializer.name():
        return DBOSDefaultSerializer.deserialize(serialized_value)
    if serialization is not None and serialization != serializer.name():
        raise TypeError(f"Serialization {serialization} is not available")
    if type_hint is not None:
        # A serializer that only duck-types Serializer may lack deserialize_typed.
        deserialize_typed = getattr(serializer, "deserialize_typed", None)
        if deserialize_typed is not None:
            return deserialize_typed(serialized_value, type_hint)
    return serializer.deserialize(serialized_value)


def serialize_args(
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    serialization_type: Optional[WorkflowSerializationFormat],
    serializer: Serializer,
) -> tuple[str, str]:
    serialization = serialization_for_type(serialization_type, serializer)
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
        try:
            # Only include the attribute if it is portably serializable; otherwise
            # fall back to a string so serializing the error can never itself fail
            # (which would prevent the workflow's error from being recorded at all).
            # Secrets stay masked: unlike a checkpoint, an error report must not reveal them.
            out["data"] = to_portable_masked(v)
        except Exception:
            out["data"] = _safe_str(v)
        break

    return out


def serialize_exception(
    value: Exception,
    serialization: Optional[str],
    serializer: Serializer,
) -> tuple[str, str]:
    if serialization is None:
        serialization = serializer.name()

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
    if serialization is None:
        serialization = serializer.name()

    if serialized_value is None:
        return None
    if serialization == DBOSPortableJSON.name():
        errdata: JsonWorkflowErrorData = DBOSPortableJSON.deserialize(serialized_value)
        return PortableWorkflowError(
            errdata["message"],
            errdata["name"],
            errdata.get("code"),
            errdata.get("data"),
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


def safe_deserialize_schedule_context(
    serializer: Serializer,
    schedule_name: str,
    *,
    serialized_context: str,
) -> Any:
    """
    This function safely deserializes a schedule's recorded context.
    If it is not deserializable, it logs a warning and returns a string instead of throwing an exception.

    This function is used in schedule introspection methods (list_schedules and get_schedule)
    to ensure errors related to nondeserializable objects are observable.
    """
    try:
        return serializer.deserialize(serialized_context)
    except Exception as e:
        dbos_logger.warning(
            f"Warning: context object could not be deserialized for schedule {schedule_name}, returning as string: {e}"
        )
        return serialized_context
