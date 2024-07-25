import base64
import pickle
from typing import Any


def serialize(data: Any) -> str:
    """Use pickle to serialize an object to a base64-encoded string"""
    return base64.b64encode(pickle.dumps(data)).decode()


def deserialize(serialized_data: str) -> Any:
    """Deserialize a base64-encoded string back to a Python object using pickle"""
    return pickle.loads(base64.b64decode(serialized_data))
