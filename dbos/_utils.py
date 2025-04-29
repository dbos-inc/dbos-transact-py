import importlib.metadata
import os

INTERNAL_QUEUE_NAME = "_dbos_internal_queue"

request_id_header = "x-request-id"


class GlobalParams:
    app_version: str = os.environ.get("DBOS__APPVERSION", "")
    executor_id: str = os.environ.get("DBOS__VMID", "local")
    try:
        # Only works on Python >= 3.8
        dbos_version = importlib.metadata.version("dbos")
    except importlib.metadata.PackageNotFoundError:
        # If package is not installed or during development
        dbos_version = "unknown"
