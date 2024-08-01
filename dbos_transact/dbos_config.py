import json
import os
import re
from importlib import resources
from typing import Any, Dict, List, Optional, TypedDict

import yaml
from jsonschema import ValidationError, validate

from dbos_transact.error import DBOSInitializationError


class RuntimeConfig(TypedDict, total=False):
    start: List[str]


class DatabaseConfig(TypedDict, total=False):
    hostname: str
    port: int
    username: str
    password: Optional[str]
    connectionTimeoutMillis: Optional[int]
    app_db_name: str
    sys_db_name: Optional[str]
    ssl: Optional[bool]
    ssl_ca: Optional[str]
    app_db_client: Optional[str]
    migrate: Optional[List[str]]
    rollback: Optional[List[str]]


class OTLPExporterConfig(TypedDict, total=False):
    logsEndpoint: Optional[str]
    tracesEndpoint: Optional[str]


class LoggerConfig(TypedDict, total=False):
    logLevel: Optional[str]


class TelemetryConfig(TypedDict, total=False):
    logs: Optional[LoggerConfig]
    OTLPExporter: Optional[OTLPExporterConfig]


class ConfigFile(TypedDict):
    name: str
    language: str
    runtimeConfig: RuntimeConfig
    database: DatabaseConfig
    telemetry: Optional[TelemetryConfig]
    application: Dict[str, Any]
    env: Dict[str, str]


def substitute_env_vars(content: str) -> str:
    regex = r"\$\{([^}]+)\}"  # Regex to match ${VAR_NAME} style placeholders

    def replace_func(match: re.Match[str]) -> str:
        var_name = match.group(1)
        return os.environ.get(
            var_name, '""'
        )  # If the env variable is not set, return an empty string

    return re.sub(regex, replace_func, content)


def load_config(configFilePath: str = "dbos-config.yaml") -> ConfigFile:
    # Load the YAML file
    with open(configFilePath, "r") as file:
        content = file.read()
        substituted_content = substitute_env_vars(content)
        data = yaml.safe_load(substituted_content)

    # Load the JSON schema relative to the package root
    with resources.open_text("dbos_transact", "dbos-config.schema.json") as schema_file:
        schema = json.load(schema_file)

    # Validate the data against the schema
    try:
        validate(instance=data, schema=schema)
    except ValidationError as e:
        raise DBOSInitializationError(f"Validation error: {e}")

    if "name" not in data:
        raise DBOSInitializationError(
            f"dbos-config.yaml must specify an application name"
        )

    if "language" not in data:
        raise DBOSInitializationError(
            f"dbos-config.yaml must specify the application language is Python"
        )

    if data["language"] != "python":
        raise DBOSInitializationError(
            f'dbos-config.yaml specifies invalid language { data["language"] }'
        )

    if "runtimeConfig" not in data or "start" not in data["runtimeConfig"]:
        raise DBOSInitializationError(f"dbos-config.yaml must specify a start command")

    # Return data as ConfigFile type
    return data  # type: ignore
