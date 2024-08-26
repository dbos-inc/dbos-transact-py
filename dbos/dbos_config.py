import json
import os
import re
from importlib import resources
from typing import Any, Dict, List, Optional, TypedDict

import yaml
from jsonschema import ValidationError, validate
from sqlalchemy import URL

from dbos.error import DBOSInitializationError
from dbos.logger import dbos_logger


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


class ConfigFile(TypedDict, total=False):
    name: str
    language: str
    runtimeConfig: RuntimeConfig
    database: DatabaseConfig
    telemetry: Optional[TelemetryConfig]
    env: Dict[str, str]


def substitute_env_vars(content: str) -> str:
    regex = r"\$\{([^}]+)\}"  # Regex to match ${VAR_NAME} style placeholders

    def replace_func(match: re.Match[str]) -> str:
        var_name = match.group(1)
        value = os.environ.get(
            var_name, ""
        )  # If the env variable is not set, return an empty string
        if value == "":
            dbos_logger.warning(
                f"Variable {var_name} would be substituted from the process environment into dbos-config.yaml, but is not defined"
            )
        return value

    return re.sub(regex, replace_func, content)


def get_dbos_database_url(config_file_path: str = "dbos-config.yaml") -> str:
    dbos_config = load_config(config_file_path)
    db_url = URL.create(
        "postgresql",
        username=dbos_config["database"]["username"],
        password=dbos_config["database"]["password"],
        host=dbos_config["database"]["hostname"],
        port=dbos_config["database"]["port"],
        database=dbos_config["database"]["app_db_name"],
    )
    return db_url.render_as_string(hide_password=False)


def load_config(config_file_path: str = "dbos-config.yaml") -> ConfigFile:
    # Load the YAML file
    with open(config_file_path, "r") as file:
        content = file.read()
        substituted_content = substitute_env_vars(content)
        data = yaml.safe_load(substituted_content)

    # Load the JSON schema relative to the package root
    schema_file = resources.files("dbos").joinpath("dbos-config.schema.json")
    with schema_file.open("r") as f:
        schema = json.load(f)

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


def set_env_vars(config: ConfigFile) -> None:
    for env, value in config.get("env", {}).items():
        if value is not None:
            os.environ[env] = value
