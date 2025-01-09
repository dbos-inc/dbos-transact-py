import json
import os
import re
from importlib import resources
from typing import Any, Dict, List, Optional, TypedDict, cast

import yaml
from jsonschema import ValidationError, validate
from sqlalchemy import URL

from ._db_wizard import db_connect
from ._error import DBOSInitializationError
from ._logger import dbos_logger


class RuntimeConfig(TypedDict, total=False):
    start: List[str]
    setup: Optional[List[str]]
    admin_port: Optional[int]


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
    local_suffix: Optional[bool]
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
    """
    Data structure containing the DBOS Configuration.

    This configuration data is typically loaded from `dbos-config.yaml`.
    See `https://docs.dbos.dev/api-reference/configuration`_

    Attributes:
        name (str): Application name
        language (str): The app language (probably `python`)
        runtimeConfig (RuntimeConfig): Configuration for request serving
        database (DatabaseConfig): Configuration for the application and system databases
        telemetry (TelemetryConfig): Configuration for tracing / logging
        env (Dict[str,str]): Environment varialbes
        application (Dict[str, Any]): Application-specific configuration section

    """

    name: str
    language: str
    runtimeConfig: RuntimeConfig
    database: DatabaseConfig
    telemetry: Optional[TelemetryConfig]
    env: Dict[str, str]
    application: Dict[str, Any]


def _substitute_env_vars(content: str) -> str:
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
    """
    Retrieve application database URL from configuration `.yaml` file.

    Loads the DBOS `ConfigFile` from the specified path (typically `dbos-config.yaml`),
        and returns the database URL for the application database.

    Args:
        config_file_path (str): The path to the yaml configuration file.

    Returns:
        str: Database URL for the application database

    """
    dbos_config = load_config(config_file_path)
    db_url = URL.create(
        "postgresql+psycopg",
        username=dbos_config["database"]["username"],
        password=dbos_config["database"]["password"],
        host=dbos_config["database"]["hostname"],
        port=dbos_config["database"]["port"],
        database=dbos_config["database"]["app_db_name"],
    )
    return db_url.render_as_string(hide_password=False)


def load_config(config_file_path: str = "dbos-config.yaml") -> ConfigFile:
    """
    Load the DBOS `ConfigFile` from the specified path (typically `dbos-config.yaml`).

    The configuration is also validated against the configuration file schema.

    Args:
        config_file_path (str): The path to the yaml configuration file.

    Returns:
        ConfigFile: The loaded configuration

    """

    with open(config_file_path, "r") as file:
        content = file.read()
        substituted_content = _substitute_env_vars(content)
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

    data = cast(ConfigFile, data)

    if not _is_valid_app_name(data["name"]):
        raise DBOSInitializationError(
            f'Invalid app name {data["name"]}.  App names must be between 3 and 30 characters long and contain only lowercase letters, numbers, dashes, and underscores.'
        )

    if "app_db_name" not in data["database"]:
        data["database"]["app_db_name"] = _app_name_to_db_name(data["name"])

    if "local_suffix" in data["database"] and data["database"]["local_suffix"]:
        data["database"]["app_db_name"] = f"{data['database']['app_db_name']}_local"

    # Check the connectivity to the database and make sure it's properly configured
    data = db_connect(data, config_file_path)

    # Return data as ConfigFile type
    return data  # type: ignore


def _is_valid_app_name(name: str) -> bool:
    name_len = len(name)
    if name_len < 3 or name_len > 30:
        return False
    match = re.match("^[a-z0-9-_]+$", name)
    return True if match != None else False


def _app_name_to_db_name(app_name: str) -> str:
    name = app_name.replace("-", "_")
    return name if not name[0].isdigit() else f"_{name}"


def _set_env_vars(config: ConfigFile) -> None:
    for env, value in config.get("env", {}).items():
        if value is not None:
            os.environ[env] = str(value)
