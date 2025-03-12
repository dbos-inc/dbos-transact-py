import json
import os
import re
import sys
from importlib import resources
from typing import Any, Dict, List, Optional, TypedDict, Union, cast

if sys.version_info >= (3, 10):
    from typing import TypeGuard
else:
    from typing_extensions import TypeGuard

import yaml
from jsonschema import ValidationError, validate
from rich import print
from sqlalchemy import URL, make_url

from ._db_wizard import db_wizard, load_db_connection
from ._error import DBOSInitializationError
from ._logger import dbos_logger, init_logger

DBOS_CONFIG_PATH = "dbos-config.yaml"


class DBOSConfig(TypedDict):
    """
    Data structure containing the DBOS library configuration.
    """

    name: str
    db_string: str
    sys_db_name: Optional[str]
    log_level: Optional[str]
    otlp_traces_endpoints: Optional[List[str]]
    admin_port: Optional[int]


def is_dbos_config(obj: Any) -> TypeGuard[DBOSConfig]:
    """
    Type guard to check if an object is a valid DBOSConfig.

    Args:
        obj: Any object to check

    Returns:
        True if the object is a valid DBOSConfig, False otherwise
    """
    if not isinstance(obj, dict):
        return False

    # Check required fields
    if not isinstance(obj.get("name"), str):
        return False
    if not isinstance(obj.get("db_string"), str):
        return False

    # Check optional fields
    if (
        "sys_db_name" in obj
        and obj["sys_db_name"] is not None
        and not isinstance(obj["sys_db_name"], str)
    ):
        return False

    if (
        "log_level" in obj
        and obj["log_level"] is not None
        and not isinstance(obj["log_level"], str)
    ):
        return False

    if "otlp_traces_endpoints" in obj:
        endpoints = obj["otlp_traces_endpoints"]
        if endpoints is not None:
            if not isinstance(endpoints, list):
                return False
            if not all(isinstance(endpoint, str) for endpoint in endpoints):
                return False

    if (
        "admin_port" in obj
        and obj["admin_port"] is not None
        and not isinstance(obj["admin_port"], int)
    ):
        return False

    # Check for unexpected keys
    valid_keys = {
        "name",
        "db_string",
        "sys_db_name",
        "log_level",
        "otlp_traces_endpoints",
        "admin_port",
    }
    if not all(key in valid_keys for key in obj):
        return False

    return True


class RuntimeConfig(TypedDict, total=False):
    start: List[str]
    setup: Optional[List[str]]
    admin_port: Optional[int]


class DatabaseConfig(TypedDict, total=False):
    hostname: str
    port: int
    username: str
    password: str
    connectionTimeoutMillis: Optional[int]
    app_db_name: str
    sys_db_name: Optional[str]
    ssl: Optional[bool]
    ssl_ca: Optional[str]
    local_suffix: Optional[bool]
    migrate: Optional[List[str]]
    rollback: Optional[List[str]]


def parse_db_string_to_dbconfig(db_string: str) -> DatabaseConfig:
    db_url = make_url(db_string)
    db_config = {
        "hostname": db_url.host,
        "port": db_url.port or 5432,
        "username": db_url.username,
        "password": db_url.password,
        "app_db_name": db_url.database,
    }
    for key, value in db_url.query.items():
        str_value = value[0] if isinstance(value, tuple) else value
        if key == "connect_timeout":
            db_config["connectionTimeoutMillis"] = int(str_value) * 1000
        elif key == "sslmode":
            db_config["ssl"] = str_value == "require"
        elif key == "sslcert":
            db_config["ssl_ca"] = str_value
    return cast(DatabaseConfig, db_config)


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
    runtimeConfig: RuntimeConfig
    database: DatabaseConfig
    telemetry: Optional[TelemetryConfig]
    env: Dict[str, str]


def is_config_file(obj: object) -> TypeGuard[ConfigFile]:
    return (
        isinstance(obj, dict)
        and "name" in obj
        and "runtimeConfig" in obj
        and isinstance(obj.get("name"), str)
        and isinstance(obj.get("runtimeConfig"), dict)
    )


def translate_dbos_config_to_config_file(config: DBOSConfig) -> ConfigFile:
    db_config = parse_db_string_to_dbconfig(config["db_string"])
    if "sys_db_name" in config:
        db_config["sys_db_name"] = config.get("sys_db_name")

    # Start with the mandatory fields
    translated_config: ConfigFile = {
        "name": config["name"],
        "database": db_config,
        "runtimeConfig": {
            "start": [],
        },
    }
    # Add admin_port to runtimeConfig if present
    if "admin_port" in config:
        translated_config["runtimeConfig"]["admin_port"] = config["admin_port"]
    # Add telemetry section only if needed
    telemetry = {}
    # Add OTLPExporter if traces endpoints exist
    otlp_trace_endpoints = config.get("otlp_traces_endpoints", [])
    if otlp_trace_endpoints:
        telemetry["OTLPExporter"] = {"tracesEndpoint": otlp_trace_endpoints[0]}
    # Add logs section if log_level exists
    log_level = config.get("log_level", "")
    if log_level:
        telemetry["logs"] = {"logLevel": log_level}
    # Only add telemetry section if it has content
    if telemetry:
        translated_config["telemetry"] = cast(TelemetryConfig, telemetry)

    return translated_config


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


def get_dbos_database_url(config_file_path: str = DBOS_CONFIG_PATH) -> str:
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


def parse_config_file(config_file_path: str = DBOS_CONFIG_PATH) -> Dict[str, Any]:
    with open(config_file_path, "r") as file:
        content = file.read()
        substituted_content = _substitute_env_vars(content)
        data = yaml.safe_load(substituted_content)

    if not isinstance(data, dict):
        raise DBOSInitializationError(
            f"dbos-config.yaml must contain a dictionary, not {type(data)}"
        )
    data = cast(Dict[str, Any], data)

    # Load the JSON schema relative to the package root
    schema_file = resources.files("dbos").joinpath("dbos-config.schema.json")
    with schema_file.open("r") as f:
        schema = json.load(f)

    # Validate the data against the schema
    try:
        validate(instance=data, schema=schema)
    except ValidationError as e:
        raise DBOSInitializationError(f"Validation error: {e}")

    return data


def load_config(
    config_file_path: str = DBOS_CONFIG_PATH,
    *,
    use_db_wizard: bool = True,
    silent: bool = False,
) -> ConfigFile:
    """
    Load the DBOS `ConfigFile` from the specified path (typically `dbos-config.yaml`).

    The configuration is also validated against the configuration file schema.

    Args:
        config_file_path (str): The path to the yaml configuration file.

    Returns:
        ConfigFile: The loaded configuration

    """

    init_logger()

    data = parse_config_file(config_file_path)

    config: ConfigFile = process_config(data=data, silent=silent)
    # Check the connectivity to the database and make sure it's properly configured
    # Note, never use db wizard if the DBOS is running in debug mode (i.e. DBOS_DEBUG_WORKFLOW_ID env var is set)
    debugWorkflowId = os.getenv("DBOS_DEBUG_WORKFLOW_ID")
    if use_db_wizard and debugWorkflowId is None:
        config = db_wizard(config, config_file_path)

    if "local_suffix" in config["database"] and config["database"]["local_suffix"]:
        config["database"]["app_db_name"] = f"{config['database']['app_db_name']}_local"

    return config


def process_config(
    *,
    data: Union[ConfigFile, Dict[str, Any]],
    silent: bool = False,
) -> ConfigFile:

    if "database" not in data:
        data["database"] = {}

    if "name" not in data:
        raise DBOSInitializationError(
            f"dbos-config.yaml must specify an application name"
        )

    if "runtimeConfig" not in data or "start" not in data["runtimeConfig"]:
        raise DBOSInitializationError(f"dbos-config.yaml must specify a start command")

    if not _is_valid_app_name(data["name"]):
        raise DBOSInitializationError(
            f'Invalid app name {data["name"]}.  App names must be between 3 and 30 characters long and contain only lowercase letters, numbers, dashes, and underscores.'
        )

    if "app_db_name" not in data["database"]:
        data["database"]["app_db_name"] = _app_name_to_db_name(data["name"])

    # Load the DB connection file. Use its values for missing fields from dbos-config.yaml. Use defaults otherwise.
    data = cast(ConfigFile, data)
    db_connection = load_db_connection()
    if not silent:
        if os.getenv("DBOS_DBHOST"):
            print(
                "[bold blue]Loading database connection parameters from debug environment variables[/bold blue]"
            )
        elif data["database"].get("hostname"):
            print(
                "[bold blue]Loading database connection parameters from dbos-config.yaml[/bold blue]"
            )
        elif db_connection.get("hostname"):
            print(
                "[bold blue]Loading database connection parameters from .dbos/db_connection[/bold blue]"
            )
        else:
            print(
                "[bold blue]Using default database connection parameters (localhost)[/bold blue]"
            )

    dbos_dbport: Optional[int] = None
    dbport_env = os.getenv("DBOS_DBPORT")
    if dbport_env:
        try:
            dbos_dbport = int(dbport_env)
        except ValueError:
            pass
    dbos_dblocalsuffix: Optional[bool] = None
    dblocalsuffix_env = os.getenv("DBOS_DBLOCALSUFFIX")
    if dblocalsuffix_env:
        try:
            dbos_dblocalsuffix = dblocalsuffix_env.casefold() == "true".casefold()
        except ValueError:
            pass

    data["database"]["hostname"] = (
        os.getenv("DBOS_DBHOST")
        or data["database"].get("hostname")
        or db_connection.get("hostname")
        or "localhost"
    )

    data["database"]["port"] = (
        dbos_dbport or data["database"].get("port") or db_connection.get("port") or 5432
    )
    data["database"]["username"] = (
        os.getenv("DBOS_DBUSER")
        or data["database"].get("username")
        or db_connection.get("username")
        or "postgres"
    )
    data["database"]["password"] = (
        os.getenv("DBOS_DBPASSWORD")
        or data["database"].get("password")
        or db_connection.get("password")
        or os.environ.get("PGPASSWORD")
        or "dbos"
    )

    local_suffix = False
    dbcon_local_suffix = db_connection.get("local_suffix")
    if dbcon_local_suffix is not None:
        local_suffix = dbcon_local_suffix
    db_local_suffix = data["database"].get("local_suffix")
    if db_local_suffix is not None:
        local_suffix = db_local_suffix
    if dbos_dblocalsuffix is not None:
        local_suffix = dbos_dblocalsuffix
    data["database"]["local_suffix"] = local_suffix
    # Return data as ConfigFile type
    return data


def _is_valid_app_name(name: str) -> bool:
    name_len = len(name)
    if name_len < 3 or name_len > 30:
        return False
    match = re.match("^[a-z0-9-_]+$", name)
    return True if match != None else False


def _app_name_to_db_name(app_name: str) -> str:
    name = app_name.replace("-", "_")
    return name if not name[0].isdigit() else f"_{name}"


def set_env_vars(config: ConfigFile) -> None:
    for env, value in config.get("env", {}).items():
        if value is not None:
            os.environ[env] = str(value)


def overwrite_config(provided_config: ConfigFile) -> ConfigFile:
    # Load the DBOS configuration file and force the use of:
    # 1. The database connection parameters (sub the file data to the provided config)
    # 2. OTLP traces endpoints (add the config data to the provided config)
    # 3. Custom setup steps (sub the file data to the provided config)
    # ? Name: should we override it with the config file ?
    # ? logs level: right now this code ignores log level from the config file.

    config_from_file = parse_config_file()
    # Be defensive
    if config_from_file is None:
        return provided_config

    # Database config
    provided_config["database"]["hostname"] = config_from_file["database"]["hostname"]
    provided_config["database"]["port"] = config_from_file["database"]["port"]
    provided_config["database"]["username"] = config_from_file["database"]["username"]
    provided_config["database"]["password"] = config_from_file["database"]["password"]
    provided_config["database"]["app_db_name"] = config_from_file["database"][
        "app_db_name"
    ]
    provided_config["database"]["sys_db_name"] = config_from_file["database"][
        "sys_db_name"
    ]
    provided_config["database"]["ssl"] = config_from_file["database"]["ssl"]
    provided_config["database"]["ssl_ca"] = config_from_file["database"]["ssl_ca"]

    # Telemetry config
    if "telemetry" not in provided_config or provided_config["telemetry"] is None:
        provided_config["telemetry"] = {
            "OTLPExporter": {},
        }
    elif "OTLPExporter" not in provided_config["telemetry"]:
        provided_config["telemetry"]["OTLPExporter"] = {}

    # This is a super messy from a typing perspective.
    # Some of ConfigFile keys are optional -- but in practice they'll always be present in hosted environments
    # So, for Mypy, we have to (1) check the keys are present in config_from_file and (2) cast telemetry/otlp_exporters to Dict[str, Any]
    # (2) is required because, even tho we resolved these keys earlier, mypy doesn't remember that
    if (
        config_from_file.get("telemetry")
        and config_from_file["telemetry"]
        and config_from_file["telemetry"].get("OTLPExporter")
    ):

        telemetry = cast(Dict[str, Any], provided_config["telemetry"])
        otlp_exporter = cast(Dict[str, Any], telemetry["OTLPExporter"])

        source_otlp = config_from_file["telemetry"]["OTLPExporter"]
        if "tracesEndpoint" in source_otlp:
            otlp_exporter["tracesEndpoint"] = source_otlp["tracesEndpoint"]
        if "logsEndpoint" in source_otlp:
            otlp_exporter["logsEndpoint"] = source_otlp["logsEndpoint"]

    # Runtime config
    if (
        "runtimeConfig" in provided_config
        and "admin_port" in provided_config["runtimeConfig"]
    ):
        del provided_config["runtimeConfig"][
            "admin_port"
        ]  # Admin port is expected to be 3001 (the default in dbos/_admin_server.py::__init__ ) by DBOS Cloud

    # Env should be set from the hosting provider (e.g., DBOS Cloud)
    if "env" in provided_config:
        del provided_config["env"]

    return provided_config
