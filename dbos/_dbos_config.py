import json
import os
import re
import sys
from importlib import resources
from typing import Any, Dict, List, Optional, Tuple, TypedDict, Union, cast

if sys.version_info < (3, 10):
    from typing_extensions import TypeGuard
else:
    from typing import TypeGuard

import yaml
from jsonschema import ValidationError, validate
from rich import print
from sqlalchemy import URL, make_url

from ._error import DBOSInitializationError
from ._logger import dbos_logger

DBOS_CONFIG_PATH = "dbos-config.yaml"


class DBOSConfig(TypedDict, total=False):
    """
    Data structure containing the DBOS library configuration.

    Attributes:
        name (str): Application name
        database_url (str): Database connection string
        app_db_pool_size (int): Application database pool size
        sys_db_name (str): System database name
        sys_db_pool_size (int): System database pool size
        log_level (str): Log level
        otlp_traces_endpoints: List[str]: OTLP traces endpoints
        otlp_logs_endpoints: List[str]: OTLP logs endpoints
        admin_port (int): Admin port
        run_admin_server (bool): Whether to run the DBOS admin server
    """

    name: str
    database_url: Optional[str]
    app_db_pool_size: Optional[int]
    sys_db_name: Optional[str]
    sys_db_pool_size: Optional[int]
    log_level: Optional[str]
    otlp_traces_endpoints: Optional[List[str]]
    otlp_logs_endpoints: Optional[List[str]]
    admin_port: Optional[int]
    run_admin_server: Optional[bool]


class RuntimeConfig(TypedDict, total=False):
    start: List[str]
    setup: Optional[List[str]]
    admin_port: Optional[int]
    run_admin_server: Optional[bool]


class DatabaseConfig(TypedDict, total=False):
    """
    Data structure containing the DBOS database configuration.
    Attributes:
        app_db_pool_size (int): Application database pool size
        sys_db_name (str): System database name
        sys_db_pool_size (int): System database pool size
        migrate (List[str]): Migration commands to run on startup
    """

    hostname: str  # Will be removed in a future version
    port: int  # Will be removed in a future version
    username: str  # Will be removed in a future version
    password: str  # Will be removed in a future version
    connectionTimeoutMillis: Optional[int]  # Will be removed in a future version
    app_db_name: str  # Will be removed in a future version
    app_db_pool_size: Optional[int]
    sys_db_name: Optional[str]
    sys_db_pool_size: Optional[int]
    ssl: Optional[bool]  # Will be removed in a future version
    ssl_ca: Optional[str]  # Will be removed in a future version
    migrate: Optional[List[str]]
    rollback: Optional[List[str]]  # Will be removed in a future version


class OTLPExporterConfig(TypedDict, total=False):
    logsEndpoint: Optional[List[str]]
    tracesEndpoint: Optional[List[str]]


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
        runtimeConfig (RuntimeConfig): Configuration for request serving
        database (DatabaseConfig): Configure pool sizes, migrate commands
        database_url (str): Database connection string
        telemetry (TelemetryConfig): Configuration for tracing / logging
        env (Dict[str,str]): Environment varialbes
        application (Dict[str, Any]): Application-specific configuration section

    """

    name: str
    runtimeConfig: RuntimeConfig
    database: DatabaseConfig
    database_url: Optional[str]
    telemetry: Optional[TelemetryConfig]
    env: Dict[str, str]


def is_dbos_configfile(data: Union[ConfigFile, DBOSConfig]) -> TypeGuard[DBOSConfig]:
    """
    Type guard to check if the provided data is a DBOSConfig.

    Args:
        data: The configuration object to check

    Returns:
        True if the data is a DBOSConfig, False otherwise
    """
    return (
        isinstance(data, dict)
        and "name" in data
        and (
            "runtimeConfig" in data
            or "database" in data
            or "env" in data
            or "telemetry" in data
        )
    )


def translate_dbos_config_to_config_file(config: DBOSConfig) -> ConfigFile:
    if "name" not in config:
        raise DBOSInitializationError(f"Configuration must specify an application name")

    translated_config: ConfigFile = {
        "name": config["name"],
    }

    # Database config
    db_config: DatabaseConfig = {}
    if "sys_db_name" in config:
        db_config["sys_db_name"] = config.get("sys_db_name")
    if "app_db_pool_size" in config:
        db_config["app_db_pool_size"] = config.get("app_db_pool_size")
    if "sys_db_pool_size" in config:
        db_config["sys_db_pool_size"] = config.get("sys_db_pool_size")
    if db_config:
        translated_config["database"] = db_config

    if "database_url" in config:
        translated_config["database_url"] = config.get("database_url")

    # Runtime config
    translated_config["runtimeConfig"] = {"run_admin_server": True}
    if "admin_port" in config:
        translated_config["runtimeConfig"]["admin_port"] = config["admin_port"]
    if "run_admin_server" in config:
        translated_config["runtimeConfig"]["run_admin_server"] = config[
            "run_admin_server"
        ]

    # Telemetry config
    telemetry: TelemetryConfig = {
        "OTLPExporter": {"tracesEndpoint": [], "logsEndpoint": []}
    }
    # For mypy
    assert telemetry["OTLPExporter"] is not None

    # Add OTLPExporter if traces endpoints exist
    otlp_trace_endpoints = config.get("otlp_traces_endpoints")
    if isinstance(otlp_trace_endpoints, list) and len(otlp_trace_endpoints) > 0:
        telemetry["OTLPExporter"]["tracesEndpoint"] = otlp_trace_endpoints
    # Same for the logs
    otlp_logs_endpoints = config.get("otlp_logs_endpoints")
    if isinstance(otlp_logs_endpoints, list) and len(otlp_logs_endpoints) > 0:
        telemetry["OTLPExporter"]["logsEndpoint"] = otlp_logs_endpoints

    # Default to INFO -- the logging seems to default to WARN otherwise.
    log_level = config.get("log_level", "INFO")
    if log_level:
        telemetry["logs"] = {"logLevel": log_level}
    if telemetry:
        translated_config["telemetry"] = telemetry

    return translated_config


def _substitute_env_vars(content: str, silent: bool = False) -> str:

    # Regex to match ${DOCKER_SECRET:SECRET_NAME} style placeholders for Docker secrets
    secret_regex = r"\$\{DOCKER_SECRET:([^}]+)\}"
    # Regex to match ${VAR_NAME} style placeholders for environment variables
    env_regex = r"\$\{(?!DOCKER_SECRET:)([^}]+)\}"

    def replace_env_func(match: re.Match[str]) -> str:
        var_name = match.group(1)
        value = os.environ.get(
            var_name, ""
        )  # If the env variable is not set, return an empty string
        if value == "" and not silent:
            dbos_logger.warning(
                f"Variable {var_name} would be substituted from the process environment into dbos-config.yaml, but is not defined"
            )
        return value

    def replace_secret_func(match: re.Match[str]) -> str:
        secret_name = match.group(1)
        try:
            # Docker secrets are stored in /run/secrets/
            secret_path = f"/run/secrets/{secret_name}"
            if os.path.exists(secret_path):
                with open(secret_path, "r") as f:
                    return f.read().strip()
            elif not silent:
                dbos_logger.warning(
                    f"Docker secret {secret_name} would be substituted from /run/secrets/{secret_name}, but the file does not exist"
                )
            return ""
        except Exception as e:
            if not silent:
                dbos_logger.warning(
                    f"Error reading Docker secret {secret_name}: {str(e)}"
                )
            return ""

    # First replace Docker secrets
    content = re.sub(secret_regex, replace_secret_func, content)
    # Then replace environment variables
    return re.sub(env_regex, replace_env_func, content)


def get_dbos_database_url(config_file_path: str = DBOS_CONFIG_PATH) -> str:
    """
    Retrieve application database URL from configuration `.yaml` file and returns it as a string with clear password

    Args:
        config_file_path (str): The path to the yaml configuration file.

    Returns:
        str: Database URL for the application database

    """
    dbos_config = load_config(config_file_path, run_process_config=True)
    assert dbos_config["database_url"] is not None
    db_url = make_url(dbos_config["database_url"]).set(drivername="postgresql+psycopg")
    return db_url.render_as_string(hide_password=False)


def load_config(
    config_file_path: str = DBOS_CONFIG_PATH,
    *,
    run_process_config: bool = True,
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

    with open(config_file_path, "r") as file:
        content = file.read()
        substituted_content = _substitute_env_vars(content, silent=silent)
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

    # Special case: convert logsEndpoint and tracesEndpoint from strings to lists of strings, if present
    if "telemetry" in data and "OTLPExporter" in data["telemetry"]:
        if "logsEndpoint" in data["telemetry"]["OTLPExporter"]:
            data["telemetry"]["OTLPExporter"]["logsEndpoint"] = [
                data["telemetry"]["OTLPExporter"]["logsEndpoint"]
            ]
        if "tracesEndpoint" in data["telemetry"]["OTLPExporter"]:
            data["telemetry"]["OTLPExporter"]["tracesEndpoint"] = [
                data["telemetry"]["OTLPExporter"]["tracesEndpoint"]
            ]

    data = cast(ConfigFile, data)
    if run_process_config:
        data = process_config(data=data, silent=silent)
    return data  # type: ignore


def process_config(
    *,
    data: ConfigFile,
    silent: bool = False,
) -> ConfigFile:
    """
    If a database_url is provided, pass it as is in the config (MAYBE: and backfill the data.database fields)

    Else, build a database_url from the data.database fields

    Pool sizes still need to be set in data.database

    In debug mode, apply overrides from DBOS_DBHOST, DBOS_DBPORT, DBOS_DBUSER, and DBOS_DBPASSWORD.

    Default configuration:
        - Hostname: localhost
        - Port: 5432
        - Username: postgres
        - Password: $PGPASSWORD
        - Database name: transformed application name. [TODO: update]
    """

    if "name" not in data:
        raise DBOSInitializationError(f"Configuration must specify an application name")

    if not _is_valid_app_name(data["name"]):
        raise DBOSInitializationError(
            f'Invalid app name {data["name"]}.  App names must be between 3 and 30 characters long and contain only lowercase letters, numbers, dashes, and underscores.'
        )

    if data.get("telemetry") is None:
        data["telemetry"] = {}
    telemetry = cast(TelemetryConfig, data["telemetry"])
    if telemetry.get("logs") is None:
        telemetry["logs"] = {}
    logs = cast(LoggerConfig, telemetry["logs"])
    if logs.get("logLevel") is None:
        logs["logLevel"] = "INFO"

    # Handle admin server config
    if not data.get("runtimeConfig"):
        data["runtimeConfig"] = {
            "run_admin_server": True,
        }
    elif "run_admin_server" not in data["runtimeConfig"]:
        data["runtimeConfig"]["run_admin_server"] = True

    isDebugMode = os.getenv("DBOS_DBHOST") is not None

    if "database" not in data:
        data["database"] = {}
    data["database"]["app_db_pool_size"] = data["database"].get("app_db_pool_size", 20)
    data["database"]["sys_db_pool_size"] = data["database"].get("sys_db_pool_size", 20)

    if data.get("database_url") is not None:
        # Parse the db string and check required fields
        assert data["database_url"] is not None
        url = make_url(data["database_url"])
        required_fields = [
            ("username", "Username must be specified in the connection URL"),
            ("password", "Password must be specified in the connection URL"),
            ("host", "Host must be specified in the connection URL"),
            ("database", "Database name must be specified in the connection URL"),
        ]
        for field_name, error_message in required_fields:
            field_value = getattr(url, field_name, None)
            if not field_value:
                raise DBOSInitializationError(error_message)

        # In debug mode perform env vars overrides
        if isDebugMode:
            # Override the username, password, host, and port
            port_str = os.getenv("DBOS_DBPORT")
            port = int(port_str) if port_str is not None and port_str.isdigit() else url.port
            data["database_url"] = url.set(
                username=os.getenv("DBOS_DBUSER", url.username),
                password=os.getenv("DBOS_DBPASSWORD", url.password),
                host=os.getenv("DBOS_DBHOST", url.host),
                port=port,
            ).render_as_string(hide_password=False)

        if not silent and logs["logLevel"] == "INFO" or logs["logLevel"] == "DEBUG":
            url = make_url(data["database_url"]).render_as_string(hide_password=True)
            print(f"[bold blue]Using database connection string: {url}[/bold blue]")
    else:
        if "app_db_name" not in data["database"] or not data["database"]["app_db_name"]:
            _app_db_name = _app_name_to_db_name(data["name"])
        else:
            _app_db_name = data["database"]["app_db_name"]

        dbos_dbport: Optional[int] = None
        dbport_env = os.getenv("DBOS_DBPORT")
        if dbport_env:
            try:
                dbos_dbport = int(dbport_env)
            except ValueError:
                pass

        _hostname = (
            os.getenv("DBOS_DBHOST") or data["database"].get("hostname") or "localhost"
        )
        _port = dbos_dbport or data["database"].get("port") or 5432
        _username = (
            os.getenv("DBOS_DBUSER") or data["database"].get("username") or "postgres"
        )
        _password = (
            os.getenv("DBOS_DBPASSWORD")
            or data["database"].get("password")
            or os.environ.get("PGPASSWORD")
            or "dbos"
        )

        _connect_timeout = data["database"].get("connectionTimeoutMillis", 10000)
        assert _connect_timeout is not None
        _connect_timeout = int(_connect_timeout / 1000)

        connection_string = f"postgres://{_username}:{_password}@{_hostname}:{_port}/{_app_db_name}?connect_timeout={int(_connect_timeout)}"

        # Pretty-print where we've loaded database connection information from, respecting the log level
        if not silent and logs["logLevel"] == "INFO" or logs["logLevel"] == "DEBUG":
            conn_string = make_url(connection_string)
            if isDebugMode:
                print(
                    f"[bold blue]Loading database connection string from debug environment variables: {conn_string}[/bold blue]"
                )
            else:
                print(
                    f"[bold blue]Using default database connection string: {conn_string}[/bold blue]"
                )

        # Finally craft the database url
        data["database_url"] = connection_string

    # Return data as ConfigFile type
    return data


def _is_valid_app_name(name: str) -> bool:
    name_len = len(name)
    if name_len < 3 or name_len > 30:
        return False
    match = re.match("^[a-z0-9-_]+$", name)
    return True if match != None else False


def _app_name_to_db_name(app_name: str) -> str:
    name = app_name.replace("-", "_").replace(" ", "_").lower()
    return name if not name[0].isdigit() else f"_{name}"


def set_env_vars(config: ConfigFile) -> None:
    for env, value in config.get("env", {}).items():
        if value is not None:
            os.environ[env] = str(value)


def overwrite_config(provided_config: ConfigFile) -> ConfigFile:
    # Load the DBOS configuration file and force the use of:
    # 1. The database connection parameters (sub the file data to the provided config)
    # 2. OTLP traces endpoints (add the config data to the provided config)
    # 3. Use the application name from the file. This is a defensive measure to ensure the application name is whatever it was registered with in the cloud
    # 4. Remove admin_port is provided in code
    # 5. Remove env vars if provided in code
    # Optimistically assume that expected fields in config_from_file are present

    config_from_file = load_config(run_process_config=False)
    # Be defensive
    if config_from_file is None:
        return provided_config

    # Name
    provided_config["name"] = config_from_file["name"]

    # Database config. We build a connection string with sslmode set based on the presence of ssl_ca in the config file.

    # If ssl_ca is present, set sslmode to verify-full
    # If ssl_ca is not present, set sslmode to no-verify
    username = config_from_file["database"]["username"]
    password = config_from_file["database"]["password"]
    hostname = config_from_file["database"]["hostname"]
    port = config_from_file["database"]["port"]
    dbname = config_from_file["database"]["app_db_name"]
    provided_config["database_url"] = (
        f"postgres://{username}:{password}@{hostname}:{port}/{dbname}?connect_timeout=10"
    )
    assert provided_config["database_url"] is not None
    if "ssl_ca" in config_from_file["database"]:
        ssl_ca = config_from_file["database"]["ssl_ca"]
        provided_config["database_url"] += f"&sslmode=verify-full&sslrootcert={ssl_ca}"
    else:
        provided_config["database_url"] += "&sslmode=no-verify"

    if "database" not in provided_config:
        provided_config["database"] = {}
    provided_config["database"]["sys_db_name"] = config_from_file["database"][
        "sys_db_name"
    ]

    # Telemetry config
    if "telemetry" not in provided_config or provided_config["telemetry"] is None:
        provided_config["telemetry"] = {
            "OTLPExporter": {"tracesEndpoint": [], "logsEndpoint": []},
        }
    elif "OTLPExporter" not in provided_config["telemetry"]:
        provided_config["telemetry"]["OTLPExporter"] = {
            "tracesEndpoint": [],
            "logsEndpoint": [],
        }

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

        # Merge the logsEndpoint and tracesEndpoint lists from the file with what we have
        source_otlp = config_from_file["telemetry"]["OTLPExporter"]
        if source_otlp:
            tracesEndpoint = source_otlp.get("tracesEndpoint")
            if tracesEndpoint:
                otlp_exporter["tracesEndpoint"].extend(tracesEndpoint)
            logsEndpoint = source_otlp.get("logsEndpoint")
            if logsEndpoint:
                otlp_exporter["logsEndpoint"].extend(logsEndpoint)

    # Runtime config
    if "runtimeConfig" in provided_config:
        if "admin_port" in provided_config["runtimeConfig"]:
            del provided_config["runtimeConfig"][
                "admin_port"
            ]  # Admin port is expected to be 3001 (the default in dbos/_admin_server.py::__init__ ) by DBOS Cloud
        if "run_admin_server" in provided_config["runtimeConfig"]:
            del provided_config["runtimeConfig"]["run_admin_server"]

    # Env should be set from the hosting provider (e.g., DBOS Cloud)
    if "env" in provided_config:
        del provided_config["env"]

    return provided_config


def check_config_consistency(
    *,
    name: str,
    config_file_path: str = DBOS_CONFIG_PATH,
) -> None:
    # First load the config file and check whether it is present
    try:
        config = load_config(config_file_path, silent=True, run_process_config=False)
    except FileNotFoundError:
        dbos_logger.debug(
            f"No configuration file {config_file_path} found. Skipping consistency check with provided config."
        )
        return
    except Exception as e:
        raise e

    # Check the name
    if name != config["name"]:
        raise DBOSInitializationError(
            f"Provided app name '{name}' does not match the app name '{config['name']}' in {config_file_path}."
        )
