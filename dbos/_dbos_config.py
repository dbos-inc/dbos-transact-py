import os
import re
from importlib import resources
from typing import Any, Dict, List, Optional, TypedDict, cast

import sqlalchemy as sa
import yaml
from sqlalchemy import make_url

from dbos._serialization import Serializer

from ._error import DBOSInitializationError
from ._logger import dbos_logger
from ._schemas.system_database import SystemSchema

DBOS_CONFIG_PATH = "dbos-config.yaml"


class DBOSConfig(TypedDict, total=False):
    """
    Data structure containing the DBOS library configuration.

    Attributes:
        name (str): Application name
        system_database_url (str): Connection string for the DBOS system database. Defaults to sqlite:///{name} if not provided.
        application_database_url (str): Connection string for the DBOS application database, in which DBOS @Transaction functions run. Optional. Should be the same type of database (SQLite or Postgres) as the system database.
        database_url (str): (DEPRECATED) Database connection string
        sys_db_pool_size (int): System database pool size
        db_engine_kwargs (Dict[str, Any]): SQLAlchemy engine kwargs (See https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine)
        log_level (str): Log level
        otlp_log_level: Optional[str]: log level specficially for OTLP logging (if enabled); must be no less severe than log_level
        console_log_level: Optional[str]: log level specficially for console logging; must be no less severe than log_level
        otlp_traces_endpoints: List[str]: OTLP traces endpoints
        otlp_logs_endpoints: List[str]: OTLP logs endpoints
        admin_port (int): Admin port
        run_admin_server (bool): Whether to run the DBOS admin server
        otlp_attributes (dict[str, str]): A set of custom attributes to apply OTLP-exported logs and traces
        application_version (str): Application version
        executor_id (str): Executor ID, used to identify the application instance in distributed environments
        dbos_system_schema (str): Schema name for DBOS system tables. Defaults to "dbos".
        enable_otlp (bool): If True, enable built-in DBOS OTLP tracing and logging.
        system_database_engine (sa.Engine): A custom system database engine. If provided, DBOS will not create an engine but use this instead.
        conductor_key (str): An API key for DBOS Conductor. Pass this in to connect your process to Conductor.
        conductor_url (str): The websockets URL for your DBOS Conductor service. Only set if you're self-hosting Conductor.
        serializer (Serializer): A custom serializer and deserializer DBOS uses when storing program data in the system database
        use_listen_notify (bool): Whether to use LISTEN/NOTIFY or polling to listen for notifications and events.  Defaults to True. As this affects migrations, may not be changed after the system database is first created.
    """

    name: str
    system_database_url: Optional[str]
    application_database_url: Optional[str]
    database_url: Optional[str]
    sys_db_pool_size: Optional[int]
    db_engine_kwargs: Optional[Dict[str, Any]]
    log_level: Optional[str]
    otlp_log_level: Optional[str]
    console_log_level: Optional[str]
    otlp_traces_endpoints: Optional[List[str]]
    otlp_logs_endpoints: Optional[List[str]]
    admin_port: Optional[int]
    run_admin_server: Optional[bool]
    otlp_attributes: Optional[dict[str, str]]
    application_version: Optional[str]
    executor_id: Optional[str]
    dbos_system_schema: Optional[str]
    enable_otlp: Optional[bool]
    system_database_engine: Optional[sa.Engine]
    conductor_key: Optional[str]
    conductor_url: Optional[str]
    serializer: Optional[Serializer]
    enable_patching: Optional[bool]
    use_listen_notify: Optional[bool]
    max_executor_threads: Optional[int]


class RuntimeConfig(TypedDict, total=False):
    start: List[str]
    setup: Optional[List[str]]
    admin_port: Optional[int]
    run_admin_server: Optional[bool]
    max_executor_threads: Optional[int]


class DatabaseConfig(TypedDict, total=False):
    sys_db_pool_size: Optional[int]
    db_engine_kwargs: Optional[Dict[str, Any]]
    sys_db_engine_kwargs: Optional[Dict[str, Any]]
    migrate: Optional[List[str]]


class OTLPExporterConfig(TypedDict, total=False):
    logsEndpoint: Optional[List[str]]
    tracesEndpoint: Optional[List[str]]


class LoggerConfig(TypedDict, total=False):
    logLevel: Optional[str]
    consoleLogLevel: Optional[str]
    otlpLogLevel: Optional[str]


class TelemetryConfig(TypedDict, total=False):
    logs: Optional[LoggerConfig]
    OTLPExporter: Optional[OTLPExporterConfig]
    otlp_attributes: Optional[dict[str, str]]
    disable_otlp: bool


class ConfigFile(TypedDict, total=False):
    """
    Data structure containing the DBOS Configuration.
    The DBOSConfig object is parsed into this.
    """

    name: str
    runtimeConfig: RuntimeConfig
    database: DatabaseConfig
    database_url: Optional[str]
    system_database_url: Optional[str]
    telemetry: Optional[TelemetryConfig]
    env: Dict[str, str]
    system_database_engine: Optional[sa.Engine]
    dbos_system_schema: Optional[str]
    use_listen_notify: bool


def translate_dbos_config_to_config_file(config: DBOSConfig) -> ConfigFile:
    if "name" not in config:
        raise DBOSInitializationError(f"Configuration must specify an application name")

    translated_config: ConfigFile = {
        "name": config["name"],
    }

    # Database config
    db_config: DatabaseConfig = {}
    if "sys_db_pool_size" in config:
        db_config["sys_db_pool_size"] = config.get("sys_db_pool_size")
    if "db_engine_kwargs" in config:
        db_config["db_engine_kwargs"] = config.get("db_engine_kwargs")
    if db_config:
        translated_config["database"] = db_config

    # Use application_database_url instead of the deprecated database_url if provided
    if "database_url" in config:
        translated_config["database_url"] = config.get("database_url")
    elif "application_database_url" in config:
        translated_config["database_url"] = config.get("application_database_url")

    if "system_database_url" in config:
        translated_config["system_database_url"] = config.get("system_database_url")

    if "dbos_system_schema" in config:
        translated_config["dbos_system_schema"] = config.get("dbos_system_schema")

    # Runtime config
    translated_config["runtimeConfig"] = {"run_admin_server": True}
    if "admin_port" in config:
        translated_config["runtimeConfig"]["admin_port"] = config["admin_port"]
    if "run_admin_server" in config:
        translated_config["runtimeConfig"]["run_admin_server"] = config[
            "run_admin_server"
        ]
    if "max_executor_threads" in config:
        translated_config["runtimeConfig"]["max_executor_threads"] = config[
            "max_executor_threads"
        ]

    # Telemetry config
    enable_otlp = config.get("enable_otlp", None)
    disable_otlp = True if enable_otlp is None else not enable_otlp
    telemetry: TelemetryConfig = {
        "OTLPExporter": {"tracesEndpoint": [], "logsEndpoint": []},
        "otlp_attributes": config.get("otlp_attributes", {}),
        "disable_otlp": disable_otlp,
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
    otlp_log_level = config.get("otlp_log_level", log_level)
    console_log_level = config.get("console_log_level", log_level)
    if log_level:
        telemetry["logs"] = LoggerConfig(
            logLevel=log_level,
            consoleLogLevel=cast(str, console_log_level),
            otlpLogLevel=cast(str, otlp_log_level),
        )
    if telemetry:
        translated_config["telemetry"] = telemetry

    translated_config["system_database_engine"] = config.get("system_database_engine")
    use_listen_notify = config.get("use_listen_notify", None)
    translated_config["use_listen_notify"] = (
        use_listen_notify if use_listen_notify is not None else True
    )

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


def load_config(
    config_file_path: str = DBOS_CONFIG_PATH,
    *,
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

    return cast(ConfigFile, data)


def process_config(
    *,
    data: ConfigFile,
    silent: bool = False,
) -> ConfigFile:
    """
    If a database_url is provided, pass it as is in the config.

    Else, default to SQLite.

    Also build SQL Alchemy "kwargs" base on user input + defaults.
    Specifically, db_engine_kwargs takes precedence over app_db_pool_size
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

    # Ensure database dict exists
    data.setdefault("database", {})
    connect_timeout = None

    # Process the application database URL, if provided
    if data.get("database_url"):
        # Parse the db string and check required fields
        assert data["database_url"] is not None
        assert is_valid_database_url(data["database_url"])

        url = make_url(data["database_url"])

        # Gather connect_timeout from the URL if provided. It should be used in engine kwargs if not provided there (instead of our default)
        connect_timeout_str = url.query.get("connect_timeout")
        if connect_timeout_str is not None:
            assert isinstance(
                connect_timeout_str, str
            ), "connect_timeout must be a string and defined once in the URL"
            if connect_timeout_str.isdigit():
                connect_timeout = int(connect_timeout_str)

    if (
        data.get("system_database_engine")
        and not data.get("system_database_url")
        and not data.get("database_url")
    ):
        engine = data.get("system_database_engine")
        assert engine is not None
        if "sqlite" in engine.dialect.name:
            data["system_database_url"] = (
                "sqlite:///custom_system_database_engine.sqlite"
            )
        else:
            data["system_database_url"] = "postgresql://custom:system@database/engine"

    # Process the system database URL, if provided
    if data.get("system_database_url"):
        # Parse the db string and check required fields
        assert data["system_database_url"]
        assert is_valid_database_url(data["system_database_url"])

        url = make_url(data["system_database_url"])

        # Gather connect_timeout from the URL if provided. It should be used in engine kwargs if not provided there (instead of our default). This overrides a timeout from the application database, if any.
        connect_timeout_str = url.query.get("connect_timeout")
        if connect_timeout_str is not None:
            assert isinstance(
                connect_timeout_str, str
            ), "connect_timeout must be a string and defined once in the URL"
            if connect_timeout_str.isdigit():
                connect_timeout = int(connect_timeout_str)

    # If an application database URL is provided but not the system database URL,
    # construct the system database URL.
    if data.get("database_url") and not data.get("system_database_url"):
        assert data["database_url"]
        if data["database_url"].startswith("sqlite"):
            data["system_database_url"] = data["database_url"]
        else:
            url = make_url(data["database_url"])
            assert url.database
            url = url.set(database=f"{url.database}{SystemSchema.sysdb_suffix}")
            data["system_database_url"] = url.render_as_string(hide_password=False)

    # If a system database URL is provided but not an application database URL,
    # do not create an application database.
    if data.get("system_database_url") and not data.get("database_url"):
        assert data["system_database_url"]
        data["database_url"] = None

    # If neither URL is provided, use a default SQLite system database URL.
    if not data.get("database_url") and not data.get("system_database_url"):
        _app_db_name = _app_name_to_db_name(data["name"])
        data["system_database_url"] = f"sqlite:///{_app_db_name}.sqlite"
        data["database_url"] = None

    configure_db_engine_parameters(data["database"], connect_timeout=connect_timeout)

    # Return data as ConfigFile type
    return data


def configure_db_engine_parameters(
    data: DatabaseConfig, connect_timeout: Optional[int] = None
) -> None:
    """
    Configure SQLAlchemy engine parameters for both user and system databases.

    If provided, sys_db_pool_size will take precedence over user_kwargs for the system db engine.

    Args:
        data: Configuration dictionary containing database settings
    """

    # Configure user database engine parameters
    app_engine_kwargs: dict[str, Any] = {
        "connect_args": {"application_name": "dbos_transact"},
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 20,
        "pool_pre_ping": True,
    }
    # If user-provided kwargs are present, use them instead
    user_kwargs = data.get("db_engine_kwargs")
    if user_kwargs is not None:
        app_engine_kwargs.update(user_kwargs)

    # If user-provided kwargs do not contain connect_timeout, check if their URL did (this function connect_timeout parameter).
    # Else default to 10
    if "connect_args" not in app_engine_kwargs:
        app_engine_kwargs["connect_args"] = {}
    if "connect_timeout" not in app_engine_kwargs["connect_args"]:
        app_engine_kwargs["connect_args"]["connect_timeout"] = (
            connect_timeout if connect_timeout else 10
        )

    # Configure system database engine parameters. User-provided sys_db_pool_size takes precedence
    system_engine_kwargs = app_engine_kwargs.copy()
    if data.get("sys_db_pool_size") is not None:
        system_engine_kwargs["pool_size"] = data["sys_db_pool_size"]

    data["db_engine_kwargs"] = app_engine_kwargs
    data["sys_db_engine_kwargs"] = system_engine_kwargs


def is_valid_database_url(database_url: str) -> bool:
    if database_url.startswith("sqlite"):
        return True
    url = make_url(database_url)
    required_fields = [
        ("database", "Database name must be specified in the connection URL"),
    ]
    for field_name, error_message in required_fields:
        field_value = getattr(url, field_name, None)
        if not field_value:
            raise DBOSInitializationError(error_message)
    return True


def _is_valid_app_name(name: str) -> bool:
    name_len = len(name)
    if name_len < 3 or name_len > 30:
        return False
    match = re.match("^[a-z0-9-_]+$", name)
    return True if match != None else False


def _app_name_to_db_name(app_name: str) -> str:
    name = app_name.replace("-", "_").replace(" ", "_").lower()
    return name if not name[0].isdigit() else f"_{name}"


def overwrite_config(provided_config: ConfigFile) -> ConfigFile:
    # Load the DBOS configuration file and force the use of:
    # 1. The application and system database url provided by DBOS_DATABASE_URL and DBOS_SYSTEM_DATABASE_URL
    # 2. OTLP traces endpoints (add the config data to the provided config)
    # 3. Use the application name from the file. This is a defensive measure to ensure the application name is whatever it was registered with in the cloud
    # 4. Remove admin_port is provided in code
    # 5. Remove env vars if provided in code
    # Optimistically assume that expected fields in config_from_file are present

    config_from_file = load_config()
    # Be defensive
    if config_from_file is None:
        return provided_config

    # Set the application name to the cloud app name
    provided_config["name"] = config_from_file["name"]

    # Use the DBOS Cloud application and system database URLs
    db_url = os.environ.get("DBOS_DATABASE_URL")
    if db_url is None:
        raise DBOSInitializationError(
            "DBOS_DATABASE_URL environment variable is not set. This is required to connect to the database."
        )
    provided_config["database_url"] = db_url
    system_db_url = os.environ.get("DBOS_SYSTEM_DATABASE_URL")
    if system_db_url is None:
        raise DBOSInitializationError(
            "DBOS_SYSTEM_DATABASE_URL environment variable is not set. This is required to connect to the database."
        )
    provided_config["system_database_url"] = system_db_url
    # Always use the "dbos" schema when deploying to DBOS Cloud
    provided_config["dbos_system_schema"] = "dbos"

    # Telemetry config
    if "telemetry" not in provided_config or provided_config["telemetry"] is None:
        provided_config["telemetry"] = {
            "OTLPExporter": {"tracesEndpoint": [], "logsEndpoint": []},
            "disable_otlp": False,
        }
    else:
        provided_config["telemetry"]["disable_otlp"] = False
        if "OTLPExporter" not in provided_config["telemetry"]:
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


def get_system_database_url(config: ConfigFile) -> str:
    if "system_database_url" in config and config["system_database_url"] is not None:
        return config["system_database_url"]
    else:
        assert config["database_url"] is not None
        if config["database_url"].startswith("sqlite"):
            return config["database_url"]
        app_db_url = make_url(config["database_url"])
        assert app_db_url.database is not None
        sys_db_name = app_db_url.database + SystemSchema.sysdb_suffix
        return app_db_url.set(database=sys_db_name).render_as_string(
            hide_password=False
        )


def get_application_database_url(config: ConfigFile) -> str | None:
    # For backwards compatibility, the application database URL is "database_url"
    if config.get("database_url"):
        assert config["database_url"]
        return config["database_url"]
    else:
        # If the application database URL is not specified, return None
        return None
