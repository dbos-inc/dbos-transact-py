from typing import TypedDict, Optional, List, Dict
import yaml
import json
from jsonschema import validate, ValidationError

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

class HttpConfig(TypedDict, total=False):
    cors_middleware: Optional[bool]
    credentials: Optional[bool]
    allowed_origins: Optional[List[str]]

class OTLPExporterConfig(TypedDict, total=False):
    logsEndpoint: Optional[str]
    tracesEndpoint: Optional[str]

class LoggerConfig(TypedDict, total=False):
    pass # Not used in Python

class TelemetryConfig(TypedDict, total=False):
    logs: Optional[LoggerConfig]
    OTLPExporter: Optional[OTLPExporterConfig]

class DBOSRuntimeConfig(TypedDict):
    pass # Not used in Python

class ConfigFile(TypedDict):
    database: DatabaseConfig
    http: Optional[HttpConfig]
    telemetry: Optional[TelemetryConfig]
    application: dict
    env: Dict[str, str]
    runtimeConfig: Optional[DBOSRuntimeConfig]

def load_config(configFilePath: str) -> ConfigFile:
    # Load the YAML file
    with open(configFilePath, 'r') as file:
        data = yaml.safe_load(file)

    # # Load the JSON schema
    # with open('dbos-config.schema.json', 'r') as schema_file:
    #     schema = json.load(schema_file)

    # # Validate the data against the schema
    # try:
    #     validate(instance=data, schema=schema)
    # except ValidationError as e:
    #     raise ValueError(f"Validation error: {e}")

    # Return data as ConfigFile type
    return data  # type: ignore