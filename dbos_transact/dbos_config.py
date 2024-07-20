from typing import Any, TypedDict, Optional, List, Dict
import yaml
import json
from jsonschema import validate, ValidationError
from importlib import resources

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

class TelemetryConfig(TypedDict, total=False):
    OTLPExporter: Optional[OTLPExporterConfig]

class ConfigFile(TypedDict):
    database: DatabaseConfig
    telemetry: Optional[TelemetryConfig]
    application: Dict[str, Any]
    env: Dict[str, str]

def load_config(configFilePath: str) -> ConfigFile:
    # Load the YAML file
    with open(configFilePath, 'r') as file:
        data = yaml.safe_load(file)

    # Load the JSON schema relative to the package root
    with resources.open_text('dbos_transact', 'dbos-config.schema.json') as schema_file:
        schema = json.load(schema_file)

    # Validate the data against the schema
    try:
        validate(instance=data, schema=schema)
    except ValidationError as e:
        raise ValueError(f"Validation error: {e}")

    # Return data as ConfigFile type
    return data  # type: ignore