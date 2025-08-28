# type: ignore

import os
from unittest.mock import mock_open
from urllib.parse import quote

import pytest
import pytest_mock
from sqlalchemy import event
from sqlalchemy.exc import OperationalError

# Public API
from dbos import DBOS, DBOSClient
from dbos._dbos_config import (
    ConfigFile,
    DBOSConfig,
    configure_db_engine_parameters,
    load_config,
    overwrite_config,
    process_config,
    translate_dbos_config_to_config_file,
)
from dbos._error import DBOSInitializationError
from dbos._schemas.system_database import SystemSchema

mock_filename = "dbos-config.yaml"
original_open = __builtins__["open"]


def generate_mock_open(filenames, mock_files):
    if not isinstance(filenames, list):
        filenames = [filenames]
    if not isinstance(mock_files, list):
        mock_files = [mock_files]

    def conditional_mock_open(*args, **kwargs):
        for filename, mock_file in zip(filenames, mock_files):
            if args[0] == filename:
                m = mock_open(read_data=mock_file)
                return m()
        return original_open(*args, **kwargs)

    return conditional_mock_open


"""
Test all the possible ways to configure DBOS.
- First test the "switches" in the DBOS.__init__() method, ensuring they find the config from the right place
- Then test each of the individual functions that process the config (load_config, overwrite_config, translate_dbos_config_to_config_file, process_config)
"""


####################
# DBOS launch
####################
def test_no_config_provided():
    with pytest.raises(TypeError) as exc_info:
        DBOS()
    assert "missing 1 required keyword-only argument: 'config'" in str(exc_info.value)


def test_dbosconfig_type_provided():
    config: DBOSConfig = {
        "name": "some-app",
        "database_url": f"postgres://postgres:{os.environ.get('PGPASSWORD', 'dbos')}@localhost:5432/some_app",
    }
    dbos = DBOS(config=config)
    assert dbos._config["name"] == "some-app"
    assert dbos._config["database_url"] == config["database_url"]
    dbos.destroy()


####################
# LOAD CONFIG
####################


def test_load_valid_config_file(mocker):
    mock_config = """
        name: "some-app"
        runtimeConfig:
            start:
                - "python3 main.py"
            admin_port: 8001
        database_url: "postgres://user:dbos@localhost:5432/dbname?connect_timeout=10&sslmode=require&sslrootcert=ca.pem"
        system_database_url: "postgres://user:dbos@localhost:5432/dbname_dbos_sys?connect_timeout=10&sslmode=require&sslrootcert=ca.pem"
        telemetry:
            OTLPExporter:
                logsEndpoint: 'fooLogs'
                tracesEndpoint: 'fooTraces'
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename)
    assert configFile["name"] == "some-app"
    assert (
        configFile["database_url"]
        == f"postgres://user:dbos@localhost:5432/dbname?connect_timeout=10&sslmode=require&sslrootcert=ca.pem"
    )
    assert (
        configFile["system_database_url"]
        == f"postgres://user:dbos@localhost:5432/dbname_dbos_sys?connect_timeout=10&sslmode=require&sslrootcert=ca.pem"
    )

    assert configFile["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["fooLogs"]
    assert configFile["telemetry"]["OTLPExporter"]["tracesEndpoint"] == ["fooTraces"]


def test_load_config_with_unset_database_url_env_var(mocker):
    mock_config = """
    name: "some-app"
    database_url: ${UNSET}
    """

    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename)
    assert configFile["name"] == "some-app"


def test_load_config_file_open_error(mocker):
    """Test handling when the config file can't be opened."""
    mocker.patch("builtins.open", side_effect=FileNotFoundError("File not found"))

    with pytest.raises(FileNotFoundError):
        load_config()


def test_load_config_file_not_a_dict(mocker):
    """Test handling when YAML doesn't parse to a dictionary."""
    mock_config = "just a string"
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )

    with pytest.raises(DBOSInitializationError) as exc_info:
        load_config()

    assert "must contain a dictionary" in str(exc_info.value)


def test_load_config_file_schema_validation_error(mocker):
    """Test handling when the config fails schema validation."""
    mock_config = """
    name: "test-app"
    invalid_field: "this shouldn't be here"
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )

    with pytest.raises(DBOSInitializationError) as exc_info:
        load_config()

    assert (
        "Validation error: Additional properties are not allowed ('invalid_field' was unexpected)"
        in str(exc_info.value)
    )


def test_load_config_file_custom_path():
    """Test parsing a config file from a custom path."""
    mock_config = """
    name: "test-app"
    """
    custom_path = "/custom/path/dbos-config.yaml"
    from unittest.mock import mock_open, patch

    with patch("builtins.open", mock_open(read_data=mock_config)) as mock_file:
        result = load_config(custom_path)
        mock_file.assert_called_with(custom_path, "r")
        assert result["name"] == "test-app"


####################
# PROCESS CONFIG
####################


# Full config provided
def test_process_config_full():
    config: ConfigFile = {
        "name": "some-app",
        "database_url": "postgres://user:password@localhost:7777/dbn?connect_timeout=1&sslmode=require&sslrootcert=ca.pem",
        "database": {
            "sys_db_name": "sys_db",
            "sys_db_pool_size": 27,
            "db_engine_kwargs": {"key": "value"},
            "migrate": ["alembic upgrade head"],
        },
        "runtimeConfig": {
            "start": ["python3 main.py"],
            "admin_port": 8001,
            "run_admin_server": False,
            "setup": ["echo 'hello'"],
        },
        "telemetry": {
            "logs": {
                "logLevel": "DEBUG",
            },
            "OTLPExporter": {
                "logsEndpoint": ["thelogsendpoint"],
                "tracesEndpoint": ["thetracesendpoint"],
            },
        },
        "env": {
            "FOO": "BAR",
        },
    }

    configFile = process_config(data=config)
    assert configFile["name"] == "some-app"
    assert (
        configFile["database_url"]
        == "postgres://user:password@localhost:7777/dbn?connect_timeout=1&sslmode=require&sslrootcert=ca.pem"
    )
    assert configFile["database"]["sys_db_name"] == "sys_db"
    assert (
        configFile["system_database_url"]
        == f"postgres://user:password@localhost:7777/{config['database']['sys_db_name']}?connect_timeout=1&sslmode=require&sslrootcert=ca.pem"
    )
    assert configFile["database"]["migrate"] == ["alembic upgrade head"]
    assert configFile["database"]["db_engine_kwargs"] == {
        "key": "value",
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 20,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 1},
    }
    assert configFile["database"]["sys_db_engine_kwargs"] == {
        "key": "value",
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 27,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 1},
    }
    assert configFile["runtimeConfig"]["start"] == ["python3 main.py"]
    assert configFile["runtimeConfig"]["admin_port"] == 8001
    assert configFile["runtimeConfig"]["run_admin_server"] == False
    assert configFile["runtimeConfig"]["setup"] == ["echo 'hello'"]
    assert configFile["telemetry"]["logs"]["logLevel"] == "DEBUG"
    assert configFile["telemetry"]["OTLPExporter"]["logsEndpoint"] == [
        "thelogsendpoint"
    ]
    assert configFile["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "thetracesendpoint"
    ]
    assert configFile["env"]["FOO"] == "BAR"


def test_process_config_system_database():
    config: ConfigFile = {
        "name": "some-app",
        "database_url": "postgres://user:password@localhost:7777/dbn?connect_timeout=1&sslmode=require&sslrootcert=ca.pem",
        "system_database_url": "postgres://user:password@localhost:7778/dbn_sys?connect_timeout=1&sslmode=require&sslrootcert=ca.pem",
        "database": {
            "sys_db_name": "sys_db",
            "sys_db_pool_size": 27,
            "db_engine_kwargs": {"key": "value"},
            "migrate": ["alembic upgrade head"],
        },
    }

    configFile = process_config(data=config)
    assert configFile["name"] == "some-app"
    assert configFile["database_url"] == config["database_url"]
    assert configFile["system_database_url"] == config["system_database_url"]
    assert configFile["database"]["db_engine_kwargs"] == {
        "key": "value",
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 20,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 1},
    }
    assert configFile["database"]["sys_db_engine_kwargs"] == {
        "key": "value",
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 27,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 1},
    }


def test_process_config_only_system_database():
    config: ConfigFile = {
        "name": "some-app",
        "system_database_url": "postgres://user:password@localhost:7778/dbn_sys?connect_timeout=1&sslmode=require&sslrootcert=ca.pem",
    }

    configFile = process_config(data=config)
    assert configFile["name"] == "some-app"
    assert configFile["system_database_url"] == config["system_database_url"]
    assert configFile["database_url"] == config["system_database_url"]


def test_process_config_sqlite():
    config: ConfigFile = {
        "name": "some-app",
        "database_url": "sqlite:///test.sqlite",
        "system_database_url": "sqlite:///test.sys.sqlite",
    }

    configFile = process_config(data=config)
    assert configFile["name"] == "some-app"
    assert configFile["database_url"] == config["database_url"]
    assert configFile["system_database_url"] == config["system_database_url"]


def test_debug_override_database_url(mocker: pytest_mock.MockFixture):
    mocker.patch.dict(
        os.environ,
        {
            "DBOS_DBHOST": "fakehost",
            "DBOS_DBPORT": "1234",
            "DBOS_DBUSER": "fakeuser",
            "DBOS_DBPASSWORD": "fakepassword",
        },
    )
    config: ConfigFile = {
        "name": "some-app",
        "database_url": "postgres://user:password@localhost:7777/dbn?connect_timeout=1&sslmode=require&sslrootcert=ca.pem",
    }
    processed_config = process_config(data=config)
    assert (
        processed_config["database_url"]
        == "postgres://fakeuser:fakepassword@fakehost:1234/dbn?connect_timeout=1&sslmode=require&sslrootcert=ca.pem"
    )
    assert processed_config["name"] == "some-app"
    assert (
        processed_config["system_database_url"]
        == f"postgres://fakeuser:fakepassword@fakehost:1234/dbn{SystemSchema.sysdb_suffix}?connect_timeout=1&sslmode=require&sslrootcert=ca.pem"
    )
    assert processed_config["database"]["db_engine_kwargs"] is not None
    assert processed_config["database"]["sys_db_engine_kwargs"] is not None
    assert processed_config["runtimeConfig"]["run_admin_server"] == True
    assert processed_config["telemetry"]["logs"]["logLevel"] == "INFO"


def test_process_config_load_defaults():
    config: ConfigFile = {
        "name": "some-app",
    }
    processed_config = process_config(data=config)
    assert processed_config["name"] == "some-app"
    assert processed_config["database_url"] == f"sqlite:///some_app.sqlite"
    assert processed_config["system_database_url"] == processed_config["database_url"]
    assert processed_config["database"]["db_engine_kwargs"] is not None
    assert processed_config["database"]["sys_db_engine_kwargs"] is not None
    assert processed_config["telemetry"]["logs"]["logLevel"] == "INFO"
    assert processed_config["runtimeConfig"]["run_admin_server"] == True


def test_process_config_load_default_with_None_database_url():
    config: ConfigFile = {
        "name": "some-app",
        "database_url": None,
    }
    processed_config = process_config(data=config)
    assert processed_config["name"] == "some-app"
    assert processed_config["database_url"] == f"sqlite:///some_app.sqlite"
    assert processed_config["system_database_url"] == processed_config["database_url"]
    assert processed_config["database"]["db_engine_kwargs"] is not None
    assert processed_config["database"]["sys_db_engine_kwargs"] is not None
    assert processed_config["telemetry"]["logs"]["logLevel"] == "INFO"
    assert processed_config["runtimeConfig"]["run_admin_server"] == True


def test_process_config_load_default_with_empty_database_url():
    config: ConfigFile = {
        "name": "some-app",
        "database_url": "",
    }
    processed_config = process_config(data=config)
    assert processed_config["name"] == "some-app"
    assert processed_config["database_url"] == f"sqlite:///some_app.sqlite"
    assert processed_config["system_database_url"] == processed_config["database_url"]
    assert processed_config["database"]["db_engine_kwargs"] is not None
    assert processed_config["database"]["sys_db_engine_kwargs"] is not None
    assert processed_config["telemetry"]["logs"]["logLevel"] == "INFO"
    assert processed_config["runtimeConfig"]["run_admin_server"] == True


def test_config_missing_name():
    config = {}
    with pytest.raises(DBOSInitializationError) as exc_info:
        process_config(data=config)

    assert "must specify an application name" in str(exc_info.value)


def test_config_bad_name():
    config: ConfigFile = {
        "name": "some app",
    }
    with pytest.raises(DBOSInitializationError) as exc_info:
        process_config(data=config)
    assert "Invalid app name" in str(exc_info.value)


def test_config_mixed_params():
    config = {
        "name": "some-app",
        "database": {
            "sys_db_name": "yoohoo",
        },
    }

    configFile = process_config(data=config)
    assert configFile["name"] == "some-app"
    assert configFile["database_url"] == f"sqlite:///some_app.sqlite"
    assert configFile["system_database_url"] == configFile["database_url"]
    assert configFile["database"]["db_engine_kwargs"] is not None
    assert configFile["database"]["sys_db_engine_kwargs"] is not None
    assert configFile["telemetry"]["logs"]["logLevel"] == "INFO"
    assert configFile["runtimeConfig"]["run_admin_server"] == True


####################
# PROCESS DB ENGINE KWARGS
####################
def test_configure_db_engine_parameters_defaults():
    """Test that default values are set when no pool sizes or kwargs are provided."""
    data: DatabaseConfig = {}

    configure_db_engine_parameters(data)

    assert data["db_engine_kwargs"] == {
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 20,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 10},
    }
    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 20,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 10},
    }


def test_configure_db_engine_parameters_custom_sys_db_pool_sizes():
    """Test that custom pool sizes are preserved and used in engine kwargs."""
    data: DatabaseConfig = {"sys_db_pool_size": 35}

    configure_db_engine_parameters(data)

    assert data["db_engine_kwargs"] == {
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 20,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 10},
    }
    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 35,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 10},
    }


def test_configure_db_engine_parameters_user_kwargs_override():
    """Test that user-provided db_engine_kwargs override defaults."""
    data: DatabaseConfig = {
        "sys_db_pool_size": 35,
        "db_engine_kwargs": {
            "pool_timeout": 60,
            "max_overflow": 10,
            "pool_pre_ping": True,
            "custom_param": "value",
            "pool_size": 50,
            "connect_args": {"connect_timeout": 30, "key": "value"},
        },
    }

    configure_db_engine_parameters(data)

    # User kwargs should override defaults and include custom params
    assert data["db_engine_kwargs"] == {
        "pool_timeout": 60,
        "max_overflow": 10,
        "pool_pre_ping": True,
        "custom_param": "value",
        "pool_size": 50,
        "connect_args": {"connect_timeout": 30, "key": "value"},
    }

    # System engine kwargs should use system pool size but same user overrides
    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 60,
        "max_overflow": 10,
        "pool_pre_ping": True,
        "custom_param": "value",
        "pool_size": 35,
        "connect_args": {"connect_timeout": 30, "key": "value"},
    }


def test_configure_db_engine_parameters_user_kwargs_and_db_url_connect_timeout():
    """Test that user-provided db_engine_kwargs override defaults and include connect_timeout from db_url."""
    data: DatabaseConfig = {
        "db_engine_kwargs": {
            "pool_timeout": 60,
            "pool_pre_ping": True,
            "custom_param": "value",
            "pool_size": 50,
        },
    }

    configure_db_engine_parameters(data, connect_timeout=22)

    # User kwargs should override defaults and include custom params
    assert data["db_engine_kwargs"] == {
        "pool_timeout": 60,
        "max_overflow": 0,
        "pool_pre_ping": True,
        "custom_param": "value",
        "pool_size": 50,
        "connect_args": {"connect_timeout": 22},
    }

    # System engine kwargs should use system pool size but same user overrides
    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 60,
        "max_overflow": 0,
        "pool_pre_ping": True,
        "custom_param": "value",
        "pool_size": 50,
        "connect_args": {"connect_timeout": 22},
    }


def test_configure_db_engine_parameters_user_kwargs_plus_db_url_connect_timeout():
    """Test that user-provided db_engine_kwargs override defaults and connect_timeout from db_url."""
    data: DatabaseConfig = {
        "db_engine_kwargs": {
            "pool_timeout": 60,
            "pool_pre_ping": True,
            "custom_param": "value",
            "pool_size": 50,
            "connect_args": {"connect_timeout": 1},
        },
    }

    configure_db_engine_parameters(data, connect_timeout=22)

    # User kwargs should override defaults and include custom params
    assert data["db_engine_kwargs"] == {
        "pool_timeout": 60,
        "max_overflow": 0,
        "pool_pre_ping": True,
        "custom_param": "value",
        "pool_size": 50,
        "connect_args": {"connect_timeout": 1},
    }

    # System engine kwargs should use system pool size but same user overrides
    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 60,
        "max_overflow": 0,
        "pool_pre_ping": True,
        "custom_param": "value",
        "pool_size": 50,
        "connect_args": {"connect_timeout": 1},
    }


def test_configure_db_engine_parameters_user_kwargs_mixed_params():
    """Test that user-provided db_engine_kwargs override defaults."""
    data: DatabaseConfig = {
        "db_engine_kwargs": {
            "pool_timeout": 60,
            "pool_pre_ping": True,
            "custom_param": "value",
            "pool_size": 50,
        }
    }

    configure_db_engine_parameters(data)

    # User kwargs should override defaults and include custom params
    assert data["db_engine_kwargs"] == {
        "pool_timeout": 60,
        "max_overflow": 0,
        "pool_pre_ping": True,
        "custom_param": "value",
        "pool_size": 50,
        "connect_args": {"connect_timeout": 10},
    }

    # System engine kwargs should use system pool size but same user overrides
    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 60,
        "max_overflow": 0,
        "pool_pre_ping": True,
        "custom_param": "value",
        "pool_size": 50,
        "connect_args": {"connect_timeout": 10},
    }


def test_configure_db_engine_parameters_empty_user_kwargs():
    """Test handling of empty user kwargs dict."""
    data: DatabaseConfig = {"db_engine_kwargs": {}}

    configure_db_engine_parameters(data)

    assert data["db_engine_kwargs"] == {
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 20,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 10},
    }
    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 20,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 10},
    }


####################
# VALIDATE DATABASE URL
####################


def test_process_config_with_wrong_db_url():
    # Missing username
    config: ConfigFile = {
        "name": "some-app",
        "database_url": "postgres://:password@h:1234/dbname",
    }
    with pytest.raises(DBOSInitializationError) as exc_info:
        process_config(data=config)
    assert "Username must be specified in the connection URL" in str(exc_info.value)

    # Missing host
    config: ConfigFile = {
        "name": "some-app",
        "database_url": "postgres://user:password@:1234/dbname",
    }
    with pytest.raises(DBOSInitializationError) as exc_info:
        process_config(data=config)
    assert "Host must be specified in the connection URL" in str(exc_info.value)

    # Missing dbname
    config: ConfigFile = {
        "name": "some-app",
        "database_url": "postgres://user:password@h:1234",
    }
    with pytest.raises(DBOSInitializationError) as exc_info:
        process_config(data=config)
    assert "Database name must be specified in the connection URL" in str(
        exc_info.value
    )

    # Missing dbname in system database
    config: ConfigFile = {
        "name": "some-app",
        "system_database_url": "postgres://user:password@h:1234",
    }
    with pytest.raises(DBOSInitializationError) as exc_info:
        process_config(data=config)
    assert "Database name must be specified in the connection URL" in str(
        exc_info.value
    )


def test_database_url_no_password(skip_with_sqlite: None):
    """Test that the database URL can be provided without a password."""
    expected_url = "postgresql://postgres@localhost:5432/dbostestpy?sslmode=disable"
    config: DBOSConfig = {
        "name": "some-app",
        "database_url": expected_url,
    }
    processed_config = translate_dbos_config_to_config_file(config)
    assert processed_config["name"] == "some-app"
    assert processed_config["database_url"] == expected_url

    # Make sure we can use it to construct a DBOS Client and connect to the database without a password
    client = DBOSClient(expected_url)
    try:
        res = client.list_queued_workflows()
        assert res is not None
    finally:
        client.destroy()


####################
# TRANSLATE DBOSConfig to ConfigFile
####################


def test_translate_dbosconfig_full_input():
    # Give all fields
    config: DBOSConfig = {
        "name": "test-app",
        "database_url": "postgres://user:password@localhost:5432/dbname?connect_timeout=11&sslmode=require&sslrootcert=ca.pem",
        "sys_db_name": "sysdb",
        "sys_db_pool_size": 27,
        "db_engine_kwargs": {"key": "value"},
        "log_level": "DEBUG",
        "otlp_traces_endpoints": ["http://otel:7777", "notused"],
        "admin_port": 8001,
        "run_admin_server": False,
    }

    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["name"] == "test-app"
    assert translated_config["database_url"] == config["database_url"]
    assert translated_config["database"]["sys_db_name"] == "sysdb"
    assert translated_config["database"]["sys_db_pool_size"] == 27
    assert translated_config["database"]["db_engine_kwargs"] == {"key": "value"}
    assert translated_config["telemetry"]["logs"]["logLevel"] == "DEBUG"
    assert translated_config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "http://otel:7777",
        "notused",
    ]
    assert translated_config["telemetry"]["OTLPExporter"]["logsEndpoint"] == []
    assert translated_config["runtimeConfig"]["admin_port"] == 8001
    assert translated_config["runtimeConfig"]["run_admin_server"] == False
    assert "start" not in translated_config["runtimeConfig"]
    assert "setup" not in translated_config["runtimeConfig"]
    assert "env" not in translated_config


def test_translate_dbosconfig_minimal_input():
    config: DBOSConfig = {
        "name": "test-app",
    }
    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["name"] == "test-app"
    assert translated_config["telemetry"]["logs"]["logLevel"] == "INFO"
    assert translated_config["runtimeConfig"]["run_admin_server"] == True
    assert "admin_port" not in translated_config["runtimeConfig"]
    assert "database" not in translated_config
    assert "env" not in translated_config


def test_translate_dbosconfig_just_sys_db_name():
    config: DBOSConfig = {
        "name": "test-app",
        "sys_db_name": "sysdb",
    }
    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["database"]["sys_db_name"] == "sysdb"
    assert "sys_db_pool_size" not in translated_config["database"]
    assert "env" not in translated_config
    assert "admin_port" not in translated_config["runtimeConfig"]


def test_translate_dbosconfig_just_sys_db_pool_size():
    config: DBOSConfig = {
        "name": "test-app",
        "sys_db_pool_size": 27,
    }
    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["database"]["sys_db_pool_size"] == 27
    assert "sys_db_name" not in translated_config["database"]
    assert "env" not in translated_config


def test_translate_dbosconfig_just_db_engine_kwargs():
    config: DBOSConfig = {
        "name": "test-app",
        "db_engine_kwargs": {"key": "value"},
    }
    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["database"]["db_engine_kwargs"] == {"key": "value"}
    assert "sys_db_pool_size" not in translated_config["database"]
    assert "sys_db_name" not in translated_config["database"]
    assert "env" not in translated_config
    assert "admin_port" not in translated_config["runtimeConfig"]


def test_translate_dbosconfig_just_admin_port():
    config: DBOSConfig = {
        "name": "test-app",
        "admin_port": 8001,
    }
    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["runtimeConfig"]["admin_port"] == 8001
    assert translated_config["runtimeConfig"]["run_admin_server"] == True
    assert "env" not in translated_config
    assert "database" not in translated_config


def test_translate_dbosconfig_just_run_admin_server():
    config: DBOSConfig = {
        "name": "test-app",
        "run_admin_server": True,
    }
    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["name"] == "test-app"
    assert translated_config["telemetry"]["logs"]["logLevel"] == "INFO"
    assert translated_config["runtimeConfig"]["run_admin_server"] == True
    assert "admin_port" not in translated_config["runtimeConfig"]
    assert "env" not in translated_config
    assert "database" not in translated_config


def test_translate_empty_otlp_traces_endpoints():
    # Give an empty OTLP traces endpoint list
    config: DBOSConfig = {
        "name": "test-app",
        "otlp_traces_endpoints": [],
    }
    translated_config = translate_dbos_config_to_config_file(config)
    assert len(translated_config["telemetry"]["OTLPExporter"]["logsEndpoint"]) == 0
    assert len(translated_config["telemetry"]["OTLPExporter"]["tracesEndpoint"]) == 0
    assert translated_config["telemetry"]["logs"]["logLevel"] == "INFO"


def test_translate_ignores_otlp_traces_not_list():
    # Give an empty OTLP traces endpoint list
    config: DBOSConfig = {
        "name": "test-app",
        "otlp_traces_endpoints": "http://otel:7777",
    }
    translated_config = translate_dbos_config_to_config_file(config)
    assert translated_config["name"] == "test-app"
    assert len(translated_config["telemetry"]["OTLPExporter"]["logsEndpoint"]) == 0
    assert len(translated_config["telemetry"]["OTLPExporter"]["tracesEndpoint"]) == 0


def test_translate_missing_name():
    with pytest.raises(DBOSInitializationError) as exc_info:
        translate_dbos_config_to_config_file({})
    assert (
        "Error initializing DBOS Transact: Configuration must specify an application name"
        in str(exc_info.value)
    )


def test_translate_application_database_url():
    config: DBOSConfig = {
        "name": "test-app",
        "application_database_url": "postgres://user:password@localhost:5432/dbname?connect_timeout=11&sslmode=require&sslrootcert=ca.pem",
        "system_database_url": "postgres://user:password@localhost:5432/dbname_sys?connect_timeout=11&sslmode=require&sslrootcert=ca.pem",
    }
    translated_config = translate_dbos_config_to_config_file(config)
    assert translated_config["name"] == "test-app"
    assert translated_config["database_url"] == config["application_database_url"]
    assert translated_config["system_database_url"] == config["system_database_url"]


####################
# CONFIG OVERWRITE
####################


def test_overwrite_config(mocker):
    # Setup a typical dbos-config.yaml file
    mock_config = """
    name: "stock-prices"
    language: "python"
    database:
        sys_db_name: sysdbname
        migrate:
            - alembic upgrade head
    telemetry:
        logs:
            logLevel: INFO
        OTLPExporter:
            logsEndpoint: thelogsendpoint
            tracesEndpoint:  thetracesendpoint
    runtimeConfig:
        start:
            - "a start command"
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )

    provided_config: ConfigFile = {
        "name": "test-app",
        "database": {
            "sys_db_name": "sysdb",
        },
        "telemetry": {
            "OTLPExporter": {
                "tracesEndpoint": ["a"],
                "logsEndpoint": ["b"],
            },
            "logs": {
                "logLevel": "DEBUG",
            },
        },
        "runtimeConfig": {
            "admin_port": 8001,
            "run_admin_server": True,
        },
        "env": {
            "FOO": "BAR",
        },
    }
    exported_db_url = "postgres://dbosadmin:pwd@hostname:1234/appdbname?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"
    os.environ["DBOS_DATABASE_URL"] = exported_db_url
    exported_sys_db_url = "postgres://dbosadmin:pwd@hostname:1234/appdbname_dbos_sys?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"
    os.environ["DBOS_SYSTEM_DATABASE_URL"] = exported_sys_db_url

    config = overwrite_config(provided_config)

    assert config["name"] == "stock-prices"
    assert config["database_url"] == exported_db_url
    assert config["system_database_url"] == exported_sys_db_url
    assert "sys_db_pool_size" not in config["database"]
    assert config["telemetry"]["logs"]["logLevel"] == "DEBUG"
    assert config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "a",
        "thetracesendpoint",
    ]
    assert config["telemetry"]["OTLPExporter"]["logsEndpoint"] == [
        "b",
        "thelogsendpoint",
    ]
    assert "admin_port" not in config["runtimeConfig"]
    assert "run_admin_server" not in config["runtimeConfig"]
    assert "env" not in config

    del os.environ["DBOS_DATABASE_URL"]


def test_overwrite_config_minimal(mocker):
    mock_config = """
    name: "stock-prices"
    language: "python"
    database:
        sys_db_name: sysdbname
        migrate:
            - alembic upgrade head
    telemetry:
        OTLPExporter:
            logsEndpoint: thelogsendpoint
            tracesEndpoint:  thetracesendpoint
    runtimeConfig:
        start:
            - "a start command"
        setup:
            - "echo 'hello'"
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )

    provided_config: ConfigFile = {
        "name": "test-app",
    }

    exported_db_url = "postgres://dbosadmin:pwd@hostname:1234/appdbname?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"
    os.environ["DBOS_DATABASE_URL"] = exported_db_url
    exported_sys_db_url = "postgres://dbosadmin:pwd@hostname:1234/appdbname_dbos_sys?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"
    os.environ["DBOS_SYSTEM_DATABASE_URL"] = exported_sys_db_url

    config = overwrite_config(provided_config)

    assert config["name"] == "stock-prices"
    assert config["database_url"] == exported_db_url
    assert config["system_database_url"] == exported_sys_db_url
    assert config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "thetracesendpoint"
    ]
    assert config["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["thelogsendpoint"]
    assert "runtimeConfig" not in config
    assert "env" not in config

    del os.environ["DBOS_DATABASE_URL"]


def test_overwrite_config_has_telemetry(mocker):
    mock_config = """
    name: "stock-prices"
    language: "python"
    database:
        sys_db_name: sysdbname
        migrate:
            - alembic upgrade head
    telemetry:
        OTLPExporter:
            logsEndpoint: thelogsendpoint
            tracesEndpoint:  thetracesendpoint
    runtimeConfig:
        start:
            - "a start command"
        setup:
            - "echo 'hello'"
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )

    provided_config: ConfigFile = {
        "name": "test-app",
        "telemetry": {"logs": {"logLevel": "DEBUG"}},
    }

    exported_db_url = "postgres://dbosadmin:pwd@hostname:1234/appdbname?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"
    os.environ["DBOS_DATABASE_URL"] = exported_db_url
    exported_sys_db_url = "postgres://dbosadmin:pwd@hostname:1234/appdbname_dbos_sys?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"
    os.environ["DBOS_SYSTEM_DATABASE_URL"] = exported_sys_db_url

    config = overwrite_config(provided_config)

    assert config["name"] == "stock-prices"
    assert config["database_url"] == exported_db_url
    assert config["system_database_url"] == exported_sys_db_url
    assert config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "thetracesendpoint"
    ]
    assert config["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["thelogsendpoint"]
    assert config["telemetry"]["logs"]["logLevel"] == "DEBUG"
    assert "runtimeConfig" not in config
    assert "env" not in config

    del os.environ["DBOS_DATABASE_URL"]


# Not expected in practice, but exercise the code path
def test_overwrite_config_no_telemetry_in_file(mocker):
    mock_config = """
    name: "stock-prices"
    language: "python"
    database:
        sys_db_name: sysdbname
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )

    provided_config: ConfigFile = {
        "name": "test-app",
        "telemetry": {"logs": {"logLevel": "DEBUG"}},
    }

    exported_db_url = "postgres://dbosadmin:pwd@hostname:1234/appdbname?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"
    os.environ["DBOS_DATABASE_URL"] = exported_db_url
    exported_sys_db_url = "postgres://dbosadmin:pwd@hostname:1234/appdbname_dbos_sys?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"
    os.environ["DBOS_SYSTEM_DATABASE_URL"] = exported_sys_db_url

    config = overwrite_config(provided_config)
    # Test that telemetry from provided_config is preserved
    assert config["database_url"] == exported_db_url
    assert config["system_database_url"] == exported_sys_db_url
    assert config["telemetry"]["logs"]["logLevel"] == "DEBUG"
    assert config["telemetry"]["OTLPExporter"] == {
        "tracesEndpoint": [],
        "logsEndpoint": [],
    }

    del os.environ["DBOS_DATABASE_URL"]


# Not expected in practice, but exercise the code path
def test_overwrite_config_no_otlp_in_file(mocker):
    mock_config = """
    name: "stock-prices"
    language: "python"
    database:
        sys_db_name: sysdbname
    telemetry:
        logs:
            logLevel: INFO
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )

    provided_config: ConfigFile = {
        "name": "test-app",
        "telemetry": {
            "OTLPExporter": {
                "tracesEndpoint": ["original-trace"],
                "logsEndpoint": ["original-log"],
            }
        },
    }

    exported_db_url = "postgres://dbosadmin:pwd@hostname:1234/appdbname?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"
    os.environ["DBOS_DATABASE_URL"] = exported_db_url
    exported_sys_db_url = "postgres://dbosadmin:pwd@hostname:1234/appdbname_dbos_sys?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"
    os.environ["DBOS_SYSTEM_DATABASE_URL"] = exported_sys_db_url

    config = overwrite_config(provided_config)
    assert config["database_url"] == exported_db_url
    assert config["system_database_url"] == exported_sys_db_url
    # Test that OTLPExporter from provided_config is preserved
    assert config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == ["original-trace"]
    assert config["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["original-log"]
    assert "logs" not in config["telemetry"]

    del os.environ["DBOS_DATABASE_URL"]


def test_overwrite_config_with_provided_database_url(mocker):
    mock_config = """
    name: "stock-prices"
    language: "python"
    database:
        sys_db_name: sysdbname
        migrate:
            - alembic upgrade head
    telemetry:
        OTLPExporter:
            logsEndpoint: thelogsendpoint
            tracesEndpoint:  thetracesendpoint
    runtimeConfig:
        start:
            - "a start command"
        setup:
            - "echo 'hello'"
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )

    provided_config: ConfigFile = {
        "name": "test-app",
        "database_url": "ignored",
    }

    exported_db_url = "postgres://dbosadmin:pwd@hostname:1234/appdbname?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"
    os.environ["DBOS_DATABASE_URL"] = exported_db_url
    exported_sys_db_url = "postgres://dbosadmin:pwd@hostname:1234/appdbname_dbos_sys?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"
    os.environ["DBOS_SYSTEM_DATABASE_URL"] = exported_sys_db_url

    config = overwrite_config(provided_config)

    assert config["name"] == "stock-prices"
    assert config["database_url"] == exported_db_url
    assert config["system_database_url"] == exported_sys_db_url
    assert config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "thetracesendpoint"
    ]
    assert config["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["thelogsendpoint"]
    assert "runtimeConfig" not in config
    assert "env" not in config

    del os.environ["DBOS_DATABASE_URL"]


def test_overwrite_config_missing_dbos_database_url(mocker):
    mock_config = """
    name: "stock-prices"
    database:
        sys_db_name: "sysdbname"
    """

    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )

    provided_config: ConfigFile = {
        "name": "test-app",
    }
    with pytest.raises(DBOSInitializationError) as exc_info:
        overwrite_config(provided_config)
    assert (
        "DBOS_DATABASE_URL environment variable is not set. This is required to connect to the database."
        in str(exc_info.value)
    )


####################
# DATABASES CONNECTION POOLS
####################


def test_configured_pool_default():
    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "database_url": f"postgres://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'))}@localhost:5432/postgres",
    }

    dbos = DBOS(config=config)
    dbos.launch()
    assert dbos._app_db.engine.pool._pool.maxsize == 20
    assert dbos._app_db.engine.pool._timeout == 30
    assert dbos._app_db.engine.pool._max_overflow == 0
    assert dbos._app_db.engine.pool._pre_ping == True

    assert dbos._sys_db.engine.pool._pool.maxsize == 20
    assert dbos._sys_db.engine.pool._timeout == 30
    assert dbos._sys_db.engine.pool._max_overflow == 0
    assert dbos._sys_db.engine.pool._pre_ping == True

    # force the release of connections so we can intercept on connect.
    app_db_engine = dbos._app_db.engine
    app_db_engine.dispose()

    @event.listens_for(app_db_engine, "connect")
    def inspect_connection(dbapi_connection, connection_record):
        connect_timeout = dbapi_connection.info.get_parameters()["connect_timeout"]
        assert connect_timeout == "10"

    with app_db_engine.connect() as conn:
        pass

    dbos.destroy()


def test_configured_pool_custom_url():
    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "database_url": f"postgres://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'))}@localhost:5432/postgres",
        "system_database_url": f"postgres://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'))}@localhost:5432/dbostesturl",
    }

    dbos = DBOS(config=config)
    dbos.launch()
    assert "postgres" in dbos._app_db.engine.url
    assert "dbostesturl" in dbos._sys_db.engine.url

    dbos.destroy()


def test_configured_pool_user_provided():
    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "sys_db_pool_size": 43,
        "database_url": f"postgres://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'))}@localhost:5432/postgres",
        "db_engine_kwargs": {
            "pool_size": 22,
            "pool_timeout": 42,
            "max_overflow": 27,
            "pool_pre_ping": True,
            "connect_args": {"connect_timeout": 7},
        },
    }

    dbos = DBOS(config=config)
    dbos.launch()
    assert dbos._app_db.engine.pool._pool.maxsize == 22
    assert dbos._app_db.engine.pool._timeout == 42
    assert dbos._app_db.engine.pool._max_overflow == 27
    assert dbos._app_db.engine.pool._pre_ping == True

    assert dbos._sys_db.engine.pool._pool.maxsize == 43
    assert dbos._sys_db.engine.pool._timeout == 42
    assert dbos._sys_db.engine.pool._max_overflow == 27
    assert dbos._sys_db.engine.pool._pre_ping == True

    # force the release of connections so we can intercept on connect.
    app_db_engine = dbos._app_db.engine
    app_db_engine.dispose()

    @event.listens_for(app_db_engine, "connect")
    def inspect_connection(dbapi_connection, connection_record):
        connect_timeout = dbapi_connection.info.get_parameters()["connect_timeout"]
        assert connect_timeout == "7"

    with app_db_engine.connect() as conn:
        pass

    dbos.destroy()


def test_configured_pool_user_provided_dburl_connect_timeout():
    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "sys_db_pool_size": 43,
        "database_url": f"postgres://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'))}@localhost:5432/postgres?connect_timeout=22",
        "db_engine_kwargs": {
            "pool_size": 22,
            "pool_timeout": 42,
            "max_overflow": 27,
            "pool_pre_ping": True,
        },
    }

    dbos = DBOS(config=config)
    dbos.launch()
    assert dbos._app_db.engine.pool._pool.maxsize == 22
    assert dbos._app_db.engine.pool._timeout == 42
    assert dbos._app_db.engine.pool._max_overflow == 27
    assert dbos._app_db.engine.pool._pre_ping == True

    assert dbos._sys_db.engine.pool._pool.maxsize == 43
    assert dbos._sys_db.engine.pool._timeout == 42
    assert dbos._sys_db.engine.pool._max_overflow == 27
    assert dbos._sys_db.engine.pool._pre_ping == True

    # force the release of connections so we can intercept on connect.
    app_db_engine = dbos._app_db.engine
    app_db_engine.dispose()

    @event.listens_for(app_db_engine, "connect")
    def inspect_connection(dbapi_connection, connection_record):
        connect_timeout = dbapi_connection.info.get_parameters()["connect_timeout"]
        assert connect_timeout == "22"

    with app_db_engine.connect() as conn:
        pass

    dbos.destroy()


def test_configured_pool_user_provided_dburl_connect_timeout_precedence():
    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "sys_db_pool_size": 43,
        "database_url": f"postgres://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'))}@localhost:5432/postgres?connect_timeout=22",  # connect_args will take precedence
        "db_engine_kwargs": {
            "pool_size": 22,
            "pool_timeout": 42,
            "max_overflow": 27,
            "pool_pre_ping": True,
            "connect_args": {"connect_timeout": 7},
        },
    }

    dbos = DBOS(config=config)
    dbos.launch()
    assert dbos._app_db.engine.pool._pool.maxsize == 22
    assert dbos._app_db.engine.pool._timeout == 42
    assert dbos._app_db.engine.pool._max_overflow == 27
    assert dbos._app_db.engine.pool._pre_ping == True

    assert dbos._sys_db.engine.pool._pool.maxsize == 43
    assert dbos._sys_db.engine.pool._timeout == 42
    assert dbos._sys_db.engine.pool._max_overflow == 27
    assert dbos._sys_db.engine.pool._pre_ping == True

    # force the release of connections so we can intercept on connect.
    app_db_engine = dbos._app_db.engine
    app_db_engine.dispose()

    @event.listens_for(app_db_engine, "connect")
    def inspect_connection(dbapi_connection, connection_record):
        connect_timeout = dbapi_connection.info.get_parameters()["connect_timeout"]
        assert connect_timeout == "7"

    with app_db_engine.connect() as conn:
        pass

    dbos.destroy()


def test_pool_connection_times_out_by_default():
    import socket

    ipv4_addr = socket.gethostbyname("example.com")

    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "database_url": f"postgres://postgres:dbos@{ipv4_addr}/postgres",
    }

    dbos = DBOS(config=config)
    with pytest.raises(OperationalError) as exc_info:
        dbos.launch()

    assert "timeout" in str(exc_info.value).lower()
    dbos.destroy()
