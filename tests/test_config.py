# type: ignore

import os
from unittest.mock import mock_open
from urllib.parse import quote

import pytest
import pytest_mock
from sqlalchemy import URL, event

# Public API
from dbos import DBOS, get_dbos_database_url, load_config
from dbos._dbos_config import (
    ConfigFile,
    DBOSConfig,
    check_config_consistency,
    overwrite_config,
    parse_database_url_to_dbconfig,
    process_config,
    set_env_vars,
    translate_dbos_config_to_config_file,
)
from dbos._error import DBOSInitializationError

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
# SWITCHES
####################
def test_no_config_provided(mocker):
    mock_config = """
        name: "some-app"
        language: "python"
        runtimeConfig:
            start:
                - "python3 main.py"
            admin_port: 8001
        database:
          hostname: 'localhost'
          port: 5432
          username: 'postgres'
          password: ${PGPASSWORD}
          app_db_name: 'some db'
          connectionTimeoutMillis: 3000
        env:
            foo: ${BARBAR}
            bazbaz: BAZBAZ
            bob: ${BOBBOB}
            test_number: 123
    """
    os.environ["BARBAR"] = "FOOFOO"
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    dbos = DBOS()
    assert dbos.config["name"] == "some-app"
    assert dbos.config["language"] == "python"
    assert dbos.config["database"]["hostname"] == "localhost"
    assert dbos.config["database"]["port"] == 5432
    assert dbos.config["database"]["username"] == "postgres"
    assert dbos.config["database"]["password"] == os.environ["PGPASSWORD"]
    assert dbos.config["database"]["app_db_name"] == "some db"
    assert dbos.config["database"]["connectionTimeoutMillis"] == 3000
    assert dbos.config["env"]["foo"] == "FOOFOO"
    assert dbos.config["env"]["bob"] is None  # Unset environment variable
    assert dbos.config["env"]["test_number"] == 123

    set_env_vars(dbos.config)
    assert os.environ["bazbaz"] == "BAZBAZ"
    assert os.environ["foo"] == "FOOFOO"
    assert os.environ["test_number"] == "123"
    assert "bob" not in os.environ

    dbos.destroy()


def test_configfile_type_provided():
    config: ConfigFile = {
        "name": "some-app",
        "database": {
            "hostname": "localhost",
        },
    }
    dbos = DBOS(config=config)
    assert dbos.config["name"] == "some-app"
    assert dbos.config["database"]["hostname"] == "localhost"
    assert dbos.config["database"]["port"] == 5432
    assert dbos.config["database"]["username"] == "postgres"
    assert dbos.config["database"]["password"] == os.environ["PGPASSWORD"]
    assert dbos.config["database"]["app_db_name"] == "some_app"
    dbos.destroy()


def test_dbosconfig_type_provided(mocker):
    config: DBOSConfig = {
        "name": "some-app",
    }
    dbos = DBOS(config=config)
    assert dbos.config["name"] == "some-app"
    assert dbos.config["database"]["hostname"] == "localhost"
    assert dbos.config["database"]["port"] == 5432
    assert dbos.config["database"]["username"] == "postgres"
    assert dbos.config["database"]["password"] == os.environ["PGPASSWORD"]
    assert dbos.config["database"]["app_db_name"] == "some_app"
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
        database:
          hostname: 'localhost'
          port: 5432
          username: 'postgres'
          password: ${PGPASSWORD}
          app_db_name: 'some db'
          connectionTimeoutMillis: 3000
        env:
            foo: ${BARBAR}
            bazbaz: BAZBAZ
            bob: ${BOBBOB}
            test_number: 123
        telemetry:
            OTLPExporter:
                logsEndpoint: 'fooLogs'
                tracesEndpoint: 'fooTraces'
    """
    os.environ["BARBAR"] = "FOOFOO"
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename, run_process_config=False)
    assert configFile["name"] == "some-app"
    assert configFile["database"]["hostname"] == "localhost"
    assert configFile["database"]["port"] == 5432
    assert configFile["database"]["username"] == "postgres"
    assert configFile["database"]["password"] == os.environ["PGPASSWORD"]
    assert configFile["database"]["app_db_name"] == "some db"
    assert configFile["database"]["connectionTimeoutMillis"] == 3000
    assert "database_url" not in configFile
    assert configFile["env"]["foo"] == "FOOFOO"
    assert configFile["env"]["bob"] is None  # Unset environment variable
    assert configFile["env"]["test_number"] == 123

    set_env_vars(configFile)
    assert os.environ["bazbaz"] == "BAZBAZ"
    assert os.environ["foo"] == "FOOFOO"
    assert os.environ["test_number"] == "123"
    assert "bob" not in os.environ

    assert configFile["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["fooLogs"]
    assert configFile["telemetry"]["OTLPExporter"]["tracesEndpoint"] == ["fooTraces"]


def test_load_config_database_url(mocker):
    mock_config = """
        name: "some-app"
        database_url: "postgresql://user:password@localhost:5432/dbname?connect_timeout=10&sslmode=require&sslrootcert=ca.pem"
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename, run_process_config=False)
    assert configFile["name"] == "some-app"
    assert (
        configFile["database_url"]
        == "postgresql://user:password@localhost:5432/dbname?connect_timeout=10&sslmode=require&sslrootcert=ca.pem"
    )
    assert "database" not in configFile


def test_load_config_database_url_and_database(mocker):
    mock_config = """
        name: "some-app"
        database_url: "postgresql://user:password@localhost:5432/dbname?connect_timeout=10&sslmode=require&sslrootcert=ca.pem"
        database:
            hostname: 'localhost'
            port: 5432
            username: 'postgres'
            password: ${PGPASSWORD}
            app_db_name: 'some db'
            connectionTimeoutMillis: 3000
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename, run_process_config=False)
    assert configFile["name"] == "some-app"
    assert (
        configFile["database_url"]
        == "postgresql://user:password@localhost:5432/dbname?connect_timeout=10&sslmode=require&sslrootcert=ca.pem"
    )
    assert configFile["database"]["hostname"] == "localhost"
    assert configFile["database"]["port"] == 5432
    assert configFile["database"]["username"] == "postgres"
    assert configFile["database"]["password"] == os.environ["PGPASSWORD"]
    assert configFile["database"]["app_db_name"] == "some db"
    assert configFile["database"]["connectionTimeoutMillis"] == 3000


def test_load_config_with_unset_database_url_env_var(mocker):
    mock_config = """
    name: "some-app"
    database_url: ${UNSET}
    """

    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename, run_process_config=False)
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
    database:
        hostname: "localhost"
    """
    custom_path = "/custom/path/dbos-config.yaml"
    from unittest.mock import mock_open, patch

    with patch("builtins.open", mock_open(read_data=mock_config)) as mock_file:
        result = load_config(custom_path, run_process_config=False)
        mock_file.assert_called_with(custom_path, "r")
        assert result["name"] == "test-app"


####################
# PROCESS CONFIG
####################


# Full config provided
def test_process_config_full():
    config: ConfigFile = {
        "name": "some-app",
        "database": {
            "hostname": "example.com",
            "port": 2345,
            "username": "example",
            "password": "password",
            "connectionTimeoutMillis": 3000,
            "app_db_name": "example_db",
            "app_db_pool_size": 45,
            "sys_db_name": "sys_db",
            "sys_db_pool_size": 27,
            "ssl": True,
            "ssl_ca": "ca.pem",
            "local_suffix": False,
            "migrate": ["alembic upgrade head"],
            "rollback": ["alembic downgrade base"],
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

    configFile = process_config(data=config, use_db_wizard=False)
    assert configFile["name"] == "some-app"
    assert configFile["database"]["hostname"] == "example.com"
    assert configFile["database"]["port"] == 2345
    assert configFile["database"]["username"] == "example"
    assert configFile["database"]["password"] == "password"
    assert configFile["database"]["connectionTimeoutMillis"] == 3000
    assert configFile["database"]["app_db_name"] == "example_db"
    assert configFile["database"]["sys_db_name"] == "sys_db"
    assert configFile["database"]["ssl"] == True
    assert configFile["database"]["ssl_ca"] == "ca.pem"
    assert configFile["database"]["local_suffix"] == False
    assert configFile["database"]["migrate"] == ["alembic upgrade head"]
    assert configFile["database"]["rollback"] == ["alembic downgrade base"]
    assert configFile["database"]["app_db_pool_size"] == 45
    assert configFile["database"]["sys_db_pool_size"] == 27
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


def test_process_config_with_db_url():
    config: ConfigFile = {
        "name": "some-app",
        "database_url": "postgresql://user:password@localhost:7777/dbn?connect_timeout=1&sslmode=require&sslrootcert=ca.pem",
    }
    processed_config = process_config(data=config, use_db_wizard=False)
    assert processed_config["name"] == "some-app"
    assert processed_config["database"]["hostname"] == "localhost"
    assert processed_config["database"]["port"] == 7777
    assert processed_config["database"]["username"] == "user"
    assert processed_config["database"]["password"] == "password"
    assert processed_config["database"]["app_db_name"] == "dbn"
    assert processed_config["database"]["connectionTimeoutMillis"] == 1000
    assert processed_config["database"]["ssl"] == True
    assert processed_config["database"]["ssl_ca"] == "ca.pem"
    assert processed_config["database"]["local_suffix"] == False
    assert processed_config["database"]["app_db_pool_size"] == 20
    assert processed_config["database"]["sys_db_pool_size"] == 20
    assert "rollback" not in processed_config["database"]
    assert "migrate" not in processed_config["database"]
    assert processed_config["runtimeConfig"]["run_admin_server"] == True


def test_process_config_with_db_url_taking_precedence_over_database():
    config: ConfigFile = {
        "name": "some-app",
        "database": {
            "hostname": "example.com",
            "port": 2345,
            "username": "example",
            "password": "password",
            "connectionTimeoutMillis": 3000,
            "app_db_name": "example_db",
            "sys_db_name": "sys_db",
            "ssl": True,
            "ssl_ca": "ca.pem",
            "local_suffix": True,
            "migrate": ["alembic upgrade head"],
            "rollback": ["alembic downgrade base"],
        },
        "database_url": "postgresql://boo:whoisdiz@remotehost:7777/takesprecedence",
    }
    processed_config = process_config(data=config, use_db_wizard=False)
    assert processed_config["name"] == "some-app"
    assert processed_config["database"]["hostname"] == "remotehost"
    assert processed_config["database"]["port"] == 7777
    assert processed_config["database"]["username"] == "boo"
    assert processed_config["database"]["password"] == "whoisdiz"
    assert processed_config["database"]["app_db_name"] == "takesprecedence_local"
    assert processed_config["database"]["migrate"] == ["alembic upgrade head"]
    assert processed_config["database"]["rollback"] == ["alembic downgrade base"]
    assert processed_config["database"]["local_suffix"] == True
    assert processed_config["database"]["app_db_pool_size"] == 20
    assert processed_config["database"]["sys_db_pool_size"] == 20
    assert processed_config["database"]["connectionTimeoutMillis"] == 10000
    assert "ssl" not in processed_config["database"]
    assert "ssl_ca" not in processed_config["database"]
    assert processed_config["runtimeConfig"]["run_admin_server"] == True


# Note this exercise going through the db wizard
def test_process_config_load_defaults():
    config: ConfigFile = {
        "name": "some-app",
    }
    processed_config = process_config(data=config)
    assert processed_config["name"] == "some-app"
    assert processed_config["database"]["app_db_name"] == "some_app"
    assert processed_config["database"]["hostname"] == "localhost"
    assert processed_config["database"]["port"] == 5432
    assert processed_config["database"]["username"] == "postgres"
    assert processed_config["database"]["password"] == os.environ.get(
        "PGPASSWORD", "dbos"
    )
    assert processed_config["database"]["connectionTimeoutMillis"] == 10000
    assert processed_config["database"]["app_db_pool_size"] == 20
    assert processed_config["database"]["sys_db_pool_size"] == 20
    assert processed_config["telemetry"]["logs"]["logLevel"] == "INFO"
    assert processed_config["runtimeConfig"]["run_admin_server"] == True


def test_process_config_load_default_with_None_database_url():
    config: ConfigFile = {
        "name": "some-app",
        "database_url": None,
    }
    processed_config = process_config(data=config)
    assert processed_config["name"] == "some-app"
    assert processed_config["database"]["app_db_name"] == "some_app"
    assert processed_config["database"]["hostname"] == "localhost"
    assert processed_config["database"]["port"] == 5432
    assert processed_config["database"]["username"] == "postgres"
    assert processed_config["database"]["password"] == os.environ.get(
        "PGPASSWORD", "dbos"
    )
    assert processed_config["database"]["connectionTimeoutMillis"] == 10000
    assert processed_config["database"]["app_db_pool_size"] == 20
    assert processed_config["database"]["sys_db_pool_size"] == 20


def test_process_config_with_None_app_db_name():
    config: ConfigFile = {
        "name": "some-app",
        "database": {
            "app_db_name": None,
        },
    }
    processed_config = process_config(data=config)
    print(processed_config)
    assert processed_config["name"] == "some-app"
    assert processed_config["database"]["app_db_name"] == "some_app"
    assert processed_config["database"]["hostname"] == "localhost"
    assert processed_config["database"]["port"] == 5432
    assert processed_config["database"]["username"] == "postgres"
    assert processed_config["database"]["password"] == os.environ.get(
        "PGPASSWORD", "dbos"
    )
    assert processed_config["database"]["connectionTimeoutMillis"] == 10000
    assert processed_config["database"]["app_db_pool_size"] == 20
    assert processed_config["database"]["sys_db_pool_size"] == 20


def test_process_config_with_empty_app_db_name():
    config: ConfigFile = {
        "name": "some-app",
        "database": {
            "app_db_name": "",
        },
    }
    processed_config = process_config(data=config)
    print(processed_config)
    assert processed_config["name"] == "some-app"
    assert processed_config["database"]["app_db_name"] == "some_app"
    assert processed_config["database"]["hostname"] == "localhost"
    assert processed_config["database"]["port"] == 5432
    assert processed_config["database"]["username"] == "postgres"
    assert processed_config["database"]["password"] == os.environ.get(
        "PGPASSWORD", "dbos"
    )
    assert processed_config["database"]["connectionTimeoutMillis"] == 10000
    assert processed_config["database"]["app_db_pool_size"] == 20
    assert processed_config["database"]["sys_db_pool_size"] == 20


def test_config_missing_name():
    config = {
        "database": {
            "hostname": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "dbos",
            "app_db_name": "dbostestpy",
        },
    }
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


def test_load_config_load_db_connection(mocker):
    mock_db_connection = """
    {"hostname": "example.com", "port": 2345, "username": "example", "password": "password", "local_suffix": true}
    """
    mocker.patch(
        "builtins.open",
        side_effect=generate_mock_open([".dbos/db_connection"], [mock_db_connection]),
    )

    config = {
        "name": "some-app",
    }

    configFile = process_config(data=config, use_db_wizard=False)
    assert configFile["name"] == "some-app"
    assert configFile["database"]["hostname"] == "example.com"
    assert configFile["database"]["port"] == 2345
    assert configFile["database"]["username"] == "example"
    assert configFile["database"]["password"] == "password"
    assert configFile["database"]["local_suffix"] == True
    assert configFile["database"]["app_db_name"] == "some_app_local"
    assert configFile["database"]["app_db_pool_size"] == 20
    assert configFile["database"]["sys_db_pool_size"] == 20
    assert configFile["database"]["connectionTimeoutMillis"] == 10000


def test_config_mixed_params():
    config = {
        "name": "some-app",
        "database": {
            "port": 1234,
            "username": "some user",
            "password": "abc123",
            "app_db_pool_size": 3,
        },
    }

    configFile = process_config(data=config, use_db_wizard=False)
    assert configFile["name"] == "some-app"
    assert configFile["database"]["hostname"] == "localhost"
    assert configFile["database"]["port"] == 1234
    assert configFile["database"]["username"] == "some user"
    assert configFile["database"]["password"] == "abc123"
    assert configFile["database"]["app_db_pool_size"] == 3
    assert configFile["database"]["sys_db_pool_size"] == 20
    assert configFile["database"]["connectionTimeoutMillis"] == 10000


def test_debug_override(mocker: pytest_mock.MockFixture):
    mocker.patch.dict(
        os.environ,
        {
            "DBOS_DBHOST": "fakehost",
            "DBOS_DBPORT": "1234",
            "DBOS_DBUSER": "fakeuser",
            "DBOS_DBPASSWORD": "fakepassword",
            "DBOS_DBLOCALSUFFIX": "false",
        },
    )

    config: Configfile = {
        "name": "some-app",
    }
    configFile = process_config(data=config, use_db_wizard=False)
    assert configFile["database"]["hostname"] == "fakehost"
    assert configFile["database"]["port"] == 1234
    assert configFile["database"]["username"] == "fakeuser"
    assert configFile["database"]["password"] == "fakepassword"
    assert configFile["database"]["local_suffix"] == False
    assert configFile["database"]["app_db_pool_size"] == 20
    assert configFile["database"]["sys_db_pool_size"] == 20
    assert configFile["database"]["connectionTimeoutMillis"] == 10000


def test_local_config():
    config: ConfigFile = {
        "name": "some-app",
        "database": {
            "hostname": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": os.environ["PGPASSWORD"],
            "app_db_name": "some_db",
            "connectionTimeoutMillis": 3000,
            "local_suffix": True,
        },
    }
    processed_config = process_config(data=config)

    assert processed_config["name"] == "some-app"
    assert processed_config["database"]["local_suffix"] == True
    assert processed_config["database"]["hostname"] == "localhost"
    assert processed_config["database"]["port"] == 5432
    assert processed_config["database"]["username"] == "postgres"
    assert processed_config["database"]["password"] == os.environ["PGPASSWORD"]
    assert processed_config["database"]["app_db_name"] == "some_db_local"
    assert processed_config["database"]["connectionTimeoutMillis"] == 3000
    assert processed_config["database"]["app_db_pool_size"] == 20
    assert processed_config["database"]["sys_db_pool_size"] == 20


def test_local_config_without_name(mocker):
    config: ConfigFile = {
        "name": "some-app",
        "database": {
            "hostname": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": os.environ["PGPASSWORD"],
            "connectionTimeoutMillis": 3000,
            "local_suffix": True,
        },
    }
    processed_config = process_config(data=config)

    assert processed_config["name"] == "some-app"
    assert processed_config["database"]["local_suffix"] == True
    assert processed_config["database"]["hostname"] == "localhost"
    assert processed_config["database"]["port"] == 5432
    assert processed_config["database"]["username"] == "postgres"
    assert processed_config["database"]["password"] == os.environ["PGPASSWORD"]
    assert processed_config["database"]["app_db_name"] == "some_app_local"
    assert processed_config["database"]["connectionTimeoutMillis"] == 3000
    assert processed_config["database"]["app_db_pool_size"] == 20
    assert processed_config["database"]["sys_db_pool_size"] == 20


####################
# DB STRING PARSING
####################


def test_basic_fields_mapping():
    """Test that basic fields from db_url are correctly mapped to db_config."""
    database_url = "postgresql://user:password@localhost:5432/dbname"
    db_config = parse_database_url_to_dbconfig(database_url)

    assert db_config["hostname"] == "localhost"
    assert db_config["port"] == 5432
    assert db_config["username"] == "user"
    assert db_config["password"] == "password"
    assert db_config["app_db_name"] == "dbname"


def test_no_db_name():
    """None app_db_name when dbname is not provided."""
    database_url = "postgresql://user:password@localhost:5432"
    db_config = parse_database_url_to_dbconfig(database_url)
    assert db_config["hostname"] == "localhost"
    assert db_config["port"] == 5432
    assert db_config["username"] == "user"
    assert db_config["password"] == "password"
    assert db_config["app_db_name"] == None


def test_no_db_name_end_with_slash():
    """Test that an exception is raised when dbname is not provided."""
    database_url = "postgresql://user:password@localhost:5432/"
    db_config = parse_database_url_to_dbconfig(database_url)
    assert db_config["hostname"] == "localhost"
    assert db_config["port"] == 5432
    assert db_config["username"] == "user"
    assert db_config["password"] == "password"
    assert db_config["app_db_name"] == ""


def test_default_port():
    """Test that default port (5432) is used when port is not specified."""
    database_url = "postgresql://user:password@localhost/dbname"
    db_config = parse_database_url_to_dbconfig(database_url)
    assert db_config["port"] == 5432


def test_query_parameters():
    """Test processing of various query parameters."""

    # Test connect_timeout conversion
    database_url = "postgresql://user:password@localhost:5432/dbname?connect_timeout=7"
    db_config = parse_database_url_to_dbconfig(database_url)
    assert db_config["connectionTimeoutMillis"] == 7000

    # Test sslmode=require (should set ssl=True)
    database_url = "postgresql://user:password@localhost:5432/dbname?sslmode=require"
    db_config = parse_database_url_to_dbconfig(database_url)
    assert db_config["ssl"] == True

    # Test sslmode=disable (should set ssl=False)
    database_url = "postgresql://user:password@localhost:5432/dbname?sslmode=disable"
    db_config = parse_database_url_to_dbconfig(database_url)
    assert db_config["ssl"] == False

    # Test sslmode=prefer (should set ssl=False as it's not 'require')
    database_url = "postgresql://user:password@localhost:5432/dbname?sslmode=prefer"
    db_config = parse_database_url_to_dbconfig(database_url)
    assert db_config["ssl"] == False

    # Test sslrootcert mapping to ssl_ca
    database_url = "postgresql://user:password@localhost:5432/dbname?sslrootcert=ca.pem"
    db_config = parse_database_url_to_dbconfig(database_url)
    assert db_config["ssl_ca"] == "ca.pem"

    # Test multiple parameters together
    database_url = "postgresql://user:password@localhost:5432/dbname?connect_timeout=8&sslmode=require&sslrootcert=ca.pem&application_name=myapp"
    db_config = parse_database_url_to_dbconfig(database_url)
    assert db_config["connectionTimeoutMillis"] == 8000
    assert db_config["ssl"] == True
    assert db_config["ssl_ca"] == "ca.pem"


def test_complex_password():
    """Test handling of complex passwords with special characters."""
    database_url = "postgresql://user:complex%23password@localhost:5432/dbname"
    db_config = parse_database_url_to_dbconfig(database_url)
    assert db_config["password"] == "complex#password"


def test_hostname_with_dots():
    """Test handling of hostnames with dots."""
    database_url = "postgresql://user:password@hostname.with.dots:5432/dbname"
    db_config = parse_database_url_to_dbconfig(database_url)
    assert db_config["hostname"] == "hostname.with.dots"


def test_invalid_string():
    """Test handling of invalid db strings."""
    with pytest.raises(Exception):
        database_url = "invalid"
        parse_database_url_to_dbconfig(database_url)


####################
# TRANSLATE DBOSConfig to ConfigFile
####################


def test_translate_dbosconfig_full_input():
    # Give all fields
    config: DBOSConfig = {
        "name": "test-app",
        "database_url": "postgresql://user:password@localhost:5432/dbname?connect_timeout=10&sslmode=require&sslrootcert=ca.pem",
        "app_db_pool_size": 45,
        "sys_db_name": "sysdb",
        "sys_db_pool_size": 27,
        "log_level": "DEBUG",
        "otlp_traces_endpoints": ["http://otel:7777", "notused"],
        "admin_port": 8001,
        "run_admin_server": False,
    }

    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["name"] == "test-app"
    assert translated_config["database"]["hostname"] == "localhost"
    assert translated_config["database"]["port"] == 5432
    assert translated_config["database"]["username"] == "user"
    assert translated_config["database"]["password"] == "password"
    assert translated_config["database"]["ssl"] == True
    assert translated_config["database"]["ssl_ca"] == "ca.pem"
    assert translated_config["database"]["connectionTimeoutMillis"] == 10000
    assert translated_config["database"]["app_db_name"] == "dbname"
    assert translated_config["database"]["sys_db_name"] == "sysdb"
    assert translated_config["database"]["app_db_pool_size"] == 45
    assert translated_config["database"]["sys_db_pool_size"] == 27
    assert "database_url" not in translated_config
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
    assert "app_db_pool_size" not in translated_config["database"]
    assert "sys_db_pool_size" not in translated_config["database"]
    assert "env" not in translated_config
    assert "admin_port" not in translated_config["runtimeConfig"]


def test_translate_dbosconfig_just_app_db_pool_size():
    config: DBOSConfig = {
        "name": "test-app",
        "app_db_pool_size": 45,
    }
    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["database"]["app_db_pool_size"] == 45
    assert "sys_db_name" not in translated_config["database"]
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
    assert "app_db_pool_size" not in translated_config["database"]
    assert "sys_db_name" not in translated_config["database"]
    assert "env" not in translated_config


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


####################
# CONFIG OVERWRITE
####################


def test_overwrite_config(mocker):
    # Setup a typical dbos-config.yaml file
    mock_config = """
    name: "stock-prices"
    language: "python"
    database:
        hostname: "hostname"
        port: 1234
        username: dbosadmin
        password: pwd
        app_db_name: appdbname
        sys_db_name: sysdbname
        ssl: true
        ssl_ca: cert.pem
        migrate:
            - alembic upgrade head
    telemetry:
        logs:
            logLevel: INFO
        OTLPExporter:
            logsEndpoint: thelogsendpoint
            tracesEndpoint:  thetracesendpoint
    env:
        KEY: "VALUE"
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
            "hostname": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "dbos",
            "app_db_name": "dbostestpy",
            "sys_db_name": "sysdb",
            "connectionTimeoutMillis": 10000,
            "app_db_pool_size": 10,
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
    config = overwrite_config(provided_config)

    assert config["name"] == "stock-prices"
    assert config["database"]["hostname"] == "hostname"
    assert config["database"]["port"] == 1234
    assert config["database"]["username"] == "dbosadmin"
    assert config["database"]["password"] == "pwd"
    assert config["database"]["app_db_name"] == "appdbname"
    assert config["database"]["sys_db_name"] == "sysdbname"
    assert config["database"]["ssl"] == True
    assert config["database"]["ssl_ca"] == "cert.pem"
    assert config["database"]["connectionTimeoutMillis"] == 10000
    assert config["database"]["app_db_pool_size"] == 10
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


def test_overwrite_config_minimal(mocker):
    mock_config = """
    name: "stock-prices"
    language: "python"
    database:
        hostname: "hostname"
        port: 1234
        username: dbosadmin
        password: pwd
        app_db_name: appdbname
        sys_db_name: sysdbname
        ssl: true
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
    config = overwrite_config(provided_config)

    assert config["name"] == "stock-prices"
    assert config["database"]["hostname"] == "hostname"
    assert config["database"]["port"] == 1234
    assert config["database"]["username"] == "dbosadmin"
    assert config["database"]["password"] == "pwd"
    assert config["database"]["app_db_name"] == "appdbname"
    assert config["database"]["sys_db_name"] == "sysdbname"
    assert config["database"]["ssl"] == True
    assert "ssl_ca" not in config["database"]
    assert config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "thetracesendpoint"
    ]
    assert config["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["thelogsendpoint"]
    assert "runtimeConfig" not in config
    assert "env" not in config


def test_overwrite_config_has_telemetry(mocker):
    mock_config = """
    name: "stock-prices"
    language: "python"
    database:
        hostname: "hostname"
        port: 1234
        username: dbosadmin
        password: pwd
        app_db_name: appdbname
        sys_db_name: sysdbname
        ssl: true
        ssl_ca: cert.pem
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
        "database": {
            "hostname": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "dbos",
            "app_db_name": "dbostestpy",
        },
        "telemetry": {"logs": {"logLevel": "DEBUG"}},
    }
    config = overwrite_config(provided_config)

    assert config["name"] == "stock-prices"
    assert config["database"]["hostname"] == "hostname"
    assert config["database"]["port"] == 1234
    assert config["database"]["username"] == "dbosadmin"
    assert config["database"]["password"] == "pwd"
    assert config["database"]["app_db_name"] == "appdbname"
    assert config["database"]["sys_db_name"] == "sysdbname"
    assert config["database"]["ssl"] == True
    assert config["database"]["ssl_ca"] == "cert.pem"
    assert config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "thetracesendpoint"
    ]
    assert config["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["thelogsendpoint"]
    assert config["telemetry"]["logs"]["logLevel"] == "DEBUG"
    assert "runtimeConfig" not in config
    assert "env" not in config


# Not expected in practice, but exercise the code path
def test_overwrite_config_no_telemetry_in_file(mocker):
    mock_config = """
    name: "stock-prices"
    language: "python"
    database:
        hostname: "hostname"
        port: 1234
        username: dbosadmin
        password: pwd
        app_db_name: appdbname
        sys_db_name: sysdbname
        ssl: true
        ssl_ca: cert.pem
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )

    provided_config: ConfigFile = {
        "name": "test-app",
        "database": {
            "hostname": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "dbos",
            "app_db_name": "dbostestpy",
        },
        "telemetry": {"logs": {"logLevel": "DEBUG"}},
    }

    config = overwrite_config(provided_config)
    # Test that telemetry from provided_config is preserved
    assert config["telemetry"]["logs"]["logLevel"] == "DEBUG"
    assert config["telemetry"]["OTLPExporter"] == {
        "tracesEndpoint": [],
        "logsEndpoint": [],
    }


# Not expected in practice, but exercise the code path
def test_overwrite_config_no_otlp_in_file(mocker):
    mock_config = """
    name: "stock-prices"
    language: "python"
    database:
        hostname: "hostname"
        port: 1234
        username: dbosadmin
        password: pwd
        app_db_name: appdbname
        sys_db_name: sysdbname
        ssl: true
        ssl_ca: cert.pem
    telemetry:
        logs:
            logLevel: INFO
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )

    provided_config: ConfigFile = {
        "name": "test-app",
        "database": {
            "hostname": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "dbos",
            "app_db_name": "dbostestpy",
        },
        "telemetry": {
            "OTLPExporter": {
                "tracesEndpoint": ["original-trace"],
                "logsEndpoint": ["original-log"],
            }
        },
    }

    config = overwrite_config(provided_config)
    # Test that OTLPExporter from provided_config is preserved
    assert config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == ["original-trace"]
    assert config["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["original-log"]
    assert "logs" not in config["telemetry"]


# Not expected in practice, but exercise the code path
def test_overwrite_config_with_provided_database_url(mocker):
    mock_config = """
    name: "stock-prices"
    language: "python"
    database:
        hostname: "hostname"
        port: 1234
        username: dbosadmin
        password: pwd
        app_db_name: appdbname
        sys_db_name: sysdbname
        ssl: true
        ssl_ca: cert.pem
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
    config = overwrite_config(provided_config)

    assert config["name"] == "stock-prices"
    assert config["database"]["hostname"] == "hostname"
    assert config["database"]["port"] == 1234
    assert config["database"]["username"] == "dbosadmin"
    assert config["database"]["password"] == "pwd"
    assert config["database"]["app_db_name"] == "appdbname"
    assert config["database"]["sys_db_name"] == "sysdbname"
    assert config["database"]["ssl"] == True
    assert config["database"]["ssl_ca"] == "cert.pem"
    assert config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "thetracesendpoint"
    ]
    assert config["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["thelogsendpoint"]
    assert "runtimeConfig" not in config
    assert "env" not in config


####################
# PROVIDED CONFIGS vs CONFIG FILE
####################


def test_no_discrepancy(mocker):
    mock_config = """
    name: "stock-prices" \
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )
    check_config_consistency(name="stock-prices")


def test_name_does_no_match(mocker):
    mock_config = """
    name: "stock-prices" \
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open("dbos-config.yaml", mock_config)
    )
    with pytest.raises(DBOSInitializationError) as exc_info:
        check_config_consistency(name="stock-prices-wrong")
    assert (
        "Provided app name 'stock-prices-wrong' does not match the app name 'stock-prices' in dbos-config.yaml"
        in str(exc_info.value)
    )


def test_no_config_file():
    # Handles FileNotFoundError
    check_config_consistency(name="stock-prices")


####################
# DATABASES CONNECTION POOLS
####################


def test_configured_pool_sizes():
    config: DBOSConfig = {
        "name": "test-app",
        "app_db_pool_size": 42,
        "sys_db_pool_size": 43,
    }

    dbos = DBOS(config=config)
    dbos.launch()
    assert dbos._app_db.engine.pool._pool.maxsize == 42
    assert dbos._sys_db.engine.pool._pool.maxsize == 43
    dbos.destroy()


def test_default_pool_params():
    config: DBOSConfig = {
        "name": "test-app",
    }

    dbos = DBOS(config=config)
    dbos.launch()
    app_db_engine = dbos._app_db.engine
    assert app_db_engine.pool._pool.maxsize == 20

    # force the release of connections so we can intercept on connect.
    app_db_engine.dispose()

    @event.listens_for(app_db_engine, "connect")
    def inspect_connection(dbapi_connection, connection_record):
        connect_timeout = dbapi_connection.info.get_parameters()["connect_timeout"]
        assert connect_timeout == "10"

    with app_db_engine.connect() as conn:
        pass

    assert dbos._sys_db.engine.pool._pool.maxsize == 20
    dbos.destroy()


def test_configured_app_db_connect_timeout():
    config: DBOSConfig = {
        "name": "test-app",
        "database_url": f"postgresql://postgres:@localhost:5432/dbname?connect_timeout=7",
    }

    dbos = DBOS(config=config)
    dbos.launch()
    app_db_engine = dbos._app_db.engine

    # force the release of connections so we can intercept on connect.
    app_db_engine.dispose()

    @event.listens_for(app_db_engine, "connect")
    def inspect_connection(dbapi_connection, connection_record):
        connect_timeout = dbapi_connection.info.get_parameters()["connect_timeout"]
        assert connect_timeout == "7"

    with app_db_engine.connect() as conn:
        pass

    dbos.destroy()


def test_get_dbos_database_url(mocker):
    mock_config = """
        name: "some-app"
        database:
          hostname: 'localhost'
          port: 5432
          username: 'postgres'
          password: ${PGPASSWORD}
          app_db_name: 'some_db'
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    expected_url = URL.create(
        "postgresql+psycopg",
        username="postgres",
        password=os.environ.get("PGPASSWORD"),
        host="localhost",
        port=5432,
        database="some_db",
    ).render_as_string(hide_password=False)
    assert get_dbos_database_url() == expected_url


def test_get_dbos_database_url_local_suffix(mocker):
    mock_config = """
        name: "some-app"
        database:
          hostname: 'localhost'
          port: 5432
          username: 'postgres'
          password: ${PGPASSWORD}
          app_db_name: 'some_db'
          local_suffix: true
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )
    expected_url = URL.create(
        "postgresql+psycopg",
        username="postgres",
        password=os.environ.get("PGPASSWORD"),
        host="localhost",
        port=5432,
        database="some_db_local",
    ).render_as_string(hide_password=False)
    assert get_dbos_database_url() == expected_url
