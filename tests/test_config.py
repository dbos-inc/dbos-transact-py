# type: ignore

import os
from unittest.mock import mock_open

import pytest
import pytest_mock

# Public API
from dbos import DBOS, load_config
from dbos._dbos_config import DBOSConfig, parse_db_string_to_dbconfig, set_env_vars
from dbos._error import DBOSInitializationError

mock_filename = "test.yaml"
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


def test_valid_config(mocker):
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

    configFile = load_config(mock_filename)
    assert configFile["name"] == "some-app"
    assert configFile["language"] == "python"
    assert configFile["database"]["hostname"] == "localhost"
    assert configFile["database"]["port"] == 5432
    assert configFile["database"]["username"] == "postgres"
    assert configFile["database"]["password"] == os.environ["PGPASSWORD"]
    assert configFile["database"]["app_db_name"] == "some db"
    assert configFile["database"]["connectionTimeoutMillis"] == 3000
    assert configFile["env"]["foo"] == "FOOFOO"
    assert configFile["env"]["bob"] is None  # Unset environment variable
    assert configFile["env"]["test_number"] == 123

    set_env_vars(configFile)
    assert os.environ["bazbaz"] == "BAZBAZ"
    assert os.environ["foo"] == "FOOFOO"
    assert os.environ["test_number"] == "123"
    assert "bob" not in os.environ


def test_valid_config_without_appdbname(mocker):
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
          connectionTimeoutMillis: 3000
    """
    os.environ["BARBAR"] = "FOOFOO"
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename)
    assert configFile["database"]["app_db_name"] == "some_app"


def test_config_load_defaults(mocker):
    mock_config = """
        name: "some-app"
        language: "python"
        runtimeConfig:
            start:
                - "python3 main.py"
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename)
    assert configFile["name"] == "some-app"
    assert configFile["language"] == "python"
    assert configFile["database"]["hostname"] == "localhost"
    assert configFile["database"]["port"] == 5432
    assert configFile["database"]["username"] == "postgres"
    assert configFile["database"]["password"] == os.environ.get("PGPASSWORD", "dbos")


def test_config_load_db_connection(mocker):
    mock_config = """
        name: "some-app"
        language: "python"
        runtimeConfig:
            start:
                - "python3 main.py"
    """
    mock_db_connection = """
    {"hostname": "example.com", "port": 2345, "username": "example", "password": "password", "local_suffix": true}
    """
    mocker.patch(
        "builtins.open",
        side_effect=generate_mock_open(
            [mock_filename, ".dbos/db_connection"], [mock_config, mock_db_connection]
        ),
    )

    configFile = load_config(mock_filename, use_db_wizard=False)
    assert configFile["name"] == "some-app"
    assert configFile["language"] == "python"
    assert configFile["database"]["hostname"] == "example.com"
    assert configFile["database"]["port"] == 2345
    assert configFile["database"]["username"] == "example"
    assert configFile["database"]["password"] == "password"
    assert configFile["database"]["local_suffix"] == True
    assert configFile["database"]["app_db_name"] == "some_app_local"


def test_config_mixed_params(mocker):
    mock_config = """
        name: "some-app"
        language: "python"
        runtimeConfig:
            start:
                - "python3 main.py"
        database:
          port: 1234
          username: 'some user'
          password: abc123
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename, use_db_wizard=False)
    assert configFile["name"] == "some-app"
    assert configFile["language"] == "python"
    assert configFile["database"]["hostname"] == "localhost"
    assert configFile["database"]["port"] == 1234
    assert configFile["database"]["username"] == "some user"
    assert configFile["database"]["password"] == "abc123"


def test_config_extra_params(mocker):
    mock_config = """
        name: "some-app"
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: abc123
          app_db_name: 'some db'
          connectionTimeoutMillis: 3000
        bob: 5555
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    with pytest.raises(DBOSInitializationError) as exc_info:
        load_config(mock_filename)

    assert (
        "Validation error: Additional properties are not allowed ('bob' was unexpected)"
        in str(exc_info.value)
    )


def test_config_missing_name(mocker):
    mock_config = """
        language: python
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: abc123
          app_db_name: 'some db'
          connectionTimeoutMillis: 3000
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    with pytest.raises(DBOSInitializationError) as exc_info:
        load_config(mock_filename)

    assert "must specify an application name" in str(exc_info.value)


def test_config_missing_language(mocker):
    mock_config = """
        name: "some-app"
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: abc123
          app_db_name: 'some db'
          connectionTimeoutMillis: 3000
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    with pytest.raises(DBOSInitializationError) as exc_info:
        load_config(mock_filename)

    assert "must specify the application language" in str(exc_info.value)


def test_config_bad_language(mocker):
    mock_config = """
        name: "some-app"
        language: typescript
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: abc123
          app_db_name: 'some db'
          connectionTimeoutMillis: 3000
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    with pytest.raises(DBOSInitializationError) as exc_info:
        load_config(mock_filename)

    assert "invalid language" in str(exc_info.value)


def test_config_bad_name(mocker):
    mock_config = """
        name: "some app"
        language: python
        runtimeConfig:
            start:
                - "python3 main.py"
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: abc123
          app_db_name: 'some db'
          connectionTimeoutMillis: 3000
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    with pytest.raises(DBOSInitializationError) as exc_info:
        load_config(mock_filename)

    assert "Invalid app name" in str(exc_info.value)


def test_config_no_start(mocker):
    mock_config = """
        name: "some-app"
        language: python
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: abc123
          app_db_name: 'some db'
          connectionTimeoutMillis: 3000
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    with pytest.raises(DBOSInitializationError) as exc_info:
        load_config(mock_filename)

    assert "start command" in str(exc_info.value)


def test_local_config(mocker):
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
          app_db_name: 'some_db'
          connectionTimeoutMillis: 3000
          local_suffix: true
    """
    os.environ["BARBAR"] = "FOOFOO"
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename)
    assert configFile["name"] == "some-app"
    assert configFile["database"]["local_suffix"] == True
    assert configFile["language"] == "python"
    assert configFile["database"]["hostname"] == "localhost"
    assert configFile["database"]["port"] == 5432
    assert configFile["database"]["username"] == "postgres"
    assert configFile["database"]["password"] == os.environ["PGPASSWORD"]
    assert configFile["database"]["app_db_name"] == "some_db_local"
    assert configFile["database"]["connectionTimeoutMillis"] == 3000


def test_local_config_without_name(mocker):
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
          connectionTimeoutMillis: 3000
          local_suffix: true
    """
    os.environ["BARBAR"] = "FOOFOO"
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename)
    assert configFile["name"] == "some-app"
    assert configFile["database"]["local_suffix"] == True
    assert configFile["language"] == "python"
    assert configFile["database"]["hostname"] == "localhost"
    assert configFile["database"]["port"] == 5432
    assert configFile["database"]["username"] == "postgres"
    assert configFile["database"]["password"] == os.environ["PGPASSWORD"]
    assert configFile["database"]["app_db_name"] == "some_app_local"
    assert configFile["database"]["connectionTimeoutMillis"] == 3000


def test_db_connect_failed(mocker):
    mock_config = """
        name: "some-app"
        language: "python"
        runtimeConfig:
            start:
                - "python3 main.py"
        database:
          hostname: 'example.com'
          port: 5432
          username: 'pgu'
          password: ${PGPASSWORD}
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    with pytest.raises(DBOSInitializationError) as exc_info:
        load_config(mock_filename)

    assert "Could not connect to the database" in str(exc_info.value)


def test_no_db_wizard(mocker):
    mock_config = """
        name: "some-app"
        language: "python"
        runtimeConfig:
            start:
                - "python3 main.py"
        database:
          hostname: 'localhost'
          port: 5432
          username: 'postgres'
          password: 'somerandom'

    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    with pytest.raises(DBOSInitializationError) as exc_info:
        load_config(mock_filename)
    assert "Could not connect" in str(exc_info.value)


def test_debug_override(mocker: pytest_mock.MockFixture):
    mock_config = """
        name: "some-app"
        language: "python"
        runtimeConfig:
            start:
                - "python3 main.py"
        database:
          hostname: 'localhost'
          port: 5432
          username: 'postgres'
          password: 'super-secret-password'
          local_suffix: true
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

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

    configFile = load_config(mock_filename, use_db_wizard=False)
    assert configFile["database"]["hostname"] == "fakehost"
    assert configFile["database"]["port"] == 1234
    assert configFile["database"]["username"] == "fakeuser"
    assert configFile["database"]["password"] == "fakepassword"
    assert configFile["database"]["local_suffix"] == False


def test_parse_db_string_to_dbconfig():
    db_string = "postgresql://user:password@localhost:5432/dbname"
    db_config = parse_db_string_to_dbconfig(db_string)
    assert db_config["hostname"] == "localhost"
    assert db_config["port"] == 5432
    assert db_config["username"] == "user"
    assert db_config["password"] == "password"
    assert db_config["app_db_name"] == "dbname"

    db_string = "postgresql://user:password@localhost:5432/dbname?connect_timeout=10&sslmode=require&sslcert=ca.pem"
    db_config = parse_db_string_to_dbconfig(db_string)
    assert db_config["ssl"] == True
    assert db_config["ssl_ca"] == "ca.pem"
    assert db_config["connectionTimeoutMillis"] == 10000

    # Test unusual but valid DB strings
    db_string = "postgresql://user:complex%23password@hostname.with.dots:5432/dbname?sslmode=require&application_name=myapp"
    db_config = parse_db_string_to_dbconfig(db_string)
    assert db_config["hostname"] == "hostname.with.dots"
    assert db_config["password"] == "complex#password"  # Ensure URL decoding works

    # Missing required field
    with pytest.raises(Exception):
        db_string = "invalid"
        parse_db_string_to_dbconfig(db_string)


def test_dbosconfig():
    # Give all fields
    config: DBOSConfig = {
        "name": "test-app",
        "db_string": "postgresql://user:password@localhost:5432/dbname",
        "sys_db_name": "sysdb",
        "log_level": "DEBUG",
        "otlp_traces_endpoints": ["http://otel:7777", "notused"],
        "admin_port": 8001,
    }
    dbos = DBOS(config=config)
    assert dbos.config["name"] == "test-app"
    assert dbos.config["database"]["hostname"] == "localhost"
    assert dbos.config["database"]["port"] == 5432
    assert dbos.config["database"]["username"] == "user"
    assert dbos.config["database"]["password"] == "password"
    assert dbos.config["database"]["app_db_name"] == "dbname"
    assert dbos.config["database"]["sys_db_name"] == "sysdb"
    assert dbos.config["telemetry"]["logs"]["logLevel"] == "DEBUG"
    assert (
        dbos.config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == "http://otel:7777"
    )
    assert dbos.config["runtimeConfig"]["admin_port"] == 8001
    assert dbos.config["runtimeConfig"]["start"] == []

    dbos.destroy()

    # Give only mandatory fields
    config: DBOSConfig = {
        "name": "test-app",
        "db_string": "postgresql://user:password@localhost:5432/dbname",
    }
    dbos = DBOS(config=config)
    assert dbos.config["name"] == "test-app"
    assert dbos.config["database"]["hostname"] == "localhost"
    assert dbos.config["database"]["port"] == 5432
    assert dbos.config["database"]["username"] == "user"
    assert dbos.config["database"]["password"] == "password"
    assert dbos.config["database"]["app_db_name"] == "dbname"
    assert "sys_db_name" not in dbos.config["database"]
    assert "telemetry" not in dbos.config
    assert "admin_port" not in dbos.config["runtimeConfig"]
    assert dbos.config["runtimeConfig"]["start"] == []

    dbos.destroy()

    # Give an empty OTLP traces endpoint list
    config: DBOSConfig = {
        "name": "test-app",
        "db_string": "postgresql://user:password@localhost:5432/dbname",
        "otlp_traces_endpoints": [],
    }
    dbos = DBOS(config=config)
    assert "telemetry" not in dbos.config
    dbos.destroy()

    # Missing required field
    with pytest.raises(Exception):
        config: DBOSConfig = {
            "db_string": "postgresql://user:password@localhost:5432/dbname"
        }
        try:
            dbos = DBOS(config=config)
        finally:
            if dbos is not None:
                dbos.destroy()


def test_dbos_config_overwrite(mocker):
    os.environ["DBOS__CLOUD"] = "true"

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

    # Give all fields
    config: DBOSConfig = {
        "name": "test-app",
        "db_string": "postgresql://user:password@localhost:5432/dbname?connect_timeout=10&sslmode=require&sslcert=ca.pem",
        "sys_db_name": "sysdb",
        "log_level": "DEBUG",
        "otlp_traces_endpoints": ["http://otel:7777", "notused"],
        "admin_port": 8001,
    }
    dbos = DBOS(config=config)

    assert dbos.config["name"] == "test-app"
    assert dbos.config["database"]["hostname"] == "hostname"
    assert dbos.config["database"]["port"] == 1234
    assert dbos.config["database"]["username"] == "dbosadmin"
    assert dbos.config["database"]["password"] == "pwd"
    assert dbos.config["database"]["app_db_name"] == "appdbname"
    assert dbos.config["database"]["sys_db_name"] == "sysdbname"
    assert dbos.config["database"]["ssl"] == True
    assert dbos.config["database"]["ssl_ca"] == "cert.pem"
    assert dbos.config["database"]["connectionTimeoutMillis"] == 10000
    assert dbos.config["telemetry"]["logs"]["logLevel"] == "DEBUG"
    assert (
        dbos.config["telemetry"]["OTLPExporter"]["tracesEndpoint"]
        == "thetracesendpoint"
    )
    assert dbos.config["runtimeConfig"]["admin_port"] == 8001
    assert dbos.config["runtimeConfig"]["start"] == ["a start command"]
    assert dbos.config["env"]["KEY"] == "VALUE"

    # Provide custom setup steps
    dbos.destroy()

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

    # Give all fields
    config: DBOSConfig = {
        "name": "test-app",
        "db_string": "postgresql://user:password@localhost:5432/dbname",
    }
    dbos = DBOS(config=config)

    assert dbos.config["name"] == "test-app"
    assert dbos.config["database"]["hostname"] == "hostname"
    assert dbos.config["database"]["port"] == 1234
    assert dbos.config["database"]["username"] == "dbosadmin"
    assert dbos.config["database"]["password"] == "pwd"
    assert dbos.config["database"]["app_db_name"] == "appdbname"
    assert dbos.config["database"]["sys_db_name"] == "sysdbname"
    assert dbos.config["database"]["ssl"] == True
    assert dbos.config["database"]["ssl_ca"] == "cert.pem"
    assert (
        dbos.config["telemetry"]["OTLPExporter"]["tracesEndpoint"]
        == "thetracesendpoint"
    )
    assert "admin_port" not in dbos.config["runtimeConfig"]
    assert dbos.config["runtimeConfig"]["setup"] == ["echo 'hello'"]
    assert dbos.config["runtimeConfig"]["start"] == ["a start command"]
    assert "env" not in dbos.config

    del os.environ["DBOS__CLOUD"]
