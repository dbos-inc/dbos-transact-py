# type: ignore

import os
from unittest.mock import mock_open

import pytest

# Public API
from dbos import load_config
from dbos._dbos_config import set_env_vars
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
