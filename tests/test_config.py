# type: ignore

import os
from unittest.mock import mock_open

import pytest

# Public API
from dbos import load_config
from dbos.dbos_config import set_env_vars
from dbos.error import DBOSInitializationError

mock_filename = "test.yaml"
original_open = __builtins__["open"]


def generate_mock_open(filename, mock_data):
    def conditional_mock_open(*args, **kwargs):
        if args[0] == filename:
            m = mock_open(read_data=mock_data)
            return m()
        else:
            return original_open(*args, **kwargs)

    return conditional_mock_open


def test_valid_config(mocker):
    mock_config = """
        name: "some app"
        language: "python"
        runtimeConfig:
            start:
                - "python3 main.py"
            admin_port: 8001
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: ${PGPASSWORD}
          app_db_name: 'some db'
          connectionTimeoutMillis: 3000
        env:
            foo: ${BARBAR}
            bazbaz: BAZBAZ
            bob: ${BOBBOB}
    """
    os.environ["BARBAR"] = "FOOFOO"
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename)
    assert configFile["name"] == "some app"
    assert configFile["language"] == "python"
    assert configFile["database"]["hostname"] == "some host"
    assert configFile["database"]["port"] == 1234
    assert configFile["database"]["username"] == "some user"
    assert configFile["database"]["password"] == os.environ["PGPASSWORD"]
    assert configFile["database"]["app_db_name"] == "some db"
    assert configFile["database"]["connectionTimeoutMillis"] == 3000
    assert configFile["env"]["foo"] == "FOOFOO"
    assert configFile["env"]["bob"] is None  # Unset environment variable

    set_env_vars(configFile)
    assert os.environ["bazbaz"] == "BAZBAZ"
    assert os.environ["foo"] == "FOOFOO"
    assert "bob" not in os.environ


def test_config_missing_params(mocker):
    mock_config = """
        name: "some app"
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: abc123
          connectionTimeoutMillis: 3000
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    with pytest.raises(DBOSInitializationError) as exc_info:
        load_config(mock_filename)

    assert "'app_db_name' is a required property" in str(exc_info.value)


def test_config_extra_params(mocker):
    mock_config = """
        name: "some app"
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
        name: "some app"
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
        name: "some app"
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


def test_config_no_start(mocker):
    mock_config = """
        name: "some app"
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
        name: "some app"
        language: "python"
        runtimeConfig:
            start:
                - "python3 main.py"
            admin_port: 8001
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
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
    assert configFile["name"] == "some app"
    assert configFile["database"]["local_suffix"] == True
    assert configFile["language"] == "python"
    assert configFile["database"]["hostname"] == "some host"
    assert configFile["database"]["port"] == 1234
    assert configFile["database"]["username"] == "some user"
    assert configFile["database"]["password"] == os.environ["PGPASSWORD"]
    assert configFile["database"]["app_db_name"] == "some_db_local"
    assert configFile["database"]["connectionTimeoutMillis"] == 3000
