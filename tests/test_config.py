# type: ignore

from unittest.mock import mock_open

import pytest

import dbos_transact.dbos_config

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

    configFile = dbos_transact.dbos_config.load_config(mock_filename)
    assert configFile["database"]["hostname"] == "some host"
    assert configFile["database"]["port"] == 1234
    assert configFile["database"]["username"] == "some user"
    assert configFile["database"]["password"] == "abc123"
    assert configFile["database"]["app_db_name"] == "some db"
    assert configFile["database"]["connectionTimeoutMillis"] == 3000


def test_config_missing_params(mocker):
    mock_config = """
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

    with pytest.raises(ValueError) as exc_info:
        dbos_transact.dbos_config.load_config(mock_filename)

    assert "'app_db_name' is a required property" in str(exc_info.value)


def test_config_extra_params(mocker):
    mock_config = """
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

    with pytest.raises(ValueError) as exc_info:
        dbos_transact.dbos_config.load_config(mock_filename)

    assert (
        "Validation error: Additional properties are not allowed ('bob' was unexpected)"
        in str(exc_info.value)
    )
