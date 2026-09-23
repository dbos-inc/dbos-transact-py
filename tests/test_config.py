# type: ignore

import os
import threading
import time
import uuid
from typing import Any, List
from unittest.mock import mock_open
from urllib.parse import quote

import pytest
import sqlalchemy as sa
from sqlalchemy import NullPool, event
from sqlalchemy.exc import DBAPIError, OperationalError

# Public API
from dbos import DBOS, SetWorkflowID
from dbos._dbos_config import (
    ConfigFile,
    DBOSConfig,
    configure_db_engine_parameters,
    load_config,
    overwrite_config,
    process_config,
    translate_dbos_config_to_config_file,
)
from dbos._error import DBOSException, DBOSInitializationError
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import DefaultSerializer
from dbos._sys_db import SystemDatabase
from tests.conftest import postgres_urls, retry_until_success

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
        "system_database_url": f"postgres://postgres:{os.environ.get('PGPASSWORD', 'dbos')}@localhost:5432/some_app_dbos_sys",
    }
    dbos = DBOS(config=config)
    assert dbos._config["name"] == "some-app"
    assert dbos._config["system_database_url"] == config["system_database_url"]
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
        system_database_url: "postgres://user:dbos@localhost:5432/dbname_dbos_sys?connect_timeout=10&sslmode=require&sslrootcert=ca.pem"
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename)
    assert configFile["name"] == "some-app"
    assert (
        configFile["system_database_url"]
        == f"postgres://user:dbos@localhost:5432/dbname_dbos_sys?connect_timeout=10&sslmode=require&sslrootcert=ca.pem"
    )


def test_load_config_with_unset_database_url_env_var(mocker):
    mock_config = """
    name: "some-app"
    system_database_url: ${UNSET}
    """

    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename)
    assert configFile["name"] == "some-app"


def test_load_config_substitutes_env_vars(mocker, monkeypatch):
    monkeypatch.setenv("TEST_DB_PASSWORD", "secret")
    mock_config = """
    name: "some-app"
    system_database_url: postgres://postgres:${TEST_DB_PASSWORD}@localhost:5432/some_app_dbos_sys
    """

    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    configFile = load_config(mock_filename)
    assert (
        configFile["system_database_url"]
        == "postgres://postgres:secret@localhost:5432/some_app_dbos_sys"
    )


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
        "system_database_url": "postgres://user:password@localhost:7777/dbn_dbos_sys?connect_timeout=1&sslmode=require&sslrootcert=ca.pem",
        "database": {
            "sys_db_pool_size": 27,
            "db_engine_kwargs": {"key": "value"},
            "migrate": ["alembic upgrade head"],
        },
        "runtimeConfig": {
            "start": ["python3 main.py"],
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
    }

    configFile = process_config(data=config)
    assert configFile["name"] == "some-app"
    assert (
        configFile["system_database_url"]
        == "postgres://user:password@localhost:7777/dbn_dbos_sys?connect_timeout=1&sslmode=require&sslrootcert=ca.pem"
    )
    assert configFile["database"]["migrate"] == ["alembic upgrade head"]
    assert configFile["database"]["sys_db_engine_kwargs"] == {
        "key": "value",
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 27,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 1, "application_name": "dbos_transact"},
    }
    assert configFile["runtimeConfig"]["start"] == ["python3 main.py"]
    assert configFile["runtimeConfig"]["setup"] == ["echo 'hello'"]
    assert configFile["telemetry"]["logs"]["logLevel"] == "DEBUG"
    assert configFile["telemetry"]["OTLPExporter"]["logsEndpoint"] == [
        "thelogsendpoint"
    ]
    assert configFile["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "thetracesendpoint"
    ]


def test_process_config_system_database():
    config: ConfigFile = {
        "name": "some-app",
        "system_database_url": "postgres://user:password@localhost:7778/dbn_sys?connect_timeout=1&sslmode=require&sslrootcert=ca.pem",
        "database": {
            "sys_db_pool_size": 27,
            "db_engine_kwargs": {"key": "value"},
            "migrate": ["alembic upgrade head"],
        },
    }

    configFile = process_config(data=config)
    assert configFile["name"] == "some-app"
    assert configFile["system_database_url"] == config["system_database_url"]
    assert configFile["database"]["sys_db_engine_kwargs"] == {
        "key": "value",
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 27,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 1, "application_name": "dbos_transact"},
    }


def test_process_config_only_system_database():
    config: ConfigFile = {
        "name": "some-app",
        "system_database_url": "postgres://user:password@localhost:7778/dbn_sys?connect_timeout=1&sslmode=require&sslrootcert=ca.pem",
    }

    configFile = process_config(data=config)
    assert configFile["name"] == "some-app"
    assert configFile["system_database_url"] == config["system_database_url"]


def test_process_config_sqlite():
    config: ConfigFile = {
        "name": "some-app",
        "system_database_url": "sqlite:///test.sys.sqlite",
    }

    configFile = process_config(data=config)
    assert configFile["name"] == "some-app"
    assert configFile["system_database_url"] == config["system_database_url"]


def test_process_config_load_defaults():
    config: ConfigFile = {
        "name": "some-app",
    }
    processed_config = process_config(data=config)
    assert processed_config["name"] == "some-app"
    assert processed_config["system_database_url"] == f"sqlite:///some_app.sqlite"
    assert processed_config["database"]["sys_db_engine_kwargs"] is not None
    assert processed_config["telemetry"]["logs"]["logLevel"] == "INFO"


def test_process_config_load_default_with_None_system_database_url():
    config: ConfigFile = {
        "name": "some-app",
        "system_database_url": None,
    }
    processed_config = process_config(data=config)
    assert processed_config["name"] == "some-app"
    assert processed_config["system_database_url"] == f"sqlite:///some_app.sqlite"
    assert processed_config["database"]["sys_db_engine_kwargs"] is not None
    assert processed_config["telemetry"]["logs"]["logLevel"] == "INFO"


def test_process_config_load_default_with_empty_system_database_url():
    config: ConfigFile = {
        "name": "some-app",
        "system_database_url": "",
    }
    processed_config = process_config(data=config)
    assert processed_config["name"] == "some-app"
    assert processed_config["system_database_url"] == f"sqlite:///some_app.sqlite"
    assert processed_config["database"]["sys_db_engine_kwargs"] is not None
    assert processed_config["telemetry"]["logs"]["logLevel"] == "INFO"


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


def test_config_name_length_bounds():
    for name in ["abc", "a" * 256]:
        config: ConfigFile = {"name": name}
        assert process_config(data=config)["name"] == name

    for name in ["ab", "a" * 257]:
        config = {"name": name}
        with pytest.raises(DBOSInitializationError) as exc_info:
            process_config(data=config)
        assert "Invalid app name" in str(exc_info.value)


####################
# PROCESS DB ENGINE KWARGS
####################
def test_configure_db_engine_parameters_defaults():
    """Test that default values are set when no pool sizes or kwargs are provided."""
    data: DatabaseConfig = {}

    configure_db_engine_parameters(data)

    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 20,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 10, "application_name": "dbos_transact"},
    }


def test_configure_db_engine_parameters_custom_sys_db_pool_sizes():
    """Test that custom pool sizes are preserved and used in engine kwargs."""
    data: DatabaseConfig = {"sys_db_pool_size": 35}

    configure_db_engine_parameters(data)

    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 35,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 10, "application_name": "dbos_transact"},
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
            "connect_args": {
                "connect_timeout": 30,
                "key": "value",
                "application_name": "dbos_transact",
            },
        },
    }

    configure_db_engine_parameters(data)

    # User kwargs should override defaults and include custom params

    # System engine kwargs should use system pool size but same user overrides
    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 60,
        "max_overflow": 10,
        "pool_pre_ping": True,
        "custom_param": "value",
        "pool_size": 35,
        "connect_args": {
            "connect_timeout": 30,
            "key": "value",
            "application_name": "dbos_transact",
        },
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

    # System engine kwargs should use system pool size but same user overrides
    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 60,
        "max_overflow": 0,
        "pool_pre_ping": True,
        "custom_param": "value",
        "pool_size": 50,
        "connect_args": {"connect_timeout": 22, "application_name": "dbos_transact"},
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

    # System engine kwargs should use system pool size but same user overrides
    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 60,
        "max_overflow": 0,
        "pool_pre_ping": True,
        "custom_param": "value",
        "pool_size": 50,
        "connect_args": {"connect_timeout": 10, "application_name": "dbos_transact"},
    }


def test_configure_db_engine_parameters_empty_user_kwargs():
    """Test handling of empty user kwargs dict."""
    data: DatabaseConfig = {"db_engine_kwargs": {}}

    configure_db_engine_parameters(data)

    assert data["sys_db_engine_kwargs"] == {
        "pool_timeout": 30,
        "max_overflow": 0,
        "pool_size": 20,
        "pool_pre_ping": True,
        "connect_args": {"connect_timeout": 10, "application_name": "dbos_transact"},
    }


####################
# VALIDATE DATABASE URL
####################


def test_process_config_with_wrong_db_url():
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
        "system_database_url": expected_url,
    }
    processed_config = translate_dbos_config_to_config_file(config)
    assert processed_config["name"] == "some-app"
    assert processed_config["system_database_url"] == expected_url


####################
# TRANSLATE DBOSConfig to ConfigFile
####################


def test_translate_dbosconfig_full_input():
    # Give all fields
    config: DBOSConfig = {
        "name": "test-app",
        "system_database_url": "postgres://user:password@localhost:5432/dbname?connect_timeout=11&sslmode=require&sslrootcert=ca.pem",
        "sys_db_pool_size": 27,
        "db_engine_kwargs": {"key": "value"},
        "log_level": "DEBUG",
        "otlp_traces_endpoints": ["http://otel:7777", "notused"],
        "dbos_system_schema": "foobar",
    }

    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["name"] == "test-app"
    assert translated_config["system_database_url"] == config["system_database_url"]
    assert translated_config["database"]["sys_db_pool_size"] == 27
    assert translated_config["database"]["db_engine_kwargs"] == {"key": "value"}
    assert translated_config["telemetry"]["logs"]["logLevel"] == "DEBUG"
    assert translated_config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "http://otel:7777",
        "notused",
    ]
    assert translated_config["telemetry"]["OTLPExporter"]["logsEndpoint"] == []
    assert translated_config["telemetry"]["disable_otlp"] == True
    assert translated_config["dbos_system_schema"] == "foobar"
    assert translated_config["use_listen_notify"] == True
    assert translated_config["run_migrations"] == True
    assert "start" not in translated_config["runtimeConfig"]
    assert "setup" not in translated_config["runtimeConfig"]


def test_translate_dbosconfig_notification_coalesce_sec():
    # A valid value is threaded into runtimeConfig.
    ok: DBOSConfig = {"name": "test-app", "notification_coalesce_sec": 0.001}
    translated = translate_dbos_config_to_config_file(ok)
    assert translated["runtimeConfig"]["notification_coalesce_sec"] == 0.001

    # Invalid values are rejected, including NaN/inf which would otherwise crash run_notifier's time.sleep.
    for bad in [0.0005, 0.0, -1.0, float("nan"), float("inf")]:
        with pytest.raises(DBOSInitializationError) as exc_info:
            translate_dbos_config_to_config_file(
                {"name": "test-app", "notification_coalesce_sec": bad}
            )
        assert "notification_coalesce_sec" in str(exc_info.value)


def test_translate_dbosconfig_observability_query_timeout_sec():
    # A valid value is threaded into runtimeConfig, including a non-positive one, which disables the cap.
    for ok_value in [5.0, 0]:
        ok: DBOSConfig = {
            "name": "test-app",
            "observability_query_timeout_sec": ok_value,
        }
        translated = translate_dbos_config_to_config_file(ok)
        assert (
            translated["runtimeConfig"]["observability_query_timeout_sec"] == ok_value
        )

    # NaN/inf are rejected: the timeout becomes an integer number of milliseconds.
    for bad in [float("nan"), float("inf")]:
        with pytest.raises(DBOSInitializationError) as exc_info:
            translate_dbos_config_to_config_file(
                {"name": "test-app", "observability_query_timeout_sec": bad}
            )
        assert "observability_query_timeout_sec" in str(exc_info.value)

        with pytest.raises(DBOSInitializationError) as exc_info:
            SystemDatabase.create(
                system_database_url="sqlite:///dbos.sqlite",
                engine_kwargs={},
                engine=None,
                schema=None,
                serializer=DefaultSerializer(),
                executor_id=None,
                observability_query_timeout_sec=bad,
            )
        assert "observability_query_timeout_sec" in str(exc_info.value)


def test_translate_dbosconfig_idle_transaction_timeout_sec():
    # A valid value is threaded into the database config, including a non-positive one, which disables it.
    for ok_value in [5.0, 0]:
        ok: DBOSConfig = {
            "name": "test-app",
            "sys_db_idle_transaction_timeout_sec": ok_value,
        }
        translated = translate_dbos_config_to_config_file(ok)
        assert translated["database"]["sys_db_idle_transaction_timeout_sec"] == ok_value

    for bad in [float("nan"), float("inf")]:
        with pytest.raises(DBOSInitializationError) as exc_info:
            translate_dbos_config_to_config_file(
                {"name": "test-app", "sys_db_idle_transaction_timeout_sec": bad}
            )
        assert "sys_db_idle_transaction_timeout_sec" in str(exc_info.value)

        with pytest.raises(DBOSInitializationError) as exc_info:
            SystemDatabase.create(
                system_database_url="sqlite:///dbos.sqlite",
                engine_kwargs={},
                engine=None,
                schema=None,
                serializer=DefaultSerializer(),
                executor_id=None,
                idle_transaction_timeout_sec=bad,
            )
        assert "sys_db_idle_transaction_timeout_sec" in str(exc_info.value)


SETTING = "SHOW idle_in_transaction_session_timeout"


def _make_sysdb(**kwargs: Any) -> SystemDatabase:
    kwargs.setdefault("engine_kwargs", {"pool_size": 1, "max_overflow": 0})
    return SystemDatabase.create(
        system_database_url=postgres_urls()[1],
        engine=kwargs.pop("engine", None),
        schema="dbos",
        serializer=DefaultSerializer(),
        executor_id=None,
        **kwargs,
    )


def _settings(engine: sa.Engine, checkouts: int = 2) -> List[str]:
    """The setting as seen by consecutive checkouts of the same pooled connection."""
    seen = []
    for _ in range(checkouts):
        with engine.connect() as c:
            seen.append(str(c.execute(sa.text(SETTING)).scalar()))
    return seen


def _server_default() -> str:
    engine = sa.create_engine(postgres_urls()[1])
    try:
        with engine.connect() as c:
            return str(c.execute(sa.text(SETTING)).scalar())
    finally:
        engine.dispose()


def test_default_timeout_survives_pool_reuse(skip_with_sqlite: None) -> None:
    # A bare SET in the connect hook is undone by the pool's rollback on return.
    sys_db = _make_sysdb()
    try:
        assert _settings(sys_db.engine) == ["1min", "1min"]
    finally:
        sys_db.destroy()


def test_custom_and_disabled_timeout(skip_with_sqlite: None) -> None:
    sys_db = _make_sysdb(idle_transaction_timeout_sec=5)
    try:
        assert _settings(sys_db.engine) == ["5s", "5s"]
    finally:
        sys_db.destroy()

    server_default = _server_default()
    sys_db = _make_sysdb(idle_transaction_timeout_sec=0)
    try:
        assert _settings(sys_db.engine) == [server_default, server_default]
    finally:
        sys_db.destroy()


def test_user_setting_takes_precedence(skip_with_sqlite: None) -> None:
    sys_db = _make_sysdb(
        engine_kwargs={
            "pool_size": 1,
            "max_overflow": 0,
            "connect_args": {"options": "-c idle_in_transaction_session_timeout=7000"},
        },
    )
    try:
        assert _settings(sys_db.engine) == ["7s", "7s"]
    finally:
        sys_db.destroy()


def test_custom_engine_is_untouched(skip_with_sqlite: None) -> None:
    engine = sa.create_engine(postgres_urls()[1], pool_size=1, max_overflow=0)
    sys_db = _make_sysdb(engine=engine, engine_kwargs={})
    try:
        server_default = _server_default()
        assert _settings(sys_db.engine) == [server_default, server_default]
    finally:
        sys_db.destroy()
        engine.dispose()


def test_stranded_lock_does_not_block_cancel(
    skip_with_sqlite: None, dbos: DBOS, config: DBOSConfig
) -> None:
    """A session frozen inside a transaction that holds a workflow's status row is
    ended by the server, so cancelling that workflow returns instead of hanging."""
    config["sys_db_idle_transaction_timeout_sec"] = 1
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    release = threading.Event()
    started = threading.Event()

    @DBOS.workflow()
    def blocked_workflow() -> None:
        started.set()
        release.wait()

    DBOS.launch()
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        DBOS.start_workflow(blocked_workflow)
    assert started.wait(10)

    # The frozen client: it locks the row, then never sends another statement.
    holder = dbos._sys_db.engine.connect()
    holder.begin()
    holder.execute(
        sa.select(SystemSchema.workflow_status.c.workflow_uuid)
        .where(SystemSchema.workflow_status.c.workflow_uuid == wfid)
        .with_for_update()
    )
    try:
        begin = time.monotonic()
        DBOS.cancel_workflow(wfid)
        elapsed = time.monotonic() - begin
        # Waited on the stranded lock, and was released by the timeout, not by the holder.
        assert 0.5 < elapsed < 15, elapsed

        def cancelled() -> None:
            status = DBOS.get_workflow_status(wfid)
            assert status is not None and status.status == "CANCELLED"

        retry_until_success(cancelled, interval=0.1, max_attempts=50)

        # The holder's session is gone; its next statement fails as a lost connection.
        with pytest.raises(DBAPIError) as exc_info:
            holder.execute(sa.text("SELECT 1"))
        assert exc_info.value.connection_invalidated
    finally:
        holder.close()
        release.set()

    # The pool replaces the killed connection.
    with dbos._sys_db.engine.begin() as c:
        assert c.execute(sa.text("SELECT 1")).scalar() == 1


def test_translate_dbosconfig_run_migrations():
    # Defaults to True, and an explicit setting survives translation.
    assert (
        translate_dbos_config_to_config_file({"name": "test-app"})["run_migrations"]
        == True
    )
    disabled: DBOSConfig = {"name": "test-app", "run_migrations": False}
    assert translate_dbos_config_to_config_file(disabled)["run_migrations"] == False


def test_translate_dbosconfig_kafka_queue_polling_interval_sec():
    # A valid value is threaded into runtimeConfig.
    ok: DBOSConfig = {"name": "test-app", "kafka_queue_polling_interval_sec": 5.0}
    translated = translate_dbos_config_to_config_file(ok)
    assert translated["runtimeConfig"]["kafka_queue_polling_interval_sec"] == 5.0

    # Invalid values are rejected, including NaN/inf which would otherwise crash the queue worker's wait.
    for bad in [0.0005, 0.0, -1.0, float("nan"), float("inf")]:
        with pytest.raises(DBOSInitializationError) as exc_info:
            translate_dbos_config_to_config_file(
                {"name": "test-app", "kafka_queue_polling_interval_sec": bad}
            )
        assert "kafka_queue_polling_interval_sec" in str(exc_info.value)


def test_translate_dbosconfig_minimal_input():
    config: DBOSConfig = {
        "name": "test-app",
    }
    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["name"] == "test-app"
    assert translated_config["telemetry"]["logs"]["logLevel"] == "INFO"
    assert "database" not in translated_config


def test_translate_dbosconfig_just_sys_db_pool_size():
    config: DBOSConfig = {
        "name": "test-app",
        "sys_db_pool_size": 27,
    }
    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["database"]["sys_db_pool_size"] == 27


def test_translate_dbosconfig_sys_db_polling_concurrency():
    config: DBOSConfig = {
        "name": "test-app",
        "sys_db_pool_size": 50,
        "sys_db_polling_concurrency": 8,
    }
    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["database"]["sys_db_pool_size"] == 50
    assert translated_config["database"]["sys_db_polling_concurrency"] == 8

    # When unset, translation leaves it absent; the default is materialized later in SystemDatabase.
    translated_config = translate_dbos_config_to_config_file(
        {
            "name": "test-app",
            "sys_db_pool_size": 50,
        }
    )
    assert translated_config["database"]["sys_db_pool_size"] == 50
    assert "sys_db_polling_concurrency" not in translated_config["database"]


def test_translate_dbosconfig_just_db_engine_kwargs():
    config: DBOSConfig = {
        "name": "test-app",
        "db_engine_kwargs": {"key": "value"},
    }
    translated_config = translate_dbos_config_to_config_file(config)

    assert translated_config["database"]["db_engine_kwargs"] == {"key": "value"}
    assert "sys_db_pool_size" not in translated_config["database"]


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


@pytest.mark.parametrize("key", ["database_url", "application_database_url"])
def test_translate_rejects_removed_application_database_url(key):
    config = {
        "name": "test-app",
        key: "postgres://user:password@localhost:5432/dbname",
    }
    with pytest.raises(DBOSInitializationError) as exc_info:
        translate_dbos_config_to_config_file(config)
    assert f"DBOSConfig sets {key}" in str(exc_info.value)


@pytest.mark.parametrize("key", ["database_url", "application_database_url"])
@pytest.mark.parametrize(
    "value", ['"postgres://user:pw@localhost:5432/shop"', "", None]
)
def test_load_config_ignores_removed_application_database_url(mocker, key, value):
    """DBOS Cloud rewrites dbos-config.yaml to add a database URL, so rejecting the key
    there would fail every cloud deploy, not just stale ones. It is ignored instead."""
    rendered = "" if value is None else f" {value}"
    mock_config = f"""
    name: "some-app"
    system_database_url: "postgres://user:pw@localhost:5432/shop_dbos_sys"
    {key}:{rendered}
    """
    mocker.patch(
        "builtins.open", side_effect=generate_mock_open(mock_filename, mock_config)
    )

    config = load_config(mock_filename)

    assert config["name"] == "some-app"
    assert (
        process_config(data=config)["system_database_url"]
        == "postgres://user:pw@localhost:5432/shop_dbos_sys"
    )


@pytest.mark.parametrize("key", ["database_url", "application_database_url"])
@pytest.mark.parametrize("value", [None, ""])
def test_removed_application_database_url_ignored_when_empty(key, value):
    """A key left null meant "no application database" in 2.x and resolved exactly
    as omitting it does now, so DBOSConfig must not reject it either. The 2.x starter
    template produced one whenever ${DBOS_DATABASE_URL} was unset."""
    translated = translate_dbos_config_to_config_file({"name": "some-app", key: value})
    assert process_config(data=translated)["system_database_url"] == (
        "sqlite:///some_app.sqlite"
    )


####################
# CONFIG OVERWRITE
####################


CLOUD_SYS_DB_URL = "postgres://dbosadmin:pwd@hostname:1234/appdbname_dbos_sys?connect_timeout=10000&sslmode=require&sslrootcert=cert.pem"


@pytest.fixture()
def cloud_env(mocker, monkeypatch: pytest.MonkeyPatch) -> None:
    # The variables DBOS Cloud exports; the config file must never be read.
    monkeypatch.setenv("DBOS_APP_NAME", "stock-prices")
    monkeypatch.setenv("DBOS_SYSTEM_DATABASE_URL", CLOUD_SYS_DB_URL)
    monkeypatch.setenv("DBOS__OTLP_TRACES_ENDPOINT", "thetracesendpoint")
    monkeypatch.setenv("DBOS__OTLP_LOGS_ENDPOINT", "thelogsendpoint")
    mocker.patch(
        "dbos._dbos_config.load_config",
        side_effect=AssertionError("overwrite_config must not read dbos-config.yaml"),
    )


def test_overwrite_config(cloud_env: None) -> None:
    provided_config: ConfigFile = {
        "name": "test-app",
        "database": {},
        "telemetry": {
            "OTLPExporter": {
                "tracesEndpoint": ["a"],
                "logsEndpoint": ["b"],
            },
            "logs": {
                "logLevel": "DEBUG",
            },
        },
    }

    config = overwrite_config(provided_config)

    assert config["name"] == "stock-prices"
    assert config["system_database_url"] == CLOUD_SYS_DB_URL
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
    assert config["telemetry"]["disable_otlp"] == False


def test_overwrite_config_minimal(cloud_env: None) -> None:
    provided_config: ConfigFile = {
        "name": "test-app",
        "dbos_system_schema": "foobar",
    }

    config = overwrite_config(provided_config)

    assert config["name"] == "stock-prices"
    assert config["system_database_url"] == CLOUD_SYS_DB_URL
    assert config["dbos_system_schema"] == "dbos"
    assert config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "thetracesendpoint"
    ]
    assert config["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["thelogsendpoint"]
    assert "runtimeConfig" not in config


def test_overwrite_config_has_telemetry(cloud_env: None) -> None:
    provided_config: ConfigFile = {
        "name": "test-app",
        "telemetry": {"logs": {"logLevel": "DEBUG"}},
    }

    config = overwrite_config(provided_config)

    assert config["name"] == "stock-prices"
    assert config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == [
        "thetracesendpoint"
    ]
    assert config["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["thelogsendpoint"]
    assert config["telemetry"]["logs"]["logLevel"] == "DEBUG"


# Not expected in practice, but exercise the code path
def test_overwrite_config_no_otlp_env(
    cloud_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DBOS__OTLP_TRACES_ENDPOINT")
    monkeypatch.delenv("DBOS__OTLP_LOGS_ENDPOINT")

    # Telemetry from provided_config is preserved, with no endpoints added
    config = overwrite_config(
        {"name": "test-app", "telemetry": {"logs": {"logLevel": "DEBUG"}}}
    )
    assert config["telemetry"]["logs"]["logLevel"] == "DEBUG"
    assert config["telemetry"]["OTLPExporter"] == {
        "tracesEndpoint": [],
        "logsEndpoint": [],
    }

    config = overwrite_config(
        {
            "name": "test-app",
            "telemetry": {
                "OTLPExporter": {
                    "tracesEndpoint": ["original-trace"],
                    "logsEndpoint": ["original-log"],
                }
            },
        }
    )
    assert config["telemetry"]["OTLPExporter"]["tracesEndpoint"] == ["original-trace"]
    assert config["telemetry"]["OTLPExporter"]["logsEndpoint"] == ["original-log"]


def test_overwrite_config_with_provided_system_database_url(cloud_env: None) -> None:
    provided_config: ConfigFile = {
        "name": "test-app",
        "system_database_url": "ignored",
    }

    config = overwrite_config(provided_config)

    assert config["name"] == "stock-prices"
    assert config["system_database_url"] == CLOUD_SYS_DB_URL


def test_overwrite_config_missing_dbos_system_database_url(
    cloud_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DBOS_SYSTEM_DATABASE_URL")
    with pytest.raises(DBOSInitializationError) as exc_info:
        overwrite_config({"name": "test-app"})
    assert (
        "DBOS_SYSTEM_DATABASE_URL environment variable is not set. This is required to connect to the database."
        in str(exc_info.value)
    )


def test_overwrite_config_missing_dbos_app_name(
    cloud_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DBOS_APP_NAME")
    with pytest.raises(DBOSInitializationError) as exc_info:
        overwrite_config({"name": "test-app"})
    assert "DBOS_APP_NAME environment variable is not set" in str(exc_info.value)


####################
# DATABASES CONNECTION POOLS
####################

_SYS_DB_URL = (
    f"postgres://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'))}"
    "@localhost:5432/postgres_dbos_sys"
)


def test_configured_pool_default():
    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "system_database_url": _SYS_DB_URL,
    }

    dbos = DBOS(config=config)
    dbos.launch()
    assert dbos._sys_db.engine.pool._pool.maxsize == 20
    assert dbos._sys_db.engine.pool._timeout == 30
    assert dbos._sys_db.engine.pool._max_overflow == 0
    assert dbos._sys_db.engine.pool._pre_ping == True

    # force the release of connections so we can intercept on connect.
    sys_db_engine = dbos._sys_db.engine
    sys_db_engine.dispose()

    @event.listens_for(sys_db_engine, "connect")
    def inspect_connection(dbapi_connection, connection_record):
        connect_timeout = dbapi_connection.info.get_parameters()["connect_timeout"]
        assert connect_timeout == "10"

    with sys_db_engine.connect() as conn:
        pass

    dbos.destroy()


def test_configured_pool_user_provided():
    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "sys_db_pool_size": 43,
        "system_database_url": _SYS_DB_URL,
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
    assert dbos._sys_db.engine.pool._pool.maxsize == 43
    assert dbos._sys_db.engine.pool._timeout == 42
    assert dbos._sys_db.engine.pool._max_overflow == 27
    assert dbos._sys_db.engine.pool._pre_ping == True

    # force the release of connections so we can intercept on connect.
    sys_db_engine = dbos._sys_db.engine
    sys_db_engine.dispose()

    @event.listens_for(sys_db_engine, "connect")
    def inspect_connection(dbapi_connection, connection_record):
        connect_timeout = dbapi_connection.info.get_parameters()["connect_timeout"]
        assert connect_timeout == "7"

    with sys_db_engine.connect() as conn:
        pass

    dbos.destroy()


def test_configured_pool_user_provided_dburl_connect_timeout():
    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "sys_db_pool_size": 43,
        "system_database_url": f"{_SYS_DB_URL}?connect_timeout=22",
        "db_engine_kwargs": {
            "pool_size": 22,
            "pool_timeout": 42,
            "max_overflow": 27,
            "pool_pre_ping": True,
        },
    }

    dbos = DBOS(config=config)
    dbos.launch()
    assert dbos._sys_db.engine.pool._pool.maxsize == 43
    assert dbos._sys_db.engine.pool._timeout == 42
    assert dbos._sys_db.engine.pool._max_overflow == 27
    assert dbos._sys_db.engine.pool._pre_ping == True

    # force the release of connections so we can intercept on connect.
    sys_db_engine = dbos._sys_db.engine
    sys_db_engine.dispose()

    @event.listens_for(sys_db_engine, "connect")
    def inspect_connection(dbapi_connection, connection_record):
        connect_timeout = dbapi_connection.info.get_parameters()["connect_timeout"]
        assert connect_timeout == "22"

    with sys_db_engine.connect() as conn:
        pass

    dbos.destroy()


def test_configured_pool_user_provided_dburl_connect_timeout_precedence():
    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "sys_db_pool_size": 43,
        # connect_args will take precedence
        "system_database_url": f"{_SYS_DB_URL}?connect_timeout=22",
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
    assert dbos._sys_db.engine.pool._pool.maxsize == 43
    assert dbos._sys_db.engine.pool._timeout == 42
    assert dbos._sys_db.engine.pool._max_overflow == 27
    assert dbos._sys_db.engine.pool._pre_ping == True

    # force the release of connections so we can intercept on connect.
    sys_db_engine = dbos._sys_db.engine
    sys_db_engine.dispose()

    @event.listens_for(sys_db_engine, "connect")
    def inspect_connection(dbapi_connection, connection_record):
        connect_timeout = dbapi_connection.info.get_parameters()["connect_timeout"]
        assert connect_timeout == "7"

    with sys_db_engine.connect() as conn:
        pass

    dbos.destroy()


def test_pool_connection_times_out_by_default():
    import socket

    ipv4_addr = socket.gethostbyname("example.com")

    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "system_database_url": f"postgres://postgres:dbos@{ipv4_addr}/postgres_dbos_sys",
    }

    dbos = DBOS(config=config)
    with pytest.raises(OperationalError) as exc_info:
        dbos.launch()

    assert "timeout" in str(exc_info.value).lower()
    dbos.destroy()


def test_log_config(dbos: DBOS):
    DBOS.destroy(destroy_registry=True)
    config: DBOSConfig = {
        "name": "test-app",
        "log_level": "DEBUG",
        "console_log_level": "ERROR",
        "otlp_log_level": "INFO",
        "enable_otlp": False,
    }
    dbos = DBOS(config=config)
    DBOS.launch()
    assert any(
        h.level == 40 for h in dbos.logger.handlers
    )  # at least one handler at ERROR
    DBOS.destroy(destroy_registry=True)


def test_null_pool(dbos: DBOS, config: DBOSConfig):
    DBOS.destroy(destroy_registry=True)
    config["db_engine_kwargs"] = {"poolclass": NullPool}
    dbos = DBOS(config=config)
    DBOS.launch()
    assert isinstance(dbos._sys_db.engine.pool, NullPool)
    DBOS.destroy(destroy_registry=True)


def test_conductor_executor_metadata_valid():
    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "conductor_executor_metadata": {"region": "us-east-1", "instance": 42},
    }
    dbos = DBOS(config=config)
    assert dbos._conductor_executor_metadata == {"region": "us-east-1", "instance": 42}
    dbos.destroy()


def test_conductor_executor_metadata_not_serializable():
    DBOS.destroy()
    config: DBOSConfig = {
        "name": "test-app",
        "conductor_executor_metadata": {"bad": object()},
    }
    with pytest.raises(DBOSException):
        DBOS(config=config)
    DBOS.destroy()
