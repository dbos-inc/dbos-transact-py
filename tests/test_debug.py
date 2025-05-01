import uuid

import pytest
import sqlalchemy as sa

from dbos import DBOS, DBOSConfig, SetWorkflowID
from dbos._dbos import _get_dbos_instance
from dbos._debug import PythonModule, parse_start_command
from dbos._schemas.system_database import SystemSchema


def test_parse_fast_api_command() -> None:
    command = "fastapi run app/main.py"
    actual = parse_start_command(command)
    assert isinstance(actual, PythonModule)
    assert actual.module_name == "main"


def test_parse_python_command() -> None:
    command = "python app/main.py"
    expected = "app/main.py"
    actual = parse_start_command(command)
    assert actual == expected


def test_parse_python3_command() -> None:
    command = "python3 app/main.py"
    expected = "app/main.py"
    actual = parse_start_command(command)
    assert actual == expected


def test_parse_python_module_command() -> None:
    command = "python -m some_module"
    actual = parse_start_command(command)
    assert isinstance(actual, PythonModule)
    assert actual.module_name == "some_module"


def test_parse_python3_module_command() -> None:
    command = "python3 -m some_module"
    actual = parse_start_command(command)
    assert isinstance(actual, PythonModule)
    assert actual.module_name == "some_module"


def get_recovery_attempts(wfuuid: str) -> int:
    dbos = _get_dbos_instance()
    with dbos._sys_db.engine.connect() as c:
        stmt = sa.select(
            SystemSchema.workflow_status.c.recovery_attempts,
            SystemSchema.workflow_status.c.created_at,
            SystemSchema.workflow_status.c.updated_at,
        ).where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        result = c.execute(stmt).fetchone()
        assert result is not None
        recovery_attempts, created_at, updated_at = result
        return int(recovery_attempts)


def test_wf_debug(dbos: DBOS, config: DBOSConfig) -> None:
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_step(var)
        DBOS.logger.info("I'm test_workflow")
        return res

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        handle = DBOS.start_workflow(test_workflow, "test")
        result = handle.get_result()
        assert result == "test"
        assert wf_counter == 1
        assert step_counter == 1

    expected_retry_attempts = get_recovery_attempts(wfuuid)

    DBOS.destroy()
    DBOS(config=config)
    DBOS.launch(debug_mode=True)

    handle = DBOS._execute_workflow_id(wfuuid)
    result = handle.get_result()
    assert result == "test"
    assert wf_counter == 2
    assert step_counter == 1

    actual_retry_attempts = get_recovery_attempts(wfuuid)
    assert actual_retry_attempts == expected_retry_attempts


def test_wf_debug_exception(dbos: DBOS, config: DBOSConfig) -> None:
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_step(var)
        DBOS.logger.info("I'm test_workflow")
        raise Exception("test_wf_debug_exception")

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        handle = DBOS.start_workflow(test_workflow, "test")
        with pytest.raises(Exception) as excinfo:
            handle.get_result()
        assert str(excinfo.value) == "test_wf_debug_exception"

        assert wf_counter == 1
        assert step_counter == 1

    expected_retry_attempts = get_recovery_attempts(wfuuid)

    DBOS.destroy()
    DBOS(config=config)
    DBOS.launch(debug_mode=True)

    handle = DBOS._execute_workflow_id(wfuuid)
    with pytest.raises(Exception) as excinfo:
        handle.get_result()
    assert str(excinfo.value) == "test_wf_debug_exception"
    assert wf_counter == 2
    assert step_counter == 1

    actual_retry_attempts = get_recovery_attempts(wfuuid)
    assert actual_retry_attempts == expected_retry_attempts
