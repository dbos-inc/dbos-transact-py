"""
PostgreSQL client tests for direct SQL stored function calls.

This module tests the PostgreSQL stored functions `enqueue_workflow` and `send_message` 
directly via SQL calls, mirroring the test_client.py tests that use enqueue or send.
"""

import json
import math
import os
import runpy
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, TypedDict

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from dbos import DBOS, DBOSConfig, SetWorkflowID
from dbos._dbos import WorkflowHandle
from tests import client_collateral
from tests.client_collateral import send_test


class Person(TypedDict):
    first: str
    last: str
    age: int


def run_client_collateral() -> None:
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, "client_collateral.py")
    runpy.run_path(filename)


def _get_db_connection(config: DBOSConfig) -> sa.Engine:
    """Get database connection for direct SQL calls."""
    assert config["system_database_url"] is not None
    return sa.create_engine(config["system_database_url"])


def _get_schema_name(config: DBOSConfig) -> str:
    """Get schema name from DBOS config."""
    return config.get("dbos_system_schema", "dbos")


def _execute_sql(engine: sa.Engine, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """Execute SQL and return result."""
    with engine.begin() as conn:
        result = conn.execute(sa.text(sql), params or {})
        return result.fetchone()


def _get_workflow_status(engine: sa.Engine, schema: str, workflow_id: str) -> Optional[Dict[str, Any]]:
    """Get workflow status from database."""
    sql = f"""
    SELECT workflow_uuid, status, name, class_name, queue_name, 
           deduplication_id, priority, inputs, created_at, updated_at,
           application_version, output, error
    FROM "{schema}".workflow_status 
    WHERE workflow_uuid = :workflow_id
    """
    row = _execute_sql(engine, sql, {"workflow_id": workflow_id})
    if row:
        return {
            "workflow_uuid": row[0],
            "status": row[1], 
            "name": row[2],
            "class_name": row[3],
            "queue_name": row[4],
            "deduplication_id": row[5],
            "priority": row[6],
            "inputs": row[7],
            "created_at": row[8],
            "updated_at": row[9],
            "application_version": row[10],
            "output": row[11],
            "error": row[12]
        }
    return None


def _get_notifications(engine: sa.Engine, schema: str, destination_id: str) -> List[Dict[str, Any]]:
    """Get notifications for a workflow."""
    sql = f"""
    SELECT message_uuid, destination_uuid, topic, message, consumed
    FROM "{schema}".notifications 
    WHERE destination_uuid = :destination_id
    ORDER BY created_at_epoch_ms
    """
    with engine.begin() as conn:
        result = conn.execute(sa.text(sql), {"destination_id": destination_id})
        rows = result.fetchall()
    return [
        {
            "message_uuid": row[0],
            "destination_uuid": row[1],
            "topic": row[2],
            "message": row[3],
            "consumed": row[4]
        }
        for row in rows
    ]


def test_pgsql_enqueue_and_get_result(dbos: DBOS, config: DBOSConfig, skip_with_sqlite: None) -> None:
    """Test basic enqueue_workflow functionality using PostgreSQL stored function."""
    run_client_collateral()

    engine = _get_db_connection(config)
    schema = _get_schema_name(config)
    
    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    # Call enqueue_workflow stored function directly
    sql = f"""
    SELECT "{schema}".enqueue_workflow(
        workflow_name => 'enqueue_test',
        queue_name => 'test_queue',
        positional_args => ARRAY[:arg1, :arg2, :arg3]::json[],
        workflow_id => :wfid
    )
    """
    
    result = _execute_sql(engine, sql, {
        "arg1": json.dumps(42),
        "arg2": json.dumps("test"), 
        "arg3": json.dumps(johnDoe),
        "wfid": wfid
    })
    assert result is not None
    returned_wfid = result[0]
    assert returned_wfid == wfid

    # Wait for workflow completion and verify result
    handle = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'

    # Verify workflow status in database
    status = _get_workflow_status(engine, schema, wfid)
    assert status is not None
    assert status["status"] == "SUCCESS"
    assert status["name"] == "enqueue_test"
    # The output is stored as JSON in the database, so decode it for comparison
    assert json.loads(status["output"]) == result
    assert status["inputs"] is not None


def test_pgsql_enqueue_with_timeout(dbos: DBOS, config: DBOSConfig, skip_with_sqlite: None) -> None:
    """Test enqueue_workflow timeout functionality."""
    run_client_collateral()
    
    engine = _get_db_connection(config)
    schema = _get_schema_name(config)
    
    wfid = str(uuid.uuid4())
    
    # Enqueue blocked_workflow with timeout (in milliseconds)
    sql = f"""
    SELECT "{schema}".enqueue_workflow(
        workflow_name => 'blocked_workflow',
        queue_name => 'test_queue',
        workflow_id => :wfid,
        timeout_ms => 1000
    )
    """
    
    _execute_sql(engine, sql, {"wfid": wfid})
    
    # Verify workflow gets cancelled due to timeout
    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    
    with pytest.raises(Exception) as exc_info:
        handle.get_result()
    assert "was cancelled" in str(exc_info.value)


def test_pgsql_enqueue_appver_not_set(dbos: DBOS, config: DBOSConfig, skip_with_sqlite: None) -> None:
    """Test enqueue_workflow without explicit app version."""
    run_client_collateral()

    engine = _get_db_connection(config)
    schema = _get_schema_name(config)
    
    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    sql = f"""
    SELECT "{schema}".enqueue_workflow(
        workflow_name => 'enqueue_test',
        queue_name => 'test_queue',
        positional_args => ARRAY[:arg1, :arg2, :arg3]::json[],
        workflow_id => :wfid
    )
    """
    
    _execute_sql(engine, sql, {
        "arg1": json.dumps(42),
        "arg2": json.dumps("test"),
        "arg3": json.dumps(johnDoe),
        "wfid": wfid
    })

    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'

    wf_status = dbos.get_workflow_status(wfid)
    assert wf_status is not None
    assert wf_status.status == "SUCCESS"
    assert wf_status.name == "enqueue_test"
    assert wf_status.app_version == DBOS.application_version


def test_pgsql_enqueue_appver_set(dbos: DBOS, config: DBOSConfig, skip_with_sqlite: None) -> None:
    """Test enqueue_workflow with explicit app version."""
    run_client_collateral()

    engine = _get_db_connection(config)
    schema = _get_schema_name(config)
    
    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    sql = f"""
    SELECT "{schema}".enqueue_workflow(
        workflow_name => 'enqueue_test',
        queue_name => 'test_queue',
        positional_args => ARRAY[:arg1, :arg2, :arg3]::json[],
        workflow_id => :wfid,
        app_version => :app_version
    )
    """
    
    _execute_sql(engine, sql, {
        "arg1": json.dumps(42),
        "arg2": json.dumps("test"),
        "arg3": json.dumps(johnDoe),
        "wfid": wfid,
        "app_version": DBOS.application_version
    })

    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'

    wf_status = dbos.get_workflow_status(wfid)
    assert wf_status is not None
    assert wf_status.status == "SUCCESS"
    assert wf_status.name == "enqueue_test"
    assert wf_status.app_version == DBOS.application_version


def test_pgsql_enqueue_wrong_appver(dbos: DBOS, config: DBOSConfig, skip_with_sqlite: None) -> None:
    """Test enqueue_workflow with wrong app version."""
    run_client_collateral()

    engine = _get_db_connection(config)
    schema = _get_schema_name(config)
    
    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    sql = f"""
    SELECT "{schema}".enqueue_workflow(
        workflow_name => 'enqueue_test',
        queue_name => 'test_queue',
        positional_args => ARRAY[:arg1, :arg2, :arg3]::json[],
        workflow_id => :wfid,
        app_version => '0123456789abcdef'
    )
    """
    
    _execute_sql(engine, sql, {
        "arg1": json.dumps(42),
        "arg2": json.dumps("test"),
        "arg3": json.dumps(johnDoe),
        "wfid": wfid
    })

    time.sleep(5)

    wf_status = dbos.get_workflow_status(wfid)
    assert wf_status is not None
    assert wf_status.status == "ENQUEUED"
    assert wf_status.name == "enqueue_test"
    assert wf_status.app_version == "0123456789abcdef"


def test_pgsql_enqueue_idempotent(config: DBOSConfig, skip_with_sqlite: None) -> None:
    """Test enqueue_workflow idempotent behavior."""
    DBOS.destroy(destroy_registry=True)

    engine = _get_db_connection(config)
    schema = _get_schema_name(config)
    
    johnDoe: Person = {"first": "John", "last": "Doe", "age": 30}
    wfid = str(uuid.uuid4())

    sql = f"""
    SELECT "{schema}".enqueue_workflow(
        workflow_name => 'enqueue_test',
        queue_name => 'test_queue',
        positional_args => ARRAY[:arg1, :arg2, :arg3]::json[],
        workflow_id => :wfid
    )
    """
    
    params = {
        "arg1": json.dumps(42),
        "arg2": json.dumps("test"),
        "arg3": json.dumps(johnDoe),
        "wfid": wfid
    }
    
    # Enqueue twice with same workflow ID - should be idempotent
    _execute_sql(engine, sql, params)
    _execute_sql(engine, sql, params)

    DBOS(config=config)
    DBOS.launch()
    run_client_collateral()

    handle: WorkflowHandle[str] = DBOS.retrieve_workflow(wfid)
    result = handle.get_result()
    assert result == '42-test-{"first": "John", "last": "Doe", "age": 30}'


def test_pgsql_send_with_topic(dbos: DBOS, config: DBOSConfig, skip_with_sqlite: None) -> None:
    """Test send_message with topic using PostgreSQL stored function."""
    run_client_collateral()

    engine = _get_db_connection(config)
    schema = _get_schema_name(config)
    
    now = time.time_ns()
    wfid = str(uuid.uuid4())
    topic = f"test-topic-{now}"
    message = f"Hello, DBOS! {now}"

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(send_test, topic)

    # Use send_message stored function
    sql = f"""
    SELECT "{schema}".send_message(
        destination_id => :dest_id,
        message => :message,
        topic => :topic
    )
    """
    
    _execute_sql(engine, sql, {
        "dest_id": handle.get_workflow_id(),
        "message": json.dumps(message),
        "topic": topic
    })

    result = handle.get_result()
    assert result == message


def test_pgsql_send_no_topic(dbos: DBOS, config: DBOSConfig, skip_with_sqlite: None) -> None:
    """Test send_message without topic using PostgreSQL stored function."""
    run_client_collateral()

    engine = _get_db_connection(config)
    schema = _get_schema_name(config)
    
    now = time.time_ns()
    wfid = str(uuid.uuid4())
    message = f"Hello, DBOS! {now}"

    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(send_test)

    # Use send_message stored function without topic
    sql = f"""
    SELECT "{schema}".send_message(
        destination_id => :dest_id,
        message => :message
    )
    """
    
    _execute_sql(engine, sql, {
        "dest_id": handle.get_workflow_id(), 
        "message": json.dumps(message)
    })

    result = handle.get_result()
    assert result == message


def test_pgsql_send_idempotent(dbos: DBOS, config: DBOSConfig, skip_with_sqlite: None) -> None:
    """Test send_message idempotent behavior using PostgreSQL stored function."""
    run_client_collateral()

    engine = _get_db_connection(config)
    schema = _get_schema_name(config)
    
    now = math.floor(time.time())
    wfid = f"test-send-{now}"
    topic = f"test-topic-{now}"
    message = f"Hello, DBOS! {now}"
    message_id = f"test-message-{now}"

    # Start workflow manually
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(send_test, topic)

    # Send same message twice with same message_id
    sql = f"""
    SELECT "{schema}".send_message(
        destination_id => :dest_id,
        message => :message,
        topic => :topic,
        message_id => :message_id
    )
    """
    
    params = {
        "dest_id": wfid,
        "message": json.dumps(message),
        "topic": topic,
        "message_id": message_id
    }
    
    _execute_sql(engine, sql, params)
    _execute_sql(engine, sql, params)  # Should be deduplicated

    # Verify only one notification exists
    notifications = _get_notifications(engine, schema, wfid)
    assert len(notifications) == 1

    result = handle.get_result()
    assert result == message


def test_pgsql_enqueue_with_deduplication(dbos: DBOS, config: DBOSConfig, skip_with_sqlite: None) -> None:
    """Test enqueue_workflow deduplication functionality."""
    run_client_collateral()

    engine = _get_db_connection(config)
    schema = _get_schema_name(config)
    
    wfid = str(uuid.uuid4())
    dedup_id = f"dedup-{wfid}"

    sql = f"""
    SELECT "{schema}".enqueue_workflow(
        workflow_name => 'retrieve_test',
        queue_name => 'test_queue',
        positional_args => ARRAY[:arg1]::json[],
        workflow_id => :wfid,
        deduplication_id => :dedup_id
    )
    """
    
    # First enqueue should succeed
    _execute_sql(engine, sql, {
        "arg1": json.dumps("abc"),
        "wfid": wfid,
        "dedup_id": dedup_id
    })
    
    # Second enqueue with same dedup_id and wfid should be idempotent
    _execute_sql(engine, sql, {
        "arg1": json.dumps("def"),
        "wfid": wfid,
        "dedup_id": dedup_id
    })

    # Third enqueue with same dedup_id but different workflow ID should fail
    wfid2 = str(uuid.uuid4())
    with pytest.raises((IntegrityError, sa.exc.IntegrityError)):
        _execute_sql(engine, sql, {
            "arg1": json.dumps("def"),
            "wfid": wfid2,
            "dedup_id": dedup_id
        })

    # Verify workflow completes with first argument
    handle = DBOS.retrieve_workflow(wfid)
    assert handle.get_result() == "abc"


def test_pgsql_enqueue_with_priority(dbos: DBOS, config: DBOSConfig, skip_with_sqlite: None) -> None:
    """Test enqueue_workflow priority parameter."""
    run_client_collateral()

    engine = _get_db_connection(config)
    schema = _get_schema_name(config)
    
    wfid = str(uuid.uuid4())
    priority = 5

    sql = f"""
    SELECT "{schema}".enqueue_workflow(
        workflow_name => 'retrieve_test',
        queue_name => 'inorder_queue',
        positional_args => ARRAY[:arg1]::json[],
        workflow_id => :wfid,
        priority => :priority
    )
    """
    
    _execute_sql(engine, sql, {
        "arg1": json.dumps("priority-test"),
        "wfid": wfid,
        "priority": priority
    })

    # Verify priority was set correctly in database
    status = _get_workflow_status(engine, schema, wfid) 
    assert status is not None
    assert status["priority"] == priority

    # Verify workflow completes
    handle = DBOS.retrieve_workflow(wfid)
    assert handle.get_result() == "priority-test"