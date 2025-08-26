import threading
import time
import uuid
from typing import cast

import pytest
import sqlalchemy as sa
from psycopg.errors import SerializationFailure
from sqlalchemy.exc import InvalidRequestError, OperationalError

from dbos import DBOS, Queue, SetWorkflowID
from dbos._dbos_config import DBOSConfig
from dbos._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSMaxStepRetriesExceeded,
    DBOSNotAuthorizedError,
    DBOSQueueDeduplicatedError,
    DBOSUnexpectedStepError,
    DBOSWorkflowFunctionNotFoundError,
    MaxRecoveryAttemptsExceededError,
)
from dbos._registrations import DEFAULT_MAX_RECOVERY_ATTEMPTS
from dbos._serialization import (
    deserialize_exception,
    safe_deserialize,
    serialize_exception,
)
from dbos._sys_db import WorkflowStatusString
from dbos._sys_db_postgres import PostgresSystemDatabase

from .conftest import queue_entries_are_cleaned_up


def test_transaction_errors(dbos: DBOS, skip_with_sqlite: None) -> None:
    retry_counter: int = 0

    @DBOS.transaction()
    def test_retry_transaction(max_retry: int) -> int:
        nonlocal retry_counter
        if retry_counter < max_retry:
            retry_counter += 1
            raise OperationalError(
                "Serialization test error", {}, SerializationFailure()
            )
        return max_retry

    @DBOS.transaction()
    def test_noretry_transaction() -> None:
        nonlocal retry_counter
        retry_counter += 1
        DBOS.sql_session.execute(sa.text("selct abc from c;")).fetchall()

    res = test_retry_transaction(10)
    assert res == 10
    assert retry_counter == 10

    with pytest.raises(Exception) as exc_info:
        test_noretry_transaction()
    assert exc_info.value.orig.sqlstate == "42601"  # type: ignore
    assert retry_counter == 11


def test_invalid_transaction_error(dbos: DBOS) -> None:
    commit_txn_counter: int = 0
    rollback_txn_counter: int = 0

    @DBOS.transaction()
    def test_commit_transaction() -> None:
        nonlocal commit_txn_counter
        commit_txn_counter += 1
        # Commit shouldn't be allowed to be called in a transaction. The error message should be clear.
        DBOS.sql_session.commit()
        return

    @DBOS.transaction()
    def test_abort_transaction() -> None:
        nonlocal rollback_txn_counter
        rollback_txn_counter += 1
        # Rollback shouldn't be allowed to be called in a transaction. The error message should be clear.
        DBOS.sql_session.rollback()
        return

    # Test OAOO and exception handling
    wfuuid = str(uuid.uuid4())
    with pytest.raises(InvalidRequestError) as exc_info:
        with SetWorkflowID(wfuuid):
            test_commit_transaction()
    assert "Can't operate on closed transaction inside context manager." in str(
        exc_info.value
    )
    print(exc_info.value)

    with pytest.raises(InvalidRequestError) as exc_info:
        with SetWorkflowID(wfuuid):
            test_commit_transaction()
    assert "Can't operate on closed transaction inside context manager." in str(
        exc_info.value
    )

    assert commit_txn_counter == 1

    wfuuid = str(uuid.uuid4())
    with pytest.raises(InvalidRequestError) as exc_info:
        with SetWorkflowID(wfuuid):
            test_abort_transaction()
    assert "Can't operate on closed transaction inside context manager." in str(
        exc_info.value
    )
    print(exc_info.value)

    with pytest.raises(InvalidRequestError) as exc_info:
        with SetWorkflowID(wfuuid):
            test_abort_transaction()
    assert "Can't operate on closed transaction inside context manager." in str(
        exc_info.value
    )
    assert rollback_txn_counter == 1


def test_notification_errors(dbos: DBOS, skip_with_sqlite: None) -> None:
    @DBOS.workflow()
    def test_send_workflow(dest_uuid: str, topic: str) -> str:
        DBOS.send(dest_uuid, "test1")
        DBOS.send(dest_uuid, "test2", topic=topic)
        DBOS.send(dest_uuid, "test3")
        return dest_uuid

    @DBOS.workflow()
    def test_recv_workflow(topic: str) -> str:
        msg1 = DBOS.recv(topic, timeout_seconds=10)
        msg2 = DBOS.recv(timeout_seconds=10)
        msg3 = DBOS.recv(timeout_seconds=10)
        return "-".join([str(msg1), str(msg2), str(msg3)])

    # Crash the notification connection and make sure send/recv works on time.
    system_database = cast(PostgresSystemDatabase, dbos._sys_db)
    while system_database.notification_conn is None:
        time.sleep(1)
    system_database.notification_conn.close()
    assert system_database.notification_conn.closed == 1

    # Wait for the connection to be re-established
    while system_database.notification_conn.closed != 0:
        time.sleep(1)

    dest_uuid = str("sruuid1")
    with SetWorkflowID(dest_uuid):
        handle = dbos.start_workflow(test_recv_workflow, "testtopic")
        assert handle.get_workflow_id() == dest_uuid

    send_uuid = str("sruuid2")
    with SetWorkflowID(send_uuid):
        res = test_send_workflow(handle.get_workflow_id(), "testtopic")
        assert res == dest_uuid

    begin_time = time.time()
    assert handle.get_result() == "test2-test1-test3"
    duration = time.time() - begin_time
    assert duration < 3.0


def test_dead_letter_queue(dbos: DBOS) -> None:
    event = threading.Event()
    max_recovery_attempts = 20
    recovery_count = 0

    @DBOS.workflow(max_recovery_attempts=max_recovery_attempts)
    def dead_letter_workflow() -> None:
        nonlocal recovery_count
        recovery_count += 1
        event.wait()

    # Start a workflow that blocks forever
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(dead_letter_workflow)

    # Attempt to recover the blocked workflow the maximum number of times
    for i in range(max_recovery_attempts):
        DBOS._recover_pending_workflows()
        assert recovery_count == i + 2

    # Verify an additional attempt (either through recovery or through a direct call) throws a DLQ error
    # and puts the workflow in the DLQ status.
    with pytest.raises(Exception) as exc_info:
        DBOS._recover_pending_workflows()
    assert exc_info.errisinstance(MaxRecoveryAttemptsExceededError)
    assert (
        handle.get_status().status
        == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value
    )
    with pytest.raises(Exception) as exc_info:
        with SetWorkflowID(wfid):
            dead_letter_workflow()
    assert exc_info.errisinstance(MaxRecoveryAttemptsExceededError)

    # Resume the workflow. Verify it can recover again without error.
    resumed_handle = dbos.resume_workflow(wfid)
    DBOS._recover_pending_workflows()

    # Complete the blocked workflow
    event.set()
    assert handle.get_result() == resumed_handle.get_result() == None
    assert handle.get_status().status == WorkflowStatusString.SUCCESS.value

    # Verify that retries of a completed workflow do not raise the DLQ exception
    for _ in range(max_recovery_attempts * 2):
        with SetWorkflowID(wfid):
            dead_letter_workflow()

    event.clear()

    @DBOS.workflow(max_recovery_attempts=None)
    def infinite_dead_letter_workflow() -> None:
        event.wait()

    # Verify that a workflow with max_recovery_attempts=None is retried infinitely.
    wfid = str(uuid.uuid4())
    handles = []
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(infinite_dead_letter_workflow)
        handles.append(handle)

    # Attempt to recover the blocked workflow the maximum number of times
    for i in range(DEFAULT_MAX_RECOVERY_ATTEMPTS * 2):
        handles.extend(DBOS._recover_pending_workflows())
    event.set()
    for handle in handles:
        assert handle.get_result() == None


def test_nondeterministic_workflow(dbos: DBOS) -> None:
    flag = True
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.step()
    def step_one() -> None:
        return

    @DBOS.step()
    def step_two() -> None:
        return

    @DBOS.workflow()
    def non_deterministic_workflow() -> None:
        if flag:
            step_one()
        else:
            step_two()
        main_thread_event.set()
        workflow_event.wait()

    # Start the workflow. It will complete step_one then wait.
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = dbos.start_workflow(non_deterministic_workflow)
    main_thread_event.wait()

    # To simulate nondeterminism, set the flag then restart the workflow.
    flag = False
    with SetWorkflowID(wfid):
        handle_two = dbos.start_workflow(non_deterministic_workflow)

    # Due to the nondeterminism, the workflow should encounter an unexpected step.
    with pytest.raises(DBOSUnexpectedStepError) as exc_info:
        handle_two.get_result()

    # The original workflow should complete successfully.
    workflow_event.set()
    assert handle.get_result() == None


def test_nondeterministic_workflow_txn(dbos: DBOS) -> None:
    flag = True
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.transaction()
    def txn_one() -> None:
        return

    @DBOS.transaction()
    def txn_two() -> None:
        return

    @DBOS.workflow()
    def non_deterministic_workflow() -> None:
        if flag:
            txn_one()
        else:
            txn_two()
        main_thread_event.set()
        workflow_event.wait()

    # Start the workflow. It will complete step_one then wait.
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = dbos.start_workflow(non_deterministic_workflow)
    main_thread_event.wait()

    # To simulate nondeterminism, set the flag then restart the workflow.
    flag = False
    with SetWorkflowID(wfid):
        handle_two = dbos.start_workflow(non_deterministic_workflow)

    # Due to the nondeterminism, the workflow should encounter an unexpected step.
    with pytest.raises(DBOSUnexpectedStepError) as exc_info:
        handle_two.get_result()

    # The original workflow should complete successfully.
    workflow_event.set()
    assert handle.get_result() == None


def test_step_retries(dbos: DBOS) -> None:
    step_counter = 0

    queue = Queue("test-queue")
    max_attempts = 2

    @DBOS.step(retries_allowed=True, interval_seconds=0, max_attempts=max_attempts)
    def failing_step() -> None:
        nonlocal step_counter
        step_counter += 1
        raise Exception("fail")

    @DBOS.workflow()
    def failing_workflow() -> None:
        failing_step()

    @DBOS.workflow()
    def enqueue_failing_step() -> None:
        queue.enqueue(failing_step).get_result()

    error_message = f"Step {failing_step.__qualname__} has exceeded its maximum of {max_attempts} retries"

    # Test calling the step directly
    with pytest.raises(Exception) as excinfo:
        failing_step()

    # Test calling the workflow
    step_counter = 0
    with pytest.raises(DBOSMaxStepRetriesExceeded) as excinfo:
        failing_workflow()
    assert error_message in str(excinfo.value)
    assert step_counter == max_attempts
    assert len(excinfo.value.errors) == max_attempts
    for error in excinfo.value.errors:
        assert isinstance(error, Exception)
        assert error
        assert "fail" in str(error)

    # Test enqueueing the step
    step_counter = 0
    handle = queue.enqueue(failing_step)
    with pytest.raises(DBOSMaxStepRetriesExceeded) as excinfo:
        handle.get_result()
    assert error_message in str(excinfo.value)
    assert step_counter == max_attempts

    # Test enqueuing the workflow
    step_counter = 0
    handle = queue.enqueue(failing_workflow)
    with pytest.raises(DBOSMaxStepRetriesExceeded) as excinfo:
        handle.get_result()
    assert error_message in str(excinfo.value)
    assert step_counter == max_attempts

    # Test enqueuing the step from a workflow
    step_counter = 0
    with pytest.raises(DBOSMaxStepRetriesExceeded) as excinfo:
        enqueue_failing_step()
    assert error_message in str(excinfo.value)
    assert step_counter == max_attempts

    assert queue_entries_are_cleaned_up(dbos)


def test_step_status(dbos: DBOS) -> None:
    step_counter = 0

    max_attempts = 5

    @DBOS.step(retries_allowed=True, interval_seconds=0, max_attempts=max_attempts)
    def failing_step() -> None:
        nonlocal step_counter
        step_status = DBOS.step_status
        assert step_status is not None
        assert step_status.step_id == 1
        assert step_status.current_attempt == step_counter
        assert step_status.max_attempts == max_attempts
        step_counter += 1
        if step_counter < max_attempts:
            raise Exception("fail")

    @DBOS.workflow()
    def failing_workflow() -> None:
        failing_step()

    assert failing_workflow() == None
    step_counter = 0


def test_recovery_during_retries(dbos: DBOS) -> None:
    step_counter = 0
    start_event = threading.Event()
    blocking_event = threading.Event()

    max_attempts = 3

    @DBOS.step(retries_allowed=True, interval_seconds=0, max_attempts=max_attempts)
    def failing_step() -> None:
        nonlocal step_counter
        step_counter += 1
        if step_counter < max_attempts:
            raise Exception("fail")
        else:
            start_event.set()
            blocking_event.wait()

    @DBOS.workflow()
    def failing_workflow() -> None:
        failing_step()

    handle = DBOS.start_workflow(failing_workflow)
    start_event.wait()
    recovery_handles = DBOS._recover_pending_workflows()
    assert len(recovery_handles) == 1
    blocking_event.set()
    assert handle.get_result() is None
    assert recovery_handles[0].get_result() is None


def test_keyboardinterrupt_during_retries(dbos: DBOS) -> None:
    # To test the issue raised in https://github.com/dbos-inc/dbos-transact-py/issues/260
    raise_interrupt = True

    max_attempts = 3

    @DBOS.step(retries_allowed=True, interval_seconds=0, max_attempts=max_attempts)
    def failing_step() -> None:
        if raise_interrupt:
            raise KeyboardInterrupt

    @DBOS.workflow()
    def failing_workflow() -> str:
        failing_step()
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        return workflow_id

    with pytest.raises(KeyboardInterrupt):
        failing_workflow()
    raise_interrupt = False
    recovery_handles = DBOS._recover_pending_workflows()
    assert len(recovery_handles) == 1
    assert recovery_handles[0].get_result() == recovery_handles[0].workflow_id


def test_error_serialization() -> None:
    # Verify that each exception that can be thrown in a workflow
    # is serializable and deserializable
    # DBOSMaxStepRetriesExceeded
    e: Exception = DBOSMaxStepRetriesExceeded("step", 1, [Exception()])
    d = deserialize_exception(serialize_exception(e))
    assert isinstance(d, DBOSMaxStepRetriesExceeded)
    assert str(d) == str(e)
    assert isinstance(d.errors[0], Exception)
    # DBOSNotAuthorizedError
    e = DBOSNotAuthorizedError("no")
    d = deserialize_exception(serialize_exception(e))
    assert isinstance(d, DBOSNotAuthorizedError)
    assert str(d) == str(e)
    # DBOSQueueDeduplicatedError
    e = DBOSQueueDeduplicatedError("id", "queue", "dedup")
    d = deserialize_exception(serialize_exception(e))
    assert isinstance(d, DBOSQueueDeduplicatedError)
    assert str(d) == str(e)
    # AwaitedWorkflowCancelledError
    e = DBOSAwaitedWorkflowCancelledError("id")
    d = deserialize_exception(serialize_exception(e))
    assert isinstance(d, DBOSAwaitedWorkflowCancelledError)
    assert str(d) == str(e)

    # Test safe_deserialize
    class BadException(Exception):
        def __init__(self, one: int, two: int) -> None:
            super().__init__(f"Message: {one}, {two}")

    bad_exception = BadException(1, 2)
    with pytest.raises(TypeError):
        deserialize_exception(serialize_exception(bad_exception))
    input, output, exception = safe_deserialize(
        "my_id",
        serialized_input=None,
        serialized_exception=serialize_exception(bad_exception),
        serialized_output=None,
    )
    assert input is None
    assert output is None
    assert isinstance(exception, str)
    assert "Message: 1, 2" in exception


def test_unregistered_workflow(dbos: DBOS, config: DBOSConfig) -> None:

    @DBOS.workflow()
    def workflow() -> None:
        return

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        workflow()

    dbos._sys_db.update_workflow_outcome(wfid, "PENDING")

    DBOS.destroy(destroy_registry=True)
    config["executor_id"] = str(uuid.uuid4())
    DBOS(config=config)
    DBOS.launch()

    with pytest.raises(DBOSWorkflowFunctionNotFoundError):
        DBOS._recover_pending_workflows()
