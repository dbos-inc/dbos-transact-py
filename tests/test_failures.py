import asyncio
import threading
import time
import uuid
from typing import Any, Generator, cast

import pytest
import sqlalchemy as sa
from psycopg.errors import SerializationFailure
from sqlalchemy.exc import DBAPIError, InvalidRequestError, OperationalError

from dbos import DBOS, SetWorkflowID
from dbos._client import DBOSClient
from dbos._context import DBOSContext, _set_local_dbos_context, get_local_dbos_context
from dbos._dbos import WorkflowHandle
from dbos._dbos_config import DBOSConfig
from dbos._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded,
    DBOSException,
    DBOSMaxStepRetriesExceeded,
    DBOSNotAuthorizedError,
    DBOSQueueDeduplicatedError,
    DBOSUnexpectedStepError,
    DBOSWorkflowConflictIDError,
    MaxRecoveryAttemptsExceededError,
)
from dbos._registrations import DEFAULT_MAX_RECOVERY_ATTEMPTS
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import DefaultSerializer, safe_deserialize
from dbos._sys_db import WorkflowStatusString
from dbos._sys_db_postgres import PostgresSystemDatabase

from .conftest import queue_entries_are_cleaned_up, retry_until_success, set_workflow_status


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
    system_database._cleanup_connections()

    # Wait for the connection to re-establish
    time.sleep(3)

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

    # Start a workflow that we will restart
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(dead_letter_workflow)
    handle.get_result()

    # Attempt to recover the blocked workflow the maximum number of times
    for i in range(max_recovery_attempts):
        set_workflow_status(dbos._sys_db, wfid, "PENDING")
        handles = DBOS._recover_pending_workflows()
        handles[0].get_result()
        assert recovery_count == i + 2

    # Verify an additional attempt (either through recovery or through a direct call) throws a DLQ error
    # and puts the workflow in the DLQ status.
    set_workflow_status(dbos._sys_db, wfid, "PENDING")
    DBOS._recover_pending_workflows()
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

    # Complete the resumed workflow
    event.set()
    assert handle.get_result() == resumed_handle.get_result() == None
    assert handle.get_status().status == WorkflowStatusString.SUCCESS.value

    # Verify that retries of a completed workflow do not raise the DLQ exception
    for _ in range(max_recovery_attempts * 2):
        with SetWorkflowID(wfid):
            dead_letter_workflow()

    @DBOS.workflow(max_recovery_attempts=None)
    def infinite_dead_letter_workflow() -> None:
        return

    # Verify that a workflow with max_recovery_attempts=None is retried infinitely.
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(infinite_dead_letter_workflow)
        handle.get_result

    # Attempt to recover the blocked workflow the maximum number of times
    for i in range(DEFAULT_MAX_RECOVERY_ATTEMPTS * 2):
        handles = DBOS._recover_pending_workflows()
        for handle in handles:
            assert handle.get_result() == None


def test_nondeterministic_workflow(dbos: DBOS) -> None:
    flag = True

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

    # Start the workflow. It will complete step_one then wait.
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        non_deterministic_workflow()

    # To simulate nondeterminism, set the flag then restart the workflow w/ fork;
    flag = False
    handle_two = DBOS.fork_workflow(wfid, 2)

    # Due to the nondeterminism, the workflow should encounter an unexpected step.
    with pytest.raises(DBOSUnexpectedStepError) as exc_info:
        handle_two.get_result()


def test_nondeterministic_workflow_txn(dbos: DBOS) -> None:
    flag = True

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

    # Start the workflow. It will complete step_one then wait.
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        non_deterministic_workflow()

    # To simulate nondeterminism, set the flag then restart the workflow.
    flag = False
    handle_two = DBOS.fork_workflow(wfid, 2)

    # Due to the nondeterminism, the workflow should encounter an unexpected step.
    with pytest.raises(DBOSUnexpectedStepError) as exc_info:
        handle_two.get_result()


def test_step_retries(dbos: DBOS) -> None:
    step_counter = 0

    DBOS.register_queue("test-queue")
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
        DBOS.enqueue_workflow("test-queue", failing_step).get_result()

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
    handle = DBOS.enqueue_workflow("test-queue", failing_step)
    with pytest.raises(DBOSMaxStepRetriesExceeded) as excinfo:
        handle.get_result()
    assert error_message in str(excinfo.value)
    assert step_counter == max_attempts

    # Test enqueuing the workflow
    step_counter = 0
    handle = DBOS.enqueue_workflow("test-queue", failing_workflow)
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


def test_step_retries_no_final_sleep(dbos: DBOS) -> None:
    # Regression test for #667: the retry loop must not sleep after the
    # final failed attempt — otherwise DBOSMaxStepRetriesExceeded is delayed
    # by a full backoff interval for nothing.
    max_attempts = 3
    interval_seconds = 1.0
    backoff_rate = 2.0
    # Useful backoffs total interval * (1 + backoff_rate) = 3.0s; a wasted
    # final sleep would add another backoff_rate**(max_attempts-1) = 4.0s.
    expected_max_seconds = 3.5
    step_counter = 0

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=interval_seconds,
        backoff_rate=backoff_rate,
        max_attempts=max_attempts,
    )
    def failing_step() -> None:
        nonlocal step_counter
        step_counter += 1
        raise Exception("fail")

    @DBOS.workflow()
    def failing_workflow() -> None:
        failing_step()

    start = time.monotonic()
    with pytest.raises(DBOSMaxStepRetriesExceeded):
        failing_workflow()
    elapsed = time.monotonic() - start
    assert step_counter == max_attempts
    assert elapsed < expected_max_seconds, (
        f"Retry loop took {elapsed:.2f}s; expected < {expected_max_seconds}s "
        "(a wasted final sleep would push this past 7s)"
    )


@pytest.mark.asyncio
async def test_step_retries_no_final_sleep_async(dbos: DBOS) -> None:
    # Async variant of the #667 regression test (covers Pending._retry).
    max_attempts = 3
    interval_seconds = 1.0
    backoff_rate = 2.0
    expected_max_seconds = 3.5
    step_counter = 0

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=interval_seconds,
        backoff_rate=backoff_rate,
        max_attempts=max_attempts,
    )
    async def failing_step_async() -> None:
        nonlocal step_counter
        step_counter += 1
        raise Exception("fail")

    @DBOS.workflow()
    async def failing_workflow_async() -> None:
        await failing_step_async()

    start = time.monotonic()
    with pytest.raises(DBOSMaxStepRetriesExceeded):
        await failing_workflow_async()
    elapsed = time.monotonic() - start
    assert step_counter == max_attempts
    assert elapsed < expected_max_seconds, (
        f"Retry loop took {elapsed:.2f}s; expected < {expected_max_seconds}s "
        "(a wasted final sleep would push this past 7s)"
    )


class ShouldRetryFatalError(Exception):
    pass


class ShouldRetryRetryableError(Exception):
    pass


def test_step_should_retry(dbos: DBOS) -> None:
    # A step that fails with a non-retryable exception should bail out on the
    # first failure instead of exhausting retries.
    step_counter = 0
    max_attempts = 4

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=0,
        max_attempts=max_attempts,
        should_retry=lambda e: not isinstance(e, ShouldRetryFatalError),
    )
    def fatal_step() -> None:
        nonlocal step_counter
        step_counter += 1
        raise ShouldRetryFatalError("no retry")

    @DBOS.workflow()
    def fatal_workflow() -> None:
        fatal_step()

    # should_retry returns False -> the original exception propagates, not
    # DBOSMaxStepRetriesExceeded.
    with pytest.raises(ShouldRetryFatalError, match="no retry"):
        fatal_workflow()
    assert step_counter == 1

    # Sanity check: a retryable exception exhausts max_attempts as usual.
    retryable_counter = 0

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=0,
        max_attempts=max_attempts,
        should_retry=lambda e: not isinstance(e, ShouldRetryFatalError),
    )
    def retryable_step() -> None:
        nonlocal retryable_counter
        retryable_counter += 1
        raise ShouldRetryRetryableError("retry")

    @DBOS.workflow()
    def retryable_workflow() -> None:
        retryable_step()

    with pytest.raises(DBOSMaxStepRetriesExceeded):
        retryable_workflow()
    assert retryable_counter == max_attempts


def test_run_step_should_retry(dbos: DBOS) -> None:
    # DBOS.run_step should honor should_retry passed via StepOptions.
    sync_counter = 0
    async_counter = 0

    def fatal_step() -> None:
        nonlocal sync_counter
        sync_counter += 1
        raise ShouldRetryFatalError("no retry sync")

    async def fatal_step_async() -> None:
        nonlocal async_counter
        async_counter += 1
        raise ShouldRetryFatalError("no retry async")

    @DBOS.workflow()
    def run_step_wf() -> None:
        with pytest.raises(ShouldRetryFatalError, match="no retry sync"):
            DBOS.run_step(
                {
                    "retries_allowed": True,
                    "max_attempts": 3,
                    "interval_seconds": 0,
                    "should_retry": lambda e: not isinstance(e, ShouldRetryFatalError),
                },
                fatal_step,
            )
        with pytest.raises(ShouldRetryFatalError, match="no retry async"):
            DBOS.run_step(
                {
                    "retries_allowed": True,
                    "max_attempts": 3,
                    "interval_seconds": 0,
                    "should_retry": lambda e: not isinstance(e, ShouldRetryFatalError),
                },
                fatal_step_async,
            )

    run_step_wf()
    # Each inner step should have run exactly once — no retries.
    assert sync_counter == 1
    assert async_counter == 1


@pytest.mark.asyncio
async def test_run_step_async_should_retry(dbos: DBOS) -> None:
    # DBOS.run_step_async should honor should_retry passed via StepOptions.
    counter = 0

    async def fatal_step_async() -> None:
        nonlocal counter
        counter += 1
        raise ShouldRetryFatalError("no retry")

    @DBOS.workflow()
    async def run_step_async_wf() -> None:
        with pytest.raises(ShouldRetryFatalError, match="no retry"):
            await DBOS.run_step_async(
                {
                    "retries_allowed": True,
                    "max_attempts": 3,
                    "interval_seconds": 0,
                    "should_retry": lambda e: not isinstance(e, ShouldRetryFatalError),
                },
                fatal_step_async,
            )

    await run_step_async_wf()
    assert counter == 1


@pytest.mark.asyncio
async def test_step_should_retry_async_validator(dbos: DBOS) -> None:
    # Async step paired with an async validator: the validator is awaited,
    # and returning False short-circuits retries.
    step_counter = 0
    max_attempts = 4

    async def is_retryable(e: BaseException) -> bool:
        await asyncio.sleep(0)  # actually async
        return not isinstance(e, ShouldRetryFatalError)

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=0,
        max_attempts=max_attempts,
        should_retry=is_retryable,
    )
    async def fatal_step_async() -> None:
        nonlocal step_counter
        step_counter += 1
        raise ShouldRetryFatalError("no retry")

    @DBOS.workflow()
    async def wf() -> None:
        await fatal_step_async()

    with pytest.raises(ShouldRetryFatalError, match="no retry"):
        await wf()
    assert step_counter == 1

    # And an async validator that returns True still exhausts retries.
    retry_counter = 0

    async def always_retry(e: BaseException) -> bool:
        return True

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=0,
        max_attempts=max_attempts,
        should_retry=always_retry,
    )
    async def retry_step_async() -> None:
        nonlocal retry_counter
        retry_counter += 1
        raise Exception("retry")

    @DBOS.workflow()
    async def retry_wf() -> None:
        await retry_step_async()

    with pytest.raises(DBOSMaxStepRetriesExceeded):
        await retry_wf()
    assert retry_counter == max_attempts

    # DBOS.run_step_async with an async validator: short-circuits on False.
    run_step_counter = 0

    async def run_step_fatal() -> None:
        nonlocal run_step_counter
        run_step_counter += 1
        raise ShouldRetryFatalError("no retry run_step")

    @DBOS.workflow()
    async def run_step_wf() -> None:
        await DBOS.run_step_async(
            {
                "retries_allowed": True,
                "max_attempts": max_attempts,
                "interval_seconds": 0,
                "should_retry": is_retryable,
            },
            run_step_fatal,
        )

    with pytest.raises(ShouldRetryFatalError, match="no retry run_step"):
        await run_step_wf()
    assert run_step_counter == 1


def test_step_should_retry_sync_step_async_validator_rejected(dbos: DBOS) -> None:
    # A sync step paired with an async validator should be rejected when
    # invoked as a step, since the coroutine can't be awaited from a sync context.
    async def is_retryable(e: BaseException) -> bool:
        return True

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=0,
        max_attempts=2,
        should_retry=is_retryable,
    )
    def bad_step() -> None:
        raise Exception("boom")

    @DBOS.workflow()
    def wf() -> None:
        bad_step()

    with pytest.raises(Exception, match="sync but should_retry is async"):
        wf()

    def bad_run_step() -> None:
        raise Exception("boom")

    @DBOS.workflow()
    def run_step_wf() -> None:
        DBOS.run_step(
            {
                "retries_allowed": True,
                "max_attempts": 2,
                "interval_seconds": 0,
                "should_retry": is_retryable,
            },
            bad_run_step,
        )

    with pytest.raises(Exception, match="sync but should_retry is async"):
        run_step_wf()


def test_step_should_retry_on_last_attempt(dbos: DBOS) -> None:
    # If should_retry returns False on the final attempt, the caught exception
    # should propagate instead of being wrapped as DBOSMaxStepRetriesExceeded.
    step_counter = 0
    max_attempts = 3

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=0,
        max_attempts=max_attempts,
        should_retry=lambda e: not isinstance(e, ShouldRetryFatalError),
    )
    def mixed_step() -> None:
        nonlocal step_counter
        step_counter += 1
        if step_counter < max_attempts:
            raise Exception("retry me")
        raise ShouldRetryFatalError("stop now")

    @DBOS.workflow()
    def mixed_workflow() -> None:
        mixed_step()

    with pytest.raises(ShouldRetryFatalError, match="stop now"):
        mixed_workflow()
    assert step_counter == max_attempts


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


class BadException(Exception):
    def __init__(self, one: int, two: int) -> None:
        super().__init__(f"Message: {one}, {two}")


def test_error_serialization() -> None:
    # Verify that each exception that can be thrown in a workflow
    # is serializable and deserializable
    # DBOSMaxStepRetriesExceeded
    serializer = DefaultSerializer()
    e: Exception = DBOSMaxStepRetriesExceeded("step", 1, [Exception()])
    d = serializer.deserialize(serializer.serialize(e))
    assert isinstance(d, DBOSMaxStepRetriesExceeded)
    assert str(d) == str(e)
    assert isinstance(d.errors[0], Exception)
    # DBOSNotAuthorizedError
    e = DBOSNotAuthorizedError("no")
    d = serializer.deserialize(serializer.serialize(e))
    assert isinstance(d, DBOSNotAuthorizedError)
    assert str(d) == str(e)
    # DBOSQueueDeduplicatedError
    e = DBOSQueueDeduplicatedError("id", "queue", "dedup")
    d = serializer.deserialize(serializer.serialize(e))
    assert isinstance(d, DBOSQueueDeduplicatedError)
    assert str(d) == str(e)
    # AwaitedWorkflowCancelledError
    e = DBOSAwaitedWorkflowCancelledError("id")
    d = serializer.deserialize(serializer.serialize(e))
    assert isinstance(d, DBOSAwaitedWorkflowCancelledError)
    assert str(d) == str(e)

    # Test safe_deserialize

    bad_exception = BadException(1, 2)
    with pytest.raises(TypeError):
        serializer.deserialize(serializer.serialize(bad_exception))
    input, output, exception = safe_deserialize(
        serializer,
        serializer.name(),
        "my_id",
        serialized_input=None,
        serialized_exception=serializer.serialize(bad_exception),
        serialized_output=None,
    )
    assert input is None
    assert output is None
    assert isinstance(exception, str)


def test_workflow_error_serialization(dbos: DBOS, client: DBOSClient) -> None:

    @DBOS.step()
    def step() -> None:
        raise BadException(1, 2)

    @DBOS.workflow()
    def workflow() -> None:
        step()

    handle = DBOS.start_workflow(workflow)

    with pytest.raises(BadException):
        handle.get_result()

    workflows = DBOS.list_workflows()
    assert len(workflows) == 1
    assert workflows[0].error is not None

    steps = DBOS.list_workflow_steps(handle.workflow_id)
    assert len(steps) == 1
    assert steps[0]["error"] is not None

    status = handle.get_status()
    assert status.error is not None

    status = client.retrieve_workflow(handle.workflow_id).get_status()
    assert status.error is not None


def test_nonserializable_return(dbos: DBOS) -> None:
    @DBOS.step()
    def step() -> Generator[str, Any, None]:
        yield "val"

    @DBOS.workflow()
    def workflow() -> None:
        step()

    with pytest.raises(TypeError):
        workflow()


def test_recovery_attempts(dbos: DBOS, config: DBOSConfig) -> None:

    config["application_version"] = "0.0.1"
    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.launch()

    event = threading.Event()

    child_id = str(uuid.uuid4())

    @DBOS.workflow(max_recovery_attempts=1)
    def child_workflow() -> None:
        event.wait()

    @DBOS.workflow(max_recovery_attempts=2)
    def parent_workflow() -> None:
        with SetWorkflowID(child_id):
            child_workflow()
        event.wait()

    DBOS.register_queue("test_queue", polling_interval_sec=0.1)

    parent_handle = DBOS.enqueue_workflow("test_queue", parent_workflow)
    child_handle: WorkflowHandle[None] = DBOS.retrieve_workflow(
        child_id, existing_workflow=False
    )

    def check_attempt_1() -> None:
        assert parent_handle.get_status().recovery_attempts == 1
        assert child_handle.get_status().recovery_attempts == 1

    retry_until_success(check_attempt_1)

    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.launch()

    def check_attempt_2() -> None:
        assert parent_handle.get_status().recovery_attempts == 2
        assert child_handle.get_status().recovery_attempts == 2

    retry_until_success(check_attempt_2)

    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.launch()

    def check_attempt_3() -> None:
        assert parent_handle.get_status().recovery_attempts == 3
        assert child_handle.get_status().recovery_attempts == 3
        assert (
            child_handle.get_status().status
            == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value
        )
        assert parent_handle.get_status().status == WorkflowStatusString.ERROR.value
        assert isinstance(
            parent_handle.get_status().error,
            DBOSAwaitedWorkflowMaxRecoveryAttemptsExceeded,
        )

    retry_until_success(check_attempt_3)
    event.set()
    DBOS.destroy(destroy_registry=True)


def test_get_result_no_hang_on_connection_invalidated_error(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """Test that a DBAPIError doesn't poison the parent workflow's get_result()
    (check_workflow_result is wrapped in db_retry, so if we just rethrow an error that contains
    a db retryable error from the child, get_result() is stuck retrying forever
    """
    # Dedicated engine so reproducing the timeout doesn't disturb the sys-db pool.
    boom_engine = sa.create_engine(dbos._sys_db.engine.url)

    @DBOS.step()
    def boom_step() -> None:
        with boom_engine.begin() as c:
            c.execute(sa.text("SET LOCAL idle_in_transaction_session_timeout = '400'"))
            c.execute(sa.text("SELECT 1"))
            time.sleep(1.0)  # leave the transaction idle past the timeout
            c.execute(sa.text("SELECT 1"))  # -> InternalError, connection_invalidated

    @DBOS.workflow()
    def child() -> None:
        boom_step()

    handle: WorkflowHandle[None] = DBOS.start_workflow(child)

    with pytest.raises(DBAPIError):
        handle.get_result()

    poll_handle: WorkflowHandle[None] = DBOS.retrieve_workflow(handle.workflow_id)
    outcome: dict[str, Any] = {}

    def retrieve() -> None:
        try:
            poll_handle.get_result()
            outcome["returned"] = True
        except Exception as e:
            outcome["exc"] = e

    t = threading.Thread(target=retrieve, daemon=True)
    t.start()
    t.join(timeout=15)

    assert not t.is_alive(), (
        "get_result() hung: db_retry treated the child's stored "
        "connection-invalidated DBAPIError as a retriable connection failure"
    )
    exc = outcome.get("exc")
    assert isinstance(exc, DBAPIError), f"expected DBAPIError, got {outcome!r}"
    assert "idle-in-transaction" in str(exc)
    boom_engine.dispose()


def test_retriable_sqlite_exception() -> None:
    from sqlalchemy.exc import ResourceClosedError

    from dbos._utils import retriable_sqlite_exception

    # The pysqlite "INSERT ... RETURNING" cursor-invalidation flake is retriable
    assert retriable_sqlite_exception(
        ResourceClosedError(
            "This result object does not return rows. "
            "It has been closed automatically."
        )
    )
    # "database is locked" remains retriable
    assert retriable_sqlite_exception(Exception("database is locked"))
    # An unrelated ResourceClosedError is not retried (would otherwise loop forever)
    assert not retriable_sqlite_exception(
        ResourceClosedError("This Connection is closed")
    )
    # Unrelated errors are not retriable
    assert not retriable_sqlite_exception(Exception("syntax error"))


def _make_status_row(dbos: DBOS, workflow_id: str) -> None:
    """Run a trivial workflow under a fixed id so a workflow_status row exists
    (operation_outputs and notifications both FK to it)."""

    @DBOS.workflow()
    def _noop() -> None:
        return None

    with SetWorkflowID(workflow_id):
        _noop()


def test_recv_consume_idempotent_on_db_retry(dbos: DBOS) -> None:
    """recv_consume is wrapped in db_retry, which re-runs the whole body if the
    connection drops after the consume committed but before the result was
    acknowledged. The re-run must return the already-recorded message rather
    than consume a *second* message (or raise a spurious conflict from
    re-recording the result at the same function_id)."""
    dest_id = str(uuid.uuid4())
    _make_status_row(dbos, dest_id)

    DBOS.send(dest_id, "msg1")
    DBOS.send(dest_id, "msg2")

    function_id = 1
    start_time = int(time.time() * 1000)

    # Which of the two messages is consumed first depends on created_at_epoch_ms
    # ordering, which can tie under coarse timestamp resolution -- the
    # idempotency property holds for either, so don't assume an order.
    first = dbos._sys_db.recv_consume(dest_id, function_id, None, start_time)
    assert first in ("msg1", "msg2")

    # A db_retry re-run re-invokes recv_consume with identical arguments. It must
    # return the same already-recorded message, not consume the other one.
    replay = dbos._sys_db.recv_consume(dest_id, function_id, None, start_time)
    assert replay == first

    # The replay must not have consumed the second message.
    with dbos._sys_db.engine.connect() as c:
        unconsumed = c.execute(
            sa.select(SystemSchema.notifications.c.message).where(
                SystemSchema.notifications.c.destination_uuid == dest_id,
                SystemSchema.notifications.c.consumed == False,
            )
        ).fetchall()
    assert len(unconsumed) == 1


def test_recv_consume_idempotent_on_timeout(dbos: DBOS) -> None:
    """The idempotency path must also round-trip the no-message (timeout) case,
    where the recorded output is a serialized None rather than NULL."""
    dest_id = str(uuid.uuid4())
    _make_status_row(dbos, dest_id)

    function_id = 1
    start_time = int(time.time() * 1000)

    # No message available: records None as the durable result.
    first = dbos._sys_db.recv_consume(dest_id, function_id, None, start_time)
    assert first is None

    # Re-run returns the recorded None, not a freshly consumed message even if
    # one has since arrived.
    DBOS.send(dest_id, "late")
    replay = dbos._sys_db.recv_consume(dest_id, function_id, None, start_time)
    assert replay is None

    with dbos._sys_db.engine.connect() as c:
        unconsumed = c.execute(
            sa.select(SystemSchema.notifications.c.message).where(
                SystemSchema.notifications.c.destination_uuid == dest_id,
                SystemSchema.notifications.c.consumed == False,
            )
        ).fetchall()
    assert len(unconsumed) == 1


def test_record_child_workflow_idempotent_on_db_retry(dbos: DBOS) -> None:
    """record_child_workflow is wrapped in db_retry. If a prior attempt
    committed and the connection then dropped, the re-run hits a unique
    violation; recording the *same* child is an idempotent replay (return),
    while a *different* child is real nondeterminism (conflict)."""
    parent_id = str(uuid.uuid4())
    _make_status_row(dbos, parent_id)

    child_id = str(uuid.uuid4())
    function_id = 1
    function_name = "test_child"

    dbos._sys_db.record_child_workflow(parent_id, child_id, function_id, function_name)

    # Re-recording the same child at the same function_id is idempotent.
    dbos._sys_db.record_child_workflow(parent_id, child_id, function_id, function_name)

    # A different child at the same function_id is a genuine conflict.
    with pytest.raises(DBOSWorkflowConflictIDError):
        dbos._sys_db.record_child_workflow(
            parent_id, str(uuid.uuid4()), function_id, function_name
        )

    # An empty child id is rejected loudly rather than silently wedging recovery.
    with pytest.raises(DBOSException):
        dbos._sys_db.record_child_workflow(parent_id, "", 2, function_name)


class _RetryOnceEngine:
    """Engine proxy whose first begin() on the test's own thread raises a
    retriable error (a simulated dropped connection), forcing the surrounding
    db_retry loop to re-run its body exactly once. Calls from other threads
    (background pollers) and all later calls delegate to the real engine."""

    def __init__(self, real: Any, thread_id: int) -> None:
        self._real = real
        self._thread_id = thread_id
        self.fired = False

    def begin(self) -> Any:
        if not self.fired and threading.get_ident() == self._thread_id:
            self.fired = True
            raise Exception("database is locked")  # retriable on pg and sqlite
        return self._real.begin()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


def test_record_get_result_increments_function_id_once_on_db_retry(
    dbos: DBOS,
) -> None:
    """record_get_result increments ctx.function_id once as part of recording
    the get_result step. The increment must happen outside the db_retry loop:
    if the DB write retries, the function_id must not advance a second time, or
    the workflow's step sequence desyncs from its recorded history."""
    workflow_id = str(uuid.uuid4())
    _make_status_row(dbos, workflow_id)

    ctx = DBOSContext()
    ctx.workflow_id = workflow_id
    ctx.function_id = 0
    assert ctx.is_workflow()

    real_engine = dbos._sys_db.engine
    proxy = _RetryOnceEngine(real_engine, threading.get_ident())

    prev = get_local_dbos_context()
    _set_local_dbos_context(ctx)
    try:
        dbos._sys_db.engine = proxy  # type: ignore[assignment]
        dbos._sys_db.record_get_result(str(uuid.uuid4()), "some-output", None, None)
    finally:
        dbos._sys_db.engine = real_engine
        _set_local_dbos_context(prev)

    # The injected failure actually forced a retry...
    assert proxy.fired
    # ...but the function_id advanced exactly once (0 -> 1).
    assert ctx.function_id == 1

    # Exactly one get_result row was recorded, at that function_id.
    with dbos._sys_db.engine.connect() as c:
        rows = c.execute(
            sa.select(SystemSchema.operation_outputs.c.function_id).where(
                SystemSchema.operation_outputs.c.workflow_uuid == workflow_id,
                SystemSchema.operation_outputs.c.function_name == "DBOS.getResult",
            )
        ).fetchall()
    assert [r[0] for r in rows] == [1]
