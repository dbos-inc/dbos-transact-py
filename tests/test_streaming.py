import time
import uuid
from typing import Any

import pytest

# Public API
from dbos import DBOS, SetWorkflowID
from dbos._client import DBOSClient


def test_basic_stream_write_read(dbos: DBOS) -> None:
    """Test basic stream write and read functionality."""
    test_values = ["hello", 42, {"key": "value"}, [1, 2, 3], None]
    stream_key = "test_stream"

    @DBOS.workflow()
    def writer_workflow() -> None:
        for value in test_values:
            DBOS.write_stream(stream_key, value)
        DBOS.close_stream(stream_key)

    # Start the writer workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_workflow()

    # Read the stream
    read_values = []
    for value in DBOS.read_stream(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values

    # Read the stream again, verify no changes
    read_values = []
    for value in DBOS.read_stream(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values


def test_unclosed_stream(dbos: DBOS) -> None:
    """Test that reading from a stream stops when the workflow terminates."""
    test_values = ["hello", 42, {"key": "value"}, [1, 2, 3], None]
    stream_key = "test_stream"

    @DBOS.workflow()
    def writer_workflow() -> None:
        for value in test_values:
            DBOS.write_stream(stream_key, value)

    @DBOS.workflow()
    def writer_workflow_error() -> None:
        for value in test_values:
            DBOS.write_stream(stream_key, value)
        raise Exception()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_workflow()

    read_values = []
    for value in DBOS.read_stream(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            writer_workflow_error()

    read_values = []
    for value in DBOS.read_stream(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values


def test_stream_concurrent_write_read(dbos: DBOS) -> None:
    """Test reading from a stream while it's being written to."""
    stream_key = "concurrent_stream"
    num_values = 10

    @DBOS.workflow()
    def writer_workflow() -> None:
        for i in range(num_values):
            DBOS.write_stream(stream_key, f"value_{i}")
            # Small delay to simulate real work
            DBOS.sleep(0.5)
        DBOS.close_stream(stream_key)

    # Start the writer workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        handle = DBOS.start_workflow(writer_workflow)

    # Start reading immediately (while writing)
    read_values = []
    start_time = time.time()

    for value in DBOS.read_stream(wfid, stream_key):
        read_values.append(value)
        # Ensure we're not waiting too long for each value
        assert time.time() - start_time < 30  # Safety timeout

    # Wait for writer to complete
    handle.get_result()

    # Verify all values were read
    expected_values = [f"value_{i}" for i in range(num_values)]
    assert read_values == expected_values


def test_stream_multiple_keys(dbos: DBOS) -> None:
    """Test multiple streams with different keys in the same workflow."""

    @DBOS.workflow()
    def multi_stream_workflow() -> None:
        # Write to stream A
        DBOS.write_stream("stream_a", "a1")
        DBOS.write_stream("stream_a", "a2")

        # Write to stream B
        DBOS.write_stream("stream_b", "b1")
        DBOS.write_stream("stream_b", "b2")
        DBOS.write_stream("stream_b", "b3")

        # Close both streams
        DBOS.close_stream("stream_a")
        DBOS.close_stream("stream_b")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        multi_stream_workflow()

    # Read stream A
    stream_a_values = list(DBOS.read_stream(wfid, "stream_a"))
    assert stream_a_values == ["a1", "a2"]

    # Read stream B
    stream_b_values = list(DBOS.read_stream(wfid, "stream_b"))
    assert stream_b_values == ["b1", "b2", "b3"]


def test_stream_empty_stream(dbos: DBOS) -> None:
    """Test reading from an empty stream (only close marker)."""

    @DBOS.workflow()
    def empty_stream_workflow() -> None:
        DBOS.close_stream("empty_stream")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        empty_stream_workflow()

    # Read the empty stream
    values = list(DBOS.read_stream(wfid, "empty_stream"))
    assert values == []


class CustomClass:
    def __init__(self, value: str):
        self.value = value

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, CustomClass) and self.value == other.value


def test_stream_serialization_types(dbos: DBOS) -> None:
    """Test that various data types are properly serialized/deserialized."""

    test_values = [
        "string",
        42,
        3.14,
        True,
        False,
        None,
        [1, 2, 3],
        {"nested": {"dict": "value"}},
        CustomClass("test"),
        (1, 2, 3),  # Tuple
        {1, 2, 3},  # Set
    ]

    @DBOS.workflow()
    def serialization_test_workflow() -> None:
        for value in test_values:
            DBOS.write_stream("serialize_test", value)
        DBOS.close_stream("serialize_test")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        serialization_test_workflow()

    # Read and verify
    read_values = list(DBOS.read_stream(wfid, "serialize_test"))

    # Note: Sets and tuples might be deserialized differently due to JSON serialization
    # So we'll check the values more carefully
    assert len(read_values) == len(test_values)

    for i, (original, read) in enumerate(zip(test_values, read_values)):
        if isinstance(original, CustomClass):
            assert isinstance(read, CustomClass)
            assert read.value == original.value
        elif isinstance(original, (set, tuple)):
            # These might be deserialized as lists
            assert list(original) == read or original == read
        else:
            assert read == original


def test_stream_error_cases(dbos: DBOS) -> None:
    """Test error cases and edge conditions."""

    # Test writing to stream outside of workflow
    with pytest.raises(Exception, match="must be called from within a workflow"):
        DBOS.write_stream("test", "value")

    # Test closing stream outside of workflow
    with pytest.raises(Exception, match="must be called from within a workflow"):
        DBOS.close_stream("test")


def test_stream_workflow_recovery(dbos: DBOS) -> None:
    """Test that stream operations are properly recovered during workflow replay."""

    call_count = 0

    @DBOS.step()
    def counting_step() -> int:
        nonlocal call_count
        call_count += 1
        return call_count

    @DBOS.workflow()
    def recovery_test_workflow() -> None:
        count1 = counting_step()
        DBOS.write_stream("recovery_stream", f"step_{count1}")

        count2 = counting_step()
        DBOS.write_stream("recovery_stream", f"step_{count2}")

        DBOS.close_stream("recovery_stream")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        recovery_test_workflow()

    # Reset call count and run the same workflow ID again (should replay)
    call_count = 0
    with SetWorkflowID(wfid):
        recovery_test_workflow()

    # The counting step should not have been called again (replayed from recorded results)
    assert call_count == 0

    # Stream should still be readable and contain the same values
    values = list(DBOS.read_stream(wfid, "recovery_stream"))
    assert values == ["step_1", "step_2"]

    steps = DBOS.list_workflow_steps(wfid)
    assert len(steps) == 5
    assert steps[1]["function_name"] == "DBOS.writeStream"
    assert steps[3]["function_name"] == "DBOS.writeStream"
    assert steps[4]["function_name"] == "DBOS.closeStream"


def test_stream_large_data(dbos: DBOS) -> None:
    """Test streaming with larger amounts of data."""

    @DBOS.workflow()
    def large_data_workflow() -> None:
        # Write 100 items
        for i in range(100):
            data = {"id": i, "data": f"item_{i}", "large_field": "x" * 1000}
            DBOS.write_stream("large_stream", data)
        DBOS.close_stream("large_stream")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        large_data_workflow()

    # Read all values
    values = list(DBOS.read_stream(wfid, "large_stream"))

    assert len(values) == 100
    for i, value in enumerate(values):
        assert value["id"] == i
        assert value["data"] == f"item_{i}"
        assert value["large_field"] == "x" * 1000


def test_stream_interleaved_operations(dbos: DBOS) -> None:
    """Test interleaved write operations across multiple streams."""

    @DBOS.workflow()
    def interleaved_workflow() -> None:
        DBOS.write_stream("stream1", "1a")
        DBOS.write_stream("stream2", "2a")
        DBOS.write_stream("stream1", "1b")
        DBOS.write_stream("stream3", "3a")
        DBOS.write_stream("stream2", "2b")
        DBOS.write_stream("stream1", "1c")

        DBOS.close_stream("stream1")
        DBOS.close_stream("stream2")
        DBOS.close_stream("stream3")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        interleaved_workflow()

    # Verify each stream has the correct values in order
    stream1_values = list(DBOS.read_stream(wfid, "stream1"))
    stream2_values = list(DBOS.read_stream(wfid, "stream2"))
    stream3_values = list(DBOS.read_stream(wfid, "stream3"))

    assert stream1_values == ["1a", "1b", "1c"]
    assert stream2_values == ["2a", "2b"]
    assert stream3_values == ["3a"]


def test_stream_write_from_step(dbos: DBOS) -> None:
    """Test writing to a stream from inside a step function that retries and throws exceptions."""

    call_count = 0

    @DBOS.step(retries_allowed=True, max_attempts=4, interval_seconds=0)
    def step_that_writes_and_fails(stream_key: str, value: Any) -> int:
        nonlocal call_count
        call_count += 1

        # Always write to stream first
        DBOS.write_stream(stream_key, f"{value}_attempt_{call_count}")

        # Throw exception to trigger retry (will succeed after 3 attempts)
        if call_count < 4:
            raise RuntimeError(f"Step failed on attempt {call_count}")

        step_id = DBOS.step_id
        assert step_id is not None
        return step_id

    @DBOS.workflow()
    def workflow_with_failing_step() -> None:
        # This step will fail 3 times, then succeed on the 4th attempt
        # But each failure should still write to the stream
        result = step_that_writes_and_fails("retry_stream", "test_value")
        assert result == 1

        # Also write directly from workflow
        DBOS.write_stream("retry_stream", "from_workflow")

        # Close the stream
        DBOS.close_stream("retry_stream")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        workflow_with_failing_step()

    # Read the stream and verify all values are present
    # Should have 4 writes from the step (one per attempt) plus 1 from workflow
    stream_values = list(DBOS.read_stream(wfid, "retry_stream"))

    # Verify we have the expected number of values
    assert len(stream_values) == 5

    # Verify the step writes (one per retry attempt)
    assert stream_values[0] == "test_value_attempt_1"
    assert stream_values[1] == "test_value_attempt_2"
    assert stream_values[2] == "test_value_attempt_3"
    assert stream_values[3] == "test_value_attempt_4"

    # Verify the workflow write
    assert stream_values[4] == "from_workflow"

    # Verify the step was called exactly 4 times (3 failures + 1 success)
    assert call_count == 4


@pytest.mark.asyncio
async def test_async_stream_basic_write_read(dbos: DBOS) -> None:
    """Test basic async stream write and read functionality."""
    test_values = [
        "async_hello",
        123,
        {"async_key": "async_value"},
        [10, 20, 30],
        None,
    ]
    stream_key = "async_test_stream"

    @DBOS.workflow()
    async def async_writer_workflow() -> None:
        for value in test_values:
            await DBOS.write_stream_async(stream_key, value)
        await DBOS.close_stream_async(stream_key)

    # Start the writer workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await async_writer_workflow()

    # Read the stream
    read_values = []
    async for value in DBOS.read_stream_async(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values


@pytest.mark.asyncio
async def test_async_stream_concurrent_write_read(dbos: DBOS) -> None:
    """Test async reading from a stream while it's being written to."""

    stream_key = "async_concurrent_stream"
    num_values = 5

    @DBOS.workflow()
    async def async_writer_workflow() -> None:
        for i in range(num_values):
            await DBOS.write_stream_async(stream_key, f"async_value_{i}")
            # Small delay to simulate real work
            await DBOS.sleep_async(0.5)
        await DBOS.close_stream_async(stream_key)

    # Start the writer workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        writer_handle = await DBOS.start_workflow_async(async_writer_workflow)

    # Start reading immediately (while writing)
    read_values = []
    start_time = time.time()

    async for value in DBOS.read_stream_async(wfid, stream_key):
        read_values.append(value)
        # Ensure we're not waiting too long for each value
        assert time.time() - start_time < 30  # Safety timeout

    # Wait for writer to complete
    await writer_handle.get_result()

    # Verify all values were read
    expected_values = [f"async_value_{i}" for i in range(num_values)]
    assert read_values == expected_values


@pytest.mark.asyncio
async def test_async_stream_empty_stream(dbos: DBOS) -> None:
    """Test async reading from an empty stream (only close marker)."""

    @DBOS.workflow()
    async def async_empty_stream_workflow() -> None:
        await DBOS.close_stream_async("async_empty_stream")

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await async_empty_stream_workflow()

    # Read the empty stream
    values = []
    async for value in DBOS.read_stream_async(wfid, "async_empty_stream"):
        values.append(value)
    assert values == []


@pytest.mark.asyncio
async def test_unclosed_stream_async(dbos: DBOS) -> None:
    """Test that reading from a stream stops when the workflow terminates."""
    test_values = ["hello", 42, {"key": "value"}, [1, 2, 3], None]
    stream_key = "test_stream"

    @DBOS.workflow()
    async def writer_workflow() -> None:
        for value in test_values:
            await DBOS.write_stream_async(stream_key, value)

    @DBOS.workflow()
    async def writer_workflow_error() -> None:
        for value in test_values:
            await DBOS.write_stream_async(stream_key, value)
        raise Exception()

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await writer_workflow()

    read_values = []
    async for value in DBOS.read_stream_async(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        with pytest.raises(Exception):
            await writer_workflow_error()

    read_values = []
    async for value in DBOS.read_stream_async(wfid, stream_key):
        read_values.append(value)

    assert read_values == test_values


def test_client_read_stream(dbos: DBOS, client: DBOSClient) -> None:
    """Test reading streams from a DBOS client."""
    test_values = [
        "client_hello",
        99,
        {"client_key": "client_value"},
        [100, 200, 300],
        None,
    ]
    stream_key = "client_test_stream"

    @DBOS.workflow()
    def client_writer_workflow() -> None:
        for value in test_values:
            DBOS.write_stream(stream_key, value)
        DBOS.close_stream(stream_key)

    # Start the writer workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        client_writer_workflow()

    # Create a client and read the stream
    try:
        read_values = []
        for value in client.read_stream(wfid, stream_key):
            read_values.append(value)

        assert read_values == test_values
    finally:
        client.destroy()


@pytest.mark.asyncio
async def test_client_read_stream_async(dbos: DBOS, client: DBOSClient) -> None:
    """Test async reading streams from a DBOS client."""
    test_values = [
        "async_client_hello",
        88,
        {"async_client_key": "async_client_value"},
        [11, 22, 33],
        None,
    ]
    stream_key = "async_client_test_stream"

    @DBOS.workflow()
    async def async_client_writer_workflow() -> None:
        for value in test_values:
            await DBOS.write_stream_async(stream_key, value)
        await DBOS.close_stream_async(stream_key)

    # Start the writer workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await async_client_writer_workflow()

    # Create a client and read the stream asynchronously
    try:
        read_values = []
        async for value in client.read_stream_async(wfid, stream_key):
            read_values.append(value)

        assert read_values == test_values
    finally:
        client.destroy()


def test_client_read_stream_workflow_termination(
    dbos: DBOS, client: DBOSClient
) -> None:
    """Test that client read_stream stops when workflow terminates without closing stream."""
    test_values = ["terminated_1", "terminated_2", "terminated_3"]
    stream_key = "termination_test_stream"

    @DBOS.workflow()
    def terminating_workflow() -> None:
        for value in test_values:
            DBOS.write_stream(stream_key, value)
        # Intentionally don't close the stream

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        terminating_workflow()

    # Create a client and read the stream - should stop when workflow terminates
    try:
        read_values = []
        for value in client.read_stream(wfid, stream_key):
            read_values.append(value)

        assert read_values == test_values
    finally:
        client.destroy()


@pytest.mark.asyncio
async def test_client_read_stream_async_workflow_termination(
    dbos: DBOS, client: DBOSClient
) -> None:
    """Test that client read_stream_async stops when workflow terminates without closing stream."""
    test_values = ["async_terminated_1", "async_terminated_2", "async_terminated_3"]
    stream_key = "async_termination_test_stream"

    @DBOS.workflow()
    async def async_terminating_workflow() -> None:
        for value in test_values:
            await DBOS.write_stream_async(stream_key, value)
        # Intentionally don't close the stream

    # Start the workflow
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        await async_terminating_workflow()

    # Create a client and read the stream asynchronously - should stop when workflow terminates
    try:
        read_values = []
        async for value in client.read_stream_async(wfid, stream_key):
            read_values.append(value)

        assert read_values == test_values
    finally:
        client.destroy()
