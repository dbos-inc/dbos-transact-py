"""Cross-language interoperability tests for portable workflow serialization.

These tests prove that portable-serialized DB records are identical across
Python, TypeScript, and Java by:
1. Running a canonical workflow and verifying DB records match golden JSON strings
2. Replaying from raw SQL inserts (proving cross-language compatibility)
3. Testing Python kwargs mapping to namedArgs

Corresponding tests in other languages:
  TypeScript: dbos-transact-ts/tests/interop.test.ts
  Java:       dbos-transact-java/transact/src/test/java/dev/dbos/transact/json/InteropTest.java
"""

import json
import time
import uuid
from typing import Any, Dict, List, Optional

import sqlalchemy as sa

from dbos import DBOS, Queue, WorkflowHandle, pydantic_args_validator
from dbos._client import DBOSClient
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import WorkflowSerializationFormat

# ============================================================================
# Golden JSON strings (byte-identical across Python, TypeScript, Java tests)
# ============================================================================

# Golden inputs (positionalArgs only, no namedArgs — the minimal portable form)
GOLDEN_INPUTS_JSON = (
    '{"positionalArgs":["hello-interop",42,"2025-06-15T10:30:00.000Z",'
    '["alpha","beta","gamma"],'
    '{"key1":"value1","key2":99,"nested":{"deep":true}},'
    "true,null]}"
)

GOLDEN_MESSAGE_JSON = '{"sender":"test","payload":[1,2,3]}'

# Golden output JSON — the exact string each language's portable serializer must produce.
# Key ordering matches the dict construction order in all three language implementations.
GOLDEN_OUTPUT_JSON = (
    '{"echo_text":"hello-interop","echo_num":42,'
    '"echo_dt":"2025-06-15T10:30:00.000Z",'
    '"items_count":3,"meta_keys":["key1","key2","nested"],'
    '"flag":true,"empty":null,'
    '"received":{"sender":"test","payload":[1,2,3]}}'
)

# Golden event value JSON
GOLDEN_EVENT_JSON = '{"text":"hello-interop","num":42,"flag":true}'

# Golden stream value JSON
GOLDEN_STREAM_JSON = '{"item":"hello-interop"}'

# Parsed forms (for programmatic result assertions)
EXPECTED_RESULT = json.loads(GOLDEN_OUTPUT_JSON)
EXPECTED_EVENT_VALUE = json.loads(GOLDEN_EVENT_JSON)
EXPECTED_STREAM_VALUE = json.loads(GOLDEN_STREAM_JSON)

# Canonical test input values
CANONICAL_TEXT = "hello-interop"
CANONICAL_NUM = 42
CANONICAL_DT = "2025-06-15T10:30:00.000Z"
CANONICAL_ITEMS = ["alpha", "beta", "gamma"]
CANONICAL_META: Dict[str, Any] = {
    "key1": "value1",
    "key2": 99,
    "nested": {"deep": True},
}
CANONICAL_FLAG = True
CANONICAL_EMPTY = None
CANONICAL_MESSAGE: Dict[str, Any] = {"sender": "test", "payload": [1, 2, 3]}


# ============================================================================
# Test: Canonical workflow execution and DB record verification
# ============================================================================


def test_interop_canonical(dbos: DBOS, client: DBOSClient) -> None:
    """Run the canonical workflow, verify result and all DB records as exact strings."""

    @DBOS.dbos_class("interop")
    class InteropWF:
        @classmethod
        @DBOS.workflow(
            name="canonicalWorkflow",
            serialization_type=WorkflowSerializationFormat.PORTABLE,
            validate_args=pydantic_args_validator,
        )
        def canonical_workflow(
            cls,
            text: str,
            num: int,
            dt: str,
            items: List[str],
            meta: Dict[str, Any],
            flag: bool,
            empty: Optional[Any],
        ) -> Dict[str, Any]:
            DBOS.set_event("interop_status", {"text": text, "num": num, "flag": flag})
            DBOS.write_stream("interop_stream", {"item": text})
            msg = DBOS.recv("interop_topic")
            return {
                "echo_text": text,
                "echo_num": num,
                "echo_dt": dt,
                "items_count": len(items),
                "meta_keys": sorted(meta.keys()),
                "flag": flag,
                "empty": empty,
                "received": msg,
            }

    queue = Queue("interopq")

    # Start the canonical workflow w/ Enqueue
    wfh: WorkflowHandle[str] = client.enqueue(
        {
            "queue_name": "interopq",
            "workflow_name": "canonicalWorkflow",
            "class_name": "interop",
            "serialization_type": WorkflowSerializationFormat.PORTABLE,
        },
        CANONICAL_TEXT,
        CANONICAL_NUM,
        CANONICAL_DT,
        CANONICAL_ITEMS,
        CANONICAL_META,
        CANONICAL_FLAG,
        CANONICAL_EMPTY,
    )

    # Send the canonical message via client
    client.send(
        wfh.workflow_id,
        CANONICAL_MESSAGE,
        "interop_topic",
        serialization_type=WorkflowSerializationFormat.PORTABLE,
    )

    # Verify result
    result = wfh.get_result()
    assert result == EXPECTED_RESULT

    # ---- Verify DB records as exact golden strings ----

    with dbos._sys_db.engine.connect() as c:
        # workflow_status
        ws_row = c.execute(
            sa.select(
                SystemSchema.workflow_status.c.inputs,
                SystemSchema.workflow_status.c.output,
                SystemSchema.workflow_status.c.serialization,
                SystemSchema.workflow_status.c.status,
                SystemSchema.workflow_status.c.name,
                SystemSchema.workflow_status.c.class_name,
            ).where(
                SystemSchema.workflow_status.c.workflow_uuid == wfh.workflow_id,
            )
        ).fetchone()
        assert ws_row is not None
        assert ws_row.serialization == "portable_json"
        assert ws_row.status == "SUCCESS"
        assert ws_row.name == "canonicalWorkflow"
        assert ws_row.class_name == "interop"

        # Inputs: verify positionalArgs matches golden set (Python adds namedArgs:{})
        assert ws_row.inputs.replace('"namedArgs":{},', "") == GOLDEN_INPUTS_JSON

        # Output: exact string comparison
        assert ws_row.output == GOLDEN_OUTPUT_JSON

        # workflow_events: exact string comparison
        evt_row = c.execute(
            sa.select(
                SystemSchema.workflow_events.c.value,
                SystemSchema.workflow_events.c.serialization,
            ).where(
                SystemSchema.workflow_events.c.workflow_uuid == wfh.workflow_id,
                SystemSchema.workflow_events.c.key == "interop_status",
            )
        ).fetchone()
        assert evt_row is not None
        assert evt_row.serialization == "portable_json"
        assert evt_row.value == GOLDEN_EVENT_JSON

        # workflow_events_history: exact string comparison
        evth_row = c.execute(
            sa.select(
                SystemSchema.workflow_events_history.c.value,
                SystemSchema.workflow_events_history.c.serialization,
            ).where(
                SystemSchema.workflow_events_history.c.workflow_uuid == wfh.workflow_id,
                SystemSchema.workflow_events_history.c.key == "interop_status",
            )
        ).fetchone()
        assert evth_row is not None
        assert evth_row.serialization == "portable_json"
        assert evth_row.value == GOLDEN_EVENT_JSON

        # streams: exact string comparison
        stream_row = c.execute(
            sa.select(
                SystemSchema.streams.c.value,
                SystemSchema.streams.c.serialization,
                SystemSchema.streams.c.offset,
            ).where(
                SystemSchema.streams.c.workflow_uuid == wfh.workflow_id,
                SystemSchema.streams.c.key == "interop_stream",
            )
        ).fetchone()
        assert stream_row is not None
        assert stream_row.serialization == "portable_json"
        assert stream_row.offset == 0
        assert stream_row.value == GOLDEN_STREAM_JSON


# ============================================================================
# Test: Direct-insert replay (proves cross-language compatibility)
# ============================================================================


def test_interop_direct_insert(dbos: DBOS) -> None:
    """Insert golden DB records via raw SQL and verify the workflow executes correctly.

    This proves that records produced by TypeScript or Java can be consumed by Python.
    The golden JSON strings are byte-identical to what those languages produce.
    """
    dburl = dbos._config["system_database_url"]
    assert dburl is not None
    schema = "dbos." if dburl.startswith("postgres") else ""

    @DBOS.dbos_class("interop")
    class InteropWF:
        @classmethod
        @DBOS.workflow(
            name="canonicalWorkflow",
            serialization_type=WorkflowSerializationFormat.PORTABLE,
            validate_args=pydantic_args_validator,
        )
        def canonical_workflow(
            cls,
            text: str,
            num: int,
            dt: str,
            items: List[str],
            meta: Dict[str, Any],
            flag: bool,
            empty: Optional[Any],
        ) -> Dict[str, Any]:
            DBOS.set_event("interop_status", {"text": text, "num": num, "flag": flag})
            DBOS.write_stream("interop_stream", {"item": text})
            msg = DBOS.recv("interop_topic")
            return {
                "echo_text": text,
                "echo_num": num,
                "echo_dt": dt,
                "items_count": len(items),
                "meta_keys": sorted(meta.keys()),
                "flag": flag,
                "empty": empty,
                "received": msg,
            }

    queue = Queue("interopq")

    wf_id = str(uuid.uuid4())
    with dbos._sys_db.engine.begin() as c:
        # Insert golden workflow_status
        c.execute(
            sa.text(
                f"""
            INSERT INTO {schema}workflow_status(
              workflow_uuid, name, class_name, queue_name,
              status, inputs, created_at, serialization
            )
            VALUES (:workflow_uuid, :name, :class_name, :queue_name,
                    :status, :inputs, :created_at, :serialization);
            """
            ),
            {
                "workflow_uuid": wf_id,
                "name": "canonicalWorkflow",
                "class_name": "interop",
                "queue_name": "interopq",
                "status": "ENQUEUED",
                "inputs": GOLDEN_INPUTS_JSON,
                "created_at": int(time.time() * 1000),
                "serialization": "portable_json",
            },
        )

        # Insert golden notification
        c.execute(
            sa.text(
                f"""
            INSERT INTO {schema}notifications(
              destination_uuid, topic, message, serialization
            )
            VALUES (:destination_uuid, :topic, :message, :serialization);
            """
            ),
            {
                "destination_uuid": wf_id,
                "topic": "interop_topic",
                "message": GOLDEN_MESSAGE_JSON,
                "serialization": "portable_json",
            },
        )

        c.commit()

    # Retrieve and verify the workflow executes correctly
    wfh: WorkflowHandle[Dict[str, Any]] = DBOS.retrieve_workflow(wf_id)
    result = wfh.get_result()
    assert result == EXPECTED_RESULT

    # Verify the output was written as the golden string
    with dbos._sys_db.engine.connect() as c:
        ws_row = c.execute(
            sa.select(SystemSchema.workflow_status.c.output).where(
                SystemSchema.workflow_status.c.workflow_uuid == wf_id,
            )
        ).fetchone()
        assert ws_row is not None
        assert ws_row.output == GOLDEN_OUTPUT_JSON


# ============================================================================
# Test: Python kwargs → namedArgs interop
# ============================================================================


def test_interop_kwargs(dbos: DBOS) -> None:
    """Test that Python kwargs are serialized as namedArgs in portable JSON,
    matching what TypeScript and Java produce via enqueuePortable with namedArgs.
    """
    dburl = dbos._config["system_database_url"]
    assert dburl is not None
    schema = "dbos." if dburl.startswith("postgres") else ""

    @DBOS.dbos_class("interop")
    class KwargWF:
        @classmethod
        @DBOS.workflow(
            name="kwargWorkflow",
            serialization_type=WorkflowSerializationFormat.PORTABLE,
            validate_args=pydantic_args_validator,
        )
        def kwarg_workflow(cls, *, name: str, count: int, tags: List[str]) -> str:
            return f"{name}-{count}-{','.join(tags)}"

    # Invoke with kwargs
    wfh = DBOS.start_workflow(
        KwargWF.kwarg_workflow, name="test", count=42, tags=["a", "b"]
    )
    result = wfh.get_result()
    assert result == "test-42-a,b"

    # Verify inputs in DB contain namedArgs
    with dbos._sys_db.engine.connect() as c:
        ws_row = c.execute(
            sa.select(
                SystemSchema.workflow_status.c.inputs,
                SystemSchema.workflow_status.c.serialization,
            ).where(
                SystemSchema.workflow_status.c.workflow_uuid == wfh.workflow_id,
            )
        ).fetchone()
        assert ws_row is not None
        assert ws_row.serialization == "portable_json"

        stored_inputs = json.loads(ws_row.inputs)
        assert stored_inputs["positionalArgs"] == []
        assert stored_inputs["namedArgs"] == {
            "name": "test",
            "count": 42,
            "tags": ["a", "b"],
        }

    # Verify the golden namedArgs JSON (as TS/Java would produce) can be consumed
    golden_kwargs_json = (
        '{"positionalArgs":[],"namedArgs":{"name":"test","count":42,"tags":["a","b"]}}'
    )

    queue = Queue("interopkq")

    wf_id2 = str(uuid.uuid4())
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.text(
                f"""
            INSERT INTO {schema}workflow_status(
              workflow_uuid, name, class_name, queue_name,
              status, inputs, created_at, serialization
            )
            VALUES (:workflow_uuid, :name, :class_name, :queue_name,
                    :status, :inputs, :created_at, :serialization);
            """
            ),
            {
                "workflow_uuid": wf_id2,
                "name": "kwargWorkflow",
                "class_name": "interop",
                "queue_name": "interopkq",
                "status": "ENQUEUED",
                "inputs": golden_kwargs_json,
                "created_at": int(time.time() * 1000),
                "serialization": "portable_json",
            },
        )
        c.commit()

    wfh2: WorkflowHandle[str] = DBOS.retrieve_workflow(wf_id2)
    result2 = wfh2.get_result()
    assert result2 == "test-42-a,b"

    # Verify the output matches the golden string
    with dbos._sys_db.engine.connect() as c:
        ws_row2 = c.execute(
            sa.select(SystemSchema.workflow_status.c.output).where(
                SystemSchema.workflow_status.c.workflow_uuid == wf_id2,
            )
        ).fetchone()
        assert ws_row2 is not None
        assert ws_row2.output == '"test-42-a,b"'
