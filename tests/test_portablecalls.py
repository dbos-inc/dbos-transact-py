import json
import os
import subprocess
import time
import uuid
from typing import Any, Dict, Optional

import pytest
import sqlalchemy as sa

from dbos import DBOS, Queue, WorkflowHandle
from dbos._client import DBOSClient
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import (
    DBOSDefaultSerializer,
    DBOSPortableJSON,
    PortableWorkflowError,
    WorkflowSerializationFormat,
)


def workflow_func(
    s: str,
    x: int,
    o: Dict[str, Any],
    wfid: Optional[str] = None,
) -> str:
    DBOS.set_event("defstat", {"status": "Happy"})
    DBOS.set_event(
        "nstat",
        {"status": "Happy"},
        serialization_type=WorkflowSerializationFormat.NATIVE,
    )
    DBOS.set_event(
        "pstat",
        {"status": "Happy"},
        serialization_type=WorkflowSerializationFormat.PORTABLE,
    )

    DBOS.write_stream("defstream", {"stream": "OhYeah"})
    DBOS.write_stream(
        "nstream",
        {"stream": "OhYeah"},
        serialization_type=WorkflowSerializationFormat.NATIVE,
    )
    DBOS.write_stream(
        "pstream",
        {"stream": "OhYeah"},
        serialization_type=WorkflowSerializationFormat.PORTABLE,
    )

    if wfid is not None:
        DBOS.send(wfid, {"message": "Hello!"}, "default")
        DBOS.send(
            wfid,
            {"message": "Hello!"},
            "native",
            serialization_type=WorkflowSerializationFormat.NATIVE,
        )
        DBOS.send(
            wfid,
            {"message": "Hello!"},
            "portable",
            serialization_type=WorkflowSerializationFormat.PORTABLE,
        )

    r = DBOS.recv("incoming")

    return f"{s}-{x}-{o['k']}:{','.join(o['v'])}@{json.dumps(r)}"


def test_portable_ser(dbos: DBOS, client: DBOSClient) -> None:
    @DBOS.dbos_class("workflows")
    class WFTest:
        @staticmethod
        @DBOS.workflow(name="workflowDefault")
        def defSerDefault(
            s: str,
            x: int,
            o: Dict[str, Any],
            wfid: Optional[str] = None,
        ) -> str:
            DBOS.logger.info("defSerDefault was called...")
            return workflow_func(s, x, o, wfid)

        @staticmethod
        @DBOS.workflow(
            name="workflowPortable",
            serialization_type=WorkflowSerializationFormat.PORTABLE,
        )
        def defSerPortable(
            s: str,
            x: int,
            o: Dict[str, Any],
            wfid: Optional[str] = None,
        ) -> str:
            DBOS.logger.info("defSerPortable was called...")
            return workflow_func(s, x, o, wfid)

        @classmethod
        @DBOS.workflow(
            name="workflowPortableCls",
            serialization_type=WorkflowSerializationFormat.PORTABLE,
        )
        def defSerPortableCls(
            cls,
            s: str,
            x: int,
            o: Dict[str, Any],
            wfid: Optional[str] = None,
        ) -> str:
            DBOS.logger.info("defSerPortableCls was called...")
            return workflow_func(s, x, o, wfid)

        @classmethod
        @DBOS.workflow(name="simpleRecv")
        def recv(cls, topic: str) -> Any:
            return DBOS.recv(topic)

        last_wf_id: str | None = ""

        @staticmethod
        @DBOS.workflow(
            name="workflowPortableError",
            serialization_type=WorkflowSerializationFormat.PORTABLE,
        )
        def defSerError() -> None:
            WFTest.last_wf_id = DBOS.workflow_id
            raise Exception("This is just a plain error")

    queue = Queue("testq")

    def check_wf_ser(wfid: str, ser: str) -> None:
        with dbos._sys_db.engine.connect() as c:
            result = c.execute(
                sa.select(SystemSchema.workflow_status.c.serialization).where(
                    SystemSchema.workflow_status.c.workflow_uuid == wfid,
                )
            )
            row = result.fetchone()
            assert row is not None
            assert row.serialization == ser

    def check_msg_ser(dstdid: str, topic: str, ser: str) -> None:
        with dbos._sys_db.engine.connect() as c:
            result = c.execute(
                sa.select(SystemSchema.notifications.c.serialization).where(
                    SystemSchema.notifications.c.destination_uuid == dstdid,
                    SystemSchema.notifications.c.topic == topic,
                )
            )
            row = result.fetchone()
            assert row is not None
            assert row.serialization == ser

    def check_evt_ser(wfid: str, key: str, ser: str) -> None:
        with dbos._sys_db.engine.connect() as c:
            result = c.execute(
                sa.select(SystemSchema.workflow_events.c.serialization).where(
                    SystemSchema.workflow_events.c.workflow_uuid == wfid,
                    SystemSchema.workflow_events.c.key == key,
                )
            )
            row = result.fetchone()

            assert row is not None

            assert row.serialization == ser

    def check_stream_ser(wfid: str, key: str, ser: str) -> None:
        with dbos._sys_db.engine.connect() as c:
            result = c.execute(
                sa.select(SystemSchema.streams.c.serialization).where(
                    SystemSchema.streams.c.workflow_uuid == wfid,
                    SystemSchema.streams.c.key == key,
                )
            )
            row = result.fetchone()

            assert row is not None

            assert row.serialization == ser

    # Run WF with default serialization
    # But first, receivers
    drpwfh = DBOS.start_workflow(WFTest.recv, "native")
    wfhd = DBOS.start_workflow(
        WFTest.defSerDefault, "s", 1, {"k": "k", "v": ["v"]}, drpwfh.workflow_id
    )
    DBOS.send(wfhd.workflow_id, "m", "incoming")
    assert DBOS.get_event(wfhd.workflow_id, "defstat") == {"status": "Happy"}
    assert DBOS.get_event(wfhd.workflow_id, "nstat") == {"status": "Happy"}
    assert DBOS.get_event(wfhd.workflow_id, "pstat") == {"status": "Happy"}
    ddread = list(DBOS.read_stream(wfhd.workflow_id, "defstream"))
    assert ddread == [{"stream": "OhYeah"}]
    dnread = list(DBOS.read_stream(wfhd.workflow_id, "nstream"))
    assert dnread == [{"stream": "OhYeah"}]
    dpread = list(DBOS.read_stream(wfhd.workflow_id, "pstream"))
    assert dpread == [{"stream": "OhYeah"}]

    rvd = wfhd.get_result()
    assert rvd == 's-1-k:v@"m"'
    assert drpwfh.get_result() == {"message": "Hello!"}

    # Snoop the DB to make sure serialization format is correct
    # WF
    check_wf_ser(wfhd.workflow_id, DBOSDefaultSerializer.name())
    # Messages
    check_msg_ser(drpwfh.workflow_id, "default", DBOSDefaultSerializer.name())
    # check_msg_ser(drpwfh.workflow_id, 'native', DBOSDefaultSerializer.name()) # This got deleted
    check_msg_ser(drpwfh.workflow_id, "portable", DBOSPortableJSON.name())

    # Events
    check_evt_ser(wfhd.workflow_id, "defstat", DBOSDefaultSerializer.name())
    check_evt_ser(wfhd.workflow_id, "nstat", DBOSDefaultSerializer.name())
    check_evt_ser(wfhd.workflow_id, "pstat", DBOSPortableJSON.name())

    # Streams
    check_stream_ser(wfhd.workflow_id, "defstream", DBOSDefaultSerializer.name())
    check_stream_ser(wfhd.workflow_id, "nstream", DBOSDefaultSerializer.name())
    check_stream_ser(wfhd.workflow_id, "pstream", DBOSPortableJSON.name())

    # Run with portable serialization
    for fmt in ["dbos", "client", "cclient"]:
        drpwfh = DBOS.start_workflow(WFTest.recv, "portable")
        if fmt == "client":
            wfhd = client.enqueue(
                {
                    "queue_name": "testq",
                    # "class_name": "workflows", # Static does not actually register with the class_name
                    "workflow_name": "workflowPortable",
                    "serialization_type": WorkflowSerializationFormat.PORTABLE,
                },
                "s",
                1,
                {"k": "k", "v": ["v"]},
                drpwfh.workflow_id,
            )
        elif fmt == "cclient":
            wfhd = client.enqueue(
                {
                    "queue_name": "testq",
                    "class_name": "workflows",
                    "workflow_name": "workflowPortableCls",
                    "serialization_type": WorkflowSerializationFormat.PORTABLE,
                },
                "s",
                1,
                {"k": "k", "v": ["v"]},
                drpwfh.workflow_id,
            )
        else:
            wfhd = DBOS.start_workflow(
                WFTest.defSerPortable,
                "s",
                1,
                {"k": "k", "v": ["v"]},
                drpwfh.workflow_id,
            )
        DBOS.send(wfhd.workflow_id, "m", "incoming")
        assert DBOS.get_event(wfhd.workflow_id, "defstat") == {"status": "Happy"}
        assert DBOS.get_event(wfhd.workflow_id, "nstat") == {"status": "Happy"}
        assert DBOS.get_event(wfhd.workflow_id, "pstat") == {"status": "Happy"}
        ddread = list(DBOS.read_stream(wfhd.workflow_id, "defstream"))
        assert ddread == [{"stream": "OhYeah"}]
        dnread = list(DBOS.read_stream(wfhd.workflow_id, "nstream"))
        assert dnread == [{"stream": "OhYeah"}]
        dpread = list(DBOS.read_stream(wfhd.workflow_id, "pstream"))
        assert dpread == [{"stream": "OhYeah"}]

        rvd = wfhd.get_result()
        assert rvd == 's-1-k:v@"m"'
        assert drpwfh.get_result() == {"message": "Hello!"}

        # Snoop the DB to make sure serialization format is correct
        # WF
        check_wf_ser(wfhd.workflow_id, DBOSPortableJSON.name())
        # Messages
        check_msg_ser(drpwfh.workflow_id, "default", DBOSPortableJSON.name())
        check_msg_ser(drpwfh.workflow_id, "native", DBOSDefaultSerializer.name())
        # check_msg_ser(drpwfh.workflow_id, "portable", DBOSPortableJSON.name()) # This got deleted

        # Events
        check_evt_ser(wfhd.workflow_id, "defstat", DBOSPortableJSON.name())
        check_evt_ser(wfhd.workflow_id, "nstat", DBOSDefaultSerializer.name())
        check_evt_ser(wfhd.workflow_id, "pstat", DBOSPortableJSON.name())

        # Streams
        check_stream_ser(wfhd.workflow_id, "defstream", DBOSPortableJSON.name())
        check_stream_ser(wfhd.workflow_id, "nstream", DBOSDefaultSerializer.name())
        check_stream_ser(wfhd.workflow_id, "pstream", DBOSPortableJSON.name())

    # Test copy+paste workflow
    # Export w/ children
    expwf = dbos._sys_db.export_workflow(wfhd.workflow_id, export_children=True)
    assert 1 == len(expwf)
    assert 10 == len(expwf[0]["operation_outputs"])
    assert wfhd.workflow_id == expwf[0]["workflow_status"]["workflow_uuid"]
    # Delete so it can be reimported
    DBOS.delete_workflow(wfhd.workflow_id, delete_children=True)
    # Verify step outputs are deleted
    pwfs = DBOS.list_workflows(workflow_ids=[wfhd.workflow_id])
    psteps = DBOS.list_workflow_steps(wfhd.workflow_id)
    assert 0 == len(pwfs)
    assert 0 == len(psteps)

    # Import after deletion
    dbos._sys_db.import_workflow(expwf)

    # Check that everything is still there
    rvd = wfhd.get_result()
    assert rvd == 's-1-k:v@"m"'
    # WF
    check_wf_ser(wfhd.workflow_id, DBOSPortableJSON.name())
    # Messages
    check_msg_ser(drpwfh.workflow_id, "default", DBOSPortableJSON.name())
    check_msg_ser(drpwfh.workflow_id, "native", DBOSDefaultSerializer.name())
    # check_msg_ser(drpwfh.workflow_id, "portable", DBOSPortableJSON.name()) # This got deleted
    # Events
    check_evt_ser(wfhd.workflow_id, "defstat", DBOSPortableJSON.name())
    check_evt_ser(wfhd.workflow_id, "nstat", DBOSDefaultSerializer.name())
    check_evt_ser(wfhd.workflow_id, "pstat", DBOSPortableJSON.name())
    # Streams
    check_stream_ser(wfhd.workflow_id, "defstream", DBOSPortableJSON.name())
    check_stream_ser(wfhd.workflow_id, "nstream", DBOSDefaultSerializer.name())
    check_stream_ser(wfhd.workflow_id, "pstream", DBOSPortableJSON.name())

    # Reexecute
    dbos._sys_db.update_workflow_outcome(wfhd.workflow_id, "PENDING")
    wfhrex = dbos._execute_workflow_id(wfhd.workflow_id)
    assert wfhrex.get_result() == 's-1-k:v@"m"'

    # Errors
    with pytest.raises(Exception):
        WFTest.defSerError()
    lwfid = WFTest.last_wf_id
    assert lwfid is not None
    check_wf_ser(lwfid, DBOSPortableJSON.name())
    with pytest.raises(PortableWorkflowError) as ex_info:
        DBOS.retrieve_workflow(lwfid).get_result()
    assert ex_info.value.name == "Exception"
    assert ex_info.value.message == "This is just a plain error"


def test_directinsert_workflows(dbos: DBOS) -> None:
    dburl = dbos._config["system_database_url"]
    assert dburl is not None
    schema = "dbos." if dburl.startswith("postgres") else ""

    @DBOS.dbos_class("workflows")
    class WFTest:
        @classmethod
        @DBOS.workflow(name="workflowPortable")
        def defSerPortable(
            cls,
            s: str,
            x: int,
            o: Dict[str, Any],
            wfid: Optional[str] = None,
        ) -> str:
            DBOS.logger.info("defSerPortable was called...")
            return workflow_func(s, x, o, wfid)

    queue = Queue("testq")

    id = str(uuid.uuid4())
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.text(
                f"""
            INSERT INTO {schema}workflow_status(
              workflow_uuid,
              name,
              class_name,
              queue_name,
              status,
              inputs,
              created_at,
              serialization
            )
            VALUES (:workflow_uuid, :name, :class_name, :queue_name, :status, :inputs, :created_at, :serialization);
            """
            ),
            {
                "workflow_uuid": id,
                "name": "workflowPortable",
                "class_name": "workflows",
                "queue_name": "testq",
                "status": "ENQUEUED",
                "inputs": json.dumps(
                    {"positionalArgs": ["s", 1, {"k": "k", "v": ["v"]}]},
                    separators=(",", ":"),
                ),
                # milliseconds since epoch
                "created_at": int(time.time() * 1000),
                "serialization": "portable_json",
            },
        )

        c.execute(
            sa.text(
                f"""
            INSERT INTO {schema}notifications(
              destination_uuid,
              topic,
              message,
              serialization
            )
            VALUES (:destination_uuid, :topic, :message, :serialization);
            """
            ),
            {
                "destination_uuid": id,
                "topic": "incoming",
                "message": json.dumps("M"),
                "serialization": "portable_json",
            },
        )

        c.commit()

    wfh: WorkflowHandle[str] = DBOS.retrieve_workflow(id)
    res = wfh.get_result()
    assert res == 's-1-k:v@"M"'


def test_nodejs_invoke(dbos: DBOS) -> None:
    dburl = dbos._config["system_database_url"]
    assert dburl is not None
    if not dburl.startswith("postgres"):
        DBOS.logger.warning(
            "Not a Postgres database - skipping TypeScript enqueue test"
        )
        return

    @DBOS.dbos_class("workflows")
    class WFTest:
        @classmethod
        @DBOS.workflow(name="workflowPortable")
        def defSerPortable(
            cls,
            s: str,
            x: int,
            o: Dict[str, Any],
            wfid: Optional[str] = None,
        ) -> str:
            DBOS.logger.info("defSerPortable was called...")
            return workflow_func(s, x, o, wfid)

    queue = Queue("testq")

    script_path = os.path.join(
        os.path.dirname(__file__), "ts_client", "bundles", "portableinvoke.cjs"
    )
    args = ["node", script_path, dburl]

    env = os.environ.copy()
    result = subprocess.run(args, env=env, capture_output=True, text=True)
    assert result.returncode == 0, f"Worker failed with error: {result.stderr}"
    DBOS.logger.info(result.stdout)
