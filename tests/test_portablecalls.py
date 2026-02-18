import json
import os
import subprocess
import time
import uuid
from typing import Any, Dict, Optional

import pytest
import sqlalchemy as sa

from dbos import DBOS, DBOSConfig, Queue, WorkflowHandle
from dbos._client import DBOSClient
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import (
    DBOSDefaultSerializer,
    DBOSPortableJSON,
    PortableWorkflowError,
    Serializer,
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


def test_custom_serializer_across_restarts(
    config: DBOSConfig,
    cleanup_test_databases: None,
) -> None:
    """Test that serializer changes across DBOS restarts are handled correctly.

    Phase 1: Run workflows with the default (pickle) serializer.
    Phase 2: Restart with a custom JSON serializer; verify old and new data are readable.
    Phase 3: Restart without the custom serializer; verify old pickle data still works,
             custom-serialized data raises TypeError on hard paths, and safe
             introspection (list_workflows / list_workflow_steps) degrades gracefully.
    """

    class JsonSerializer(Serializer):
        def serialize(self, data: Any) -> str:
            return json.dumps(data)

        def deserialize(self, serialized_data: str) -> Any:
            return json.loads(serialized_data)

        def name(self) -> str:
            return "custom_json"

    # --- DB inspection helpers ---
    def check_wf_ser(engine: sa.Engine, wfid: str, expected_ser: str) -> None:
        with engine.connect() as c:
            row = c.execute(
                sa.select(SystemSchema.workflow_status.c.serialization).where(
                    SystemSchema.workflow_status.c.workflow_uuid == wfid,
                )
            ).fetchone()
            assert row is not None
            assert row.serialization == expected_ser

    def check_evt_ser(
        engine: sa.Engine, wfid: str, key: str, expected_ser: str
    ) -> None:
        with engine.connect() as c:
            row = c.execute(
                sa.select(SystemSchema.workflow_events.c.serialization).where(
                    SystemSchema.workflow_events.c.workflow_uuid == wfid,
                    SystemSchema.workflow_events.c.key == key,
                )
            ).fetchone()
            assert row is not None
            assert row.serialization == expected_ser

    # --- Workflow registration helper ---
    # Must produce the same qualified name each time so retrieve_workflow works
    # across restarts.
    def register_workflows() -> Any:
        @DBOS.workflow(name="ser_restart_workflow")
        def ser_restart_workflow(val: str) -> str:
            step_result = ser_restart_step(val)
            DBOS.set_event("evt_key", {"data": val})
            return f"result_{step_result}"

        @DBOS.step()
        def ser_restart_step(val: str) -> str:
            return val

        return ser_restart_workflow

    sys_db_url = config["system_database_url"]
    assert sys_db_url is not None

    # ======= PHASE 1: Default serializer (py_pickle) =======
    DBOS.destroy(destroy_registry=True)
    dbos_inst = DBOS(config=config)
    DBOS.launch()
    phase_workflow = register_workflows()

    handle1 = DBOS.start_workflow(phase_workflow, "phase1")
    result1 = handle1.get_result()
    assert result1 == "result_phase1"
    phase1_wfid = handle1.workflow_id

    # Verify DB serialization column
    check_wf_ser(dbos_inst._sys_db.engine, phase1_wfid, DBOSDefaultSerializer.name())
    check_evt_ser(
        dbos_inst._sys_db.engine, phase1_wfid, "evt_key", DBOSDefaultSerializer.name()
    )

    # Verify get_event works
    assert DBOS.get_event(phase1_wfid, "evt_key") == {"data": "phase1"}

    # Verify list_workflows returns properly deserialized data
    wfs = DBOS.list_workflows(workflow_ids=[phase1_wfid])
    assert len(wfs) == 1
    assert wfs[0].output == "result_phase1"

    # Verify list_workflow_steps returns step outputs
    steps1 = DBOS.list_workflow_steps(phase1_wfid)
    assert len(steps1) >= 1

    # DBOSClient (default serializer) can also read phase 1 data
    client = DBOSClient(system_database_url=sys_db_url)
    cwfs = client.list_workflows(workflow_ids=[phase1_wfid])
    assert len(cwfs) == 1
    assert cwfs[0].output == "result_phase1"
    assert client.retrieve_workflow(phase1_wfid).get_result() == "result_phase1"
    assert client.get_event(phase1_wfid, "evt_key") == {"data": "phase1"}
    csteps = client.list_workflow_steps(phase1_wfid)
    assert len(csteps) >= 1
    client.destroy()

    DBOS.destroy(destroy_registry=True)

    # ======= PHASE 2: Custom serializer (custom_json) =======
    config["serializer"] = JsonSerializer()
    dbos_inst = DBOS(config=config)
    DBOS.launch()
    phase_workflow = register_workflows()

    handle2 = DBOS.start_workflow(phase_workflow, "phase2")
    result2 = handle2.get_result()
    assert result2 == "result_phase2"
    phase2_wfid = handle2.workflow_id

    # Verify DB uses custom_json for the new workflow
    check_wf_ser(dbos_inst._sys_db.engine, phase2_wfid, "custom_json")
    check_evt_ser(dbos_inst._sys_db.engine, phase2_wfid, "evt_key", "custom_json")

    # Phase 1 data (py_pickle) is still readable because it's a built-in serializer
    assert DBOS.retrieve_workflow(phase1_wfid).get_result() == "result_phase1"
    assert DBOS.get_event(phase1_wfid, "evt_key") == {"data": "phase1"}

    # list_workflows sees both, all properly deserialized
    wfs = DBOS.list_workflows(workflow_ids=[phase1_wfid, phase2_wfid])
    assert len(wfs) == 2
    wf_by_id = {wf.workflow_id: wf for wf in wfs}
    assert wf_by_id[phase1_wfid].output == "result_phase1"
    assert wf_by_id[phase2_wfid].output == "result_phase2"

    # DBOSClient with the custom serializer reads both phases correctly
    client_custom = DBOSClient(
        system_database_url=sys_db_url, serializer=JsonSerializer()
    )
    assert client_custom.retrieve_workflow(phase1_wfid).get_result() == "result_phase1"
    assert client_custom.retrieve_workflow(phase2_wfid).get_result() == "result_phase2"
    assert client_custom.get_event(phase1_wfid, "evt_key") == {"data": "phase1"}
    assert client_custom.get_event(phase2_wfid, "evt_key") == {"data": "phase2"}
    cwfs = client_custom.list_workflows(workflow_ids=[phase1_wfid, phase2_wfid])
    assert len(cwfs) == 2
    cwf_by_id = {wf.workflow_id: wf for wf in cwfs}
    assert cwf_by_id[phase1_wfid].output == "result_phase1"
    assert cwf_by_id[phase2_wfid].output == "result_phase2"
    client_custom.destroy()

    # DBOSClient without the custom serializer: safe paths degrade gracefully
    client_default = DBOSClient(system_database_url=sys_db_url)
    # Phase 1 data still readable (py_pickle is built-in)
    assert client_default.retrieve_workflow(phase1_wfid).get_result() == "result_phase1"
    assert client_default.get_event(phase1_wfid, "evt_key") == {"data": "phase1"}
    # Phase 2 data raises TypeError on hard paths
    with pytest.raises(TypeError, match="custom_json is not available"):
        client_default.retrieve_workflow(phase2_wfid).get_result()
    with pytest.raises(TypeError, match="custom_json is not available"):
        client_default.get_event(phase2_wfid, "evt_key")
    # list_workflows returns raw strings for phase 2 data
    cwfs = client_default.list_workflows(workflow_ids=[phase1_wfid, phase2_wfid])
    assert len(cwfs) == 2
    cwf_by_id = {wf.workflow_id: wf for wf in cwfs}
    assert cwf_by_id[phase1_wfid].output == "result_phase1"
    assert isinstance(cwf_by_id[phase2_wfid].output, str)
    assert cwf_by_id[phase2_wfid].output == json.dumps("result_phase2")
    client_default.destroy()

    DBOS.destroy(destroy_registry=True)

    # ======= PHASE 3: Back to default serializer (custom removed) =======
    config.pop("serializer", None)
    dbos_inst = DBOS(config=config)
    DBOS.launch()
    phase_workflow = register_workflows()

    # Phase 1 data (py_pickle) still works fine
    assert DBOS.retrieve_workflow(phase1_wfid).get_result() == "result_phase1"
    assert DBOS.get_event(phase1_wfid, "evt_key") == {"data": "phase1"}

    # Phase 2 data (custom_json) raises TypeError on hard deserialization paths
    with pytest.raises(TypeError, match="custom_json is not available"):
        DBOS.retrieve_workflow(phase2_wfid).get_result()

    with pytest.raises(TypeError, match="custom_json is not available"):
        DBOS.get_event(phase2_wfid, "evt_key")

    # Safe introspection paths (list_workflows, list_workflow_steps) don't crash;
    # they fall back to returning raw serialized strings for undeserializable data.
    wfs = DBOS.list_workflows(workflow_ids=[phase1_wfid, phase2_wfid])
    assert len(wfs) == 2
    wf_by_id = {wf.workflow_id: wf for wf in wfs}

    # Phase 1 workflow: properly deserialized
    wf1 = wf_by_id[phase1_wfid]
    assert wf1.output == "result_phase1"

    # Phase 2 workflow: raw string fallback (the JSON-encoded serialized form)
    wf2 = wf_by_id[phase2_wfid]
    assert isinstance(wf2.output, str)
    assert wf2.output == json.dumps("result_phase2")

    # list_workflow_steps also uses safe_deserialize
    steps2 = DBOS.list_workflow_steps(phase2_wfid)
    assert len(steps2) >= 1
    # The step output should be a raw string since custom_json is not available
    assert isinstance(steps2[0]["output"], str)

    # A new workflow in Phase 3 works normally with the default serializer
    handle3 = DBOS.start_workflow(phase_workflow, "phase3")
    assert handle3.get_result() == "result_phase3"
    phase3_wfid = handle3.workflow_id
    check_wf_ser(dbos_inst._sys_db.engine, phase3_wfid, DBOSDefaultSerializer.name())

    # DBOSClient with the custom serializer can still rescue phase 2 data,
    # even though the DBOS instance itself no longer has the custom serializer.
    client_rescue = DBOSClient(
        system_database_url=sys_db_url, serializer=JsonSerializer()
    )
    assert client_rescue.retrieve_workflow(phase1_wfid).get_result() == "result_phase1"
    assert client_rescue.retrieve_workflow(phase2_wfid).get_result() == "result_phase2"
    assert client_rescue.retrieve_workflow(phase3_wfid).get_result() == "result_phase3"
    assert client_rescue.get_event(phase2_wfid, "evt_key") == {"data": "phase2"}
    cwfs = client_rescue.list_workflows(
        workflow_ids=[phase1_wfid, phase2_wfid, phase3_wfid]
    )
    assert len(cwfs) == 3
    cwf_by_id = {wf.workflow_id: wf for wf in cwfs}
    assert cwf_by_id[phase1_wfid].output == "result_phase1"
    assert cwf_by_id[phase2_wfid].output == "result_phase2"
    assert cwf_by_id[phase3_wfid].output == "result_phase3"
    client_rescue.destroy()

    # DBOSClient without the custom serializer sees the same degradation as DBOS
    client_no_custom = DBOSClient(system_database_url=sys_db_url)
    assert (
        client_no_custom.retrieve_workflow(phase1_wfid).get_result() == "result_phase1"
    )
    assert (
        client_no_custom.retrieve_workflow(phase3_wfid).get_result() == "result_phase3"
    )
    with pytest.raises(TypeError, match="custom_json is not available"):
        client_no_custom.retrieve_workflow(phase2_wfid).get_result()
    # list_workflow_steps via client also degrades gracefully
    csteps2 = client_no_custom.list_workflow_steps(phase2_wfid)
    assert len(csteps2) >= 1
    assert isinstance(csteps2[0]["output"], str)
    client_no_custom.destroy()

    DBOS.destroy(destroy_registry=True)
