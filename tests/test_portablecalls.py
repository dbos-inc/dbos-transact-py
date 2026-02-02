import json
import time
import uuid
from typing import Any, Dict, Optional

import pytest
import sqlalchemy as sa

from dbos import DBOS, Queue, WorkflowHandle


def workflow_func(
    s: str,
    x: int,
    o: Dict[str, Any],
    wfid: Optional[str] = None,
) -> str:
    DBOS.set_event("defstat", {"status": "Happy"})
    DBOS.set_event("nstat", {"status": "Happy"})  # TODO Ser
    DBOS.set_event("pstat", {"status": "Happy"})  # TODO Ser

    DBOS.write_stream("defstream", {"stream": "OhYeah"})
    DBOS.write_stream("nstream", {"stream": "OhYeah"})  # TODO Ser
    DBOS.write_stream("pstream", {"stream": "OhYeah"})  # TODO Ser

    if wfid is not None:
        DBOS.send(wfid, {"message": "Hello!"}, "default")
        DBOS.send(wfid, {"message": "Hello!"}, "native")  # TODO: Ser
        DBOS.send(wfid, {"message": "Hello!"}, "portable")  # TODO: Ser

    r = DBOS.recv("incoming")

    return f"{s}-{x}-{o['k']}:{','.join(o['v'])}@{json.dumps(r)}"


def test_directinsert_workflows(dbos: DBOS) -> None:
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
                """
            INSERT INTO dbos.workflow_status(
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
                """
            INSERT INTO dbos.notifications(
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
