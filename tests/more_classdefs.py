from typing import Optional

# Public API
from dbos import DBOS


@DBOS.dbos_class()
class DBOSWFEvents:
    @staticmethod
    @DBOS.workflow()
    def test_setevent_workflow() -> None:
        DBOS.set_event("key1", "value1")
        DBOS.set_event("key2", "value2")
        DBOS.set_event("key3", None)

    @staticmethod
    @DBOS.workflow()
    def test_getevent_workflow(
        target_uuid: str, key: str, timeout_seconds: float = 10
    ) -> Optional[str]:
        msg = DBOS.get_event(target_uuid, key, timeout_seconds)
        return str(msg) if msg is not None else None


@DBOS.workflow()
def wfFunc(arg: str) -> str:
    assert DBOS.workflow_id == "wfid"
    return arg + "1"
