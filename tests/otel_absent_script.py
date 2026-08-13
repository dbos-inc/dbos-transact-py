"""Run workflows with opentelemetry unimportable.

opentelemetry is an optional extra (`dbos[otel]`) and OTLP is off by default, so the
workflow execution path must never import it. Run as a subprocess by
tests/test_spans.py::test_workflows_run_without_opentelemetry: the blocker has to be
installed before dbos is imported, and the test suite itself needs the real package.

Usage: python otel_absent_script.py <sqlite_path>
"""

import sys
from typing import Any, Optional, Sequence


class _BlockOtel:
    """Meta path finder that makes any opentelemetry import fail."""

    def find_spec(
        self, name: str, path: Optional[Sequence[str]] = None, target: Any = None
    ) -> None:
        if name == "opentelemetry" or name.startswith("opentelemetry."):
            raise ModuleNotFoundError(f"No module named {name!r} (blocked by test)")
        return None


def main() -> None:
    sqlite_path = sys.argv[1]
    sys.meta_path.insert(0, _BlockOtel())

    # Imported here, not at module scope: the blocker must be in place first.
    import sqlalchemy as sa

    from dbos import DBOS, DBOSConfig, Queue, SetWorkflowAttributes
    from dbos._schemas.system_database import SystemSchema

    config: DBOSConfig = {
        "name": "otel-absent-app",
        "application_database_url": f"sqlite:///{sqlite_path}",
        "system_database_url": f"sqlite:///{sqlite_path}",
        # Default in production; spelled out because it is the whole point of this script.
        "enable_otlp": False,
    }
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.step()
    def a_step() -> str:
        return "stepped"

    @DBOS.workflow()
    def a_workflow() -> str:
        return a_step()

    DBOS.launch()
    queue = Queue("otel_absent_queue")

    # The executor path: start_workflow captures the caller's otel context.
    assert DBOS.start_workflow(a_workflow).get_result() == "stepped"

    # The dequeue path: execute_dequeued_workflow reads the carrier out of the status.
    assert queue.enqueue(a_workflow).get_result() == "stepped"

    # A carrier written by another process that *does* have opentelemetry installed.
    carrier = {"traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01"}
    with SetWorkflowAttributes({"dbos.otelContext": carrier}):
        assert queue.enqueue(a_workflow).get_result() == "stepped"

    # The recovery path.
    handle = DBOS.start_workflow(a_workflow)
    assert handle.get_result() == "stepped"
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING"})
            .where(SystemSchema.workflow_status.c.workflow_uuid == handle.workflow_id)
        )
    recovered = DBOS._recover_pending_workflows()
    assert len(recovered) == 1
    assert recovered[0].get_result() == "stepped"

    assert not [m for m in sys.modules if m.startswith("opentelemetry")]
    DBOS.destroy(destroy_registry=True)
    print("OK")


if __name__ == "__main__":
    main()
