"""Tests that every row DBOS writes carries its owning application's name.

Nothing reads application_name yet, so these assert only that ownership is
stamped correctly: rows that will run in this process are owned outright, and
rows routed by a globally unique queue name are left unclaimed.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

import sqlalchemy as sa

from dbos import DBOS, DBOSClient, Queue, WorkflowHandle
from dbos._schemas.system_database import SystemSchema
from dbos._utils import INTERNAL_QUEUE_NAME

from .conftest import retry_until_success

APP_NAME = "test-app"  # matches conftest.default_config


def daily_cron_far_from_now() -> str:
    # Daily cron pinned ~12 hours away, so it can't fire mid-test at any wall-clock time.
    t = datetime.now(timezone.utc) + timedelta(hours=12)
    return f"{t.minute} {t.hour} * * *"


def application_name_of(dbos: DBOS, workflow_id: str) -> Any:
    with dbos._sys_db.engine.begin() as c:
        return c.execute(
            sa.select(SystemSchema.workflow_status.c.application_name).where(
                SystemSchema.workflow_status.c.workflow_uuid == workflow_id
            )
        ).scalar()


def test_started_workflow_is_owned(dbos: DBOS) -> None:
    @DBOS.workflow()
    def wf() -> int:
        return 5

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 5
    assert application_name_of(dbos, handle.workflow_id) == APP_NAME


def test_enqueued_workflow_on_served_queue_is_owned(dbos: DBOS) -> None:
    queue = Queue("owned-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 6

    handle = queue.enqueue(wf)
    assert handle.get_result() == 6
    assert application_name_of(dbos, handle.workflow_id) == APP_NAME


def test_enqueue_to_unserved_queue_is_still_owned(dbos: DBOS) -> None:
    """The target queue's name never affects ownership: this application owns what
    it enqueues, and reaching another application requires naming it."""

    @DBOS.workflow()
    def wf() -> int:
        return 7

    handle = DBOS.enqueue_workflow_with_options(
        {"workflow_name": wf.__qualname__, "queue_name": "some-other-apps-queue"}
    )
    assert application_name_of(dbos, handle.workflow_id) == APP_NAME


def test_enqueue_with_options_to_served_queue_is_owned(dbos: DBOS) -> None:
    Queue("options-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 8

    handle = DBOS.enqueue_workflow_with_options(
        {"workflow_name": wf.__qualname__, "queue_name": "options-queue"}
    )
    assert handle.get_result() == 8
    assert application_name_of(dbos, handle.workflow_id) == APP_NAME


def test_enqueue_options_application_name_wins(dbos: DBOS) -> None:
    """An explicit target beats the runtime's automatic stamping, so an application
    can enqueue on behalf of another one."""
    Queue("override-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 14

    handle = DBOS.enqueue_workflow_with_options(
        {
            "workflow_name": wf.__qualname__,
            "queue_name": "override-queue",
            "application_name": "other-app",
        }
    )
    assert application_name_of(dbos, handle.workflow_id) == "other-app"


def test_client_enqueue_options_application_name_wins(
    dbos: DBOS, client: DBOSClient
) -> None:
    Queue("client-override-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 15

    handle: WorkflowHandle[int] = client.enqueue(
        {
            "workflow_name": wf.__qualname__,
            "queue_name": "client-override-queue",
            "application_name": "other-app",
        }
    )
    assert application_name_of(dbos, handle.workflow_id) == "other-app"


def test_internal_queue_workflow_is_owned(dbos: DBOS) -> None:
    """The internal queue's name is shared by every application, so a row on it
    can only be routed by ownership."""

    @DBOS.workflow()
    def wf() -> int:
        return 9

    handle = DBOS.enqueue_workflow_with_options(
        {"workflow_name": wf.__qualname__, "queue_name": INTERNAL_QUEUE_NAME}
    )
    assert handle.get_result() == 9
    assert application_name_of(dbos, handle.workflow_id) == APP_NAME


def test_scheduled_workflow_is_owned(dbos: DBOS) -> None:
    fired: list[str] = []

    @DBOS.workflow()
    def scheduled(scheduled_at: datetime, ctx: Any) -> None:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        fired.append(workflow_id)

    DBOS.create_schedule(
        schedule_name="owned-schedule",
        workflow_fn=scheduled,
        schedule="* * * * * *",
    )

    # Indexing raises until the schedule fires, which is what retry_until_success waits on.
    workflow_id = retry_until_success(lambda: fired[0])
    assert application_name_of(dbos, workflow_id) == APP_NAME


def test_client_enqueue_is_unclaimed(dbos: DBOS, client: DBOSClient) -> None:
    """A client has no application identity, so it writes unclaimed rows that the
    application serving the queue picks up."""
    Queue("client-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 10

    handle: WorkflowHandle[int] = client.enqueue(
        {"workflow_name": wf.__qualname__, "queue_name": "client-queue"}
    )
    assert handle.get_result() == 10
    # Nothing filters by owner yet, so the unclaimed row still runs here.
    assert application_name_of(dbos, handle.workflow_id) is None


def test_fork_inherits_owner(dbos: DBOS) -> None:
    @DBOS.workflow()
    def wf() -> int:
        return 11

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 11

    forked = DBOS.fork_workflow(handle.workflow_id, 1)
    assert forked.get_result() == 11
    assert application_name_of(dbos, forked.workflow_id) == APP_NAME


def test_queue_row_is_owned(dbos: DBOS) -> None:
    DBOS.register_queue("registered-queue")
    with dbos._sys_db.engine.begin() as c:
        owner = c.execute(
            sa.select(SystemSchema.queues.c.application_name).where(
                SystemSchema.queues.c.name == "registered-queue"
            )
        ).scalar()
    assert owner == APP_NAME


def test_schedule_row_is_owned(dbos: DBOS) -> None:
    @DBOS.workflow()
    def scheduled(scheduled_at: datetime, ctx: Any) -> None:
        pass

    DBOS.create_schedule(
        schedule_name="owned-schedule-row",
        workflow_fn=scheduled,
        schedule=daily_cron_far_from_now(),
    )
    schedule = dbos._sys_db.get_schedule("owned-schedule-row")
    assert schedule is not None
    assert schedule["application_name"] == APP_NAME


def test_application_version_row_is_owned(dbos: DBOS) -> None:
    versions = dbos._sys_db.list_application_versions()
    assert len(versions) == 1
    assert versions[0]["application_name"] == APP_NAME
    assert dbos._sys_db.get_latest_application_version()["application_name"] == APP_NAME


def test_status_reads_surface_owner(dbos: DBOS) -> None:
    @DBOS.workflow()
    def wf() -> int:
        return 12

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 12

    internal = dbos._sys_db.get_workflow_status(handle.workflow_id)
    assert internal is not None
    assert internal["application_name"] == APP_NAME

    listed = DBOS.list_workflows(workflow_ids=[handle.workflow_id])
    assert len(listed) == 1
    assert listed[0].application_name == APP_NAME
    # The trailing input/output columns must still line up after the new column.
    assert listed[0].output == 12
    assert listed[0].input is not None


def test_export_import_round_trips_owner(dbos: DBOS) -> None:
    @DBOS.workflow()
    def wf() -> int:
        return 13

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 13

    exported = dbos._sys_db.export_workflow(handle.workflow_id, export_children=False)
    assert exported[0]["workflow_status"]["application_name"] == APP_NAME

    dbos._sys_db.delete_workflows([handle.workflow_id])
    dbos._sys_db.import_workflow(exported)
    assert application_name_of(dbos, handle.workflow_id) == APP_NAME
