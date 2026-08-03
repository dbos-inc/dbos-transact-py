"""Ownership of system database rows, and the isolation it buys.

Two scoping rules are exercised here and are deliberately different. Claiming —
dequeue, recovery, enumeration, and the bulk delete/cancel — matches this
application's rows plus unclaimed ones, so upgrading never strands in-flight
work. The observability filters match an owner exactly and default to matching
every application.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pytest
import sqlalchemy as sa

from dbos import DBOS, DBOSClient, Queue, WorkflowHandle
from dbos._error import DBOSException
from dbos._schemas.system_database import SystemSchema
from dbos._utils import INTERNAL_QUEUE_NAME

from .conftest import retry_until_success

APP_NAME = "test-app"  # matches conftest.default_config
OTHER_APP = "other-app"
# Comfortably after any row DBOS writes during the test; epoch ms is ~1.7e12, so a
# literal like 2**40 would silently be in the past.
FUTURE_MS = int(datetime.now(timezone.utc).timestamp() * 1000) + 10**9


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
    """A client with no application identity writes unclaimed rows."""

    @DBOS.workflow()
    def wf() -> int:
        return 10

    # A queue no application polls, so the row stays exactly as written.
    handle: WorkflowHandle[int] = client.enqueue(
        {"workflow_name": wf.__qualname__, "queue_name": "unpolled-client-queue"}
    )
    assert application_name_of(dbos, handle.workflow_id) is None


def test_unclaimed_workflow_is_adopted_on_dequeue(
    dbos: DBOS, client: DBOSClient
) -> None:
    """Unclaimed rows are claimed by whichever application runs them, so the
    unclaimed partition drains instead of needing a bulk backfill."""
    Queue("client-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 10

    handle: WorkflowHandle[int] = client.enqueue(
        {"workflow_name": wf.__qualname__, "queue_name": "client-queue"}
    )
    assert handle.get_result() == 10
    assert application_name_of(dbos, handle.workflow_id) == APP_NAME


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


# ── Isolation between applications ────────────────────────────────────────────
#
# The dbos fixture's application is "test-app". These tests write rows owned by a
# second application directly, then assert "test-app" neither runs nor destroys
# them. Writing the foreign rows rather than launching a second DBOS keeps this to
# one process, which the shared test databases require.


def insert_foreign_workflow(
    dbos: DBOS,
    workflow_id: str,
    *,
    status: str,
    queue_name: Optional[str] = None,
    application_name: Optional[str] = OTHER_APP,
    created_at: int = 1,
) -> None:
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.workflow_status).values(
                workflow_uuid=workflow_id,
                status=status,
                name="foreign_workflow",
                queue_name=queue_name,
                created_at=created_at,
                updated_at=created_at,
                priority=0,
                application_name=application_name,
                inputs='{"args": [], "kwargs": {}}',
            )
        )


def workflow_exists(dbos: DBOS, workflow_id: str) -> bool:
    with dbos._sys_db.engine.begin() as c:
        return (
            c.execute(
                sa.select(SystemSchema.workflow_status.c.workflow_uuid).where(
                    SystemSchema.workflow_status.c.workflow_uuid == workflow_id
                )
            ).fetchone()
            is not None
        )


def test_dequeue_skips_another_applications_workflow(dbos: DBOS) -> None:
    queue = Queue("shared-name-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 20

    insert_foreign_workflow(
        dbos, "foreign-enqueued", status="ENQUEUED", queue_name=queue.name
    )
    # Something this application does own, on the same queue, must still run.
    handle = queue.enqueue(wf)
    assert handle.get_result() == 20

    with dbos._sys_db.engine.begin() as c:
        status = c.execute(
            sa.select(SystemSchema.workflow_status.c.status).where(
                SystemSchema.workflow_status.c.workflow_uuid == "foreign-enqueued"
            )
        ).scalar()
    assert status == "ENQUEUED"


def test_internal_queue_is_scoped(dbos: DBOS) -> None:
    """The internal queue's name is shared by every application, so ownership is
    the only thing that can keep one application off another's workflows."""

    @DBOS.workflow()
    def wf() -> int:
        return 21

    insert_foreign_workflow(
        dbos, "foreign-internal", status="ENQUEUED", queue_name=INTERNAL_QUEUE_NAME
    )
    handle = DBOS.enqueue_workflow_with_options(
        {"workflow_name": wf.__qualname__, "queue_name": INTERNAL_QUEUE_NAME}
    )
    assert handle.get_result() == 21

    with dbos._sys_db.engine.begin() as c:
        status = c.execute(
            sa.select(SystemSchema.workflow_status.c.status).where(
                SystemSchema.workflow_status.c.workflow_uuid == "foreign-internal"
            )
        ).scalar()
    assert status == "ENQUEUED"


def test_recovery_skips_another_applications_workflow(dbos: DBOS) -> None:
    """executor_id defaults to the literal "local", so it collides across
    applications; ownership is what disambiguates recovery."""
    from dbos._utils import GlobalParams

    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.workflow_status).values(
                workflow_uuid="foreign-pending",
                status="PENDING",
                name="foreign_workflow",
                created_at=1,
                updated_at=1,
                priority=0,
                executor_id=GlobalParams.executor_id,
                application_version=GlobalParams.app_version,
                application_name=OTHER_APP,
                inputs='{"args": [], "kwargs": {}}',
            )
        )
    pending = dbos._sys_db.get_pending_workflows(
        GlobalParams.executor_id, GlobalParams.app_version
    )
    assert "foreign-pending" not in [p.workflow_id for p in pending]


def test_garbage_collect_spares_another_application(dbos: DBOS) -> None:
    """GC collects this application's rows and unclaimed ones — excluding
    unclaimed would leak every pre-upgrade row forever — but never another
    application's."""

    @DBOS.workflow()
    def wf() -> int:
        return 22

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 22

    insert_foreign_workflow(dbos, "foreign-old", status="SUCCESS")
    insert_foreign_workflow(
        dbos, "unclaimed-old", status="SUCCESS", application_name=None
    )

    dbos._sys_db.garbage_collect(
        cutoff_epoch_timestamp_ms=int(datetime.now(timezone.utc).timestamp() * 1000)
        + 1000,
        rows_threshold=None,
        batch_size=None,
    )

    assert workflow_exists(dbos, "foreign-old")
    assert not workflow_exists(dbos, "unclaimed-old")
    assert not workflow_exists(dbos, handle.workflow_id)


def test_global_timeout_spares_another_application(dbos: DBOS) -> None:
    from dbos._workflow_commands import global_timeout

    insert_foreign_workflow(dbos, "foreign-inflight", status="ENQUEUED")
    insert_foreign_workflow(
        dbos, "unclaimed-inflight", status="ENQUEUED", application_name=None
    )

    global_timeout(dbos, int(datetime.now(timezone.utc).timestamp() * 1000) + 1000)

    def status_of(workflow_id: str) -> Any:
        with dbos._sys_db.engine.begin() as c:
            return c.execute(
                sa.select(SystemSchema.workflow_status.c.status).where(
                    SystemSchema.workflow_status.c.workflow_uuid == workflow_id
                )
            ).scalar()

    assert status_of("foreign-inflight") == "ENQUEUED"
    assert status_of("unclaimed-inflight") == "CANCELLED"


def test_scheduler_skips_another_applications_schedule(dbos: DBOS) -> None:
    @DBOS.workflow()
    def scheduled(scheduled_at: datetime, ctx: Any) -> None:
        pass

    DBOS.create_schedule(
        schedule_name="mine",
        workflow_fn=scheduled,
        schedule=daily_cron_far_from_now(),
    )
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.workflow_schedules).values(
                schedule_id="foreign-schedule-id",
                schedule_name="theirs",
                workflow_name="foreign_workflow",
                schedule="* * * * *",
                status="ACTIVE",
                context="null",
                application_name=OTHER_APP,
            )
        )
    names = {s["schedule_name"] for s in dbos._sys_db.list_schedules()}
    assert "mine" in names
    assert "theirs" not in names


def test_queue_thread_skips_another_applications_queue(dbos: DBOS) -> None:
    DBOS.register_queue("mine-queue")
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.queues).values(
                queue_id="foreign-queue-id",
                name="theirs-queue",
                created_at=1,
                updated_at=1,
                application_name=OTHER_APP,
            )
        )
    names = {q.name for q in dbos._sys_db.list_queues()}
    assert "mine-queue" in names
    assert "theirs-queue" not in names
    # Name-addressed lookups stay global: a unique name is an identity.
    assert dbos._sys_db.get_queue("theirs-queue") is not None


def test_another_applications_version_does_not_demote(dbos: DBOS) -> None:
    """Another application deploying must not make this one look stale, which is
    what makes it stop dequeueing version-less workflows."""
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.application_versions).values(
                version_id="foreign-version-id",
                version_name="foreign-version",
                version_timestamp=FUTURE_MS,
                created_at=FUTURE_MS,
                application_name=OTHER_APP,
            )
        )
    from dbos._utils import GlobalParams

    latest = dbos._sys_db.get_latest_application_version()
    assert latest["version_name"] == GlobalParams.app_version


def test_unclaimed_newer_version_does_demote(dbos: DBOS) -> None:
    """An unclaimed version row that is newer means an older-SDK peer deployed
    after this worker, so it really is stale. Excluding these would break rolling
    upgrades, where the peer's rows carry no owner."""
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.application_versions).values(
                version_id="unclaimed-version-id",
                version_name="unclaimed-version",
                version_timestamp=FUTURE_MS,
                created_at=FUTURE_MS,
                application_name=None,
            )
        )
    latest = dbos._sys_db.get_latest_application_version()
    assert latest["version_name"] == "unclaimed-version"


def test_set_latest_version_adopts_a_legacy_row(dbos: DBOS) -> None:
    """Naming a version explicitly is an operator action, so it claims the row —
    which is what makes rolling back to a pre-upgrade version possible at all."""
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.application_versions).values(
                version_id="legacy-version-id",
                version_name="legacy-version",
                version_timestamp=1,
                created_at=1,
                application_name=None,
            )
        )
    dbos._sys_db.update_application_version_timestamp("legacy-version", FUTURE_MS)
    latest = dbos._sys_db.get_latest_application_version()
    assert latest["version_name"] == "legacy-version"
    assert latest["application_name"] == APP_NAME


# ── Cross-application operations that must keep working ───────────────────────


def test_id_addressed_reads_are_global(dbos: DBOS) -> None:
    """Workflow IDs are unique, so identity operations are never scoped —
    cross-application get_result and status depend on it."""
    insert_foreign_workflow(dbos, "foreign-visible", status="SUCCESS")
    status = dbos._sys_db.get_workflow_status("foreign-visible")
    assert status is not None
    assert status["application_name"] == OTHER_APP


def test_enqueue_for_another_application(dbos: DBOS) -> None:
    """Naming the target is the only way to enqueue across applications, so the
    row must not be claimed by this one."""
    Queue("cross-app-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 23

    handle = DBOS.enqueue_workflow_with_options(
        {
            "workflow_name": wf.__qualname__,
            "queue_name": "cross-app-queue",
            "application_name": OTHER_APP,
        }
    )
    assert application_name_of(dbos, handle.workflow_id) == OTHER_APP


# ── Adoption ──────────────────────────────────────────────────────────────────


def test_registration_adopts_unclaimed_rows(dbos: DBOS) -> None:
    """Adoption is per-name, so an application only ever claims rows for names it
    declares itself."""
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.queues).values(
                queue_id="legacy-queue-id",
                name="legacy-queue",
                created_at=1,
                updated_at=1,
                application_name=None,
            )
        )
        c.execute(
            sa.insert(SystemSchema.workflow_schedules).values(
                schedule_id="legacy-schedule-id",
                schedule_name="legacy-schedule",
                workflow_name="scheduled",
                schedule="* * * * *",
                status="ACTIVE",
                context="null",
                application_name=None,
            )
        )

    @DBOS.workflow()
    def scheduled(scheduled_at: datetime, ctx: Any) -> None:
        pass

    DBOS.register_queue("legacy-queue")
    DBOS.apply_schedules(
        [
            {
                "schedule_name": "legacy-schedule",
                "workflow_fn": scheduled,
                "schedule": daily_cron_far_from_now(),
            }
        ]
    )

    with dbos._sys_db.engine.begin() as c:
        queue_owner = c.execute(
            sa.select(SystemSchema.queues.c.application_name).where(
                SystemSchema.queues.c.name == "legacy-queue"
            )
        ).scalar()
        schedule_row = c.execute(
            sa.select(
                SystemSchema.workflow_schedules.c.application_name,
                SystemSchema.workflow_schedules.c.schedule_id,
            ).where(
                SystemSchema.workflow_schedules.c.schedule_name == "legacy-schedule"
            )
        ).fetchone()
    assert queue_owner == APP_NAME
    assert schedule_row is not None
    assert schedule_row[0] == APP_NAME
    # Adoption must preserve identity, not recreate the row.
    assert schedule_row[1] == "legacy-schedule-id"


# ── Ownership conflicts ───────────────────────────────────────────────────────


def test_queue_owned_by_another_application_raises(dbos: DBOS) -> None:
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.queues).values(
                queue_id="conflict-queue-id",
                name="conflict-queue",
                created_at=1,
                updated_at=1,
                application_name=OTHER_APP,
            )
        )
    with pytest.raises(DBOSException, match="already registered by application"):
        DBOS.register_queue("conflict-queue")


def test_schedule_owned_by_another_application_raises(dbos: DBOS) -> None:
    @DBOS.workflow()
    def scheduled(scheduled_at: datetime, ctx: Any) -> None:
        pass

    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.workflow_schedules).values(
                schedule_id="conflict-schedule-id",
                schedule_name="conflict-schedule",
                workflow_name="scheduled",
                schedule="* * * * *",
                status="ACTIVE",
                context="null",
                application_name=OTHER_APP,
            )
        )
    with pytest.raises(DBOSException, match="already registered by application"):
        DBOS.create_schedule(
            schedule_name="conflict-schedule",
            workflow_fn=scheduled,
            schedule=daily_cron_far_from_now(),
        )


def test_version_owned_by_another_application_raises(dbos: DBOS) -> None:
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.application_versions).values(
                version_id="conflict-version-id",
                version_name="conflict-version",
                version_timestamp=1,
                created_at=1,
                application_name=OTHER_APP,
            )
        )
    with pytest.raises(DBOSException, match="already registered by application"):
        dbos._sys_db.create_application_version("conflict-version")


# ── Observability filters: exact match, no default ────────────────────────────


def test_list_workflows_filter_is_exact_and_optional(dbos: DBOS) -> None:
    @DBOS.workflow()
    def wf() -> int:
        return 24

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 24
    insert_foreign_workflow(dbos, "foreign-listed", status="SUCCESS")

    unfiltered = {w.workflow_id for w in DBOS.list_workflows()}
    assert {handle.workflow_id, "foreign-listed"} <= unfiltered

    mine = {w.workflow_id for w in DBOS.list_workflows(application_name=APP_NAME)}
    assert handle.workflow_id in mine
    assert "foreign-listed" not in mine

    theirs = {w.workflow_id for w in DBOS.list_workflows(application_name=OTHER_APP)}
    assert theirs == {"foreign-listed"}


def test_aggregates_filter_by_application(dbos: DBOS) -> None:
    @DBOS.workflow()
    def wf() -> int:
        return 25

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 25
    insert_foreign_workflow(dbos, "foreign-aggregated", status="SUCCESS")

    rows = dbos._sys_db.get_workflow_aggregates(
        group_by_name=True, select_count=True, application_name=[OTHER_APP]
    )
    assert [r["group"]["name"] for r in rows] == ["foreign_workflow"]
