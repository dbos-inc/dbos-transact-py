"""Row ownership and the isolation it buys. Claiming (dequeue, recovery,
enumeration, bulk delete) takes own plus unclaimed; filters match an owner exactly."""

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pytest
import sqlalchemy as sa

from dbos import DBOS, DBOSClient, Queue, WorkflowHandle
from dbos._error import DBOSException
from dbos._schemas.system_database import SystemSchema
from dbos._utils import INTERNAL_QUEUE_NAME, GlobalParams

APP_NAME = "test-app"  # matches conftest.default_config
OTHER_APP = "other-app"
# After any row written during the test; epoch ms is ~1.7e12, so 2**40 is the past.
FUTURE_MS = int(datetime.now(timezone.utc).timestamp() * 1000) + 10**9


def daily_cron_far_from_now() -> str:
    # Daily cron pinned ~12 hours away, so it can't fire mid-test at any wall-clock time.
    t = datetime.now(timezone.utc) + timedelta(hours=12)
    return f"{t.minute} {t.hour} * * *"


def _column_of(dbos: DBOS, column: Any, workflow_id: str) -> Any:
    with dbos._sys_db.engine.begin() as c:
        return c.execute(
            sa.select(column).where(
                SystemSchema.workflow_status.c.workflow_uuid == workflow_id
            )
        ).scalar()


def application_name_of(dbos: DBOS, workflow_id: str) -> Any:
    return _column_of(
        dbos, SystemSchema.workflow_status.c.application_name, workflow_id
    )


def status_of(dbos: DBOS, workflow_id: str) -> Any:
    return _column_of(dbos, SystemSchema.workflow_status.c.status, workflow_id)


def workflow_exists(dbos: DBOS, workflow_id: str) -> bool:
    return (
        _column_of(dbos, SystemSchema.workflow_status.c.workflow_uuid, workflow_id)
        is not None
    )


def step_owners(dbos: DBOS, workflow_id: str) -> set[Any]:
    with dbos._sys_db.engine.begin() as c:
        return {
            r[0]
            for r in c.execute(
                sa.select(SystemSchema.operation_outputs.c.application_name).where(
                    SystemSchema.operation_outputs.c.workflow_uuid == workflow_id
                )
            )
        }


def insert_foreign_workflow(
    dbos: DBOS,
    workflow_id: str,
    *,
    status: str,
    queue_name: Optional[str] = None,
    application_name: Optional[str] = OTHER_APP,
) -> None:
    """A row owned by a second application, written directly rather than by
    launching another DBOS, which keeps these tests to one process."""
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.workflow_status).values(
                workflow_uuid=workflow_id,
                status=status,
                name="foreign_workflow",
                queue_name=queue_name,
                created_at=1,
                updated_at=1,
                priority=0,
                application_name=application_name,
                inputs='{"args": [], "kwargs": {}}',
            )
        )


def insert_foreign_step(
    dbos: DBOS, workflow_id: str, *, function_name: str, completed_at: int = 1
) -> None:
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.operation_outputs).values(
                workflow_uuid=workflow_id,
                function_id=1,
                function_name=function_name,
                completed_at_epoch_ms=completed_at,
                started_at_epoch_ms=completed_at,
                application_name=OTHER_APP,
            )
        )


# ── Stamping ──────────────────────────────────────────────────────────────────


def test_runtime_stamps_its_own_application(dbos: DBOS) -> None:
    """Ownership never depends on the target queue: one code path stamps
    everything this application starts or enqueues."""
    served = Queue("served-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 5

    workflow_ids = [DBOS.start_workflow(wf).workflow_id, served.enqueue(wf).workflow_id]
    # A queue this application serves and one it has never heard of: an earlier
    # design consulted the queue here, and left the unknown one unclaimed.
    for queue_name in ("served-queue", "never-served-queue"):
        workflow_ids.append(
            DBOS.enqueue_workflow_with_options(
                {"workflow_name": wf.__qualname__, "queue_name": queue_name}
            ).workflow_id
        )
    for workflow_id in workflow_ids:
        assert application_name_of(dbos, workflow_id) == APP_NAME, workflow_id


def test_explicit_application_name_wins(dbos: DBOS, client: DBOSClient) -> None:
    """Naming a target is the only way to enqueue across applications, from either
    the runtime or a client."""
    Queue("override-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 6

    def options() -> Any:
        return {
            "workflow_name": wf.__qualname__,
            "queue_name": "override-queue",
            "application_name": OTHER_APP,
        }

    from_runtime = DBOS.enqueue_workflow_with_options(options())
    from_client: WorkflowHandle[int] = client.enqueue(options())
    assert application_name_of(dbos, from_runtime.workflow_id) == OTHER_APP
    assert application_name_of(dbos, from_client.workflow_id) == OTHER_APP


def test_client_without_identity_writes_unclaimed_rows(
    dbos: DBOS, client: DBOSClient
) -> None:
    """A nameless client writes unclaimed rows, which whichever application runs
    them then claims, so the unclaimed partition drains on its own."""
    Queue("adopt-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 7

    # A queue nobody polls, so the row stays exactly as written.
    unpolled: WorkflowHandle[int] = client.enqueue(
        {"workflow_name": wf.__qualname__, "queue_name": "unpolled-queue"}
    )
    assert application_name_of(dbos, unpolled.workflow_id) is None

    polled: WorkflowHandle[int] = client.enqueue(
        {"workflow_name": wf.__qualname__, "queue_name": "adopt-queue"}
    )
    assert polled.get_result() == 7
    assert application_name_of(dbos, polled.workflow_id) == APP_NAME


def test_metadata_rows_are_owned(dbos: DBOS) -> None:
    """Queue, schedule, and version rows, plus the workflows a schedule fires —
    those land on the internal queue, whose shared name cannot route them."""

    @DBOS.workflow()
    def scheduled(scheduled_at: datetime, ctx: Any) -> None:
        pass

    DBOS.register_queue("registered-queue")
    DBOS.create_schedule(
        schedule_name="owned-schedule",
        workflow_fn=scheduled,
        schedule=daily_cron_far_from_now(),
    )
    with dbos._sys_db.engine.begin() as c:
        queue_owner = c.execute(
            sa.select(SystemSchema.queues.c.application_name).where(
                SystemSchema.queues.c.name == "registered-queue"
            )
        ).scalar()
    schedule = dbos._sys_db.get_schedule("owned-schedule")
    versions = dbos._sys_db.list_application_versions()
    assert queue_owner == APP_NAME
    assert schedule is not None and schedule["application_name"] == APP_NAME
    assert [v["application_name"] for v in versions] == [APP_NAME]

    # trigger_schedule enqueues through the same path the cron loop uses.
    fired = DBOS.trigger_schedule("owned-schedule")
    assert application_name_of(dbos, fired.workflow_id) == APP_NAME


def test_steps_carry_owner_and_forks_inherit_it(dbos: DBOS) -> None:
    @DBOS.step()
    def a_step() -> int:
        return 8

    @DBOS.workflow()
    def wf() -> int:
        return a_step()

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 8
    assert step_owners(dbos, handle.workflow_id) == {APP_NAME}

    # Forking past the step copies its row, which must carry the owner too.
    forked = DBOS.fork_workflow(handle.workflow_id, 2)
    assert forked.get_result() == 8
    assert application_name_of(dbos, forked.workflow_id) == APP_NAME
    assert step_owners(dbos, forked.workflow_id) == {APP_NAME}


# ── Reads ─────────────────────────────────────────────────────────────────────


def test_reads_surface_owner_and_ids_stay_global(dbos: DBOS) -> None:
    """Workflow IDs are unique, so identity reads are never scoped —
    cross-application get_result and status depend on it."""

    @DBOS.workflow()
    def wf() -> int:
        return 9

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 9

    internal = dbos._sys_db.get_workflow_status(handle.workflow_id)
    assert internal is not None and internal["application_name"] == APP_NAME

    listed = DBOS.list_workflows(workflow_ids=[handle.workflow_id])
    assert len(listed) == 1
    assert listed[0].application_name == APP_NAME
    # The trailing input/output columns must still line up after the new column.
    assert listed[0].output == 9
    assert listed[0].input is not None

    insert_foreign_workflow(dbos, "foreign-visible", status="SUCCESS")
    foreign = dbos._sys_db.get_workflow_status("foreign-visible")
    assert foreign is not None and foreign["application_name"] == OTHER_APP


def test_export_import_round_trips_owner(dbos: DBOS) -> None:
    @DBOS.workflow()
    def wf() -> int:
        return 10

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 10

    exported = dbos._sys_db.export_workflow(handle.workflow_id, export_children=False)
    assert exported[0]["workflow_status"]["application_name"] == APP_NAME

    dbos._sys_db.delete_workflows([handle.workflow_id])
    dbos._sys_db.import_workflow(exported)
    assert application_name_of(dbos, handle.workflow_id) == APP_NAME


def test_observability_filters_are_exact_and_optional(dbos: DBOS) -> None:
    """The four observability surfaces take an ordinary exact-match predicate,
    unset meaning every application, unlike the claiming scope."""

    @DBOS.step()
    def a_step() -> int:
        return 11

    @DBOS.workflow()
    def wf() -> int:
        return a_step()

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 11
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    insert_foreign_workflow(dbos, "foreign-listed", status="SUCCESS")
    insert_foreign_step(
        dbos, "foreign-listed", function_name="their_step", completed_at=now_ms
    )

    unfiltered = {w.workflow_id for w in DBOS.list_workflows()}
    assert {handle.workflow_id, "foreign-listed"} <= unfiltered
    mine = {w.workflow_id for w in DBOS.list_workflows(application_name=APP_NAME)}
    assert handle.workflow_id in mine and "foreign-listed" not in mine
    theirs = {w.workflow_id for w in DBOS.list_workflows(application_name=OTHER_APP)}
    assert theirs == {"foreign-listed"}

    aggregates = dbos._sys_db.get_workflow_aggregates(
        group_by_name=True, select_count=True, application_name=[OTHER_APP]
    )
    assert [r["group"]["name"] for r in aggregates] == ["foreign_workflow"]

    steps = dbos._sys_db.get_step_aggregates(
        group_by_function_name=True, select_count=True, application_name=[OTHER_APP]
    )
    assert [r["group"]["function_name"] for r in steps] == ["their_step"]

    window_start = datetime.fromtimestamp(0, timezone.utc).isoformat()
    window_end = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    metrics = dbos._sys_db.get_metrics(
        window_start, window_end, application_name=[OTHER_APP]
    )
    assert {m["metric_name"] for m in metrics if m["metric_type"] == "step_count"} == {
        "their_step"
    }


# ── Isolation between applications ────────────────────────────────────────────


def test_dequeue_skips_another_applications_workflow(dbos: DBOS) -> None:
    """The internal queue is the load-bearing case: its name is shared, so only
    ownership can keep one application off another's workflows."""
    queue = Queue("shared-name-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 12

    insert_foreign_workflow(
        dbos, "foreign-named", status="ENQUEUED", queue_name=queue.name
    )
    insert_foreign_workflow(
        dbos, "foreign-internal", status="ENQUEUED", queue_name=INTERNAL_QUEUE_NAME
    )
    # This application's own work on both queues still runs.
    assert queue.enqueue(wf).get_result() == 12
    assert (
        DBOS.enqueue_workflow_with_options(
            {"workflow_name": wf.__qualname__, "queue_name": INTERNAL_QUEUE_NAME}
        ).get_result()
        == 12
    )
    assert status_of(dbos, "foreign-named") == "ENQUEUED"
    assert status_of(dbos, "foreign-internal") == "ENQUEUED"


def test_recovery_skips_another_applications_workflow(dbos: DBOS) -> None:
    """executor_id defaults to the literal "local", so it collides across
    applications; ownership is what disambiguates recovery."""
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


def test_bulk_operations_spare_another_application(dbos: DBOS) -> None:
    """Both take own plus unclaimed rows. Unclaimed are included deliberately:
    excluding them would leak every pre-upgrade row forever."""
    from dbos._workflow_commands import global_timeout

    @DBOS.workflow()
    def wf() -> int:
        return 13

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 13
    insert_foreign_workflow(dbos, "foreign-inflight", status="ENQUEUED")
    insert_foreign_workflow(
        dbos, "unclaimed-inflight", status="ENQUEUED", application_name=None
    )
    insert_foreign_workflow(dbos, "foreign-old", status="SUCCESS")
    insert_foreign_workflow(
        dbos, "unclaimed-old", status="SUCCESS", application_name=None
    )
    cutoff = int(datetime.now(timezone.utc).timestamp() * 1000) + 1000

    global_timeout(dbos, cutoff)
    assert status_of(dbos, "foreign-inflight") == "ENQUEUED"
    assert status_of(dbos, "unclaimed-inflight") == "CANCELLED"

    dbos._sys_db.garbage_collect(
        cutoff_epoch_timestamp_ms=cutoff, rows_threshold=None, batch_size=None
    )
    assert workflow_exists(dbos, "foreign-old")
    assert not workflow_exists(dbos, "unclaimed-old")
    assert not workflow_exists(dbos, handle.workflow_id)


def test_enumeration_skips_another_applications_rows(dbos: DBOS) -> None:
    @DBOS.workflow()
    def scheduled(scheduled_at: datetime, ctx: Any) -> None:
        pass

    DBOS.create_schedule(
        schedule_name="mine", workflow_fn=scheduled, schedule=daily_cron_far_from_now()
    )
    DBOS.register_queue("mine-queue")
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
        c.execute(
            sa.insert(SystemSchema.queues).values(
                queue_id="foreign-queue-id",
                name="theirs-queue",
                created_at=1,
                updated_at=1,
                application_name=OTHER_APP,
            )
        )
    schedules = {s["schedule_name"] for s in dbos._sys_db.list_schedules()}
    queues = {q.name for q in dbos._sys_db.list_queues()}
    assert "mine" in schedules and "theirs" not in schedules
    assert "mine-queue" in queues and "theirs-queue" not in queues
    # Name-addressed lookups stay global: a globally unique name is an identity.
    assert dbos._sys_db.get_queue("theirs-queue") is not None


# ── Application versions ──────────────────────────────────────────────────────


def test_latest_version_is_scoped(dbos: DBOS) -> None:
    """Another application's deploy must not demote this one, but an unclaimed
    newer row is an older-SDK peer's deploy and really does."""
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
    assert (
        dbos._sys_db.get_latest_application_version()["version_name"]
        == GlobalParams.app_version
    )

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
    assert (
        dbos._sys_db.get_latest_application_version()["version_name"]
        == "unclaimed-version"
    )


def test_create_application_version_claims_only_unclaimed_rows(dbos: DBOS) -> None:
    """Claiming keeps a pinned application_version — whose string never changes,
    so no fresh owned row is ever minted — from staying unclaimed forever."""
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.application_versions).values(
                version_id="pinned-version-id",
                version_name="1.0.0",
                version_timestamp=7,
                created_at=7,
                application_name=None,
            )
        )
        c.execute(
            sa.insert(SystemSchema.application_versions).values(
                version_id="owned-version-id",
                version_name="2.0.0",
                version_timestamp=7,
                created_at=7,
                application_name=APP_NAME,
            )
        )
    dbos._sys_db.create_application_version("1.0.0")
    dbos._sys_db.create_application_version("2.0.0")

    with dbos._sys_db.engine.begin() as c:
        rows = {
            r[0]: (r[1], r[2], r[3])
            for r in c.execute(
                sa.select(
                    SystemSchema.application_versions.c.version_name,
                    SystemSchema.application_versions.c.application_name,
                    SystemSchema.application_versions.c.version_id,
                    SystemSchema.application_versions.c.version_timestamp,
                ).where(
                    SystemSchema.application_versions.c.version_name.in_(
                        ["1.0.0", "2.0.0"]
                    )
                )
            )
        }
    # Claimed, but neither recreated nor promoted to latest.
    assert rows["1.0.0"] == (APP_NAME, "pinned-version-id", 7)
    # Already owned, so re-registering is a no-op.
    assert rows["2.0.0"] == (APP_NAME, "owned-version-id", 7)


# ── Pre-upgrade rows and conflicts ────────────────────────────────────────────


def test_unclaimed_metadata_is_visible_then_claimed_on_registration(
    dbos: DBOS,
) -> None:
    """Nothing needs claiming in advance — the claiming scope already includes
    unclaimed rows — but re-registering claims them through the ordinary upsert."""
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
                schedule=daily_cron_far_from_now(),
                status="ACTIVE",
                context="null",
                application_name=None,
            )
        )
    assert "legacy-queue" in {q.name for q in dbos._sys_db.list_queues()}
    assert "legacy-schedule" in {
        s["schedule_name"] for s in dbos._sys_db.list_schedules()
    }

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
    # Claiming must preserve identity, not recreate the row.
    assert schedule_row == (APP_NAME, "legacy-schedule-id")


def test_conflicting_names_across_applications_raise(dbos: DBOS) -> None:
    """Names stay globally unique, so a collision is a conflict rather than a
    silent overwrite of another application's configuration."""

    @DBOS.workflow()
    def scheduled(scheduled_at: datetime, ctx: Any) -> None:
        pass

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
        DBOS.register_queue("conflict-queue")
    with pytest.raises(DBOSException, match="already registered by application"):
        DBOS.create_schedule(
            schedule_name="conflict-schedule",
            workflow_fn=scheduled,
            schedule=daily_cron_far_from_now(),
        )
    with pytest.raises(DBOSException, match="already registered by application"):
        dbos._sys_db.create_application_version("conflict-version")
