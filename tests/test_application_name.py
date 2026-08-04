"""Row ownership and the isolation it buys. Claiming (dequeue, recovery,
enumeration, bulk delete) takes own plus unclaimed; filters match an owner exactly."""

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pytest
import sqlalchemy as sa

import dbos._conductor.protocol as p
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
    dbos: DBOS,
    workflow_id: str,
    *,
    function_name: str,
    completed_at: int = 1,
    application_name: Optional[str] = OTHER_APP,
) -> None:
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.operation_outputs).values(
                workflow_uuid=workflow_id,
                function_id=1,
                function_name=function_name,
                completed_at_epoch_ms=completed_at,
                started_at_epoch_ms=completed_at,
                application_name=application_name,
            )
        )


# ── Stamping ──────────────────────────────────────────────────────────────────


def test_runtime_stamps_everything_it_writes(dbos: DBOS) -> None:
    """Workflows, whatever queue they target, and the metadata rows that route
    them. Ownership never consults the queue, which for the internal one is
    shared and so cannot route anything."""
    served = Queue("served-queue")

    @DBOS.workflow()
    def wf() -> int:
        return 5

    @DBOS.workflow()
    def scheduled(scheduled_at: datetime, ctx: Any) -> None:
        pass

    workflow_ids = [DBOS.start_workflow(wf).workflow_id, served.enqueue(wf).workflow_id]
    # A known queue and an unknown one: ownership is stamped the same either way.
    for queue_name in ("served-queue", "never-served-queue"):
        workflow_ids.append(
            DBOS.enqueue_workflow_with_options(
                {"workflow_name": wf.__qualname__, "queue_name": queue_name}
            ).workflow_id
        )

    DBOS.register_queue("registered-queue")
    DBOS.create_schedule(
        schedule_name="owned-schedule",
        workflow_fn=scheduled,
        schedule=daily_cron_far_from_now(),
    )
    # trigger_schedule enqueues through the same path the cron loop uses.
    workflow_ids.append(DBOS.trigger_schedule("owned-schedule").workflow_id)
    for workflow_id in workflow_ids:
        assert application_name_of(dbos, workflow_id) == APP_NAME, workflow_id

    with dbos._sys_db.engine.begin() as c:
        queue_owner = c.execute(
            sa.select(SystemSchema.queues.c.application_name).where(
                SystemSchema.queues.c.name == "registered-queue"
            )
        ).scalar()
    schedule = dbos._sys_db.get_schedule("owned-schedule")
    assert queue_owner == APP_NAME
    assert schedule is not None and schedule["application_name"] == APP_NAME
    assert [
        v["application_name"] for v in dbos._sys_db.list_application_versions()
    ] == [APP_NAME]


def test_destroy_clears_the_application_identity(dbos: DBOS) -> None:
    """Identity is set at launch, so a relaunch under another name must not
    inherit this one from the process."""
    assert GlobalParams.app_name == APP_NAME
    DBOS.destroy()
    assert GlobalParams.app_name is None


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


def test_observability_filters_include_unclaimed_rows(dbos: DBOS) -> None:
    """All four surfaces follow the one rule: unset lists every application, and
    naming one lists its rows plus unclaimed ones."""

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
    insert_foreign_workflow(
        dbos, "unclaimed-listed", status="SUCCESS", application_name=None
    )

    unfiltered = {w.workflow_id for w in DBOS.list_workflows()}
    assert {handle.workflow_id, "foreign-listed", "unclaimed-listed"} <= unfiltered
    mine = {w.workflow_id for w in DBOS.list_workflows(application_name=APP_NAME)}
    assert {handle.workflow_id, "unclaimed-listed"} <= mine
    assert "foreign-listed" not in mine
    theirs = {w.workflow_id for w in DBOS.list_workflows(application_name=OTHER_APP)}
    assert theirs == {"foreign-listed", "unclaimed-listed"}

    aggregates = dbos._sys_db.get_workflow_aggregates(
        group_by_name=True, select_count=True, application_name=[OTHER_APP]
    )
    assert [r["group"]["name"] for r in aggregates] == ["foreign_workflow"]

    # Grouping partitions where the filter deliberately overlaps.
    grouped = dbos._sys_db.get_workflow_aggregates(
        group_by_application_name=True, select_count=True
    )
    assert {r["group"]["application_name"] for r in grouped} == {
        APP_NAME,
        OTHER_APP,
        None,
    }

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

    # The Conductor's metrics fan-out is per-application, so its request must carry the predicate.
    fields = {
        "type": "get_metrics",
        "request_id": "r",
        "start_time": window_start,
        "end_time": window_end,
        "metric_class": "workflow_step_count",
    }
    assert p.GetMetricsRequest.from_json(
        json.dumps({**fields, "application_name": [OTHER_APP]})
    ).application_name == [OTHER_APP]
    assert p.GetMetricsRequest.from_json(json.dumps(fields)).application_name is None


# ── Isolation between applications ────────────────────────────────────────────


def test_claiming_skips_another_applications_workflows(dbos: DBOS) -> None:
    """Dequeue and recovery, the two ways a row gets picked up. Both collide
    across applications by default — the internal queue's name is shared and
    executor_id is the literal "local" — so only ownership separates them."""
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

    # Recovery matches on executor and version, which another application shares.
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


def test_unclaimed_rows_belong_to_every_application(
    dbos: DBOS, client: DBOSClient
) -> None:
    """One rule everywhere: naming an application lists its rows plus unclaimed
    ones. Re-registering an unclaimed row claims it through the ordinary upsert."""

    @DBOS.workflow()
    def scheduled(scheduled_at: datetime, ctx: Any) -> None:
        pass

    DBOS.create_schedule(
        schedule_name="mine", workflow_fn=scheduled, schedule=daily_cron_far_from_now()
    )
    DBOS.register_queue("mine-queue")
    with dbos._sys_db.engine.begin() as c:
        for schedule_id, name, owner in (
            ("foreign-schedule-id", "theirs", OTHER_APP),
            ("legacy-schedule-id", "unclaimed", None),
        ):
            c.execute(
                sa.insert(SystemSchema.workflow_schedules).values(
                    schedule_id=schedule_id,
                    schedule_name=name,
                    workflow_name="scheduled",
                    schedule=daily_cron_far_from_now(),
                    status="ACTIVE",
                    context="null",
                    application_name=owner,
                )
            )
        for queue_id, name, owner in (
            ("foreign-queue-id", "theirs-queue", OTHER_APP),
            ("legacy-queue-id", "unclaimed-queue", None),
        ):
            c.execute(
                sa.insert(SystemSchema.queues).values(
                    queue_id=queue_id,
                    name=name,
                    created_at=1,
                    updated_at=1,
                    application_name=owner,
                )
            )

    # Unset lists every application.
    assert {"mine", "theirs", "unclaimed"} <= {
        s["schedule_name"] for s in DBOS.list_schedules()
    }
    assert {"mine-queue", "theirs-queue", "unclaimed-queue"} <= {
        q.name for q in DBOS.list_queues()
    }

    # Naming an application adds the unclaimed rows, as the loops themselves do.
    assert {
        s["schedule_name"] for s in DBOS.list_schedules(application_name=APP_NAME)
    } == {"mine", "unclaimed"}
    assert {q.name for q in DBOS.list_queues(application_name=APP_NAME)} == {
        "mine-queue",
        "unclaimed-queue",
    }
    theirs = DBOS.list_queues(application_name=OTHER_APP)
    assert {q.name for q in theirs} == {"theirs-queue", "unclaimed-queue"}
    # The listing is attributable: a Queue carries its owner.
    assert {q.application_name for q in theirs} == {OTHER_APP, None}
    # The client runs the same query, so it takes the same filter.
    assert {q.name for q in client.list_queues(application_name=OTHER_APP)} == {
        "theirs-queue",
        "unclaimed-queue",
    }

    # A read-through handle picks up ownership along with the rest of the row.
    handle = Queue("theirs-queue", database_backed_queue=True)
    assert handle.application_name is None
    assert handle.concurrency is None
    assert handle.application_name == OTHER_APP

    # Name-addressed lookups stay global: a globally unique name is an identity.
    assert dbos._sys_db.get_queue("theirs-queue") is not None

    # Re-registering an unclaimed row claims it, without recreating it.
    DBOS.register_queue("unclaimed-queue")
    DBOS.apply_schedules(
        [
            {
                "schedule_name": "unclaimed",
                "workflow_fn": scheduled,
                "schedule": daily_cron_far_from_now(),
            }
        ]
    )
    with dbos._sys_db.engine.begin() as c:
        queue_owner = c.execute(
            sa.select(SystemSchema.queues.c.application_name).where(
                SystemSchema.queues.c.name == "unclaimed-queue"
            )
        ).scalar()
        schedule_row = c.execute(
            sa.select(
                SystemSchema.workflow_schedules.c.application_name,
                SystemSchema.workflow_schedules.c.schedule_id,
            ).where(SystemSchema.workflow_schedules.c.schedule_name == "unclaimed")
        ).fetchone()
    assert queue_owner == APP_NAME
    assert schedule_row == (APP_NAME, "legacy-schedule-id")


# ── Application versions ──────────────────────────────────────────────────────


def test_versions_are_per_application(dbos: DBOS, client: DBOSClient) -> None:
    """Version names stay global addresses, so a row records which application
    registered it. Another application's deploy must not demote this one, but an
    unclaimed newer row is an older SDK's and does."""
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

    # A name another application holds is a collision, so the caller's launch fails.
    with pytest.raises(DBOSException, match="already registered by application"):
        dbos._sys_db.create_application_version("1.0.0", application_name=OTHER_APP)

    # Promoting a name another application owns is a collision, not a retiming.
    with pytest.raises(DBOSException, match="already registered by application"):
        dbos._sys_db.update_application_version_timestamp(
            "1.0.0", FUTURE_MS + 1, application_name=OTHER_APP
        )

    # This application owns the row, so it promotes it, past the unclaimed one.
    dbos._sys_db.update_application_version_timestamp("1.0.0", FUTURE_MS + 1)
    assert dbos._sys_db.get_latest_application_version()["version_name"] == "1.0.0"

    def owner_of(version_name: str) -> Any:
        with dbos._sys_db.engine.begin() as c:
            return c.execute(
                sa.select(SystemSchema.application_versions.c.application_name).where(
                    SystemSchema.application_versions.c.version_name == version_name
                )
            ).scalar()

    # A nameless client administers any row without taking it.
    client.set_latest_application_version("1.0.0")
    assert owner_of("1.0.0") == APP_NAME

    # Promotion claims an unclaimed row, which would otherwise be every peer's latest.
    dbos._sys_db.update_application_version_timestamp(
        "unclaimed-version", FUTURE_MS + 2
    )
    assert owner_of("unclaimed-version") == APP_NAME


# ── Pre-upgrade rows and conflicts ────────────────────────────────────────────


def test_conflicting_names_across_applications_raise(
    dbos: DBOS, client: DBOSClient
) -> None:
    """Queue and schedule names stay globally unique — they are addresses — so a
    collision is a conflict rather than a silent overwrite. A writer with no
    identity of its own is exempt: it administers any row without taking it."""

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
    with pytest.raises(DBOSException, match="already registered by application"):
        DBOS.register_queue("conflict-queue")
    with pytest.raises(DBOSException, match="already registered by application"):
        DBOS.create_schedule(
            schedule_name="conflict-schedule",
            workflow_fn=scheduled,
            schedule=daily_cron_far_from_now(),
        )

    def conflict_queue() -> Any:
        with dbos._sys_db.engine.begin() as c:
            return c.execute(
                sa.select(
                    SystemSchema.queues.c.application_name,
                    SystemSchema.queues.c.concurrency,
                ).where(SystemSchema.queues.c.name == "conflict-queue")
            ).one()

    def upsert(sys_db: Any, concurrency: Optional[int], update_existing: bool) -> bool:
        return bool(
            sys_db.upsert_queue(
                name="conflict-queue",
                concurrency=concurrency,
                worker_concurrency=None,
                rate_limit_max=None,
                rate_limit_period_sec=None,
                priority_enabled=False,
                partition_queue=False,
                polling_interval_sec=1.0,
                update_existing=update_existing,
            )
        )

    # Declining to update is still a collision: only the owner ever polls the queue.
    with pytest.raises(DBOSException, match="already registered by application"):
        upsert(dbos._sys_db, 3, update_existing=False)
    assert conflict_queue() == (OTHER_APP, None)

    # A nameless writer updates the row without stripping the owner off it.
    assert not upsert(client._sys_db, 7, update_existing=True)
    assert conflict_queue() == (OTHER_APP, 7)

    # Same through the schedule upsert's update clause, the other way to strip one.
    new_cron = daily_cron_far_from_now()
    client.apply_schedules(
        [
            {
                "schedule_name": "conflict-schedule",
                "workflow_name": "scheduled",
                "schedule": new_cron,
            }
        ]
    )
    with dbos._sys_db.engine.begin() as c:
        assert c.execute(
            sa.select(
                SystemSchema.workflow_schedules.c.application_name,
                SystemSchema.workflow_schedules.c.schedule,
            ).where(
                SystemSchema.workflow_schedules.c.schedule_name == "conflict-schedule"
            )
        ).one() == (OTHER_APP, new_cron)


def test_cli_filters_by_application(dbos: DBOS, config: Any) -> None:
    """Click binds options to parameters by name, which mypy cannot check, so the
    two list commands need one real invocation."""
    from click.testing import CliRunner

    from dbos.cli.cli import app

    @DBOS.workflow()
    def wf() -> int:
        return 14

    handle = DBOS.start_workflow(wf)
    assert handle.get_result() == 14
    insert_foreign_workflow(dbos, "cli-foreign", status="SUCCESS")

    runner = CliRunner()
    for command in (["workflow", "list"], ["workflow", "queue", "list"]):
        result = runner.invoke(
            app, command + ["-s", config["system_database_url"], "-a", OTHER_APP]
        )
        assert result.exit_code == 0, result.output
        assert handle.workflow_id not in result.output
    listed = runner.invoke(
        app,
        ["workflow", "list", "-s", config["system_database_url"], "-a", OTHER_APP],
    )
    assert "cli-foreign" in listed.output


# ── Two applications, one system database ─────────────────────────────────────


def test_two_applications_share_one_system_database(dbos: DBOS, config: Any) -> None:
    """The only test with genuine second and third applications. DBOS is a process
    singleton, so each peer is its own SystemDatabase against the same database."""
    from dbos._serialization import DefaultSerializer
    from dbos._sys_db import SystemDatabase

    url = config["system_database_url"]
    names = ("app-a", "app-b")
    peers = {
        name: SystemDatabase.create(
            system_database_url=url,
            engine_kwargs={},
            engine=None,
            schema=dbos._sys_db.schema,
            serializer=DefaultSerializer(),
            executor_id=f"exec-{name}",
            use_listen_notify=False,
            app_name=name,
        )
        for name in names
    }
    clients = {
        name: DBOSClient(system_database_url=url, application_name=name)
        for name in names
    }
    try:
        enqueued = {}
        for name in names:
            peers[name].upsert_queue(
                name=f"{name}-queue",
                concurrency=None,
                worker_concurrency=None,
                rate_limit_max=None,
                rate_limit_period_sec=None,
                priority_enabled=False,
                partition_queue=False,
                polling_interval_sec=1.0,
                update_existing=True,
            )
            # All on the internal queue, the one name every application shares, so only ownership routes it.
            handle: WorkflowHandle[int] = clients[name].enqueue(
                {"workflow_name": "wf", "queue_name": INTERNAL_QUEUE_NAME}
            )
            enqueued[name] = handle.workflow_id
            assert application_name_of(dbos, handle.workflow_id) == name

        # Each application dequeues its own work and only its own.
        internal = Queue(INTERNAL_QUEUE_NAME, database_backed_queue=True)
        for name in names:
            dequeued = peers[name].start_queued_workflows(
                internal, f"exec-{name}", f"version-{name}", None, 0
            )
            assert dequeued == [enqueued[name]], name

        # Registration made each queue owned, so neither application enumerates the other's.
        for name in names:
            visible = {q.name for q in peers[name].list_queues(application_name=name)}
            assert f"{name}-queue" in visible
            assert not {f"{other}-queue" for other in names if other != name} & visible
    finally:
        for c in clients.values():
            c.destroy()
        for p in peers.values():
            p.destroy()


# ── Renaming an application ───────────────────────────────────────────────────


RENAMED_APP = "renamed-app"


def insert_foreign_control_plane(dbos: DBOS, suffix: str = "") -> None:
    """A queue, a schedule, and a version, all held by OTHER_APP."""
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.queues).values(
                queue_id=f"rename-queue-id{suffix}",
                name=f"rename-queue{suffix}",
                created_at=1,
                updated_at=1,
                application_name=OTHER_APP,
            )
        )
        c.execute(
            sa.insert(SystemSchema.workflow_schedules).values(
                schedule_id=f"rename-schedule-id{suffix}",
                schedule_name=f"rename-schedule{suffix}",
                workflow_name="foreign_workflow",
                schedule=daily_cron_far_from_now(),
                status="ACTIVE",
                context="null",
                application_name=OTHER_APP,
            )
        )
        c.execute(
            sa.insert(SystemSchema.application_versions).values(
                version_id=f"rename-version-id{suffix}",
                version_name=f"rename-version{suffix}",
                version_timestamp=7,
                created_at=7,
                application_name=OTHER_APP,
            )
        )


def owners_of_everything(dbos: DBOS) -> dict[str, set[Any]]:
    """Every owner recorded in each of the five owned tables."""
    tables = {
        "queues": SystemSchema.queues,
        "schedules": SystemSchema.workflow_schedules,
        "versions": SystemSchema.application_versions,
        "workflows": SystemSchema.workflow_status,
        "steps": SystemSchema.operation_outputs,
    }
    with dbos._sys_db.engine.begin() as c:
        return {
            label: {r[0] for r in c.execute(sa.select(table.c.application_name))}
            for label, table in tables.items()
        }


def test_rename_moves_every_owned_table(dbos: DBOS) -> None:
    """Ownership lives on the rows, so renaming an application is a re-own of all
    five tables. Counts are reported per table so an operator can check the move."""
    insert_foreign_workflow(dbos, "rename-terminal", status="SUCCESS")
    insert_foreign_workflow(dbos, "rename-inflight", status="ENQUEUED")
    insert_foreign_step(dbos, "rename-terminal", function_name="step")
    insert_foreign_control_plane(dbos)

    assert dbos._sys_db.rename_application(OTHER_APP, RENAMED_APP) == {
        "queues": 1,
        "schedules": 1,
        "versions": 1,
        # Both the ENQUEUED row, moved atomically, and the terminal one, moved in batches.
        "workflows": 2,
        "steps": 1,
    }

    # Nothing is left behind in any table.
    for label, owners in owners_of_everything(dbos).items():
        assert OTHER_APP not in owners, label
        assert RENAMED_APP in owners, label


def test_rename_spares_peers_and_unclaimed_rows(dbos: DBOS) -> None:
    """Only the named application moves. Unclaimed rows belong to everyone, so a
    rename must not quietly adopt them."""

    @DBOS.workflow()
    def mine() -> int:
        return 5

    insert_foreign_workflow(dbos, "rename-mine", status="SUCCESS")
    insert_foreign_workflow(
        dbos, "rename-unclaimed", status="SUCCESS", application_name=None
    )
    handle: WorkflowHandle[int] = DBOS.start_workflow(mine)
    assert handle.get_result() == 5

    dbos._sys_db.rename_application(OTHER_APP, RENAMED_APP)

    assert application_name_of(dbos, "rename-mine") == RENAMED_APP
    assert application_name_of(dbos, "rename-unclaimed") is None
    # The live application is untouched, so it still dequeues and recovers its own work.
    assert application_name_of(dbos, handle.workflow_id) == APP_NAME


def test_rename_batches_terminal_rows_and_resumes(dbos: DBOS) -> None:
    """Terminal workflows and steps move in batches, so a long history does not run
    in one transaction. The predicate shrinks as it goes, so a re-run resumes."""
    for i in range(5):
        insert_foreign_workflow(dbos, f"rename-batched-{i}", status="SUCCESS")
        insert_foreign_step(dbos, f"rename-batched-{i}", function_name="step")

    moved = dbos._sys_db.rename_application(OTHER_APP, RENAMED_APP, batch_size=1)
    assert moved["workflows"] == 5
    assert moved["steps"] == 5

    # Idempotent: a second pass finds nothing, which is what makes an interrupted one resumable.
    again = dbos._sys_db.rename_application(OTHER_APP, RENAMED_APP, batch_size=1)
    assert again["workflows"] == 0 and again["steps"] == 0
    for i in range(5):
        assert application_name_of(dbos, f"rename-batched-{i}") == RENAMED_APP


def test_rename_rejects_names_an_application_could_not_use(dbos: DBOS) -> None:
    """A rename to a name the config validator rejects would strand the rows again,
    since no application could ever launch under it."""
    with pytest.raises(DBOSException, match="Invalid application name"):
        dbos._sys_db.rename_application(OTHER_APP, "No Spaces Allowed")
    with pytest.raises(DBOSException, match="Invalid application name"):
        dbos._sys_db.rename_application(OTHER_APP, "ab")
    with pytest.raises(DBOSException, match="already holds that name"):
        dbos._sys_db.rename_application(OTHER_APP, OTHER_APP)
    with pytest.raises(DBOSException, match="cannot be empty"):
        dbos._sys_db.rename_application("", RENAMED_APP)
    # Neither source named: a no-op that is almost certainly a mistake.
    with pytest.raises(DBOSException, match="Nothing to re-own"):
        dbos._sys_db.rename_application(None, RENAMED_APP)
    with pytest.raises(ValueError, match="batch_size"):
        dbos._sys_db.rename_application(OTHER_APP, RENAMED_APP, batch_size=0)


def test_unclaimed_rows_are_adopted_only_when_asked(dbos: DBOS) -> None:
    """Unclaimed rows belong to every application, so a rename leaves them alone
    unless it is told to take them from the peers that share them."""
    insert_foreign_workflow(dbos, "adopt-me", status="SUCCESS", application_name=None)
    insert_foreign_workflow(dbos, "rename-me", status="SUCCESS")

    moved = dbos._sys_db.rename_application(OTHER_APP, RENAMED_APP)
    assert moved["workflows"] == 1
    assert application_name_of(dbos, "rename-me") == RENAMED_APP
    assert application_name_of(dbos, "adopt-me") is None

    adopted = dbos._sys_db.rename_application(
        RENAMED_APP, "adopting-app", adopt_unclaimed_rows=True
    )
    assert adopted["workflows"] == 2
    assert application_name_of(dbos, "adopt-me") == "adopting-app"
    assert application_name_of(dbos, "rename-me") == "adopting-app"


def test_unclaimed_rows_can_be_adopted_without_a_rename(dbos: DBOS) -> None:
    """Taking over a system database whose rows predate ownership is an adoption with
    no old name to move, so the previous name is optional."""
    insert_foreign_workflow(dbos, "legacy-wf", status="SUCCESS", application_name=None)
    insert_foreign_step(dbos, "legacy-wf", function_name="step", application_name=None)
    insert_foreign_workflow(dbos, "peer-wf", status="SUCCESS")
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

    moved = dbos._sys_db.rename_application(
        None, RENAMED_APP, adopt_unclaimed_rows=True
    )
    assert moved["workflows"] == 1 and moved["steps"] == 1 and moved["queues"] == 1
    assert application_name_of(dbos, "legacy-wf") == RENAMED_APP
    assert step_owners(dbos, "legacy-wf") == {RENAMED_APP}
    # A peer's own rows are never in scope for an adoption.
    assert application_name_of(dbos, "peer-wf") == OTHER_APP


def test_registration_conflict_points_at_the_rename_command(dbos: DBOS) -> None:
    """The wrong order -- starting under the new name before re-owning -- fails at
    registration. That error is where an operator learns the remedy exists."""
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.insert(SystemSchema.application_versions).values(
                version_id="conflict-version-id",
                version_name="conflict-version",
                version_timestamp=7,
                created_at=7,
                application_name=OTHER_APP,
            )
        )
    with pytest.raises(DBOSException) as excinfo:
        dbos._sys_db.create_application_version(
            "conflict-version", application_name=RENAMED_APP
        )
    assert f"dbos rename-application --from {OTHER_APP} --to {RENAMED_APP}" in str(
        excinfo.value
    )


def test_cli_renames_application(dbos: DBOS, config: Any) -> None:
    """Click binds options to parameters by name, which mypy cannot check, so the
    command needs one real invocation."""
    from click.testing import CliRunner

    from dbos.cli.cli import app

    insert_foreign_workflow(dbos, "cli-rename-me", status="SUCCESS")
    insert_foreign_workflow(
        dbos, "cli-adopt-me", status="SUCCESS", application_name=None
    )
    insert_foreign_control_plane(dbos)
    url = config["system_database_url"]
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["rename-application", "-s", url, "-f", OTHER_APP, "-t", RENAMED_APP, "-y"],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["workflows"] == 1
    assert application_name_of(dbos, "cli-rename-me") == RENAMED_APP
    assert application_name_of(dbos, "cli-adopt-me") is None

    adopted = runner.invoke(
        app,
        [
            "rename-application",
            "-s",
            url,
            "-f",
            RENAMED_APP,
            "-t",
            "adopting-app",
            "--adopt-unclaimed-rows",
            "-y",
        ],
    )
    assert adopted.exit_code == 0, adopted.output
    assert application_name_of(dbos, "cli-adopt-me") == "adopting-app"

    # A name no application could launch under is refused, with a non-zero exit.
    bad = runner.invoke(
        app, ["rename-application", "-s", url, "-f", "adopting-app", "-t", "ab", "-y"]
    )
    assert bad.exit_code == 1
    assert "Invalid application name" in bad.output

    # Neither source named is CLI misuse, caught before anything connects.
    neither = runner.invoke(
        app, ["rename-application", "-s", url, "-t", "some-app", "-y"]
    )
    assert neither.exit_code == 2
    assert "Nothing to re-own" in neither.output
