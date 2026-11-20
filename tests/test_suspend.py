"""recv(suspend=True) and sleep(suspend=True) are Transact Enterprise features: without
suspend mode the calls are refused. Waking a suspended recv is core: every send does it,
so a sender without suspend mode still wakes a workflow another process parked. The
operations themselves are tested in dbos-enterprise."""

import re
import threading
import uuid
from types import SimpleNamespace
from typing import Any, List, Tuple, cast

import pytest
import sqlalchemy as sa
from sqlalchemy import event

from dbos import DBOS, DBOSClient, SetWorkflowID
from dbos._core import result_suspend_hooks
from dbos._error import DBOSException, DBOSWorkflowSuspendedError
from dbos._outcome import NoResult
from dbos._sys_db import (
    WAIT_FIRST_SUSPEND_KEY,
    WorkflowStatusString,
    result_suspend_key,
)
from tests.conftest import retry_until_success


def test_suspended_error_names_what_the_workflow_waits_on() -> None:
    assert (
        DBOSWorkflowSuspendedError("w", "approve").message
        == "Workflow w suspended waiting on topic approve"
    )
    assert (
        DBOSWorkflowSuspendedError("w", "__null__topic__").message
        == "Workflow w suspended waiting on a message with no topic"
    )
    assert (
        DBOSWorkflowSuspendedError("w", None).message == "Workflow w suspended sleeping"
    )


def test_recv_suspend_is_refused_without_suspend_mode(dbos: DBOS) -> None:
    assert dbos._sys_db.suspend is None

    @DBOS.workflow()
    def waiter() -> str:
        return str(DBOS.recv("topic", timeout_seconds=1, suspend=True))

    with SetWorkflowID(str(uuid.uuid4())):
        handle = DBOS.start_workflow(waiter)
    with pytest.raises(DBOSException, match="requires suspend mode"):
        handle.get_result()


def test_sleep_suspend_is_refused_without_suspend_mode(dbos: DBOS) -> None:
    assert dbos._sys_db.suspend is None

    @DBOS.workflow()
    def sleeper() -> None:
        DBOS.sleep(1, suspend=True)

    with SetWorkflowID(str(uuid.uuid4())):
        handle = DBOS.start_workflow(sleeper)
    with pytest.raises(DBOSException, match="requires suspend mode"):
        handle.get_result()


@pytest.mark.asyncio
async def test_async_suspend_is_refused_without_suspend_mode(dbos: DBOS) -> None:
    assert dbos._sys_db.suspend is None

    @DBOS.workflow()
    async def waiter() -> str:
        return str(await DBOS.recv_async("topic", timeout_seconds=1, suspend=True))

    @DBOS.workflow()
    async def sleeper() -> None:
        await DBOS.sleep_async(1, suspend=True)

    for wf in (waiter, sleeper):
        handle = await DBOS.start_workflow_async(wf)
        with pytest.raises(DBOSException, match="requires suspend mode"):
            await handle.get_result()


def test_send_wakes_suspended_receivers_without_suspend_mode(
    dbos: DBOS, client: DBOSClient, skip_with_sharding: None
) -> None:
    """Parks workflows by hand, as dbos-enterprise's recv would: a send on the awaited
    topic, from the application or a client, makes the receiver due (delay 0), and a
    send on another topic leaves it parked."""
    assert dbos._sys_db.suspend is None
    sys_db = dbos._sys_db
    ws = sys_db.tables.workflow_status
    deadline_ms = 2**62

    @DBOS.workflow()
    def parked() -> None:
        pass

    def park(topic: str) -> str:
        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid):
            parked()
        with sys_db.engine.begin() as c:
            c.execute(
                sa.update(ws)
                .where(ws.c.workflow_uuid == wfid)
                .values(
                    status=WorkflowStatusString.SUSPENDED.value,
                    suspend_key=topic,
                    delay_until_epoch_ms=deadline_ms,
                )
            )
        return wfid

    def delay(wfid: str) -> int:
        with sys_db.engine.connect() as c:
            return int(
                c.execute(
                    sa.select(ws.c.delay_until_epoch_ms).where(
                        ws.c.workflow_uuid == wfid
                    )
                ).scalar_one()
            )

    by_app = park("approve")
    by_client = park("approve")
    DBOS.send(by_app, "no", topic="other")
    assert delay(by_app) == deadline_ms
    DBOS.send(by_app, "yes", topic="approve")
    assert delay(by_app) == 0
    client.send(by_client, "yes", topic="approve")
    assert delay(by_client) == 0


def test_parked_workflow_is_still_active(dbos: DBOS, skip_with_sharding: None) -> None:
    """Parks a workflow by hand on a queue: wait_first does not take it for complete, and
    queue listings still show it."""
    sys_db = dbos._sys_db
    ws = sys_db.tables.workflow_status

    @DBOS.workflow()
    def parked() -> None:
        pass

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        parked()
    with sys_db.engine.begin() as c:
        c.execute(
            sa.update(ws)
            .where(ws.c.workflow_uuid == wfid)
            .values(
                status=WorkflowStatusString.SUSPENDED.value,
                suspend_key="approve",
                delay_until_epoch_ms=2**62,
                queue_name="suspend-q",
            )
        )

    assert isinstance(sys_db.check_first_workflow_id([wfid]), NoResult)
    queued = DBOS.list_queued_workflows(queue_name="suspend-q")
    assert [(w.workflow_id, w.status) for w in queued] == [
        (wfid, WorkflowStatusString.SUSPENDED.value)
    ]


def test_leaving_suspended_clears_suspend_key(
    dbos: DBOS, skip_with_sharding: None
) -> None:
    """Parks workflows by hand: cancel, resume and the timeout sweep each clear the
    suspend_key, so a later send on that topic no longer matches the row."""
    sys_db = dbos._sys_db
    ws = sys_db.tables.workflow_status

    @DBOS.workflow()
    def parked() -> None:
        pass

    def park(**extra: Any) -> str:
        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid):
            parked()
        with sys_db.engine.begin() as c:
            c.execute(
                sa.update(ws)
                .where(ws.c.workflow_uuid == wfid)
                .values(
                    status=WorkflowStatusString.SUSPENDED.value,
                    suspend_key="approve",
                    delay_until_epoch_ms=2**62,
                    **extra,
                )
            )
        return wfid

    def row(wfid: str) -> Tuple[str, Any]:
        with sys_db.engine.connect() as c:
            found = c.execute(
                sa.select(ws.c.status, ws.c.suspend_key).where(
                    ws.c.workflow_uuid == wfid
                )
            ).one()
        return (found[0], found[1])

    cancelled = park()
    DBOS.cancel_workflow(cancelled)
    assert row(cancelled) == (WorkflowStatusString.CANCELLED.value, None)

    resumed = park()
    DBOS.resume_workflow(resumed).get_result()
    assert row(resumed) == (WorkflowStatusString.SUCCESS.value, None)

    timed_out = park(workflow_deadline_epoch_ms=1)

    def swept() -> None:
        sys_db.cancel_timed_out_workflows(10)
        assert row(timed_out) == (WorkflowStatusString.CANCELLED.value, None)

    retry_until_success(swept)


def test_send_wake_update_query_plan(
    dbos_dropped_databases: DBOS, skip_with_sqlite: None, skip_with_sharding: None
) -> None:
    """Every send runs the wake update, so it must probe the primary key: planned with
    statistics that predate the parked rows, it must not scan idx_workflow_status_suspended.
    """
    # Freshly created tables: the plan depends on what earlier tests left in them.
    dbos = dbos_dropped_databases
    sys_db = dbos._sys_db
    ws = sys_db.tables.workflow_status

    @DBOS.workflow()
    def target() -> None:
        pass

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        target()

    captured: List[Tuple[str, Any]] = []

    def before_cursor_execute(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        # Only the wake update sets the delay: the background timeout sweep also names suspend_key.
        if statement.startswith("UPDATE") and "SET delay_until_epoch_ms=" in statement:
            captured.append((statement, parameters))

    event.listen(sys_db.engine, "before_cursor_execute", before_cursor_execute)
    try:
        DBOS.send(wfid, "m", topic="approve")
    finally:
        event.remove(sys_db.engine, "before_cursor_execute", before_cursor_execute)
    assert captured
    statement, parameters = captured[0]

    rows = [
        {
            "workflow_uuid": f"wake-plan-{i}-{uuid.uuid4()}",
            "name": "wake_plan_probe",
            "status": WorkflowStatusString.SUSPENDED.value,
            "created_at": 1,
            "updated_at": 1,
            "delay_until_epoch_ms": 2**62,
            "suspend_key": "approve",
            "application_name": sys_db.app_name,
        }
        for i in range(2000)
    ]
    with sys_db.engine.begin() as conn:
        # Analyzed before the rows are parked, the planner takes the suspended index for empty.
        conn.exec_driver_sql(f'ANALYZE "{sys_db.schema}".workflow_status')
        conn.execute(sa.insert(ws), rows)
        plan = [
            str(r[0]) for r in conn.exec_driver_sql(f"EXPLAIN {statement}", parameters)
        ]
    assert not any("idx_workflow_status_suspended" in d for d in plan), plan
    assert any("workflow_status_pkey" in d for d in plan), plan


def test_suspended_error_names_the_awaited_workflow() -> None:
    assert (
        DBOSWorkflowSuspendedError("w", None, awaited_workflow_ids=["a"]).message
        == "Workflow w suspended awaiting the result of workflow a"
    )
    assert (
        DBOSWorkflowSuspendedError("w", None, awaited_workflow_ids=["a", "b"]).message
        == "Workflow w suspended awaiting the first of 2 workflows"
    )


def test_get_result_suspend_is_refused_without_suspend_mode(dbos: DBOS) -> None:
    """Refused outside a workflow, and inside one without suspend mode, through handles, DBOS.get_result and DBOS.wait_first."""
    assert dbos._sys_db.suspend is None

    @DBOS.workflow()
    def target() -> str:
        return "x"

    @DBOS.workflow()
    def by_handle(wfid: str) -> str:
        return str(DBOS.retrieve_workflow(wfid).get_result(suspend=True))

    @DBOS.workflow()
    def by_id(wfid: str) -> str:
        return str(DBOS.get_result(wfid, suspend=True))

    @DBOS.workflow()
    def by_wait_first(wfid: str) -> str:
        return DBOS.wait_first([DBOS.retrieve_workflow(wfid)], suspend=True).workflow_id

    target_id = str(uuid.uuid4())
    with SetWorkflowID(target_id):
        target()
    for wf in (by_handle, by_id, by_wait_first):
        handle = DBOS.start_workflow(wf, target_id)
        with pytest.raises(DBOSException, match="requires suspend mode"):
            handle.get_result()
    with pytest.raises(DBOSException, match="must be called from within a workflow"):
        DBOS.retrieve_workflow(target_id).get_result(suspend=True)
    with pytest.raises(DBOSException, match="must be called from within a workflow"):
        DBOS.get_result(target_id, suspend=True)
    with pytest.raises(DBOSException, match="wait_first.*must be called from within"):
        DBOS.wait_first([DBOS.retrieve_workflow(target_id)], suspend=True)


def test_completion_wakes_result_waiters_without_suspend_mode(
    dbos: DBOS, skip_with_sharding: None
) -> None:
    """Parks waiters by hand, as dbos-enterprise's get_result and wait_first would: success, error,
    cancel, dead-letter and the timeout sweep each make the awaited workflow's waiters due and keep
    its list, and a listed waiter parked on another workflow stays parked."""
    assert dbos._sys_db.suspend is None
    sys_db = dbos._sys_db
    ws = sys_db.tables.workflow_status
    release = threading.Event()

    @DBOS.workflow()
    def target(fail: bool) -> str:
        assert release.wait(60)
        if fail:
            raise RuntimeError("failed")
        return "done"

    @DBOS.workflow()
    def parked() -> None:
        pass

    def park(suspend_key: str) -> str:
        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid):
            parked()
        with sys_db.engine.begin() as c:
            c.execute(
                sa.update(ws)
                .where(ws.c.workflow_uuid == wfid)
                .values(
                    status=WorkflowStatusString.SUSPENDED.value,
                    suspend_key=suspend_key,
                    delay_until_epoch_ms=None,
                )
            )
        return wfid

    def row(wfid: str) -> Any:
        with sys_db.engine.connect() as c:
            return c.execute(
                sa.select(ws.c.delay_until_epoch_ms, ws.c.result_waiter_ids).where(
                    ws.c.workflow_uuid == wfid
                )
            ).one()

    def start(fail: bool = False) -> Tuple[str, List[str], str]:
        """A running target with two waiters parked on it, one in wait_first, and one parked elsewhere."""
        handle = DBOS.start_workflow(target, fail)
        key = result_suspend_key(handle.workflow_id)
        waiters = [park(key), park(key), park(WAIT_FIRST_SUSPEND_KEY)]
        other = park(result_suspend_key(str(uuid.uuid4())))
        with sys_db.engine.begin() as c:
            c.execute(
                sa.update(ws)
                .where(ws.c.workflow_uuid == handle.workflow_id)
                .values(result_waiter_ids=[*waiters, other])
            )
        return handle.workflow_id, waiters, other

    def check(target_id: str, waiters: List[str], other: str) -> None:
        assert [row(w)[0] for w in waiters] == [0, 0, 0]
        assert row(other)[0] is None
        assert row(target_id)[1] == [*waiters, other]

    # Each completes while the target still runs, except success and error, released at the end.
    succeeded, failed = start(), start(fail=True)
    cancelled = start()
    DBOS.cancel_workflow(cancelled[0])
    check(*cancelled)
    dead = start()
    sys_db.dead_letter_workflows([dead[0]], min_recovery_attempts=0)
    check(*dead)
    timed_out = start()
    with sys_db.engine.begin() as c:
        c.execute(
            sa.update(ws)
            .where(ws.c.workflow_uuid == timed_out[0])
            .values(workflow_deadline_epoch_ms=1)
        )

    # The background timeout sweep may cancel it first; either sweep wakes its waiters.
    def swept() -> None:
        sys_db.cancel_timed_out_workflows(100)
        status = sys_db.get_workflow_status(timed_out[0])
        assert status is not None
        assert status["status"] == WorkflowStatusString.CANCELLED.value

    retry_until_success(swept, interval=0.1, max_attempts=50)
    check(*timed_out)
    release.set()
    assert DBOS.retrieve_workflow(succeeded[0]).get_result() == "done"
    check(*succeeded)
    with pytest.raises(RuntimeError):
        DBOS.retrieve_workflow(failed[0]).get_result()
    check(*failed)


def test_get_result_suspend_is_refused_when_sharded_or_cockroach() -> None:
    ctx = cast(Any, SimpleNamespace(is_workflow=lambda: True, workflow_id="w"))
    for sharded, cockroach, refusal in [
        (True, False, "not supported on a sharded"),
        (False, True, "not supported on CockroachDB"),
    ]:
        sys_db = SimpleNamespace(
            suspend=object(), sharded=sharded, _is_cockroach=cockroach
        )
        for fn in ("get_result", "wait_first"):
            with pytest.raises(
                DBOSException, match=rf"{fn}\(suspend=True\) is {refusal}"
            ):
                result_suspend_hooks(
                    cast(Any, SimpleNamespace(_sys_db=sys_db)), ctx, fn
                )


def test_outcome_wake_query_plans(
    dbos_dropped_databases: DBOS, skip_with_sqlite: None, skip_with_sharding: None
) -> None:
    """Every one-statement completion runs the waiter wake: planned custom or generic, with stale
    stats and many parked rows, it probes the primary key."""
    dbos = dbos_dropped_databases
    sys_db = dbos._sys_db
    ws = sys_db.tables.workflow_status
    release = threading.Event()

    @DBOS.workflow()
    def target() -> None:
        assert release.wait(10)

    @DBOS.workflow()
    def parked() -> None:
        pass

    handle = DBOS.start_workflow(target)
    waiter_id = str(uuid.uuid4())
    with SetWorkflowID(waiter_id):
        parked()
    with sys_db.engine.begin() as c:
        c.execute(
            sa.update(ws)
            .where(ws.c.workflow_uuid == waiter_id)
            .values(
                status=WorkflowStatusString.SUSPENDED.value,
                suspend_key=result_suspend_key(handle.workflow_id),
                delay_until_epoch_ms=None,
            )
        )
        c.execute(
            sa.update(ws)
            .where(ws.c.workflow_uuid == handle.workflow_id)
            .values(result_waiter_ids=[waiter_id])
        )

    captured: dict[str, Tuple[str, Any]] = {}

    def before_cursor_execute(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        if statement.startswith("WITH updated AS"):
            captured.setdefault("outcome", (statement, dict(parameters)))

    event.listen(sys_db.engine, "before_cursor_execute", before_cursor_execute)
    try:
        release.set()
        handle.get_result()
        assert set(captured) == {"outcome"}
    finally:
        event.remove(sys_db.engine, "before_cursor_execute", before_cursor_execute)

    rows = [
        {
            "workflow_uuid": f"wake-plan-{i}-{uuid.uuid4()}",
            "name": "wake_plan_probe",
            "status": WorkflowStatusString.SUSPENDED.value,
            "created_at": 1,
            "updated_at": 1,
            "suspend_key": (
                result_suspend_key("other") if i % 2 else WAIT_FIRST_SUSPEND_KEY
            ),
            "result_waiter_ids": ["other"],
            "application_name": sys_db.app_name,
        }
        for i in range(5000)
    ]

    def literal(v: Any) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, list):
            return "ARRAY[" + ",".join(literal(x) for x in v) + "]::text[]"
        if isinstance(v, (int, float)):
            return str(v)
        return "'" + str(v).replace("'", "''") + "'"

    def plans(conn: Any, statement: str, parameters: Any) -> List[Any]:
        """The custom plan, and the generic plan a prepared statement can switch to."""
        custom = conn.exec_driver_sql(
            f"EXPLAIN (FORMAT JSON) {statement}", parameters
        ).scalar()
        names: List[str] = []

        def number(m: Any) -> str:
            if m.group(1) not in names:
                names.append(m.group(1))
            return f"${names.index(m.group(1)) + 1}"

        conn.exec_driver_sql("SET plan_cache_mode = force_generic_plan")
        conn.exec_driver_sql(
            "PREPARE wake_plan AS " + re.sub(r"%\((\w+)\)s", number, statement)
        )
        args = ", ".join(literal(parameters[n]) for n in names)
        generic = conn.exec_driver_sql(
            f"EXPLAIN (FORMAT JSON) EXECUTE wake_plan({args})"
        ).scalar()
        conn.exec_driver_sql("DEALLOCATE wake_plan")
        conn.exec_driver_sql("RESET plan_cache_mode")
        return [custom[0]["Plan"], generic[0]["Plan"]]

    def subplan(plan: Any, name: str) -> Any:
        if plan.get("Subplan Name") == name:
            return plan
        for child in plan.get("Plans", []):
            if (found := subplan(child, name)) is not None:
                return found
        return None

    def workflow_status_scans(plan: Any) -> List[Tuple[str, str]]:
        found = []
        # Any node naming an index, so a Bitmap Index Scan, which has no relation, is checked too.
        if plan["Node Type"] != "ModifyTable" and (
            plan.get("Relation Name") == "workflow_status" or "Index Name" in plan
        ):
            found.append((plan["Node Type"], plan.get("Index Name", "")))
        for child in plan.get("Plans", []):
            found.extend(workflow_status_scans(child))
        return found

    with sys_db.engine.begin() as conn:
        # Analyzed before the rows arrive, the planner takes the table for near-empty.
        conn.exec_driver_sql(f'ANALYZE "{sys_db.schema}".workflow_status')
        conn.execute(sa.insert(ws), rows)
        statement, parameters = captured["outcome"]
        for plan in plans(conn, statement, parameters):
            woken = subplan(plan, "CTE woken")
            assert woken is not None
            scans = workflow_status_scans(woken)
            assert scans, woken
            for node_type, index in scans:
                assert node_type != "Seq Scan", scans
                assert index in ("", "workflow_status_pkey"), scans
