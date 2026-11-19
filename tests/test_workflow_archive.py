"""The system database's half of workflow archiving: which writes queue a workflow."""

import asyncio
import threading
import time
import uuid
from typing import Any, List, Optional, Tuple

import pytest
import sqlalchemy as sa

from dbos import (
    DBOS,
    DBOSClient,
    DBOSConfig,
    EnqueueOptions,
    SetWorkflowID,
    SetWorkflowTimeout,
    WorkflowHandle,
    _enterprise,
)
from dbos._error import (
    DBOSAwaitedWorkflowCancelledError,
    DBOSException,
    DBOSWorkflowArchiveNotConfiguredError,
)
from dbos._schemas.system_database import SystemSchema

from .conftest import retry_until_success, using_sharding


def archive_pending(dbos: DBOS, workflow_id: str) -> Optional[bool]:
    ws = SystemSchema.workflow_status
    with dbos._sys_db.engine.begin() as c:
        value = c.execute(
            sa.select(ws.c.archive_pending).where(ws.c.workflow_uuid == workflow_id)
        ).scalar()
    return None if value is None else bool(value)


@pytest.fixture()
def skip_with_sharding_here() -> None:
    if using_sharding():
        pytest.skip("Workflow archiving is not supported on a sharded system database")


def test_completions_unmarked_without_archive(dbos: DBOS) -> None:
    @DBOS.workflow()
    def ok() -> int:
        return 1

    @DBOS.workflow()
    def fails() -> None:
        raise ValueError("no")

    assert ok() == 1
    with pytest.raises(ValueError):
        fails()
    for w in dbos._sys_db.list_workflows():
        assert w.completed_at is not None
        assert archive_pending(dbos, w.workflow_id) is None


def test_every_terminal_transition_marks(
    dbos: DBOS, skip_with_sharding_here: None
) -> None:
    dbos._sys_db.archive_completions = True
    release = threading.Event()

    @DBOS.workflow()
    def ok() -> int:
        return 1

    @DBOS.workflow()
    def fails() -> None:
        raise ValueError("no")

    @DBOS.workflow()
    def blocks() -> None:
        release.wait()

    @DBOS.workflow()
    def sleeps() -> None:
        DBOS.sleep(10)

    ok_handle = DBOS.start_workflow(ok)
    ok_handle.get_result()
    assert archive_pending(dbos, ok_handle.workflow_id) is True

    fail_handle = DBOS.start_workflow(fails)
    with pytest.raises(ValueError):
        fail_handle.get_result()
    assert archive_pending(dbos, fail_handle.workflow_id) is True

    # Cancelled, then resumed: resume clears the mark, completing sets it again.
    blocked = DBOS.start_workflow(blocks)
    DBOS.cancel_workflow(blocked.workflow_id)
    assert archive_pending(dbos, blocked.workflow_id) is True
    release.set()
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        blocked.get_result()
    resumed = DBOS.resume_workflow(blocked.workflow_id)
    resumed.get_result()
    assert archive_pending(dbos, blocked.workflow_id) is True

    # Rewinding clears it while the workflow runs again.
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .where(
                SystemSchema.workflow_status.c.workflow_uuid == ok_handle.workflow_id
            )
            .values(archive_pending=None)
        )
    rewound = DBOS.rewind_workflow(ok_handle.workflow_id)
    rewound.get_result()
    assert archive_pending(dbos, ok_handle.workflow_id) is True

    # A timeout cancels through the deadline sweep.
    with SetWorkflowTimeout(0.1):
        timed_out = DBOS.start_workflow(sleeps)
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        timed_out.get_result()
    retry_until_success(
        lambda: _assert(archive_pending(dbos, timed_out.workflow_id) is True)
    )

    # Dead-lettering a workflow that ran out of recovery attempts.
    dead = DBOS.start_workflow(ok)
    dead.get_result()
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .where(SystemSchema.workflow_status.c.workflow_uuid == dead.workflow_id)
            .values(status="PENDING", completed_at=None, archive_pending=None)
        )
    dbos._sys_db.dead_letter_workflows([dead.workflow_id], min_recovery_attempts=0)
    status = DBOS.get_workflow_status(dead.workflow_id)
    assert status is not None and status.status == "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
    assert archive_pending(dbos, dead.workflow_id) is True


def test_resume_clears_mark(dbos: DBOS, skip_with_sharding_here: None) -> None:
    dbos._sys_db.archive_completions = True
    started = threading.Event()
    release = threading.Event()

    @DBOS.workflow()
    def blocks() -> None:
        started.set()
        release.wait()

    handle = DBOS.start_workflow(blocks)
    started.wait()
    DBOS.cancel_workflow(handle.workflow_id)
    assert archive_pending(dbos, handle.workflow_id) is True
    # Resumed while the cancelled execution still holds its thread: ENQUEUED, unmarked.
    dbos._sys_db.archive_completions = False
    resumed = DBOS.resume_workflow(handle.workflow_id)
    assert archive_pending(dbos, handle.workflow_id) is None
    release.set()
    resumed.get_result()
    assert archive_pending(dbos, handle.workflow_id) is None


def test_client_cancel_marks(
    config: DBOSConfig, dbos: DBOS, client: DBOSClient, skip_with_sharding_here: None
) -> None:
    release = threading.Event()

    @DBOS.workflow()
    def blocks() -> None:
        release.wait()

    handle = DBOS.start_workflow(blocks)
    client.cancel_workflow(handle.workflow_id)
    assert archive_pending(dbos, handle.workflow_id) is None

    archiving = DBOSClient(
        system_database_url=config["system_database_url"],
        workflow_archive=_FakeArchive(),
    )
    try:
        assert archiving._sys_db.archive_completions
        second = DBOS.start_workflow(blocks)
        archiving.cancel_workflow(second.workflow_id)
        assert archive_pending(dbos, second.workflow_id) is True
    finally:
        archiving.destroy()
        release.set()


def test_unclaimed_completions_unmarked(
    config: DBOSConfig, dbos: DBOS, skip_with_sharding_here: None
) -> None:
    """No application owns an unclaimed workflow, so none archives it: its completion
    isn't marked, and retention collects it like any other."""
    archiving = DBOSClient(
        system_database_url=config["system_database_url"],
        workflow_archive=_FakeArchive(),
    )
    try:
        options: EnqueueOptions = {
            "workflow_name": "never_run",
            "queue_name": "unserved",
        }
        unclaimed: WorkflowHandle[Any] = archiving.enqueue(options)
        claimed: WorkflowHandle[Any] = archiving.enqueue(
            {**options, "application_name": "some-app"}
        )
        for handle in (unclaimed, claimed):
            archiving.cancel_workflow(handle.workflow_id)
            status = DBOS.get_workflow_status(handle.workflow_id)
            assert status is not None and status.status == "CANCELLED"
        assert archive_pending(dbos, unclaimed.workflow_id) is None
        assert archive_pending(dbos, claimed.workflow_id) is True
    finally:
        archiving.destroy()


class _FakeArchive:
    @property
    def archive_id(self) -> str:
        return "fake"


def _assert(condition: bool) -> None:
    assert condition


def test_migration_adds_column_and_index(dbos: DBOS) -> None:
    inspector = sa.inspect(dbos._sys_db.engine)
    schema = None if dbos._sys_db.engine.dialect.name == "sqlite" else "dbos"
    columns = {c["name"] for c in inspector.get_columns("workflow_status", schema)}
    assert "archive_pending" in columns
    indexes = {
        i["name"]: i["column_names"]
        for i in inspector.get_indexes("workflow_status", schema)
    }
    assert indexes["idx_workflow_status_archive_pending"] == [
        "application_name",
        "completed_at",
        "workflow_uuid",
    ]


def test_import_archived_needs_an_archive(dbos: DBOS, client: DBOSClient) -> None:
    with pytest.raises(DBOSWorkflowArchiveNotConfiguredError):
        DBOS.import_archived_workflow("wf", 1)
    with pytest.raises(DBOSWorkflowArchiveNotConfiguredError):
        asyncio.run(DBOS.import_archived_workflow_async("wf", 1))
    with pytest.raises(DBOSWorkflowArchiveNotConfiguredError):
        client.import_archived_workflow("wf", 1)
    with pytest.raises(DBOSWorkflowArchiveNotConfiguredError):
        asyncio.run(client.import_archived_workflow_async("wf", 1))


def test_import_archived_calls_enterprise(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch, skip_with_sharding_here: None
) -> None:
    @DBOS.workflow()
    def work() -> int:
        return 5

    work()
    workflow_id = DBOS.list_workflows()[0].workflow_id
    calls: List[Tuple[Any, ...]] = []

    class FakeEnterprise:
        @staticmethod
        def import_archived_workflow(*args: Any) -> None:
            calls.append(args)

    monkeypatch.setattr(_enterprise, "load", lambda *a, **k: FakeEnterprise)
    archive = _FakeArchive()
    monkeypatch.setattr(dbos, "_workflow_archive", archive)
    handle = DBOS.import_archived_workflow(workflow_id, 123)
    assert calls == [(dbos._sys_db, archive, workflow_id, 123)]
    assert handle.workflow_id == workflow_id and handle.get_result() == 5


def test_cockroach_client_refuses_archive(skip_with_sharding_here: None) -> None:
    with pytest.raises(DBOSException, match="not supported on CockroachDB"):
        DBOSClient(
            system_database_url="cockroachdb://root@localhost:26257/unused",
            workflow_archive=_FakeArchive(),
        )


def test_sharded_client_refuses_archive() -> None:
    with pytest.raises(DBOSException, match="not supported on a sharded"):
        DBOSClient(
            system_database_url="postgresql://postgres:dbos@localhost:5432/unused",
            sharding=True,
            workflow_archive=_FakeArchive(),
        )


def test_import_archived_async_reserves_both_steps_first(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch, skip_with_sharding_here: None
) -> None:
    @DBOS.workflow()
    def archived() -> int:
        return 5

    archived()
    archived_id = DBOS.list_workflows()[0].workflow_id

    class SlowEnterprise:
        @staticmethod
        def import_archived_workflow(*args: Any) -> None:
            time.sleep(0.3)

    monkeypatch.setattr(_enterprise, "load", lambda *a, **k: SlowEnterprise)
    monkeypatch.setattr(dbos, "_workflow_archive", _FakeArchive())

    @DBOS.step()
    async def other_step() -> int:
        return 1

    @DBOS.workflow()
    async def importer() -> int:
        async def other() -> int:
            await asyncio.sleep(0)
            return await other_step()

        _, x = await asyncio.gather(
            DBOS.import_archived_workflow_async(archived_id, 1), other()
        )
        return int(x)

    importer_id = str(uuid.uuid4())
    with SetWorkflowID(importer_id):
        assert asyncio.run(importer()) == 1
    # The import's status read keeps the ID reserved next to it, however long it takes.
    names = [s["function_name"] for s in DBOS.list_workflow_steps(importer_id)]
    assert names[:2] == ["DBOS.importArchivedWorkflow", "DBOS.getStatus"]


def test_cancelling_again_leaves_the_completion(
    dbos: DBOS, skip_with_sharding_here: None
) -> None:
    dbos._sys_db.archive_completions = True
    started = threading.Event()
    release = threading.Event()

    @DBOS.workflow()
    def blocks() -> None:
        started.set()
        release.wait()

    handle = DBOS.start_workflow(blocks)
    try:
        _cancel_twice_and_check(dbos, handle.workflow_id, started)
    finally:
        release.set()


def _cancel_twice_and_check(
    dbos: DBOS, workflow_id: str, started: threading.Event
) -> None:
    started.wait()
    DBOS.cancel_workflow(workflow_id)
    first = DBOS.get_workflow_status(workflow_id)
    assert first is not None and first.completed_at is not None
    # As the archiver leaves it once the cancellation is archived.
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
            .values(archive_pending=None)
        )
    DBOS.cancel_workflow(workflow_id)
    again = DBOS.get_workflow_status(workflow_id)
    assert again is not None and again.status == "CANCELLED"
    # Not a new completion, so nothing is queued to archive twice.
    assert again.completed_at == first.completed_at
    assert archive_pending(dbos, workflow_id) is None
