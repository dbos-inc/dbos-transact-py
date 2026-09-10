import pytest

# Public API
from dbos import DBOS, DBOSContextSetAuth
from dbos._error import DBOSNotAuthorizedError
from dbos._sys_db import WorkflowStatusString
from tests.conftest import retry_until_success, set_workflow_status


def test_roles_recovery(dbos: DBOS) -> None:

    @DBOS.required_roles(["admin"])
    @DBOS.workflow()
    def workflow() -> None:
        assert DBOS.authenticated_roles == ["user", "admin"]
        assert DBOS.authenticated_user == "admin"
        assert DBOS.assumed_role == "admin"

    with DBOSContextSetAuth("admin", ["user", "admin"]):
        handle = DBOS.start_workflow(workflow)

    assert handle.get_result() is None

    assert handle.get_status().authenticated_user == "admin"
    assert [w.workflow_id for w in DBOS.list_workflows(user="admin")] == [
        handle.workflow_id
    ]

    # Recover the workflow, verify roles are set right
    set_workflow_status(dbos._sys_db, handle.workflow_id, "PENDING")
    recovery_handles = DBOS._recover_pending_workflows()
    assert len(recovery_handles) == 1
    recovery_handle = recovery_handles[0]
    assert recovery_handle.get_result() is None


@pytest.mark.asyncio
async def test_roles_recovery_async(dbos: DBOS) -> None:

    @DBOS.required_roles(["admin"])
    @DBOS.workflow()
    async def workflow() -> None:
        assert DBOS.authenticated_roles == ["user", "admin"]
        assert DBOS.authenticated_user == "admin"
        assert DBOS.assumed_role == "admin"

    with DBOSContextSetAuth("admin", ["user", "admin"]):
        handle = await DBOS.start_workflow_async(workflow)

    await handle.get_result()

    # Recover the workflow, verify roles are set right
    set_workflow_status(dbos._sys_db, handle.workflow_id, "PENDING")
    recovery_handles = DBOS._recover_pending_workflows()
    assert len(recovery_handles) == 1
    recovery_handle = recovery_handles[0]
    assert recovery_handle.get_result() is None


def test_roles_denied_start_workflow(dbos: DBOS) -> None:
    # A workflow whose persisted auth context does not satisfy its required roles
    # must finalize as ERROR, not stay stuck PENDING (issue #743).
    @DBOS.required_roles(["admin"])
    @DBOS.workflow()
    def workflow() -> None:
        return None

    with DBOSContextSetAuth("bob", ["reader"]):
        handle = DBOS.start_workflow(workflow)

    with pytest.raises(DBOSNotAuthorizedError):
        handle.get_result()

    status = DBOS.get_workflow_status(handle.workflow_id)
    assert status is not None
    assert status.status == WorkflowStatusString.ERROR.value


def test_roles_denied_queue(dbos: DBOS) -> None:
    # The dequeue path must also finalize a role-denied workflow as ERROR rather
    # than leaving it PENDING to be redequeued forever (issue #743).
    queue = DBOS.register_queue("test_roles_denied_queue")

    @DBOS.required_roles(["admin"])
    @DBOS.workflow()
    def workflow() -> None:
        return None

    with DBOSContextSetAuth("bob", ["reader"]):
        handle = queue.enqueue(workflow)

    with pytest.raises(DBOSNotAuthorizedError):
        handle.get_result()

    status = DBOS.get_workflow_status(handle.workflow_id)
    assert status is not None
    assert status.status == WorkflowStatusString.ERROR.value


def test_roles_denied_queue_no_auth_context(dbos: DBOS) -> None:
    # A role-gated workflow enqueued with no auth context at all (e.g. a SQL
    # enqueue that never set authenticated_roles) must finalize as ERROR rather
    # than stay PENDING (issue #743). Exercises the other raise branch in
    # check_required_roles (no authentication information).
    queue = DBOS.register_queue("test_roles_denied_queue_no_auth_context")

    @DBOS.required_roles(["admin"])
    @DBOS.workflow()
    def workflow() -> None:
        return None

    handle = queue.enqueue(workflow)

    with pytest.raises(DBOSNotAuthorizedError):
        handle.get_result()

    status = DBOS.get_workflow_status(handle.workflow_id)
    assert status is not None
    assert status.status == WorkflowStatusString.ERROR.value


def test_roles_denied_recovery(dbos: DBOS) -> None:
    # A role-denied workflow that is recovered must finalize as ERROR instead of
    # bouncing back to PENDING on every recovery attempt (issue #743).
    @DBOS.required_roles(["admin"])
    @DBOS.workflow()
    def workflow() -> None:
        return None

    with DBOSContextSetAuth("bob", ["reader"]):
        handle = DBOS.start_workflow(workflow)
    with pytest.raises(DBOSNotAuthorizedError):
        handle.get_result()

    # Reset to PENDING and recover; the role check should fail it again to ERROR.
    set_workflow_status(dbos._sys_db, handle.workflow_id, "PENDING")
    recovery_handles = DBOS._recover_pending_workflows()
    assert len(recovery_handles) == 1
    with pytest.raises(DBOSNotAuthorizedError):
        recovery_handles[0].get_result()

    status = retry_until_success(
        lambda: _require_terminal(dbos, handle.workflow_id), interval=0.2
    )
    assert status == WorkflowStatusString.ERROR.value


def _require_terminal(dbos: DBOS, workflow_id: str) -> str:
    status = DBOS.get_workflow_status(workflow_id)
    assert status is not None
    assert status.status in (
        WorkflowStatusString.SUCCESS.value,
        WorkflowStatusString.ERROR.value,
    )
    return status.status


def test_roles_denied_queue_recovery(dbos: DBOS) -> None:
    # The QUEUE + RECOVERY combination (issue #743): when a role-denied queued
    # workflow is recovered, _recover_workflow takes the queue branch -- it
    # returns the row to the queue instead of running it directly. The queue must
    # then re-dispatch it and the dequeue path must finalize it as ERROR, rather
    # than bouncing it back to PENDING and redequeueing it forever.
    queue = DBOS.register_queue("test_roles_denied_queue_recovery")

    @DBOS.required_roles(["admin"])
    @DBOS.workflow()
    def workflow() -> None:
        return None

    with DBOSContextSetAuth("bob", ["reader"]):
        handle = queue.enqueue(workflow)
    with pytest.raises(DBOSNotAuthorizedError):
        handle.get_result()

    # Reset to PENDING (queue_name is preserved) and recover. Because the row is
    # queued, recovery returns it to the queue rather than executing it directly.
    set_workflow_status(dbos._sys_db, handle.workflow_id, "PENDING")
    recovery_handles = DBOS._recover_pending_workflows()
    assert len(recovery_handles) == 1
    with pytest.raises(DBOSNotAuthorizedError):
        recovery_handles[0].get_result()

    status = retry_until_success(
        lambda: _require_terminal(dbos, handle.workflow_id), interval=0.2
    )
    assert status == WorkflowStatusString.ERROR.value


@pytest.mark.asyncio
async def test_roles_denied_async(dbos: DBOS) -> None:
    # Same as test_roles_denied_start_workflow, for the async execution path.
    @DBOS.required_roles(["admin"])
    @DBOS.workflow()
    async def workflow() -> None:
        return None

    with DBOSContextSetAuth("bob", ["reader"]):
        handle = await DBOS.start_workflow_async(workflow)

    with pytest.raises(DBOSNotAuthorizedError):
        await handle.get_result()

    status = await DBOS.get_workflow_status_async(handle.workflow_id)
    assert status is not None
    assert status.status == WorkflowStatusString.ERROR.value
