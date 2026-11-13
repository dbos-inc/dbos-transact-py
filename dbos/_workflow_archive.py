"""Workflow archiving (Transact Enterprise): completed workflows are copied out of the
system database, and retention only deletes those already copied."""

from typing import TYPE_CHECKING, Optional, Protocol

from . import _enterprise
from ._error import DBOSWorkflowArchiveNotConfiguredError

if TYPE_CHECKING:
    from ._sys_db import SystemDatabase


class WorkflowArchive(Protocol):
    """Where completed workflows are archived, such as dbos_enterprise.ParquetArchive.
    Its archive_id names the location, so it must not change once workflows are in it.
    """

    @property
    def archive_id(self) -> str: ...


def import_archived_workflow(
    sys_db: "SystemDatabase",
    archive: Optional[WorkflowArchive],
    workflow_id: str,
    completed_at: int,
) -> None:
    """Copy the archived snapshot (workflow_id, completed_at) back into sys_db."""
    if archive is None:
        raise DBOSWorkflowArchiveNotConfiguredError()
    _enterprise.load(_enterprise.WORKFLOW_ARCHIVE.description).import_archived_workflow(
        sys_db, archive, workflow_id, completed_at
    )
