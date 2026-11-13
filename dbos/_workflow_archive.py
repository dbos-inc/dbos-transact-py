"""Workflow archiving (Transact Enterprise): completed workflows are copied out of the
system database, and retention only deletes those already copied."""

from typing import Protocol


class WorkflowArchive(Protocol):
    """Where completed workflows are archived, such as dbos_enterprise.ParquetArchive.
    Its archive_id names the location, so it must not change once workflows are in it.
    """

    @property
    def archive_id(self) -> str: ...
