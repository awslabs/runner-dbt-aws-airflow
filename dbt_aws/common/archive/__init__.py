"""Content-fingerprinted dbt project archives.

Public surface:

* :class:`ProjectArchive` — result handle (path, fingerprint, cache
  status, timing).
* :class:`ArchiveError` — anything we couldn't build or read.
* :func:`build_project_archive` — main entry point.
* :func:`fingerprint_project` — exposed for callers that want the
  cache key without building.
"""

from dbt_aws.common.archive.archive import (
    ArchiveError,
    ProjectArchive,
    build_project_archive,
)
from dbt_aws.common.archive.fingerprint import fingerprint_project

__all__ = [
    "ArchiveError",
    "ProjectArchive",
    "build_project_archive",
    "fingerprint_project",
]
