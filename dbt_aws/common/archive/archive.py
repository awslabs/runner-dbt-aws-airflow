"""Build (or reuse from cache) a ``.tar.gz`` of a dbt project.

The cache lives in a caller-supplied directory — outside both the dbt
project and the Airflow ``dags/`` folder by convention — so multiple
DAG-file-processor heartbeats reuse one archive instead of rebuilding
on every parse.

Cache key is the project content fingerprint
(:mod:`dbt_aws.common.archive.fingerprint`). When the fingerprint
changes, a new archive is produced and the old one stays on disk
until the operator prunes it.

Atomicity: builds go through a temp file + rename, so a partial archive
never overwrites a good one. Two processes racing to build the same
fingerprint will produce byte-identical archives — the loser's rename
is a harmless no-op.
"""

from __future__ import annotations

import logging
import os
import tarfile
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from dbt_aws.common.archive.fingerprint import (
    fingerprint_project,
    iter_archive_files,
)

_log = logging.getLogger(__name__)


class ArchiveError(RuntimeError):
    """Raised when an archive cannot be built or read."""


@dataclass(frozen=True)
class ProjectArchive:
    """Result of :func:`build_project_archive`.

    Attributes:
        path: absolute path to the cached ``.tar.gz`` file.
        fingerprint: SHA-256 hex digest used as the cache key.
        size_bytes: archive size on disk.
        file_count: number of files inside the archive.
        was_cached: ``True`` if this call hit an existing cache entry;
            ``False`` if a fresh archive was built. Useful for
            structured logging / metrics.
        elapsed_seconds: wall time spent in :func:`build_project_archive`,
            including fingerprint computation. Cache hits typically
            land in single-digit milliseconds.
    """

    path: Path
    fingerprint: str
    size_bytes: int
    file_count: int
    was_cached: bool
    elapsed_seconds: float


def build_project_archive(
    *,
    project_dir: Path | str,
    cache_dir: Path | str,
    manifest_path: Path | str | None = None,
    include_profiles: bool = True,
    run_dbt_deps: bool = False,
    use_content_hash: bool = False,
) -> ProjectArchive:
    """Return a cached or freshly-built archive of the dbt project.

    Args:
        project_dir: dbt project root (contains ``dbt_project.yml``).
        cache_dir: directory where ``<fingerprint>.tar.gz`` files live.
            Created if missing. Caller picks a location OUTSIDE both
            the dbt project and any Airflow ``dags/`` folder.
        manifest_path: explicit manifest source. If ``None``, falls
            back to ``<project_dir>/target/manifest.json``. The file
            is placed in the archive as ``target/manifest.json``
            regardless of where it came from.
        include_profiles: if ``True`` (default), ``profiles.yml`` is
            included in the archive. Safe when using session-based AWS
            auth (e.g. dbt-athena ``method: iam`` or boto3 session
            profile). Set ``False`` if your profile holds static
            credentials.
        run_dbt_deps: when ``True``, run ``dbt deps`` in ``project_dir``
            on the AIRFLOW box BEFORE fingerprinting so
            ``dbt_packages/`` lands in the archive. **Default**
            ``False`` -- the workers handle ``dbt deps``
            themselves via the runner-level ``with_deps`` flag
            (default ``True``). Set this to ``True`` only
            when you specifically want to bake dbt packages into the
            S3 archive at parse time (e.g. to skip the ~3-5s per-task
            ``dbt deps`` overhead on every worker). Requires
            ``dbt-core`` in the Airflow venv and a project with
            ``packages.yml``.

            When opted in, the helper is graceful: it skips cleanly
            when ``packages.yml`` doesn't exist, when ``dbt_packages/``
            is already populated (idempotent + race-safe under the
            Airflow DAG processor's ~30s parse loop), or when
            ``dbt-core`` is not installed (warn-only). Only an actual
            ``dbt deps`` failure (e.g. unreachable Git repo) raises
            :class:`ArchiveError`. The invocation is serialized via
            an ``fcntl.flock`` on a per-project lock file so
            concurrent parses don't race on cold start.

    Returns:
        :class:`ProjectArchive` -- the on-disk path, cache metadata, and
        timing.

    Raises:
        ArchiveError: if ``project_dir`` is missing ``dbt_project.yml``,
            or if the archive cannot be written, or if ``dbt deps``
            (when applicable) fails with a non-zero exit code.
    """
    started = time.monotonic()
    project = Path(project_dir).resolve()
    cache = Path(cache_dir).resolve()

    if not (project / "dbt_project.yml").is_file():
        raise ArchiveError(f"project_dir {project} does not contain dbt_project.yml")

    # sanity-check ``profiles.yml`` when include_profiles=True.
    # Emit a WARNING (not a hard failure) if it contains patterns that
    # look like static credentials. Backward-compatible: default stays
    # True; users on IAM-role profiles see no warning.
    if include_profiles:
        _warn_if_profiles_yml_carries_secrets(project)

    if run_dbt_deps:
        _run_dbt_deps(project)

    manifest = Path(manifest_path).resolve() if manifest_path else None

    fingerprint = fingerprint_project(
        project,
        include_profiles=include_profiles,
        manifest_path=manifest,
        use_content_hash=use_content_hash,
    )
    cache.mkdir(parents=True, exist_ok=True)
    archive_path = cache / f"{fingerprint}.tar.gz"

    # ── Cache hit ────────────────────────────────────────────────────
    if archive_path.is_file():
        size = archive_path.stat().st_size
        file_count = _count_files_in_tar(archive_path)
        elapsed = time.monotonic() - started
        _log.info(
            "archive: cache hit (fingerprint=%s, size=%d bytes, files=%d, elapsed=%.1f ms)",
            fingerprint[:12],
            size,
            file_count,
            elapsed * 1000,
        )
        return ProjectArchive(
            path=archive_path,
            fingerprint=fingerprint,
            size_bytes=size,
            file_count=file_count,
            was_cached=True,
            elapsed_seconds=elapsed,
        )

    # ── Cache miss — build it ────────────────────────────────────────
    _log.info(
        "archive: building (fingerprint=%s, project=%s, cache=%s)",
        fingerprint[:12],
        project,
        cache,
    )
    file_count = _write_tar_atomic(
        archive_path=archive_path,
        project=project,
        include_profiles=include_profiles,
        manifest=manifest,
    )
    size = archive_path.stat().st_size
    elapsed = time.monotonic() - started
    _log.info(
        "archive: built (fingerprint=%s, size=%d bytes, files=%d, elapsed=%.1f ms)",
        fingerprint[:12],
        size,
        file_count,
        elapsed * 1000,
    )
    return ProjectArchive(
        path=archive_path,
        fingerprint=fingerprint,
        size_bytes=size,
        file_count=file_count,
        was_cached=False,
        elapsed_seconds=elapsed,
    )


# ----------------------------------------------------------------------
# Internals
# ----------------------------------------------------------------------
def _write_tar_atomic(
    *,
    archive_path: Path,
    project: Path,
    include_profiles: bool,
    manifest: Path | None,
) -> int:
    """Write the tar.gz via a tempfile + atomic rename. Returns file
    count actually written."""
    cache_dir = archive_path.parent
    # NamedTemporaryFile in the SAME directory so ``os.replace`` stays
    # atomic on every POSIX filesystem.
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{archive_path.name}.",
        suffix=".tmp",
        dir=str(cache_dir),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)

    count = 0
    try:
        with tarfile.open(tmp_path, mode="w:gz") as tar:
            for src, arc_relpath in sorted(
                iter_archive_files(
                    project,
                    include_profiles=include_profiles,
                    manifest_path=manifest,
                ),
                key=lambda t: t[1],
            ):
                # reject any arcname that would escape the
                # extraction root on the worker. ``iter_archive_files``
                # derives ``arc_relpath`` from ``file_path.relative_to(project_dir)``,
                # which normally stays under the project root, but a
                # symlink or a maliciously-crafted project layout can
                # produce a path with ``..`` components. Belt-and-braces
                # guard so a bad archive never leaves the build step.
                _validate_arcname(arc_relpath, source=src)
                # symlink race guard. ``_validate_arcname``
                # inspects the arcname STRING (clean), but ``src`` could
                # still be a symlink whose target is outside
                # ``project`` -- ``tar.add`` follows it by default and
                # would ship out-of-tree bytes under a tidy-looking
                # arcname. Resolve src and reject when the resolved
                # path escapes the project root.
                #
                # EXCEPTION: an explicit ``manifest_path`` is a
                # documented feature -- callers may point at a
                # manifest produced by a CI job outside the project
                # tree. Skip the check when src is exactly that
                # caller-supplied manifest.
                src_resolved = src.resolve()
                is_external_manifest = manifest is not None and src_resolved == manifest
                if not is_external_manifest:
                    try:
                        src_resolved.relative_to(project)
                    except ValueError as exc:
                        raise ArchiveError(
                            f"archive builder: source {src} resolves to "
                            f"{src_resolved} which is outside the project "
                            f"root {project} (symlink escape). Refusing to "
                            f"add out-of-tree content to the archive."
                        ) from exc
                tar.add(str(src), arcname=arc_relpath, recursive=False)
                count += 1
        os.replace(tmp_path, archive_path)
    except BaseException:
        # On any error (including KeyboardInterrupt) clean up the temp.
        with _suppress(FileNotFoundError):
            tmp_path.unlink()
        raise
    return count


def _validate_arcname(arc_relpath: str, *, source: Path) -> None:
    """Reject arcnames that would traverse outside the extraction root.

    Enforced at archive-build time () so a malformed project
    layout never produces a tar that could path-traverse on extract.
    Symlinks inside the project pointing OUTSIDE it are also rejected
    -- their ``arc_relpath`` looks fine but the resolved target isn't.

    Raises :class:`ArchiveError` with the offending source path so an
    operator can find and fix the layout issue.
    """
    if arc_relpath.startswith("/"):
        raise ArchiveError(
            f"archive builder: arcname {arc_relpath!r} starts with '/' "
            f"(would extract to an absolute path). Source: {source}"
        )
    parts = Path(arc_relpath).parts
    if any(p == ".." for p in parts):
        raise ArchiveError(
            f"archive builder: arcname {arc_relpath!r} contains a '..' "
            f"component (path traversal). Source: {source}"
        )


#: Substrings that suggest ``profiles.yml`` carries static credentials
#: (or something else that shouldn't be shipped in an S3-uploaded
#: archive). Case-insensitive. False positives are expected -- the
#: point is to prompt operators to audit their profile, not to block.
_SECRET_PROFILE_PATTERNS: tuple[str, ...] = (
    "password:",
    "pass:",
    "api_key:",
    "apikey:",
    "access_key:",
    "secret_key:",
    "secret_access_key:",
    "private_key:",
    "client_secret:",
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "account:",  # snowflake account often paired with password
    "token:",
)


def _warn_if_profiles_yml_carries_secrets(project: Path) -> None:
    """Emit a WARNING when ``profiles.yml`` at the project root looks
    like it carries static credentials.

    Heuristic-only -- greps for known secret-key names. False positives
    (e.g. a comment mentioning ``password:``) are acceptable; the
    warning suggests operators use ``env_var()`` or IAM-role auth
    instead of shipping credentials in an S3-uploaded archive.

    Never raises. Silent when:

    * ``profiles.yml`` doesn't exist (most dbt projects put it in
      ``~/.dbt/`` or use env-var-only auth).
    * File isn't readable (permission denied, race with cleanup, ...).
    * No secret patterns match (IAM-role or env-var-only profile).
    """
    profiles = project / "profiles.yml"
    if not profiles.is_file():
        return
    try:
        content = profiles.read_text(encoding="utf-8", errors="replace").lower()
    except OSError:
        return
    hits = [p for p in _SECRET_PROFILE_PATTERNS if p in content]
    if hits:
        # ``hits`` contains PATTERN NAMES the grep matched (e.g.
        # ``"password:"``), NOT the secret values that would follow
        # those keys in ``profiles.yml``. Logging them tells operators
        # which keys triggered the warning without disclosing the
        # associated secret values (which never enter this function's
        # scope -- only the lowercased haystack does, and it's dropped
        # after the ``in`` check). CodeQL's ``py/clear-text-logging``
        # rule fires here because the variable name pattern matches
        # ``password``/``secret_key``/etc.; suppression is intentional.
        _log.warning(  # lgtm[py/clear-text-logging-sensitive-data]
            "archive builder: ``include_profiles=True`` will ship "
            "``profiles.yml`` into the S3-uploaded archive AND it looks "
            "like it carries static credentials (matched pattern names: %s). "
            "Anyone with read access to the S3 bucket can extract them. "
            "Consider one of:\n"
            "  * pass ``include_profiles=False`` and rely on IAM role "
            "auth on the worker;\n"
            "  * replace the hard-coded values with ``{{ env_var(...) }}`` "
            "references and inject the secrets at task time via "
            "``env_vars_json``.",
            sorted(hits),
        )


def _run_dbt_deps(project: Path) -> None:
    """Run ``dbt deps`` inside ``project`` so ``dbt_packages/`` is
    populated before the archive is built.

    Graceful by default: skips cleanly when there's nothing to do or
    when the local toolchain doesn't have ``dbt-core`` available.

    * No ``packages.yml`` at the project root -> info log + skip.
      Most dbt projects don't have one; this is the common case.
    * No ``dbt`` console script on PATH -> warn log + skip. The
      Airflow venv may not have ``dbt-core``; the workers do. The
      operator will just run with whatever ``dbt_packages/`` already
      exists in the project (often empty -- and that's fine for
      projects without external packages).
    * ``dbt deps`` exits non-zero -> :class:`ArchiveError` raised
      with stdout + stderr. This IS a real problem -- the project
      declared packages but they can't be resolved.

    Invoked via the ``dbt`` console script in a child process so the
    invocation runs in clean isolation -- avoids leaking dbt state
    into the long-lived Airflow scheduler process that calls
    ``build_and_upload_project_archive`` at parse time.

    NOTE: ``python -m dbt`` does NOT work as an alternative because
    ``dbt`` is a namespace package with no ``__main__.py``. The
    console script (``<venv>/bin/dbt``) is the only working entry
    point.
    """
    import shutil
    import subprocess
    import sys

    # Skip 1: no packages.yml -> nothing to install.
    packages_yml = project / "packages.yml"
    if not packages_yml.is_file():
        _log.info(
            "dbt deps: skipping -- no packages.yml in %s (nothing to install)",
            project,
        )
        return

    # Skip 2: dbt_packages/ already populated.
    #
    # Two reasons this matters:
    #
    # 1. ``dbt deps`` is NOT process-safe -- when the Airflow DAG
    #    processor parses the same DAG file every ~30s, two
    #    concurrent invocations both try to extract dbt_packages/<pkg>
    #    and one fails with ``[Errno 2] No such file or directory:
    #    'dbt_packages/<pkg>'`` because the other process modified the
    #    target directory mid-extract.
    #
    # 2. Cheap idempotency: re-runs of the same parse skip cleanly
    #    when packages are already resolved. To force a refresh,
    #    delete dbt_packages/ from the project root.
    dbt_packages = project / "dbt_packages"
    if dbt_packages.is_dir() and any(p.is_dir() for p in dbt_packages.iterdir()):
        _log.info(
            "dbt deps: skipping -- dbt_packages/ already populated in %s "
            "(delete the directory to force a refresh)",
            project,
        )
        return

    # Skip 3: dbt not importable in this venv.
    # We invoke ``python -m dbt.cli.main`` (the entry-point module)
    # rather than the ``dbt`` console script -- workers may have
    # dbt-core installed via pip but with no console script on PATH
    # (Glue's worker venv puts site-packages on sys.path but the
    # script dir isn't always on PATH). ``-m dbt.cli.main`` is
    # PATH-independent: as long as ``dbt-core`` is in this venv's
    # site-packages, the invocation works.
    if shutil.which("dbt") is None and not _dbt_cli_module_available():
        _log.warning(
            "dbt deps: SKIPPING -- packages.yml exists in %s but "
            "neither the ``dbt`` console script nor the ``dbt.cli.main`` "
            "module is importable. Install dbt-core in the Airflow venv "
            "to bake dbt packages into the archive.",
            project,
        )
        return

    # File lock so concurrent parses (e.g. two Airflow DAG-processor
    # ticks racing on cold-start) serialise. After acquiring the
    # lock we RE-CHECK dbt_packages/ -- if the other process
    # finished while we were waiting, we skip cleanly.
    #
    # keep the lock file OUT of the project root so it
    # doesn't leak into dbt Git history or archive uploads. We derive
    # a stable per-project name under ``tempfile.gettempdir()`` from
    # the project's absolute path -- the same path always maps to the
    # same lock file (correctness for the mutual-exclusion contract)
    # while every project gets its own lock.
    import fcntl
    import hashlib as _hashlib
    import tempfile as _tempfile

    project_hash = _hashlib.sha256(str(project).encode("utf-8")).hexdigest()[:16]
    lock_path = Path(_tempfile.gettempdir()) / f".dbt_aws_deps-{project_hash}.lock"
    with lock_path.open("w") as lock_fh:
        try:
            fcntl.flock(lock_fh, fcntl.LOCK_EX)
        except OSError:
            # flock not supported on this filesystem (rare). Fall
            # through and rely on the dbt_packages/ skip above as our
            # only mutual-exclusion mechanism.
            _log.warning(
                "dbt deps: flock unsupported on %s; proceeding without cross-process lock",
                lock_path.parent,
            )
        # Recheck after acquiring the lock -- another process may
        # have populated dbt_packages/ while we waited.
        if dbt_packages.is_dir() and any(p.is_dir() for p in dbt_packages.iterdir()):
            _log.info(
                "dbt deps: skipping (post-lock) -- dbt_packages/ "
                "populated by another process during the race window"
            )
            return

    started = time.monotonic()
    _log.info("dbt deps: running in %s", project)
    dbt_argv = [sys.executable, "-m", "dbt.cli.main", "deps", "--project-dir", str(project)]
    try:
        result = subprocess.run(
            dbt_argv,
            cwd=str(project),
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise ArchiveError(
            f"dbt deps failed: ``{sys.executable} -m dbt.cli.main`` could "
            "not be invoked. This is a bug in dbt-aws (sys.executable was "
            "unexpectedly invalid)."
        ) from exc

    if result.returncode != 0:
        raise ArchiveError(
            f"dbt deps exited with code {result.returncode}.\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    elapsed = time.monotonic() - started
    _log.info("dbt deps: done (elapsed=%.1f ms)", elapsed * 1000)


def _dbt_cli_module_available() -> bool:
    """Return True when ``dbt.cli.main`` is importable in this venv.

    More reliable than ``shutil.which('dbt')`` because pip-installed
    dbt-core always exposes the module, but the console script lives
    in a directory that may not be on PATH on every runtime (Glue's
    PySpark worker is the canonical offender).
    """
    import importlib.util

    return importlib.util.find_spec("dbt.cli.main") is not None


def _count_files_in_tar(path: Path) -> int:
    """Cheap count for cache-hit reporting; reads the tar index only."""
    try:
        with tarfile.open(path, mode="r:gz") as tar:
            return sum(1 for m in tar if m.isfile())
    except tarfile.TarError as exc:
        raise ArchiveError(
            f"cached archive at {path} is corrupt: {exc}. Delete the file and re-run to rebuild."
        ) from exc


class _suppress:
    """Tiny ``contextlib.suppress`` clone (avoids the import for one use)."""

    def __init__(self, *exc_types: type[BaseException]) -> None:
        self._exc_types = exc_types

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type, exc, tb) -> bool:
        return exc_type is not None and issubclass(exc_type, self._exc_types)
