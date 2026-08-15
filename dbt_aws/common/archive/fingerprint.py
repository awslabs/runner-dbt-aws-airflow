"""Content fingerprint of a dbt project — fast, stable, mtime-based.

Used by the archive builder to decide whether a cached ``.tar.gz`` is
still current or needs to be rebuilt.

Fingerprint is the SHA-256 of
``<relpath>\\0<size>\\0<mtime_ns>\\n`` for every file we'd ship in the
archive, sorted by ``relpath``. mtime is a reasonable signal in
practice (faster than content hashing by 100-1000x); content-based
fingerprinting can be added later as an opt-in if false negatives from
``touch``-without-change become a real problem.

Files included in the fingerprint match :func:`iter_archive_files` —
i.e. exactly the same files that would land in the archive, plus the
optional manifest override path.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterator
from pathlib import Path

#: Top-level files (in the project root) that we always include if
#: present. ``profiles.yml`` is included by default — callers using
#: static-credentials profiles must opt out via ``include_profiles=False``.
_TOP_LEVEL_FILES: frozenset[str] = frozenset(
    {
        "dbt_project.yml",
        "packages.yml",
        "dependencies.yml",
        "selectors.yml",
    }
)

#: Top-level directories whose contents we include recursively when
#: present.
_INCLUDED_DIRS: frozenset[str] = frozenset(
    {
        "models",
        "seeds",
        "snapshots",
        "tests",
        "macros",
        "analyses",
        "dbt_packages",
    }
)

#: Names anywhere in the tree that we always skip — bytecode caches,
#: hidden directories, log dumps, IDE state.
_ALWAYS_SKIP: frozenset[str] = frozenset(
    {
        "__pycache__",
        ".git",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".idea",
        ".vscode",
        ".DS_Store",
        "logs",
    }
)

#: Extra files under ``target/`` that we ship (in addition to
#: ``manifest.json``) when present. These are dbt's parse-cache
#: artefacts; shipping them saves a cold ``full parse`` on the worker.
#:
#: * ``partial_parse.msgpack`` -- the parse cache dbt reads at the
#:   start of every invocation. Missing it triggers dbt's ``Unable to
#:   do partial parsing because saved manifest not found. Starting
#:   full parse.`` warning and adds 5-15s cold-start per JobRun.
#: * ``graph.gpickle`` -- the compiled DAG, useful for ``--state`` /
#:   ``--defer`` comparisons.
#:
#: Deliberately NOT in the fingerprint (see :func:`fingerprint_project`).
_EXTRA_TARGET_FILES: frozenset[str] = frozenset(
    {
        "partial_parse.msgpack",
        "graph.gpickle",
    }
)


def iter_archive_files(
    project_dir: Path,
    *,
    include_profiles: bool,
    manifest_path: Path | None,
) -> Iterator[tuple[Path, str]]:
    """Yield ``(absolute_path, archive_relpath)`` for every file we'd
    ship in the archive.

    Order is unspecified -- callers that need a deterministic order
    must sort the result.

    Args:
        project_dir: dbt project root.
        include_profiles: if True, include ``profiles.yml`` (if present).
            Default behaviour at the public API is True; callers using
            static-credentials profiles must opt out.
        manifest_path: explicit manifest source. If ``None``, we try
            ``<project_dir>/target/manifest.json``. The file is placed
            in the archive as ``target/manifest.json`` regardless of
            where it came from.

    Notes:
        Files under ``target/`` beyond ``manifest.json`` (specifically
        ``partial_parse.msgpack`` and ``graph.gpickle``) are shipped
        when present. They are dbt's parse-cache artefacts -- omitting
        them forces a full cold parse on every worker invocation and
        shows up as
        ``Unable to do partial parsing because saved manifest not
        found. Starting full parse.`` in dbt's log. These artefacts
        are content-derived from the source files, so they do NOT
        appear in :func:`fingerprint_project` -- see the docstring
        there for the cache-key rationale.
    """
    yield from _iter_source_files(project_dir, include_profiles=include_profiles)

    # Manifest -- either the override path or the default location.
    src = (
        Path(manifest_path)
        if manifest_path is not None
        else project_dir / "target" / "manifest.json"
    )
    if src.is_file():
        yield src, "target/manifest.json"

    # Extra parse-cache artefacts. Same directory as manifest.json
    # regardless of ``manifest_path`` (dbt writes them next to the
    # manifest). Include them ONLY when they live in the standard
    # location; a custom ``manifest_path`` pointing at an unrelated
    # directory is out of scope.
    for name in _EXTRA_TARGET_FILES:
        p = project_dir / "target" / name
        if p.is_file():
            yield p, f"target/{name}"


def _iter_source_files(project_dir: Path, *, include_profiles: bool) -> Iterator[tuple[Path, str]]:
    """Source files only — the inputs to ``dbt parse``, NOT its output.

    The fingerprint is built from these so that re-running ``dbt parse``
    against the same sources produces the same cache key (``dbt parse``
    rewrites ``target/manifest.json`` with a fresh mtime on every run,
    which would otherwise bust the cache on every DAG-parse heartbeat).
    """
    # Top-level files.
    for name in _TOP_LEVEL_FILES:
        p = project_dir / name
        if p.is_file():
            yield p, name

    if include_profiles:
        p = project_dir / "profiles.yml"
        if p.is_file():
            yield p, "profiles.yml"

    # Top-level directories, recursively.
    for dirname in _INCLUDED_DIRS:
        d = project_dir / dirname
        if not d.is_dir():
            continue
        for file_path in _walk_files(d):
            rel = file_path.relative_to(project_dir)
            yield file_path, rel.as_posix()


def fingerprint_project(
    project_dir: Path | str,
    *,
    include_profiles: bool = True,
    manifest_path: Path | str | None = None,  # noqa: ARG001 - accepted for API stability
    use_content_hash: bool = False,
) -> str:
    """Return a SHA-256 hex digest fingerprinting the project's SOURCE.

    The fingerprint covers the dbt project inputs only -- ``dbt_project.yml``,
    ``profiles.yml`` (when included), ``packages.yml`` / ``dependencies.yml``,
    and the source directories (``models/``, ``seeds/``, ``snapshots/``,
    ``tests/``, ``macros/``, ``analyses/``, ``dbt_packages/``).

    Two fingerprinting modes:

    * **Fast (default, backward-compatible).** Uses ``(relpath, size,
      mtime_ns)`` per file. Very fast even on large projects but
      trusts the filesystem's mtime -- an attacker who can replace
      file CONTENT while preserving size and mtime can poison the
      cache. Adequate for single-tenant developer machines and CI
      workspaces.
    * **Content-hash (, opt-in).** Pass ``use_content_hash=True``
      to SHA-256 each file's bytes. Cryptographically binds the
      fingerprint to actual content, so cache poisoning via inode
      overwrite is not possible. Recommended for shared filesystems
      (``/efs``, ``/mnt``, ``/nfs``) or CI environments where the
      project directory is writable by multiple principals. Costs
      ~50-200ms extra on typical dbt projects (~500 files).

    It deliberately does NOT include ``target/manifest.json``. ``dbt
    parse`` rewrites the manifest with a fresh mtime on every invocation
    even when the project is byte-identical, which would otherwise bust
    the archive cache on every DAG-parse heartbeat. The manifest still
    ships in the archive (latest available is included by
    :func:`build_project_archive`); it just doesn't drive the cache key.

    The ``manifest_path`` parameter is accepted for API stability but
    ignored -- the manifest doesn't affect the fingerprint.

    Two invocations against an identical source tree return the same
    digest; any change in source file path, size, or mtime (fast mode)
    or content (content-hash mode) changes it.
    """
    project = Path(project_dir)

    digest = hashlib.sha256()

    if use_content_hash:
        # Content-hash mode: SHA-256 each file's bytes. Files are
        # streamed 1MB at a time to bound peak memory even on large
        # seeds / manifests.
        items_ch: list[tuple[str, int, str]] = []
        for src, archive_relpath in _iter_source_files(project, include_profiles=include_profiles):
            content_hasher = hashlib.sha256()
            size = 0
            with src.open("rb") as fh:
                for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                    content_hasher.update(chunk)
                    size += len(chunk)
            items_ch.append((archive_relpath, size, content_hasher.hexdigest()))
        items_ch.sort(key=lambda t: t[0])
        for relpath, size, content_hex in items_ch:
            digest.update(relpath.encode("utf-8"))
            digest.update(b"\0")
            digest.update(str(size).encode("ascii"))
            digest.update(b"\0")
            digest.update(content_hex.encode("ascii"))
            digest.update(b"\n")
        return digest.hexdigest()

    # Fast mode (default): (relpath, size, mtime_ns) per file.
    items: list[tuple[str, int, int]] = []
    for src, archive_relpath in _iter_source_files(project, include_profiles=include_profiles):
        st = src.stat()
        items.append((archive_relpath, st.st_size, st.st_mtime_ns))

    items.sort(key=lambda t: t[0])

    for relpath, size, mtime_ns in items:
        digest.update(relpath.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(size).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(mtime_ns).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _walk_files(root: Path) -> Iterator[Path]:
    """Yield files under ``root`` recursively, skipping
    :data:`_ALWAYS_SKIP` names at any depth and files starting with a
    dot.
    """
    for dirpath, dirnames, filenames in os.walk(root):
        # Prune in place so os.walk doesn't descend into skip dirs.
        dirnames[:] = [d for d in dirnames if d not in _ALWAYS_SKIP and not d.startswith(".")]
        for fname in filenames:
            if fname in _ALWAYS_SKIP or fname.startswith("."):
                continue
            yield Path(dirpath) / fname
