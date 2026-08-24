"""Auto-deploy helpers for the lib's mode='create' path.

What the lib auto-uploads when ``mode='create'`` and the caller asks
for it via ``deploy_bucket=``:

* The bundled worker entry script (via :func:`upload_worker_entrypoint`).
  Runs once per (bucket, prefix); idempotent via ETag/md5 compare. Lets
  callers point a runner at a bucket instead of pre-uploading the
  script themselves.

* The dbt project archive (via :func:`upload_project_archive`). Called
  from DAG code at parse time so remote runners can fetch it.

What the lib does NOT auto-upload (still external responsibility):

* The lib's own wheel. Users install the wheel from PyPI
  (``pip install runner-dbt-aws-airflow``) or, for Glue Python Shell
  which can't reach PyPI, mirror the wheel to their own S3 bucket and
  reference the ``s3://`` URI in the Glue Job's
  ``--additional-python-modules``.

What the lib NEVER creates:

* S3 buckets -- user provisions ahead of time.
* IAM roles -- user provisions ahead of time.

All uploads are idempotent (HEAD-before-PUT, content-addressed where
the content changes, ETag-compared where the key is stable), so it's
safe to call them on every DAG-parse heartbeat.
"""

from __future__ import annotations

import hashlib
import logging
import os
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from dbt_aws.common.archive.archive import ProjectArchive

_log = logging.getLogger(__name__)

#: Default S3 key prefix when none provided.
DEFAULT_DEPLOY_PREFIX = "dbt-aws"


# ----------------------------------------------------------------------
# Entry script source (bundled with the lib)
# ----------------------------------------------------------------------
def get_worker_entrypoint_source() -> str:
    """Return the worker entry script Python source as a string.

    Sourced from ``dbt_aws.common._worker_entrypoint`` which ships
    inside this wheel. External deploy scripts read this and upload
    it to S3 themselves; the lib does NOT upload it.
    """
    return (
        resources.files("dbt_aws.common")
        .joinpath("_worker_entrypoint.py")
        .read_text(encoding="utf-8")
    )


def upload_worker_entrypoint(
    *,
    bucket: str,
    prefix: str = DEFAULT_DEPLOY_PREFIX,
    region_name: str | None = None,
    boto3_session: Any | None = None,
) -> str:
    """Upload the bundled entry script to S3. Returns the S3 URI.

    Content-addressed key: ``{prefix}/worker_entrypoint/<md5>.py``.
    Every distinct script version lives at its own URI, so existing
    Glue Jobs referencing an older script keep working when the lib
    is upgraded -- only newly-created / updated Glue Jobs pick up the
    new URI. Manual S3 deletes don't cascade across versions.

    Idempotent: HEAD by key first; skip the PUT when the object
    already exists (which by definition has the same content, since
    the key IS the md5 of the content).

    Called by ``GlueSparkRunner`` / ``GluePythonShellRunner`` when
    ``mode='create'`` and the caller passed ``deploy_bucket=`` instead
    of ``script_location=``. Lets infrastructure-style users avoid
    managing the script upload separately from the rest of the lib's
    lifecycle.
    """
    source_bytes = get_worker_entrypoint_source().encode("utf-8")
    source_md5 = hashlib.md5(source_bytes, usedforsecurity=False).hexdigest()
    key = f"{prefix.rstrip('/')}/worker_entrypoint/{source_md5}.py"
    s3_uri = f"s3://{bucket}/{key}"

    s3 = _get_s3_client(region_name=region_name, boto3_session=boto3_session)
    if _object_exists(s3, bucket, key):
        _log.info("deploy: entry script already at %s (skipping)", s3_uri)
        return s3_uri

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=source_bytes,
        ContentType="text/x-python",
    )
    _log.info(
        "deploy: uploaded entry script (%d bytes) -> %s",
        len(source_bytes),
        s3_uri,
    )
    return s3_uri


# ----------------------------------------------------------------------
# Project archive upload (the only auto-upload the lib performs)
# ----------------------------------------------------------------------
def upload_project_archive(
    *,
    archive: ProjectArchive | Path | str,
    bucket: str,
    prefix: str = DEFAULT_DEPLOY_PREFIX,
    region_name: str | None = None,
    boto3_session: Any | None = None,
) -> str:
    """Upload a project archive to S3. Returns the S3 URI.

    Idempotent: archives are content-fingerprinted (the filename IS
    the SHA-256), so we HEAD by key first and skip the PUT if found.

    adds a process-local TTL cache on the HEAD result so
    Airflow's DAG-processor tick (typically every 30s) doesn't fire
    a fresh HEAD on every parse cycle. TTL is 60 seconds; controlled
    by ``DBT_AWS_S3_HEAD_TTL_SECONDS`` env var if you want it tighter
    or looser. Set to ``0`` to disable the cache entirely (returns to
    earlier behaviour).
    """
    archive_path = _coerce_archive_path(archive)
    key = f"{prefix.rstrip('/')}/archives/{archive_path.name}"
    s3_uri = f"s3://{bucket}/{key}"

    # TTL cache -- content-fingerprinted key never changes when the
    # project source hasn't, so once we've confirmed the object
    # exists we can trust the local cache for a bounded window
    # without hitting S3 again.
    if _head_cache_has_recent_hit(bucket=bucket, key=key):
        _log.debug(
            "deploy: HEAD cache hit for %s (within TTL); skipping S3 call",
            s3_uri,
        )
        return s3_uri

    s3 = _get_s3_client(region_name=region_name, boto3_session=boto3_session)
    if _object_exists(s3, bucket, key):
        _log.info("deploy: archive already present at %s (skipping upload)", s3_uri)
        _head_cache_record(bucket=bucket, key=key)
        return s3_uri

    s3.upload_file(str(archive_path), bucket, key)
    _log.info(
        "deploy: uploaded archive %s (%d bytes) -> %s",
        archive_path.name,
        archive_path.stat().st_size,
        s3_uri,
    )
    _head_cache_record(bucket=bucket, key=key)
    return s3_uri


_head_cache: dict[tuple[str, str], float] = {}


def _head_ttl_seconds() -> float:
    """Read the S3 HEAD cache TTL from the env, defaulting to 60s.

    Set ``DBT_AWS_S3_HEAD_TTL_SECONDS=0`` to disable (matches earlier
    behaviour where every DAG-parse tick hit S3).
    """
    raw = os.environ.get("DBT_AWS_S3_HEAD_TTL_SECONDS", "60")
    try:
        ttl = float(raw)
    except ValueError:
        ttl = 60.0
    return max(ttl, 0.0)


def _head_cache_has_recent_hit(*, bucket: str, key: str) -> bool:
    ttl = _head_ttl_seconds()
    if ttl <= 0:
        return False
    import time as _time

    last = _head_cache.get((bucket, key))
    if last is None:
        return False
    return (_time.monotonic() - last) < ttl


def _head_cache_record(*, bucket: str, key: str) -> None:
    import time as _time

    _head_cache[(bucket, key)] = _time.monotonic()


def build_and_upload_project_archive(
    *,
    project_dir: Path | str,
    cache_dir: Path | str,
    bucket: str,
    prefix: str = DEFAULT_DEPLOY_PREFIX,
    region_name: str | None = None,
    include_profiles: bool = True,
    run_dbt_deps: bool = False,
    use_content_hash: bool = False,
    boto3_session: Any | None = None,
) -> str:
    """Build the archive (idempotent via :func:`build_project_archive`)
        then upload it to S3 (idempotent via HEAD). Returns the S3 URI.

        Safe to call on every DAG-parse heartbeat -- both steps are
        content-hash-keyed and skip when the content is unchanged.

        Args:
            run_dbt_deps: default ``False``. When set, runs
                ``dbt deps`` in ``project_dir`` on the AIRFLOW box before
                archiving so external dbt packages (``dbt_utils``,
                ``dbt_expectations``, …) land in ``dbt_packages/`` and
                ship to workers as part of the archive. Most users leave
                this ``False`` -- workers install dbt packages themselves
                via the runner-level ``with_deps`` flag (default ``True``
    ), which keeps MWAA's ``requirements.txt`` lean
                and avoids parse-time races. See
                :func:`build_project_archive` for the full skip-path
                behaviour when this is enabled.
            (rest of kwargs documented on :func:`build_project_archive`).
    """
    from dbt_aws.common.archive.archive import build_project_archive

    archive = build_project_archive(
        project_dir=project_dir,
        cache_dir=cache_dir,
        include_profiles=include_profiles,
        run_dbt_deps=run_dbt_deps,
        use_content_hash=use_content_hash,
    )
    return upload_project_archive(
        archive=archive,
        bucket=bucket,
        prefix=prefix,
        region_name=region_name,
        boto3_session=boto3_session,
    )


# ----------------------------------------------------------------------
# Internals
# ----------------------------------------------------------------------
def _coerce_archive_path(
    archive: ProjectArchive | Path | str,
) -> Path:
    """Accept either a :class:`ProjectArchive`, a path string, or a
    :class:`Path`."""
    if hasattr(archive, "path"):
        return Path(archive.path)
    return Path(archive)


def _get_s3_client(*, region_name: str | None, boto3_session: Any | None):  # noqa: ANN201
    """Lazy-import boto3 so this module is loadable without it."""
    if boto3_session is not None:
        return boto3_session.client("s3")
    import boto3

    return boto3.client("s3", region_name=region_name)


def _object_exists(s3: Any, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).lower()
        if "404" in msg or "not found" in msg or "nosuchkey" in msg:
            return False
        raise


def _object_etag_matches(
    s3: Any,
    bucket: str,
    key: str,
    expected_md5: str,
) -> bool:
    """Return True iff key exists AND its ETag matches expected md5.

    S3 ETag for non-multipart uploads equals the md5 of the body,
    wrapped in quotes. We tolerate either form.
    """
    try:
        resp = s3.head_object(Bucket=bucket, Key=key)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).lower()
        if "404" in msg or "not found" in msg or "nosuchkey" in msg:
            return False
        raise
    etag = (resp.get("ETag") or "").strip('"')
    return etag == expected_md5


__all__ = [
    "DEFAULT_DEPLOY_PREFIX",
    "build_and_upload_project_archive",
    "get_worker_entrypoint_source",
    "upload_project_archive",
    "upload_worker_entrypoint",
]
