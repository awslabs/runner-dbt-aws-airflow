"""Worker-side runner for dbt-aws.

Lives in :mod:`dbt_aws.common` because every backend (Glue Spark, Glue
Python Shell, EMR Serverless, ECS task, ...) does the same six things
on the worker:

1. Parse the runner CLI args.
2. Apply env vars from ``--env-vars``.
3. Download + extract the dbt project archive from S3.
4. Optionally sync a state directory from S3 for dbt ``--state`` /
   ``--defer``.
5. Build a dbt argv and exec ``python -m dbt.cli.main``.
6. Optionally upload ``target/`` to S3.

The bundled worker entry-point (``dbt_aws/common/_worker_entrypoint.py``)
is uploaded to S3 by the Airflow-side helpers and is the same file Glue
and EMR both use; it does nothing but ``sys.exit(main())``.

Boto3 is imported lazily inside the functions that need it so this
module can be imported in environments without boto3 (CI, type-checking,
doc builds). At runtime on a Glue or EMR worker, boto3 is always there.
"""

from __future__ import annotations

import argparse
import atexit
import contextlib
import json
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from urllib.parse import urlparse

# ``botocore`` is a worker-side dep. Import lazily so tools that
# import this module for its typing / helpers (e.g. Airflow's
# DAG-file-processor via ``from dbt_aws.common import ...``) don't
# require boto3/botocore. ``_client_error()`` returns the class at
# call time; falls back to a generic ``Exception`` catch when
# botocore isn't installed (no-op change vs. earlier broad catch).
try:
    from botocore.exceptions import ClientError as ClientError  # noqa: F401,PLC0414
except ImportError:  # pragma: no cover -- CI + type-check environments
    ClientError = Exception  # noqa: F811

_log = logging.getLogger(__name__)

# Lazy per-process working directory. Populated on first call to
# :func:`_get_work_base` so unit tests that only import this module
# don't create an empty ``/tmp/dbt_aws_*`` on disk.
#
# SECURITY: the earlier base was a deterministic
# ``<tempdir>/dbt_aws`` -- readable to any other user on the same host.
# We now ``tempfile.mkdtemp`` a private (mode 0o700, random-suffix)
# root per Python process and register an ``atexit`` cleanup so
# scratch data doesn't linger on long-lived Glue Interactive Session
# workers.
_WORK_BASE: Path | None = None


def _get_work_base() -> Path:
    """Return the per-process working-directory root (memoised).

    First call: ``tempfile.mkdtemp(prefix="dbt_aws_")`` (mode 0o700,
    random suffix). Cleanup is registered via ``atexit`` so long-lived
    workers reclaim the disk on process exit.

    Subsequent calls in the same process return the same path so
    parallel same-DagRun tasks can share the extracted archive.
    Different Python processes get different roots -- cross-user or
    cross-JobRun read/write leakage is not possible.
    """
    global _WORK_BASE
    if _WORK_BASE is None:
        base = Path(tempfile.mkdtemp(prefix="dbt_aws_"))
        atexit.register(shutil.rmtree, base, ignore_errors=True)
        _WORK_BASE = base
    return _WORK_BASE


# ----------------------------------------------------------------------
# Argparse
# ----------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    """Mirror of the flags emitted by every concrete runner in this
    lib (see :meth:`GlueSparkRunner._build_script_args`)."""
    p = argparse.ArgumentParser(prog="dbt-aws-runner")
    p.add_argument(
        "--command",
        required=True,
        choices=["run", "snapshot", "seed", "test", "build", "compile"],
    )
    p.add_argument("--select", required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--project-archive", required=True)
    p.add_argument("--stratus-run-id", default="")
    p.add_argument("--full-refresh", default=None)
    p.add_argument("--vars", default=None)
    p.add_argument("--profile-name", default=None)
    p.add_argument("--state-s3", default=None)
    p.add_argument("--defer", default=None)
    p.add_argument("--env-vars", default=None)
    p.add_argument("--dbt-extra-flags", default=None)
    p.add_argument("--upload-artefacts-s3", default=None)
    # When ``"true"``, run ``dbt deps`` on the worker BEFORE the main
    # dbt command. Default ``"true"`` -- workers install dbt packages
    # into ``dbt_packages/`` in the per-task ``/tmp`` working dir so
    # the project's ``packages.yml`` resolves without MWAA needing
    # dbt-core. Skipped if no ``packages.yml`` or if
    # ``dbt_packages/`` is already populated (the Airflow-side
    # ``run_dbt_deps=True`` baked them in).
    p.add_argument("--with-deps", default="true")
    # ------------------------------------------------------------------
    # OpenLineage / SMUS integration (optional).
    # These flags are inert unless ``--ol-namespace`` is set (i.e. the
    # runner opted in via ``openlineage=OpenLineageConfig(...)``).
    # ------------------------------------------------------------------
    p.add_argument(
        "--dbt-binary",
        default="dbt",
        choices=["dbt", "dbt-ol"],
        help="'dbt' (default) or 'dbt-ol' to emit OpenLineage events.",
    )
    p.add_argument("--ol-namespace", default=None)
    p.add_argument("--ol-s3-uri", default=None)
    p.add_argument("--ol-smus-domain", default=None)
    p.add_argument("--ol-smus-region", default=None)
    p.add_argument("--ol-parent-run-id", default=None)
    p.add_argument("--ol-parent-job-name", default=None)
    p.add_argument("--ol-parent-job-namespace", default=None)
    p.add_argument("--ol-node-unique-id", default=None)
    p.add_argument("--ol-extra-env", default=None)
    return p


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """``parse_known_args`` silently drops Glue/EMR framework flags
    (``--JOB_NAME`` etc.) that leak into ``sys.argv``."""
    args, _ = _build_parser().parse_known_args(argv)
    return args


# ----------------------------------------------------------------------
# Steps
# ----------------------------------------------------------------------
def _apply_env_vars(env_vars_json: str | None) -> None:
    if env_vars_json is None:
        return
    parsed = json.loads(env_vars_json)
    if not isinstance(parsed, dict):
        raise ValueError(f"--env-vars must decode to a JSON object, got {type(parsed).__name__}")
    for k, v in parsed.items():
        os.environ[str(k)] = str(v)
    # SECURITY: log only the keys, never the values. ``--env-vars``
    # commonly carries credentials (adapter passwords, API tokens);
    # the value would end up in CloudWatch verbatim otherwise. The
    # key list is still useful for debugging "why is X not set".
    _log.info("env: applied %d variable(s): keys=%s", len(parsed), sorted(parsed.keys()))


def _run_dbt_deps_on_worker(project_dir: Path) -> None:
    """Run ``dbt deps`` on the worker before the main dbt command.

    Companion to ``dbt_aws.common.archive.archive._run_dbt_deps`` (the
    Airflow-side helper). This one runs on the WORKER -- inside the
    Glue / EMR Python process -- so projects can ship dbt packages
    without needing ``dbt-core`` on the Airflow box.

    Skip paths:

    * No ``packages.yml`` -> info log + skip. Nothing to install.
    * ``dbt_packages/`` already populated -> info log + skip. The
      Airflow-side helper already ran; no need to repeat.
    * ``dbt deps`` exits non-zero -> raise. The runner explicitly
      opted in to worker-side deps; failing silently would be wrong.

    Invocation: ``sys.executable -m dbt.cli.main deps`` -- avoids
    relying on the ``dbt`` console script being on PATH. Glue's
    PySpark worker installs dbt-core via ``--additional-python-modules``
    so the ``dbt.cli.main`` module is in this Python's site-packages,
    but the console script is in a directory PATH may not include.
    """
    import importlib.util

    packages_yml = project_dir / "packages.yml"
    if not packages_yml.is_file():
        _log.info("dbt deps (worker): skipping -- no packages.yml in %s", project_dir)
        return

    dbt_packages = project_dir / "dbt_packages"
    if dbt_packages.is_dir() and any(p.is_dir() for p in dbt_packages.iterdir()):
        # Validate each sub-package -- a populated ``dbt_packages/`` is
        # only safe to skip if every package directory contains its own
        # ``dbt_project.yml`` (dbt itself fails hard otherwise). A
        # corrupt extract -- e.g. an archive that shipped a half-built
        # ``dbt_packages/`` from a flaky local ``dbt deps`` -- gets
        # wiped and rebuilt here so the worker recovers cleanly.
        corrupt = [
            p.name
            for p in dbt_packages.iterdir()
            if p.is_dir() and not (p / "dbt_project.yml").is_file()
        ]
        if not corrupt:
            _log.info(
                "dbt deps (worker): skipping -- dbt_packages/ already populated in %s",
                project_dir,
            )
            return
        _log.warning(
            "dbt deps (worker): wiping corrupt dbt_packages/ in %s "
            "(missing dbt_project.yml in: %s); will re-run dbt deps",
            project_dir,
            ", ".join(sorted(corrupt)),
        )
        import shutil  # noqa: PLC0415

        shutil.rmtree(dbt_packages, ignore_errors=True)

    if importlib.util.find_spec("dbt.cli.main") is None:
        raise RuntimeError(
            "dbt deps (worker): packages.yml present in "
            f"{project_dir} but ``dbt.cli.main`` is not importable. The "
            "worker installs dbt-core via ``--additional-python-modules`` -- "
            "check the worker's pip install logs for failures."
        )

    _log.info("dbt deps (worker): running in %s", project_dir)
    rc = subprocess.call(
        [sys.executable, "-m", "dbt.cli.main", "deps", "--project-dir", str(project_dir)],
        cwd=str(project_dir),
    )
    if rc != 0:
        raise RuntimeError(f"dbt deps (worker) exited with code {rc}")
    _log.info("dbt deps (worker): done")


def _fetch_archive(s3_uri: str, dest_dir: Path) -> Path:
    """Download ``s3://bucket/key.tar.gz`` and extract under
    ``dest_dir``. Returns the extracted directory."""
    bucket, key = _split_s3_uri(s3_uri)
    dest_dir.mkdir(parents=True, exist_ok=True)
    _log.info("archive: s3://%s/%s -> %s", bucket, key, dest_dir)

    s3 = _s3_client()
    fd, tmp_name = tempfile.mkstemp(prefix="archive-", suffix=".tar.gz", dir=str(dest_dir.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        s3.download_file(bucket, key, str(tmp_path))
        _safe_extract(tmp_path, dest_dir)
    finally:
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()

    _log.info(
        "archive: extracted %d file(s)",
        sum(1 for p in dest_dir.rglob("*") if p.is_file()),
    )
    return dest_dir


def _fetch_state_dir(s3_uri: str, dest_dir: Path) -> Path:
    """Recursively download every object under ``s3_uri`` (an S3
    prefix) into ``dest_dir``. Used for dbt's ``--state`` flag.

    Every downloaded object's local path is validated: the resolved
    destination MUST remain under ``dest_dir``. Keys with ``..``
    components, absolute-looking suffixes, or embedded null bytes are
    rejected. Without this guard, a principal with ``s3:PutObject`` on
    the state prefix could drop a key like
    ``<state-prefix>/../project/models/evil.sql`` and overwrite files
    outside ``dest_dir`` -- most importantly, project files fetched
    earlier by :func:`_fetch_archive`, giving them code execution
    under the Glue/EMR worker role.

    Raises:
        ValueError: if the S3 key would escape ``dest_dir``.
    """
    bucket, prefix = _split_s3_uri(s3_uri)
    if not prefix.endswith("/"):
        prefix += "/"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_resolved = dest_dir.resolve()
    _log.info("state: s3://%s/%s -> %s", bucket, prefix, dest_dir)

    s3 = _s3_client()
    count = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            rel = key[len(prefix) :]
            if not rel or key.endswith("/"):
                continue
            # Reject absolute-looking suffixes + null bytes before we
            # even build a Path. ``Path('/foo').resolve()`` on POSIX
            # would silently escape ``dest_dir``.
            if rel.startswith("/") or "\x00" in rel:
                raise ValueError(
                    f"state: refusing to download S3 key {key!r} "
                    f"(absolute-looking or contains null byte)."
                )
            local = (dest_dir / rel).resolve()
            try:
                local.relative_to(dest_resolved)
            except ValueError as exc:
                raise ValueError(
                    f"state: refusing to download S3 key {key!r} -- "
                    f"resolved path {local} escapes state directory "
                    f"{dest_resolved} (path traversal)."
                ) from exc
            local.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(local))
            count += 1
    _log.info("state: synced %d file(s)", count)
    return dest_dir


def _build_dbt_argv(
    args: argparse.Namespace,
    *,
    project_dir: Path,
    state_dir: Path | None,
) -> list[str]:
    """Translate parsed CLI args into a ``python -m dbt.cli.main`` argv,
    OR into a ``dbt-ol`` invocation when ``--dbt-binary=dbt-ol``.

    ``dbt-ol`` (from the ``openlineage-dbt`` package) is a wrapper: it
    invokes ``dbt`` under the hood and post-processes ``target/`` to
    emit OpenLineage events. It exposes the same CLI surface as ``dbt``
    with a small caveat -- there is no stable ``python -m`` entry
    point in older versions, but ``openlineage_dbt.cli`` has been the
    module name across all releases we care about (>=1.20).
    """
    if getattr(args, "dbt_binary", "dbt") == "dbt-ol":
        # ``dbt-ol`` is a console script (``openlineage.dbt:main``) with
        # no ``python -m`` entry. We wrap it via a tiny inline script
        # written to the per-process work base so the subprocess argv
        # shape becomes:
        #     python <wrapper.py> run --select ...
        # and dbt-ol's ``sys.argv[1:]`` sees ``[run, --select, ...]`` --
        # exactly the shape it expects.
        #
        # use ``tempfile.NamedTemporaryFile`` for a
        # unique-per-invocation filename so concurrent tasks on a
        # shared worker host can't race on the same path. The
        # per-process work dir already isolates most cases ()
        # but the wrapper filename itself is now cryptographically
        # unique too.
        with tempfile.NamedTemporaryFile(
            mode="w",
            prefix="dbt_ol_wrapper_",
            suffix=".py",
            dir=str(project_dir.parent),
            delete=False,
            encoding="utf-8",
        ) as wf:
            wf.write(
                "import sys\n"
                "from openlineage.dbt import main\n"
                "sys.exit(main() or 0)\n"
            )
            wrapper = Path(wf.name)
        launcher: list[str] = [sys.executable, str(wrapper)]
    else:
        launcher = [sys.executable, "-m", "dbt.cli.main"]
    argv: list[str] = [
        *launcher,
        args.command,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
        "--target",
        args.target,
        "--select",
        args.select,
    ]
    if (args.full_refresh or "").lower() == "true":
        argv.append("--full-refresh")
    if args.vars is not None:
        argv.extend(["--vars", args.vars])
    if args.profile_name is not None:
        argv.extend(["--profile", args.profile_name])
    if state_dir is not None:
        argv.extend(["--state", str(state_dir)])
    if (args.defer or "").lower() == "true":
        argv.append("--defer")
    if args.dbt_extra_flags:
        argv.extend(json.loads(args.dbt_extra_flags))
    return argv


def _upload_target_dir(local_target: Path, s3_prefix: str) -> int:
    """Walk ``local_target`` and upload every file to
    ``<s3_prefix>/<relpath>``. Returns the file count uploaded."""
    if not local_target.is_dir():
        _log.warning(
            "target/ upload requested but %s does not exist (dbt may "
            "have failed before writing it). Skipping.",
            local_target,
        )
        return 0

    bucket, prefix = _split_s3_uri(s3_prefix.rstrip("/") + "/")
    s3 = _s3_client()
    count = 0
    for path in local_target.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(local_target).as_posix()
        s3.upload_file(str(path), bucket, f"{prefix}{rel}")
        count += 1
    _log.info("target: uploaded %d file(s) to s3://%s/%s", count, bucket, prefix)
    return count


# ----------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------
# ``_export_aws_creds_to_env`` is a scoped helper for OIDC-only
# credential providers.
# Some backends (EMR Serverless, EKS IRSA) deliver the IAM role via
# ``AWS_WEB_IDENTITY_TOKEN_FILE`` -- duckdb-httpfs doesn't understand
# that flow and fails with HTTP 403 on any S3 op. The helper resolves
# creds through boto3 (which DOES honour OIDC) and exposes them as
# env vars so duckdb-httpfs's ``env`` provider signs S3 requests.
#
# Deliberately narrow: only runs when both conditions hold, so the
# credentials do NOT enter the environment on every worker:
#   1. The worker is on an OIDC-delivered IAM role
#      (``AWS_WEB_IDENTITY_TOKEN_FILE`` set), AND
#   2. AWS_ACCESS_KEY_ID is NOT already set (which it IS on Glue).
# Env stays clean on Glue workers, Lambda, EC2 instance-profile hosts.
def _export_aws_creds_to_env() -> None:
    """Resolve AWS credentials via boto3 and export them as env vars.

    No-op unless the worker is on an OIDC-delivered role
    (``AWS_WEB_IDENTITY_TOKEN_FILE`` present) AND static creds are not
    already in the environment. Fail-open (silently skip) when boto3
    is missing or the credential chain returns nothing.

    Only exports the three variables we set. Never touches values the
    caller supplied.
    """
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        return
    if not os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE"):
        return
    try:
        import boto3  # noqa: PLC0415
    except ImportError:
        return
    try:
        session = boto3.Session()
        creds = session.get_credentials()
        if creds is None:
            return
        frozen = creds.get_frozen_credentials()
    except Exception as exc:  # noqa: BLE001 -- defensive
        _log.info(
            "runner: could not resolve AWS creds via boto3 (%s); leaving env unset",
            exc,
        )
        return
    os.environ["AWS_ACCESS_KEY_ID"] = frozen.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = frozen.secret_key
    if frozen.token:
        os.environ["AWS_SESSION_TOKEN"] = frozen.token
    _log.info(
        "runner: exported OIDC-derived AWS creds as env vars for "
        "duckdb-httpfs (session token: %s)",
        "yes" if frozen.token else "no",
    )


def _disable_dbt_ansi_colors() -> None:
    """Set ``NO_COLOR=1`` so dbt-core emits plain-text stdout.

    Why this exists: Glue Interactive Session's RunStatement parser
    tries to decode statement output as JSON and chokes on ANSI escape
    sequences (``\x1b[0m`` etc.) emitted by dbt-core by default. A
    successful ``dbt run`` then surfaces in Airflow as
    ``com.fasterxml.jackson.core.JsonParseException: Illegal character
    ((CTRL-CHAR, code 27))`` instead of ``success``, even though dbt
    actually finished cleanly.

    NO_COLOR is a cross-tool convention (https://no-color.org) that
    dbt-core (and many other CLIs) respect. We set it unconditionally;
    it's harmless on backends like Glue Spark Job + EMR where output
    goes to plain CloudWatch text streams that handle ANSI fine.
    """
    os.environ.setdefault("NO_COLOR", "1")


def _ensure_writable_home() -> None:
    """Make sure ``$HOME`` is writable so dbt-duckdb's extension cache
    (``$HOME/.duckdb``), dbt's user-config dir, and any third-party tool
    that touches a dotfile in home all work.

    Why this exists: EMR YARN containers ship with a hardcoded
    ``HOME="/home/"`` (no user) and ``/home`` is NOT writable to the
    container user. The first ``conn.install_extension(...)`` call in
    dbt-duckdb tries to ``mkdir $HOME/.duckdb`` and dies with
    ``Permission denied``. Spark's ``spark.yarn.appMasterEnv.HOME`` /
    ``spark.executorEnv.HOME`` configs don't help -- YARN's container
    launcher rewrites HOME back to ``/home/`` AFTER Spark's env-passing.
    The only place we can reliably override is inside the user
    program itself.

    Glue workers (Spark Job, Session) ship HOME=/home/spark or similar
    writable values so this is a no-op there. We leave a writable HOME
    alone and only repoint to a per-process private tempdir when the
    current one fails a write probe.

    SECURITY: the earlier fallback was a hardcoded ``/tmp``
    (world-writable, mode 0o1777). Now we ``mkdtemp`` a private
    (mode 0o700, random-suffix) dir per process and register an
    ``atexit`` cleanup so dbt / duckdb / dbt-adapter dotfile caches
    don't sit under a world-readable path on shared hosts.
    """
    home = os.environ.get("HOME", "")
    if home:
        # Probe with a tempdir, not a tempfile -- duckdb's extension cache
        # needs to ``mkdir $HOME/.duckdb`` then write into it, and some
        # YARN containers allow tempfile-creation in $HOME but not
        # mkdir() (we saw this on EMR-on-EC2 7.5+ where HOME="/home/"
        # the container user can write files to but can't create
        # directories in).
        probe_dir = None
        try:
            probe_dir = tempfile.mkdtemp(dir=home, prefix=".dbt_aws_probe_")
            return
        except (OSError, PermissionError):
            pass
        finally:
            if probe_dir is not None:
                shutil.rmtree(probe_dir, ignore_errors=True)
    # Repoint HOME to a per-process private directory. mkdtemp gives
    # mode 0o700 (owner-only) + a random suffix so we're not leaking
    # dbt / duckdb dotfile caches into a world-readable ``/tmp``.
    private_home = Path(tempfile.mkdtemp(prefix="dbt_aws_home_"))
    atexit.register(shutil.rmtree, private_home, ignore_errors=True)
    os.environ["HOME"] = str(private_home)
    _log.info(
        "runner: HOME was unwritable (%r); repointed to per-process private dir %s",
        home,
        private_home,
    )


def _setup_openlineage(
    *,
    args: argparse.Namespace,
    project_dir: Path,
    work_dir: Path,
) -> Path | None:
    """When ``--ol-namespace`` is set, drop the openlineage.yml + custom
    transport into the project dir and export the OL parent-run env
    vars so ``dbt-ol`` picks them up.

    Returns the local NDJSON path for the file transport (used for the
    post-run S3 upload), or ``None`` when OL is inactive.

    Never raises: any lineage setup failure logs + returns ``None`` so
    dbt still runs.
    """
    if not args.ol_namespace:
        return None
    try:
        from dbt_aws.common.lineage.worker import (
            build_openlineage_env,
            write_openlineage_yml,
        )

        events = work_dir / "ol-events.ndjson"
        write_openlineage_yml(
            project_dir=project_dir,
            namespace=args.ol_namespace,
            ol_events_ndjson=events,
            smus_domain_id=args.ol_smus_domain,
            smus_region=args.ol_smus_region,
        )
        extra_env: dict[str, str] = {}
        if args.ol_extra_env:
            parsed = json.loads(args.ol_extra_env)
            if isinstance(parsed, dict):
                extra_env = {str(k): str(v) for k, v in parsed.items()}
        env = build_openlineage_env(
            parent_run_id=args.ol_parent_run_id or args.stratus_run_id or "unknown",
            parent_job_name=args.ol_parent_job_name or "dbt-aws",
            parent_job_namespace=args.ol_parent_job_namespace or "airflow",
            project_dir=project_dir,
            extra_env=extra_env,
        )
        for k, v in env.items():
            os.environ[k] = v
        _log.info(
            "openlineage: enabled (namespace=%s, s3=%s, smus=%s)",
            args.ol_namespace,
            args.ol_s3_uri or "(off)",
            args.ol_smus_domain or "(off)",
        )
        return events
    except Exception as exc:  # noqa: BLE001 -- lineage setup must not fail dbt
        _log.warning("openlineage: setup failed, running without lineage: %s", exc)
        return None


def main(argv: list[str] | None = None) -> int:
    """Run one dbt invocation against an S3-resident project bundle.

    Returns dbt's exit code unchanged. The entry-point script wraps
    this with ``sys.exit(main())``.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )

    _ensure_writable_home()
    _disable_dbt_ansi_colors()
    _export_aws_creds_to_env()
    args = _parse_args(argv)
    _log.info(
        "runner: starting command=%s select=%s target=%s run_id=%s",
        args.command,
        args.select,
        args.target,
        args.stratus_run_id or "(none)",
    )

    _apply_env_vars(args.env_vars)

    work = _get_work_base() / _safe_dirname(args.stratus_run_id)
    project_dir = _fetch_archive(args.project_archive, work / "project")
    state_dir = _fetch_state_dir(args.state_s3, work / "state") if args.state_s3 else None

    if (args.with_deps or "").lower() == "true":
        _run_dbt_deps_on_worker(project_dir)

    ol_events_path = _setup_openlineage(args=args, project_dir=project_dir, work_dir=work)

    dbt_argv = _build_dbt_argv(args, project_dir=project_dir, state_dir=state_dir)
    _log.info("runner: exec %s", " ".join(_redact_dbt_argv(dbt_argv)))
    sys.stdout.flush()
    try:
        rc = subprocess.call(dbt_argv, cwd=str(project_dir))
    finally:
        # clean up the dbt-ol wrapper if one was written.
        # The launcher path is ``[sys.executable, <wrapper>]`` when
        # ol is active; when it's not the launcher is ``-m dbt.cli.main``
        # and there's nothing to clean. atexit still catches the file
        # eventually via the per-process work-dir tree removal, but
        # explicit cleanup keeps long-lived warm workers tidy.
        if (
            len(dbt_argv) >= 2
            and dbt_argv[0] == sys.executable
            and "dbt_ol_wrapper_" in dbt_argv[1]
        ):
            Path(dbt_argv[1]).unlink(missing_ok=True)
    _log.info("runner: dbt exited with code %d", rc)

    if ol_events_path is not None and args.ol_s3_uri:
        try:
            from dbt_aws.common.lineage.worker import upload_ol_events_to_s3

            upload_ol_events_to_s3(
                ol_events_ndjson=ol_events_path,
                s3_uri=args.ol_s3_uri,
                parent_run_id=args.ol_parent_run_id or args.stratus_run_id or "unknown",
                node_unique_id=args.ol_node_unique_id or args.select,
            )
        except (OSError, ClientError) as exc:  # narrow catch ()
            _log.warning("runner: OL S3 upload failed: %s", exc)

    if args.upload_artefacts_s3:
        try:
            _upload_target_dir(project_dir / "target", args.upload_artefacts_s3)
        except (OSError, ClientError) as exc:  # narrow catch ()
            _log.error("runner: target/ upload failed: %s", exc)

    return rc


# ----------------------------------------------------------------------
# Small utilities
# ----------------------------------------------------------------------
def _split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"expected s3://bucket/key, got {uri!r}")
    bucket = parsed.netloc
    _enforce_s3_allowlist(bucket, uri=uri)
    return bucket, parsed.path.lstrip("/")


def _s3_allowlist_from_env() -> frozenset[str] | None:
    """Parse ``DBT_AWS_ALLOWED_S3_BUCKETS`` into a set of allowed
    bucket names, or return ``None`` when the env var is unset /
    empty.

    Format: comma-separated bucket names. Whitespace stripped;
    duplicates collapsed. Empty entries ignored. When unset, the
    worker imposes NO bucket restriction (byte-identical to
    earlier behaviour) -- opt-in gate.

    Example (in a Glue Job's ``DefaultArguments`` or an env-var
    injected via a MWAA connection):

        DBT_AWS_ALLOWED_S3_BUCKETS=stratus-prod-us-east-1,dbt-aws-mwaa-prod
    """
    raw = os.environ.get("DBT_AWS_ALLOWED_S3_BUCKETS", "").strip()
    if not raw:
        return None
    buckets = frozenset(b.strip() for b in raw.split(",") if b.strip())
    return buckets or None


def _enforce_s3_allowlist(bucket: str, *, uri: str) -> None:
    """Fail-fast when ``DBT_AWS_ALLOWED_S3_BUCKETS`` is set and the
    caller-supplied ``bucket`` isn't on the allowlist.

    Runs at the single ``_split_s3_uri`` choke point so every worker
    S3 read/write goes through the check. No enforcement when the
    env var is unset (opt-in security hardening, ).

    emit a ONE-SHOT WARNING when the env var is unset AND
    the worker is running in a Glue / EMR / Lambda / Airflow
    environment (detected via ``AWS_EXECUTION_ENV``). Local dev boxes
    stay quiet. Warning fires once per process to avoid spamming
    CloudWatch on long-lived Glue Sessions.

    Rejects with a clear message so an operator scanning CloudWatch
    logs immediately sees which bucket tripped the guard.
    """
    allowed = _s3_allowlist_from_env()
    if allowed is None:
        _warn_missing_s3_allowlist_once()
        return
    if bucket not in allowed:
        raise ValueError(
            f"S3 bucket {bucket!r} (from URI {uri!r}) is not on the "
            f"``DBT_AWS_ALLOWED_S3_BUCKETS`` allowlist "
            f"({sorted(allowed)!r}). Set the env var to include this "
            f"bucket or unset it entirely to disable the check."
        )


_s3_allowlist_warning_fired: bool = False


def _warn_missing_s3_allowlist_once() -> None:
    """One-shot WARNING (per process) when running in a managed AWS
    execution environment WITHOUT ``DBT_AWS_ALLOWED_S3_BUCKETS`` set.

    Detection: ``AWS_EXECUTION_ENV`` is present in the environment.
    AWS sets it in Glue / Lambda / EMR / MWAA / SageMaker containers
    (e.g. ``AWS_Glue_PythonShell_1.0``, ``AWS_Lambda_python3.11``).
    On developer laptops it's absent, so the warning stays quiet.

    The warning is defensive: without an allowlist a compromised
    Airflow DAG can point ``--project-archive`` at an attacker-
    controlled bucket and the worker would happily fetch code from
    it. Operators who accept the risk (or manage bucket access via
    IAM policy) can ignore the warning; those who want defence in
    depth set ``DBT_AWS_ALLOWED_S3_BUCKETS`` and eliminate it.
    """
    global _s3_allowlist_warning_fired
    if _s3_allowlist_warning_fired:
        return
    if not os.environ.get("AWS_EXECUTION_ENV"):
        return
    _s3_allowlist_warning_fired = True
    _log.warning(
        "runner: ``DBT_AWS_ALLOWED_S3_BUCKETS`` is unset in a managed "
        "AWS execution environment (AWS_EXECUTION_ENV=%s). Every S3 URI "
        "passed via ``--project-archive`` / ``--state-s3`` / "
        "``--upload-artefacts-s3`` will be accepted. Set the env var to "
        "a comma-separated list of allowed buckets to defence-in-depth "
        "the worker against attacker-influenced S3 URIs.",
        os.environ.get("AWS_EXECUTION_ENV"),
    )


#: dbt CLI flags whose VALUE (next argv element) may carry
#: credentials or otherwise sensitive data. Applied by
#: :func:`_redact_dbt_argv` before ``dbt_argv`` is joined into a log
#: string. ``--vars`` is the canonical case (dbt users routinely pass
#: DB passwords / API tokens in ``--vars '{...}'``); ``--profiles-dir``
#: is included because it can point at a directory containing
#: ``profiles.yml`` with static credentials, and a copy-paste of the
#: exec line would then be a hint at credential location.
_REDACTED_DBT_ARG_KEYS: frozenset[str] = frozenset(
    {
        "--vars",
        "--profiles-dir",
    }
)


def _redact_dbt_argv(argv: list[str]) -> list[str]:
    """Return a copy of ``argv`` with sensitive VALUES replaced by a
    placeholder before it flows into a log line.

    Only the value AFTER each recognised flag is redacted; the flag
    itself stays so the reader can still see which arguments dbt is
    receiving. Attempts to parse ``--vars`` as JSON so the placeholder
    can carry the key count (helpful for debugging). Non-JSON values
    are replaced with a plain ``<redacted>``.

    Does NOT mutate the input list -- returns a shallow copy. Never
    changes what the subprocess actually receives.
    """
    import json as _json

    out = list(argv)
    for i in range(len(out) - 1):
        if out[i] in _REDACTED_DBT_ARG_KEYS:
            value = out[i + 1]
            try:
                parsed = _json.loads(value)
            except (ValueError, TypeError):
                out[i + 1] = "<redacted>"
                continue
            if isinstance(parsed, dict):
                out[i + 1] = f"<redacted:{len(parsed)}-keys>"
            elif isinstance(parsed, list):
                out[i + 1] = f"<redacted:{len(parsed)}-items>"
            else:
                out[i + 1] = "<redacted>"
    return out


def _safe_extract(tar_path: Path, dest: Path) -> None:
    """Extract a tar.gz, refusing path-traversal + symlink-escape members.

    Runs the same rejection logic on ALL Python versions:

    * ``member.name`` is normalised via ``os.path.join(dest, name)`` and
      the resolved path must fall under ``dest.resolve()``. Rejects
      ``..`` traversal on 3.9-3.11 (where ``filter='data'`` is
      unavailable).
    * ``SYMTYPE`` / ``LNKTYPE`` members whose target (``member.linkname``)
      resolves outside ``dest`` are rejected. Prior versions relied on
      the ``target.resolve()`` prefix check, which follows symlinks that
      already exist on disk but doesn't catch a tar member that IS a
      symlink pointing out; the fix explicitly inspects
      ``member.linkname``.

    On Python 3.12+ we ALSO pass ``filter='data'`` (PEP 706's stricter
    checks) as belt-and-braces. Any archive that passes the manual
    validation but trips the PEP 706 filter still fails safely.
    """
    dest_resolved = dest.resolve()
    with tarfile.open(tar_path, mode="r:gz") as tar:
        for member in tar.getmembers():
            # 1. Regular path check -- rejects ``../`` traversal.
            target = (dest / member.name).resolve()
            try:
                target.relative_to(dest_resolved)
            except ValueError as exc:
                raise ValueError(
                    f"archive contains unsafe member (path escapes "
                    f"extraction root): {member.name!r}"
                ) from exc
            # 2. Symlink / hardlink target check -- rejects members
            #    whose ``linkname`` points outside ``dest``. Empty
            #    ``linkname`` = regular file, skip. Relative linknames
            #    are resolved against the member's own parent directory
            #    (POSIX symlink semantics).
            if member.issym() or member.islnk():
                if not member.linkname:
                    continue
                link_target = Path(member.linkname)
                if not link_target.is_absolute():
                    link_target = (dest / member.name).parent / link_target
                link_target = link_target.resolve()
                try:
                    link_target.relative_to(dest_resolved)
                except ValueError as exc:
                    raise ValueError(
                        f"archive contains unsafe {'symlink' if member.issym() else 'hardlink'} "
                        f"member: {member.name!r} -> {member.linkname!r}"
                    ) from exc
        # ``filter='data'`` is Python 3.12+ only (PEP 706). The manual
        # rejection loop above is the primary guard on all versions;
        # on 3.12+ we ALSO enable ``filter='data'`` for defence in
        # depth (rejects setuid bits, absolute paths, device files
        # etc. per PEP 706).
        if sys.version_info >= (3, 12):
            tar.extractall(path=dest, filter="data")
        else:
            tar.extractall(path=dest)


def _safe_dirname(run_id: str) -> str:
    cleaned = "".join(c if c.isalnum() or c in "-_." else "_" for c in (run_id or ""))
    return cleaned or "run"


def _s3_client():  # noqa: ANN201 — boto3 client has no clean public type
    """Lazy-import boto3."""
    import boto3

    return boto3.client("s3")


__all__ = ["main", "run_one_node"]


# ----------------------------------------------------------------------
# In-process invocation (for Glue Interactive Session)
# ----------------------------------------------------------------------
def run_one_node(
    *,
    command: str,
    select: str,
    target: str,
    project_archive_s3: str,
    full_refresh: bool = False,
    vars_json: str | None = None,
    profile_name: str | None = None,
    state_s3: str | None = None,
    defer: bool = False,
    env_vars_json: str | None = None,
    dbt_extra_flags: list[str] | None = None,
    upload_artefacts_s3: str | None = None,
    run_id: str = "",
    with_deps: bool = True,
    # OpenLineage / SMUS integration. When set, the runtime writes
    # ``openlineage.yml`` + exports parent-facet env vars and runs
    # ``dbt-ol`` in-process via ``openlineage_dbt.cli.main``.
    ol_namespace: str | None = None,
    ol_s3_uri: str | None = None,
    ol_smus_domain: str | None = None,
    ol_smus_region: str | None = None,
    ol_parent_run_id: str | None = None,
    ol_parent_job_name: str | None = None,
    ol_parent_job_namespace: str | None = None,
    ol_node_unique_id: str | None = None,
    ol_extra_env: dict[str, str] | None = None,
) -> int:
    """Run one dbt invocation IN-PROCESS via :class:`dbt.cli.main.dbtRunner`.

    Companion to :func:`main`. Use this from inside a Glue Interactive
    Session statement: subprocess + Spark conflict, so we call dbt's
    Python entry directly so it shares the session's Spark context.

    Returns dbt's exit code. Same env-vars / state-dir / target-upload
    semantics as :func:`main`.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )

    _log.info(
        "runner (in-session): command=%s select=%s target=%s run_id=%s",
        command,
        select,
        target,
        run_id or "(none)",
    )
    _apply_env_vars(env_vars_json)
    _ensure_writable_home()
    _disable_dbt_ansi_colors()
    _export_aws_creds_to_env()

    work = _get_work_base() / _safe_dirname(run_id)
    project_dir = _fetch_archive(project_archive_s3, work / "project")
    state_dir = _fetch_state_dir(state_s3, work / "state") if state_s3 else None

    if with_deps:
        _run_dbt_deps_on_worker(project_dir)

    ol_events_path: Path | None = None
    if ol_namespace:
        try:
            from dbt_aws.common.lineage.worker import (
                build_openlineage_env,
                write_openlineage_yml,
            )

            events = work / "ol-events.ndjson"
            write_openlineage_yml(
                project_dir=project_dir,
                namespace=ol_namespace,
                ol_events_ndjson=events,
                smus_domain_id=ol_smus_domain,
                smus_region=ol_smus_region,
            )
            for k, v in build_openlineage_env(
                parent_run_id=ol_parent_run_id or run_id or "unknown",
                parent_job_name=ol_parent_job_name or "dbt-aws",
                parent_job_namespace=ol_parent_job_namespace or "airflow",
                project_dir=project_dir,
                extra_env=ol_extra_env,
            ).items():
                os.environ[k] = v
            ol_events_path = events
            _log.info(
                "openlineage (in-session): enabled (namespace=%s, s3=%s, smus=%s)",
                ol_namespace,
                ol_s3_uri or "(off)",
                ol_smus_domain or "(off)",
            )
        except Exception as exc:  # noqa: BLE001 -- lineage setup must not fail dbt
            _log.warning(
                "openlineage (in-session): setup failed, running without lineage: %s", exc
            )
            ol_events_path = None

    dbt_argv: list[str] = [
        command,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
        "--target",
        target,
        "--select",
        select,
    ]
    if full_refresh:
        dbt_argv.append("--full-refresh")
    if vars_json is not None:
        dbt_argv.extend(["--vars", vars_json])
    if profile_name is not None:
        dbt_argv.extend(["--profile", profile_name])
    if state_dir is not None:
        dbt_argv.extend(["--state", str(state_dir)])
    if defer:
        dbt_argv.append("--defer")
    if dbt_extra_flags:
        dbt_argv.extend(dbt_extra_flags)

    _log.info("runner (in-session): dbt argv: %s", " ".join(_redact_dbt_argv(dbt_argv)))
    if ol_namespace:
        # dbt-ol uses tqdm which calls ``sys.stdout.fileno()`` -- Glue
        # Interactive Session's Livy REPL replaces stdout with a
        # pseudo-file that raises ``UnsupportedOperation: fileno``.
        # Solution: run dbt-ol as a subprocess with real /dev/null
        # stdout instead of in-process, so tqdm sees real fds.
        #
        # unique-per-invocation wrapper filename via
        # ``NamedTemporaryFile`` so concurrent statements can't race
        # on the same path.
        with tempfile.NamedTemporaryFile(
            mode="w",
            prefix="dbt_ol_wrapper_",
            suffix=".py",
            dir=str(project_dir.parent),
            delete=False,
            encoding="utf-8",
        ) as wf:
            wf.write(
                "import sys\n"
                "from openlineage.dbt import main\n"
                "sys.exit(main() or 0)\n"
            )
            wrapper = Path(wf.name)
        try:
            with open(os.devnull, "w") as _null:
                rc = subprocess.call(
                    [sys.executable, str(wrapper), *dbt_argv],
                    cwd=str(project_dir),
                    stdout=_null,  # dbt-core's structlog spam
                    # stderr stays connected so OL/dbt errors surface
                )
        finally:
            # explicit cleanup so long-lived warm Glue Session
            # workers don't accumulate wrapper files under the
            # per-process work base.
            wrapper.unlink(missing_ok=True)
    else:
        from dbt.cli.main import dbtRunner

        result = dbtRunner().invoke(dbt_argv)
        rc = 0 if result.success else 1
    _log.info("runner (in-session): dbt result rc=%d", rc)

    if ol_events_path is not None and ol_s3_uri:
        try:
            from dbt_aws.common.lineage.worker import upload_ol_events_to_s3

            upload_ol_events_to_s3(
                ol_events_ndjson=ol_events_path,
                s3_uri=ol_s3_uri,
                parent_run_id=ol_parent_run_id or run_id or "unknown",
                node_unique_id=ol_node_unique_id or select,
            )
        except (OSError, ClientError) as exc:  # narrow catch ()
            _log.warning("runner (in-session): OL S3 upload failed: %s", exc)

    if upload_artefacts_s3:
        try:
            _upload_target_dir(project_dir / "target", upload_artefacts_s3)
        except (OSError, ClientError) as exc:  # narrow catch ()
            _log.error("runner (in-session): target/ upload failed: %s", exc)

    return rc
