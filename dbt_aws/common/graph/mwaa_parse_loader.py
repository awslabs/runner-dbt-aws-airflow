"""Build a :class:`DbtGraph` by shelling out to ``dbt parse``.

This mode is intended for environments where shipping a pre-built
manifest is awkward — MWAA being the canonical example: a single
``requirements.txt`` line installs ``dbt-core`` + the adapter, and the
DAG file calls into this loader at parse time.

Trade-offs:

* SLOW. ``dbt parse`` walks every model + Jinja-compiles them. For
  any non-trivial project, this runs on every scheduler heartbeat.
  Use ``mode='manifest'`` instead whenever feasible.
* Requires ``dbt-core`` + the adapter in the same Python environment
  that imports this module.

Production-grade behaviour included here:

* Structured logging via ``logging.getLogger(__name__)`` — start /
  success / failure entries with duration + node count, so MWAA's
  CloudWatch shows what's happening at every DAG parse.
* Pre-flight checks: ``dbt_project.yml`` must exist; if
  ``packages.yml`` exists, ``dbt_packages/`` must exist too (i.e.
  ``dbt deps`` has been run) — otherwise the subprocess would fail
  with a cryptic dbt error.
* Empty-graph warning: if the manifest contains zero runnable nodes,
  emits a ``WARNING`` log line so silently-empty deployments surface.
"""

from __future__ import annotations

import logging
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path

from dbt_aws.common.graph.graph import DbtGraph
from dbt_aws.common.graph.manifest_loader import (
    load_graph_from_manifest_file,
)

_log = logging.getLogger(__name__)


class DbtParseError(RuntimeError):
    """``dbt parse`` exited non-zero, timed out, or produced no manifest."""


# How much subprocess output to surface in error messages. The full
# captured stream is still available via the exception chain.
_STDERR_TAIL_CHARS = 4_000
_STDOUT_TAIL_CHARS = 2_000


def load_graph_via_dbt_parse(
    *,
    project_dir: str | Path,
    profiles_dir: str | Path | None = None,
    target: str | None = None,
    dbt_argv: Sequence[str] | None = None,
    timeout: float | None = 600.0,
) -> DbtGraph:
    """Run ``dbt parse`` against the project and load the resulting
    manifest.

    Args:
        project_dir: directory containing ``dbt_project.yml``.
        profiles_dir: directory containing ``profiles.yml``. If ``None``,
            dbt uses its own defaults (``~/.dbt`` or
            ``DBT_PROFILES_DIR``).
        target: dbt target name (passed via ``--target``). If ``None``,
            dbt uses the project's default.
        dbt_argv: how to invoke dbt. Defaults to
            ``[sys.executable, '-m', 'dbt.cli.main']`` which uses the
            same interpreter that imported this module — typically the
            right thing on MWAA where dbt is installed alongside Airflow.
        timeout: subprocess timeout in seconds. ``None`` = wait forever.

    Raises:
        DbtParseError: if pre-flight checks fail, the subprocess fails
            or times out, or no manifest is produced.
        ManifestParseError: if the produced manifest is malformed.
    """
    project_path = _check_project_layout(project_dir)
    argv = _build_argv(
        project_path=project_path,
        profiles_dir=profiles_dir,
        target=target,
        dbt_argv=dbt_argv,
    )

    _log.info(
        "dbt parse: starting (project_dir=%s, target=%s, timeout=%.0fs)",
        project_path,
        target or "<default>",
        timeout or float("inf"),
    )
    started = time.monotonic()

    try:
        completed = subprocess.run(  # noqa: S603 — argstructed above
            argv,
            cwd=str(project_path),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise DbtParseError(
            f"could not invoke dbt: {exc}. Is dbt-core installed in "
            f"the current Python environment? Tried argv[0]={argv[0]!r}."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        elapsed = time.monotonic() - started
        _log.error("dbt parse: timed out after %.1fs (limit=%.0fs)", elapsed, timeout)
        raise DbtParseError(
            f"dbt parse timed out after {timeout}s (project_dir={project_path})"
        ) from exc

    elapsed = time.monotonic() - started

    if completed.returncode != 0:
        stderr_tail = (completed.stderr or "")[-_STDERR_TAIL_CHARS:]
        stdout_tail = (completed.stdout or "")[-_STDOUT_TAIL_CHARS:]
        _log.error(
            "dbt parse: failed (exit=%d, elapsed=%.1fs)",
            completed.returncode,
            elapsed,
        )
        raise DbtParseError(
            f"dbt parse failed (exit {completed.returncode}, "
            f"elapsed {elapsed:.1f}s).\n"
            f"argv: {_redact_argv_for_error(argv)}\n"
            f"stdout (last {_STDOUT_TAIL_CHARS} chars):\n{stdout_tail}\n"
            f"stderr (last {_STDERR_TAIL_CHARS} chars):\n{stderr_tail}"
        )

    manifest_path = project_path / "target" / "manifest.json"
    if not manifest_path.is_file():
        raise DbtParseError(
            f"dbt parse succeeded (exit 0, elapsed {elapsed:.1f}s) but "
            f"produced no manifest at {manifest_path}. Check 'target-path' "
            f"in dbt_project.yml."
        )

    graph = load_graph_from_manifest_file(manifest_path)

    if len(graph) == 0:
        _log.warning(
            "dbt parse: succeeded but produced 0 runnable nodes "
            "(project_dir=%s). Project may be empty or all nodes are "
            "non-runnable types.",
            project_path,
        )
    _log.info(
        "dbt parse: succeeded (elapsed=%.1fs, nodes=%d)",
        elapsed,
        len(graph),
    )
    return graph


# ----------------------------------------------------------------------
# Pre-flight checks
# ----------------------------------------------------------------------
def _check_project_layout(project_dir: str | Path) -> Path:
    """Validate the project directory before spending any subprocess time.

    Raises:
        DbtParseError: with a clear, actionable message.
    """
    project_path = Path(project_dir)
    if not project_path.is_dir():
        raise DbtParseError(f"project_dir not found: {project_path}")
    if not (project_path / "dbt_project.yml").is_file():
        raise DbtParseError(f"project_dir does not contain dbt_project.yml: {project_path}")

    # ``packages.yml`` (or ``dependencies.yml``) present but
    # ``dbt_packages/`` missing → user forgot to run ``dbt deps``. The
    # subprocess error in that case is cryptic; surface it upfront.
    has_packages_file = (project_path / "packages.yml").is_file() or (
        project_path / "dependencies.yml"
    ).is_file()
    has_packages_dir = (project_path / "dbt_packages").is_dir()
    if has_packages_file and not has_packages_dir:
        raise DbtParseError(
            f"project at {project_path} declares packages "
            f"(packages.yml / dependencies.yml) but dbt_packages/ is "
            f"missing. Run 'dbt deps --project-dir {project_path}' before "
            f"calling load_graph_via_dbt_parse()."
        )
    return project_path


def _build_argv(
    *,
    project_path: Path,
    profiles_dir: str | Path | None,
    target: str | None,
    dbt_argv: Sequence[str] | None,
) -> list[str]:
    """Assemble the argv for the dbt subprocess."""
    argv: list[str] = list(dbt_argv) if dbt_argv else [sys.executable, "-m", "dbt.cli.main"]
    argv.extend(["parse", "--project-dir", str(project_path)])
    if profiles_dir is not None:
        argv.extend(["--profiles-dir", str(Path(profiles_dir))])
    if target is not None:
        argv.extend(["--target", target])
    return argv


def _redact_argv_for_error(argv: Sequence[str]) -> list[str]:
    """Return a copy of ``argv`` with sensitive values redacted.

    Called only from the ``DbtParseError`` construction site so a
    caller-supplied ``dbt_argv`` containing ``--vars '{...}'`` or
    ``--profiles-dir /path/with/secrets`` never lands in an
    exception message (which propagates to Airflow scheduler logs,
    CloudWatch, and any error-tracking sink).

    Mirror of ``dbt_aws.common.runtime._redact_dbt_argv`` -- kept
    separate so this module has zero runtime dependencies on the
    worker runtime.
    """
    import json as _json

    redacted_keys = frozenset({"--vars", "--profiles-dir", "--env-vars"})
    out = list(argv)
    for i in range(len(out) - 1):
        if out[i] in redacted_keys:
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
