"""OpenLineage + SageMaker Unified Studio (SMUS) integration -- OPTIONAL feature.

This subpackage is dormant unless a caller passes ``openlineage=OpenLineageConfig(...)``
to a runner. The base wheel installs without any of the OpenLineage
third-party packages -- users opt in via the ``lineage`` extra::

    pip install 'runner-dbt-aws-airflow[lineage]'

Two lineage stores are supported at once via OpenLineage's
``CompositeTransport``:

1. **S3 archive store (default)** -- worker writes NDJSON events to a
   local file, then uploads to ``s3://<ol_s3_uri>/<parent_run>/<node>.ndjson``.
   Portable, replayable into any OpenLineage-compatible tool later.
2. **SMUS ingest (optional)** -- worker calls
   ``datazone.post_lineage_event(domainIdentifier=..., event=...)`` per
   event so SMUS shows a live lineage graph.

Multiple runners in one DAG share a single OL parent run id (derived
from the Airflow ``run_id``) so SMUS collapses their child runs into
one visible graph per Airflow DAG execution.

Import safety:

* :class:`OpenLineageConfig` is pure-stdlib -- no OpenLineage import.
* The concrete transports in ``lineage.transport`` import boto3 and
  ``openlineage.*`` lazily inside their emit methods so
  ``import dbt_aws.common.lineage`` succeeds even when the extra is
  not installed. The runner surfaces a clear ``ImportError`` at
  construction time only when ``openlineage=`` is passed without the
  extra installed.
"""

from dbt_aws.common.lineage.config import OpenLineageConfig, OpenLineageStore

#: Default pip specs for the worker-side lineage stack. Users spread
#: these into their ``--additional-python-modules`` string for Glue,
#: their EMR bootstrap script, or their EMR Serverless venv. Keeps
#: version pins in one place so a lineage upgrade needs one edit,
#: not one per DAG.
#:
#: NOTE: pins use ``==`` rather than range specs because AWS Glue's
#: ``--additional-python-modules`` uses comma-splitting BEFORE pip
#: sees the requirement string. ``openlineage-python>=1.20,<2`` reads
#: as two half-invalid requirements (``openlineage-python>=1.20`` +
#: ``<2``) and pip explodes with ``Invalid requirement: '<2'``.
OPENLINEAGE_WORKER_PIP_SPECS: tuple[str, ...] = (
    "openlineage-python==1.50.0",
    "openlineage-dbt==1.50.0",
)


def openlineage_pip_specs() -> tuple[str, ...]:
    """Public accessor for :data:`OPENLINEAGE_WORKER_PIP_SPECS`.

    Example -- appending to Glue's ``--additional-python-modules``::

        from dbt_aws.common.lineage import openlineage_pip_specs

        modules = ",".join(["runner-dbt-aws-airflow==<version>", "dbt-core==1.11.11",
                            *openlineage_pip_specs()])
    """
    return OPENLINEAGE_WORKER_PIP_SPECS


def append_lineage_args(
    args: dict[str, str],
    *,
    openlineage: OpenLineageConfig | None,
    region_fallback: str | None,
    node_unique_id: str,
) -> None:
    """Extend a Glue-style ``script_args`` dict with the ``--ol-*``
    flags the worker runtime consumes.

    Shared by every runner that expresses argv as a ``dict``
    (:class:`GlueSparkRunner`, :class:`GluePythonShellRunner`). EMR-
    style list-argv runners have a matching helper below.

    No-op when ``openlineage`` is ``None`` -- users who never opted in
    pay zero cost.
    """
    if openlineage is None:
        return
    args["--dbt-binary"] = "dbt-ol"
    args["--ol-namespace"] = openlineage.namespace
    args["--ol-parent-run-id"] = openlineage.parent_run_id_template
    args["--ol-parent-job-name"] = openlineage.parent_job_name_template
    args["--ol-parent-job-namespace"] = openlineage.parent_job_namespace
    args["--ol-node-unique-id"] = node_unique_id
    if openlineage.s3_uri:
        args["--ol-s3-uri"] = openlineage.s3_uri
    if openlineage.smus_domain_id:
        args["--ol-smus-domain"] = openlineage.smus_domain_id
        args["--ol-smus-region"] = openlineage.smus_region or (region_fallback or "")
    if openlineage.extra_env:
        import json as _json

        args["--ol-extra-env"] = _json.dumps(openlineage.extra_env)


def append_lineage_argv_list(
    argv: list[str],
    *,
    openlineage: OpenLineageConfig | None,
    region_fallback: str | None,
    node_unique_id: str,
) -> None:
    """``append_lineage_args`` for list-argv runners (EMR Serverless,
    EMR Cluster Step). Emits ``--flag value`` pairs."""
    if openlineage is None:
        return
    tmp: dict[str, str] = {}
    append_lineage_args(
        tmp,
        openlineage=openlineage,
        region_fallback=region_fallback,
        node_unique_id=node_unique_id,
    )
    for k, v in tmp.items():
        argv.extend([k, v])


def validate_lineage_optin(
    config: OpenLineageConfig | None,
    *,
    region_fallback: str | None,
    runner_class_name: str,
) -> None:
    """Constructor-time validation for any runner that accepts an
    ``openlineage=`` kwarg. Raises:

    * :class:`ImportError` when the caller opted in but hasn't installed
      the ``lineage`` extra (``openlineage-python`` not on sys.path).
    * :class:`ValueError` when SMUS is enabled but there is no region
      to fall back on.
    """
    if config is None:
        return
    try:
        import openlineage.client  # noqa: F401,PLC0415
    except ImportError as exc:
        raise ImportError(
            f"{runner_class_name}(openlineage=...) requires the 'lineage' extra: "
            "pip install 'runner-dbt-aws-airflow[lineage]'."
        ) from exc
    if config.smus_domain_id and not config.smus_region and not region_fallback:
        raise ValueError(
            f"{runner_class_name}: OpenLineageConfig.smus_region is required when "
            "smus_domain_id is set and the runner has no region_name to fall back on."
        )


__all__ = [
    "OPENLINEAGE_WORKER_PIP_SPECS",
    "OpenLineageConfig",
    "OpenLineageStore",
    "append_lineage_args",
    "append_lineage_argv_list",
    "openlineage_pip_specs",
    "validate_lineage_optin",
]
