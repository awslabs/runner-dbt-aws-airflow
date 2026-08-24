"""``OpenLineageConfig`` -- pure-stdlib dataclass declared at DAG-parse
time and forwarded from the runner to the worker over the CLI wire.

Design intent:

* No ``openlineage.*`` imports here. This module is safe to import
  even when the ``lineage`` extra is not installed. The concrete
  transports in :mod:`dbt_aws.common.lineage.transport` do the lazy
  third-party imports.
* Frozen so the runner can hash it and log it deterministically.
* Two stores expressed as ``bool``-ish fields: ``s3_uri`` and
  ``smus_domain_id``. Either / both / neither is valid. Neither means
  "no lineage" -- treat the config as absent.
* Parent-run identity is templated (``{{ run_id }}`` etc) so the
  Airflow ``run_id`` becomes the OL parent run id at task-execute
  time. That's how three Glue runs in one DAG collapse into one
  lineage graph in SMUS.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

#: Which lineage store(s) the worker sends events to. The runner
#: builds a ``CompositeTransport`` covering every entry.
OpenLineageStore = Literal["s3", "smus"]


@dataclass(frozen=True)
class OpenLineageConfig:
    """Declarative lineage config passed to a runner.

    Args:
        namespace: OpenLineage job namespace. Groups jobs from the
            same "project" in the SMUS graph. Default ``"dbt-aws"``.
        s3_uri: ``s3://bucket/prefix/`` for the S3 archive store. When
            set, the worker writes NDJSON events to a local file and
            uploads them here after dbt finishes. ``None`` disables the
            S3 store.
        smus_domain_id: SageMaker Unified Studio (DataZone) domain id
            for the SMUS store. When set, the worker calls
            ``datazone.post_lineage_event(...)`` for each event. ``None``
            disables the SMUS store.
        smus_region: AWS region of the DataZone domain. Required when
            ``smus_domain_id`` is set; falls back to the runner's
            ``region_name`` if that's set and this one isn't.
        parent_run_id_template: Jinja template resolved on the worker
            side to the OpenLineage parent run id. Default
            ``"{{ run_id }}"`` -- ties every child run to one Airflow
            DAG execution. Change only when you want to force a shared
            parent across DAGs.
        parent_job_name_template: Jinja template for the parent job
            name. Default ``"{{ dag.dag_id }}"``.
        parent_job_namespace: OL namespace for the parent job.
            Default ``"airflow"`` -- SMUS shows the Airflow DAG as the
            root node.
        extra_env: Extra environment variables to export on the worker
            before dbt-ol runs. Handy for injecting
            ``OPENLINEAGE_CLIENT_LOGGING=DEBUG`` for local debugging.
    """

    namespace: str = "dbt-aws"
    s3_uri: str | None = None
    smus_domain_id: str | None = None
    smus_region: str | None = None
    parent_run_id_template: str = "{{ run_id }}"
    parent_job_name_template: str = "{{ dag.dag_id }}"
    parent_job_namespace: str = "airflow"
    extra_env: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.s3_uri is not None and not self.s3_uri.startswith("s3://"):
            raise ValueError(
                f"OpenLineageConfig.s3_uri must start with 's3://', got {self.s3_uri!r}"
            )
        if self.smus_domain_id is not None and not self.smus_domain_id.strip():
            raise ValueError("OpenLineageConfig.smus_domain_id must be a non-empty string")
        if self.smus_domain_id is not None and self.smus_region is None:
            # Downgraded from a hard error to a warning-at-runtime: the
            # runner may inherit ``region_name`` and pass it down.
            # We can't check that here (would couple the config to the
            # runner class), so we defer to the runner's validator.
            pass
        if not self.namespace.strip():
            raise ValueError("OpenLineageConfig.namespace must be a non-empty string")

    def is_active(self) -> bool:
        """``True`` when at least one store is enabled."""
        return self.s3_uri is not None or self.smus_domain_id is not None

    def active_stores(self) -> list[OpenLineageStore]:
        """Ordered list of enabled stores. Empty when :meth:`is_active`
        is ``False``."""
        out: list[OpenLineageStore] = []
        if self.s3_uri is not None:
            out.append("s3")
        if self.smus_domain_id is not None:
            out.append("smus")
        return out
