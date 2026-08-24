"""Runner ABC + a few helpers shared by every concrete runner.

The :class:`Runner` is the contract between the orchestration layer
(:class:`dbt_aws.common.builder.DbtDag`) and concrete backends
(``dbt-aws-spark.GlueSparkRunner``, ``dbt-aws-nonspark.GluePythonShellRunner``,
\u2026). One implementation = one AWS compute target.

Each concrete runner produces ONE deferrable Airflow operator per
filtered dbt node. Cross-cutting orchestration concerns (graph load,
selectors, archive upload, DAG wiring) sit OUTSIDE the runner; the
runner only knows how to materialise a single task.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from dbt_aws.common.graph.node import DbtNode

if TYPE_CHECKING:  # pragma: no cover
    from airflow.models import BaseOperator
    from airflow.sdk import DAG


#: Mapping from a dbt node's ``resource_type`` to the dbt CLI verb that
#: runs it. Matches dbt-core's own dispatch table.
DBT_COMMAND_FOR_RESOURCE_TYPE: dict[str, str] = {
    "model": "run",
    "snapshot": "snapshot",
    "seed": "seed",
    "test": "test",
}


def dbt_command_for(node: DbtNode) -> str:
    """Return the dbt CLI verb that runs ``node``.

    Raises:
        ValueError: if the resource type is not runnable (sources,
            macros, exposures, semantic models, etc.).
    """
    try:
        return DBT_COMMAND_FOR_RESOURCE_TYPE[node.resource_type]
    except KeyError as exc:
        raise ValueError(
            f"node {node.unique_id!r} has resource_type "
            f"{node.resource_type!r} which is not runnable. Filter "
            f"non-runnable nodes via RUNNABLE_RESOURCE_TYPES before "
            f"reaching the runner."
        ) from exc


class Runner(ABC):
    """Contract for a per-node Airflow task factory.

    A Runner takes one filtered :class:`~dbt_aws.common.DbtNode` plus
    the information needed to run it (dbt command, selector string,
    target, project archive location, Airflow run_id) and returns
    exactly one deferrable Airflow operator.

    Runners MUST be cheap to instantiate -- the DAG-file-processor
    re-imports the DAG file on a schedule, which reconstructs every
    Runner on every parse. Any expensive setup (creating AWS resources,
    uploading scripts) belongs in CI / Terraform / a one-time deploy
    step, NOT in ``__init__``.

    Runners SHOULD pass ``deferrable=True`` to whatever Airflow operator
    they wrap. Worker pinning during async waits is what we exist to
    avoid.

    Reusability: when a runner returns non-``None`` from
    :meth:`make_setup_task` / :meth:`make_teardown_task`,
    :class:`DbtDag` wires the setup task upstream of root nodes and
    the teardown task downstream of leaf nodes (with
    ``trigger_rule='all_done'`` so cleanup runs even on failure).
    Non-reusable runners return ``None`` and get the simpler
    1-task-per-node DAG.
    """

    @abstractmethod
    def make_task(
        self,
        *,
        task_id: str,
        node: DbtNode,
        dbt_command: str,
        select: str,
        target: str,
        dag: DAG | None = None,
        project_archive_s3: str,
        run_id_template: str = "{{ run_id }}",
        airflow_kwargs: dict[str, Any] | None = None,
        overrides: dict[str, dict[str, Any]] | None = None,
        tag_name: str | None = None,
    ) -> BaseOperator:
        """Build the Airflow operator for one filtered dbt node.

        Args:
            task_id: Airflow task id. Caller has already escaped any
                characters that are illegal as Airflow task ids.
            node: parsed dbt node.
            dbt_command: dbt CLI verb (``run`` / ``snapshot`` / ``seed``
                / ``test``). Use :func:`dbt_command_for` to derive it.
            select: value to pass to dbt's ``--select``. Typically
                ``node.name`` so the runner executes exactly one node.
            target: dbt target name (``dev`` / ``prod`` / \u2026).
            dag: the :class:`airflow.sdk.DAG` to attach the task to,
                or ``None`` to use the ambient DAG context (typical
                when called from inside :class:`DbtTaskGroup`).
            project_archive_s3: ``s3://bucket/key.tar.gz`` of the
                content-fingerprinted project archive. The remote
                worker downloads and extracts this before invoking dbt.
            run_id_template: Jinja template that resolves to the
                Airflow ``run_id`` at task-execute time. Default is
                ``"{{ run_id }}"`` and is sufficient for almost all
                use cases.
            airflow_kwargs: extra kwargs passed through to the
                underlying Airflow operator (``retries``,
                ``execution_timeout``, ``pool``, ``trigger_rule``, etc.).
            overrides: optional ``{unique_id: {field: value}}`` map of
                per-node overrides forwarded from :class:`DbtDag`.
                The runner resolves these against its ``OVERRIDE_TYPE``
                using :func:`resolve_override`.
            tag_name: optional tag driving this task, set by the builder
                when the task materialises a ``tag_groups`` collapsed
                group. Runners that resolve their resource name via
                :func:`resolve_resource_name` should forward this
                verbatim so the per-tag default template kicks in.

        Returns:
            A configured deferrable Airflow operator. The operator
            MUST NOT be wired to upstream/downstream tasks here \u2014
            :class:`DbtDag` owns wiring.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Reusable-runner hooks
    # ------------------------------------------------------------------
    def make_setup_task(
        self,
        *,
        dag: DAG | None = None,
        airflow_kwargs: dict[str, Any] | None = None,
    ) -> BaseOperator | None:
        """Return an Airflow task that provisions the reusable compute
        target (e.g. ``glue:CreateSession``).

        Default returns ``None`` -- non-reusable runners skip this.
        Concrete reusable runners override.
        """
        return None

    def make_teardown_task(
        self,
        *,
        dag: DAG | None = None,
        airflow_kwargs: dict[str, Any] | None = None,
    ) -> BaseOperator | None:
        """Return an Airflow task that tears down the reusable compute
        target (e.g. ``glue:DeleteSession``).

        Default returns ``None``. Concrete reusable runners override.
        :class:`DbtDag` wires this with ``trigger_rule='all_done'``
        so the cleanup runs even on failures.
        """
        return None
