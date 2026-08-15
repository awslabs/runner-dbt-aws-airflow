"""EMR cluster-step runner -- attach to an existing EMR-on-EC2
cluster (``mode='attach'``) OR have the lib create+terminate one for
the DAG run (``mode='create'``). One ``spark-submit`` step per dbt
node in either case.

Two lifecycle modes:

* ``mode='attach'`` (default) -- the EMR cluster is provisioned
  out-of-band (CFN / Terraform / console / persistent cluster). The
  runner only calls ``AddJobFlowSteps`` against it.

* ``mode='create'`` -- the lib creates the cluster at DAG-start via
  :class:`airflow.providers.amazon.aws.operators.emr.EmrCreateJobFlowOperator`
  and terminates at DAG-end via ``EmrTerminateJobFlowOperator`` (or
  per-node terminate when ``reusable=False``). The cluster id is
  pushed to XCom and consumed by per-node tasks.

Reusable knob (only relevant with ``mode='create'``):

* ``reusable=True`` (default) -- ONE cluster shared by every dbt node
  in the run. Setup/teardown wrap the whole runner subgroup.
  ~5-7 min cold-start paid once; cheap per node thereafter.

* ``reusable=False`` -- one cluster per dbt node. Each per-node task
  emits its own (setup, statement, teardown) triplet. Maximum
  isolation; pays the ~5-7 min cluster startup PER NODE. Use only
  when you need strict per-model network/IAM/sizing isolation and
  understand the cost / runtime impact.

Every node becomes one
:class:`airflow.providers.amazon.aws.operators.emr.EmrAddStepsOperator`
with ``deferrable=True``. Worker slots are freed during the wait; the
Triggerer polls asynchronously via aiobotocore.

Runner CLI contract -- identical to ``GlueSparkRunner`` and
``EmrServerlessRunner`` (same ``_worker_entrypoint.py``):

* ``--command``       dbt verb (``run`` / ``snapshot`` / ``seed`` / ``test``)
* ``--select``        dbt selector
* ``--target``        dbt target
* ``--project-archive``  ``s3://...tar.gz`` of the project bundle
* ``--stratus-run-id``   Airflow run_id for log correlation
* ``--full-refresh``     when ``full_refresh=True``
* ``--vars``             when ``vars_json`` is provided
* ``--upload-artefacts-s3``  per-node sub-prefix when configured

The args ride after the entry-script path in the step's
``HadoopJarStep.Args`` list, which is the standard
``command-runner.jar`` form::

    [
        "spark-submit",
        "--deploy-mode", "cluster",
        "--conf", "spark.executor.cores=2",
        ...,
        s3://.../worker_entrypoint.py,
        "--command", "run",
        "--select", "stg_orders",
        ...,
    ]
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from dbt_aws.common.lineage import (
    OpenLineageConfig,
    append_lineage_argv_list,
    validate_lineage_optin,
)
from dbt_aws.common.runner import (
    Runner,
    RunnerOverride,
    effective,
    resolve_override,
    validate_resource_tags,
)

if TYPE_CHECKING:  # pragma: no cover
    from airflow.models import BaseOperator
    from airflow.sdk import DAG

    from dbt_aws.common.graph.node import DbtNode


#: ``ActionOnFailure`` on each EMR step. Default ``CONTINUE`` lets
#: downstream steps still run when an upstream step fails (Airflow's
#: trigger_rule handles the dbt-level dependency anyway).
EmrClusterStepMode = Literal["attach", "create"]

#: Jinja template that resolves to the JobFlow id created by the
#: setup task. Same shape as EMR Serverless.
_CLUSTER_ID_XCOM_TEMPLATE = "{{{{ ti.xcom_pull(task_ids='{setup_task_id}') }}}}"

#: Task ids used by the shared (reusable=True) setup/teardown.
SETUP_TASK_ID = "emr_cluster_step_setup"
TEARDOWN_TASK_ID = "emr_cluster_step_teardown"


EmrStepActionOnFailure = Literal[
    "TERMINATE_JOB_FLOW",
    "TERMINATE_CLUSTER",
    "CANCEL_AND_WAIT",
    "CONTINUE",
]


#: Waiter acceptors that retry ``InvalidRequestException`` (which
#: EMR's ``DescribeCluster`` throws for a few seconds after
#: ``RunJobFlow`` returns, while the freshly-created cluster id
#: propagates through EMR's read path). Without this, the first
#: waiter poll re-raises and the Airflow task fails at t~=2s even
#: though the cluster is being provisioned normally and reaches
#: ``WAITING`` a few minutes later. The stock provider waiter (see
#: ``airflow/providers/amazon/aws/waiters/emr.json`` -
#: ``job_flow_waiting``) only has state-based acceptors and no error
#: acceptors, so any ``DescribeCluster`` exception is fatal.
#:
#: We ship the full acceptor list (original 4 + retry) because
#: ``get_waiter(..., config_overrides={"acceptors": [...]})`` in
#: airflow's AWS base hook uses ``dict.update`` which REPLACES the
#: list rather than merging it.
_JOB_FLOW_WAITING_ACCEPTORS: list[dict[str, str]] = [
    # -- original state-based acceptors (must stay in sync with the
    #    stock ``job_flow_waiting`` waiter in providers-amazon)
    {"matcher": "path", "argument": "Cluster.Status.State",
     "expected": "WAITING", "state": "success"},
    {"matcher": "path", "argument": "Cluster.Status.State",
     "expected": "RUNNING", "state": "success"},
    {"matcher": "path", "argument": "Cluster.Status.State",
     "expected": "TERMINATED", "state": "success"},
    {"matcher": "path", "argument": "Cluster.Status.State",
     "expected": "TERMINATED_WITH_ERRORS", "state": "failure"},
    # -- new error acceptor: eventual-consistency retry.
    #
    # NOTE: Airflow's custom-waiter loader
    # (``AwsGenericHook._apply_parameters_value``) walks EVERY
    # acceptor and unconditionally reads ``a['argument']`` to run
    # a Jinja render pass on it -- even for ``matcher: 'error'``
    # acceptors, which upstream boto3 waiters don't require the
    # field on. Missing the field raises ``KeyError: 'argument'``
    # in the Triggerer and fails the task. We include an empty
    # ``argument`` here so the Jinja render is a no-op.
    {"matcher": "error", "argument": "",
     "expected": "InvalidRequestException", "state": "retry"},
]


#: Waiter config overrides derived once from the acceptor list.
_JOB_FLOW_WAITER_CONFIG_OVERRIDES: dict[str, Any] = {
    "acceptors": _JOB_FLOW_WAITING_ACCEPTORS,
}


# ---------------------------------------------------------------------------
# Module-scoped resilient EMR create classes.
#
# Airflow serialises deferrable triggers by dotted classpath and
# re-imports the class in the Triggerer process. That means the
# trigger class MUST live at module scope: nested (in-function)
# classes get a classpath like
# ``pkg.mod.factory.<locals>.Cls`` which ``importlib.import_module``
# cannot resolve, and the Triggerer fails the task with
# ``TaskDeferralError: Trigger failure`` (``ModuleNotFoundError``).
#
# We keep the ``apache-airflow-providers-amazon`` import inside a
# module-level ``try`` block so importing this module in a Python
# environment without the provider (e.g. unit tests) still works;
# the resilient classes simply won't be built there.
# ---------------------------------------------------------------------------
try:  # pragma: no cover - exercised on real Airflow only
    from airflow.exceptions import AirflowException
    from airflow.providers.amazon.aws.operators.emr import (
        EmrCreateJobFlowOperator as _EmrCreateJobFlowOperator,
    )
    from airflow.providers.amazon.aws.triggers.emr import (
        EmrCreateJobFlowTrigger as _EmrCreateJobFlowTrigger,
    )
except Exception:  # pragma: no cover
    AirflowException = Exception  # type: ignore[assignment,misc]
    _EmrCreateJobFlowOperator = None  # type: ignore[assignment,misc]
    _EmrCreateJobFlowTrigger = None  # type: ignore[assignment,misc]


if _EmrCreateJobFlowTrigger is not None:

    class _HardenedTrigger(_EmrCreateJobFlowTrigger):
        """Deferrable trigger that injects our retry acceptor into
        the ``job_flow_waiting`` waiter config, so the initial
        ``DescribeCluster`` poll can retry through EMR's eventual-
        consistency window instead of blowing up on the first call.

        Defined at module scope so Airflow's classpath-based trigger
        loader in the Triggerer process can re-import it.
        """

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            # AwsBaseWaiterTrigger.__init__ hard-codes
            # ``waiter_config_overrides=None``; set it here so the
            # base ``run()`` forwards it into ``hook.get_waiter(...)``.
            self.waiter_config_overrides = _JOB_FLOW_WAITER_CONFIG_OVERRIDES


if _EmrCreateJobFlowOperator is not None:

    class _ResilientEmrCreateJobFlow(_EmrCreateJobFlowOperator):
        """``EmrCreateJobFlowOperator`` subclass whose
        ``job_flow_waiting`` waiter retries on
        ``InvalidRequestException``.

        Handles both non-deferrable (waiter runs inline on the
        worker) and deferrable (waiter runs in the Triggerer)
        paths.
        """

        def execute(self, context: Any) -> Any:
            # Non-deferrable path: reuse parent's execute() and
            # monkey-patch this instance's hook.get_waiter for one
            # call so the waiter gets our acceptors.
            if not self.deferrable:
                orig_get_waiter = self.hook.get_waiter

                def _patched(name: str, **kw: Any) -> Any:
                    if name == "job_flow_waiting":
                        kw = dict(kw)
                        kw["config_overrides"] = _JOB_FLOW_WAITER_CONFIG_OVERRIDES
                    return orig_get_waiter(name, **kw)

                self.hook.get_waiter = _patched  # type: ignore[method-assign,assignment]
                try:
                    return super().execute(context)
                finally:
                    self.hook.get_waiter = orig_get_waiter  # type: ignore[method-assign]

            # Deferrable path: reproduce the parent's execute() up
            # through the ``self.defer(...)`` call, swapping in our
            # module-scoped hardened trigger. The parent doesn't
            # expose a hook to override the trigger class, so we
            # mirror its body.
            import ast
            from datetime import timedelta

            self.log.info(
                "Creating job flow using aws_conn_id: %s, emr_conn_id: %s",
                self.aws_conn_id,
                self.emr_conn_id,
            )
            if isinstance(self.job_flow_overrides, str):
                jfo = ast.literal_eval(self.job_flow_overrides)
                self.job_flow_overrides = jfo
            else:
                jfo = self.job_flow_overrides
            response = self.hook.create_job_flow(jfo)
            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise AirflowException(f"Job flow creation failed: {response}")
            self._job_flow_id = response["JobFlowId"]
            self.log.info("Job flow with id %s created", self._job_flow_id)
            self.defer(
                trigger=_HardenedTrigger(
                    job_flow_id=self._job_flow_id,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_complete",
                timeout=timedelta(
                    seconds=self.waiter_max_attempts * self.waiter_delay + 60
                ),
            )


def _resilient_create_job_flow_operator() -> type:
    """Return the module-scoped :class:`_ResilientEmrCreateJobFlow`
    class. Errors clearly if the upstream provider is unavailable.
    """
    if _EmrCreateJobFlowOperator is None:  # pragma: no cover
        raise ImportError(
            "apache-airflow-providers-amazon is required to build the "
            "resilient EmrCreateJobFlowOperator."
        )
    return _ResilientEmrCreateJobFlow


def _resilient_terminate_operator() -> type:
    """Return a :class:`EmrTerminateJobFlowOperator` subclass that
    skips its terminate call when the ``job_flow_id`` template
    resolves to ``None`` or the literal string ``"None"``.

    This is the per-node teardown for the ``reusable=False`` setup/
    statement/teardown TaskGroup. Without this guard, when the setup
    task is ``upstream_failed`` (never ran, XCom empty), the teardown
    still fires under ``trigger_rule='all_done'`` and calls
    ``EmrTerminateJobFlowOperator(job_flow_id='None')``, which hits
    ``DescribeCluster: Cluster id 'None' is not valid``.
    """
    from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
    from airflow.sdk.exceptions import AirflowSkipException

    class _ResilientEmrTerminate(EmrTerminateJobFlowOperator):
        def execute(self, context: Any) -> Any:
            # After Jinja rendering, self.job_flow_id is a plain string.
            resolved = self.job_flow_id
            if not resolved or resolved == "None" or resolved.lower() == "none":
                self.log.info(
                    "cluster_id resolved to %r -- setup task must have "
                    "been upstream_failed or skipped. Nothing to "
                    "terminate; skipping teardown.",
                    resolved,
                )
                raise AirflowSkipException("no cluster to terminate (setup didn't push an XCom)")
            return super().execute(context)

    return _ResilientEmrTerminate


@dataclass(frozen=True)
class EmrClusterStepOverride(RunnerOverride):
    """Per-model overrides for :class:`EmrClusterStepRunner`.

    All fields optional; ``None`` means "use the runner default".
    """

    cluster_id: str | None = None
    action_on_failure: EmrStepActionOnFailure | None = None

    #: Per-node override for the ``{prefix}`` token used by
    #: :func:`resolve_resource_name`.
    name_prefix: str | None = None

    #: AWS resource tags to layer ON TOP of the runner-level
    #: ``resource_tags``. Shallow-merged (later layers win per key).
    resource_tags: dict[str, str] | None = None

    # Spark sizing (driver/executor)
    driver_cores: int | None = None
    driver_memory: str | None = None
    executor_cores: int | None = None
    executor_memory: str | None = None
    num_executors: int | None = None
    # dbt-side knobs
    full_refresh: bool | None = None
    vars_json: str | None = None
    #: Override the dbt ``--profile`` flag for this single node.
    profile_name: str | None = None
    #: Override the dbt ``--target`` flag for this single node.
    target: str | None = None
    #: Override the dbt CLI verb for this single node (e.g.
    #: ``"build"``). Forwarded verbatim to the worker entry point.
    command: str | None = None


class EmrClusterStepRunner(Runner):
    """Per-node runner that submits a ``spark-submit`` step to an
    existing EMR-on-EC2 cluster via
    :class:`airflow.providers.amazon.aws.operators.emr.EmrAddStepsOperator`
    with ``deferrable=True``.

    Per-model overrides are declared via :class:`EmrClusterStepOverride`.

    Args:
        cluster_id: the EMR JobFlow ID (``j-XXXXXXXXXXXXX``) of the
            existing cluster. Required.
        script_location: ``s3://...`` URL of the runner entry script.
            Required unless ``deploy_bucket=`` is given.
        deploy_bucket: S3 bucket the lib will upload the bundled entry
            script to (``s3://{bucket}/{prefix}/worker_entrypoint.py``)
            on first DAG build. Idempotent via S3 HEAD + ETag compare.
            Mutually exclusive with ``script_location=``.
        deploy_prefix: S3 key prefix paired with ``deploy_bucket``.
            Default ``"dbt-aws"``.
        deploy_mode: ``"cluster"`` (default, recommended) or
            ``"client"`` for ``spark-submit --deploy-mode``.
        action_on_failure: EMR's behaviour when the step fails. Default
            ``"CONTINUE"`` -- the Airflow trigger_rule handles the
            dbt-level dependency, no need to cascade-kill the cluster.
        driver_cores / driver_memory: Spark driver sizing. Defaults
            ``2`` cores / ``"4g"``.
        executor_cores / executor_memory: Spark executor sizing.
            Defaults ``2`` cores / ``"4g"``.
        num_executors: number of Spark executors. Default ``2``.
        spark_extra_conf: list of additional ``--conf k=v`` strings
            appended to the ``spark-submit`` argv (e.g.
            ``["spark.sql.shuffle.partitions=200"]``).
        pyspark_python: absolute path to the Python interpreter that
            Spark's YARN containers (driver + executors) should use.
            Default ``"/usr/bin/python3.11"`` -- matches the EMR 7.5+
            (Amazon Linux 2023) bootstrap layout where the dbt-aws
            install lives at ``/usr/lib/python3.11/site-packages``.
            Wired into ``spark.yarn.appMasterEnv.PYSPARK_PYTHON`` and
            ``spark.executorEnv.PYSPARK_PYTHON`` so that ``import
            dbt_aws`` resolves on YARN containers. Pass ``None`` to
            skip (Spark falls back to ``/usr/bin/python3`` which on
            EMR 7.5 is Python 3.9 and does NOT have dbt-aws).
        full_refresh / vars_json / upload_artefacts_s3_prefix /
        state_s3 / defer / profile_name / env_vars_json /
        dbt_extra_flags: see :class:`GlueSparkRunner` for semantics.
        step_execution_role_arn: optional IAM role ARN for the step
            (EMR's per-step execution role; cluster's default role is
            used when ``None``).
        aws_conn_id: Airflow connection id. Default ``aws_default``.
        region_name: AWS region; ``None`` lets the connection decide.
        deferrable: defer to the Triggerer. Default ``True``.
        waiter_delay: seconds between poll attempts.
        waiter_max_attempts: max polls before timing out.

    Raises:
        ValueError: when required args are missing.
    """

    OVERRIDE_TYPE: ClassVar[type[RunnerOverride]] = EmrClusterStepOverride
    RUNNER_KIND: ClassVar[str] = "emr-cluster-step"

    def __init__(
        self,
        *,
        # Resource identity:
        cluster_id: str | None = None,
        mode: EmrClusterStepMode = "attach",
        reusable: bool = True,
        # mode='create' only:
        job_flow_overrides: dict[str, Any] | None = None,
        auto_terminate: bool = True,
        # Entry script:
        script_location: str | None = None,
        deploy_bucket: str | None = None,
        deploy_prefix: str = "dbt-aws",
        # spark-submit knobs:
        deploy_mode: Literal["cluster", "client"] = "cluster",
        action_on_failure: EmrStepActionOnFailure = "CONTINUE",
        # Spark sizing (passed as --conf flags on spark-submit):
        driver_cores: int = 2,
        driver_memory: str = "4g",
        executor_cores: int = 2,
        executor_memory: str = "4g",
        num_executors: int = 2,
        spark_extra_conf: list[str] | None = None,
        pyspark_python: str | None = "/usr/bin/python3.11",
        # dbt-side knobs:
        full_refresh: bool = False,
        vars_json: str | None = None,
        upload_artefacts_s3_prefix: str | None = None,
        state_s3: str | None = None,
        defer: bool = False,
        profile_name: str | None = None,
        target: str | None = None,
        env_vars_json: str | None = None,
        dbt_extra_flags: list[str] | None = None,
        # Operator behaviour:
        step_execution_role_arn: str | None = None,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        deferrable: bool = True,
        with_deps: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 120,
        # Per-task callbacks. ``audit_log=False`` disables the audit
        # layer entirely.
        on_execute_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        on_success_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        on_failure_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        audit_log: bool = True,
        # OpenLineage / SMUS integration. ``None`` = feature off.
        openlineage: OpenLineageConfig | None = None,
        # AWS resource tags applied to the EMR cluster (JobFlow) at
        # ``RunJobFlow`` time (mode='create' only; ignored under
        # mode='attach' where the cluster pre-exists). Merged into
        # ``job_flow_overrides['Tags']`` in the RunJobFlow-expected
        # list-of-dicts shape. Caller-supplied ``job_flow_overrides
        # ['Tags']`` (if any) merges on top per-Key. See
        # ``dbt_aws.common.runner.tags`` for validation rules.
        resource_tags: dict[str, str] | None = None,
    ) -> None:
        if mode not in ("attach", "create"):
            raise ValueError(f"mode must be 'attach' or 'create', got {mode!r}")

        if mode == "attach":
            if not cluster_id:
                raise ValueError(
                    "mode='attach' requires cluster_id= (the EMR JobFlow "
                    "ID, e.g. 'j-XXXXXXXXXXXXX')."
                )
            for name, value in (("job_flow_overrides", job_flow_overrides),):
                if value is not None:
                    raise ValueError(
                        f"mode='attach' does not use {name}= (the cluster "
                        f"is expected to exist already). Got {value!r}."
                    )
            if not auto_terminate:
                # auto_terminate only meaningful in create mode where
                # the lib owns the cluster lifecycle.
                raise ValueError(
                    "auto_terminate=False is only valid in mode='create' "
                    "(attach mode never terminates the cluster -- it "
                    "didn't create it)."
                )
        else:  # mode == "create"
            if cluster_id is not None:
                raise ValueError(
                    "mode='create' does not use cluster_id=; the lib "
                    "creates a fresh cluster and passes the ID through "
                    "XCom."
                )
            if not job_flow_overrides:
                raise ValueError(
                    "mode='create' requires job_flow_overrides= (the "
                    "EmrCreateJobFlowOperator config -- ``Instances``, "
                    "``ServiceRole``, ``JobFlowRole``, ``Applications``, "
                    "``ReleaseLabel`` and so on)."
                )

        if not reusable and mode != "create":
            raise ValueError(
                "reusable=False requires mode='create' (per-node cluster "
                "lifecycle means the lib has to own the create+terminate; "
                "attach mode reuses one existing cluster so non-reusable "
                "doesn't apply). Beware: per-node EMR clusters incur the "
                "~5-7 min cold start AND hourly EC2 cost PER NODE -- only "
                "use when strict per-model isolation is required."
            )
        if (script_location is None) == (deploy_bucket is None):
            raise ValueError(
                "EmrClusterStepRunner requires EXACTLY ONE of "
                "script_location= (pre-uploaded URI) or "
                "deploy_bucket= (lib auto-uploads the bundled "
                "entry script). Got "
                f"script_location={script_location!r}, "
                f"deploy_bucket={deploy_bucket!r}."
            )
        if deploy_mode not in ("cluster", "client"):
            raise ValueError(f"deploy_mode must be 'cluster' or 'client', got {deploy_mode!r}")
        if action_on_failure not in (
            "TERMINATE_JOB_FLOW",
            "TERMINATE_CLUSTER",
            "CANCEL_AND_WAIT",
            "CONTINUE",
        ):
            raise ValueError(
                f"action_on_failure must be one of "
                f"TERMINATE_JOB_FLOW/TERMINATE_CLUSTER/"
                f"CANCEL_AND_WAIT/CONTINUE, got {action_on_failure!r}"
            )

        self.cluster_id = cluster_id
        self.mode = mode
        self.reusable = reusable

        # AWS resource tags -- validated at DAG-parse; folded into
        # ``job_flow_overrides['Tags']`` (list-of-dicts shape that
        # RunJobFlow expects). Caller-supplied Tags list merges on top
        # per-Key so the caller can override individual entries without
        # discarding the runner-level ``resource_tags=``.
        validate_resource_tags(
            resource_tags, where="EmrClusterStepRunner.resource_tags"
        )
        self.resource_tags: dict[str, str] | None = (
            dict(resource_tags) if resource_tags else None
        )
        merged_jfo = dict(job_flow_overrides or {})
        if self.resource_tags:
            # Build ``{Key: {Key, Value}}`` starting from resource_tags
            # (lowest precedence) and letting caller Tags win per-Key.
            by_key: dict[str, dict[str, str]] = {
                k: {"Key": k, "Value": v} for k, v in self.resource_tags.items()
            }
            for entry in merged_jfo.get("Tags") or []:
                if not isinstance(entry, dict):
                    continue
                key = entry.get("Key")
                if not isinstance(key, str):
                    continue
                by_key[key] = entry
            merged_jfo["Tags"] = list(by_key.values())
        self.job_flow_overrides = merged_jfo
        self.auto_terminate = auto_terminate
        self.script_location = script_location
        self.deploy_bucket = deploy_bucket
        self.deploy_prefix = deploy_prefix
        self.deploy_mode = deploy_mode
        self.action_on_failure = action_on_failure

        self.driver_cores = driver_cores
        self.driver_memory = driver_memory
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.num_executors = num_executors
        self.spark_extra_conf: list[str] = list(spark_extra_conf or [])
        self.pyspark_python = pyspark_python

        self.full_refresh = full_refresh
        self.vars_json = vars_json
        if upload_artefacts_s3_prefix is not None and not upload_artefacts_s3_prefix.startswith(
            "s3://"
        ):
            raise ValueError(
                f"upload_artefacts_s3_prefix must start with 's3://', "
                f"got {upload_artefacts_s3_prefix!r}"
            )
        self.upload_artefacts_s3_prefix = upload_artefacts_s3_prefix

        if state_s3 is not None and not state_s3.startswith("s3://"):
            raise ValueError(f"state_s3 must start with 's3://', got {state_s3!r}")
        if defer and state_s3 is None:
            raise ValueError(
                "defer=True requires state_s3= (dbt --defer needs a "
                "state manifest to compare against)."
            )
        self.state_s3 = state_s3
        self.defer = defer

        self.profile_name = profile_name
        self.target = target

        if env_vars_json is not None:
            import json as _json

            try:
                parsed = _json.loads(env_vars_json)
            except _json.JSONDecodeError as exc:
                raise ValueError(f"env_vars_json must be valid JSON: {exc}") from exc
            if not isinstance(parsed, dict):
                raise ValueError(
                    f"env_vars_json must decode to a JSON object, got {type(parsed).__name__}"
                )
        self.env_vars_json = env_vars_json
        self.dbt_extra_flags: list[str] = list(dbt_extra_flags or [])

        self.step_execution_role_arn = step_execution_role_arn
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.deferrable = deferrable
        self.with_deps = with_deps
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

        self.on_execute_callback = on_execute_callback
        self.on_success_callback = on_success_callback
        self.on_failure_callback = on_failure_callback
        self.audit_log = audit_log
        validate_lineage_optin(
            openlineage, region_fallback=region_name, runner_class_name="EmrClusterStepRunner"
        )
        self.openlineage = openlineage

        # Resolve the script_location eagerly when deploy_bucket is set
        # so subsequent ``make_task`` calls don't repeat the work. The
        # underlying helper does HEAD + ETag compare before PUT.
        if self.script_location is None and self.deploy_bucket is not None:
            self._resolve_script_location()

    # ------------------------------------------------------------------
    # Runner contract
    # ------------------------------------------------------------------
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
        from airflow.providers.amazon.aws.operators.emr import (
            EmrAddStepsOperator,
        )

        ov = resolve_override(
            node=node,
            override_class=EmrClusterStepOverride,
            explicit_overrides=overrides,
        )
        assert isinstance(ov, EmrClusterStepOverride)
        if ov.command:
            dbt_command = ov.command

        # When reusable=False the per-node setup task creates a fresh
        # cluster and the AddSteps op must pull THAT cluster's id from
        # XCom (not the DAG-level shared setup task's). See
        # ``_effective_cluster_id`` for the resolution rules.
        per_node_setup_task_id = f"{task_id}.setup" if not self.reusable else None
        eff_cluster_id = self._effective_cluster_id(
            ov, setup_task_id_override=per_node_setup_task_id
        )
        eff_action_on_failure = effective(
            self,
            ov,
            "action_on_failure",
            default=self.action_on_failure,
        )

        if self.script_location is None:
            raise RuntimeError(
                "EmrClusterStepRunner.make_task: no entry-point script "
                "URL resolved. This is a bug in dbt-aws (the "
                "constructor should have caught it)."
            )

        step_args = self._build_step_args(
            node=node,
            dbt_command=dbt_command,
            select=select,
            target=target,
            project_archive_s3=project_archive_s3,
            run_id_template=run_id_template,
            override=ov,
        )

        step = {
            "Name": node.unique_id,
            "ActionOnFailure": eff_action_on_failure,
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": step_args,
            },
        }

        kwargs: dict[str, Any] = {
            "task_id": task_id,
            "dag": dag,
            "job_flow_id": eff_cluster_id,
            "steps": [step],
            "aws_conn_id": self.aws_conn_id,
            "region_name": self.region_name,
            "deferrable": self.deferrable,
            "waiter_delay": self.waiter_delay,
            "waiter_max_attempts": self.waiter_max_attempts,
            "wait_for_completion": True,
        }
        if self.step_execution_role_arn is not None:
            kwargs["execution_role_arn"] = self.step_execution_role_arn

        if airflow_kwargs:
            kwargs.update(airflow_kwargs)

        self._attach_callbacks(
            kwargs=kwargs,
            node=node,
            override=ov,
            eff_cluster_id=eff_cluster_id,
            eff_action_on_failure=eff_action_on_failure,
            dbt_command=dbt_command,
            select=select,
            target=target,
        )

        if self.reusable:
            return EmrAddStepsOperator(**kwargs)

        # ------------------------------------------------------------------
        # reusable=False: emit a per-node (setup, statement, teardown)
        # triplet wrapped in a TaskGroup. Each dbt node owns its own
        # short-lived EMR cluster. EXPENSIVE -- ~5-7 min cluster cold
        # start AND hourly EC2 billing PER NODE. Validated in __init__
        # to require mode='create'.
        # ------------------------------------------------------------------
        from dbt_aws.common._airflow_compat import TaskGroup

        _CreateOp = _resilient_create_job_flow_operator()

        # AddSteps op kwargs already reference the per-node setup task
        # for the cluster id XCom pull (see
        # ``per_node_setup_task_id`` threaded through
        # ``_effective_cluster_id`` above). Adjust the AddSteps op
        # task_id so it lives INSIDE the group.
        statement_kwargs = dict(kwargs)
        statement_kwargs["task_id"] = "statement"
        statement_kwargs["dag"] = None  # bound by ambient TaskGroup ctx

        # Per-node cluster name: ``<base>-<run_id>-<node.name>``.
        per_node_jfo = dict(self.job_flow_overrides)
        base_name = per_node_jfo.get("Name", "dbt-aws-perNode")
        per_node_jfo["Name"] = f"{base_name}-{{{{ run_id }}}}-{node.name}"

        with TaskGroup(group_id=task_id) as tg:
            setup = _CreateOp(
                task_id="setup",
                job_flow_overrides=per_node_jfo,
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                deferrable=self.deferrable,
                waiter_delay=self.waiter_delay,
                waiter_max_attempts=self.waiter_max_attempts,
                wait_for_completion=True,
            )
            statement = EmrAddStepsOperator(**statement_kwargs)
            teardown = _resilient_terminate_operator()(
                task_id="teardown",
                job_flow_id=_CLUSTER_ID_XCOM_TEMPLATE.format(
                    setup_task_id=per_node_setup_task_id,
                ),
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                deferrable=self.deferrable,
                waiter_delay=self.waiter_delay,
                waiter_max_attempts=self.waiter_max_attempts,
                # all_done so the cluster is terminated even if the
                # step fails -- prevents leaked EC2 instances burning
                # money. The wrapper class skips the terminate call
                # when the cluster id resolves to None/'None' (which
                # happens when setup was upstream_failed and XCom is
                # empty), avoiding the ``DescribeCluster: Cluster id
                # 'None' is not valid`` error.
                trigger_rule="all_done",
            )
            setup >> statement >> teardown
        return tg  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Reusable-runner hooks: create / terminate the EMR cluster.
    # ------------------------------------------------------------------
    def make_setup_task(
        self,
        *,
        dag: DAG | None = None,
        airflow_kwargs: dict[str, Any] | None = None,
    ) -> BaseOperator | None:
        """Return an ``EmrCreateJobFlowOperator`` when ``mode='create'``
        AND ``reusable=True``. The cluster id is pushed to XCom and
        consumed by per-node ``AddSteps`` tasks.

        Returns ``None`` in ``attach`` mode (cluster exists) OR when
        ``reusable=False`` (per-node tasks own their own setup).
        """
        if self.mode != "create" or not self.reusable:
            return None

        _CreateOp = _resilient_create_job_flow_operator()

        kwargs: dict[str, Any] = {
            "task_id": SETUP_TASK_ID,
            "dag": dag,
            "job_flow_overrides": dict(self.job_flow_overrides),
            "aws_conn_id": self.aws_conn_id,
            "region_name": self.region_name,
            "deferrable": self.deferrable,
            "waiter_delay": self.waiter_delay,
            "waiter_max_attempts": self.waiter_max_attempts,
            "wait_for_completion": True,
        }
        if airflow_kwargs:
            kwargs.update(airflow_kwargs)
        return _CreateOp(**kwargs)

    def make_teardown_task(
        self,
        *,
        dag: DAG | None = None,
        airflow_kwargs: dict[str, Any] | None = None,
    ) -> BaseOperator | None:
        """Return an ``EmrTerminateJobFlowOperator`` when
        ``mode='create'`` AND ``reusable=True`` AND
        ``auto_terminate=True``. Reads the cluster id from XCom on the
        setup task.

        ``DbtDag`` wires this with ``trigger_rule='all_done'`` so the
        cluster is terminated even when one of the per-node steps fails.

        Returns ``None`` in ``attach`` mode, when ``reusable=False``,
        or when ``auto_terminate=False`` (caller takes responsibility
        for terminating the cluster manually).
        """
        if self.mode != "create" or not self.reusable or not self.auto_terminate:
            return None
        from airflow.providers.amazon.aws.operators.emr import (
            EmrTerminateJobFlowOperator,
        )

        kwargs: dict[str, Any] = {
            "task_id": TEARDOWN_TASK_ID,
            "dag": dag,
            "job_flow_id": _CLUSTER_ID_XCOM_TEMPLATE.format(
                setup_task_id=SETUP_TASK_ID,
            ),
            "aws_conn_id": self.aws_conn_id,
            "region_name": self.region_name,
            "deferrable": self.deferrable,
            "waiter_delay": self.waiter_delay,
            "waiter_max_attempts": self.waiter_max_attempts,
            "trigger_rule": "all_done",
        }
        if airflow_kwargs:
            kwargs.update(airflow_kwargs)
        return EmrTerminateJobFlowOperator(**kwargs)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _effective_cluster_id(
        self,
        override: EmrClusterStepOverride,
        *,
        setup_task_id_override: str | None = None,
    ) -> str:
        """Return the cluster_id the per-node AddSteps task should
        target.

        Precedence: per-node override > runner-level literal > XCom
        template from the setup task (``mode='create'`` only). When
        ``setup_task_id_override`` is set (per-node ``reusable=False``
        branch), the XCom pull targets that task instead of the
        DAG-level shared setup task.
        """
        if override.cluster_id is not None:
            return override.cluster_id
        if self.cluster_id is not None:
            return self.cluster_id
        if self.mode == "create":
            setup_task_id = setup_task_id_override or SETUP_TASK_ID
            return _CLUSTER_ID_XCOM_TEMPLATE.format(setup_task_id=setup_task_id)
        raise RuntimeError(
            "EmrClusterStepRunner: no cluster_id resolved. This is a bug in dbt-aws."
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _build_step_args(
        self,
        *,
        node: DbtNode,
        dbt_command: str,
        select: str,
        target: str,
        project_archive_s3: str,
        run_id_template: str,
        override: EmrClusterStepOverride,
    ) -> list[str]:
        """Build the ``HadoopJarStep.Args`` list for ``spark-submit``.

        Shape::

            [
              "spark-submit",
              "--deploy-mode", "<cluster|client>",
              "--conf", "spark.driver.cores=2",
              ...,
              s3://.../worker_entrypoint.py,
              "--command", "run", "--select", "...", ...
            ]
        """
        effective_full_refresh = (
            override.full_refresh if override.full_refresh is not None else self.full_refresh
        )
        effective_vars_json = (
            override.vars_json if override.vars_json is not None else self.vars_json
        )
        # ``profile_name`` and ``target`` follow the same override -> runner
        # -> caller precedence. See notes in glue_job.py / glue_python_shell.py.
        effective_profile_name = (
            override.profile_name if override.profile_name is not None else self.profile_name
        )
        effective_target = (
            override.target if override.target is not None else (self.target or target)
        )
        eff_driver_cores = override.driver_cores or self.driver_cores
        eff_driver_memory = override.driver_memory or self.driver_memory
        eff_executor_cores = override.executor_cores or self.executor_cores
        eff_executor_memory = override.executor_memory or self.executor_memory
        eff_num_executors = override.num_executors or self.num_executors

        args: list[str] = [
            "spark-submit",
            "--deploy-mode",
            self.deploy_mode,
            "--conf",
            f"spark.driver.cores={eff_driver_cores}",
            "--conf",
            f"spark.driver.memory={eff_driver_memory}",
            "--conf",
            f"spark.executor.cores={eff_executor_cores}",
            "--conf",
            f"spark.executor.memory={eff_executor_memory}",
            "--conf",
            f"spark.executor.instances={eff_num_executors}",
        ]
        for conf in self.spark_extra_conf:
            args.extend(["--conf", conf])

        # PySpark interpreter wiring. EMR 7.5+ (AL2023) ships both
        # ``/usr/bin/python3`` (-> 3.9) and ``/usr/bin/python3.11``,
        # but our bootstrap installs dbt-aws into 3.11. Without this,
        # Spark picks up 3.9 and ``import dbt_aws`` fails with
        # ``ModuleNotFoundError`` before the entry script even reaches
        # ``main()``.
        #
        # We set FOUR configs to cover every spark-submit deploy mode:
        # - ``spark.pyspark.python``: executor + driver (canonical, both modes)
        # - ``spark.pyspark.driver.python``: driver in client mode
        # - ``spark.yarn.appMasterEnv.PYSPARK_PYTHON``: AM env in cluster mode
        # - ``spark.executorEnv.PYSPARK_PYTHON``: executor env var (any mode)
        if self.pyspark_python is not None:
            args.extend(
                [
                    "--conf",
                    f"spark.pyspark.python={self.pyspark_python}",
                    "--conf",
                    f"spark.pyspark.driver.python={self.pyspark_python}",
                    "--conf",
                    f"spark.yarn.appMasterEnv.PYSPARK_PYTHON={self.pyspark_python}",
                    "--conf",
                    f"spark.executorEnv.PYSPARK_PYTHON={self.pyspark_python}",
                ]
            )

        # YARN containers on EMR set HOME=/home (no user) by default.
        # duckdb tries to create ``$HOME/.duckdb`` for its extension cache
        # and dies with ``Permission denied`` because /home isn't writable
        # to the YARN user. Override HOME to /tmp on the driver + executors
        # so dbt-duckdb can write its extension cache.
        args.extend(
            [
                "--conf",
                "spark.yarn.appMasterEnv.HOME=/tmp",
                "--conf",
                "spark.executorEnv.HOME=/tmp",
            ]
        )

        # Entry-script URL is the spark-submit positional argument; any
        # ``--key value`` pairs AFTER it are forwarded to the script's
        # argv by Spark.
        args.append(self.script_location or "")

        # dbt-aws contract args (same order as Glue / EMR Serverless).
        args.extend(
            [
                "--command",
                dbt_command,
                "--select",
                select,
                "--target",
                effective_target,
                "--project-archive",
                project_archive_s3,
                "--stratus-run-id",
                run_id_template,
            ]
        )
        if effective_full_refresh:
            args.extend(["--full-refresh", "true"])
        if effective_vars_json is not None:
            args.extend(["--vars", effective_vars_json])
        if self.upload_artefacts_s3_prefix:
            prefix = self.upload_artefacts_s3_prefix.rstrip("/")
            args.extend(["--upload-artefacts-s3", f"{prefix}/{node.unique_id}/"])
        args.extend(["--with-deps", "true" if self.with_deps else "false"])
        if self.state_s3 is not None:
            args.extend(["--state-s3", self.state_s3])
        if self.defer:
            args.extend(["--defer", "true"])
        if effective_profile_name is not None:
            args.extend(["--profile-name", effective_profile_name])
        if self.env_vars_json is not None:
            args.extend(["--env-vars", self.env_vars_json])
        if self.dbt_extra_flags:
            import json as _json

            args.extend(["--dbt-extra-flags", _json.dumps(self.dbt_extra_flags)])
        append_lineage_argv_list(
            args,
            openlineage=self.openlineage,
            region_fallback=self.region_name,
            node_unique_id=node.unique_id,
        )
        return args

    def _resolve_script_location(self) -> str:
        """Lazy-upload the bundled entry script when ``deploy_bucket``
        was given. Memoised on ``self.script_location``.
        """
        if self.script_location is not None:
            return self.script_location
        if self.deploy_bucket is None:
            raise RuntimeError(
                "_resolve_script_location() called without "
                "script_location or deploy_bucket -- this is a bug in "
                "dbt-aws."
            )
        from dbt_aws.common.airflow_extras.auto_deploy import (
            upload_worker_entrypoint,
        )

        self.script_location = upload_worker_entrypoint(
            bucket=self.deploy_bucket,
            prefix=self.deploy_prefix,
            region_name=self.region_name,
        )
        return self.script_location

    def _attach_callbacks(
        self,
        *,
        kwargs: dict[str, Any],
        node: DbtNode,
        override: EmrClusterStepOverride,
        eff_cluster_id: str,
        eff_action_on_failure: str,
        dbt_command: str,
        select: str,
        target: str,
    ) -> None:
        """Merge user-supplied callbacks with the log-link audit
        callbacks into the operator ``kwargs`` dict. See
        :mod:`dbt_aws.common.airflow_extras.log_link`.
        """
        from dbt_aws.common.airflow_extras.log_link import (
            make_emr_cluster_step_audit_callback,
            merge_callbacks,
        )

        if self.audit_log and self.region_name:
            audit = make_emr_cluster_step_audit_callback(
                region=self.region_name,
                cluster_id=eff_cluster_id,
            )
        else:
            audit = {}

        for key in (
            "on_execute_callback",
            "on_success_callback",
            "on_failure_callback",
        ):
            user_cb = getattr(self, key)
            audit_cb = audit.get(key)
            merged = merge_callbacks(user_cb, audit_cb)
            if merged is not None:
                kwargs[key] = merged
