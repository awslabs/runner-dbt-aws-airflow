"""Glue Spark runner \u2014 one Glue Job run per dbt node.

Two modes:

* ``mode="attach"`` \u2014 the Glue Job is provisioned out-of-band (CFN /
  Terraform / console). The runner only submits ``StartJobRun`` against
  it. This is the recommended pattern for production.

* ``mode="create"`` \u2014 the lib auto-creates the Glue Job on first
  invocation if missing (handled natively by
  ``GlueJobOperator.execute()``). Use when you want the DAG to fully
  own the Glue Job lifecycle. Pass ``iam_role_name`` and
  ``script_location``; tune the Job spec via ``create_job_kwargs``.

Every node becomes one ``GlueJobOperator(deferrable=True)`` instance.
Worker slots are freed during the wait; the Triggerer polls
asynchronously via ``aiobotocore``.

Runner CLI contract (what the operator's ``script_args`` deliver to
the Glue script):

* ``--command``  dbt verb (``run``/``snapshot``/``seed``/``test``)
* ``--select``   dbt selector for this single node (typically ``node.name``)
* ``--target``   dbt target (``dev``/``prod``/\u2026)
* ``--project-archive``  ``s3://...tar.gz`` of the dbt project bundle
* ``--stratus-run-id``   Airflow run_id for log correlation
* ``--full-refresh``     only when the runner's ``full_refresh=True``
* ``--vars``             only when ``vars_json`` is provided
* ``--upload-artefacts-s3``  per-node sub-prefix when configured

These match the contract documented in the runner ABC; concrete runner
scripts (uploaded to the Glue Job's ``script_location``) consume these
flags via ``argparse``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from dbt_aws.common.airflow_extras.glue_concurrent import (
    ConcurrentRunsMode,
    build_concurrent_runs_operator_class,
)
from dbt_aws.common.lineage import (
    OpenLineageConfig,
    append_lineage_args,
    validate_lineage_optin,
)
from dbt_aws.common.runner import (
    Runner,
    RunnerOverride,
    effective,
    resolve_override,
    resolve_resource_name,
    validate_resource_tags,
)
from dbt_aws.common.runner.tags import merge_resource_tags

if TYPE_CHECKING:  # pragma: no cover
    from airflow.models import BaseOperator
    from airflow.sdk import DAG

    from dbt_aws.common.graph.node import DbtNode


GlueSparkMode = Literal["attach", "create"]


@dataclass(frozen=True)
class GlueSparkOverride(RunnerOverride):
    """Per-model overrides for :class:`GlueSparkRunner`.

    All fields optional; ``None`` means "use the runner default".

    Resource identity / lifecycle overrides let one model use a
    different Glue Job (e.g. an IaC-managed one) while the rest of the
    DAG uses the runner's default Job.
    """

    # Resource identity / lifecycle
    job_name: str | None = None
    job_name_template: str | None = None
    #: Per-node override for the ``{prefix}`` token used by
    #: :func:`resolve_resource_name`. Rarely needed -- typically the
    #: runner-level ``name_prefix`` is enough. Useful when one model
    #: belongs to a different cost centre / team than the rest of the
    #: DAG and its Glue Job should reflect that.
    name_prefix: str | None = None
    mode: Literal["attach", "create"] | None = None
    iam_role_name: str | None = None
    script_location: str | None = None

    # Compute sizing
    worker_type: str | None = None
    number_of_workers: int | None = None
    timeout_minutes: int | None = None

    # dbt-side knobs
    full_refresh: bool | None = None
    vars_json: str | None = None
    #: Override the dbt ``--profile`` flag for this single node.
    #: Precedence: ``override.profile_name`` > ``runner.profile_name``.
    profile_name: str | None = None
    #: Override the dbt ``--target`` flag for this single node.
    #: Precedence (resolved in :mod:`dbt_aws.common.builder`):
    #: ``override.target`` > ``meta.stratus.target`` > ``tag_targets``
    #: > ``runner.target`` > DAG-level ``target``.
    target: str | None = None
    #: Override the dbt CLI verb for this single node. Defaults to
    #: :func:`dbt_command_for` (model -> run, snapshot -> snapshot,
    #: seed -> seed, test -> test). Set to ``"build"`` to run dbt's
    #: combined build (run + test) for this node, or any other dbt
    #: verb you want. Validated as a non-empty string -- the runner
    #: forwards it verbatim to the worker entry point.
    command: str | None = None

    # Concurrent-runs policy (allow / join / queue). Rarely overridden
    # per-model but supported for completeness.
    concurrent_runs: ConcurrentRunsMode | None = None

    #: AWS resource tags to layer ON TOP of the runner-level
    #: ``resource_tags``. Shallow-merged (later layers win per key),
    #: so a per-model override adds/overrides individual tags without
    #: nuking the runner-wide baseline. Only takes effect under
    #: ``mode='create'`` -- ``mode='attach'`` Jobs keep their
    #: IaC-managed tags.
    resource_tags: dict[str, str] | None = None

    #: Per-model Spark configuration overrides (``spark.sql.*``,
    #: ``spark.default.parallelism``, etc.). Shallow-merged with the
    #: runner-level ``spark_conf`` -- same layering rule as
    #: ``resource_tags`` -- and materialised into the JobRun's
    #: ``--conf`` argument (Glue StartJobRun-level override). This
    #: overrides the Job's default ``--conf`` per JobRun without
    #: touching the Job definition, so concurrent runs of the same
    #: Job with different ``spark_conf`` values don't race.
    #:
    #: Only DRIVER/EXECUTOR-side runtime configs work here. Configs
    #: that must be set before JVM start (``spark.jars.packages``,
    #: ``spark.hadoop.fs.*``, ``spark.driver.memory`` etc.) MUST live
    #: on the runner's ``create_job_kwargs.DefaultArguments`` -- Glue
    #: silently ignores those when passed at StartJobRun time.
    spark_conf: dict[str, str] | None = None

    #: Escape hatch companion to :attr:`spark_conf`. When set, REPLACES
    #: the merged ``spark_conf`` from all lower layers (runner, tag,
    #: ``meta.stratus``) instead of shallow-merging on top. Use when a
    #: single model needs a completely different Spark config profile
    #: from the runner-wide defaults and enumerating every default key
    #: back to Spark's own default would be tedious.
    #:
    #: Precedence: if ``spark_conf_replace`` is set on ANY layer, the
    #: whole ladder collapses and only that layer's dict is sent to
    #: Glue. If both ``spark_conf`` and ``spark_conf_replace`` are set
    #: on the same layer, ``spark_conf_replace`` wins for that layer.
    #: Same runtime-only limitation as :attr:`spark_conf`.
    spark_conf_replace: dict[str, str] | None = None


class GlueSparkRunner(Runner):
    """Per-node runner that submits a Glue Spark JobRun via
    :class:`airflow.providers.amazon.aws.operators.glue.GlueJobOperator`
    with ``deferrable=True``.

    Per-model overrides are declared via :class:`GlueSparkOverride`.

    Args:
        job_name: name of the AWS Glue Job. In ``attach`` mode the Job
            already exists at this name; in ``create`` mode this is the
            name the operator creates.
        mode: ``"attach"`` (default) or ``"create"``. See module
            docstring.
        iam_role_name: IAM role NAME (not ARN) used by
            ``GlueJobOperator`` when creating / updating the Job.
            Required when ``mode="create"``.
        script_location: ``s3://...`` URL of the runner script Glue
            invokes. Required when ``mode="create"``.
        create_job_kwargs: extra fields forwarded to
            ``GlueJobOperator.create_job_kwargs`` (e.g.
            ``SecurityConfiguration``, ``Connections``,
            ``DefaultArguments``). Merged with the runtime-sizing dict
            this runner builds; caller keys win on conflict.
        update_config: when ``True`` and ``mode="create"``, the operator
            updates an existing Job to match the requested config
            instead of skipping it silently.
        worker_type: Glue Spark worker type (``G.1X``/``G.2X``/``G.4X``).
        number_of_workers: Glue Spark worker count.
        timeout_minutes: per-JobRun timeout (Glue's own kill-switch).
        glue_version: Glue runtime version. Default ``"5.0"``.
        full_refresh: when ``True``, every task passes
            ``--full-refresh`` to dbt.
        vars_json: optional JSON string forwarded as the dbt
            ``--vars`` argument.
        upload_artefacts_s3_prefix: ``s3://...`` prefix. When set, each
            JobRun is told to upload its ``target/`` directory to
            ``<prefix>/<unique_id>/`` after dbt finishes.
        aws_conn_id: Airflow connection id. Default ``aws_default``.
        region_name: AWS region; ``None`` lets the connection decide.
        deferrable: defer to the Triggerer instead of pinning the
            worker. Default ``True`` and should stay that way.
        waiter_delay: seconds between poll attempts in the Triggerer.
        waiter_max_attempts: max polls before timing out.
        stop_job_run_on_kill: when the Airflow task is killed,
            cancel the Glue JobRun. Default ``True``.
        verbose: stream Glue CloudWatch logs into the Airflow task log.

    Raises:
        ValueError: on invalid mode or missing required args for the
            selected mode.
    """

    #: Type of per-model override accepted by this runner.
    OVERRIDE_TYPE: ClassVar[type[RunnerOverride]] = GlueSparkOverride

    #: Short identifier used in default resource names (``{runner_kind}``
    #: token in ``job_name_template``).
    RUNNER_KIND: ClassVar[str] = "spark"

    def __init__(
        self,
        *,
        job_name: str | None = None,
        mode: GlueSparkMode = "attach",
        # Resource naming (used when job_name is not given):
        name_prefix: str | None = None,
        job_name_template: str | None = None,
        # mode="create" only:
        iam_role_name: str | None = None,
        script_location: str | None = None,
        # Auto-upload of the entry script (mode='create' only).
        # Pass this instead of ``script_location`` to have the lib
        # upload the bundled entry script to
        # ``s3://{deploy_bucket}/{deploy_prefix}/worker_entrypoint.py``
        # on DAG-build (HEAD+ETag-compared, idempotent).
        deploy_bucket: str | None = None,
        deploy_prefix: str = "dbt-aws",
        create_job_kwargs: dict[str, Any] | None = None,
        update_config: bool = False,
        # Runtime sizing:
        worker_type: str = "G.1X",
        number_of_workers: int = 2,
        timeout_minutes: int = 60,
        glue_version: str = "5.0",
        # dbt-side knobs (DAG-level for v1; per-node overrides come later):
        full_refresh: bool = False,
        vars_json: str | None = None,
        upload_artefacts_s3_prefix: str | None = None,
        # State / defer (state:modified+ and dbt --defer):
        state_s3: str | None = None,
        defer: bool = False,
        # Profile selection + environment:
        profile_name: str | None = None,
        target: str | None = None,
        env_vars_json: str | None = None,
        # Escape hatch — raw dbt flags forwarded verbatim. JSON-encoded
        # into a single arg because GlueJobOperator's ``script_args``
        # is a dict (can't carry repeated keys).
        dbt_extra_flags: list[str] | None = None,
        # Operator behaviour:
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        deferrable: bool = True,
        with_deps: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 120,
        stop_job_run_on_kill: bool = True,
        verbose: bool = False,
        # Concurrent-runs policy across DAGs that hit the same Glue Job.
        # See ``dbt_aws.common.airflow_extras.glue_concurrent``.
        concurrent_runs: ConcurrentRunsMode = "allow",
        # AWS resource tags applied to the Glue Job at ``create_job``
        # time (mode='create' only; ignored under mode='attach' where
        # the Job pre-exists and IaC owns its tags). See
        # ``dbt_aws.common.runner.tags`` for the validation rules.
        resource_tags: dict[str, str] | None = None,
        # Runner-level Spark configuration. Applied as the default
        # ``--conf`` argument on the Glue Job (via
        # ``create_job_kwargs.DefaultArguments`` under ``mode='create'``).
        # Per-model / per-tag entries under ``overrides.spark_conf``
        # shallow-merge on top and materialise into the StartJobRun
        # ``--conf`` argument for that single run only. See
        # :class:`GlueSparkOverride.spark_conf` for the limitations
        # (runtime configs only; JVM-start configs must live in
        # ``create_job_kwargs``).
        spark_conf: dict[str, str] | None = None,
        # Replace-mode counterpart to ``spark_conf``. If set at any
        # layer, REPLACES the merged spark_conf from lower layers
        # (runner defaults, tags, meta.stratus, ...). Escape hatch for
        # models that need a completely different profile.
        spark_conf_replace: dict[str, str] | None = None,
        # OpenLineage / SMUS integration. When set, the worker runs
        # ``dbt-ol`` (instead of ``dbt``) and emits events to the
        # configured store(s). ``None`` = feature off, byte-identical
        # behaviour to earlier versions.
        openlineage: OpenLineageConfig | None = None,
        # Per-task callbacks. The lib unconditionally appends an audit
        # callback (writes to Airflow's audit Log table) unless
        # ``audit_log=False``. User callbacks always fire first.
        on_execute_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        on_success_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        on_failure_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        audit_log: bool = True,
    ) -> None:
        if mode not in ("attach", "create"):
            raise ValueError(f"mode must be 'attach' or 'create', got {mode!r}")
        if mode == "create":
            if iam_role_name is None:
                raise ValueError(
                    "mode='create' requires iam_role_name=; pass it via "
                    "the GlueSparkRunner constructor."
                )
            if (script_location is None) == (deploy_bucket is None):
                raise ValueError(
                    "mode='create' requires EXACTLY ONE of "
                    "script_location= (use a pre-uploaded URI) or "
                    "deploy_bucket= (lib auto-uploads the bundled "
                    "entry script)."
                )
        else:
            # ``attach`` mode forbids the create-time args so the
            # caller's intent is unambiguous.
            for name, value in (
                ("iam_role_name", iam_role_name),
                ("script_location", script_location),
                ("deploy_bucket", deploy_bucket),
                ("create_job_kwargs", create_job_kwargs),
            ):
                if value is not None:
                    raise ValueError(
                        f"mode='attach' does not use {name}=; the Glue "
                        f"Job is expected to exist already. Got "
                        f"{value!r}."
                    )

        self.job_name = job_name
        self.name_prefix = name_prefix
        self.job_name_template = job_name_template
        self.mode = mode
        self.iam_role_name = iam_role_name
        self.script_location = script_location
        self.deploy_bucket = deploy_bucket
        self.deploy_prefix = deploy_prefix
        self._resolved_script_location: str | None = script_location
        self.create_job_kwargs = dict(create_job_kwargs or {})
        self.update_config = update_config

        # AWS resource tags -- validated at DAG-parse; folded into
        # create_job_kwargs["Tags"] in ``_build_create_job_kwargs``.
        validate_resource_tags(resource_tags, where="GlueSparkRunner.resource_tags")
        self.resource_tags: dict[str, str] | None = dict(resource_tags) if resource_tags else None

        # Spark conf -- validated at DAG-parse; folded into
        # ``DefaultArguments['--conf']`` (runner-level default) and
        # ``run_job_kwargs['Arguments']['--conf']`` (per-JobRun override,
        # populated per-model in ``make_task``).
        _validate_spark_conf(spark_conf, where="GlueSparkRunner.spark_conf")
        self.spark_conf: dict[str, str] | None = dict(spark_conf) if spark_conf else None

        # Spark conf replace-mode. Same validation, same materialisation
        # point; the merger consults this AFTER building the merged
        # spark_conf and replaces the whole dict when set.
        _validate_spark_conf(spark_conf_replace, where="GlueSparkRunner.spark_conf_replace")
        self.spark_conf_replace: dict[str, str] | None = (
            dict(spark_conf_replace) if spark_conf_replace else None
        )

        self.worker_type = worker_type
        self.number_of_workers = number_of_workers
        self.timeout_minutes = timeout_minutes
        self.glue_version = glue_version

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
                "defer=True requires state_s3= (dbt --defer needs a state "
                "manifest to compare against)."
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

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.deferrable = deferrable
        self.with_deps = with_deps
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.stop_job_run_on_kill = stop_job_run_on_kill
        self.verbose = verbose
        if concurrent_runs not in ("allow", "join", "queue"):
            raise ValueError(
                f"concurrent_runs must be 'allow', 'join', or 'queue', got {concurrent_runs!r}"
            )
        self.concurrent_runs: ConcurrentRunsMode = concurrent_runs
        validate_lineage_optin(
            openlineage, region_fallback=region_name, runner_class_name="GlueSparkRunner"
        )
        self.openlineage = openlineage
        self.on_execute_callback = on_execute_callback
        self.on_success_callback = on_success_callback
        self.on_failure_callback = on_failure_callback
        self.audit_log = audit_log

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
        ov = resolve_override(
            node=node,
            override_class=GlueSparkOverride,
            explicit_overrides=overrides,
        )
        assert isinstance(ov, GlueSparkOverride)  # narrow for type checker

        # Per-node command override (e.g. ``"build"`` to run dbt's
        # combined build for this single node). Defaults to the
        # command passed in (which is :func:`dbt_command_for(node)`
        # by default).
        if ov.command:
            dbt_command = ov.command

        # Choose the operator class based on concurrent_runs policy.
        # Falls back to plain GlueJobOperator for mode='allow' so the
        # factory call is uniform.
        eff_concurrent = effective(self, ov, "concurrent_runs", default="allow")
        operator_cls = build_concurrent_runs_operator_class(eff_concurrent)

        # Resolve effective values: override beats runner default.
        eff_name_prefix = ov.name_prefix if ov.name_prefix is not None else self.name_prefix
        eff_job_name = resolve_resource_name(
            explicit=ov.job_name or self.job_name,
            template=ov.job_name_template or self.job_name_template,
            name_prefix=eff_name_prefix,
            dag_id=dag.dag_id if dag is not None else "",
            runner_kind=self.RUNNER_KIND,
            node=node,
            tag_name=tag_name,
        )
        eff_mode = effective(self, ov, "mode")
        eff_iam_role_name = effective(self, ov, "iam_role_name")
        if eff_mode == "create" and self.script_location is None and self.deploy_bucket is not None:
            # Lazy auto-upload of the bundled entry script. Idempotent
            # via S3 HEAD + ETag compare, so it's cheap even though
            # ``make_task`` is called per-node.
            self._resolve_script_location()
        eff_script_location = effective(self, ov, "script_location")

        script_args = self._build_script_args(
            node=node,
            dbt_command=dbt_command,
            select=select,
            target=target,
            project_archive_s3=project_archive_s3,
            run_id_template=run_id_template,
            override=ov,
        )

        kwargs: dict[str, Any] = {
            "task_id": task_id,
            "dag": dag,
            "job_name": eff_job_name,
            "aws_conn_id": self.aws_conn_id,
            "region_name": self.region_name,
            "script_args": script_args,
            "deferrable": self.deferrable,
            "waiter_delay": self.waiter_delay,
            "waiter_max_attempts": self.waiter_max_attempts,
            "stop_job_run_on_kill": self.stop_job_run_on_kill,
            "verbose": self.verbose,
            "wait_for_completion": True,
        }

        # Per-JobRun sizing overrides via ``run_job_kwargs``. These take
        # precedence over the Job's default WorkerType/NumberOfWorkers
        # for this single StartJobRun call.
        run_job_kwargs = _build_run_job_kwargs(ov)

        # per-model Spark conf. Merge runner-level + override
        # layers (Layer A [meta.stratus] and Layer B [overrides] already
        # merged into the dataclass by ``resolve_override``), then
        # honour the ``spark_conf_replace`` escape hatch. Non-empty
        # result lands in ``script_args['--conf']`` -- which Airflow's
        # ``GlueJobOperator`` forwards to ``glue:StartJobRun`` as
        # ``Arguments['--conf']``. Safe under
        # ``concurrent_runs='allow'`` (each JobRun carries its own
        # Arguments; Job DefaultArguments untouched).
        #
        # We deliberately route through ``script_args`` (not
        # ``run_job_kwargs['Arguments']``). Airflow's Glue hook spreads
        # ``**run_kwargs`` alongside a named ``Arguments=`` kwarg in
        # ``start_job_run`` (see ``initialize_job`` in the amazon
        # provider), so an ``Arguments`` key inside ``run_kwargs``
        # raises ``TypeError: got multiple values for keyword argument
        # 'Arguments'``. ``script_args`` matches every other ``--<flag>``
        # the runner emits and never collides with the hook's explicit
        # ``Arguments=`` kwarg.
        #
        # Glue's StartJobRun ``Arguments`` REPLACE (do not merge with)
        # the Job's ``DefaultArguments`` per key. If the caller declared
        # JVM-start configs in
        # ``create_job_kwargs['DefaultArguments']['--conf']`` (Iceberg
        # extension registrations, catalog impls, Kryo serializer),
        # emitting just the runtime ``spark_conf`` here would nuke the
        # JVM-start baseline for THIS JobRun and every downstream
        # statement would fail (``StorageDescriptor#InputFormat cannot
        # be null`` etc. for Iceberg-backed tables). We fold the caller's
        # baseline ``--conf`` back into the merged dict so the per-JobRun
        # string carries BOTH the JVM-start registrations AND the
        # runtime tunings. ``spark_conf_replace`` intentionally drops the
        # baseline too (documented escape hatch: users who set
        # replace-mode want a totally different profile).
        eff_spark_conf = _resolve_effective_spark_conf(runner=self, override=ov)
        if eff_spark_conf:
            replace_mode = ov.spark_conf_replace is not None or self.spark_conf_replace is not None
            if not replace_mode:
                # Parse the caller-supplied Job-level ``--conf`` string
                # (JVM-start registrations) and merge under the runtime
                # override so per-model tunings win per key but the
                # baseline registrations survive.
                baseline_conf_raw = (
                    self.create_job_kwargs.get("DefaultArguments", {}).get("--conf")
                    if isinstance(self.create_job_kwargs, dict)
                    else None
                )
                if baseline_conf_raw:
                    baseline_dict = _parse_spark_conf_string(baseline_conf_raw)
                    if baseline_dict:
                        eff_spark_conf = {**baseline_dict, **eff_spark_conf}
            script_args["--conf"] = _format_spark_conf(eff_spark_conf)

        if run_job_kwargs:
            kwargs["run_job_kwargs"] = run_job_kwargs

        if eff_mode == "create":
            kwargs["iam_role_name"] = eff_iam_role_name
            kwargs["script_location"] = eff_script_location
            kwargs["update_config"] = self.update_config
            kwargs["create_job_kwargs"] = self._build_create_job_kwargs()

        if airflow_kwargs:
            kwargs.update(airflow_kwargs)

        # Audit + per-model callbacks. User callbacks fire first; audit
        # rows are emitted after so a slow user callback can't drop
        # them. ``audit_log=False`` disables the audit layer entirely.
        self._attach_callbacks(
            kwargs=kwargs,
            node=node,
            override=ov,
            eff_job_name=eff_job_name,
            eff_mode=eff_mode,
            dbt_command=dbt_command,
            select=select,
            target=target,
        )

        return operator_cls(**kwargs)

    # ------------------------------------------------------------------
    # Internal: argv + create-time config builders
    # ------------------------------------------------------------------
    def _build_script_args(
        self,
        *,
        node: DbtNode,
        dbt_command: str,
        select: str,
        target: str,
        project_archive_s3: str,
        run_id_template: str,
        override: GlueSparkOverride,
    ) -> dict[str, str]:
        """Assemble the runner script's argv as a dict for
        ``GlueJobOperator.script_args``. The operator serialises this
        as ``--key value`` pairs on the Spark command line.

        Per-model overrides win over the runner-level defaults for the
        fields they cover (``full_refresh``, ``vars_json``).
        """
        # Effective values: per-node override (Layer A+B merged) beats
        # runner-level default. ``None`` in an override means "keep the
        # runner default".
        effective_full_refresh = (
            override.full_refresh if override.full_refresh is not None else self.full_refresh
        )
        effective_vars_json = (
            override.vars_json if override.vars_json is not None else self.vars_json
        )
        # ``profile_name`` and ``target`` follow the same override -> runner
        # -> caller precedence. ``target`` gets a caller-supplied fallback
        # (the DAG-level value the builder threads through) because the
        # builder resolves tag/meta layers before ``make_task`` is called;
        # by the time we get here, ``target`` is already the effective
        # value from those higher layers.
        effective_profile_name = (
            override.profile_name if override.profile_name is not None else self.profile_name
        )
        effective_target = (
            override.target if override.target is not None else (self.target or target)
        )

        args: dict[str, str] = {
            "--command": dbt_command,
            "--select": select,
            "--target": effective_target,
            "--project-archive": project_archive_s3,
            "--stratus-run-id": run_id_template,
        }
        if effective_full_refresh:
            args["--full-refresh"] = "true"
        if effective_vars_json is not None:
            args["--vars"] = effective_vars_json
        if self.upload_artefacts_s3_prefix:
            prefix = self.upload_artefacts_s3_prefix.rstrip("/")
            args["--upload-artefacts-s3"] = f"{prefix}/{node.unique_id}/"
        # Worker-side dbt deps install: tells the entry script to run
        # ``dbt deps`` into ``/tmp/<run-id>/project/dbt_packages/``
        # before the main dbt invocation. Default ``True`` -- if the
        # archive already ships dbt_packages/ (the Airflow-side helper
        # ran), the worker detects + skips.
        args["--with-deps"] = "true" if self.with_deps else "false"
        if self.state_s3 is not None:
            args["--state-s3"] = self.state_s3
        if self.defer:
            args["--defer"] = "true"
        if effective_profile_name is not None:
            args["--profile-name"] = effective_profile_name
        if self.env_vars_json is not None:
            args["--env-vars"] = self.env_vars_json
        if self.dbt_extra_flags:
            import json as _json

            args["--dbt-extra-flags"] = _json.dumps(self.dbt_extra_flags)
        append_lineage_args(
            args,
            openlineage=self.openlineage,
            region_fallback=self.region_name,
            node_unique_id=node.unique_id,
        )
        return args

    def _attach_callbacks(
        self,
        *,
        kwargs: dict[str, Any],
        node: DbtNode,
        override: GlueSparkOverride,
        eff_job_name: str,
        eff_mode: str,
        dbt_command: str,
        select: str,
        target: str,
    ) -> None:
        """Merge user-supplied callbacks with the log-link audit
        callbacks (when ``audit_log=True``) into the operator ``kwargs``.

        Audit emits two structured blocks to the Airflow task log:
        a pre-execute block with the static Glue Job config + Studio
        URL + script args, and a post-execute block with the JobRun
        URL + CloudWatch deep links. URLs also pushed to XCom under
        stable ``dbt_aws_*`` keys so downstream tasks can consume.

        ``region_name`` is resolved from the runner; if neither the
        runner nor the override sets it, we skip audit (no region =
        no useful URLs). ``audit_log=False`` disables audit entirely.
        """
        from dbt_aws.common.airflow_extras.log_link import (
            make_glue_job_audit_callback,
            merge_callbacks,
        )

        if self.audit_log and self.region_name:
            audit = make_glue_job_audit_callback(
                region=self.region_name,
                job_name=eff_job_name,
                backend_label="AWS Glue Spark Job",
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

        # tag-sync callback. If ``resource_tags`` is set,
        # reconcile the Glue Job's tags to match before the JobRun
        # starts via glue:GetTags + glue:TagResource / UntagResource.
        # ``Tags`` is deliberately NOT in ``create_job_kwargs``
        # because glue:UpdateJob rejects it -- see docstring on
        # ``_build_create_job_kwargs``.
        #
        # shallow-merge runner-level tags with any per-node /
        # per-tag ``resource_tags`` override. Runner tags provide the
        # baseline; overrides add or replace per key.
        eff_resource_tags = merge_resource_tags(self.resource_tags, override.resource_tags)
        if eff_resource_tags:
            validate_resource_tags(
                eff_resource_tags,
                where=f"GlueSparkRunner.resource_tags[{node.unique_id!r}]",
            )
            from dbt_aws.common.runner.tags import make_glue_tag_sync_callback

            tag_cb = make_glue_tag_sync_callback(
                job_name=eff_job_name,
                resource_tags=eff_resource_tags,
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
            )
            existing = kwargs.get("on_execute_callback")
            kwargs["on_execute_callback"] = merge_callbacks(existing, tag_cb)

    # ------------------------------------------------------------------
    def _resolve_script_location(self) -> str:
        """Return the effective ``script_location`` URI, uploading the
        bundled entry script to S3 on first call when the caller used
        ``deploy_bucket=`` instead of ``script_location=``.

        Memoised on ``self.script_location`` so the upload happens at
        most once per process. Idempotent within S3 too -- the
        underlying helper does HEAD + ETag compare before PUT.
        """
        if self.script_location is not None:
            return self.script_location
        if self.deploy_bucket is None:
            raise RuntimeError(
                "_resolve_script_location() called without script_location "
                "or deploy_bucket -- this is a bug in dbt-aws (constructor "
                "should have caught it)."
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

    def _build_create_job_kwargs(self) -> dict[str, Any]:
        """``create_job_kwargs`` for ``mode='create'`` -- baseline
        spec from the constructor, with caller-supplied keys winning
        on conflict (so callers can override e.g. ``Timeout`` if they
        prefer their own value).

        Notably does NOT include ``Tags``. Airflow's ``GlueJobHook``
        forwards ``create_job_kwargs`` verbatim to both
        ``glue:CreateJob`` (accepts ``Tags``) AND ``glue:UpdateJob``
        (does NOT -- ``JobUpdate`` rejects ``Tags`` with a boto3
        ``ParamValidationError``). To keep the same runner spec
        working across both code paths, ``resource_tags`` are
        applied via ``glue:TagResource`` post-create/update at
        task-execute time (see :meth:`_apply_resource_tags`).

        An earlier series shipped ``Tags`` inside
        ``create_job_kwargs``; that broke ``update_config=True`` on
        existing Jobs. Fixed.
        """
        baseline: dict[str, Any] = {
            "GlueVersion": self.glue_version,
            "WorkerType": self.worker_type,
            "NumberOfWorkers": self.number_of_workers,
            "Timeout": self.timeout_minutes,
        }
        # Runner-level ``spark_conf`` -- baked into the Job's default
        # ``--conf`` argument at create/update time. Per-JobRun overrides
        # from ``spark_conf`` on per-model overrides land in
        # ``run_job_kwargs`` in ``make_task`` (see
        # :func:`_resolve_effective_spark_conf`). Callers who set the
        # same ``--conf`` explicitly in ``create_job_kwargs`` win: the
        # ``baseline.update(caller_kwargs)`` below merges caller values
        # on top.
        if self.spark_conf:
            baseline["DefaultArguments"] = {"--conf": _format_spark_conf(self.spark_conf)}
        # Deep-merge caller's ``create_job_kwargs`` on top -- but drop
        # any ``Tags`` the caller supplied there too. Tags go through
        # a separate ``glue:TagResource`` call, not through the
        # Create/Update Job APIs. Log a warning if we're discarding
        # caller-supplied ``Tags`` so users notice.
        caller_kwargs = dict(self.create_job_kwargs)
        caller_tags = caller_kwargs.pop("Tags", None)
        if caller_tags:
            import logging

            _log = logging.getLogger(__name__)
            _log.warning(
                "GlueSparkRunner: ``create_job_kwargs['Tags']`` is not "
                "forwarded to Glue -- glue:UpdateJob rejects ``Tags`` in "
                "``JobUpdate``. Set ``resource_tags`` on the runner "
                "constructor (or the top-level ``resource_tags:`` YAML "
                "key) so tags flow through ``glue:TagResource`` instead. "
                "Dropping keys: %s",
                sorted(caller_tags),
            )
        baseline.update(caller_kwargs)
        return baseline


def _build_run_job_kwargs(override: GlueSparkOverride) -> dict[str, Any]:
    """Per-JobRun overrides via ``GlueJobOperator.run_job_kwargs``.

    These ride on top of the Glue Job's default config for one
    ``StartJobRun`` call only. Only fields that Glue's ``StartJobRun``
    actually accepts as overrides are emitted here -- WorkerType,
    NumberOfWorkers, Timeout.
    """
    kwargs: dict[str, Any] = {}
    if override.worker_type is not None:
        kwargs["WorkerType"] = override.worker_type
    if override.number_of_workers is not None:
        kwargs["NumberOfWorkers"] = override.number_of_workers
    if override.timeout_minutes is not None:
        kwargs["Timeout"] = override.timeout_minutes
    return kwargs


# ----------------------------------------------------------------------
# Spark conf helpers (per-model spark_conf feature)
# ----------------------------------------------------------------------

#: Recognised shape for a Spark configuration key. Deliberately narrow
#: -- Glue's ``--conf`` parser splits on spaces, so any space in a key
#: silently truncates the config. Keys start with a letter, then
#: alphanumerics and ``.-_``.
import re as _re  # noqa: E402 -- co-located with the compiled pattern

_SPARK_CONF_KEY_RE = _re.compile(r"^[a-zA-Z][a-zA-Z0-9._-]*$")


def _validate_spark_conf(conf: Any, *, where: str) -> None:
    """Validate a ``spark_conf`` dict for the Glue ``--conf`` argument.

    Enforced at DAG-parse (fail-hard). Rules:

    * ``None`` accepted (no configs).
    * Must be ``dict[str, str]``.
    * Keys match ``^[a-zA-Z][a-zA-Z0-9._-]*$`` -- Glue splits ``--conf``
      on spaces so any space in a key silently drops the config on the
      worker side. Also rejects keys that already start with
      ``--conf`` (a common footgun -- callers copy-pasting from a
      ``spark-submit`` command).
    * Values must be non-empty strings. Glue's ``--conf`` uses
      ``key=value`` syntax; an empty value produces an ambiguous
      ``key=`` fragment.

    Args:
        conf: candidate dict. ``None`` accepted.
        where: caller-friendly label (e.g. ``"GlueSparkRunner.spark_conf"``
            or ``"overrides[model.p.a].spark_conf"``) prepended to any
            error so users see the offending source.

    Raises:
        ValueError: on any rule violation. Error message names the
            offending key or value.
    """
    if conf is None:
        return
    if not isinstance(conf, dict):
        raise ValueError(f"{where}: must be a dict[str, str], got {type(conf).__name__}")
    for k, v in conf.items():
        if not isinstance(k, str) or not k:
            raise ValueError(f"{where}: spark_conf keys must be non-empty strings, got {k!r}")
        if k.startswith("--conf"):
            raise ValueError(
                f"{where}: spark_conf key {k!r} must not include the"
                f" ``--conf`` prefix; the runner adds it. Use e.g."
                f" {{'spark.sql.shuffle.partitions': '400'}} instead of"
                f" {{'--conf spark.sql.shuffle.partitions': '400'}}."
            )
        if not _SPARK_CONF_KEY_RE.fullmatch(k):
            raise ValueError(
                f"{where}: spark_conf key {k!r} contains characters Glue's"
                f" ``--conf`` parser will trip on. Keys must match"
                f" ``^[a-zA-Z][a-zA-Z0-9._-]*$``."
            )
        if not isinstance(v, str):
            raise ValueError(
                f"{where}: spark_conf value for key {k!r} must be a"
                f" string, got {type(v).__name__}. Cast numeric values"
                f" to strings before passing them."
            )
        if not v:
            raise ValueError(
                f"{where}: spark_conf value for key {k!r} is empty; Glue's"
                f" ``--conf`` uses ``key=value`` syntax so an empty value"
                f" produces an ambiguous ``key=`` fragment."
            )


def _format_spark_conf(conf: dict[str, str]) -> str:
    """Format a ``spark_conf`` dict into the ``--conf`` argument value
    Glue expects.

    Glue Spark's ``--conf`` argument on ``DefaultArguments`` /
    ``run_job_kwargs.Arguments`` takes a space-separated string of
    ``--conf key=value`` fragments (each ``--conf`` verbatim, one per
    key). Example: ``--conf spark.sql.shuffle.partitions=400 --conf
    spark.default.parallelism=200``.

    Emits keys in sorted order so the resulting string is stable across
    runs (helps caching / diffing).
    """
    return " ".join(f"--conf {k}={conf[k]}" for k in sorted(conf))


def _parse_spark_conf_string(raw: str) -> dict[str, str]:
    """Inverse of :func:`_format_spark_conf`.

    Parses the Glue ``--conf`` argument string back into a
    ``{key: value}`` dict so the runner can merge caller-supplied
    ``create_job_kwargs['DefaultArguments']['--conf']`` with a
    per-model ``spark_conf`` override.

    Grammar (best-effort; matches what ``_format_spark_conf`` emits
    AND the raw ``--conf a=b --conf c=d`` strings users hand-write in
    YAML):

    * Split on whitespace after collapsing YAML block-style newlines.
    * Each ``--conf`` token consumes the next token as ``key=value``.
    * Tokens with an ``=`` and no leading ``--conf`` are also treated
      as ``key=value`` (Glue accepts either shape; be liberal).
    * ``key=value`` splits on the FIRST ``=`` only, so values may
      contain ``=`` (e.g. base64 payloads).
    * Empty input -> empty dict.

    Not a full ``spark-submit`` parser: doesn't try to preserve
    non-``--conf`` fragments (``--jars``, ``--py-files``). Users
    who need those must keep them in
    ``create_job_kwargs.DefaultArguments`` under their own key.
    """
    if not raw:
        return {}
    # YAML block scalars often ship with embedded newlines; collapse.
    tokens = raw.replace("\n", " ").split()
    out: dict[str, str] = {}
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if t == "--conf" and i + 1 < len(tokens):
            kv = tokens[i + 1]
            i += 2
        elif "=" in t and not t.startswith("--"):
            kv = t
            i += 1
        else:
            # Non-``--conf`` fragment we don't understand -- skip. We
            # deliberately don't raise here because the caller's
            # ``DefaultArguments['--conf']`` may include jars /
            # py-files / other spark-submit flags we shouldn't fail on.
            i += 1
            continue
        if "=" in kv:
            k, v = kv.split("=", 1)
            if k:
                out[k] = v
    return out


def _resolve_effective_spark_conf(
    *,
    runner: GlueSparkRunner,
    override: GlueSparkOverride,
) -> dict[str, str]:
    """Return the effective per-JobRun ``spark_conf`` for one node.

    Two-mode precedence:

    * **Standard layered merge (Reading A).** Shallow-merge across
      layers, later wins per key:

        1. Runner-level ``runner.spark_conf`` (baseline).
        2. Per-node ``override.spark_conf`` (already carries any
           tag / meta / overrides[uid] merge from ``resolve_override``
           + the builder's tag-fold pass).

    * **Replace mode (escape hatch, Reading B).** If EITHER layer sets
      ``spark_conf_replace``, the layered merge is discarded and the
      replace dict is returned verbatim. Per-node replace wins over
      runner replace. This is the escape hatch for models that need a
      totally different Spark profile from the runner defaults.

    Returns an empty dict when no layer configured Spark conf --
    callers should skip emitting ``--conf`` at all in that case.
    """
    # Replace mode: per-node override.spark_conf_replace beats
    # runner.spark_conf_replace; either one bypasses the layered
    # merge entirely.
    if override.spark_conf_replace is not None:
        return dict(override.spark_conf_replace)
    if runner.spark_conf_replace is not None:
        return dict(runner.spark_conf_replace)

    # Layered merge -- shallow-merge later wins per key.
    merged: dict[str, str] = {}
    if runner.spark_conf:
        merged.update(runner.spark_conf)
    if override.spark_conf:
        merged.update(override.spark_conf)
    return merged
