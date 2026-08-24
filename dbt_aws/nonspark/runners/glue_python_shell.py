"""Glue Python Shell runner -- one Glue Python Shell JobRun per dbt node.

Sister to :class:`~dbt_aws.spark.runners.GlueSparkRunner` for
warehouse-bound dbt adapters (dbt-athena / dbt-redshift / dbt-snowflake
/ dbt-bigquery / dbt-postgres / dbt-duckdb / dbt-trino) that hand SQL
off to a remote engine and DON'T need a Spark JVM.

Trade-offs vs the Spark runner:

* 5-15s cold start vs 30-60s
* 0.0625 or 1.0 DPU (minimum 1 DPU billing for Spark)
* ~5-60x cheaper for warehouse-bound dbt models
* Same worker entry script, same ``runtime.main()`` -- only the Glue
  Job definition (``Command.Name='pythonshell'``, ``MaxCapacity``,
  ``GlueVersion='3.0'``, Python 3.9) differs.

Two modes (same as the Spark runner):

* ``mode='attach'`` -- a Python Shell Job is provisioned out of band
  (CFN / Terraform). The runner submits StartJobRun only.
* ``mode='create'`` (default) -- the lib auto-creates on first run if
  missing. Requires ``iam_role_name`` + ``script_location``.

Every node becomes one ``GlueJobOperator(deferrable=True)``.
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


GluePythonShellMode = Literal["attach", "create"]

#: Glue Python Shell only accepts these two DPU sizes.
_VALID_MAX_CAPACITY = (0.0625, 1.0)


@dataclass(frozen=True)
class GluePythonShellOverride(RunnerOverride):
    """Per-model overrides for :class:`GluePythonShellRunner`.

    All fields optional; ``None`` means "use the runner default".
    """

    # Resource identity / lifecycle
    job_name: str | None = None
    job_name_template: str | None = None
    #: Per-node override for the ``{prefix}`` token used by
    #: :func:`resolve_resource_name`.
    name_prefix: str | None = None
    mode: Literal["attach", "create"] | None = None
    iam_role_name: str | None = None
    script_location: str | None = None

    # Compute sizing
    max_capacity: float | None = None
    timeout_minutes: int | None = None

    # dbt-side knobs
    full_refresh: bool | None = None
    vars_json: str | None = None
    #: Override the dbt ``--profile`` flag for this single node.
    #: Precedence: ``override.profile_name`` > ``runner.profile_name``
    #: (see :func:`dbt_aws.common.runner.override.effective`).
    profile_name: str | None = None
    #: Override the dbt ``--target`` flag for this single node.
    #: Precedence (resolved in :mod:`dbt_aws.common.builder`):
    #: ``override.target`` > ``meta.stratus.target`` > ``tag_targets``
    #: > ``runner.target`` > DAG-level ``target``.
    target: str | None = None
    #: Override the dbt CLI verb for this single node (e.g.
    #: ``"build"``). Forwarded verbatim to the worker entry point.
    command: str | None = None

    # Concurrent-runs policy (allow / join / queue).
    concurrent_runs: ConcurrentRunsMode | None = None

    #: AWS resource tags to layer ON TOP of the runner-level
    #: ``resource_tags``. Shallow-merged (later layers win per key).
    resource_tags: dict[str, str] | None = None


class GluePythonShellRunner(Runner):
    """Submits each dbt node as a Glue Python Shell JobRun via
    :class:`airflow.providers.amazon.aws.operators.glue.GlueJobOperator`
    with ``deferrable=True``.

    Args:
        job_name: AWS Glue Job name.
        mode: ``"attach"`` (existing Job) or ``"create"`` (lib creates
            on first run). Default ``"create"``.
        iam_role_name: IAM role NAME (not ARN) for the Glue Job.
            Required when ``mode='create'``.
        script_location: ``s3://...`` URL of the entry script.
            Required when ``mode='create'``.
        create_job_kwargs: extra fields forwarded to
            ``GlueJobOperator.create_job_kwargs``.
        update_config: when True and ``mode='create'``, update an
            existing Job to match the requested spec.
        max_capacity: Glue Python Shell DPU sizing. Must be ``0.0625``
            or ``1.0`` (Glue API constraint). Default ``1.0``.
        timeout_minutes: per-JobRun timeout. Glue Python Shell tops out
            at 2880 minutes (48h).
        glue_version: Glue runtime. Default ``"3.0"`` (Python 3.9).
            Glue 4.0/5.0 are Spark-only.
        python_version: Python version for the Glue runtime.
            Default ``"3.9"``.
        full_refresh: when True, every task passes ``--full-refresh``
            to dbt.
        vars_json: optional JSON string forwarded as dbt ``--vars``.
        upload_artefacts_s3_prefix: when set, each JobRun uploads its
            ``target/`` to ``<prefix>/<unique_id>/``.
        state_s3: ``s3://`` prefix of a previous run's state for
            dbt ``--state``.
        defer: when True (requires ``state_s3``), pass ``--defer`` to
            dbt.
        profile_name: dbt ``--profile`` override applied to every task
            this runner produces. Per-model / per-tag overrides are
            resolved by :mod:`dbt_aws.common.builder`; this value is
            the runner-level fallback.
        target: dbt ``--target`` override applied to every task this
            runner produces. Beats the DAG-level ``target=`` but loses
            to per-model / per-tag overrides. Default ``None`` -- use
            the DAG-level target.
        env_vars_json: JSON object of env vars set before dbt runs.
        dbt_extra_flags: list of raw flags appended to the dbt argv.
        aws_conn_id: Airflow connection id. Default ``"aws_default"``.
        region_name: AWS region (``None`` lets the connection decide).
        deferrable: defer to the Triggerer. Default True.
        waiter_delay: seconds between deferred poll attempts.
        waiter_max_attempts: max polls before timeout.
        stop_job_run_on_kill: cancel the Glue JobRun on Airflow kill.
        verbose: stream Glue CloudWatch logs to Airflow task log.

    Raises:
        ValueError: invalid mode, missing required args for selected
            mode, or invalid sizing values.
    """

    OVERRIDE_TYPE: ClassVar[type[RunnerOverride]] = GluePythonShellOverride

    #: Short identifier used in default resource names.
    RUNNER_KIND: ClassVar[str] = "pyshell"

    def __init__(
        self,
        *,
        job_name: str | None = None,
        mode: GluePythonShellMode = "create",
        # Resource naming (used when job_name is not given):
        name_prefix: str | None = None,
        job_name_template: str | None = None,
        iam_role_name: str | None = None,
        script_location: str | None = None,
        # Auto-upload of the entry script (mode='create' only). Pass
        # instead of ``script_location`` -- lib uploads the bundled
        # entry script to ``s3://{deploy_bucket}/{deploy_prefix}/
        # worker_entrypoint.py`` (HEAD+ETag-compared, idempotent).
        deploy_bucket: str | None = None,
        deploy_prefix: str = "dbt-aws",
        create_job_kwargs: dict[str, Any] | None = None,
        update_config: bool = False,
        max_capacity: float = 1.0,
        timeout_minutes: int = 60,
        glue_version: str = "3.0",
        python_version: str = "3.9",
        full_refresh: bool = False,
        vars_json: str | None = None,
        upload_artefacts_s3_prefix: str | None = None,
        state_s3: str | None = None,
        defer: bool = False,
        profile_name: str | None = None,
        target: str | None = None,
        env_vars_json: str | None = None,
        dbt_extra_flags: list[str] | None = None,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        deferrable: bool = True,
        with_deps: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 120,
        stop_job_run_on_kill: bool = True,
        verbose: bool = False,
        # Concurrent-runs policy across DAGs that hit the same Glue Job.
        concurrent_runs: ConcurrentRunsMode = "allow",
        # AWS resource tags applied to the Glue Job at ``create_job``
        # time (mode='create' only). See
        # ``dbt_aws.common.runner.tags`` for validation rules.
        resource_tags: dict[str, str] | None = None,
        # Per-task callbacks. The lib appends an audit-log callback
        # unless ``audit_log=False``. User callbacks always fire first.
        on_execute_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        on_success_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        on_failure_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        audit_log: bool = True,
        # OpenLineage / SMUS integration. ``None`` = feature off.
        openlineage: OpenLineageConfig | None = None,
    ) -> None:
        if mode not in ("attach", "create"):
            raise ValueError(f"mode must be 'attach' or 'create', got {mode!r}")
        if mode == "create":
            if iam_role_name is None:
                raise ValueError(
                    "mode='create' requires iam_role_name=; pass it via "
                    "the GluePythonShellRunner constructor."
                )
            if (script_location is None) == (deploy_bucket is None):
                raise ValueError(
                    "mode='create' requires EXACTLY ONE of "
                    "script_location= (use a pre-uploaded URI) or "
                    "deploy_bucket= (lib auto-uploads the bundled "
                    "entry script)."
                )
        else:
            for name, value in (
                ("iam_role_name", iam_role_name),
                ("script_location", script_location),
                ("deploy_bucket", deploy_bucket),
                ("create_job_kwargs", create_job_kwargs),
            ):
                if value is not None:
                    raise ValueError(
                        f"mode='attach' does not use {name}=; the Glue "
                        f"Job is expected to exist already. Got {value!r}."
                    )

        if max_capacity not in _VALID_MAX_CAPACITY:
            raise ValueError(
                f"max_capacity must be one of {_VALID_MAX_CAPACITY} "
                f"(Glue Python Shell constraint), got {max_capacity!r}"
            )

        if upload_artefacts_s3_prefix is not None and not upload_artefacts_s3_prefix.startswith(
            "s3://"
        ):
            raise ValueError(
                f"upload_artefacts_s3_prefix must start with 's3://', "
                f"got {upload_artefacts_s3_prefix!r}"
            )
        if state_s3 is not None and not state_s3.startswith("s3://"):
            raise ValueError(f"state_s3 must start with 's3://', got {state_s3!r}")
        if defer and state_s3 is None:
            raise ValueError(
                "defer=True requires state_s3= (dbt --defer needs a "
                "state manifest to compare against)."
            )

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

        self.job_name = job_name
        self.name_prefix = name_prefix
        self.job_name_template = job_name_template
        self.mode = mode
        self.iam_role_name = iam_role_name
        self.script_location = script_location
        self.deploy_bucket = deploy_bucket
        self.deploy_prefix = deploy_prefix
        self.create_job_kwargs = dict(create_job_kwargs or {})
        self.update_config = update_config

        # AWS resource tags -- validated at DAG-parse; folded into
        # create_job_kwargs["Tags"] in ``_build_create_job_kwargs``.
        validate_resource_tags(resource_tags, where="GluePythonShellRunner.resource_tags")
        self.resource_tags: dict[str, str] | None = dict(resource_tags) if resource_tags else None

        self.max_capacity = max_capacity
        self.timeout_minutes = timeout_minutes
        self.glue_version = glue_version
        self.python_version = python_version

        self.full_refresh = full_refresh
        self.vars_json = vars_json
        self.upload_artefacts_s3_prefix = upload_artefacts_s3_prefix
        self.state_s3 = state_s3
        self.defer = defer
        self.profile_name = profile_name
        self.target = target
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
        self.on_execute_callback = on_execute_callback
        self.on_success_callback = on_success_callback
        self.on_failure_callback = on_failure_callback
        self.audit_log = audit_log
        validate_lineage_optin(
            openlineage, region_fallback=region_name, runner_class_name="GluePythonShellRunner"
        )
        self.openlineage = openlineage

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
        from airflow.providers.amazon.aws.operators.glue import GlueJobOperator  # noqa: F401

        ov = resolve_override(
            node=node,
            override_class=GluePythonShellOverride,
            explicit_overrides=overrides,
        )
        assert isinstance(ov, GluePythonShellOverride)
        if ov.command:
            dbt_command = ov.command

        # Choose the operator class based on concurrent_runs policy.
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
            # Pass MaxCapacity via GlueJobOperator's ``num_of_dpus`` so
            # Airflow's GlueJobHook doesn't silently override it with
            # its default of 10 DPUs (which Glue rejects for Python
            # Shell, where only 0.0625 and 1 are valid). The hook's
            # ``create_glue_job_config`` writes ``MaxCapacity =
            # self.num_of_dpus`` AFTER merging create_job_kwargs.
            "num_of_dpus": (ov.max_capacity if ov.max_capacity is not None else self.max_capacity),
        }

        # Per-JobRun overrides via run_job_kwargs. Python Shell accepts
        # MaxCapacity and Timeout overrides.
        run_job_kwargs = _build_run_job_kwargs(ov)
        if run_job_kwargs:
            kwargs["run_job_kwargs"] = run_job_kwargs

        if eff_mode == "create":
            kwargs["iam_role_name"] = eff_iam_role_name
            kwargs["script_location"] = eff_script_location
            kwargs["update_config"] = self.update_config
            kwargs["create_job_kwargs"] = self._build_create_job_kwargs()

        if airflow_kwargs:
            kwargs.update(airflow_kwargs)

        self._attach_callbacks(
            kwargs=kwargs,
            node=node,
            override=ov,
            eff_job_name=str(kwargs.get("job_name")),
            eff_mode=self.mode,
            dbt_command=dbt_command,
            select=select,
            target=target,
        )

        return operator_cls(**kwargs)

    # ------------------------------------------------------------------
    def _attach_callbacks(
        self,
        *,
        kwargs: dict[str, Any],
        node: DbtNode,
        override: GluePythonShellOverride,
        eff_job_name: str,
        eff_mode: str,
        dbt_command: str,
        select: str,
        target: str,
    ) -> None:
        """Merge user-supplied callbacks with the log-link audit
        callbacks into the operator kwargs. See
        :mod:`dbt_aws.common.airflow_extras.log_link`.

        Glue Python Shell jobs use the same ``GlueJobOperator`` as
        Spark jobs, so we reuse :func:`make_glue_job_audit_callback`
        with a Python-Shell-specific label.
        """
        from dbt_aws.common.airflow_extras.log_link import (
            make_glue_job_audit_callback,
            merge_callbacks,
        )

        audit: dict[str, Callable[..., None]] = {}
        if self.audit_log and self.region_name:
            audit = make_glue_job_audit_callback(
                region=self.region_name,
                job_name=eff_job_name,
                backend_label="AWS Glue Python Shell",
            )

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

        # tag-sync callback. Mirrors GlueSparkRunner -- see
        # ``dbt_aws.spark.runners.glue_job._merge_audit_callbacks`` for
        # the design rationale. glue:UpdateJob rejects ``Tags``, so
        # tags flow through glue:TagResource at task-execute time.
        #
        # shallow-merge runner-level tags with any per-node /
        # per-tag ``resource_tags`` override.
        eff_resource_tags = merge_resource_tags(self.resource_tags, override.resource_tags)
        if eff_resource_tags:
            validate_resource_tags(
                eff_resource_tags,
                where=f"GluePythonShellRunner.resource_tags[{node.unique_id!r}]",
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
    def _build_script_args(
        self,
        *,
        node: DbtNode,
        dbt_command: str,
        select: str,
        target: str,
        project_archive_s3: str,
        run_id_template: str,
        override: GluePythonShellOverride,
    ) -> dict[str, str]:
        """Assemble the runner script's argv. Per-model overrides win
        over the runner-level defaults for the fields they cover."""
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

    def _resolve_script_location(self) -> str:
        """Return the effective ``script_location`` URI, uploading the
        bundled entry script to S3 on first call when the caller used
        ``deploy_bucket=`` instead of ``script_location=``.

        Memoised on ``self.script_location``; underlying helper is
        idempotent via HEAD + ETag compare.
        """
        if self.script_location is not None:
            return self.script_location
        if self.deploy_bucket is None:
            raise RuntimeError(
                "_resolve_script_location() called without script_location "
                "or deploy_bucket -- this is a bug in dbt-aws."
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
        """Glue Job spec at create time. Note: Python Shell uses
        ``MaxCapacity`` (NOT ``NumberOfWorkers`` / ``WorkerType``),
        and ``Command.Name='pythonshell'``.

        ``Command.ScriptLocation`` is required by Glue's CreateJob
        API. Airflow's ``GlueJobHook.create_glue_job_config`` REPLACES
        the default Command dict with whatever we pass in
        ``create_job_kwargs.Command``, so we must include
        ScriptLocation here (the hook's defaulting logic only kicks in
        when ``Command`` is absent from ``create_job_kwargs``).

        Notably does NOT include ``Tags``. Airflow's ``GlueJobHook``
        forwards ``create_job_kwargs`` verbatim to both
        ``glue:CreateJob`` (accepts ``Tags``) AND ``glue:UpdateJob``
        (does NOT -- ``JobUpdate`` rejects ``Tags`` with a boto3
        ``ParamValidationError``). Same bug as GlueSparkRunner --
        see :meth:`dbt_aws.spark.runners.glue_job.GlueSparkRunner._build_create_job_kwargs`.
        Fixed.
        """
        baseline: dict[str, Any] = {
            "GlueVersion": self.glue_version,
            "MaxCapacity": self.max_capacity,
            "Timeout": self.timeout_minutes,
            "Command": {
                "Name": "pythonshell",
                "PythonVersion": self.python_version,
                "ScriptLocation": self.script_location,
            },
        }
        # Caller-supplied keys win; if they want a different Command
        # config (e.g. explicit ScriptLocation) it overrides ours.
        # ``Tags`` are dropped -- see docstring above.
        caller_kwargs = dict(self.create_job_kwargs)
        caller_tags = caller_kwargs.pop("Tags", None)
        if caller_tags:
            import logging

            _log = logging.getLogger(__name__)
            _log.warning(
                "GluePythonShellRunner: ``create_job_kwargs['Tags']`` is not "
                "forwarded to Glue -- glue:UpdateJob rejects ``Tags`` in "
                "``JobUpdate``. Set ``resource_tags`` on the runner "
                "constructor (or the top-level ``resource_tags:`` YAML "
                "key) so tags flow through ``glue:TagResource`` instead. "
                "Dropping keys: %s",
                sorted(caller_tags),
            )
        baseline.update(caller_kwargs)
        return baseline


def _build_run_job_kwargs(override: GluePythonShellOverride) -> dict[str, Any]:
    """Per-JobRun overrides via ``GlueJobOperator.run_job_kwargs``."""
    kwargs: dict[str, Any] = {}
    if override.max_capacity is not None:
        kwargs["MaxCapacity"] = override.max_capacity
    if override.timeout_minutes is not None:
        kwargs["Timeout"] = override.timeout_minutes
    return kwargs
