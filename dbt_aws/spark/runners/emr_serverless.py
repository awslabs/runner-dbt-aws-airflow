"""EMR Serverless runner -- one Spark job run per dbt node.

Two modes:

* ``mode="attach"`` -- the EMR Serverless application is provisioned
  out-of-band (CFN / Terraform / console). The runner only submits
  ``StartJobRun`` against the existing ``application_id``. This is the
  recommended pattern for production.

* ``mode="create"`` -- the lib adds setup / teardown tasks that
  create the application at DAG-run-start and delete it at
  DAG-run-end. Per-node tasks reference the application ID via
  XCom pull. Use when you want one DAG to fully own the application
  lifecycle.

Every node becomes one
:class:`airflow.providers.amazon.aws.operators.emr.EmrServerlessStartJobOperator`
with ``deferrable=True``. Worker slots are freed during the wait; the
Triggerer polls asynchronously.

Runner CLI contract -- identical to ``GlueSparkRunner`` (same
``_worker_entrypoint.py``):

* ``--command``       dbt verb (``run`` / ``snapshot`` / ``seed`` / ``test``)
* ``--select``        dbt selector
* ``--target``        dbt target
* ``--project-archive``  ``s3://...tar.gz`` of the project bundle
* ``--stratus-run-id``   Airflow run_id for log correlation
* ``--full-refresh``     when ``full_refresh=True``
* ``--vars``             when ``vars_json`` is provided
* ``--upload-artefacts-s3``  per-node sub-prefix when configured

The args go into ``job_driver.sparkSubmit.entryPointArguments`` as a
flat ``[--key, value, --key2, value2, ...]`` list (EMR Serverless
takes argv as a list, not a dict like Glue).
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
    resolve_override,
    resolve_resource_name,
    validate_resource_tags,
)

if TYPE_CHECKING:  # pragma: no cover
    from airflow.models import BaseOperator
    from airflow.sdk import DAG

    from dbt_aws.common.graph.node import DbtNode


EmrServerlessMode = Literal["attach", "create"]

#: Jinja template that resolves to the application_id created by the
#: setup task. Per-node tasks render this at task-execute time.
_APP_ID_XCOM_TEMPLATE = "{{{{ ti.xcom_pull(task_ids='{setup_task_id}') }}}}"

#: Task IDs for the setup / teardown tasks. ``DbtDag`` wires them
#: in via ``Runner.make_setup_task`` / ``make_teardown_task``.
SETUP_TASK_ID = "emr_serverless_setup"
TEARDOWN_TASK_ID = "emr_serverless_teardown"


@dataclass(frozen=True)
class EmrServerlessOverride(RunnerOverride):
    """Per-model overrides for :class:`EmrServerlessRunner`.

    All fields optional; ``None`` means "use the runner default".
    """

    # Resource identity
    application_id: str | None = None
    execution_role_arn: str | None = None

    #: Per-node override for the ``{prefix}`` token used by
    #: :func:`resolve_resource_name`. Rarely needed — typically the
    #: runner-level ``name_prefix`` is enough.
    name_prefix: str | None = None

    #: AWS resource tags to layer ON TOP of the runner-level
    #: ``resource_tags``. Shallow-merged (later layers win per key).
    #: Applies only under ``mode='create'``.
    resource_tags: dict[str, str] | None = None

    # Compute sizing (Spark conf, applied as ``--conf`` flags on
    # ``sparkSubmitParameters``)
    driver_cores: int | None = None
    driver_memory: str | None = None
    executor_cores: int | None = None
    executor_memory: str | None = None
    num_executors: int | None = None

    # Per-JobRun timeout (minutes)
    timeout_minutes: int | None = None

    # dbt-side knobs
    full_refresh: bool | None = None
    vars_json: str | None = None
    #: Override the dbt ``--profile`` flag for this single node.
    profile_name: str | None = None
    #: Override the dbt ``--target`` flag for this single node.
    #: Precedence (resolved in :mod:`dbt_aws.common.builder`):
    #: ``override.target`` > ``meta.stratus.target`` > ``tag_targets``
    #: > ``runner.target`` > DAG-level ``target``.
    target: str | None = None
    #: Override the dbt CLI verb for this single node (e.g.
    #: ``"build"``). Forwarded verbatim to the worker entry point.
    command: str | None = None


class EmrServerlessRunner(Runner):
    """Per-node runner that submits an EMR Serverless Spark job via
    :class:`airflow.providers.amazon.aws.operators.emr.EmrServerlessStartJobOperator`
    with ``deferrable=True``.

    Per-model overrides are declared via :class:`EmrServerlessOverride`.

    Args:
        application_id: ID of the EMR Serverless application
            (``00xxxxxxxxxxxxxx``). In ``attach`` mode this is required
            and the application must already exist. In ``create`` mode
            this MUST be ``None`` -- the lib creates the application
            via a setup task and pushes the resulting ID through XCom.
        application_name: friendly name for the created application.
            ``create`` mode only.
        release_label: EMR release label (``emr-7.0.0``, ``emr-6.15.0``,
            ...). Required when ``mode="create"``.
        execution_role_arn: IAM role ARN the job runs under. Required.
        mode: ``"attach"`` (default) or ``"create"``.
        script_location: ``s3://...`` URL of the runner entry script.
            Required in BOTH modes unless ``deploy_bucket=`` is given
            (EMR Serverless takes the entry-point script per-job, not
            baked into the application like Glue).
        deploy_bucket: S3 bucket the lib will upload the bundled entry
            script to (``s3://{bucket}/{prefix}/worker_entrypoint.py``)
            on first DAG build. Idempotent via S3 HEAD + ETag compare.
            Mutually exclusive with ``script_location=``.
        deploy_prefix: S3 key prefix paired with ``deploy_bucket``.
            Default ``"dbt-aws"``.
        driver_cores / driver_memory: Spark driver sizing. Defaults
            ``2`` cores / ``"4g"``.
        executor_cores / executor_memory: Spark executor sizing.
            Defaults ``2`` cores / ``"4g"``.
        num_executors: number of Spark executors. Default ``2``.
        timeout_minutes: per-JobRun timeout (EMR Serverless's own
            kill-switch). Default ``60``.
        full_refresh / vars_json / upload_artefacts_s3_prefix /
        state_s3 / defer / profile_name / env_vars_json /
        dbt_extra_flags: see :class:`GlueSparkRunner` for semantics.
            All forwarded verbatim into the entry script's argv.
        configuration_overrides: extra ``configuration_overrides``
            forwarded to ``EmrServerlessStartJobOperator``. Useful for
            log destinations (``monitoringConfiguration``) and Spark
            classification overrides.
        create_application_kwargs: extra fields forwarded to the
            ``EmrServerlessCreateApplicationOperator`` setup task.
            ``create`` mode only. Use this to set
            ``initialCapacity`` / ``maximumCapacity`` /
            ``autoStartConfiguration`` / ``autoStopConfiguration`` /
            ``networkConfiguration``.
        spark_submit_parameters_extra: free-form string appended to
            the generated ``sparkSubmitParameters`` for advanced
            tuning. Use sparingly.
        aws_conn_id: Airflow connection id. Default ``aws_default``.
        region_name: AWS region; ``None`` lets the connection decide.
        deferrable: defer to the Triggerer. Default ``True``.
        waiter_delay: seconds between poll attempts.
        waiter_max_attempts: max polls before timing out.
        cancel_on_kill: cancel the EMR JobRun when the Airflow task is
            killed. Default ``True``.

    Raises:
        ValueError: on invalid mode or missing required args.
    """

    OVERRIDE_TYPE: ClassVar[type[RunnerOverride]] = EmrServerlessOverride
    RUNNER_KIND: ClassVar[str] = "emr-serverless"

    def __init__(
        self,
        *,
        # Resource identity:
        application_id: str | None = None,
        application_name: str | None = None,
        release_label: str | None = None,
        execution_role_arn: str | None = None,
        mode: EmrServerlessMode = "attach",
        reusable: bool = True,
        # mode='create' only:
        script_location: str | None = None,
        deploy_bucket: str | None = None,
        deploy_prefix: str = "dbt-aws",
        create_application_kwargs: dict[str, Any] | None = None,
        # Spark sizing:
        driver_cores: int = 2,
        driver_memory: str = "4g",
        executor_cores: int = 2,
        executor_memory: str = "4g",
        num_executors: int = 2,
        timeout_minutes: int = 60,
        spark_submit_parameters_extra: str | None = None,
        configuration_overrides: dict[str, Any] | None = None,
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
        # Resource naming (used when application_name is not given,
        # mode='create' only):
        name_prefix: str | None = None,
        application_name_template: str | None = None,
        # Operator behaviour:
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        deferrable: bool = True,
        with_deps: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 120,
        cancel_on_kill: bool = True,
        # Per-task callbacks. ``audit_log=False`` disables the audit
        # layer entirely.
        on_execute_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        on_success_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        on_failure_callback: Callable[..., None] | list[Callable[..., None]] | None = None,
        audit_log: bool = True,
        # OpenLineage / SMUS integration. ``None`` = feature off.
        openlineage: OpenLineageConfig | None = None,
        # AWS resource tags applied to the EMR Serverless application
        # at ``create_application`` time (mode='create' only; ignored
        # under mode='attach' where the application pre-exists and
        # IaC owns its tags). See ``dbt_aws.common.runner.tags`` for
        # validation rules.
        resource_tags: dict[str, str] | None = None,
    ) -> None:
        if mode not in ("attach", "create"):
            raise ValueError(f"mode must be 'attach' or 'create', got {mode!r}")

        if not reusable and mode != "create":
            raise ValueError(
                "reusable=False requires mode='create' (per-node "
                "application lifecycle means the lib has to own the "
                "create+delete; attach mode reuses one existing app "
                "so non-reusable doesn't apply)."
            )

        if execution_role_arn is None:
            raise ValueError(
                "execution_role_arn= is required (the IAM role the EMR Serverless job runs under)."
            )

        if mode == "create":
            if application_id is not None:
                raise ValueError(
                    "mode='create' does not use application_id=; the "
                    "lib creates a fresh application per DAG run and "
                    "passes the ID through XCom."
                )
            if release_label is None:
                raise ValueError(
                    "mode='create' requires release_label= (e.g. "
                    "'emr-7.0.0'); pass it to the runner constructor."
                )
        else:
            # ``attach`` mode forbids create-application args so caller
            # intent is unambiguous. The script location IS still
            # required (unlike Glue, EMR Serverless does not bake the
            # entry-point script into the application; it's per-job).
            if application_id is None:
                raise ValueError(
                    "mode='attach' requires application_id= (the "
                    "existing EMR Serverless application to submit "
                    "jobs against)."
                )
            for name, value in (
                ("application_name", application_name),
                ("release_label", release_label),
                ("create_application_kwargs", create_application_kwargs),
            ):
                if value is not None:
                    raise ValueError(
                        f"mode='attach' does not use {name}=; the "
                        f"application is expected to exist already. "
                        f"Got {value!r}."
                    )

        # Entry-point script: required in BOTH modes (unlike Glue,
        # which bakes the script into the Glue Job in create mode).
        # Accept either an explicit URI or a deploy_bucket for the lib
        # to auto-upload the bundled entry script.
        if (script_location is None) == (deploy_bucket is None):
            raise ValueError(
                "EmrServerlessRunner requires EXACTLY ONE of "
                "script_location= (pre-uploaded URI) or "
                "deploy_bucket= (lib auto-uploads the bundled "
                "entry script). Got "
                f"script_location={script_location!r}, "
                f"deploy_bucket={deploy_bucket!r}."
            )

        self.application_id = application_id
        self.application_name = application_name
        self.application_name_template = application_name_template
        self.name_prefix = name_prefix
        self.release_label = release_label
        self.execution_role_arn = execution_role_arn
        self.mode = mode
        self.reusable = reusable

        self.script_location = script_location
        self.deploy_bucket = deploy_bucket
        self.deploy_prefix = deploy_prefix
        self.create_application_kwargs = dict(create_application_kwargs or {})

        # AWS resource tags -- validated at DAG-parse; folded into the
        # ``create_application`` call at task-build time (see
        # ``_apply_create_application_kwargs``).
        validate_resource_tags(resource_tags, where="EmrServerlessRunner.resource_tags")
        self.resource_tags: dict[str, str] | None = dict(resource_tags) if resource_tags else None

        self.driver_cores = driver_cores
        self.driver_memory = driver_memory
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.num_executors = num_executors
        self.timeout_minutes = timeout_minutes
        self.spark_submit_parameters_extra = spark_submit_parameters_extra
        self.configuration_overrides = dict(configuration_overrides or {})

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

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.deferrable = deferrable
        self.with_deps = with_deps
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.cancel_on_kill = cancel_on_kill

        self.on_execute_callback = on_execute_callback
        self.on_success_callback = on_success_callback
        self.on_failure_callback = on_failure_callback
        self.audit_log = audit_log
        validate_lineage_optin(
            openlineage, region_fallback=region_name, runner_class_name="EmrServerlessRunner"
        )
        self.openlineage = openlineage

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
            EmrServerlessStartJobOperator,
        )

        ov = resolve_override(
            node=node,
            override_class=EmrServerlessOverride,
            explicit_overrides=overrides,
        )
        assert isinstance(ov, EmrServerlessOverride)
        if ov.command:
            dbt_command = ov.command

        # When reusable=False the per-node setup task creates a fresh
        # application and the per-node StartJobRun must pull THAT app's
        # id from XCom (not the DAG-level shared setup task's). We
        # compute the setup-task-id here so ``_effective_application_id``
        # can pick it up via the constructor-time ``setup_task_id_override``
        # hook.
        per_node_setup_task_id = f"{task_id}.setup" if not self.reusable else None

        eff_application_id = self._effective_application_id(
            ov, setup_task_id_override=per_node_setup_task_id
        )
        eff_execution_role_arn = ov.execution_role_arn or self.execution_role_arn

        if self.script_location is None and self.deploy_bucket is not None:
            # Lazy-upload the bundled entry script on first task build.
            self._resolve_script_location()
        eff_entry_point = self.script_location
        if eff_entry_point is None:
            raise RuntimeError(
                "EmrServerlessRunner.make_task: no entry-point script "
                "URL resolved. This is a bug in dbt-aws."
            )

        entry_args = self._build_entry_point_arguments(
            node=node,
            dbt_command=dbt_command,
            select=select,
            target=target,
            project_archive_s3=project_archive_s3,
            run_id_template=run_id_template,
            override=ov,
        )

        spark_submit_params = self._build_spark_submit_parameters(ov)

        job_driver: dict[str, Any] = {
            "sparkSubmit": {
                "entryPoint": eff_entry_point,
                "entryPointArguments": entry_args,
                "sparkSubmitParameters": spark_submit_params,
            },
        }

        # Per-JobRun execution timeout. EMR Serverless takes
        # ``executionTimeoutMinutes`` at the StartJobRun level.
        eff_timeout = ov.timeout_minutes or self.timeout_minutes

        kwargs: dict[str, Any] = {
            "task_id": task_id,
            "dag": dag,
            "application_id": eff_application_id,
            "execution_role_arn": eff_execution_role_arn,
            "job_driver": job_driver,
            "aws_conn_id": self.aws_conn_id,
            "region_name": self.region_name,
            "deferrable": self.deferrable,
            "waiter_delay": self.waiter_delay,
            "waiter_max_attempts": self.waiter_max_attempts,
            "cancel_on_kill": self.cancel_on_kill,
            "wait_for_completion": True,
            "name": f"{node.unique_id}",
        }
        if self.configuration_overrides:
            kwargs["configuration_overrides"] = dict(self.configuration_overrides)
        if eff_timeout is not None:
            # Forwarded via ``config`` (the operator passes it through
            # to ``StartJobRun``).
            kwargs.setdefault("config", {})
            kwargs["config"]["executionTimeoutMinutes"] = eff_timeout

        if airflow_kwargs:
            kwargs.update(airflow_kwargs)

        self._attach_callbacks(
            kwargs=kwargs,
            node=node,
            override=ov,
            eff_application_id=eff_application_id,
            dbt_command=dbt_command,
            select=select,
            target=target,
        )

        if self.reusable:
            return EmrServerlessStartJobOperator(**kwargs)

        # ------------------------------------------------------------------
        # reusable=False: emit a per-node (setup, statement, teardown)
        # triplet wrapped in a TaskGroup. Each dbt node owns its own
        # short-lived EMR Serverless application.
        # ------------------------------------------------------------------
        from airflow.providers.amazon.aws.operators.emr import (
            EmrServerlessCreateApplicationOperator,
            EmrServerlessDeleteApplicationOperator,
        )

        from dbt_aws.common._airflow_compat import TaskGroup

        # Statement op kwargs already point at the per-node setup task
        # for the application_id XCom pull (see ``per_node_setup_task_id``
        # threaded through ``_effective_application_id`` above). Adjust
        # the statement-op task_id so it lives INSIDE the group.
        statement_kwargs = dict(kwargs)
        statement_kwargs["task_id"] = "statement"
        statement_kwargs["dag"] = None  # bound by ambient TaskGroup ctx

        # Per-node application name: ``<prefix>-<run_id>-<node.name>``.
        # Mirrors the Glue Session per-node naming.
        eff_app_name_prefix = self.name_prefix or self.RUNNER_KIND
        per_node_name = f"{eff_app_name_prefix}-{{{{ run_id }}}}-{node.name}"
        create_config: dict[str, Any] = {"name": per_node_name}
        if self.resource_tags:
            create_config["tags"] = dict(self.resource_tags)
        create_config.update(self.create_application_kwargs)

        with TaskGroup(group_id=task_id) as tg:
            # mypy: reusable=False is validated to require mode='create'
            # in __init__, which in turn requires release_label -- so by
            # the time we reach this branch ``self.release_label`` is
            # guaranteed non-None.
            assert self.release_label is not None
            setup = EmrServerlessCreateApplicationOperator(
                task_id="setup",
                release_label=self.release_label,
                job_type="SPARK",
                config=create_config,
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                deferrable=self.deferrable,
                waiter_delay=self.waiter_delay,
                waiter_max_attempts=self.waiter_max_attempts,
                wait_for_completion=True,
            )
            statement = EmrServerlessStartJobOperator(**statement_kwargs)
            teardown = EmrServerlessDeleteApplicationOperator(
                task_id="teardown",
                application_id=_APP_ID_XCOM_TEMPLATE.format(
                    setup_task_id=per_node_setup_task_id,
                ),
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                deferrable=self.deferrable,
                waiter_delay=self.waiter_delay,
                waiter_max_attempts=self.waiter_max_attempts,
                wait_for_completion=True,
                # all_done so the app is torn down even if the
                # statement task fails -- prevents leaked
                # applications burning DPU credits.
                trigger_rule="all_done",
            )
            setup >> statement >> teardown
        return tg  # type: ignore[return-value]  # TaskGroup is chainable like a BaseOperator

    # ------------------------------------------------------------------
    # Reusable-runner hooks: create/delete the EMR Serverless app
    # ------------------------------------------------------------------
    def make_setup_task(
        self,
        *,
        dag: DAG | None = None,
        airflow_kwargs: dict[str, Any] | None = None,
    ) -> BaseOperator | None:
        """Return a ``EmrServerlessCreateApplicationOperator`` when
        ``mode='create'`` AND ``reusable=True``. The application ID is
        pushed to XCom and consumed by per-node tasks at task-execute
        time.

        Returns ``None`` in ``attach`` mode (application exists) OR
        when ``reusable=False`` (per-node tasks own their own setup
        inside the per-node TaskGroup -- no shared setup task).
        """
        if self.mode != "create" or not self.reusable:
            return None
        from airflow.providers.amazon.aws.operators.emr import (
            EmrServerlessCreateApplicationOperator,
        )

        eff_name = resolve_resource_name(
            explicit=self.application_name,
            template=self.application_name_template,
            name_prefix=self.name_prefix,
            dag_id=dag.dag_id if dag is not None else "",
            runner_kind=self.RUNNER_KIND,
            node=None,
        )

        config: dict[str, Any] = {"name": eff_name}
        if self.resource_tags:
            config["tags"] = dict(self.resource_tags)
        config.update(self.create_application_kwargs)

        kwargs: dict[str, Any] = {
            "task_id": SETUP_TASK_ID,
            "dag": dag,
            "release_label": self.release_label,
            "job_type": "SPARK",
            "config": config,
            "aws_conn_id": self.aws_conn_id,
            "region_name": self.region_name,
            "deferrable": self.deferrable,
            "waiter_delay": self.waiter_delay,
            "waiter_max_attempts": self.waiter_max_attempts,
            "wait_for_completion": True,
        }
        if airflow_kwargs:
            kwargs.update(airflow_kwargs)
        return EmrServerlessCreateApplicationOperator(**kwargs)

    def make_teardown_task(
        self,
        *,
        dag: DAG | None = None,
        airflow_kwargs: dict[str, Any] | None = None,
    ) -> BaseOperator | None:
        """Return a ``EmrServerlessDeleteApplicationOperator`` when
        ``mode='create'`` AND ``reusable=True``. Reads the application
        ID via XCom from the setup task.

        ``DbtDag`` wires this with ``trigger_rule='all_done'`` so
        the application is torn down even when one of the per-node
        jobs fails.

        Returns ``None`` in ``attach`` mode OR when ``reusable=False``
        (per-node tasks own their own teardown).
        """
        if self.mode != "create" or not self.reusable:
            return None
        from airflow.providers.amazon.aws.operators.emr import (
            EmrServerlessDeleteApplicationOperator,
        )

        kwargs: dict[str, Any] = {
            "task_id": TEARDOWN_TASK_ID,
            "dag": dag,
            "application_id": _APP_ID_XCOM_TEMPLATE.format(
                setup_task_id=SETUP_TASK_ID,
            ),
            "aws_conn_id": self.aws_conn_id,
            "region_name": self.region_name,
            "deferrable": self.deferrable,
            "waiter_delay": self.waiter_delay,
            "waiter_max_attempts": self.waiter_max_attempts,
            "wait_for_completion": True,
            "trigger_rule": "all_done",
        }
        if airflow_kwargs:
            kwargs.update(airflow_kwargs)
        return EmrServerlessDeleteApplicationOperator(**kwargs)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _effective_application_id(
        self,
        override: EmrServerlessOverride,
        *,
        setup_task_id_override: str | None = None,
    ) -> str:
        """Return the application_id the per-node task should target.

        Precedence: per-node override > runner default > XCom template
        from the setup task (``mode='create'`` only). When
        ``setup_task_id_override`` is set (per-node ``reusable=False``
        branch), the XCom pull targets that task instead of the
        DAG-level shared setup task.
        """
        if override.application_id is not None:
            return override.application_id
        if self.application_id is not None:
            return self.application_id
        if self.mode == "create":
            setup_task_id = setup_task_id_override or SETUP_TASK_ID
            return _APP_ID_XCOM_TEMPLATE.format(setup_task_id=setup_task_id)
        raise RuntimeError(
            "EmrServerlessRunner: no application_id resolved. This is a bug in dbt-aws."
        )

    def _build_entry_point_arguments(
        self,
        *,
        node: DbtNode,
        dbt_command: str,
        select: str,
        target: str,
        project_archive_s3: str,
        run_id_template: str,
        override: EmrServerlessOverride,
    ) -> list[str]:
        """Build the entry point argv as a flat list.

        EMR Serverless's ``sparkSubmit.entryPointArguments`` takes a
        list, not a dict. Glue's ``script_args`` takes a dict that the
        operator unpacks; we do the equivalent here ourselves.

        Per-model overrides win over the runner-level defaults for
        the fields they cover (``full_refresh``, ``vars_json``).
        """
        effective_full_refresh = (
            override.full_refresh if override.full_refresh is not None else self.full_refresh
        )
        effective_vars_json = (
            override.vars_json if override.vars_json is not None else self.vars_json
        )
        # ``profile_name`` and ``target`` follow the same override -> runner
        # -> caller precedence. ``target`` gets a caller-supplied fallback
        # (the DAG-level value the builder threads through) because the
        # builder resolves tag/meta layers before ``make_task`` is called.
        effective_profile_name = (
            override.profile_name if override.profile_name is not None else self.profile_name
        )
        effective_target = (
            override.target if override.target is not None else (self.target or target)
        )

        args: list[tuple[str, str]] = [
            ("--command", dbt_command),
            ("--select", select),
            ("--target", effective_target),
            ("--project-archive", project_archive_s3),
            ("--stratus-run-id", run_id_template),
        ]
        if effective_full_refresh:
            args.append(("--full-refresh", "true"))
        if effective_vars_json is not None:
            args.append(("--vars", effective_vars_json))
        if self.upload_artefacts_s3_prefix:
            prefix = self.upload_artefacts_s3_prefix.rstrip("/")
            args.append(("--upload-artefacts-s3", f"{prefix}/{node.unique_id}/"))
        args.append(("--with-deps", "true" if self.with_deps else "false"))
        if self.state_s3 is not None:
            args.append(("--state-s3", self.state_s3))
        if self.defer:
            args.append(("--defer", "true"))
        if effective_profile_name is not None:
            args.append(("--profile-name", effective_profile_name))
        if self.env_vars_json is not None:
            args.append(("--env-vars", self.env_vars_json))
        if self.dbt_extra_flags:
            import json as _json

            args.append(("--dbt-extra-flags", _json.dumps(self.dbt_extra_flags)))

        flat: list[str] = []
        for key, value in args:
            flat.append(key)
            flat.append(value)
        append_lineage_argv_list(
            flat,
            openlineage=self.openlineage,
            region_fallback=self.region_name,
            node_unique_id=node.unique_id,
        )
        return flat

    def _build_spark_submit_parameters(
        self,
        override: EmrServerlessOverride,
    ) -> str:
        """Assemble the ``sparkSubmitParameters`` string -- the Spark
        sizing knobs forwarded as ``--conf k=v`` pairs.

        Per-model overrides win over the runner-level defaults.
        """
        eff_driver_cores = override.driver_cores or self.driver_cores
        eff_driver_memory = override.driver_memory or self.driver_memory
        eff_executor_cores = override.executor_cores or self.executor_cores
        eff_executor_memory = override.executor_memory or self.executor_memory
        eff_num_executors = override.num_executors or self.num_executors

        parts: list[str] = [
            f"--conf spark.driver.cores={eff_driver_cores}",
            f"--conf spark.driver.memory={eff_driver_memory}",
            f"--conf spark.executor.cores={eff_executor_cores}",
            f"--conf spark.executor.memory={eff_executor_memory}",
            f"--conf spark.executor.instances={eff_num_executors}",
        ]
        if self.spark_submit_parameters_extra:
            parts.append(self.spark_submit_parameters_extra.strip())
        return " ".join(parts)

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
        override: EmrServerlessOverride,
        eff_application_id: str,
        dbt_command: str,
        select: str,
        target: str,
    ) -> None:
        """Merge user-supplied callbacks with the log-link audit
        callbacks into the operator ``kwargs`` dict.

        Audit emits an EMR Studio app URL + script args at submit time
        and a JobRun URL + CloudWatch driver stderr/stdout deep links
        on success/failure. See
        :mod:`dbt_aws.common.airflow_extras.log_link`.
        """
        from dbt_aws.common.airflow_extras.log_link import (
            make_emr_serverless_audit_callback,
            merge_callbacks,
        )

        if self.audit_log and self.region_name:
            audit = make_emr_serverless_audit_callback(
                region=self.region_name,
                application_id=eff_application_id,
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
