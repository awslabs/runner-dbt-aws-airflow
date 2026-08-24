"""Glue / EMR audit-log callbacks: clickable URLs + full input dump in the Airflow task log.

What they do:

  * Emit clickable AWS console URLs in the Airflow task log (Glue
    Studio job & run pages, EMR Studio, CloudWatch deep links).
  * Push the same URLs to XCom under stable ``dbt_aws_*`` keys so
    downstream tasks (notifiers, alerters, observability sinks) can
    pull them.
  * Dump the **full set of job inputs** -- every Glue Job operator
    field, every script arg -- to the task log so the Airflow UI
    doubles as an audit trail.

Zero AWS API calls per task: callbacks just format URLs from operator
fields + XCom values. Cost: ~1 ms; users click through to view actual
logs.

Sample task-log output (each line is a separate INFO record so they're
all visible in the Airflow UI)::

    ======================================================================
    dbt-aws audit -- Glue Job submission
    ======================================================================
      Backend:        AWS Glue Job
      Job:            dbt-aws-my_dag-spark-stg_orders
      Region:         eu-west-1
      Job console:    https://eu-west-1.console.aws.amazon.com/gluestudio/home?region=eu-west-1#/job/dbt-aws-my_dag-spark-stg_orders
      -- Glue config:
        IAMRole:        Glue-Job-Role
        WorkerType:     G.1X
        NumberOfWorkers: 2
        ScriptLocation: s3://.../worker_entrypoint.py
        ...
      -- Script args:
        --command:      run
        --select:       stg_orders
        --project-archive-s3: s3://.../archives/<sha>.tar.gz
        ...

    [after the JobRun finishes]
    ======================================================================
    dbt-aws audit -- Glue Job result
    ======================================================================
      JobRunId:                jr_abc...
      JobRun console:          https://eu-west-1.console.aws.amazon.com/gluestudio/home?region=eu-west-1#/job/<name>/run/jr_abc...
      Driver stderr (list):    https://eu-west-1.console.aws.amazon.com/cloudwatch/home#logStream:group=/aws-glue/jobs/error;prefix=jr_abc...
      Driver stdout (list):    https://eu-west-1.console.aws.amazon.com/cloudwatch/home#logStream:group=/aws-glue/jobs/output;prefix=jr_abc...

This module replaces the older ``dbt_aws.common.audit`` Airflow-Log DB
writer. The DB-write path produced one row per Airflow task transition
(task_started / task_succeeded / task_failed) with the runner config
in an opaque ``extra`` JSON column -- noisy, invisible in the task log
UI, and missing the actual URL information operators need. We dropped
it in favour of this task-log emission + XCom push pattern.

Wired by every runner via ``audit_log=True`` (default). To turn the
audit callbacks off entirely, pass ``audit_log=False`` to the runner.
"""

from __future__ import annotations

import logging
import urllib.parse
from collections.abc import Callable
from typing import Any

# Use the standard Airflow task logger so output lands in the task
# log view in the Airflow UI on both 2.x and 3.x. ``airflow.task`` is
# the documented logger for callback output.
_log = logging.getLogger("airflow.task")

#: Default XCom key the Amazon provider's ``GlueJobOperator`` uses for
#: its return value (the JobRun id). Newer provider versions also push
#: it under ``glue_job_run_id``; we fall through several keys.
DEFAULT_JOB_ID_XCOM_KEY = "return_value"

# ---------------------------------------------------------------------------
# Stable XCom keys downstream tasks can pull. ``dbt_aws_`` prefix so they
# don't collide with provider-package keys (``glue_job_run_id``,
# ``return_value``, ...). Downstream tasks pull via
# ``ti.xcom_pull(key="dbt_aws_glue_job_run_url", task_ids="upstream_task")``.
# ---------------------------------------------------------------------------
XCOM_KEY_GLUE_JOB_URL = "dbt_aws_glue_job_url"
XCOM_KEY_GLUE_JOB_RUN_URL = "dbt_aws_glue_job_run_url"
XCOM_KEY_GLUE_OUTPUT_LOGS_URL = "dbt_aws_glue_output_logs_url"
XCOM_KEY_GLUE_ERROR_LOGS_URL = "dbt_aws_glue_error_logs_url"

XCOM_KEY_EMR_APP_URL = "dbt_aws_emr_app_url"
XCOM_KEY_EMR_JOB_RUN_URL = "dbt_aws_emr_job_run_url"
XCOM_KEY_EMR_DRIVER_STDERR_URL = "dbt_aws_emr_driver_stderr_url"
XCOM_KEY_EMR_DRIVER_STDOUT_URL = "dbt_aws_emr_driver_stdout_url"

XCOM_KEY_GLUE_SESSION_URL = "dbt_aws_glue_session_url"
XCOM_KEY_GLUE_SESSION_LOGS_URL = "dbt_aws_glue_session_logs_url"

XCOM_KEY_EMR_CLUSTER_URL = "dbt_aws_emr_cluster_url"
XCOM_KEY_EMR_STEP_ID = "dbt_aws_emr_step_id"


# ---------------------------------------------------------------------------
# URL builders -- pure string formatting, zero AWS API calls.
# ---------------------------------------------------------------------------
def _glue_studio_job_url(region: str, job_name: str) -> str:
    """Glue Studio v2 console URL for a job's details page.

    Pattern (2024+)::

        https://<region>.console.aws.amazon.com/gluestudio/home
            ?region=<region>#/job/<job_name>
    """
    return (
        f"https://{region}.console.aws.amazon.com/gluestudio/home"
        f"?region={region}#/job/{urllib.parse.quote(job_name, safe='')}"
    )


def _glue_studio_run_url(region: str, job_name: str, run_id: str) -> str:
    """Glue Studio v2 console URL for a specific JobRun.

    Pattern (2024+) is ``#/job/<name>/run/<run_id>`` -- not the legacy
    ``#/job-run/<run_id>/job/<name>``.
    """
    return (
        f"https://{region}.console.aws.amazon.com/gluestudio/home"
        f"?region={region}#/job/{urllib.parse.quote(job_name, safe='')}/run/{run_id}"
    )


def _glue_session_console_url(region: str, session_id: str) -> str:
    """Glue console URL for an Interactive Session's details page."""
    return (
        f"https://{region}.console.aws.amazon.com/gluestudio/home"
        f"?region={region}#/etl-jobs/sessions/{urllib.parse.quote(session_id, safe='')}"
    )


def _emr_serverless_app_url(region: str, application_id: str) -> str:
    """EMR Studio URL for an EMR Serverless application's dashboard."""
    return (
        f"https://{region}.console.aws.amazon.com/emr/home"
        f"?region={region}#/emrserverless/applications/{application_id}/dashboard"
    )


def _emr_serverless_run_url(region: str, application_id: str, run_id: str) -> str:
    """EMR Studio URL for a specific EMR Serverless job run."""
    return (
        f"https://{region}.console.aws.amazon.com/emr/home"
        f"?region={region}#/emrserverless/applications/"
        f"{application_id}/job-runs/{run_id}"
    )


def _emr_cluster_url(region: str, cluster_id: str) -> str:
    """EMR (EC2 cluster mode) console URL for a cluster's details page.

    Pattern::

        https://<region>.console.aws.amazon.com/emr/home
            ?region=<region>#/clusterDetails/<cluster_id>

    The cluster page has a 'Steps' tab where individual ``StepIds``
    are listed; the lib doesn't deep-link to a specific step because
    AWS doesn't expose a stable URL for that.
    """
    return (
        f"https://{region}.console.aws.amazon.com/emr/home"
        f"?region={region}#/clusterDetails/{cluster_id}"
    )


def _cw_single_stream_url(*, region: str, log_group_name: str, log_stream_name: str) -> str:
    """CloudWatch console deep link to a SINGLE stream.

    Uses the newer ``#logsV2:`` fragment routing with double-URL-encoded
    slashes (``%2F`` -> ``$252F`` via ``$25`` for ``%``).
    """
    encoded_group = urllib.parse.quote(log_group_name, safe="").replace("%", "$25")
    encoded_stream = urllib.parse.quote(log_stream_name, safe="").replace("%", "$25")
    return (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home"
        f"?region={region}#logsV2:log-groups/log-group/{encoded_group}"
        f"/log-events/{encoded_stream}"
    )


def _cw_streams_list_url(*, region: str, log_group_name: str, stream_name_prefix: str) -> str:
    """CloudWatch console list-of-streams view, filtered by prefix.

    For Glue Spark, each JobRun fans out to multiple log streams
    (driver, executors, progress) so the list view is more useful
    than a single-stream link. AWS still serves the legacy URL form
    (``#logStream:group=...``) which natively supports prefix filtering.
    """
    return (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home"
        f"#logStream:group={log_group_name};"
        f"prefix={stream_name_prefix};streamFilter=typeLogStreamPrefix"
    )


# ---------------------------------------------------------------------------
# Context helpers -- safe XCom access, operator field sniffing.
# ---------------------------------------------------------------------------
def _xcom_push_safe(context: dict[str, Any], key: str, value: str) -> None:
    """Push a value to XCom; swallow errors -- audit URLs must never
    break a task. Some Airflow callback contexts don't expose
    ``xcom_push`` (legacy 2.x test runs, shutdown races, etc.).
    """
    ti = context.get("task_instance") or context.get("ti")
    if ti is None or not hasattr(ti, "xcom_push"):
        return
    try:
        ti.xcom_push(key=key, value=value)
    except Exception:  # pragma: no cover -- defensive
        _log.debug("xcom_push(%s) failed; skipping", key, exc_info=True)


def _pull_run_id(context: dict[str, Any], primary_key: str) -> str | None:
    """Resolve a backend run id (Glue JobRunId / EMR Serverless jobRunId)
    from XCom across the operator-version differences.

    Tries the caller's preferred key first, then ``glue_job_run_id``,
    ``return_value``, ``run_id``, ``job_run_id``. Returns the first
    truthy match.
    """
    ti = context.get("task_instance") or context.get("ti")
    if ti is None:
        return None
    seen: set[str] = set()
    for key in (primary_key, "glue_job_run_id", "return_value", "run_id", "job_run_id"):
        if key in seen:
            continue
        seen.add(key)
        try:
            val = ti.xcom_pull(key=key)
        except Exception:  # pragma: no cover
            val = None
        if val:
            return str(val)
    return None


def _pull_script_args(context: dict[str, Any]) -> dict[str, str]:
    """Extract job arguments (dbt's ``--command/--select/--vars``, plus
    any other ``--key=value`` pairs) from the operator the callback
    fires on. Sensitive values (``--env-vars``, ``--vars``, anything
    matching :data:`_REDACTED_ARG_KEYS`) are replaced with a placeholder
    so callers can safely dump the result into the Airflow task log or
    XCom without leaking credentials.

    Glue's ``GlueJobOperator`` uses a ``script_args`` dict; EMR's
    ``EmrServerlessStartJobOperator`` uses ``job_driver.sparkSubmit.
    entryPointArguments`` (a flat ``[--k, v, --k2, v2, ...]`` list). We
    sniff both shapes.
    """
    ti = context.get("task_instance") or context.get("ti")
    if ti is None or not hasattr(ti, "task"):
        return {}
    op = ti.task
    args = getattr(op, "script_args", None)
    if isinstance(args, dict):
        return _redact_script_args({str(k): str(v) for k, v in args.items()})
    job_driver = getattr(op, "job_driver", None)
    if isinstance(job_driver, dict):
        argv = job_driver.get("sparkSubmit", {}).get("entryPointArguments") or []
        flat: dict[str, str] = {}
        i = 0
        while i < len(argv):
            k = str(argv[i])
            if i + 1 < len(argv) and not str(argv[i + 1]).startswith("--"):
                flat[k] = str(argv[i + 1])
                i += 2
            else:
                flat[k] = "true"
                i += 1
        return _redact_script_args(flat)
    return {}


#: Script-arg keys whose values may carry credentials or otherwise
#: sensitive data. When ``_pull_script_args`` surfaces these into the
#: audit log the value is replaced with a placeholder that keeps the
#: key-count intact so ``--env-vars`` still tells the reader how many
#: env vars were set, just not which ones.
_REDACTED_ARG_KEYS: frozenset[str] = frozenset(
    {
        "--env-vars",
        "--vars",
        "--dbt-extra-flags",
    }
)


def _redact_script_args(args: dict[str, str]) -> dict[str, str]:
    """Return a copy of ``args`` with sensitive values redacted.

    For JSON-shaped values (``--env-vars``, ``--vars``) we parse and
    replace with ``"<redacted:N-keys>"`` so the reader can still see
    how many keys were set. Non-JSON values become ``"<redacted>"``.
    Unknown keys are passed through verbatim.

    Never leaks the original value even when called twice: the
    placeholder isn't valid JSON, so a second pass downgrades to
    ``"<redacted>"`` rather than parsing back to secret material.
    """
    import json as _json

    out: dict[str, str] = {}
    for k, v in args.items():
        if k not in _REDACTED_ARG_KEYS:
            out[k] = v
            continue
        try:
            parsed = _json.loads(v)
        except (ValueError, TypeError):
            out[k] = "<redacted>"
            continue
        if isinstance(parsed, dict):
            out[k] = f"<redacted:{len(parsed)}-keys>"
        elif isinstance(parsed, list):
            out[k] = f"<redacted:{len(parsed)}-items>"
        else:
            out[k] = "<redacted>"
    return out


def _pull_glue_operator_fields(context: dict[str, Any]) -> dict[str, str]:
    """Extract static Glue Job configuration from the operator instance.

    Airflow's ``GlueJobOperator`` exposes only a small set of
    per-JobRun knobs as top-level constructor args -- ``job_name``,
    ``iam_role_name`` / ``iam_role_arn``, ``script_location``,
    ``num_of_dpus``, ``aws_conn_id``, ``region_name``,
    ``create_job_kwargs``, ``run_job_kwargs``. The actual Glue sizing
    knobs (``WorkerType``, ``NumberOfWorkers``, ``GlueVersion``,
    ``Timeout``) live inside ``create_job_kwargs`` (defaults baked into
    the Job at create time) and ``run_job_kwargs`` (per-JobRun
    overrides). Per-model overrides are applied on top through
    ``run_job_kwargs``, so the effective config for a StartJobRun call
    is ``run_job_kwargs`` merged over ``create_job_kwargs``.

    We pull both dicts and surface the resolved effective sizing at
    the top of the audit block so operators see WHAT actually runs,
    then dump the raw dicts underneath so the layering is auditable.

    Empty / unset fields are omitted so the dump stays clean.
    """
    ti = context.get("task_instance") or context.get("ti")
    if ti is None or not hasattr(ti, "task"):
        return {}
    op = ti.task

    create_job_kwargs = getattr(op, "create_job_kwargs", None) or {}
    run_job_kwargs = getattr(op, "run_job_kwargs", None) or {}
    if not isinstance(create_job_kwargs, dict):
        create_job_kwargs = {}
    if not isinstance(run_job_kwargs, dict):
        run_job_kwargs = {}

    def _effective(key: str) -> Any:
        """Per-JobRun override wins over the Job's baked-in default.

        This mirrors how Glue itself resolves a StartJobRun: fields in
        ``StartJobRun``'s request override the Job's defaults for that
        one run only.
        """
        if key in run_job_kwargs:
            return run_job_kwargs[key]
        return create_job_kwargs.get(key)

    # Python Shell's per-model MaxCapacity override rides on
    # ``num_of_dpus`` (the operator arg the hook writes into
    # ``MaxCapacity`` -- see GluePythonShellRunner). Fall through if
    # ``run_job_kwargs`` didn't already win.
    effective_max_capacity = _effective("MaxCapacity")
    if effective_max_capacity is None:
        effective_max_capacity = getattr(op, "num_of_dpus", None)

    fields: list[tuple[str, Any]] = [
        ("JobName", getattr(op, "job_name", None)),
        ("IAMRole", getattr(op, "iam_role_name", None) or getattr(op, "iam_role_arn", None)),
        ("WorkerType", _effective("WorkerType")),
        ("NumberOfWorkers", _effective("NumberOfWorkers")),
        ("MaxCapacity", effective_max_capacity),
        ("GlueVersion", _effective("GlueVersion")),
        ("ScriptLocation", getattr(op, "script_location", None)),
        ("Timeout(min)", _effective("Timeout")),
        ("MaxRetries", _effective("MaxRetries")),
        ("SecurityConfig", _effective("SecurityConfiguration")),
        ("Connections", _effective("Connections")),
        ("AwsConnId", getattr(op, "aws_conn_id", None)),
        ("Region", getattr(op, "region_name", None)),
    ]
    # ``create_job_kwargs`` carries DefaultArguments
    # (``--additional-python-modules``, ``--enable-glue-datacatalog``, ...)
    # and other Job-level config not exposed as top-level operator args.
    # Skip keys that are already surfaced above as ``effective`` fields
    # so the dump doesn't repeat them.
    _promoted_keys = {
        "WorkerType",
        "NumberOfWorkers",
        "MaxCapacity",
        "GlueVersion",
        "Timeout",
        "MaxRetries",
        "SecurityConfiguration",
        "Connections",
    }
    for k, v in create_job_kwargs.items():
        if k in _promoted_keys:
            continue
        fields.append((f"CreateJob:{k}", v))
    # ``run_job_kwargs`` carries per-JobRun overrides -- surface every
    # key so per-model overrides (WorkerType, NumberOfWorkers, Timeout,
    # MaxCapacity, ...) are auditable. This is the field the earlier
    # code missed entirely.
    for k, v in run_job_kwargs.items():
        fields.append((f"RunJob:{k}", v))

    out: dict[str, str] = {}
    for k, v in fields:
        if v in (None, "", [], {}):
            continue
        out[k] = str(v)
    return out


def _pull_emr_serverless_operator_fields(context: dict[str, Any]) -> dict[str, str]:
    """Extract EMR Serverless job config from the operator instance.

    Airflow's ``EmrServerlessStartJobOperator`` exposes ``application_id``,
    ``execution_role_arn``, ``job_driver``, ``configuration_overrides``,
    ``config``, ``name``, ``aws_conn_id`` as instance attributes.

    The Spark sizing knobs (driver/executor cores, memory, instances)
    are encoded as ``--conf k=v`` pairs inside
    ``job_driver.sparkSubmit.sparkSubmitParameters`` -- we parse them
    out so per-model overrides show up as individual audit rows.

    Empty / unset fields are omitted.
    """
    ti = context.get("task_instance") or context.get("ti")
    if ti is None or not hasattr(ti, "task"):
        return {}
    op = ti.task

    fields: list[tuple[str, Any]] = [
        ("ApplicationId", getattr(op, "application_id", None)),
        ("ExecutionRole", getattr(op, "execution_role_arn", None)),
        ("JobName", getattr(op, "name", None)),
        ("AwsConnId", getattr(op, "aws_conn_id", None)),
        ("Region", getattr(op, "region_name", None)),
    ]

    # ``config`` carries StartJobRun-level overrides (currently just
    # ``executionTimeoutMinutes`` in dbt-aws, but any StartJobRun API
    # field is allowed here).
    config = getattr(op, "config", None)
    if isinstance(config, dict):
        for k, v in config.items():
            fields.append((f"Config:{k}", v))

    # ``job_driver.sparkSubmit`` holds the entry point + Spark sizing.
    # We surface EntryPoint + the parsed --conf key/value pairs so
    # per-model sizing overrides are auditable at a glance.
    job_driver = getattr(op, "job_driver", None)
    if isinstance(job_driver, dict):
        spark_submit = job_driver.get("sparkSubmit") or {}
        entry_point = spark_submit.get("entryPoint")
        if entry_point:
            fields.append(("EntryPoint", entry_point))
        params = spark_submit.get("sparkSubmitParameters") or ""
        if isinstance(params, str) and params.strip():
            for k, v in _parse_spark_submit_params(params).items():
                fields.append((f"Spark:{k}", v))
            # Also preserve the raw string for anything the parser
            # couldn't slot into a --conf k=v pair (jars, py-files,
            # extra --conf strings the parser missed).
            fields.append(("SparkSubmitParameters", params))

    # ``configuration_overrides`` carries monitoringConfiguration
    # (log destinations) + applicationConfiguration (Spark
    # classification overrides). Surface each top-level key so audit
    # rows stay short but the layering is visible.
    conf_overrides = getattr(op, "configuration_overrides", None)
    if isinstance(conf_overrides, dict):
        for k, v in conf_overrides.items():
            fields.append((f"ConfOverride:{k}", v))

    out: dict[str, str] = {}
    for k, v in fields:
        if v in (None, "", [], {}):
            continue
        out[k] = str(v)
    return out


def _parse_spark_submit_params(params: str) -> dict[str, str]:
    """Parse a ``sparkSubmitParameters`` string into a ``--conf`` dict.

    Input example::

        --conf spark.driver.cores=2 --conf spark.driver.memory=4g

    Returns ``{"spark.driver.cores": "2", "spark.driver.memory": "4g"}``.

    Non ``--conf key=value`` tokens are ignored -- they'll still be
    visible in the raw ``SparkSubmitParameters`` row emitted alongside.
    """
    tokens = params.split()
    out: dict[str, str] = {}
    i = 0
    while i < len(tokens):
        if tokens[i] == "--conf" and i + 1 < len(tokens):
            kv = tokens[i + 1]
            if "=" in kv:
                k, v = kv.split("=", 1)
                out[k] = v
            i += 2
        else:
            i += 1
    return out


def _pull_glue_session_operator_fields(context: dict[str, Any]) -> dict[str, str]:
    """Extract session config from the Glue Interactive Session
    ``GlueSessionCreateOperator`` instance.

    Unlike Jobs, the session's sizing (``WorkerType`` /
    ``NumberOfWorkers``) is stored directly on the custom operator
    class dbt-aws defines (see
    :class:`~dbt_aws.spark.runners.glue_session.GlueSessionCreateOperator`).
    Per-model overrides land on the same attributes at task-build
    time, so a plain ``getattr`` is enough here.

    Empty / unset fields are omitted.
    """
    ti = context.get("task_instance") or context.get("ti")
    if ti is None or not hasattr(ti, "task"):
        return {}
    op = ti.task

    fields: list[tuple[str, Any]] = [
        ("IAMRoleArn", getattr(op, "iam_role_arn", None)),
        ("WorkerType", getattr(op, "worker_type", None)),
        ("NumberOfWorkers", getattr(op, "number_of_workers", None)),
        ("GlueVersion", getattr(op, "glue_version", None)),
        ("IdleTimeout(min)", getattr(op, "idle_timeout_minutes", None)),
        ("Timeout(min)", getattr(op, "timeout_minutes", None)),
        ("AdditionalPythonModules", getattr(op, "additional_python_modules", None)),
        ("AwsConnId", getattr(op, "aws_conn_id", None)),
        ("Region", getattr(op, "region_name", None)),
    ]
    default_args = getattr(op, "default_arguments", None)
    if isinstance(default_args, dict):
        for k, v in default_args.items():
            fields.append((f"DefaultArg:{k}", v))

    out: dict[str, str] = {}
    for k, v in fields:
        if v in (None, "", [], {}):
            continue
        out[k] = str(v)
    return out


# ---------------------------------------------------------------------------
# Audit-block formatter
# ---------------------------------------------------------------------------
def _audit_lines(*, title: str, rows: list[tuple[str, str]]) -> list[str]:
    """Format a structured block for the Airflow task log.

    One INFO record per line so every entry shows up individually in
    the task log UI (the UI renders one row per log record).
    """
    out = ["", "=" * 70, f"dbt-aws audit -- {title}", "=" * 70]
    for k, v in rows:
        out.append(f"  {k:<26} {v}")
    out.append("")
    return out


def _audit_arg_rows(args: dict[str, str], *, max_value_chars: int = 400) -> list[tuple[str, str]]:
    """Format every script arg as an audit row.

    Values longer than ``max_value_chars`` are truncated with an
    ellipsis -- useful for ``--vars`` which can be huge.
    """
    rows: list[tuple[str, str]] = []
    for k in sorted(args):
        v = args[k]
        if len(v) > max_value_chars:
            v = v[: max_value_chars - 3] + "..."
        rows.append((f"{k}:", v))
    return rows


# ---------------------------------------------------------------------------
# Glue Job audit callback (used by GlueSparkRunner + GluePythonShellRunner)
# ---------------------------------------------------------------------------
def make_glue_job_audit_callback(
    *,
    region: str,
    job_name: str,
    log_group_output: str = "/aws-glue/jobs/output",
    log_group_error: str = "/aws-glue/jobs/error",
    job_id_xcom_key: str = DEFAULT_JOB_ID_XCOM_KEY,
    backend_label: str = "AWS Glue Job",
) -> dict[str, Callable[[dict[str, Any]], None]]:
    """Audit-trail callback dict for a Glue Job task.

    Returns ``{"on_execute_callback", "on_success_callback",
    "on_failure_callback"}`` ready to attach to a ``GlueJobOperator``.
    Pre-execute logs the static config + Job console URL; post-execute
    logs the JobRun-id-specific URLs (run page + CloudWatch driver
    stderr/stdout prefixes).

    Args:
        region: AWS region the Job lives in.
        job_name: the Glue Job's exact name.
        log_group_output: CloudWatch log group for Glue stdout
            (``/aws-glue/jobs/output`` by default).
        log_group_error: CloudWatch log group for Glue stderr
            (``/aws-glue/jobs/error`` by default).
        job_id_xcom_key: XCom key for the JobRun id. Falls back to
            several common keys; this is just the preferred one to try.
        backend_label: shown in the audit block header. Override to
            ``"AWS Glue Python Shell"`` etc. when reused.
    """

    def _pre(context: dict[str, Any]) -> None:
        job_url = _glue_studio_job_url(region, job_name)
        _xcom_push_safe(context, XCOM_KEY_GLUE_JOB_URL, job_url)

        rows: list[tuple[str, str]] = [
            ("Backend:", backend_label),
            ("Job:", job_name),
            ("Region:", region),
            ("Job console:", job_url),
        ]
        op_fields = _pull_glue_operator_fields(context)
        if op_fields:
            rows.append(("-- Glue config:", ""))
            for k in sorted(op_fields):
                rows.append((f"  {k}:", op_fields[k]))
        args = _pull_script_args(context)
        if args:
            rows.append(("-- Script args:", ""))
            rows.extend((f"  {k}", v) for k, v in _audit_arg_rows(args))
        for line in _audit_lines(title=f"{backend_label} submission", rows=rows):
            _log.info(line)

    def _post(context: dict[str, Any]) -> None:
        run_id = _pull_run_id(context, primary_key=job_id_xcom_key)
        rows: list[tuple[str, str]] = [
            ("JobRunId:", str(run_id) if run_id else "(unavailable)"),
        ]
        if run_id:
            run_url = _glue_studio_run_url(region, job_name, run_id)
            output_logs_url = _cw_streams_list_url(
                region=region,
                log_group_name=log_group_output,
                stream_name_prefix=run_id,
            )
            error_logs_url = _cw_streams_list_url(
                region=region,
                log_group_name=log_group_error,
                stream_name_prefix=run_id,
            )
            _xcom_push_safe(context, XCOM_KEY_GLUE_JOB_RUN_URL, run_url)
            _xcom_push_safe(context, XCOM_KEY_GLUE_OUTPUT_LOGS_URL, output_logs_url)
            _xcom_push_safe(context, XCOM_KEY_GLUE_ERROR_LOGS_URL, error_logs_url)
            rows.extend(
                [
                    ("JobRun console:", run_url),
                    ("Driver stderr (list):", error_logs_url),
                    ("Driver stdout (list):", output_logs_url),
                    (
                        "Driver stderr (single):",
                        _cw_single_stream_url(
                            region=region,
                            log_group_name=log_group_error,
                            log_stream_name=run_id,
                        ),
                    ),
                    (
                        "Driver stdout (single):",
                        _cw_single_stream_url(
                            region=region,
                            log_group_name=log_group_output,
                            log_stream_name=run_id,
                        ),
                    ),
                ]
            )
        for line in _audit_lines(title=f"{backend_label} result", rows=rows):
            _log.info(line)

    return {
        "on_execute_callback": _pre,
        "on_success_callback": _post,
        "on_failure_callback": _post,
    }


# ---------------------------------------------------------------------------
# EMR Serverless audit callback
# ---------------------------------------------------------------------------
def make_emr_serverless_audit_callback(
    *,
    region: str,
    application_id: str,
    job_id_xcom_key: str = DEFAULT_JOB_ID_XCOM_KEY,
) -> dict[str, Callable[[dict[str, Any]], None]]:
    """Audit-trail callback dict for an EMR Serverless task.

    Same shape as :func:`make_glue_job_audit_callback`: pre-execute
    logs static context + EMR Studio app URL; post-execute logs the
    JobRun URL + CloudWatch deep links to ``SPARK_DRIVER/stderr`` and
    ``SPARK_DRIVER/stdout`` (where dbt's output lands).
    """
    log_group = f"/aws/emr-serverless/applications/{application_id}/jobs"

    def _pre(context: dict[str, Any]) -> None:
        app_url = _emr_serverless_app_url(region, application_id)
        _xcom_push_safe(context, XCOM_KEY_EMR_APP_URL, app_url)

        rows: list[tuple[str, str]] = [
            ("Backend:", "AWS EMR Serverless"),
            ("Application:", application_id),
            ("Region:", region),
            ("App console:", app_url),
        ]
        op_fields = _pull_emr_serverless_operator_fields(context)
        if op_fields:
            rows.append(("-- EMR Serverless config:", ""))
            for k in sorted(op_fields):
                rows.append((f"  {k}:", op_fields[k]))
        args = _pull_script_args(context)
        if args:
            rows.append(("-- Script args:", ""))
            rows.extend((f"  {k}", v) for k, v in _audit_arg_rows(args))
        for line in _audit_lines(title="EMR Serverless submission", rows=rows):
            _log.info(line)

    def _post(context: dict[str, Any]) -> None:
        run_id = _pull_run_id(context, primary_key=job_id_xcom_key)
        rows: list[tuple[str, str]] = [
            ("JobRunId:", str(run_id) if run_id else "(unavailable)"),
        ]
        if run_id:
            run_url = _emr_serverless_run_url(region, application_id, run_id)
            stderr_url = _cw_single_stream_url(
                region=region,
                log_group_name=log_group,
                log_stream_name=f"{run_id}/SPARK_DRIVER/stderr",
            )
            stdout_url = _cw_single_stream_url(
                region=region,
                log_group_name=log_group,
                log_stream_name=f"{run_id}/SPARK_DRIVER/stdout",
            )
            _xcom_push_safe(context, XCOM_KEY_EMR_JOB_RUN_URL, run_url)
            _xcom_push_safe(context, XCOM_KEY_EMR_DRIVER_STDERR_URL, stderr_url)
            _xcom_push_safe(context, XCOM_KEY_EMR_DRIVER_STDOUT_URL, stdout_url)
            rows.extend(
                [
                    ("JobRun console:", run_url),
                    ("Driver stderr:", stderr_url),
                    ("Driver stdout:", stdout_url),
                ]
            )
        for line in _audit_lines(title="EMR Serverless result", rows=rows):
            _log.info(line)

    return {
        "on_execute_callback": _pre,
        "on_success_callback": _post,
        "on_failure_callback": _post,
    }


# ---------------------------------------------------------------------------
# Glue Interactive Session audit callback
# ---------------------------------------------------------------------------
def make_glue_session_audit_callback(
    *,
    region: str,
    session_id_xcom_task_id: str | None = None,
    log_group: str = "/aws-glue/sessions/output",
) -> dict[str, Callable[[dict[str, Any]], None]]:
    """Audit-trail callback dict for a Glue Interactive Session task.

    Unlike Jobs, sessions don't have a single ``CreateSession`` ->
    ``RunStatement`` -> ``DeleteSession`` URL. The session id is the
    identifier; the same id reappears in CloudWatch and the Glue
    console. This callback emits one URL block per task lifecycle event.

    Args:
        region: AWS region.
        session_id_xcom_task_id: if set, pull the session id from this
            upstream task's ``return_value`` XCom. ``None`` falls back
            to the default key search.
        log_group: CloudWatch log group for Glue session output.
    """

    def _pull_session_id(context: dict[str, Any]) -> str | None:
        ti = context.get("task_instance") or context.get("ti")
        if ti is None:
            return None
        # Prefer the explicit upstream task's return_value when given.
        if session_id_xcom_task_id is not None:
            try:
                val = ti.xcom_pull(task_ids=session_id_xcom_task_id)
                if val:
                    return str(val)
            except Exception:  # pragma: no cover
                pass
        # Fall back to whatever this task itself pushed (CreateSession
        # operator returns the session id).
        return _pull_run_id(context, primary_key="return_value")

    def _pre(context: dict[str, Any]) -> None:
        rows: list[tuple[str, str]] = [
            ("Backend:", "AWS Glue Interactive Session"),
            ("Region:", region),
        ]
        # On the CreateSession task the id isn't known yet; on
        # downstream tasks (statement / delete) it is.
        session_id = _pull_session_id(context)
        if session_id:
            session_url = _glue_session_console_url(region, session_id)
            _xcom_push_safe(context, XCOM_KEY_GLUE_SESSION_URL, session_url)
            rows.append(("Session:", session_id))
            rows.append(("Session console:", session_url))
        # Session-level config only lives on the CreateSession
        # operator; on statement / delete tasks the helper returns
        # an empty dict which we skip.
        op_fields = _pull_glue_session_operator_fields(context)
        if op_fields:
            rows.append(("-- Session config:", ""))
            for k in sorted(op_fields):
                rows.append((f"  {k}:", op_fields[k]))
        args = _pull_script_args(context)
        if args:
            rows.append(("-- Statement args:", ""))
            rows.extend((f"  {k}", v) for k, v in _audit_arg_rows(args))
        for line in _audit_lines(title="Glue Session submission", rows=rows):
            _log.info(line)

    def _post(context: dict[str, Any]) -> None:
        session_id = _pull_session_id(context)
        rows: list[tuple[str, str]] = [
            ("Session:", str(session_id) if session_id else "(unavailable)"),
        ]
        if session_id:
            session_url = _glue_session_console_url(region, session_id)
            logs_url = _cw_streams_list_url(
                region=region,
                log_group_name=log_group,
                stream_name_prefix=session_id,
            )
            _xcom_push_safe(context, XCOM_KEY_GLUE_SESSION_URL, session_url)
            _xcom_push_safe(context, XCOM_KEY_GLUE_SESSION_LOGS_URL, logs_url)
            rows.append(("Session console:", session_url))
            rows.append(("Session logs (list):", logs_url))
        for line in _audit_lines(title="Glue Session result", rows=rows):
            _log.info(line)

    return {
        "on_execute_callback": _pre,
        "on_success_callback": _post,
        "on_failure_callback": _post,
    }


# ---------------------------------------------------------------------------
# EMR (EC2 cluster mode) Step audit callback
# ---------------------------------------------------------------------------
def make_emr_cluster_step_audit_callback(
    *,
    region: str,
    cluster_id: str,
    step_id_xcom_key: str = DEFAULT_JOB_ID_XCOM_KEY,
) -> dict[str, Callable[[dict[str, Any]], None]]:
    """Audit-trail callback dict for an EMR (EC2 cluster) step task.

    Pre-execute logs the cluster console URL + script args. Post-execute
    logs the StepId (pulled from XCom). Unlike Glue / EMR Serverless,
    AWS doesn't expose a stable URL for an individual step -- users
    navigate to the cluster's 'Steps' tab and find their step there.

    Args:
        region: AWS region.
        cluster_id: EMR cluster id (``j-XXXXX``). Resolved per-task by
            the runner if templated.
        step_id_xcom_key: XCom key holding the AWS step id
            (``EmrAddStepsOperator`` pushes it).
    """

    def _pre(context: dict[str, Any]) -> None:
        cluster_url = _emr_cluster_url(region, cluster_id)
        _xcom_push_safe(context, XCOM_KEY_EMR_CLUSTER_URL, cluster_url)

        rows: list[tuple[str, str]] = [
            ("Backend:", "AWS EMR (EC2 cluster) step"),
            ("Cluster:", cluster_id),
            ("Region:", region),
            ("Cluster console:", cluster_url),
        ]
        ti = context.get("task_instance") or context.get("ti")
        if ti is not None and hasattr(ti, "task"):
            steps = getattr(ti.task, "steps", None) or []
            if steps and isinstance(steps[0], dict):
                hjs = steps[0].get("HadoopJarStep", {})
                step_args = hjs.get("Args")
                if step_args:
                    rows.append(("-- Step args:", ""))
                    for i, arg in enumerate(step_args):
                        s = str(arg)
                        if len(s) > 400:
                            s = s[:397] + "..."
                        rows.append((f"  argv[{i}]", s))
        for line in _audit_lines(title="EMR step submission", rows=rows):
            _log.info(line)

    def _post(context: dict[str, Any]) -> None:
        step_id = _pull_run_id(context, primary_key=step_id_xcom_key)
        cluster_url = _emr_cluster_url(region, cluster_id)
        rows: list[tuple[str, str]] = [
            ("Cluster:", cluster_id),
            ("StepId:", str(step_id) if step_id else "(unavailable)"),
            ("Cluster console:", cluster_url),
        ]
        if step_id:
            _xcom_push_safe(context, XCOM_KEY_EMR_STEP_ID, str(step_id))
        for line in _audit_lines(title="EMR step result", rows=rows):
            _log.info(line)

    return {
        "on_execute_callback": _pre,
        "on_success_callback": _post,
        "on_failure_callback": _post,
    }


# ---------------------------------------------------------------------------
# Callback merging helper -- moved here from the old audit.py since
# runners use it together with the audit callbacks.
# ---------------------------------------------------------------------------
def merge_callbacks(
    *layers: Callable[..., None] | list[Callable[..., None]] | None,
) -> list[Callable[..., None]] | None:
    """Flatten and concatenate multiple callback layers, preserving
    order. Returns ``None`` if nothing was supplied -- so we don't
    shadow the operator's default with an empty list.

    User callbacks fire first, audit callbacks last; ordering preserved
    by passing user layers before audit ones.
    """
    out: list[Callable[..., None]] = []
    for layer in layers:
        if layer is None:
            continue
        if callable(layer):
            out.append(layer)
        else:
            out.extend(layer)
    return out or None


__all__ = [
    "XCOM_KEY_EMR_APP_URL",
    "XCOM_KEY_EMR_CLUSTER_URL",
    "XCOM_KEY_EMR_DRIVER_STDERR_URL",
    "XCOM_KEY_EMR_DRIVER_STDOUT_URL",
    "XCOM_KEY_EMR_JOB_RUN_URL",
    "XCOM_KEY_EMR_STEP_ID",
    "XCOM_KEY_GLUE_ERROR_LOGS_URL",
    "XCOM_KEY_GLUE_JOB_RUN_URL",
    "XCOM_KEY_GLUE_JOB_URL",
    "XCOM_KEY_GLUE_OUTPUT_LOGS_URL",
    "XCOM_KEY_GLUE_SESSION_LOGS_URL",
    "XCOM_KEY_GLUE_SESSION_URL",
    "make_emr_cluster_step_audit_callback",
    "make_emr_serverless_audit_callback",
    "make_glue_job_audit_callback",
    "make_glue_session_audit_callback",
    "merge_callbacks",
]
