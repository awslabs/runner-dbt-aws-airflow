# Runner constructor reference

Complete parameter reference for every concrete `Runner` class.

The five runner shapes all take one dbt node and return one deferrable
Airflow operator. Each accepts a common set of dbt-side kwargs plus
compute-shape kwargs specific to its AWS service. Full details per
class below.

Cross-cutting kwargs (accepted by every runner):

| Kwarg | Type | Default | Meaning |
|---|---|---|---|
| `aws_conn_id` | `str` | `"aws_default"` | Airflow connection id. |
| `region_name` | `str \| None` | `None` | AWS region; `None` lets the connection decide. |
| `deferrable` | `bool` | `True` | Defer to the Triggerer instead of pinning the Airflow worker. Keep as `True` unless you have a specific reason. |
| `with_deps` | `bool` | `True` | Have the worker run `dbt deps` (into `dbt_packages/`) before invoking dbt. |
| `full_refresh` | `bool` | `False` | Pass `--full-refresh` to every dbt invocation. |
| `vars_json` | `str \| None` | `None` | Passed as `dbt run --vars '{...}'`. |
| `state_s3` | `s3:// URI` | `None` | Sync a state directory from S3 for `dbt --state` / `--defer`. |
| `defer` | `bool` | `False` | Pass `--defer` to dbt. Requires `state_s3`. |
| `profile_name` | `str \| None` | `None` | dbt `--profile` (falls back to `dbt_project.yml`). |
| `target` | `str \| None` | `None` | dbt `--target` (falls back to `DbtDag(target=...)`). |
| `env_vars_json` | `str \| None` | `None` | Env vars serialised as JSON, applied on the worker. |
| `dbt_extra_flags` | `list[str] \| None` | `None` | Raw dbt flags forwarded verbatim. |
| `upload_artefacts_s3_prefix` | `s3:// URI` | `None` | Post-run upload of `target/` to `<prefix>/<unique_id>/`. |
| `openlineage` | [`OpenLineageConfig`](../concepts/lineage.md) | `None` | Enable OpenLineage emission. See [Concepts → Lineage](../concepts/lineage.md). |
| `resource_tags` | `dict[str, str] \| None` | `None` | AWS resource tags for the AWS resource this runner creates (mode='create' only; see [Top-level `resource_tags:`](runner-config-yaml.md#top-level-resource_tags)). |
| `on_execute_callback` / `on_success_callback` / `on_failure_callback` | callable or list | `None` | Airflow callbacks. |
| `audit_log` | `bool` | `True` | Append the built-in audit-log callback to every task. |

The tables below only list runner-specific kwargs; refer back here for
the cross-cutting set.

## `GlueSparkRunner`

Import: `from dbt_aws.spark.runners import GlueSparkRunner`

Runs each dbt node as a Glue Spark **JobRun** via `GlueJobOperator`.

| Kwarg | Type | Default | Meaning |
|---|---|---|---|
| `job_name` | `str \| None` | `None` | Name of the Glue Job. Required in `attach` mode. |
| `mode` | `"attach" \| "create"` | `"attach"` | `attach`: Job pre-exists. `create`: lib creates the Job via `create_job`. |
| `name_prefix` / `job_name_template` | `str` | `None` | Auto-name derivation when `job_name` is omitted. |
| `iam_role_name` | `str \| None` | `None` | IAM role **name** (not ARN). Required in `create` mode. |
| `script_location` | `s3:// URI \| None` | `None` | Entry-script URI. Mutually exclusive with `deploy_bucket`. |
| `deploy_bucket` | `str \| None` | `None` | Have the lib upload the bundled worker entry-script to `s3://{deploy_bucket}/{deploy_prefix}/`. |
| `deploy_prefix` | `str` | `"dbt-aws"` | S3 prefix under `deploy_bucket`. |
| `create_job_kwargs` | `dict` | `{}` | Forwarded to `GlueJobOperator.create_job_kwargs` (e.g. `SecurityConfiguration`, `DefaultArguments`, `ExecutionProperty`). Merged with the runtime-sizing dict this runner builds; caller keys win on conflict. `Tags` inside merges per-key with `resource_tags` (caller wins). |
| `update_config` | `bool` | `False` | When `create` mode + Job exists, update its config to match. |
| `worker_type` | `str` | `"G.1X"` | `G.1X` / `G.2X` / `G.4X` / `G.8X`. |
| `number_of_workers` | `int` | `2` | Worker (DPU) count. |
| `timeout_minutes` | `int` | `60` | Per-JobRun timeout. |
| `spark_conf` | `dict[str, str] \| None` | `None` | Runner-level Spark config baked into the Job's `DefaultArguments["--conf"]` at create/update time (`mode='create'`). Per-model / per-tag entries in `overrides:` shallow-merge on top and land in the per-JobRun `--conf` argument. Runtime configs only (`spark.sql.*`, `spark.default.parallelism`); JVM-start configs like `spark.jars.packages` must live under `create_job_kwargs.DefaultArguments` instead. See [Example: `spark_conf` vs `--conf`](examples/spark-conf-demo.md) for a full walkthrough. |
| `spark_conf_replace` | `dict[str, str] \| None` | `None` | Escape-hatch counterpart to `spark_conf`. When set on any layer, REPLACES the merged `spark_conf` from all lower layers instead of shallow-merging on top. Use when a single model needs a totally different Spark profile. |
| `glue_version` | `str` | `"5.0"` | Glue runtime version. |
| `waiter_delay` | `int` | `30` | Seconds between Triggerer polls. |
| `waiter_max_attempts` | `int` | `120` | Max polls before timing out. |
| `stop_job_run_on_kill` | `bool` | `True` | Cancel the JobRun when the Airflow task is killed. |
| `verbose` | `bool` | `False` | Stream CloudWatch logs into the Airflow task log. |
| `concurrent_runs` | `"allow" \| "join" \| "queue"` | `"allow"` | How the runner handles a duplicate run already in flight. |

Overrides ([`GlueSparkOverride`](runner-overrides.md#gluesparkoverride)): `job_name`,
`job_name_template`, `mode`, `iam_role_name`, `script_location`,
`worker_type`, `number_of_workers`, `timeout_minutes`, `full_refresh`,
`vars_json`, `profile_name`, `target`, `command`, `concurrent_runs`.

## `GluePythonShellRunner`

Import: `from dbt_aws.nonspark.runners import GluePythonShellRunner`

Runs each dbt node as a Glue **Python Shell** JobRun. Lighter than
Spark; capped at 1 DPU. Useful for dbt-athena / small transforms.

Runner-specific kwargs:

| Kwarg | Type | Default | Meaning |
|---|---|---|---|
| `job_name`, `mode`, `name_prefix`, `job_name_template`, `iam_role_name`, `script_location`, `deploy_bucket`, `deploy_prefix`, `create_job_kwargs`, `update_config` | as `GlueSparkRunner` | — | Same semantics. |
| `max_capacity` | `float` | `1.0` | DPUs — Glue Python Shell accepts `0.0625` or `1.0` only. |
| `timeout_minutes` | `int` | `60` | Per-JobRun timeout. |
| `glue_version` | `str` | `"3.0"` | Glue Python Shell runtime. |
| `python_version` | `str` | `"3.9"` | Glue 3.0 supports 3.9 (default) or 2.7. See [Troubleshooting](../troubleshooting.md#glue-python-shell-30-install-pipeline). |
| `concurrent_runs` | `"allow" \| "join" \| "queue"` | `"allow"` | Duplicate-run policy. |

Overrides ([`GluePythonShellOverride`](runner-overrides.md#gluepythonshelloverride)):
`job_name`, `job_name_template`, `mode`, `iam_role_name`,
`script_location`, `max_capacity`, `timeout_minutes`, `full_refresh`,
`vars_json`, `profile_name`, `target`, `command`, `concurrent_runs`.

## `GlueInteractiveSessionRunner`

Import: `from dbt_aws.spark.runners import GlueInteractiveSessionRunner`

Runs dbt models as **statements** against a Glue Interactive Session
(long-lived Spark session). Two modes: `reusable=True` shares one
session across every model in the DAG; `reusable=False` creates a
fresh per-node session.

Runner-specific kwargs:

| Kwarg | Type | Default | Meaning |
|---|---|---|---|
| `iam_role_arn` | `str` | — required — | IAM role ARN the session runs under. |
| `reusable` | `bool` | `True` | Share one session across the DAG run (warm) or spawn per-node. |
| `session_id_prefix` | `str` | `"dbt-aws"` | Prefix for the auto-generated `session_id`. |
| `additional_python_modules` | `str` | `""` | Comma-separated pip requirement string, injected as `--additional-python-modules`. |
| `default_arguments` | `dict[str, str]` | `{}` | Extra `DefaultArguments` on `create_session`. |
| `glue_version` | `str` | `"5.0"` | Glue Session runtime. |
| `worker_type` | `str` | `"G.1X"` | Worker type. |
| `number_of_workers` | `int` | `2` | Worker count. |
| `idle_timeout_minutes` | `int` | `30` | Session idle timeout. |
| `timeout_minutes` | `int` | `60` | Statement timeout. |
| `waiter_delay` | `int` | `15` | Polling interval. |
| `waiter_max_attempts_session` | `int` | `40` | Max polls to READY. |
| `waiter_max_attempts_statement` | `int` | `240` | Max polls per statement. |

Overrides ([`GlueInteractiveSessionOverride`](runner-overrides.md#glueinteractivesessionoverride)):
`full_refresh`, `vars_json`, `profile_name`, `target`, `command`,
`timeout_minutes`. Session sizing (`worker_type`, `number_of_workers`)
is fixed on the runner — declare two runners for two sizes and route
via `overrides[tag.<t>].runner`.

## `EmrServerlessRunner`

Import: `from dbt_aws.spark.runners import EmrServerlessRunner`

Runs each dbt node as an **EMR Serverless** job on a shared
application. Two modes: `attach` (application pre-exists), `create`
(lib creates + tears down the application per DAG run).

Runner-specific kwargs:

| Kwarg | Type | Default | Meaning |
|---|---|---|---|
| `application_id` | `str \| None` | `None` | EMR Serverless application id. Required in `attach` mode. |
| `application_name` / `application_name_template` / `name_prefix` | `str` | `None` | Auto-name derivation when `application_id` is omitted. |
| `release_label` | `str \| None` | `None` | EMR release label (e.g. `"emr-7.5.0"`). Required in `create` mode. |
| `execution_role_arn` | `str` | — required — | IAM role ARN. |
| `mode` | `"attach" \| "create"` | `"attach"` | Same semantics as GlueSparkRunner. |
| `reusable` | `bool` | `True` | `True`: one application shared. `False`: per-node application create+delete. Requires `create` mode. |
| `script_location` / `deploy_bucket` / `deploy_prefix` | | | Entry-script placement (same shape as GlueSparkRunner). Required in both modes here. |
| `create_application_kwargs` | `dict` | `{}` | Merged into the `create_application` call. |
| `driver_cores`, `driver_memory`, `executor_cores`, `executor_memory`, `num_executors` | | `2 / 4g / 2 / 4g / 2` | Spark sizing passed as `--conf spark.*` flags. |
| `timeout_minutes` | `int` | `60` | Per-job timeout. |
| `spark_submit_parameters_extra` | `str \| None` | `None` | Extra `--conf` fragments appended to the spark-submit command. |
| `configuration_overrides` | `dict` | `None` | EMR Serverless `configurationOverrides` (monitoring / classification / properties). |
| `waiter_delay`, `waiter_max_attempts` | `int` | `30 / 120` | Triggerer polling. |
| `cancel_on_kill` | `bool` | `True` | Cancel the EMR Serverless job when Airflow task is killed. |

Overrides ([`EmrServerlessOverride`](runner-overrides.md#emrserverlessoverride)):
`full_refresh`, `vars_json`, `profile_name`, `target`, `command`.

## `EmrClusterStepRunner`

Import: `from dbt_aws.spark.runners import EmrClusterStepRunner`

Runs each dbt node as a **Spark step** on an EMR classic cluster
(EC2). Two modes: `attach` (cluster pre-exists via `cluster_id=`),
`create` (lib runs `RunJobFlow` at DAG start).

Runner-specific kwargs:

| Kwarg | Type | Default | Meaning |
|---|---|---|---|
| `cluster_id` | `str \| None` | `None` | Existing EMR cluster id (e.g. `j-XXXX`). Required in `attach` mode. |
| `mode` | `"attach" \| "create"` | `"attach"` | Same semantics as GlueSparkRunner. |
| `reusable` | `bool` | `True` | `True`: share one cluster across the DAG. `False`: per-node cluster (requires `create`). |
| `job_flow_overrides` | `dict` | `{}` | Full `RunJobFlow` spec (required in `create` mode). `Tags` inside merges per-Key with `resource_tags`. |
| `auto_terminate` | `bool` | `True` | Auto-terminate the cluster when steps finish (create mode). |
| `script_location` / `deploy_bucket` / `deploy_prefix` | | | Entry-script placement. |
| `deploy_mode` | `"cluster" \| "client"` | `"cluster"` | `spark-submit --deploy-mode`. `client` recommended for dbt (no SparkContext needed). |
| `action_on_failure` | `"TERMINATE_CLUSTER" \| "TERMINATE_JOB_FLOW" \| "CONTINUE" \| "CANCEL_AND_WAIT"` | `"CONTINUE"` | EMR step failure action. |
| `driver_cores`, `driver_memory`, `executor_cores`, `executor_memory`, `num_executors` | | `2 / 4g / 2 / 4g / 2` | Spark sizing (spark-submit `--conf`). |
| `spark_extra_conf` | `list[str]` | `None` | Extra `--conf` fragments. |
| `pyspark_python` | `str \| None` | `"/usr/bin/python3.11"` | Path to the PySpark Python executable. |
| `step_execution_role_arn` | `str \| None` | `None` | Runtime IAM role for the step. |
| `waiter_delay`, `waiter_max_attempts` | `int` | `30 / 120` | Triggerer polling. |

Overrides ([`EmrClusterStepOverride`](runner-overrides.md#emrclusterstepoverride)):
`driver_cores`, `driver_memory`, `executor_cores`, `executor_memory`,
`num_executors`, `full_refresh`, `vars_json`, `profile_name`, `target`,
`command`.

## Cross-runner: `resource_tags`

Every runner accepts `resource_tags: dict[str, str] | None`. Tags are
applied at the AWS `create_*` call time and follow the AWS resource
the runner creates:

| Runner | AWS API | Tag kwarg shape |
|---|---|---|
| `GlueSparkRunner` | `glue:CreateJob` | `Tags: dict[str, str]` on `create_job_kwargs` |
| `GluePythonShellRunner` | `glue:CreateJob` | `Tags: dict[str, str]` on `create_job_kwargs` |
| `GlueInteractiveSessionRunner` | `glue:CreateSession` | `Tags: dict[str, str]` (session-scoped) |
| `EmrServerlessRunner` | `emr-serverless:CreateApplication` | `tags: dict[str, str]` (application-scoped) |
| `EmrClusterStepRunner` | `emr:RunJobFlow` | `Tags: [{Key, Value}, ...]` on `job_flow_overrides` |

Semantics:

- Applies to `mode='create'` only. `mode='attach'` runners silently
  ignore `resource_tags` — the AWS resource pre-exists and IaC owns
  its tags.
- Runner-level knob only. Per-model and per-tag `resource_tags` are
  intentionally not supported — route to a different runner if you
  need different tag sets.
- Caller `Tags` inside `create_job_kwargs` / `job_flow_overrides`
  merges per key over `resource_tags`. Caller wins on conflict.
- YAML users get a top-level `resource_tags:` key that cascades to
  every runner in the file. See
  [YAML → Top-level `resource_tags:`](runner-config-yaml.md#top-level-resource_tags).
- Validation at DAG-parse: keys/values must be non-empty strings,
  key ≤ 128 chars, value ≤ 256 chars, no `aws:` prefix (AWS reserved).
  Violations raise `ResourceTagsError`.

## Overrides cheatsheet

For per-node customization, see [Runner overrides](runner-overrides.md).
The routing side (`overrides[tag.<t>]`, `mode: group | single`,
`name:` prefix) is documented in
[Concepts → Routing](../concepts/routing.md) and
[YAML config](runner-config-yaml.md).
