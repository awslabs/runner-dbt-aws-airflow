# Multi-profile / multi-target DAG

Run one dbt project across two dbt targets (or profiles) from a single DAG.
Typical case: some models must use `dbt-glue` (talks to a live Glue
Interactive Session) while the rest use `dbt-spark` inside a Glue Spark Job.

## Setup

### 1. `profiles.yml` with two targets

```yaml
my_project:
  target: dev
  outputs:
    spark_dev:                      # used by the Spark JobRunner
      type: spark
      method: session
      # ... etc
    shell_dev:                      # used by the Python-Shell runner
      type: duckdb
      path: ":memory:"
      # ... etc
```

### 2. Runner-level defaults

```python
from dbt_aws.spark.runners import GlueSparkRunner
from dbt_aws.nonspark.runners import GluePythonShellRunner

spark = GlueSparkRunner(
    mode="create",
    iam_role_name="Glue-Job-Role",
    # NEW --------------------------------------------
    target="spark_dev",             # every task from this runner
                                    # gets --target spark_dev
    # profile_name unset -> dbt uses the profile named in
    # dbt_project.yml (`profile: my_project`)
)

shell = GluePythonShellRunner(
    mode="create",
    iam_role_name="Glue-Job-Role",
    # NEW --------------------------------------------
    target="shell_dev",
    profile_name="my_project",      # explicit --profile my_project
)
```

### 3. Compose them in one DAG

Two options for picking the runner per model.

**Option A -- bulk by tag:**

```python
from dbt_aws.common.builder import DbtDag

dag = DbtDag(
    dag_id="daily",
    project=...,
    runners={"spark": spark, "shell": shell},
    default_runner="spark",
    tag_runners={"landing": "shell"},      # tag "landing" -> shell runner
    project_archive_s3="s3://...",
    target="spark_dev",                    # DAG-level fallback
)
```

Every model tagged `landing` runs on the shell runner with
`--target shell_dev --profile my_project`. Everything else runs on
Spark with `--target spark_dev` and no `--profile` flag.

**Option B -- per model in the dbt project:**

```sql
-- models/landing/redshift_ingest.sql
{{ config(
    meta={"stratus": {"runner": "shell",
                       "target": "shell_dev",
                       "profile_name": "my_project"}}
) }}
select 1 as n
```

The `meta.stratus.*` block is read at DAG-parse time; no DAG code
change needed to move a model between runners/targets.

## Precedence

For any node, the effective `--target` and `--profile-name` come from
the first layer below that sets a value:

```
1. overrides={<uid>: {"target": ..., "profile_name": ...}}
2. meta.stratus.target / .profile_name    (in the dbt model YAML/SQL)
3. tag_targets[<tag>] / tag_profiles[<tag>] (on DbtDag)
4. runner.target / runner.profile_name    (on the runner constructor)
5. DbtDag(target=...)                     (DAG-level; profile has no DAG-level knob)
```

The two ladders resolve independently. A model can pick its `target` via
`tag_targets` while still using its runner's default `profile_name`.

## Verifying what got sent

Every `--target` / `--profile-name` value ends up in the Glue
JobRun's `Arguments`. Check with `aws glue get-job-runs`:

```console
$ aws glue get-job-runs --job-name my-glue-job --max-results 1 \
    --query 'JobRuns[0].Arguments' --output json
{
  "--command": "run",
  "--select": "landing_model",
  "--target": "shell_dev",
  "--profile-name": "my_project",
  ...
}
```

The `worker_entrypoint.py` script consumes these flags and translates them
to `dbt run --target ... --profile ...` on the worker.

## Parse-time visibility

When either feature is used, the builder logs the distribution at DAG-parse
time (visible in the Airflow scheduler / dag-processor log):

```
INFO  dbt_aws.common.builder  runner distribution: shell=2, spark=3
INFO  dbt_aws.common.builder  target distribution: shell_dev=2, spark_dev=3
```

A `WARNING` fires when a `tag_targets` or `tag_profiles` entry references
a tag no selected node carries -- typo guard.

## Runnable examples

Two example DAGs demonstrate the profile/target ladder end-to-end:

* `dag_smoke_profile_target_ladder.py` -- local-only DAG that
  exercises every layer of the ladder and asserts each task's
  `script_args` at DAG-parse time. Safe to load without AWS creds.
* `dag_smoke_profile_target_ladder_real.py` -- same feature set,
  hitting real Glue Spark + Glue Python Shell. Requires AWS
  credentials plus the IAM / S3 setup described in
  [Deployment prerequisites](../concepts/deployment.md#iam-and-vpc-prerequisites).

## When NOT to use this

* Every model runs the same profile and target -- just use
  `DbtDag(target=...)` and let dbt pick the profile from `dbt_project.yml`.
* Different runners already carry different `target=` in `runners.yml` --
  no per-tag / per-model overrides needed.

The feature is opt-in: DAGs that don't set `target=` / `profile_name=` on any
layer see byte-identical behaviour, no extra log lines, no new args
in the worker script.
