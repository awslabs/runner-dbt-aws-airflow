# Quickstart — Glue Spark Job

See [Getting started](../getting-started.md) for the canonical 5-minute walkthrough on
a single `GlueSparkRunner`.

This page covers Glue-Spark-specific tweaks once you have the basic DAG working.

## Per-model sizing

```python
GlueSparkRunner(
    mode="create",
    iam_role_name="GlueRole",
    deploy_bucket="my-glue-bucket",
    deploy_prefix="dbt-aws",
    create_job_kwargs={
        # Defaults for every model
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        # ...
    },
)

DbtDag(
    runners={"spark": runner},
    default_runner="spark",
    overrides={
        # One huge aggregate gets a beefier job
        "model.proj.huge_agg": {"worker_type": "G.4X", "number_of_workers": 16},
        # One nightly refresh model gets `--full-refresh`
        "model.proj.scd_history": {"full_refresh": True},
    },
    ...
)
```

## attach vs create

| Mode | When | Behaviour |
|---|---|---|
| `mode="create"` | You want the lib to manage the Glue Job lifecycle. | `glue:CreateJob` on first run (or `UpdateJob` if `update_config=True`). Idempotent. |
| `mode="attach"` | The Glue Job already exists (managed by Terraform / CDK / IaC). | The lib only calls `glue:StartJobRun`. No `CreateJob` / `UpdateJob` ever. |

```python
# attach: lib trusts the Job exists with the right script + DefaultArguments
GlueSparkRunner(
    mode="attach",
    # No need for deploy_bucket / iam_role_name / create_job_kwargs
)
```

## Concurrent runs

`MaxConcurrentRuns: 1` is the per-Job default. Bump it to absorb dev re-triggers:

```python
create_job_kwargs={
    "ExecutionProperty": {"MaxConcurrentRuns": 5},
    # ...
}
```

Without this, a fresh DAG run colliding with a still-in-flight previous run fails with
`ConcurrentRunsExceededException`.

## Logs

Every Glue Job run shows up in CloudWatch under:

```
/aws-glue/jobs/output    -- stdout (dbt log lines)
/aws-glue/jobs/error     -- stderr (Python tracebacks)
```

The Airflow operator log also embeds a clickable link to the Glue Job run in the AWS console.

## Common errors

| Symptom | Likely cause |
|---|---|
| `ConcurrentRunsExceededException` | `MaxConcurrentRuns: 1` collision (see above). |
| `pip install failed: runner-dbt-aws-airflow` | PyPI extra-index URL missing from `--python-modules-installer-option`. |
| Glue Job created but no `--additional-python-modules` | `update_config: false`; pre-existing Job missing the field. Bump to `update_config: true` or recreate manually. |
| `An error occurred (AccessDeniedException)` | The IAM role lacks `s3:GetObject` on the archive bucket, or `cloudwatch:PutLogEvents`. |
