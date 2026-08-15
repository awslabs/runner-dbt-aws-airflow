# Quickstart — Glue Interactive Session

Glue Interactive Sessions ([details](../concepts/runners.md#glue-interactive-session-glueinteractivesessionrunner))
come in two modes; pick based on the workload shape.

## Warm session (shared)

One `CreateSession` for the whole DAG, then `RunStatement` per model.

```python
from dbt_aws.spark.runners import GlueInteractiveSessionRunner

warm = GlueInteractiveSessionRunner(
    iam_role_arn="arn:aws:iam::123:role/AWSGlueServiceRole",
    reusable=True,                              # one session shared
    session_id_prefix="dbt-aws-warm",
    additional_python_modules="dbt-aws,dbt-core==1.11.11,dbt-duckdb==1.10.1",
    default_arguments={
    },
    glue_version="5.0",
    worker_type="G.1X",
    number_of_workers=2,
    idle_timeout_minutes=15,
    timeout_minutes=45,
    upload_artefacts_s3_prefix="s3://my-glue-bucket/dbt-aws/session_warm/",
)

dag = DbtDag(
    runner=warm,
    # ...
)
```

The builder injects a `dbt_aws_glue_session__setup` task at the DAG head (calls
`CreateSession`) and a `dbt_aws_glue_session__teardown` at the tail (`DeleteSession`).
Every dbt node in between submits a statement to the shared session.

## Per-node session (ephemeral)

```python
per_node = GlueInteractiveSessionRunner(
    iam_role_arn="arn:aws:iam::123:role/AWSGlueServiceRole",
    reusable=False,                             # one session per model
    session_id_prefix="dbt-aws-perNode",
    idle_timeout_minutes=5,
    timeout_minutes=15,
    # ... same other kwargs as warm
)
```

Each model gets its own `(setup, statement, teardown)` triplet.

## Combine warm + per-node in one DAG

A common pattern: warm session carries the medallion chain, per-node session isolates a
single sensitive seed.

```python
dag = DbtDag(
    runners={"warm": warm, "iso": per_node},
    default_runner="warm",
    overrides={
        "seed.proj.audit_log": {"runner": "iso"},
    },
    # ...
)
```

## Picking sizes

`worker_type` and `number_of_workers` are properties of the *session*, not the
statement. To run different models on different sizes, declare two separate runners and
route between them via [`tag_runners`](../concepts/routing.md) or `overrides`.

```python
small = GlueInteractiveSessionRunner(reusable=True, worker_type="G.1X", number_of_workers=2, ...)
big   = GlueInteractiveSessionRunner(reusable=True, worker_type="G.4X", number_of_workers=8, ...)

DbtDag(
    runners={"small": small, "big": big},
    default_runner="small",
    tag_runners={"heavy": "big"},               # tag your big models with `heavy`
    ...
)
```

## Common errors

| Symptom | Likely cause |
|---|---|
| `Session statement failed: state=AVAILABLE output_status=error error=SystemExit: 1` | dbt errored inside the session. Enable additional logging in the session config to see the dbt traceback in CloudWatch (`/aws-glue/sessions/output`). |
| `An error occurred (ConcurrentSessionsExceededException)` | Account-level concurrent-session quota hit. Lower the per-node DAG fan-out, or request a quota increase. |
| Session ID has dashes where the prefix had underscores | Glue normalises underscores to dashes when creating the CloudWatch log stream. Lookup by prefix matching, not exact name. |
