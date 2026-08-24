# Runners

A *runner* is the backend that executes a single dbt model. dbt-aws ships four first-class
runner shapes; one DAG can mix any of them (see [Routing](routing.md)).

## At a glance

| Runner | Spin-up | Reuse | dbt-aws default | Best for |
|---|---|---|---|---|
| **Glue Spark Job** | ~2-3 min (one Glue Job per model) | None — fresh executor per run | First-class | Heavy / sized workloads, one big SLA |
| **Glue Session (warm)** | ~1-2 min, **shared** across models | One session for the whole DAG | First-class | Medallion chains, fast feedback |
| **Glue Session (per-node)** | ~1-2 min each, **per model** | None | First-class | Isolation per model (sensitive PII, A/B compute config) |
| **Glue Python Shell** | ~30 s | Per model | First-class | Lightweight non-Spark (small seeds, audit logs) |

## Glue Spark Job — `GlueSparkRunner`

A `glue:CreateJob` (or `UpdateJob` if it exists) followed by `glue:StartJobRun`. One Glue
Job per model by default (configurable). Each run spins up a fresh Spark executor and tears
it down on completion.

```python
from dbt_aws.spark.runners import GlueSparkRunner

GlueSparkRunner(
    mode="create",                        # or "attach" if the Job already exists
    iam_role_name="AWSGlueServiceRole",
    deploy_bucket="my-glue-bucket",
    deploy_prefix="dbt-aws",
    create_job_kwargs={
        "DefaultArguments": {
            "--additional-python-modules": "dbt-aws,dbt-core==1.11.11,dbt-duckdb==1.10.1",
            "--job-language": "python",
        },
        "GlueVersion": "5.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        # Recommended: > 1 to absorb dev re-triggers
        "ExecutionProperty": {"MaxConcurrentRuns": 5},
    },
    update_config=True,
    region_name="eu-west-1",
)
```

Strengths

- Most isolated execution shape; no cross-model state pollution.
- Each model can pick its own `WorkerType` / `NumberOfWorkers` via [overrides](routing.md#yaml-unified-shape).
- Glue Jobs are inspectable in the AWS console long after the DAG run finishes.

Trade-offs

- Slowest spin-up of the Spark family (~2-3 min cold start per model).
- One concurrent run per Job by default. Set `ExecutionProperty.MaxConcurrentRuns > 1` for
  parallel runs of the same model (e.g. dev re-triggers).

## Glue Interactive Session — `GlueInteractiveSessionRunner`

A `glue:CreateSession` once, then `glue:RunStatement` per model. Two modes:

### Warm (shared)

```python
GlueInteractiveSessionRunner(
    iam_role_arn="arn:aws:iam::123:role/AWSGlueServiceRole",
    reusable=True,                        # ONE session shared by every model
    session_id_prefix="dbt-aws-warm",
    additional_python_modules="dbt-aws,dbt-core==1.11.11,dbt-duckdb==1.10.1",
    default_arguments={
    },
    glue_version="5.0",
    worker_type="G.1X",
    number_of_workers=2,
    idle_timeout_minutes=15,
)
```

The builder injects a single `setup` task at the start of the runner subgroup that calls
`CreateSession`, and a `teardown` task at the end that calls `DeleteSession`. Every model in
between submits a statement.

Strengths

- ~1-2 min ONE-TIME cold start, then every model statement starts in seconds.
- Single Spark application — models can share cached datasets in memory.
- Best fit for medallion chains where many small models run sequentially.

### Per-node (ephemeral)

```python
GlueInteractiveSessionRunner(
    iam_role_arn="arn:aws:iam::123:role/AWSGlueServiceRole",
    reusable=False,                       # one session per model
    session_id_prefix="dbt-aws-perNode",
    idle_timeout_minutes=5,
    # ... rest same as warm
)
```

Each model gets its own `(setup, statement, teardown)` triplet — fresh session, one
statement, immediate delete.

Strengths

- Per-model isolation (no shared state — useful for PII or compliance-sensitive nodes).
- Each model can configure its own session size.

Trade-offs

- Cold start each model (~1-2 min) — pay this cost N times for N models.
- N concurrent sessions → watch the account-level concurrent-sessions quota.

### Non-deferrable mode

By default, both the warm and per-node variants defer to the Airflow
Triggerer for async polling (`glue:GetSession` and `glue:GetStatement`).
Under high fan-out on a constrained Triggerer (single process + SQLite
metastore, or macOS Airflow standalone), the Triggerer event loop can
wedge; tasks stay `deferred` indefinitely even though AWS has finished
the work.

Opt out by setting ``deferrable=False`` on the runner. The
`CreateSession` and `RunStatement` operators then sync-poll on a worker
slot until the session/statement reaches a terminal state:

```python
GlueInteractiveSessionRunner(
    iam_role_arn="...",
    reusable=True,
    deferrable=False,   # opt out of Triggerer, sync-poll instead
    idle_timeout_minutes=60,
    ...
)
```

Cost: each active dbt model holds one Airflow worker slot for its
full runtime (~1-3 min per model). Fine for demo / local dev; on MWAA
you almost always want ``deferrable=True`` (default).

See [Troubleshooting → Airflow triggerer wedges under high fan-out](../troubleshooting.md#airflow-triggerer-wedges-under-high-fan-out)
for when to reach for this.

### Cross-runner refs

Glue Interactive Session's DuckDB is per-session, not per-node. All
models routed to the SAME warm session share a catalog. But models on
DIFFERENT runners (or per-node sessions) each get a fresh DuckDB — so
`ref('br_part')` fails with ``Catalog Error: Table with name br_part
does not exist!`` when the upstream ran on a different runner.

Fix in ``dbt_project.yml`` (once per project):

```yaml
on-run-start:
  - "{{ register_upstream_external_models() }}"

models:
  my_project:
    bronze:
      +materialized: external
      +format: parquet
```

The macro (built into dbt-duckdb) walks the manifest and creates a
DuckDB view over every ``materialized: external`` upstream Parquet
URI at run-start, so ``ref()`` works even on a fresh DuckDB.

## Glue Python Shell — `GluePythonShellRunner`

`glue:CreateJob` with `Command.Name = pythonshell`. Lightweight non-Spark runtime (1 DPU).

```python
from dbt_aws.nonspark.runners import GluePythonShellRunner

GluePythonShellRunner(
    mode="create",
    iam_role_name="AWSGlueServiceRole",
    # ...
)
```

!!! warning "Glue 3.0 Python Shell + PyPI"

    Glue 3.0 Python Shell silently ignores `--python-modules-installer-option` AND
    can't install from `s3://...whl` URIs in `--additional-python-modules`. The lib
    drops Python Shell from the demos until it can resolve `dbt-aws` from real PyPI.
    Re-enable in your own DAG once `dbt-aws` ships to PyPI.

## EMR Serverless — `EmrServerlessRunner` *(preview)*

Submit jobs to an EMR Serverless application. Application is reused across models; jobs
are tracked via `EmrServerlessJobSensor`.

Two modes:

* `mode="attach"` — point at an existing application (`application_id="..."`).
* `mode="create"` — the DAG creates the app at parse time; setup/teardown
  tasks bracket the DAG run.

**End-to-end validation status:** unit-tested (49 tests). Full end-to-end
validation on real AWS is blocked by needing a custom Docker image with
`dbt-core` and `dbt-duckdb` (EMR Serverless doesn't accept plain wheels via
`--additional-python-modules`). Lib code is complete and same runner-contract
as the other four; empirical validation waits on an ECR image-build pipeline.

## EMR Cluster Step — `EmrClusterStepRunner`

`elasticmapreduce:AddJobFlowSteps` against an existing or auto-created EMR cluster. Steps
run sequentially on the cluster.

```python
EmrClusterStepRunner(
    mode="create",              # or "attach" to a running cluster
    reusable=True,              # one cluster shared by ALL steps in a DAG run
    auto_terminate=True,        # tear down when the last step finishes
    deploy_mode="client",       # RECOMMENDED for dbt: driver on master, no YARN AM
    pyspark_python="/usr/bin/python3.11",  # default; EMR 7.5+ AL2023 has both 3.9 + 3.11
    job_flow_overrides={
        "Name": "my-cluster",
        "ReleaseLabel": "emr-7.5.0",       # >= 7.0.0 for Python 3.11
        "BootstrapActions": [
            {
                "Name": "install-dbt-aws",
                "ScriptBootstrapAction": {
                    "Path": f"s3://{bucket}/dbt-aws/bootstrap/install_dbt_aws.sh",
                    "Args": [                       # override versions per-DAG
                        "dbt-aws",
                        "dbt-core==1.11.11",
                        "dbt-duckdb==1.10.1",
                    ],
                },
            },
        ],
        "Instances": {
            "InstanceGroups": [...],
            "Ec2SubnetId": "subnet-xxx",
            # REQUIRED for reusable=False (per-node clusters). Without
            # this, EMR sees an empty step list at cluster-start time
            # and auto-terminates before ``.statement`` can
            # ``AddJobFlowSteps``. The lib's teardown task terminates
            # explicitly. See troubleshooting for the exact error.
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
        # ... standard EMR knobs (ServiceRole, JobFlowRole, LogUri)
    },
    deploy_bucket=bucket,
    deploy_prefix="dbt-aws",
    aws_conn_id="aws_default",
    region_name="eu-west-1",
)
```

Key knobs (all set to sensible defaults by the runner; override when your
cluster differs):

* **`pyspark_python`** (default `/usr/bin/python3.11`): sets four Spark
  configs so the right Python is used in BOTH `client` and `cluster` deploy
  modes: `spark.pyspark.python`, `spark.pyspark.driver.python`,
  `spark.yarn.appMasterEnv.PYSPARK_PYTHON`, `spark.executorEnv.PYSPARK_PYTHON`.
  Without this, EMR 7.5+ picks up `/usr/bin/python3` (=3.9) which doesn't
  have `dbt-aws` installed and the step fails with
  `ModuleNotFoundError: No module named 'dbt_aws'`. Pass `None` to opt out.
* **`deploy_mode`** (default `"cluster"`): use `"client"` for dbt workloads.
  Cluster mode expects the user program to initialise a `SparkContext`; the
  dbt-aws entry script runs dbt as a subprocess that doesn't need Spark itself
  and the Spark AM then errors with `User did not initialize spark context!`
  even after dbt exits 0. Client mode runs the driver on the master node
  (no YARN AM), sidesteps that.
* **`spark.yarn.appMasterEnv.HOME=/tmp` + `spark.executorEnv.HOME=/tmp`**
  are always set. Works around YARN's hardcoded `HOME=/home/` (unwritable
  to the container user) which otherwise breaks dbt-duckdb's extension
  cache with `Permission denied /home/.duckdb`. The runtime helper
  `_ensure_writable_home()` also does a defensive in-process probe.

**End-to-end validation status:** proven on real AWS eu-west-1 (Airflow 3.2.1
on MWAA + integration test suite). Worker-side `dbt deps` installs
`dbt_utils` cleanly; 601 macros load; `INSERT 4` on the sample seed.

## Per-model Spark configuration (`spark_conf`)

`GlueSparkRunner` accepts a `spark_conf: dict[str, str]` kwarg (and an
escape-hatch `spark_conf_replace`) that layer across:

1. Runner-level `spark_conf` (baked into the Job's default `--conf` at
   create/update time).
2. `overrides[tag.<name>].spark_conf`.
3. `overrides[<uid>].spark_conf` and dbt-side
   `meta.stratus.spark_conf`.

The merged result lands in the per-JobRun `--conf` argument, so
concurrent runs of the same Glue Job with different per-node configs
never race.

**Runtime-only:** Glue accepts `--conf` at StartJobRun time for
DRIVER / EXECUTOR runtime configs (`spark.sql.*`,
`spark.default.parallelism`, ...). JVM-start configs
(`spark.jars.packages`, `spark.hadoop.fs.*`, memory / cores) MUST
live under `create_job_kwargs.DefaultArguments` on the runner --
Glue silently ignores those per JobRun.

Full layering rules, replace-mode escape hatch, and validation errors
are documented in
[Reference → YAML config](../reference/runner-config-yaml.md#per-model-per-tag-spark-config-spark_conf).

## Worker-side `dbt deps` — `with_deps`

Every runner accepts `with_deps: bool = True`. When set (the default), the worker installs `packages.yml` deps into a per-task
`dbt_packages/` before invoking dbt.

```python
GlueSparkRunner(..., with_deps=True)              # default True
GlueInteractiveSessionRunner(..., with_deps=True)
GluePythonShellRunner(..., with_deps=True)
```

**Airflow deployment implication (MWAA):** you don't need `dbt-core` in
Airflow's `requirements.txt`. See
[Deployment → MWAA](deployment.md#mwaa-projects-with-packagesyml) for the
full compat notes.

## Pick a runner

Decision tree:

```
Do you need Spark?
├── No  → GluePythonShell
└── Yes
    │
    ├── Many small models, fast feedback?
    │     → GlueInteractiveSession(reusable=True)   "warm"
    │
    ├── Per-model isolation required?
    │     → GlueInteractiveSession(reusable=False)  "per-node"
    │
    └── Default — one big Spark workload per model?
          → GlueSpark
```

## Mix runners in one DAG

See [How-to → Multi-runner mix](../how-to/multi-runner-mix.md).
