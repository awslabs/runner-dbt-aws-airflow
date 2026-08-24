# Complete reference — YAML + Python, side by side

Every dbt-aws feature, in both YAML and Python form. Single page; jump to the section you need.

- [Install](#install)
- [Project layout](#project-layout)
- [The two ways to configure](#the-two-ways-to-configure)
- [Runners](#runners)
    - [`glue_spark`](#runner-glue_spark) — Glue Spark Job
    - [`glue_interactive_session` (warm)](#runner-glue_interactive_session-warm) — shared Glue session
    - [`glue_interactive_session` (per-node)](#runner-glue_interactive_session-per-node) — ephemeral per model
    - [`glue_python_shell`](#runner-glue_python_shell) — Glue Python Shell
- [Routing](#routing)
    - [`default_runner`](#routing-default_runner) — the fallback
    - [`tag_runners`](#routing-tag_runners) — bulk by tag
    - [`overrides`](#routing-overrides) — per-node escape hatch
    - [`meta.stratus`](#routing-metastratus) — per-model in the dbt project
- [Visual TaskGroups](#visual-taskgroups)
- [DAG construction](#dag-construction)
    - [`DbtDag`](#dbtdag)
    - [`DbtTaskGroup`](#dbttaskgroup)
- [Selectors](#selectors)
- [Deployment helpers](#deployment-helpers)
- [Full worked example](#full-worked-example)
- [Validation summary](#validation-summary)

---

## Install

The lib is published on PyPI:

```bash
# Locally (Airflow author)
pip install runner-dbt-aws-airflow

# Or in a uv-managed project
uv add dbt-aws
```

Glue **workers** install the same wheel via the
`--additional-python-modules` DefaultArgument the runner sets
automatically.

---

## Project layout

A typical dbt-aws project on disk:

```
my_repo/
├── dags/
│   └── my_dbt_dag.py           Airflow DAG file (Python or YAML-driven)
├── runners.yml                  optional YAML config (if not declaring inline)
└── dbt_project/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── models/
    │   ├── bronze/
    │   ├── silver/
    │   └── gold/
    ├── seeds/
    └── target/
        └── manifest.json        committed for fast DAG parse (or generated in CI)
```

---

## The two ways to configure

dbt-aws accepts runner config either as **inline Python** or as a **YAML file** loaded via
`load_runner_config()`. Both produce the same `DbtDag` — pick whichever fits your workflow.

=== "Python (inline)"

    ```python
    from dbt_aws.common import ProjectConfig
    from dbt_aws.common.builder import DbtDag
    from dbt_aws.spark.runners import GlueSparkRunner

    dag = DbtDag(
        dag_id="my_dbt",
        project=ProjectConfig(mode="manifest", manifest_path="target/manifest.json"),
        runner=GlueSparkRunner(mode="create", iam_role_name="GlueRole", ...),
        project_archive_s3="s3://my-bucket/dbt-archive.tar.gz",
    )
    ```

=== "YAML (file)"

    ```yaml
    # runners.yml
    runner:
      type: glue_spark
      mode: create
      iam_role_name: GlueRole
      # ... runner kwargs ...
    ```

    ```python
    # dag file
    from dbt_aws.common import ProjectConfig, load_runner_config
    from dbt_aws.common.builder import DbtDag

    cfg = load_runner_config("runners.yml")

    dag = DbtDag(
        dag_id="my_dbt",
        project=ProjectConfig(mode="manifest", manifest_path="target/manifest.json"),
        runner=cfg.runner,
        project_archive_s3="s3://my-bucket/dbt-archive.tar.gz",
    )
    ```

---

## Runners

### Runner: `glue_spark`

A Glue Spark Job per model. `glue:CreateJob` + `StartJobRun`, with `deferrable=True`.

=== "Python"

    ```python
    from dbt_aws.spark.runners import GlueSparkRunner

    GlueSparkRunner(
        mode="create",                            # or "attach"
        iam_role_name="AWSGlueServiceRole",
        deploy_bucket="my-glue-bucket",
        deploy_prefix="dbt-aws",
        create_job_kwargs={
            "DefaultArguments": {
                "--additional-python-modules": "runner-dbt-aws-airflow==<version>,dbt-core==1.11.11,dbt-duckdb==1.10.1",
                "--job-language": "python",
            },
            "ExecutionProperty": {"MaxConcurrentRuns": 5},  # absorb dev re-triggers
            "GlueVersion": "5.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
        },
        update_config=True,                       # push CreateJob changes to existing Jobs
        aws_conn_id="aws_default",
        region_name="eu-west-1",
        upload_artefacts_s3_prefix="s3://my-glue-bucket/dbt-aws-airflow/glue_spark/",
    )
    ```

=== "YAML"

    ```yaml
    runner:
      type: glue_spark
      mode: create
      iam_role_name: AWSGlueServiceRole
      deploy_bucket: my-glue-bucket
      deploy_prefix: dbt-aws
      create_job_kwargs:
        DefaultArguments:
          "--additional-python-modules": "runner-dbt-aws-airflow==<version>,dbt-core==1.11.11,dbt-duckdb==1.10.1"
          "--job-language": "python"
        ExecutionProperty:
          MaxConcurrentRuns: 5
        GlueVersion: "5.0"
        WorkerType: G.1X
        NumberOfWorkers: 2
      update_config: true
      aws_conn_id: aws_default
      region_name: eu-west-1
      upload_artefacts_s3_prefix: "s3://my-glue-bucket/dbt-aws-airflow/glue_spark/"
    ```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `mode` | `"attach" \| "create"` | `"create"` | Lifecycle. `attach` skips `CreateJob`. |
| `job_name` | `str` | derived per model | Specific Glue Job name. |
| `iam_role_name` | `str` | required (`create`) | IAM role NAME (not ARN). |
| `deploy_bucket` | `str` | required (`create`) | S3 bucket for the worker entrypoint script. |
| `deploy_prefix` | `str` | required (`create`) | S3 key prefix. |
| `create_job_kwargs` | `dict` | `{}` | Forwarded to `glue:CreateJob` — `GlueVersion`, `WorkerType`, `NumberOfWorkers`, `ExecutionProperty.MaxConcurrentRuns`, `DefaultArguments`, … |
| `update_config` | `bool` | `True` | When True + `mode='create'`, push changes to existing Jobs via `UpdateJob`. |
| `aws_conn_id` | `str` | `"aws_default"` | Airflow connection id. |
| `region_name` | `str` | `None` | AWS region. |
| `upload_artefacts_s3_prefix` | `str` | `None` | If set, the worker uploads `target/` here after each run. |

### Runner: `glue_interactive_session` (warm)

One `glue:CreateSession` for the whole DAG, then `RunStatement` per model. The lib injects
a single `setup` + `teardown` pair around the runner's subgroup of tasks.

=== "Python"

    ```python
    from dbt_aws.spark.runners import GlueInteractiveSessionRunner

    GlueInteractiveSessionRunner(
        iam_role_arn="arn:aws:iam::123456789012:role/AWSGlueServiceRole",
        reusable=True,                            # ONE session shared by every model
        session_id_prefix="dbt-aws-warm",
        additional_python_modules="runner-dbt-aws-airflow==<version>,dbt-core==1.11.11,dbt-duckdb==1.10.1",
        default_arguments={
            "--enable-additional-logging": "true",
        },
        glue_version="5.0",
        worker_type="G.1X",
        number_of_workers=2,
        idle_timeout_minutes=15,
        timeout_minutes=45,
        aws_conn_id="aws_default",
        region_name="eu-west-1",
        upload_artefacts_s3_prefix="s3://my-glue-bucket/dbt-aws-airflow/session_warm/",
    )
    ```

=== "YAML"

    ```yaml
    runner:
      type: glue_interactive_session
      iam_role_arn: arn:aws:iam::123456789012:role/AWSGlueServiceRole
      reusable: true                              # one shared session
      session_id_prefix: dbt-aws-warm
      additional_python_modules: "runner-dbt-aws-airflow==<version>,dbt-core==1.11.11,dbt-duckdb==1.10.1"
      default_arguments:
        "--enable-additional-logging": "true"
      glue_version: "5.0"
      worker_type: G.1X
      number_of_workers: 2
      idle_timeout_minutes: 15
      timeout_minutes: 45
      aws_conn_id: aws_default
      region_name: eu-west-1
      upload_artefacts_s3_prefix: "s3://my-glue-bucket/dbt-aws-airflow/session_warm/"
    ```

### Runner: `glue_interactive_session` (per-node)

`reusable=False` — one session per dbt model. Each model gets its own
`(setup, statement, teardown)` triplet.

=== "Python"

    ```python
    GlueInteractiveSessionRunner(
        iam_role_arn="...",
        reusable=False,                           # one session per model
        session_id_prefix="dbt-aws-perNode",
        idle_timeout_minutes=5,                   # tighter for ephemeral
        timeout_minutes=15,
        # ... rest same as warm ...
    )
    ```

=== "YAML"

    ```yaml
    runners:
      session_per_node:
        type: glue_interactive_session
        reusable: false
        session_id_prefix: dbt-aws-perNode
        idle_timeout_minutes: 5
        timeout_minutes: 15
        # ... rest same as warm ...
    ```

!!! warning "Known limitation — duckdb credential chain"

    Per-node session workers have a known issue with duckdb's
    `provider: credential_chain` — every fresh session can't initialise the S3 secret on
    Glue's worker pool, so any dbt operation that touches the S3 secret fails with
    `SystemExit: 1`. See [Troubleshooting](troubleshooting.md#session-statement-fails-with-systemexit-1).

### Runner: `glue_python_shell`

Glue Python Shell job (no Spark). 1 DPU runtime, suitable for non-Spark dbt adapters.

=== "Python"

    ```python
    from dbt_aws.nonspark.runners import GluePythonShellRunner

    GluePythonShellRunner(
        mode="create",
        iam_role_name="AWSGlueServiceRole",
        deploy_bucket="my-glue-bucket",
        deploy_prefix="dbt-aws",
        # ... etc ...
    )
    ```

=== "YAML"

    ```yaml
    runner:
      type: glue_python_shell
      mode: create
      iam_role_name: AWSGlueServiceRole
      # ...
    ```

!!! warning "Known limitation — PyPI install"

    Glue 3.0 Python Shell silently ignores `--python-modules-installer-option` AND can't
    install from `s3://...whl` URIs. The lib's demos drop Python Shell until publish to
    real PyPI. See [Troubleshooting](troubleshooting.md#glue-python-shell-30-install-pipeline).

---

## Routing

Resolution order per node (last wins):

```
1. overrides[unique_id]["runner"]       per-node escape hatch
2. node.meta["stratus"]["runner"]       per-model in dbt project
3. tag_runners[<any tag on node>]       bulk by tag
4. default_runner                       fallback
```

### Routing: `default_runner`

When `runners=` is a dict, `default_runner` is required and names the fallback:

=== "Python"

    ```python
    dag = DbtDag(
        runners={
            "spark":    GlueSparkRunner(...),
            "warm":     GlueInteractiveSessionRunner(reusable=True, ...),
            "per_node": GlueInteractiveSessionRunner(reusable=False, ...),
        },
        default_runner="warm",                # nodes with no override go here
        ...
    )
    ```

=== "YAML"

    ```yaml
    runners:
      spark:
        type: glue_spark
        # ...
      warm:
        type: glue_interactive_session
        reusable: true
        # ...
      per_node:
        type: glue_interactive_session
        reusable: false
        # ...

    default_runner: warm
    ```

### Routing: `tag_runners`

Bulk routing by dbt tag. Two shapes; both accepted by both Python and YAML.

#### Shape 1 — dict with comma-separated keys

=== "Python"

    ```python
    tag_runners = {
        "bronze":      "spark",
        "silver,gold": "warm",                # csv key -> applies to both tags
    }
    ```

=== "YAML"

    ```yaml
    tag_runners:
      bronze: spark
      silver,gold: warm
    ```

#### Shape 2 — list of objects

=== "Python"

    ```python
    tag_runners = [
        {"tags": ["bronze"],            "runner": "spark"},
        {"tags": ["silver", "gold"],    "runner": "warm"},
        {"tags": "intermediate,mart",   "runner": "per_node"},  # csv string also OK
    ]
    ```

=== "YAML"

    ```yaml
    tag_runners:
      - tags: [bronze]
        runner: spark
      - tags: [silver, gold]
        runner: warm
      - tags: intermediate,mart
        runner: per_node
    ```

### Routing: `overrides`

Per-node escape hatch. Targets one `unique_id`. Wins over `tag_runners` + `meta.stratus`.
The override may carry both a `runner` switch AND runner-specific override fields in the
same dict.

=== "Python"

    ```python
    overrides = {
        "model.proj.huge_agg":   {"worker_type": "G.4X", "number_of_workers": 16},
        "model.proj.special":    {"runner": "per_node"},
        "seed.proj.audit_log":   {"runner": "spark"},
        "snapshot.proj.history": {"timeout_minutes": 180, "full_refresh": True},
    }

    dag = DbtDag(
        ...,
        overrides=overrides,
    )
    ```

=== "YAML"

    ```yaml
    overrides:
      model.proj.huge_agg:
        worker_type: G.4X
        number_of_workers: 16
      model.proj.special:
        runner: per_node
      seed.proj.audit_log:
        runner: spark
      snapshot.proj.history:
        timeout_minutes: 180
        full_refresh: true
    ```

Override fields per runner — see [Reference → Runner overrides](reference/runner-overrides.md):

| Runner | Sizing fields | dbt fields | Identity fields |
|---|---|---|---|
| `glue_spark` | `worker_type`, `number_of_workers`, `timeout_minutes` | `full_refresh`, `vars_json` | `job_name`, `iam_role_name`, `script_location`, `mode`, `concurrent_runs` |
| `glue_interactive_session` | (session-level only) | `full_refresh`, `vars_json`, `timeout_minutes` | (none) |
| `glue_python_shell` | `max_capacity`, `timeout_minutes` | `full_refresh`, `vars_json` | `job_name`, `iam_role_name`, `script_location`, `mode`, `concurrent_runs` |

### Routing: `meta.stratus`

Per-model declaration inside the dbt project. Lower priority than Python/YAML `overrides`,
higher than `tag_runners`.

```sql title="models/silver/sv_dim_customer.sql"
{{ config(
    tags=['silver'],
    materialized='external',
    format='parquet',
    meta={'stratus': {
        'runner': 'per_node',
        'worker_type': 'G.2X',
        'number_of_workers': 4,
    }},
) }}

SELECT ...
```

---

## Visual TaskGroups

Collapsible UI folders, one per dbt layer. Independent of `tag_runners` — picks WHICH UI
folder the task lives in (not WHICH runner executes it).

=== "Python"

    ```python
    from dbt_aws.common import TaskGroupConfig, TaskGroupingConfig

    task_groups = TaskGroupingConfig(
        groups=(
            TaskGroupConfig(name="bronze",     tags=frozenset({"bronze"})),
            TaskGroupConfig(name="silver",     tags=frozenset({"silver"})),
            TaskGroupConfig(name="gold",       tags=frozenset({"gold"})),
            TaskGroupConfig(name="dimensions", tags=frozenset({"dim", "scd"})),
        ),
        ungrouped_group="other",    # fallback; None = unmatched at DAG root
    )

    dag = DbtDag(..., task_groups=task_groups)
    ```

=== "YAML"

    ```yaml
    task_groups:
      - name: bronze
        tags: [bronze]
      - name: silver
        tags: [silver]
      - name: gold
        tags: [gold]
      - name: dimensions
        tags: [dim, scd]

    ungrouped_group: other          # optional fallback
    ```

UI result:

```
my_dag
├─ ▸ bronze   (8 tasks)
├─ ▸ silver   (4 tasks)
├─ ▸ gold     (4 tasks)
├─ ▸ dimensions (2 tasks)
└─ ▸ other    (untagged tasks)
```

---

## DAG construction

### `DbtDag`

Subclass of `airflow.sdk.DAG`. Returns a populated DAG.

=== "Python (inline runner)"

    ```python
    from datetime import datetime
    from dbt_aws.common import ProjectConfig
    from dbt_aws.common.builder import DbtDag
    from dbt_aws.spark.runners import GlueSparkRunner

    dag = DbtDag(
        dag_id="my_dbt",
        project=ProjectConfig(mode="manifest", manifest_path="target/manifest.json"),
        runner=GlueSparkRunner(...),
        project_archive_s3="s3://my-bucket/archive.tar.gz",
        target="dev",
        select=["+gd_top_customers+"],
        exclude=["tag:wip"],
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
    )
    ```

=== "Python (multi-runner)"

    ```python
    dag = DbtDag(
        dag_id="my_dbt",
        project=ProjectConfig(mode="manifest", manifest_path="target/manifest.json"),
        runners={"spark": ..., "warm": ..., "per_node": ...},
        default_runner="warm",
        tag_runners={"bronze": "spark", "silver,gold": "warm"},
        overrides={"seed.proj.audit_log": {"runner": "per_node"}},
        task_groups=TaskGroupingConfig(...),
        project_archive_s3="s3://my-bucket/archive.tar.gz",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
    )
    ```

=== "Python (YAML-driven)"

    ```python
    from dbt_aws.common import load_runner_config

    cfg = load_runner_config("runners.yml")

    dag = DbtDag(
        dag_id="my_dbt",
        project=ProjectConfig(mode="manifest", manifest_path="target/manifest.json"),
        config=cfg,                                  # : auto-wires every field
        project_archive_s3="s3://my-bucket/archive.tar.gz",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
    )
    ```

Required kwargs:

| Kwarg | Type | Meaning |
|---|---|---|
| `dag_id` | `str` | Airflow DAG id. |
| `project` | `ProjectConfig` | How to load the dbt graph. |
| `project_archive_s3` | `str` | `s3://...tar.gz` URI workers download. |
| `runner` OR `runners` | `Runner` / `dict` | Exactly one is required. |

Optional kwargs:

| Kwarg | Default | Meaning |
|---|---|---|
| `default_runner` | `None` | Required when `runners=` is set. |
| `target` | `"dev"` | dbt target name. |
| `select` | `None` | List of dbt selectors. |
| `exclude` | `None` | List of selectors to subtract. |
| `overrides` | `None` | `{unique_id: {...}}` per-node overrides. |
| `tag_runners` | `None` | Bulk tag→runner map. |
| `task_groups` | `None` | `TaskGroupingConfig` for visual nesting. |
| `airflow_kwargs_per_task` | `None` | Forwarded to every underlying operator. |
| `**dag_kwargs` | — | Passed to `airflow.sdk.DAG` (`schedule`, `start_date`, `tags`, …). |

### `DbtTaskGroup`

Same kwargs, but with `group_id` instead of `dag_id` and `**task_group_kwargs` instead of
`**dag_kwargs`. Must be constructed inside an `with DAG(...)` block.

```python
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dbt_aws.common.builder import DbtTaskGroup

with DAG(dag_id="hybrid", start_date=..., schedule=None) as dag:
    preflight = PythonOperator(task_id="preflight", python_callable=lambda: None)

    dbt_tg = DbtTaskGroup(
        group_id="dbt_run",
        project=ProjectConfig(...),
        runner=GlueSparkRunner(...),
        project_archive_s3="s3://...",
    )

    notify = PythonOperator(task_id="notify", python_callable=lambda: None)
    preflight >> dbt_tg >> notify
```

---

## Selectors

dbt-style selectors. UNION semantics across `select=`. Pass as a Python list to
`DbtDag` — they are a **DAG-level** concern, not a runner concern. One `runners.yml`
can power many DAGs, each picking its own `select=` / `exclude=` in Python.

```python
DbtDag(
    ...,
    select=[
        "+gd_top_customers+",       # gd_top_customers + ancestors + descendants
        "tag:bronze",               # all bronze models
        "+gd_revenue_by_region",    # gd_revenue_by_region + ancestors
        "audit_log",                # specific node
    ],
    exclude=[
        "tag:wip",                  # exclude work-in-progress
        "test.*",                   # exclude tests
    ],
)
```

| Selector | Meaning |
|---|---|
| `model_name` | exact match |
| `tag:foo` | every node carrying tag `foo` |
| `+x` | `x` plus all upstream nodes |
| `x+` | `x` plus all downstream nodes |
| `+x+` | `x` plus full lineage |
| `^foo` | nodes above `foo` (exclusive) |
| `foo@` | only direct children of `foo` |
| `tag:foo,tag:bar` | nodes with both tags (intersection in one selector) |

**Subset-DAGs from ONE shared runner config.** This is the recommended pattern: keep the
`runners.yml` reusable, and let each DAG file pick its own subset:

```python title="dags/bronze_only.py"
cfg = load_runner_config("runners.yml")
dag = DbtDag(
    dag_id="bronze_only",
    config=cfg,                                # : auto-wires every field
    project=ProjectConfig(...),
    project_archive_s3="...",
    select=["tag:bronze"],                     # THIS DAG's subset
    start_date=datetime(2026, 1, 1),
)
```

```python title="dags/gold_only.py"
cfg = load_runner_config("runners.yml")        # SAME yaml
dag = DbtDag(
    dag_id="gold_only",
    config=cfg,                                # : auto-wires every field
    project=ProjectConfig(...),
    project_archive_s3="...",
    select=["tag:gold"],                       # different subset
    start_date=datetime(2026, 1, 1),
)
```

---

## Deployment helpers

### Project archive — `build_and_upload_project_archive`

Tar-gzips the dbt project, content-addresses with sha256, uploads to S3. Idempotent.

```python
from pathlib import Path
from dbt_aws.common.airflow_extras.auto_deploy import build_and_upload_project_archive

ARCHIVE_S3 = build_and_upload_project_archive(
    project_dir=Path("/path/to/dbt_project"),
    cache_dir=Path("/tmp/dbt_aws_cache"),
    bucket="my-glue-bucket",
    prefix="dbt-aws",
    region_name="eu-west-1",
)
# -> "s3://my-glue-bucket/dbt-aws/archives/<sha256>.tar.gz"
```

Pass `ARCHIVE_S3` straight to `DbtDag(project_archive_s3=ARCHIVE_S3)`.

### Worker entrypoint — content-addressed

The worker entry script is uploaded automatically to:

```
s3://<deploy_bucket>/<deploy_prefix>/worker_entrypoint/<md5>.py
```

When the lib upgrades and changes the entrypoint, a new key is uploaded. Old Glue Jobs
keep pointing at their original md5 — no broken jobs on lib upgrade.

---

## Full worked example

A complete demo DAG using every feature: three runners, tag-based routing, per-node
overrides, visual task groups, multi-selector.

=== "Python"

    ```python title="dags/medallion.py" linenums="1"
    from datetime import datetime
    from pathlib import Path

    from dbt_aws.common import ProjectConfig, TaskGroupConfig, TaskGroupingConfig
    from dbt_aws.common.airflow_extras.auto_deploy import (
        build_and_upload_project_archive,
    )
    from dbt_aws.common.builder import DbtDag
    from dbt_aws.spark.runners import (
        GlueInteractiveSessionRunner,
        GlueSparkRunner,
    )

    # -------------------------------------------------------------------
    # Constants
    # -------------------------------------------------------------------
    PROJECT = Path("/path/to/dbt_project")
    S3_BUCKET = "my-glue-bucket"
    AWS_REGION = "eu-west-1"
    IAM_ROLE_NAME = "AWSGlueServiceRole"
    IAM_ROLE_ARN = "arn:aws:iam::123456789012:role/AWSGlueServiceRole"

    ADDITIONAL_PYTHON_MODULES = (
        "dbt-aws,dbt-core==1.11.11,dbt-duckdb==1.10.1"
    )

    # -------------------------------------------------------------------
    # Deployment (parse-time)
    # -------------------------------------------------------------------
    ARCHIVE_S3 = build_and_upload_project_archive(
        project_dir=PROJECT,
        cache_dir=Path("/tmp/dbt_aws_cache"),
        bucket=S3_BUCKET,
        prefix="dbt-aws",
        region_name=AWS_REGION,
    )

    # -------------------------------------------------------------------
    # Runners
    # -------------------------------------------------------------------
    glue_spark = GlueSparkRunner(
        mode="create",
        iam_role_name=IAM_ROLE_NAME,
        deploy_bucket=S3_BUCKET,
        deploy_prefix="dbt-aws",
        create_job_kwargs={
            "DefaultArguments": {
                "--additional-python-modules":      ADDITIONAL_PYTHON_MODULES,
                "--python-modules-installer-option": PIP_INSTALLER_OPTIONS,
                "--job-language":                   "python",
            },
            "ExecutionProperty": {"MaxConcurrentRuns": 5},
            "GlueVersion":     "5.0",
            "WorkerType":      "G.1X",
            "NumberOfWorkers": 2,
        },
        update_config=True,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        upload_artefacts_s3_prefix=f"s3://{S3_BUCKET}/dbt-aws-airflow/glue_spark/",
    )

    _SESSION_COMMON = dict(
        iam_role_arn=IAM_ROLE_ARN,
        additional_python_modules=ADDITIONAL_PYTHON_MODULES,
        default_arguments={
            "--python-modules-installer-option":  PIP_INSTALLER_OPTIONS,
            "--enable-additional-logging":         "true",
        },
        glue_version="5.0",
        worker_type="G.1X",
        number_of_workers=2,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
    )

    session_warm = GlueInteractiveSessionRunner(
        reusable=True,
        session_id_prefix="dbt-aws-warm",
        idle_timeout_minutes=15,
        timeout_minutes=45,
        upload_artefacts_s3_prefix=f"s3://{S3_BUCKET}/dbt-aws-airflow/session_warm/",
        **_SESSION_COMMON,
    )

    session_per_node = GlueInteractiveSessionRunner(
        reusable=False,
        session_id_prefix="dbt-aws-perNode",
        idle_timeout_minutes=5,
        timeout_minutes=15,
        upload_artefacts_s3_prefix=f"s3://{S3_BUCKET}/dbt-aws-airflow/session_per_node/",
        **_SESSION_COMMON,
    )

    # -------------------------------------------------------------------
    # Routing
    # -------------------------------------------------------------------
    TAG_RUNNERS = {
        "bronze":      "glue_spark",         # every bronze model -> Glue Spark Job
        "silver,gold": "session_warm",       # silver + gold -> warm session
    }

    OVERRIDES = {
        "seed.dbt_project.regions":   {"runner": "glue_spark"},
        "seed.dbt_project.audit_log": {"runner": "glue_spark"},
    }

    TASK_GROUPS = TaskGroupingConfig(
        groups=(
            TaskGroupConfig(name="bronze", tags=frozenset({"bronze"})),
            TaskGroupConfig(name="silver", tags=frozenset({"silver"})),
            TaskGroupConfig(name="gold",   tags=frozenset({"gold"})),
        ),
        ungrouped_group="other",
    )

    # -------------------------------------------------------------------
    # DAG
    # -------------------------------------------------------------------
    dag = DbtDag(
        dag_id="medallion",
        project=ProjectConfig(
            mode="manifest", manifest_path=PROJECT / "target/manifest.json",
        ),
        runners={
            "glue_spark":       glue_spark,
            "session_warm":     session_warm,
            "session_per_node": session_per_node,
        },
        default_runner="session_warm",
        tag_runners=TAG_RUNNERS,
        overrides=OVERRIDES,
        task_groups=TASK_GROUPS,
        project_archive_s3=ARCHIVE_S3,
        target="dev",
        select=[
            "+gd_revenue_by_region+",
            "+gd_top_customers+",
            "regions",
            "audit_log",
        ],
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["dbt-aws", "medallion"],
    )
    ```

=== "YAML"

    ```yaml title="runners.yml" linenums="1"
    runners:
      glue_spark:
        type: glue_spark
        mode: create
        iam_role_name: AWSGlueServiceRole
        deploy_bucket: my-glue-bucket
        deploy_prefix: dbt-aws
        create_job_kwargs:
          DefaultArguments:
            "--additional-python-modules": "runner-dbt-aws-airflow==<version>,dbt-core==1.11.11,dbt-duckdb==1.10.1"
            "--job-language": "python"
          ExecutionProperty:
            MaxConcurrentRuns: 5
          GlueVersion: "5.0"
          WorkerType: G.1X
          NumberOfWorkers: 2
        update_config: true
        aws_conn_id: aws_default
        region_name: eu-west-1
        upload_artefacts_s3_prefix: "s3://my-glue-bucket/dbt-aws-airflow/glue_spark/"

      session_warm:
        type: glue_interactive_session
        iam_role_arn: arn:aws:iam::123456789012:role/AWSGlueServiceRole
        reusable: true
        session_id_prefix: dbt-aws-warm
        additional_python_modules: "runner-dbt-aws-airflow==<version>,dbt-core==1.11.11,dbt-duckdb==1.10.1"
        default_arguments:
          "--enable-additional-logging": "true"
        glue_version: "5.0"
        worker_type: G.1X
        number_of_workers: 2
        idle_timeout_minutes: 15
        timeout_minutes: 45
        aws_conn_id: aws_default
        region_name: eu-west-1
        upload_artefacts_s3_prefix: "s3://my-glue-bucket/dbt-aws-airflow/session_warm/"

      session_per_node:
        type: glue_interactive_session
        iam_role_arn: arn:aws:iam::123456789012:role/AWSGlueServiceRole
        reusable: false
        session_id_prefix: dbt-aws-perNode
        additional_python_modules: "runner-dbt-aws-airflow==<version>,dbt-core==1.11.11,dbt-duckdb==1.10.1"
        default_arguments:
          "--enable-additional-logging": "true"
        glue_version: "5.0"
        worker_type: G.1X
        number_of_workers: 2
        idle_timeout_minutes: 5
        timeout_minutes: 15
        aws_conn_id: aws_default
        region_name: eu-west-1
        upload_artefacts_s3_prefix: "s3://my-glue-bucket/dbt-aws-airflow/session_per_node/"

    default_runner: session_warm

    tag_runners:
      bronze: glue_spark
      silver,gold: session_warm

    overrides:
      seed.dbt_project.regions:
        runner: glue_spark
      seed.dbt_project.audit_log:
        runner: glue_spark

    task_groups:
      - name: bronze
        tags: [bronze]
      - name: silver
        tags: [silver]
      - name: gold
        tags: [gold]

    ungrouped_group: other
    ```

    ```python title="dags/medallion.py" linenums="1"
    from datetime import datetime
    from pathlib import Path

    from dbt_aws.common import ProjectConfig, load_runner_config
    from dbt_aws.common.airflow_extras.auto_deploy import (
        build_and_upload_project_archive,
    )
    from dbt_aws.common.builder import DbtDag

    PROJECT      = Path("/path/to/dbt_project")
    RUNNERS_YAML = Path(__file__).parent / "runners.yml"

    ARCHIVE_S3 = build_and_upload_project_archive(
        project_dir=PROJECT,
        cache_dir=Path("/tmp/dbt_aws_cache"),
        bucket="my-glue-bucket",
        prefix="dbt-aws",
        region_name="eu-west-1",
    )

    RUNNER_CFG = load_runner_config(RUNNERS_YAML)

    dag = DbtDag(
        dag_id="medallion",
        project=ProjectConfig(
            mode="manifest", manifest_path=PROJECT / "target/manifest.json",
        ),
        config=RUNNER_CFG,   # auto-wires runners/default/overrides/tag_*/task_groups
        project_archive_s3=ARCHIVE_S3,
        target="dev",
        select=[
            "+gd_revenue_by_region+",
            "+gd_top_customers+",
            "regions",
            "audit_log",
        ],
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["dbt-aws", "medallion"],
    )
    ```

---

## Validation summary

Every config check happens at **DAG-parse time** (when Airflow imports the file). The
`DbtDag` constructor either returns a fully-validated DAG or raises with a clear message.

| Failure | Layer | Error |
|---|---|---|
| `runner=` and `runners=` both set | DbtDag | `ValueError("...either runner= OR runners= (not both)")` |
| `runners=` without `default_runner=` | DbtDag | `ValueError("...require default_runner=")` |
| `default_runner` not in `runners=` | DbtDag | `ValueError` |
| Unknown runner `type:` in YAML | `load_runner_config` | `RunnerConfigError` listing valid types |
| Override field unknown for runner's `OVERRIDE_TYPE` | `_resolve_node_runners` | `OverrideError` listing valid fields |
| Tag mapped to two different runners | `tag_runners` parse | `RunnerConfigError` / `ValueError` |
| Node carries two tags routing to different runners | `_resolve_node_runners` | `ValueError("tag_runners conflict (tag1 -> r1, tag2 -> r2)")` |
| Tag declared in `tag_runners` but no selected node has it | parse-time warning | `WARNING` log (soft typo guard) |
| Two groups in `task_groups` share a tag | `TaskGroupingConfig` | `RunnerConfigError` |
| Node tagged with two `task_groups`-claimed tags | `_resolve_node_groups` | `ValueError("...matches multiple task_groups")` |

The parse-time log always emits a runner distribution summary so you can verify routing
resolved as expected:

```
[info] runner distribution: glue_spark=9, session_per_node=1, session_warm=22
```
