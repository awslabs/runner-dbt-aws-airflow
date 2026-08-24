# dbt-aws

Run [dbt](https://www.getdbt.com/) projects on **AWS Glue** (Spark Jobs,
Interactive Sessions, Python Shell) and **Amazon EMR** (Serverless,
on-EC2) — orchestrated from **Apache Airflow**.

One library, five runner shapes, declarative routing, visual grouping,
and **worker-side dbt package installs** so your Airflow deployment
(including MWAA) stays lean.

> **PyPI distribution:** `runner-dbt-aws-airflow`.
> **Python import path:** `dbt_aws` (PEP 420 namespace package).
> **Repo:** <https://github.com/awslabs/runner-dbt-aws-airflow>.

```python title="One DAG, three runners, layer-aware routing"
from dbt_aws.common import ProjectConfig, TaskGroupConfig, TaskGroupingConfig
from dbt_aws.common.builder import DbtDag
from dbt_aws.spark.runners import GlueInteractiveSessionRunner, GlueSparkRunner

dag = DbtDag(
    dag_id="medallion",
    project=ProjectConfig(mode="manifest", manifest_path="target/manifest.json"),
    runners={
        "glue_spark":   GlueSparkRunner(mode="create", iam_role_name="GlueRole"),
        "session_warm": GlueInteractiveSessionRunner(iam_role_arn="...", reusable=True),
        "session_per_node": GlueInteractiveSessionRunner(iam_role_arn="...", reusable=False),
    },
    default_runner="session_warm",
    tag_runners={
        "bronze":       "glue_spark",      # 8 bronze models -> Glue Spark Job
        "silver,gold":  "session_warm",    # 8 silver+gold -> shared warm session
    },
    overrides={
        "seed.proj.audit_log": {"runner": "session_per_node"},  # one-off per-node session
    },
    task_groups=TaskGroupingConfig(
        groups=(
            TaskGroupConfig(name="bronze", tags=frozenset({"bronze"})),
            TaskGroupConfig(name="silver", tags=frozenset({"silver"})),
            TaskGroupConfig(name="gold",   tags=frozenset({"gold"})),
        ),
        ungrouped_group="other",
    ),
    project_archive_s3="s3://my-bucket/dbt-archives/abc.tar.gz",
)
```

## What it does

- **Parses your dbt manifest** locally and turns every `model`/`seed`/`snapshot`/`test` into an
  Airflow task — one task per node, wired by `depends_on`.
- **Dispatches each task** to a named runner (Glue Spark Job, Glue Interactive Session, Python
  Shell, ...). Routing is declarative: per-tag, per-node, or per-model.
- **Visualises layers** as collapsible Airflow `TaskGroup`s via tag-based nesting. Independent
  of routing — bronze/silver/gold each become their own folder in the grid view.
- **Installs dbt packages on the worker** by default. Airflow deployments
  (including MWAA) don't need `dbt-core` in `requirements.txt` — workers handle
  `packages.yml` themselves via `python -m dbt.cli.main deps`.
- **Handles deployment**: builds the dbt project archive, content-addresses the worker
  entrypoint script, uploads both to S3, picks them up on every run.

## Two deployment variants (pick the one that matches your networking)

Before reading further, decide which shape applies:

| Variant | Workers have internet? | MWAA needs `dbt-core`? | Where `dbt deps` runs |
|---|---|---|---|
| **A** (most users) | Yes | **No** | On the worker, in `/tmp/<run-id>/project/dbt_packages/` |
| **B** (air-gapped) | No | **Yes** | On the Airflow scheduler; `dbt_packages/` baked into the archive |

**Variant A** is the default configuration. MWAA `requirements.txt` needs *only* the
dbt-aws wheel; workers install `packages.yml` deps themselves at task-run time. The lib
does **not** import dbt anywhere at DAG-parse time (`manifest.json` is parsed as plain
JSON), so DAG parse can't fail with `ModuleNotFoundError` even when `dbt-core` isn't
installed.

**Variant B** applies when workers can't reach GitHub (private subnets with only VPC
endpoints, corporate egress firewalls, etc.). See
[Concepts → Deployment → Two deployment variants](concepts/deployment.md#two-deployment-variants)
for the full switch.
- **YAML or Python** — runner config can live next to the DAG (Python) or in its own file
  (YAML) for ops-friendly editing.

## Quick links

<div class="grid cards" markdown>

- :material-rocket-launch:{ .lg .middle } **Get started in 5 minutes**

    ---

    Wire your first dbt project to a single Glue Spark Job runner.

    [:octicons-arrow-right-24: Getting started](getting-started.md)

- :material-puzzle:{ .lg .middle } **Concepts**

    ---

    Architecture, runners, routing, visual grouping, deployment.

    [:octicons-arrow-right-24: Concepts](concepts/index.md)

- :material-code-tags:{ .lg .middle } **Reference**

    ---

    `DbtDag` / `DbtTaskGroup` API, full YAML schema, override fields.

    [:octicons-arrow-right-24: Reference](reference/index.md)

- :material-book-open-page-variant:{ .lg .middle } **How-to**

    ---

    Recipe-style guides: route by tag, multi-runner mix, visual groups, ….

    [:octicons-arrow-right-24: How-to](how-to/index.md)

</div>

## Why runner-dbt-aws-airflow

- First-class multi-runner dispatch per DAG (`runners=`,
  `default_runner=`).
- Tag-based bulk routing via `tag_runners` (one line per layer) or the
  unified `overrides:` block.
- YAML config alternative to Python (`load_runner_config()`).
- Native runners for Glue Spark Job, Glue Interactive Session (warm +
  per-node), Glue Python Shell, EMR Serverless, and EMR-on-EC2
  cluster step.
- Per-model Glue Job naming and content-addressed worker entry
  scripts (idempotent uploads).
- Async triggers on top of `asyncio.to_thread`, so long-running Glue
  Sessions / EMR steps don't block Airflow worker slots.

## Status

- **Validated runners:** Glue Spark Job, Glue Interactive Session
  (warm + per-node), Glue Python Shell, EMR Serverless, EMR-on-EC2
  cluster step.
- **Known constraint:** Glue 3.0 Python Shell doesn't honour
  `--python-modules-installer-option`, so it can't install
  `runner-dbt-aws-airflow` directly from PyPI. Use an S3 wheel URI in
  its `--additional-python-modules` argument instead; see
  [Reference → compat](reference/compat.md).
