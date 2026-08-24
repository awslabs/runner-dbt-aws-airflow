# dbt-aws

Run [dbt](https://www.getdbt.com/) projects on **AWS Glue** (Spark
Jobs, Interactive Sessions, Python Shell) and **Amazon EMR**
(Serverless, on-EC2) &mdash; orchestrated from **Apache Airflow**
(including **MWAA**).

One library, five runner shapes, declarative routing, visual
grouping, and **worker-side dbt package installs** so your Airflow
deployment stays lean.

> **PyPI distribution:** `runner-dbt-aws-airflow` &middot;
> **Python import:** `import dbt_aws` &middot;
> **Repo:** <https://github.com/awslabs/runner-dbt-aws-airflow>

## Why runner-dbt-aws-airflow

- **One DAG, multiple compute backends.** Route bronze to a Glue
  Spark Job, silver/gold to a warm Glue Interactive Session, tiny
  Athena transforms to Glue Python Shell &mdash; from one Airflow
  DAG, without hand-rolling five operators.
- **Declarative YAML routing.** `overrides[tag.<name>]` and
  `overrides[model.<uid>]` bulk-route or per-model-tweak every knob
  the runner exposes (worker type, timeout, `--vars`, profile,
  target, `resource_tags`, ...).
- **Lean MWAA `requirements.txt`.** Workers install
  `dbt-core` + adapter (`dbt-spark`, `dbt-duckdb`, `dbt-athena`, ...)
  at task-run time via `--additional-python-modules` or EMR
  bootstrap. MWAA never needs `dbt-core` in its own requirements.
- **AWS resource tags.** Top-level `resource_tags:` cascades to
  every runner (Glue Job, Glue Session, EMR application). Per-tag /
  per-model overrides layer on top.
- **IAM-first security posture.** The Glue / EMR worker
  authenticates to AWS via its IAM instance / task role. The dbt
  `profiles.yml` is a plain adapter connection profile
  (`type: spark, method: session`) and carries no credentials.
- **OpenLineage + SageMaker Unified Studio.** Opt-in emission of
  `START` / `COMPLETE` events to S3 (NDJSON) or SMUS
  (`datazone:PostLineageEvent`). Multi-runner DAGs collapse into
  one lineage graph via a shared parent facet.
- **Task-collapse.** Fold view+consumer chains and ephemeral drops
  into a single Airflow task. Proven with Glue 5.1 + native
  Iceberg + materialised views.
- **Cosmos-compatible API.** `DbtDag` / `DbtTaskGroup` accept the
  standard `ProjectConfig` shape.

## Runner shapes

| Runner | Backend |
|---|---|
| `GlueSparkRunner` | AWS Glue Spark Job |
| `GlueInteractiveSessionRunner` | AWS Glue Interactive Session (warm or per-node) |
| `GluePythonShellRunner` | AWS Glue Python Shell (Glue 3.0, 1 DPU) |
| `EmrServerlessRunner` | Amazon EMR Serverless |
| `EmrClusterStepRunner` | Amazon EMR-on-EC2 cluster step |

## Verified end-to-end

All five runners were exercised against real AWS in `us-east-1`
using `runner-dbt-aws-airflow 1.0.0`; Glue 6.0 additionally passed
with `dbt-core 1.12.3` + `dbt-spark[session] 1.11.0`. See
[Reference &rarr; compat](reference/compat.md#verified-end-to-end-2026-08-24)
for the full table.

## Quick links

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Get started in 5 minutes**

    ---

    Wire your first dbt project to a Glue Spark Job runner and run
    it from Airflow. Complete sample project + DAG.

    [:octicons-arrow-right-24: Getting started](getting-started.md)

-   :material-puzzle:{ .lg .middle } **Concepts**

    ---

    Architecture, runner shapes, routing, task-collapse,
    OpenLineage, MWAA deployment.

    [:octicons-arrow-right-24: Concepts](concepts/index.md)

-   :material-code-tags:{ .lg .middle } **Reference**

    ---

    `DbtDag` / `DbtTaskGroup` API, full YAML schema, override
    fields, compatibility matrix.

    [:octicons-arrow-right-24: Reference](reference/index.md)

-   :material-book-open-page-variant:{ .lg .middle } **How-to**

    ---

    Recipe-style guides: MWAA quickstart, route by tag,
    multi-runner mix, OpenLineage.

    [:octicons-arrow-right-24: How-to](how-to/index.md)

</div>

## Install

```bash
pip install runner-dbt-aws-airflow

# With Airflow + provider extras
pip install "runner-dbt-aws-airflow[airflow]"

# With OpenLineage emission
pip install "runner-dbt-aws-airflow[lineage]"
```

The Python import path stays `dbt_aws` (PEP 420 namespace package)
&mdash; only the PyPI distribution + CLI name follow the repo:

```python
from dbt_aws.common.builder import DbtDag, DbtTaskGroup
from dbt_aws.common import ProjectConfig, load_runner_config
```

## Two deployment variants (pick the one that matches your networking)

| Variant | Workers have internet? | MWAA needs `dbt-core`? | Where `dbt deps` runs |
|---|---|---|---|
| **A** (most users) | Yes | No | On the worker, in `/tmp/<run-id>/project/dbt_packages/` |
| **B** (air-gapped) | No | Yes | On the Airflow scheduler; `dbt_packages/` baked into the archive |

**Variant A** is the default. MWAA `requirements.txt` needs only the
`runner-dbt-aws-airflow` wheel; workers install `packages.yml` deps
themselves at task-run time. The library does not import dbt anywhere
at DAG-parse time (`manifest.json` is parsed as plain JSON), so DAG
parse can't fail with `ModuleNotFoundError` even when `dbt-core`
isn't installed on the scheduler.

**Variant B** applies when workers can't reach the internet
(private subnets with only VPC endpoints, corporate egress
firewalls). See
[Concepts &rarr; Deployment &rarr; Two deployment variants](concepts/deployment.md#two-deployment-variants)
for the switch.
