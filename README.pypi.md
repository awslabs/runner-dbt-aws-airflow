# runner-dbt-aws-airflow

[![PyPI](https://img.shields.io/pypi/v/runner-dbt-aws-airflow?logo=pypi&logoColor=white)](https://pypi.org/project/runner-dbt-aws-airflow/)
[![Python](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9%2B%20%7C%203.x-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

Run [dbt](https://www.getdbt.com/) projects on **AWS Glue** (Spark Jobs,
Interactive Sessions, Python Shell) and **EMR** (Serverless,
on-EC2) — orchestrated from **Apache Airflow** (including **MWAA**).
One library, five runner shapes, declarative YAML routing,
task-collapse, OpenLineage emission, AWS resource-tag compliance, and
worker-side dbt package installs so your Airflow deployment stays lean.

> **Status:** pre-1.0. Published to
> [PyPI](https://pypi.org/project/runner-dbt-aws-airflow/) for early
> adopters and CI validation. Will stabilise around 1.0. Full
> documentation (mkdocs book, per-runner constructor reference, MWAA
> quickstart, troubleshooting) ships bundled in the released wheel —
> run `runner-dbt-aws-airflow docs` after `pip install` for an offline
> browser preview.

> **Python import path:** `import dbt_aws` (the internal namespace
> package name is unchanged; only the PyPI distribution and console
> script follow the awslabs repo name).

---

## Install

```bash
# Latest release
pip install runner-dbt-aws-airflow

# With Airflow + provider extras
pip install runner-dbt-aws-airflow[airflow]

# With OpenLineage emission
pip install runner-dbt-aws-airflow[lineage]
```

Every release publishes to PyPI. See the changelog bundled inside
the wheel for per-release notes.

## Offline documentation

The wheel bundles the full mkdocs book so you can browse it locally
without a public site:

```bash
pip install runner-dbt-aws-airflow
runner-dbt-aws-airflow docs        # opens http://localhost:8000
```

Contents:

- **Getting started** — first DAG in 5 minutes.
- **Concepts** — architecture, runner shapes, routing
  (`overrides` / `tag.<name>` / `mode`), task-collapse, OpenLineage,
  MWAA deployment.
- **Reference** — per-runner constructor kwargs, YAML config schema,
  per-model overrides, `DbtDag` / `DbtTaskGroup` API, package pins.
- **How-to** — MWAA quickstart, multi-runner mix, tag routing,
  OpenLineage setup, Iceberg + Glue 5.1 collapse.
- **Troubleshooting** — MWAA + Glue Python Shell gotchas.
- **Changelog** — full release history.

## Features

**Five runner shapes** — one library, uniform API:

| Runner | Backend | Best for |
|---|---|---|
| `GlueSparkRunner` | Glue Spark Job (JobRun) | Bronze bulk transforms, medallion base layer |
| `GlueInteractiveSessionRunner` | Glue Interactive Session (warm or per-node) | Silver / gold with a shared warm session |
| `GluePythonShellRunner` | Glue Python Shell (Glue 3.0, up to 1 DPU) | dbt-athena / small Python-only transforms |
| `EmrServerlessRunner` | EMR Serverless job | High-concurrency Spark without cluster mgmt |
| `EmrClusterStepRunner` | EMR-on-EC2 cluster step | Iceberg / Spark 3.5+ / custom bootstrap |

**Declarative routing** — one `overrides:` block covers per-model and
per-tag customization. `mode: single` (one Airflow task per node,
optional `name:` task-id prefix) or `mode: group` (collapse all tagged
nodes into one Airflow task). Precedence per field is validated at
DAG-parse.

**AWS resource tags** — top-level `resource_tags:` cascades to every
runner in a `runners.yml`. Applied to `glue:CreateJob` /
`glue:CreateSession` (dict shape). Per-runner blocks and per-node /
per-tag overrides merge per key.

**Worker-side dbt-core install** — MWAA's `requirements.txt` stays
lean; workers install `dbt-core` + `dbt-duckdb` + `packages.yml` deps
on the fly.

**OpenLineage + SageMaker Unified Studio (opt-in)** — emit
`START` / `COMPLETE` events to S3 (NDJSON) or SMUS via
`datazone:PostLineageEvent`. Multi-runner DAGs collapse to one lineage
graph via a shared parent facet.

**Task-collapse (opt-in)** — fold view+consumer chains and
ephemeral drops. Proven with Glue 5.1 + native Iceberg + materialised
views.

**Cosmos-compatible API** — `DbtDag` / `DbtTaskGroup` accept
Cosmos-style `ProjectConfig`. Drop-in from Cosmos-based DAGs.

## Quickstart — DAG shape

Full walkthrough (MWAA 3.2.1 setup, sample dbt project, `runners.yml`,
DAG file, `requirements.txt`, AWS CLI deploy commands) is in the
bundled docs at `runner-dbt-aws-airflow docs → Getting started`. The
30-second version:

```python
from datetime import datetime
from pathlib import Path

from dbt_aws.common import ProjectConfig, load_runner_config
from dbt_aws.common.airflow_extras.auto_deploy import build_and_upload_project_archive
from dbt_aws.common.builder import DbtDag

PROJECT = Path(__file__).parent / "my_dbt_project"

archive_s3 = build_and_upload_project_archive(
    project_dir=PROJECT,
    bucket="my-dbt-aws-bucket",
    prefix="dbt-aws/archives/",
    region_name="eu-west-1",
)

cfg = load_runner_config(Path(__file__).parent / "runners.yml")

dag = DbtDag(
    dag_id="my_dbt_daily",
    project=ProjectConfig(
        mode="manifest",
        manifest_path=PROJECT / "target/manifest.json",
    ),
    project_archive_s3=archive_s3,
    config=cfg,
    target="prod",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
)
```

Minimal `runners.yml`:

```yaml
resource_tags:
  CostCenter: data-platform
  Environment: prod

runners:
  glue_spark:
    type: glue_spark
    mode: create
    iam_role_name: AWSGlueServiceRole
    deploy_bucket: my-dbt-aws-bucket
    region_name: eu-west-1
    worker_type: G.1X
    number_of_workers: 2

default_runner: glue_spark

overrides:
  tag.bronze:
    mode: single
    name: bronze
    worker_type: G.2X
    number_of_workers: 4
```

## Contributing

Contributions welcome. See the `CONTRIBUTING` guide in the repo for
setup, style, and PR expectations. Style is enforced by
`ruff` + `mypy --strict` via `pre-commit`; tests run on every commit.

## Security

If you discover a potential security issue, please **do not** open a
public GitHub issue. Report it via the AWS Security
[vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/)
instead. Full policy in `SECURITY.md`.

## License

Apache-2.0. See the `LICENSE` file in the repo, or
[apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0).
