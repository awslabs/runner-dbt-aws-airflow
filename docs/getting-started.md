# Getting started

Five-minute walkthrough: wire a dbt project to a single Glue Spark Job runner and run it from
Airflow.

## Prerequisites

- An AWS account with permissions to create Glue Jobs and read/write S3
- An IAM role that Glue can assume (e.g. `AWSGlueServiceRole`)
- A dbt project (any adapter — local DuckDB works fine for testing)
- Apache Airflow 3.x

## 1. Install

The lib is published on PyPI (real PyPI publish coming):

```bash
pip install runner-dbt-aws-airflow
```

Or in a `uv`-managed project:

```bash
uv add dbt-aws
```

The Glue **workers** install the same wheel by passing the PyPI extra index URL
in the Glue Job's `--python-modules-installer-option` default argument
(handled automatically by the runner).

## 2. Generate your dbt manifest

The lib reads the dbt graph from `target/manifest.json` (or runs `dbt parse` to produce
one). Generate it once:

```bash
cd /path/to/your/dbt_project
dbt parse --target dev
ls target/manifest.json   # should exist
```

## 3. Wire the DAG

Drop this into your Airflow `dags_folder`:

```python title="my_dbt_dag.py" linenums="1"
from datetime import datetime
from pathlib import Path

from dbt_aws.common import ProjectConfig
from dbt_aws.common.airflow_extras.auto_deploy import build_and_upload_project_archive
from dbt_aws.common.builder import DbtDag
from dbt_aws.spark.runners import GlueSparkRunner

PROJECT = Path("/path/to/your/dbt_project")
S3_BUCKET = "my-glue-bucket"

# Uploads the dbt project to S3 once per parse (idempotent: HEAD-and-skip).
ARCHIVE_S3 = build_and_upload_project_archive(
    project_dir=PROJECT,
    cache_dir=Path("/tmp/dbt_aws_cache"),
    bucket=S3_BUCKET,
    prefix="dbt-aws",
    region_name="eu-west-1",
)

dag = DbtDag(
    dag_id="my_dbt",
    project=ProjectConfig(mode="manifest", manifest_path=PROJECT / "target/manifest.json"),
    runner=GlueSparkRunner(
        mode="create",
        iam_role_name="AWSGlueServiceRole",
        deploy_bucket=S3_BUCKET,
        deploy_prefix="dbt-aws",
        create_job_kwargs={
            "DefaultArguments": {
                "--additional-python-modules": "dbt-aws,dbt-core==1.11.11,dbt-duckdb==1.10.1",
                "--job-language": "python",
            },
            "GlueVersion": "5.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "ExecutionProperty": {"MaxConcurrentRuns": 5},  # tolerate dev re-triggers
        },
        update_config=True,
        region_name="eu-west-1",
    ),
    project_archive_s3=ARCHIVE_S3,
    target="dev",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
```

## 4. Trigger it

```bash
airflow dags unpause my_dbt
airflow dags trigger my_dbt --run-id "first-run"
```

What happens:

1. Airflow imports the DAG file. `build_and_upload_project_archive` uploads the dbt project
   archive to `s3://my-glue-bucket/dbt-aws/archives/<sha256>.tar.gz` (idempotent).
2. The dbt manifest is parsed; each model/seed/test becomes a separate Airflow task.
3. Each task: `glue:CreateJob` (or `UpdateJob` if it exists) → `glue:StartJobRun` →
   `airflow.defer` on a custom trigger that polls the Glue Job run state.
4. On the Glue worker: `pip install runner-dbt-aws-airflow ...`, download the project archive,
   run `dbt run --select <model_name>`.

You'll see one log link per task pointing at the Glue Job run page in the AWS console.

## 5. Next steps

- [**Concepts → Runners**](concepts/runners.md): the five runner shapes and when to pick each.
- [**Concepts → Routing**](concepts/routing.md): tag-based bulk routing + per-node escape hatches.
- [**How-to → Multi-runner mix**](how-to/multi-runner-mix.md): run different layers on different
  runners in one DAG.
- [**Reference → YAML config**](reference/runner-config-yaml.md): declare runners in a `.yml`
  file instead of Python.
