# Getting started

Wire a dbt project to a Glue Spark Job runner and run it from
Airflow (locally or on MWAA). Copy-paste-and-run.

## Prerequisites

- An AWS account with permissions to create Glue Jobs, read/write
  an S3 bucket, and assume an IAM role.
- An IAM role Glue can assume with `AWSGlueServiceRole` +
  read/write on your project bucket.
- Airflow 2.9+ or 3.x. MWAA 3.2.1 is the recommended managed
  runtime; see [How-to &rarr; MWAA quickstart](how-to/mwaa-quickstart.md)
  for the end-to-end deploy walkthrough.
- Python 3.10-3.12 in the Airflow scheduler venv (for local dev).

## 1. Install

```bash
pip install runner-dbt-aws-airflow

# With Airflow + AWS provider extras
pip install "runner-dbt-aws-airflow[airflow]"
```

Or in a `uv`-managed project:

```bash
uv add runner-dbt-aws-airflow
```

The Python import path stays `dbt_aws` (PEP 420 namespace package):

```python
from dbt_aws.common import ProjectConfig, load_runner_config
from dbt_aws.common.builder import DbtDag
from dbt_aws.spark.runners import GlueSparkRunner
```

## 2. Sample dbt project

Minimum shape &mdash; one seed, one model, `dbt-spark` on the Glue
worker (both locally via the `session` method and on Glue):

```
my_dbt_project/
├── dbt_project.yml
├── profiles.yml
├── seeds/
│   └── raw_orders.csv
└── models/
    └── stg_orders.sql
```

`dbt_project.yml`:

```yaml
name: my_dbt_project
version: '1.0.0'
config-version: 2
profile: my_dbt_project

seeds:
  my_dbt_project:
    +schema: raw

models:
  my_dbt_project:
    +materialized: table
    +schema: analytics
```

`profiles.yml` &mdash; dbt adapter connection profile. Carries no
credentials; the Glue worker authenticates to AWS via its IAM role
and dbt-spark's `session` method binds to the SparkSession the
Glue runtime already provides:

```yaml
my_dbt_project:
  target: dev
  outputs:
    dev:
      type: spark
      method: session
      schema: analytics
      threads: 4
```

`seeds/raw_orders.csv`:

```csv
id,customer_id,order_total,ordered_at
1,10,100.00,2026-01-01
2,20,200.00,2026-01-02
3,30,300.00,2026-01-03
```

`models/stg_orders.sql`:

```sql
{{ config(tags=['bronze']) }}

select
    id,
    customer_id,
    order_total,
    ordered_at
from {{ ref('raw_orders') }}
where order_total > 0
```

## 3. Generate your dbt manifest

The library reads the dbt graph from `target/manifest.json`.
Regenerate it whenever the project changes:

```bash
cd my_dbt_project
dbt parse --target dev
ls target/manifest.json   # should exist
```

Commit `target/manifest.json` alongside the DAG, or produce it in
your CI before syncing DAGs to S3.

## 4. Wire the DAG

Drop this into your Airflow `dags_folder`:

```python title="my_dbt_dag.py" linenums="1"
from datetime import datetime
from pathlib import Path

from dbt_aws.common import ProjectConfig
from dbt_aws.common.airflow_extras.auto_deploy import build_and_upload_project_archive
from dbt_aws.common.builder import DbtDag
from dbt_aws.spark.runners import GlueSparkRunner

PROJECT = Path("/path/to/your/my_dbt_project")
S3_BUCKET = "my-glue-bucket"
REGION = "us-east-1"

# Uploads the dbt project archive to S3 once per DAG parse (idempotent).
ARCHIVE_S3 = build_and_upload_project_archive(
    project_dir=PROJECT,
    cache_dir=Path("/tmp/dbt_aws_cache"),
    bucket=S3_BUCKET,
    prefix="dbt-aws",
    region_name=REGION,
)

runner = GlueSparkRunner(
    mode="create",
    iam_role_name="AWSGlueServiceRole",
    deploy_bucket=S3_BUCKET,
    deploy_prefix="dbt-aws",
    region_name=REGION,
    worker_type="G.1X",
    number_of_workers=2,
    glue_version="5.0",
    create_job_kwargs={
        "DefaultArguments": {
            "--additional-python-modules": (
                "runner-dbt-aws-airflow==1.0.0,"
                "dbt-core==1.11.11,"
                "dbt-spark[session]==1.9.3"
            ),
            "--job-language": "python",
        },
        "ExecutionProperty": {"MaxConcurrentRuns": 5},
    },
    update_config=True,
)

dag = DbtDag(
    dag_id="my_dbt",
    project=ProjectConfig(
        mode="manifest",
        manifest_path=PROJECT / "target/manifest.json",
    ),
    runner=runner,
    project_archive_s3=ARCHIVE_S3,
    target="dev",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
```

## 5. Trigger it

Locally:

```bash
airflow dags unpause my_dbt
airflow dags trigger my_dbt --run-id "first-run"
```

MWAA: sync the DAG file + `manifest.json` to your MWAA S3 bucket
and trigger from the Airflow UI. See
[How-to &rarr; MWAA quickstart](how-to/mwaa-quickstart.md) for the
step-by-step.

What happens on trigger:

1. Airflow imports the DAG file. `build_and_upload_project_archive`
   uploads the dbt project archive to
   `s3://<bucket>/dbt-aws/archives/<sha256>.tar.gz` (idempotent).
2. Each model / seed / test in `manifest.json` becomes an Airflow
   task, wired by `depends_on`.
3. Each task: `glue:CreateJob` (or `UpdateJob` if it exists) &rarr;
   `glue:StartJobRun` &rarr; `airflow.defer` on a custom trigger
   that polls the Glue Job run state.
4. On the Glue worker: `pip install runner-dbt-aws-airflow==1.0.0
   dbt-core==1.11.11 dbt-spark[session]==1.9.3`, download the
   archive, run `dbt run --select <model_name>`.

You'll see one log link per task pointing at the Glue Job run
page in the AWS console.

## 6. Alternative install paths

For **Glue 6.0** (Python 3.13, Spark 4.1.1), swap the install
string to:

```
runner-dbt-aws-airflow==1.0.0,dbt-core==1.12.3,dbt-spark[session]==1.11.0
```

and set `glue_version="6.0"` on the runner.

For **Glue Python Shell 3.0**, PyPI isn't reachable from the
worker &mdash; mirror the wheel to S3 and reference it there. See
[Reference &rarr; compat](reference/compat.md#compatibility-matrix)
for the full version matrix.

## Next steps

- [**Concepts &rarr; Runners**](concepts/runners.md) &mdash; the five runner shapes and when to pick each.
- [**Concepts &rarr; Routing**](concepts/routing.md) &mdash; per-tag / per-node routing via `overrides:`.
- [**How-to &rarr; MWAA quickstart**](how-to/mwaa-quickstart.md) &mdash; end-to-end MWAA deployment.
- [**How-to &rarr; Multi-runner mix**](how-to/multi-runner-mix.md) &mdash; different dbt layers on different backends.
- [**Reference &rarr; YAML config**](reference/runner-config-yaml.md) &mdash; declare runners in a `.yml` file instead of Python.
