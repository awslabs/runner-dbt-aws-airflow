# How-to: MWAA quickstart

End-to-end walkthrough for deploying a `runner-dbt-aws-airflow`
DAG to an existing **MWAA 3.2.1** environment.

Assumes you already have an MWAA environment. Creating an MWAA env
from scratch (VPC, subnets, security groups, execution role) is
out of scope; see the
[AWS MWAA quickstart](https://docs.aws.amazon.com/mwaa/latest/userguide/mwaa-get-started.html)
first. Once your environment is `AVAILABLE`, come back here.

## Prerequisites

- An MWAA environment on **Airflow 3.2.1** (`mw1.small` or larger).
- The MWAA execution role has `s3:GetObject` / `s3:PutObject` on:
    - The MWAA S3 bucket (DAGs, `requirements.txt`).
    - Your dbt archive bucket (or the same bucket, another prefix).
- A Glue execution IAM role (e.g. `AWSGlueServiceRole` + extra S3
  read/write on the archive bucket).
- A dbt project on your workstation. See
  [Getting started](../getting-started.md) for the minimum shape.

The MWAA execution role also needs a small addition to invoke
Glue on your behalf:

```json title="MWAA execution role -- inline policy addition"
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:CreateJob",
                "glue:UpdateJob",
                "glue:GetJob",
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:GetJobRuns",
                "glue:BatchStopJobRun",
                "iam:PassRole"
            ],
            "Resource": "*"
        }
    ]
}
```

`iam:PassRole` is required because MWAA passes the Glue execution
role name into `glue:CreateJob`. Constrain the `Resource` list to
your specific Glue role ARN in production.

## 1. Prepare the dbt project

Generate `target/manifest.json` locally:

```bash
cd my_dbt_project
dbt parse --target dev
```

Commit `target/manifest.json` alongside the DAG, or produce it in
CI before syncing to S3.

## 2. Write the DAG

`dags/my_dbt_dag.py` (see [Getting started &rarr; Wire the DAG](../getting-started.md#4-wire-the-dag)
for the full source). One-runner Glue Spark:

```python
from datetime import datetime
from pathlib import Path

from dbt_aws.common import ProjectConfig
from dbt_aws.common.airflow_extras.auto_deploy import build_and_upload_project_archive
from dbt_aws.common.builder import DbtDag
from dbt_aws.spark.runners import GlueSparkRunner

# MWAA mounts DAG files at /usr/local/airflow/dags/. The dbt project
# rides alongside them (sync it to the same S3 prefix).
DAGS_DIR = Path("/usr/local/airflow/dags")
PROJECT = DAGS_DIR / "my_dbt_project"
S3_BUCKET = "my-dbt-aws-bucket"
REGION = "us-east-1"

ARCHIVE_S3 = build_and_upload_project_archive(
    project_dir=PROJECT,
    cache_dir=Path("/tmp/dbt_aws_cache"),
    bucket=S3_BUCKET,
    prefix="dbt-aws",
    region_name=REGION,
)

dag = DbtDag(
    dag_id="my_dbt_daily",
    project=ProjectConfig(
        mode="manifest",
        manifest_path=PROJECT / "target/manifest.json",
    ),
    runner=GlueSparkRunner(
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
    ),
    project_archive_s3=ARCHIVE_S3,
    target="dev",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
)
```

## 3. MWAA `requirements.txt`

MWAA 3.2.1 uses `constraints-3.2.1-python3.12.txt` upstream, but
they conflict with MWAA's base-image pins. Skip the
`--constraint` line and let MWAA's own image drive the resolver:

```text title="requirements.txt"
runner-dbt-aws-airflow[airflow]==1.0.0
```

That's it. You do **not** need `dbt-core` in `requirements.txt`
&mdash; workers install `dbt-core` + `dbt-spark[session]`
themselves at task-run time via
`--additional-python-modules` (see the runner config above).

## 4. Sync to MWAA's S3 bucket

```bash
# MWAA bucket layout: /dags/, /requirements.txt (versioned).
aws s3 sync ./dags/ "s3://<mwaa-bucket>/dags/"
aws s3 cp requirements.txt "s3://<mwaa-bucket>/requirements.txt"

# MWAA must be told the new requirements.txt object version. Grab
# it and pass to update-environment.
VERSION=$(aws s3api list-object-versions \
    --bucket <mwaa-bucket> \
    --prefix requirements.txt \
    --query 'Versions[0].VersionId' \
    --output text)

aws mwaa update-environment \
    --name <mwaa-env-name> \
    --requirements-s3-object-version "$VERSION"
```

MWAA takes ~10-15 minutes to reload. Watch the
`CreateEnvironmentUpdate` status in the MWAA console or:

```bash
aws mwaa get-environment --name <mwaa-env-name> \
    --query 'Environment.Status'
```

When `AVAILABLE`, your DAG appears in the Airflow UI.

## 5. Trigger the first run

- Unpause `my_dbt_daily` in the Airflow UI.
- Trigger a manual run.
- Each dbt model becomes an Airflow task. On trigger:
    1. Airflow imports the DAG. `build_and_upload_project_archive`
       uploads the archive (idempotent; skips if already there).
    2. Each task calls `glue:CreateJob` (or `UpdateJob`) then
       `glue:StartJobRun`, then defers to a custom trigger that
       polls the Glue Job run state.
    3. On the Glue worker: `pip install runner-dbt-aws-airflow
       ...`, download the project archive, run `dbt run --select
       <model_name>`.

Task log links point directly at the Glue Job run page in the AWS
console.

## Troubleshooting

- **`ModuleNotFoundError: airflow.providers.amazon`** in MWAA
  scheduler logs &mdash; MWAA's base image ships the Amazon
  provider. If you added it to `requirements.txt` yourself, remove
  it and let MWAA's baseline resolve.
- **DAG stays paused** after `update-environment` &mdash; MWAA
  installs `requirements.txt` before it re-parses DAGs. Wait 5-10
  minutes after the environment shows `AVAILABLE`.
- **`ConcurrentRunsExceededException`** &mdash; bump
  `create_job_kwargs.ExecutionProperty.MaxConcurrentRuns` (Glue's
  default is 1) and set `update_config: true`.

Full MWAA + Glue troubleshooting matrix:
[Troubleshooting](../troubleshooting.md).

## Next steps

- [How-to &rarr; Route by tag](route-by-tag.md) &mdash; send bronze / silver / gold to different runners.
- [How-to &rarr; Multi-runner mix](multi-runner-mix.md) &mdash; Glue Spark + Interactive Session + EMR in one DAG.
- [How-to &rarr; Enable OpenLineage](enable-openlineage.md) &mdash; emit lineage events to S3 or SMUS.
