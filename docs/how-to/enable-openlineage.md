# Enable OpenLineage on a Glue Spark DAG

Minimal 5-step walkthrough. Assumes you already have a working
`dag_test_01_glue_spark.py`-style DAG on Glue Spark; this how-to adds
lineage to it.

Full worked example lives in
`dag_test_07_glue_spark_openlineage.py` (published under the
repository's example DAGs).

## 1. Install the extra locally

```bash
pip install 'runner-dbt-aws-airflow[lineage]'
```

## 2. Add the OL packages to the worker's pip list

`--additional-python-modules` is a comma-separated string. Splice in
the OL specs via the helper so the pins stay in one place:

```python
from dbt_aws.common.lineage import openlineage_pip_specs
from dbt_aws.compat import GLUE_PY311_PACKAGES

ADDITIONAL_PYTHON_MODULES = ",".join(
    [GLUE_PY311_PACKAGES, *openlineage_pip_specs()]
)
```

## 3. Build an `OpenLineageConfig`

```python
from dbt_aws.common.lineage import OpenLineageConfig

ol = OpenLineageConfig(
    namespace="my-project",
    s3_uri="s3://my-bucket/openlineage/",
    # Optional SMUS ingest (leave out for S3-only):
    # smus_domain_id="dzd_abc123",
    # smus_region="us-east-1",
)
```

Only `s3_uri` OR `smus_domain_id` is required; both can be set
simultaneously (composite transport).

## 4. Pass it to the runner

```python
from dbt_aws.spark.runners import GlueSparkRunner

runner = GlueSparkRunner(
    mode="create",
    iam_role_name="Glue-Job-Role",
    script_location="s3://my-bucket/entrypoint.py",
    create_job_kwargs={
        "DefaultArguments": {
            "--additional-python-modules": ADDITIONAL_PYTHON_MODULES,
            "--job-language": "python",
        },
        "GlueVersion": "5.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
    },
    region_name="us-east-1",
    openlineage=ol,   # <-- this
)
```

## 5. Wire the DAG and run it

Exactly like a non-OL DAG:

```python
from datetime import datetime
from dbt_aws.common import ProjectConfig
from dbt_aws.common.builder import DbtDag

dag = DbtDag(
    dag_id="my_dag_with_lineage",
    project=ProjectConfig(
        mode="manifest",
        manifest_path="target/manifest.json",
    ),
    runners={"glue_spark": runner},
    default_runner="glue_spark",
    project_archive_s3="s3://my-bucket/archives/abc.tar.gz",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
```

## Verify

After a run, inspect S3:

```bash
aws s3 ls s3://my-bucket/openlineage/<run-id>/ --recursive
```

Expected output:

```
2026-07-03 10:00:00      13902 openlineage/manual__2026-07-03/model.my_project.my_model.ndjson
```

Each `.ndjson` file has 4 events (2 START + 2 COMPLETE per model).
Inspect one:

```bash
aws s3 cp s3://my-bucket/openlineage/<run-id>/model.my_project.my_model.ndjson - \
  | jq -c '{event: .eventType, job: .job.name, in: [.inputs[]?.name], out: [.outputs[]?.name]}'
```

## Turning it off per DAG

`openlineage=None` (or just leave the kwarg out) restores the exact
earlier behavior. See the `dag_test_07b_glue_spark_no_openlineage.py`
example for the regression baseline.

## Extending to other runners

Same pattern -- pass the same `openlineage=` to a
`GlueInteractiveSessionRunner` or `GluePythonShellRunner`. All emit
events compatible with the same S3 layout and (if set) SMUS domain.
A multi-model medallion DAG that shares one `OpenLineageConfig`
across the Glue backends is available as
`dag_test_15_medallion_multi_runner_ol.py` in the repository's
example DAGs.

## Cheat sheet: runner-specific extra setup

| Runner | Extra prep |
|---|---|
| Glue Spark Job | none beyond `--additional-python-modules` |
| Glue Interactive Session | same as Glue Spark |
| Glue Python Shell | ⚠️ blocked (dbt-core requires Py 3.10+, Glue Python Shell caps at 3.9) |
