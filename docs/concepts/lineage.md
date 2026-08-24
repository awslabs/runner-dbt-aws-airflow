# OpenLineage + SageMaker Unified Studio

*Opt-in feature.*

dbt-aws can emit [OpenLineage](https://openlineage.io/) events for every
dbt node it runs on AWS -- Glue Spark Job, Glue Interactive Session,
Glue Python Shell, EMR Serverless, and EMR Cluster Step. Two lineage
"sinks" ship out of the box:

1. **S3 archive** -- NDJSON files at `s3://<bucket>/openlineage/<run_id>/<node>.ndjson`.
   Portable, replayable, works with any OpenLineage backend later.
2. **SageMaker Unified Studio (SMUS)** -- direct ingest via
   `datazone:PostLineageEvent`. Events show up in the SMUS UI as
   real-time lineage graphs.

Both can be enabled simultaneously via OpenLineage's `CompositeTransport`.

## When to enable it

Every dbt run through dbt-aws produces useful lineage:

* **Which dbt models did this DAG run?** `job.name` per event.
* **What did each model read / write?** `inputs`, `outputs` on
  START / COMPLETE events. Includes column-level schemas for
  duckdb-backed models.
* **Did it succeed?** `eventType = COMPLETE` vs `FAIL` (dbt-ol emits
  FAIL for failed dbt nodes -- captured, not swallowed).
* **Multi-runner DAGs collapse to one graph.** Every emitted event
  carries the same parent facet (derived from the Airflow `run_id`),
  so SMUS shows one lineage graph per DAG execution even when the
  physical models ran on 3 different backends (Glue Spark + EMR
  Serverless + EMR Cluster Step).

## Enabling it

### 1. Install the extra

The lineage code is opt-in via a pip extra:

```bash
pip install 'runner-dbt-aws-airflow[lineage]'
```

This pulls `openlineage-python==1.50.0` and `openlineage-dbt==1.50.0`.
Without the extra, `openlineage=...` on a runner raises a clear
`ImportError`. The base wheel behavior is byte-identical to before.

### 2. Bake OL packages into the worker

Workers install their own Python packages via
`--additional-python-modules` (Glue), the EMR-on-EC2 bootstrap script,
or the EMR Serverless venv-pack. Use the `openlineage_pip_specs()`
helper:

```python
from dbt_aws.common.lineage import openlineage_pip_specs
from dbt_aws.compat import GLUE_PY311_PACKAGES

worker_packages = ",".join([GLUE_PY311_PACKAGES, *openlineage_pip_specs()])
```

The helper returns a tuple with `openlineage-python==<pinned>` and
`openlineage-dbt==<pinned>`. Version pins use `==` because AWS Glue's
`--additional-python-modules` splits on commas *before* pip sees the
requirement, so `openlineage-python>=1.20,<2` would break.

For EMR Cluster Step, ship an EMR bootstrap script that runs
`pip install 'runner-dbt-aws-airflow[worker,lineage]'` on the primary
instance during bootstrap-actions (add
`openlineage-python>=1.20,<2` and `openlineage-dbt>=1.20,<2`
explicitly if you also want lineage). For EMR Serverless, add
`openlineage-python` + `openlineage-dbt` to the venv-pack build
recipe in
[Troubleshooting → EMR Serverless: HTTP timeout to `extensions.duckdb.org`](../troubleshooting.md#emr-serverless-http-timeout-to-extensionsduckdborg).

### 3. Configure lineage on your runner

```python
from dbt_aws.common.lineage import OpenLineageConfig
from dbt_aws.spark.runners import GlueSparkRunner

lineage = OpenLineageConfig(
    namespace="my-project",
    s3_uri="s3://my-bucket/openlineage/",
    smus_domain_id="dzd_abc123",   # optional
    smus_region="us-east-1",       # required when smus_domain_id set
)

runner = GlueSparkRunner(
    mode="create",
    iam_role_name="Glue-Job-Role",
    script_location="s3://my-bucket/entrypoint.py",
    region_name="us-east-1",
    openlineage=lineage,           # <- this line enables OL
)
```

Every task the runner produces will:

1. Write a per-task `openlineage.yml` next to `dbt_project.yml` on
   the worker that configures the composite transport.
2. Export the OL parent-run env vars (`OPENLINEAGE_PARENT_ID` etc.)
   before dbt runs.
3. Wrap `dbt` in `dbt-ol` so OL events are emitted at the end of
   each dbt invocation.
4. Upload the NDJSON events file to
   `<s3_uri>/<parent_run>/<node_unique_id>.ndjson` after dbt finishes.
5. If `smus_domain_id` is set, additionally POST every event to the
   DataZone `PostLineageEvent` API.

Any failure in the OL pipeline is logged but does NOT change dbt's
exit code -- lineage is best-effort by design.

### 4. Multi-runner: one lineage per DAG execution

The magic is that every runner in the same DAG can share the same
`OpenLineageConfig`. The default
`parent_run_id_template="{{ run_id }}"` renders to the Airflow
`run_id`, so every physical run on every backend declares the same
OL parent facet:

```python
shared_lineage = OpenLineageConfig(
    namespace="medallion",
    s3_uri="s3://my-bucket/openlineage/",
)

glue_spark = GlueSparkRunner(..., openlineage=shared_lineage)
emr_cluster = EmrClusterStepRunner(..., openlineage=shared_lineage)
emr_serverless = EmrServerlessRunner(..., openlineage=shared_lineage)

dag = DbtDag(
    runners={"glue_spark": glue_spark,
             "emr_cluster": emr_cluster,
             "emr_serverless": emr_serverless},
    default_runner="glue_spark",
    overrides={
        # Bronze -> Glue Spark
        "model.medallion.br_nation":       {"runner": "glue_spark"},
        # Silver -> EMR Serverless
        "model.medallion.sv_dim_supplier": {"runner": "emr_serverless"},
        # Gold  -> EMR Cluster Step
        "model.medallion.gd_top_customers":{"runner": "emr_cluster"},
    },
    ...
)
```

Three physical AWS runs, one lineage graph in SMUS. A complete
17-model medallion DAG exercising this pattern end-to-end is available
as `dag_test_15_medallion_multi_runner_ol.py` in the repository's
example DAGs.

## YAML config

The same knobs are available in the YAML runner config. Top-level
`openlineage:` applies to every runner; per-runner `openlineage:` wins
locally; per-runner `openlineage: null` opts out that runner:

```yaml
runners:
  glue_spark:
    type: glue_spark
    job_name: bronze-loader
  emr_serverless:
    type: emr_serverless
    application_id: 00ab...

default_runner: glue_spark

openlineage:
  namespace: medallion
  s3_uri: s3://my-bucket/openlineage/
  smus_domain_id: dzd_abc123
  smus_region: us-east-1
```

## What the events look like

Each dbt task uploads one NDJSON file to S3:
`s3://<bucket>/openlineage/<parent_run_id>/<node_unique_id>.ndjson`.

The file contains 4 events per model: 2 START (wrapper + node) and 2
COMPLETE (or FAIL). Each event carries:

* `run.runId` -- unique per event pair.
* `run.facets.parent` -- the shared parent facet (Airflow run_id
  encoded as UUID5).
* `job.namespace` / `job.name`.
* `inputs` / `outputs` -- dataset names (typically
  `<database>.<schema>.<table>`).
* schema, statistics, sqlJob facets -- when dbt-duckdb provides them.

Sample event:

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2026-07-03T12:04:32.140Z",
  "run": {
    "runId": "019f27dc-cb63-7ba6-bce9-ae35bbfb4f4b",
    "facets": {
      "parent": {
        "run": {"runId": "e6bcc13a-4b4f-5735-8acd-4304d79331e4"},
        "job": {"namespace": "airflow", "name": "test_15_medallion_multi_runner_ol"}
      }
    }
  },
  "job": {"namespace": "medallion", "name": "dbt_project.main.dbt_project.br_nation"},
  "inputs": [{"namespace": "dbt_project", "name": "main.n_input_seed"}],
  "outputs": [{"namespace": "dbt_project", "name": "main.br_nation"}]
}
```

## Terraform for SMUS

`infra/terraform/smus/` provisions the DataZone domain + KMS + IAM
grants your runner role needs to POST lineage events. See the
module's README (`infra/terraform/smus/README.md` in the repo) for
the setup steps.

Output of `terraform apply`:

```
smus_domain_id      = "dzd_a1b2c3d4"
openlineage_s3_uri  = "s3://dbt-aws-ol-archive-<account>/"
```

Wire both into your `OpenLineageConfig` and you're done.

## Known limitations

* **Python Shell runner + dbt-ol.** Glue Python Shell caps at
  Python 3.9; dbt-core 1.10+ requires 3.10+. Lineage feature is not
  a blocker -- it's the underlying dbt-core constraint.
* **Local Airflow 3.2.1 Triggerer.** The bundled Triggerer has a race
  in `sync_state_to_supervisor` that kills the trigger runner under
  any deferrable load. All OL example DAGs use `deferrable=False` to
  sidestep. MWAA / real Airflow deployments are unaffected.
* **dbt-duckdb cross-worker `:memory:` state.** When two dbt models
  in the same DAG run on different Glue workers, the second model
  can't see the first's `:memory:` tables. Use `external`
  materialization + explicit `read_parquet(...)` in the downstream
  model to bridge. Not a lineage issue -- lineage still captures both
  runs correctly.
