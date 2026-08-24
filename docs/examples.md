# Examples

End-to-end usage examples. Each example points at a How-to page in
this book with a self-contained recipe and copy-pasteable code.

## Single-runner Glue Spark DAG

Simplest shape: one runner, one DAG, all dbt nodes go through the
same Glue Spark Job.

Walkthrough: [Getting started](getting-started.md).

## Multi-runner mix (per-tag routing)

Different dbt layers on different AWS backends in one DAG. Bronze on
a Glue Spark Job, silver/gold on a warm Glue Interactive Session.

Walkthrough: [How-to: Multi-runner mix](how-to/multi-runner-mix.md).

Minimal shape:

```python
from dbt_aws.common import ProjectConfig
from dbt_aws.common.builder import DbtDag
from dbt_aws.spark.runners import GlueSparkRunner, GlueInteractiveSessionRunner

dag = DbtDag(
    dag_id="medallion",
    project=ProjectConfig(mode="manifest", manifest_path="target/manifest.json"),
    runners={
        "glue_spark":   GlueSparkRunner(mode="create", iam_role_name="GlueRole"),
        "session_warm": GlueInteractiveSessionRunner(iam_role_arn="...", reusable=True),
    },
    default_runner="session_warm",
    tag_runners={
        "bronze":      "glue_spark",
        "silver,gold": "session_warm",
    },
    project_archive_s3="s3://my-bucket/dbt-archives/abc.tar.gz",
)
```

## YAML-driven multi-runner mix

Same shape as above, but the runner objects, routing, and visual
grouping live in a `runners.yml` file for ops-friendly editing. The
Python DAG file is a thin loader.

Walkthrough: [Reference: YAML config](reference/runner-config-yaml.md).

Minimal shape:

```python
from pathlib import Path

from dbt_aws.common import ProjectConfig, load_runner_config
from dbt_aws.common.builder import DbtDag

cfg = load_runner_config(Path("runners.yml"))

dag = DbtDag(
    dag_id="medallion_yaml",
    project=ProjectConfig(mode="manifest", manifest_path="target/manifest.json"),
    config=cfg,
    project_archive_s3="s3://my-bucket/dbt-archives/abc.tar.gz",
)
```

## Per-tag task-collapse ("group" mode)

Fold a group of tagged dbt nodes into one Airflow task. Useful for
gold-layer chains where the dbt-side dependency graph is fine but you
want a single Airflow task per tag.

Walkthrough: [How-to: Bulk-by-tag task collapse](how-to/tag-groups-bulk-collapse.md).

## OpenLineage emission from a Glue runner

Emit `START` / `COMPLETE` lineage events to S3 (NDJSON) or SageMaker
Unified Studio (SMUS) via `datazone:PostLineageEvent`.

Walkthrough: [How-to: Enable OpenLineage](how-to/enable-openlineage.md).

## Iceberg + Glue 5.1 collapse

Native Iceberg tables and materialised views on Glue 5.1, with
task-collapse folding view-chain overhead into single Airflow tasks.

Walkthrough:
[How-to: Collapse + Iceberg Materialized Views on Glue 5.1](how-to/collapse-iceberg-glue.md).

## Related pages

- [Concepts &rarr; Runners](concepts/runners.md) &mdash; the five runner shapes and when to use each.
- [Concepts &rarr; Routing](concepts/routing.md) &mdash; `overrides` / `tag.<name>` / `mode` semantics.
- [Reference &rarr; Runner overrides](reference/runner-overrides.md) &mdash; every per-node knob.
- [Troubleshooting](troubleshooting.md) &mdash; MWAA + Glue Python Shell known issues.
