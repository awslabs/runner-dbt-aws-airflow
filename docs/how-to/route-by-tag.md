# How to: route by tag

**Goal.** Send every dbt model carrying a given tag through a specific
runner (or with specific profile/target/command/etc.) without touching
the DAG for every model.

Routing is expressed through the unified `overrides:` block. See the
[Routing concept page](../concepts/routing.md) for the semantics.

## The recipe

Tag your dbt model:

```sql
-- models/bronze/orders.sql
{{ config(tags=["bronze"]) }}
select ...
```

Then in your `runner.yml`:

```yaml
runners:
  spark:
    type: glue_spark
    job_name: dbt-aws-spark
    mode: attach
  shell:
    type: glue_python_shell
    job_name: dbt-aws-shell
default_runner: spark

overrides:
  tag.bronze:
    runner: spark           # every bronze model runs on the spark runner
    worker_type: G.2X       # any OVERRIDE_TYPE field works
    number_of_workers: 4
    command: build          # dbt build, not dbt run
```

Load and pass to the DAG:

```python
from dbt_aws.common import load_runner_config
from dbt_aws.common.builder import DbtDag

cfg = load_runner_config("runner.yml")
dag = DbtDag(
    dag_id="my_dbt",
    project=ProjectConfig(...),
    project_archive_s3="s3://.../archive.tar.gz",
    config=cfg,
    start_date=datetime(2026, 1, 1),
)
```

Every model with `tags: ["bronze"]` inherits the `runner`, `worker_type`,
`number_of_workers`, `command` values from `overrides[tag.bronze]`.

## Multiple tags on one model

A model can carry multiple tags. Each `overrides[tag.<t>]` entry
contributes the fields it declares:

```sql
{{ config(tags=["bronze", "hourly"]) }}
```

```yaml
overrides:
  tag.bronze:
    runner: spark
    worker_type: G.2X
  tag.hourly:
    command: build
```

Effective settings for this model: `runner=spark`, `worker_type=G.2X`,
`command=build`.

If two tags disagree on the SAME field, dbt-aws raises `ValueError` at
DAG-build time. Fix by removing one of the tags or aligning the
overrides.

## Per-node still wins

An `overrides[model.<uid>]` entry ALWAYS wins per-field over any
matching `tag.<t>` entry, even when the model carries the tag:

```yaml
overrides:
  tag.bronze:
    worker_type: G.2X
  model.proj.heavy_aggregate:
    worker_type: G.4X          # this wins for heavy_aggregate specifically
```

## Python-only alternative

If you don't use a YAML config, pass `tag_overrides=` directly on
`DbtDag` / `DbtTaskGroup`:

```python
dag = DbtDag(
    dag_id="my_dbt",
    ...,
    runners={"spark": spark_runner, "shell": shell_runner},
    default_runner="spark",
    tag_overrides={
        "bronze": {"runner": "spark", "worker_type": "G.2X", "command": "build"},
        "landing": {"runner": "shell", "profile_name": "shell_prof"},
    },
)
```

## See also

- [Routing concept doc](../concepts/routing.md) — precedence ladder,
  full field schema, conflict rules.
- [Runner-config YAML reference](../reference/runner-config-yaml.md) —
  the complete YAML schema.
- [Task-collapse by tag](tag-groups-bulk-collapse.md) — a different
  feature that FUSES tagged nodes into one Airflow task.
