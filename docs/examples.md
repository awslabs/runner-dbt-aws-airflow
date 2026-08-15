# Examples

Runnable DAGs and dbt-project scaffolding that demonstrate every
routing / config / lineage / collapse feature in one place. Each
example below names the file it corresponds to in the repository's
example DAGs and its sibling YAML / project scaffolding.

## Multi-runner mix (Python config)

**File:** `airflow_dag_all_runners_mix.py`

- Three runners declared inline (Glue Spark Job, Glue Session warm,
  Glue Session per-node).
- `tag_runners` routes `bronze` → glue_spark and `silver,gold` →
  session_warm.
- `overrides` pins standalone seeds (`regions`, `audit_log`) to
  specific runners.
- `task_groups` gives visual nesting: `bronze` / `silver` / `gold` /
  `other` folders.
- 36 Airflow tasks total, exercised end-to-end against real AWS in
  the maintainer's smoke suite.

```python
dag = DbtDag(
    dag_id="dbt_project__all_runners_mix",
    project=ProjectConfig(mode="manifest", manifest_path=MANIFEST),
    runners={
        "glue_spark":       glue_spark,
        "session_warm":     session_warm,
        "session_per_node": session_per_node,
    },
    tag_runners={
        "bronze": "glue_spark",
        "silver": "session_warm",
        "gold":   "session_warm",
    },
    overrides={
        "regions":   {"runner": "session_per_node"},
        "audit_log": {"runner": "glue_spark"},
    },
    task_groups=["bronze", "silver", "gold", "other"],
)
```

## Multi-runner mix (YAML config)

**Files:** `airflow_dag_all_runners_yaml.py` + `runners_all.yml`

1:1 mirror of the Python variant, but the runner objects + routing +
visual grouping live in YAML for ops-friendly editing.

```yaml title="runners_all.yml"
runners:
  glue_spark:
    type: glue_spark
    # ... runner kwargs ...
  session_warm:
    type: glue_interactive_session
    reusable: true
    # ...
  session_per_node:
    type: glue_interactive_session
    reusable: false
    # ...

tag_runners:
  bronze: glue_spark
  silver: session_warm
  gold:   session_warm

overrides:
  regions:   { runner: session_per_node }
  audit_log: { runner: glue_spark }

task_groups: [bronze, silver, gold, other]
```

The Python DAG file is a thin loader:

```python title="airflow_dag_all_runners_yaml.py"
from dbt_aws import DbtDag, ProjectConfig, load_runners

runners, tag_runners, overrides, task_groups = load_runners("runners_all.yml")

dag = DbtDag(
    dag_id="dbt_project__all_runners_yaml",
    project=ProjectConfig(mode="manifest", manifest_path=MANIFEST),
    runners=runners,
    tag_runners=tag_runners,
    overrides=overrides,
    task_groups=task_groups,
)
```

Both variants produce byte-identical DAGs; the YAML path picks up
every field.

## TPC-H medallion dbt project

**Path:** `dbt_project/` (a small TPC-H medallion on `dbt-duckdb`
with `external` Parquet materialization on S3, used by both DAGs
above).

| Layer  | Models                                                                     | Tags     | Materialization         |
|--------|----------------------------------------------------------------------------|----------|-------------------------|
| bronze | 8 (`br_customer`, `br_lineitem`, …)                                        | `bronze` | external Parquet on S3  |
| silver | 4 (`sv_dim_customer`, `sv_fact_orders`, …)                                 | `silver` | external Parquet on S3  |
| gold   | 4 (`gd_top_customers`, `gd_revenue_by_region`, …)                          | `gold`   | external Parquet on S3  |
| seeds  | `regions`, `audit_log`, `currencies`, `customers`, `orders`, `products`    | various  | —                       |

Tags are applied via `dbt_project.yml`:

```yaml
models:
  dbt_project:
    bronze:
      +tags: ["bronze"]
      +materialized: external
      +format: parquet
    silver:
      +tags: ["silver"]
      +materialized: external
      +format: parquet
    gold:
      +tags: ["gold"]
      +materialized: external
      +format: parquet
```

## Related how-tos

- [Multi-runner mix](how-to/multi-runner-mix.md) — narrative walkthrough of
  the Python vs YAML variants above.
- [Route by tag](how-to/route-by-tag.md) — how `tag_runners` picks a
  runner for each dbt node.
- [Tag groups: bulk collapse](how-to/tag-groups-bulk-collapse.md) — how
  `tag_targets` / `tag_profiles` layer under the same routing.
- [Multi-profile / multi-target](how-to/multi-profile-target.md) —
  per-tag profile + target selection.
- [Enable OpenLineage](how-to/enable-openlineage.md) — attach lineage
  events to any Glue runner in the mix.
