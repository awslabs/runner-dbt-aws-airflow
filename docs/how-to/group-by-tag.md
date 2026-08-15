# Visual groups in the UI

Collapsible folders in the Airflow grid view, one per dbt layer. Full concept docs:
[Concepts → Visual grouping](../concepts/visual-grouping.md).

## The simplest example

```python
from dbt_aws.common import TaskGroupConfig, TaskGroupingConfig

dag = DbtDag(
    ...,
    task_groups=TaskGroupingConfig(
        groups=(
            TaskGroupConfig(name="bronze", tags=frozenset({"bronze"})),
            TaskGroupConfig(name="silver", tags=frozenset({"silver"})),
            TaskGroupConfig(name="gold",   tags=frozenset({"gold"})),
        ),
        ungrouped_group="other",          # fallback name for unmatched tasks
    ),
)
```

YAML equivalent:

```yaml
task_groups:
  - name: bronze
    tags: [bronze]
  - name: silver
    tags: [silver]
  - name: gold
    tags: [gold]
ungrouped_group: other
```

## What the UI looks like

```
my_dag
├─ ▸ bronze (8)    ← collapsible
├─ ▸ silver (5)
├─ ▸ gold (4)
└─ ▸ other (15)
```

Click any folder to expand. The number is the task count inside the group.

## Group by multiple tags

`task_groups[i].tags` is a set. Any matching tag puts the node in the group:

```python
TaskGroupConfig(
    name="dimensions",
    tags=frozenset({"dim", "scd", "lookup"}),   # any of these tags -> "dimensions"
)
```

A node with `tags=['silver', 'dim']` would go into the group whose tag-set contains `dim` —
but only one. Two groups claiming the same node raises:

```
ValueError: node 'model.proj.x' matches multiple task_groups
(['dimensions', 'silver']) -- a model must belong to exactly one group.
Disambiguate by retagging or by removing the overlapping group.
```

## Independent from `tag_runners`

You can group visually one way and route execution another way:

```python
dag = DbtDag(
    runners={"warm": ..., "spark": ..., "iso": ...},
    default_runner="warm",
    tag_runners={
        "bronze": "spark",                # bronze tasks run on spark
        "silver,gold": "warm",            # silver+gold run on warm
    },
    task_groups=TaskGroupingConfig(
        groups=(
            TaskGroupConfig(name="ingestion",   tags=frozenset({"bronze"})),
            TaskGroupConfig(name="curated",     tags=frozenset({"silver"})),
            TaskGroupConfig(name="analytics",   tags=frozenset({"gold"})),
        ),
        ungrouped_group="utility",
    ),
)
```

UI shows `ingestion` / `curated` / `analytics` / `utility` folders. Tasks inside
`ingestion` run on `spark`; tasks inside `curated` + `analytics` run on `warm`.

## Nesting groups

`TaskGroupingConfig` produces a *flat* layout. For nested hierarchies (`warehouse/bronze`,
`warehouse/silver`), use `DbtTaskGroup` building blocks inside your own DAG:

```python
with DAG(dag_id="hybrid") as dag:
    with TaskGroup(group_id="warehouse"):
        DbtTaskGroup(
            group_id="bronze",
            project=ProjectConfig(...),
            runner=GlueSparkRunner(...),
            project_archive_s3=...,
            select=["tag:bronze"],
        )
        DbtTaskGroup(
            group_id="silver",
            project=ProjectConfig(...),
            runner=GlueInteractiveSessionRunner(reusable=True, ...),
            project_archive_s3=...,
            select=["tag:silver"],
        )
```

Each `DbtTaskGroup` applies its own selectors, so the same manifest is filtered into the
right TaskGroup.
