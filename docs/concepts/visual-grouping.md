# Visual grouping — task_groups

Collapsible folders in the Airflow grid view, one per dbt layer. **Independent of
[runner routing](routing.md)** — `task_groups` controls *which UI folder* a task lives in,
not which runner executes it.

```
dbt_project__all_runners_mix
├─ ▸ bronze   (collapsible)  -- every model carrying tag:bronze
├─ ▸ silver   (collapsible)  -- every model carrying tag:silver
├─ ▸ gold     (collapsible)  -- every model carrying tag:gold
└─ ▸ other    (collapsible)  -- everything else (fallback)
```

## Python

```python
from dbt_aws.common import TaskGroupConfig, TaskGroupingConfig

TASK_GROUPS = TaskGroupingConfig(
    groups=(
        TaskGroupConfig(name="bronze",       tags=frozenset({"bronze"})),
        TaskGroupConfig(name="silver",       tags=frozenset({"silver"})),
        TaskGroupConfig(name="gold",         tags=frozenset({"gold"})),
        TaskGroupConfig(name="dimensions",   tags=frozenset({"dim", "scd"})),
    ),
    ungrouped_group="other",   # fallback name for unmatched nodes
                               # ungrouped_group=None puts unmatched at the DAG root
)

dag = DbtDag(
    ...,
    task_groups=TASK_GROUPS,
)
```

## YAML

```yaml
task_groups:
  - name: bronze
    tags: [bronze]
  - name: silver
    tags: [silver]
  - name: gold
    tags: [gold]
  - name: dimensions
    tags: [dim, scd]

# Optional. Omit to leave unmatched models at the DAG root.
ungrouped_group: other
```

## Validation

| Failure | Result |
|---|---|
| Same tag listed in two groups | `RunnerConfigError` at config load |
| Group with empty `tags` | Error at config load |
| Node tagged with two tags from different groups | `ValueError` at DAG-parse — node must belong to exactly one group |
| Duplicate group name | Error at config load |

## Composing with tag_runners

The two features are completely independent and compose freely. The most common pattern
is to keep the *same* tag set driving both:

```python
# Tags drive both routing AND visual grouping.
TAG_RUNNERS = {
    "bronze":      "glue_spark",
    "silver,gold": "session_warm",
}
TASK_GROUPS = TaskGroupingConfig(
    groups=(
        TaskGroupConfig(name="bronze", tags=frozenset({"bronze"})),
        TaskGroupConfig(name="silver", tags=frozenset({"silver"})),
        TaskGroupConfig(name="gold",   tags=frozenset({"gold"})),
    ),
    ungrouped_group="other",
)
dag = DbtDag(..., tag_runners=TAG_RUNNERS, task_groups=TASK_GROUPS)
```

A model tagged `bronze` will:

1. Be **routed** to `glue_spark` (because `tag_runners["bronze"]="glue_spark"`)
2. Be **visually placed** in the `bronze` TaskGroup (because `task_groups.bronze.tags`
   contains `bronze`)

But you don't have to align them — a model can route to `glue_spark` AND live in the
`silver` UI folder if you want.

## Custom group hierarchy

`TaskGroupingConfig` produces a *flat* set of TaskGroups (one level under the DAG root).
For nested hierarchies (e.g. `bronze/raw`, `bronze/transformed`), use the lower-level
`DbtTaskGroup` + your own nesting:

```python
with DAG(dag_id="hybrid", ...) as dag:
    with TaskGroup(group_id="warehouse"):
        with TaskGroup(group_id="bronze"):
            DbtTaskGroup(group_id="raw",         project=..., select=["tag:bronze,tag:raw"], ...)
            DbtTaskGroup(group_id="transformed", project=..., select=["tag:bronze,tag:transformed"], ...)
        with TaskGroup(group_id="silver"):
            DbtTaskGroup(group_id="all", project=..., select=["tag:silver"], ...)
```

Each `DbtTaskGroup` parses the manifest, applies its own `select=`, and emits one task per
matching node. Edges between sibling TaskGroups are wired automatically.
