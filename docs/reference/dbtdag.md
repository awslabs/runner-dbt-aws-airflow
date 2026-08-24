# DbtDag / DbtTaskGroup

Both classes share the same construction pipeline ([Architecture](../concepts/architecture.md))
and accept the same kwargs. Differences:

| Class | Returns | Use inside `with DAG(...)`? |
|---|---|---|
| `DbtDag` | A populated `airflow.sdk.DAG` | No — it IS the DAG |
| `DbtTaskGroup` | A populated `airflow.sdk.TaskGroup` | Yes — embed inside your own DAG |

## Constructor — `DbtDag`

```python
class DbtDag(DAG):
    def __init__(
        self,
        *,
        dag_id: str,
        project: ProjectConfig,
        runner: Runner | None = None,
        runners: dict[str, Runner] | None = None,
        default_runner: str | None = None,
        project_archive_s3: str,
        target: str = "dev",
        select: list[str] | None = None,
        exclude: list[str] | None = None,
        overrides: dict[str, dict[str, Any]] | None = None,
        tag_overrides: dict[str, dict[str, Any]] | None = None,
        tag_runners: dict[str, str] | list[dict[str, Any]] | None = None,
        tag_profiles: dict[str, str] | list[dict[str, Any]] | None = None,
        tag_targets: dict[str, str] | list[dict[str, Any]] | None = None,
        tag_groups: dict[str, Any] | list[dict[str, Any]] | None = None,
        task_groups: TaskGroupingConfig | None = None,
        config: LoadedRunnerConfig | None = None,
        airflow_kwargs_per_task: dict[str, Any] | None = None,
        collapse_strategy: CollapseStrategy | None = None,
        drop_ephemeral: bool = True,
        **dag_kwargs: Any,
    ) -> None: ...
```

### Required

| Field | Type | Meaning |
|---|---|---|
| `dag_id` | `str` | Airflow DAG id. |
| `project` | [`ProjectConfig`](#projectconfig) | How to load the dbt graph. |
| `project_archive_s3` | `str` | `s3://bucket/key.tar.gz` URI workers download. Typically from [`build_and_upload_project_archive`](../concepts/deployment.md#project-archive-build_and_upload_project_archive). |
| **One of** `runner` **or** `runners` | `Runner` / `dict[str, Runner]` | Single runner OR multi-runner map. |

### Runner selection

| Field | Default | Meaning |
|---|---|---|
| `runner` | `None` | Single-runner shortcut. Mutually exclusive with `runners`. |
| `runners` | `None` | `{name: Runner}` for multi-runner DAGs. |
| `default_runner` | `None` | Required when `runners=` is set. Name of the runner used when no override / tag matches. |

### Selection & routing

| Field | Default | Meaning |
|---|---|---|
| `target` | `"dev"` | dbt target name passed to every worker. |
| `select` | `None` | List of dbt-style selectors (UNION). `None` = every node. |
| `exclude` | `None` | List of selectors to subtract. |
| `overrides` | `None` | Per-node `{unique_id: {field: value}}`. See [Routing → YAML unified shape](../concepts/routing.md#yaml-unified-shape). |
| `tag_overrides` | `None` | Bulk-by-tag `{tag: {field: value}}` (the current shape). Same field schema as `overrides` but keyed by tag. |
| `tag_runners` | `None` | Bulk tag→runner map. Kept as a Python-level back-compat kwarg; prefer `overrides[tag.<t>].runner` in YAML. |
| `tag_profiles` | `None` | Bulk tag→dbt-profile-name map. Same back-compat note as `tag_runners`. |
| `tag_targets` | `None` | Bulk tag→dbt-target map. Same back-compat note. |
| `tag_groups` | `None` | Bulk-by-tag task collapse. Kept as a Python-level back-compat kwarg; prefer `overrides[tag.<t>]: {mode: group, name: ...}` in YAML. |
| `task_groups` | `None` | `TaskGroupingConfig` for visual nesting only (one Airflow task per node, folded into UI folders). See [Visual grouping](../concepts/visual-grouping.md). |
| `config` | `None` | `LoadedRunnerConfig` from `load_runner_config()`. Auto-wires every routing field the caller didn't pass explicitly. **Recommended** entry point for YAML users — hides the split between the fields above. |
| `collapse_strategy` | `None` | `"view_chain"` / `"aggressive"` structural collapse (see [Concepts → Task-collapse](../concepts/collapse.md)). |
| `drop_ephemeral` | `True` | Drop dbt `ephemeral` models from the Airflow graph (they're inlined into consumers by dbt). |

### Operator-level

| Field | Default | Meaning |
|---|---|---|
| `airflow_kwargs_per_task` | `None` | Forwarded to every underlying operator (`retries`, `execution_timeout`, `pool`, …). |
| `**dag_kwargs` | — | Passed straight to `airflow.sdk.DAG` (`schedule`, `start_date`, `tags`, `catchup`, …). |

## Constructor — `DbtTaskGroup`

Same signature, except:

- `dag_id` → `group_id`
- `**dag_kwargs` → `**task_group_kwargs` (passed to `TaskGroup`)
- Must be constructed inside `with DAG(...)`.

```python
with DAG(dag_id="hybrid", start_date=..., schedule=None) as dag:
    pre = PythonOperator(...)
    dbt_tg = DbtTaskGroup(
        group_id="dbt_run",
        project=ProjectConfig(...),
        runner=GlueSparkRunner(...),
        project_archive_s3="s3://...",
        # same select / exclude / overrides / tag_overrides / config /
        # task_groups kwargs as DbtDag
    )
    post = PythonOperator(...)
    pre >> dbt_tg >> post
```

## `ProjectConfig`

Three modes for telling the lib how to load the dbt graph:

```python
from dbt_aws.common import ProjectConfig

# 1. From a pre-built manifest.json (RECOMMENDED -- fast, deterministic)
ProjectConfig(mode="manifest", manifest_path=Path("dbt_project/target/manifest.json"))

# 2. From an in-memory dict (tests, dynamic graph construction)
ProjectConfig(mode="manifest", manifest_dict={"metadata": {...}, "nodes": {...}})

# 3. Parse the project at DAG-import time (slowest -- runs `dbt parse`)
ProjectConfig(mode="mwaa_parse", project_dir=Path("dbt_project"))
```

## Validation order

Every error is raised at `__init__` time (DAG-parse, before any task runs):

1. `runner=` vs `runners=` mutual exclusivity (`ValueError`)
2. `runners=` requires `default_runner=` (`ValueError`)
3. `default_runner` is a valid key (`ValueError`)
4. `tag_runners` shape + runner names + tag conflicts (`ValueError`)
5. `task_groups` shape + tag uniqueness + group names (raised by `TaskGroupingConfig` ctor)
6. `project` mode + manifest reachable (raised by `load_graph`)
7. Selector syntax (raised by `apply_selectors`)
8. Per-node `_resolve_node_runners` — resolved runner must exist, no tag-routing
   conflicts (`ValueError` naming the conflicting `tag -> runner` pairs)
9. Per-node override field validity vs runner's `OVERRIDE_TYPE` (`OverrideError`)
10. Per-node task-group assignment uniqueness (`ValueError` if node matches two groups)

Once `DbtDag.__init__` returns, you know the DAG is structurally valid.

## Parse-time log lines

Every parse emits structured logs under the `dbt_aws.common.builder` logger:

```
[info] DbtDag: starting (dag_id=..., project_mode=manifest, target=dev, runners=['glue_spark','session_warm'])
[info] selectors: select '+gd_top_customers+' matched 17 node(s)
[info] runner distribution: glue_spark=9, session_warm=22, session_per_node=1
[warn] tag_runners declares tag(s) ['typo'] but no selected node carries them ...
[info] DbtDag: built my_da(32 task(s), 47 edge(s))
```

Use these to verify routing + grouping resolved as you expected without triggering a run.
