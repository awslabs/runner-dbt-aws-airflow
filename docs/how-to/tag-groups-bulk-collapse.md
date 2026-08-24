# Bulk-by-tag task collapse (`overrides[tag.*]: {mode: group}`)

When a whole layer of dbt models shares one runner and one set of
options, running each model as its own Airflow task wastes cold-starts.
Add `mode: group` to a `tag.<name>` entry under `overrides:` and every
tagged model collapses into ONE Airflow task per `(name, runner)`
bucket, running `dbt run --select A B C ...` on the assigned runner.
dbt-core handles internal execution order.

!!! note "Legacy shape"

    Older configs used a separate top-level `tag_groups:` YAML key.
    That key is a hard error at load time. Fold each entry into
    `overrides[tag.<t>]: {mode: group, name: ..., ...}` with the same
    field schema — no other behaviour change. See
    [YAML config schema → rejected keys](../reference/runner-config-yaml.md#rejected-top-level-keys-tag_runners-tag_profiles-tag_targets-tag_groups)
    for a side-by-side migration example.

## Minimum viable example

```yaml
# runners.yml
runner:
  type: glue_spark
  job_name: glue-etl
  mode: attach
  aws_conn_id: aws_default

overrides:
  # 8 models tagged "bronze" -> 1 Airflow task running
  # `dbt run --select b1 b2 b3 b4 b5 b6 b7 b8`
  tag.bronze:
    mode: group
    name: bronze_batch      # optional -- defaults to "bronze"
```

```python
from dbt_aws.common import load_runner_config
from dbt_aws.common.builder import DbtDag

cfg = load_runner_config("runners.yml")

dag = DbtDag(
    dag_id="daily",
    project=...,
    project_archive_s3="s3://...",
    config=cfg,
)
```

Produces one task named `dbt__bronze_batch__spark`. Untagged models
stay one-task-per-node.

## Rich form: group-level overrides

Any field the selected runner's `OVERRIDE_TYPE` accepts is valid inside
a `mode: group` entry. Set `command`, `target`, `profile_name`,
`full_refresh`, `vars_json`, etc. for the whole group at once:

```yaml
overrides:
  tag.gold:
    mode: group
    name: gold_batch
    runner: spark
    command: build            # dbt build instead of dbt run
    target: prod
    profile_name: gold_prof
    full_refresh: true
```

Every non-pulled-out member of `gold_batch` gets those overrides
injected at task-build time.

## Multi-runner sub-grouping

If a tag's nodes route to more than one runner (via another
`overrides[tag.<t>]: {runner: ...}` entry pointing a subset elsewhere,
or via `meta.stratus.runner` on individual nodes), the group splits
into one Airflow task per runner:

```yaml
runners:
  spark:  {type: glue_spark,        job_name: glue-etl}
  shell:  {type: glue_python_shell, job_name: glue-pyshell}
default_runner: spark

overrides:
  # Every bronze model joins the group. Routing (which runner) is
  # decided independently per node -- e.g. models also tagged
  # "landing" go to shell:
  tag.landing:
    runner: shell             # dispatch-only override; no mode -> stays single by default
  tag.bronze:
    mode: group
    name: bronze_batch
```

If out of 5 bronze-tagged models, 3 stay on `spark` and 2 also carry
tag `landing` (→ `shell`), you get **two** grouped tasks:

- `dbt__bronze_batch__spark` (3 members)
- `dbt__bronze_batch__shell` (2 members)

## Pull-out rule — preserve per-model behaviour

A node carrying ANY per-model override (in `overrides[<uid>]` or
`meta.stratus`, beyond the dispatch-only `runner` key) is pulled OUT of
the group and stays a singleton task. This means declaring a
group-level `command: build` never silently ignores a model's own
`command: snapshot` — the model stays its own task and its own config
wins.

```yaml
overrides:
  tag.bronze:
    mode: group
    name: bronze_batch
    command: build

  # `hotfix_customer` overrides one field -> pulled out as a singleton.
  model.proj.hotfix_customer:
    command: run             # forces `dbt run`, not `dbt build`
    target: hotfix_prod
```

Result: `hotfix_customer` runs on its own task with the caller's
config; the remaining bronze models still batch under
`bronze_batch__spark`.

The builder INFO-logs which nodes get pulled out:

```
INFO  dbt_aws.common.builder  tag_groups: 1 node(s) pulled out as singletons
    due to per-model overrides: ['model.proj.hotfix_customer']
```

## `mode: single` with a `name:` (task-id prefix)

The default `mode: single` keeps one Airflow task per node. In 
a `name:` on a `mode: single` entry becomes a task-id PREFIX:

```yaml
overrides:
  tag.bronze:
    mode: single             # (default; can be omitted)
    name: bronze             # -> task-ids ``bronze__model__proj__b1`` etc.
    runner: spark
    worker_type: G.2X
```

Every `bronze`-tagged node gets `bronze__` prepended to its task-id, so
siblings sort together in the Airflow Graph view. Non-bronze nodes are
untouched. Multi-tag nodes whose applying tags disagree on `name:`
raise `ValueError` at DAG-parse (tag-conflict rule).

Difference from `mode: group`:

| Mode | Airflow tasks per tag | `name:` semantics |
|---|---|---|
| `single` (default) | one per node | task-id **prefix** (`<name>__<uid>`) |
| `group` | one per `(name, runner)` bucket | collapsed **task-id** (defaults to tag) |

## Resolution ladder

Precedence for a node's final `--command` / `--target` /
`--profile-name` value, last layer wins:

```
1. overrides[<uid>][field]
2. meta.stratus[field]
3. overrides[tag.<t>][field]           (both modes; group-level defaults)
4. runner.<field>                       (runner constructor default)
5. DbtDag(target=...)                   (DAG-level default; target only)
```

## Composition with `collapse_strategy`

`collapse_strategy="view_chain"` / `"aggressive"` runs FIRST (structural
collapse). Then `mode: group` runs on top, merging what came out
further:

```python
DbtDag(
    ...,
    collapse_strategy="view_chain",     # step 1: fold view+consumer chains
    config=cfg,                         # step 2: bulk-by-tag on top
)
```

## Parse-time logging

Whenever a `mode: group` entry has at least one member:

```
INFO  dbt_aws.common.builder  tag_groups distribution:
    bronze_batch__spark=8, bronze_batch__shell=2
INFO  dbt_aws.common.builder  tag_groups: 1 node(s) pulled out as singletons
    due to per-model overrides: ['model.proj.hotfix']
```

A `WARNING` fires when a `mode: group` entry names a tag no selected
node carries (typo guard).

## What doesn't compose (yet)

- `mode: group` doesn't currently expose an `on_failure_callback:` at
  the group level. If a member fails inside a grouped `dbt run`, the
  whole Airflow task retries.
- `airflow_kwargs_per_task=` (retries, `execution_timeout`, `pool`)
  applies to every task including grouped ones — you can't yet set
  different Airflow-side kwargs per group. If you need this, raise
  an issue.
