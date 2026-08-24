# Routing — unified `overrides:` block

 there is exactly one YAML shape for customising how a dbt node
runs: the `overrides:` block. It accepts two entry kinds with an identical
field schema:

- `tag.<name>:` — bulk-by-tag. Every dbt node carrying `<name>` inherits
  the fields declared here.
- `model.<pkg>.<name>:` (or `seed.<pkg>.<name>:`, `snapshot.<pkg>.<name>:`,
  `test.<pkg>.<name>:`, `analysis.<pkg>.<name>:`) — per-node. Keyed by
  dbt's `unique_id`.

Runner selection is one of the fields on that schema:

```yaml
overrides:
  tag.bronze:
    runner: spark          # every bronze-tagged node runs on `spark`
  model.proj.hotfix:
    runner: shell          # this one node overrides the bronze default
```

## Precedence per field (top wins)

```
1. overrides[model.<uid>].<field>     -- per-node
2. node.meta.stratus.<field>          -- dbt-side, per-node
3. overrides[tag.<t>].<field>         -- bulk-by-tag
4. runner default                     -- runners: / runner: block
5. DAG-level target=                  -- for `target` only
```

Model overrides ALWAYS win per-field over tag overrides — even when the
model carries the tag.

## Full field schema

Whatever the selected runner's `OVERRIDE_TYPE` accepts. For Glue Spark
that includes: `runner`, `target`, `profile_name`, `command`,
`full_refresh`, `vars_json`, `worker_type`, `number_of_workers`,
`timeout_minutes`, `job_name`, `mode`, `iam_role_name`,
`script_location`, `concurrent_runs`. Unknown fields raise
`RunnerConfigError` at load time naming the accepted set.

## YAML (unified shape)

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
  # Bulk-by-tag: landing models run on the shell runner with their own
  # dbt profile + target.
  tag.landing:
    runner: shell
    profile_name: shell_prof
    target: shell_dev

  # Bulk-by-tag: bronze bumps sizing + switches verb to `dbt build`.
  tag.bronze:
    runner: spark
    worker_type: G.2X
    number_of_workers: 4
    command: build

  # Per-node: heavy aggregate needs bigger workers than the bronze tag.
  # These fields win over anything `tag.bronze` says.
  model.proj.heavy_aggregate:
    worker_type: G.4X
    number_of_workers: 8

  # Per-node: hotfix a single model onto the shell runner + switch verb.
  model.proj.hotfix:
    runner: shell
    command: build
```

## Python (kwargs on `DbtDag` / `DbtTaskGroup`)

Two equivalent kwarg shapes:

**Recommended: forward `config=`** — no per-field plumbing:

```python
from dbt_aws.common import load_runner_config
from dbt_aws.common.builder import DbtDag

cfg = load_runner_config("runner.yml")
dag = DbtDag(
    dag_id="my_dbt",
    project=ProjectConfig(...),
    project_archive_s3="s3://.../archive.tar.gz",
    config=cfg,                       # ships every routing field
    start_date=datetime(2026, 1, 1),
)
```

**Or explicit `tag_overrides=` + `overrides=`**:

```python
dag = DbtDag(
    dag_id="my_dbt",
    ...,
    runners={"spark": spark_runner, "shell": shell_runner},
    default_runner="spark",
    tag_overrides={
        "landing": {"runner": "shell", "profile_name": "shell_prof", "target": "shell_dev"},
        "bronze":  {"worker_type": "G.2X", "number_of_workers": 4, "command": "build"},
    },
    overrides={
        "model.proj.heavy_aggregate": {"worker_type": "G.4X", "number_of_workers": 8},
        "model.proj.hotfix":          {"runner": "shell", "command": "build"},
    },
)
```

## Conflict rules

- **Same tag defined twice**: `overrides[tag.foo]` appearing twice raises
  `RunnerConfigError` at load time. One entry per tag.
- **A node carries two tags whose `overrides[tag.*]` disagree on the same
  field**: raises `ValueError` at DAG-build time (matches the existing
  dispatch-conflict guard). Fix by removing one of the tags or aligning
  the overrides.
- **Unknown field on any entry**: `RunnerConfigError` naming the accepted
  fields for the selected runner's `OVERRIDE_TYPE`.
- **Comma-separated tag key** (`tag.a,tag.b:`): rejected. Declare one
  entry per tag.

## `mode: group` -- task collapse

`overrides[tag.<name>]: {mode: group, ...}` is a different flavour of the
same `overrides:` section -- it collapses every dbt node carrying the
tag into ONE Airflow task per `(name, runner)` bucket. Uses the SAME
field schema as the default `mode: single` case; the only difference
is the emitted graph shape:

- `mode: single` (default) leaves each node as its own Airflow task.
  An optional `name:` becomes a task-id prefix (`<name>__<uid>`) so
  tagged siblings sort together in the Graph view.
- `mode: group` fuses tagged nodes into one collapsed task. An
  optional `name:` sets the collapsed task's id (defaults to the tag
  name itself).

Both flavours honour the same pull-out rule: a node with a per-model
`overrides[<uid>]` entry (any field) is pulled OUT of the group into a
singleton task, keeping per-model behaviour untouched.

!!! info "Migration from earlier releases `tag_groups:`"

    The earlier releases top-level `tag_groups:` YAML key is a hard error at
    load time. Fold each entry into `overrides[tag.<t>]: {mode: group,
    name: ..., ...group-level defaults}`. See
    [YAML config schema](../reference/runner-config-yaml.md#rejected-top-level-keys-tag_runners-tag_profiles-tag_targets-tag_groups)
    for a side-by-side migration example.

See [Task-collapse by tag](../how-to/tag-groups-bulk-collapse.md).

## Removed

The earlier releases top-level keys `tag_runners:`, `tag_profiles:`,
`tag_targets:` were removed. The loader raises `RunnerConfigError` with
a per-key migration example. See the
the [Reference → YAML config](../reference/runner-config-yaml.md) for full details.

Migration is mechanical:

```yaml
# BEFORE (0.5.x)
tag_runners:
  bronze: shell
tag_profiles:
  bronze: bronze_prof
tag_targets:
  bronze: bronze_target

# AFTER
overrides:
  tag.bronze:
    runner: shell
    profile_name: bronze_prof
    target: bronze_target
```
