# Precedence ladder

`dbt-aws` resolves per-node configuration through five ordered layers.
This page is the canonical reference; every other doc page's precedence
blurb should link here rather than re-describe the ladder.

## The ladder — higher number wins

```
1. Runner constructor defaults           (runners: block)
2. overrides.all                          
3. overrides[tag.<name>]                 (bulk-by-tag)
4. node.meta.stratus.<field>             (per-model, in dbt project)
5. overrides[<unique_id>]                (per-model, in runners.yml)
```

For the three dispatch fields (`runner`, `profile_name`, `target`) there
is a sixth layer:

```
6. DbtDag(target=...)                    (DAG-level default; target only)
```

Higher-numbered layers win per key. `setdefault` semantics mean each
layer only writes fields the layer above hasn't already set — so a
per-model override never gets clobbered by a broader-scope layer.

## Why `all:` is layer 2

`overrides.all` is the broadest selector — it applies to every node
in the rendered graph (after `select` / `exclude`). By placing it
above runner defaults but below tag / meta / per-node, we preserve
every existing escape hatch:

- A tag override on one layer can override the `all:` default.
- A `meta.stratus` block in the dbt project can override both.
- A per-node override in `runners.yml` wins over everything except
  the `DbtDag(target=...)` DAG-level default (for `target` only).

This means "test mode: run everything as one collapsed task" via
`overrides.all: {mode: group}` still lets a specific model opt out
via its own per-model override entry, which pulls it back to a
singleton task.

## Worked example

```yaml
runners:
  spark:
    type: glue_spark
    job_name: prod
    worker_type: G.1X          # layer 1: runner default

default_runner: spark

overrides:
  all:
    worker_type: G.2X          # layer 2: applies to everyone by default

  tag.gold:
    worker_type: G.4X          # layer 3: overrides layer 2 for gold-tagged nodes

  model.proj.hotpath:
    worker_type: G.8X          # layer 5: overrides everything for this one node
```

Effective `worker_type` per node:

| Node | Tags | meta.stratus? | Effective | Won by |
|---|---|---|---|---|
| `model.proj.orders` | `[]` | none | `G.2X` | layer 2 (`all:`) |
| `model.proj.customers` | `[bronze]` | none | `G.2X` | layer 2 (`all:`) -- no `tag.bronze` entry |
| `model.proj.revenue` | `[gold]` | none | `G.4X` | layer 3 (`tag.gold:`) |
| `model.proj.hotpath` | `[gold]` | none | `G.8X` | layer 5 (per-node) |

## Layer-by-layer breakdown

### Layer 1 — runner constructor default

The value you pass to the runner's `__init__` (or set on the
`runners:` block in YAML). If no override layer wins, this is what
the runner uses.

```yaml
runners:
  spark:
    type: glue_spark
    worker_type: G.1X          # every node runs on G.1X unless overridden
```

### Layer 2 — `overrides.all`

Broadest-scope override entry. Applies to every node in the
rendered graph. Same field schema as tag / per-node entries:

- Accepts every field the selected runner's `OVERRIDE_TYPE` supports.
- Accepts the meta-keys `mode: single | group` (default: `single`)
  and `name: <str>`.
- Skipped by dispatch resolvers when a higher layer (tag / meta /
  per-node) supplies the same field.

Full schema in [YAML config → Top-level `overrides.all:`](runner-config-yaml.md#overridesall).

### Layer 3 — `overrides[tag.<name>]`

Applies to every node carrying the named tag. Same field schema as
per-node. If a node carries two tags whose entries disagree on the
same field, the loader raises `ValueError` at DAG-parse.

### Layer 4 — `node.meta.stratus`

Per-model override declared inside the dbt project (`meta:` block on
a model). Reserved for cases where the config belongs next to the
SQL. Rarely the right layer -- prefer the per-node entry in
`runners.yml` unless there's a specific reason.

### Layer 5 — `overrides[<unique_id>]`

Per-model escape hatch. Wins over everything except the DAG-level
`target=` kwarg. Use for the one weird model that needs different
compute / a different runner / a different dbt command.

### Layer 6 — `DbtDag(target=...)` (target only)

DAG-level default for the dbt `--target` flag. Applies when no
layer above set `target` on the node. Kept as a separate layer
because it's a Python kwarg, not a YAML entry.

## Dispatch fields vs generic fields

Three fields have dedicated resolvers with the ladder above:

- **`runner`** — which named runner executes the node. Resolved by
  `_resolve_node_runners` in `dbt_aws/common/builder.py`.
- **`profile_name`** — dbt `--profile` for this node.
- **`target`** — dbt `--target` for this node.

Every other field (`worker_type`, `number_of_workers`, `command`,
`full_refresh`, `vars_json`, `timeout_minutes`, etc.) goes through
the same ladder via `_apply_all_override_to_effective` +
`_apply_tag_overrides_to_effective` (which fold layer 2 and layer 3
into the effective override bucket) followed by `resolve_override`
inside each runner's `make_task` (which merges layers 4 and 5).

## Pull-out rule for `mode: group`

When either `overrides.all: {mode: group}` or
`overrides[tag.<t>]: {mode: group}` is set, the builder collapses
matching nodes into one Airflow task per `(name, runner)` bucket.

A node with **any** per-model override entry (in `overrides[<uid>]`
or `node.meta.stratus`, beyond the dispatch-only `runner` key) gets
pulled OUT of the group into a singleton task. This preserves
per-model behaviour when it disagrees with the group-level config.

```yaml
overrides:
  all:
    mode: group
    name: dbt_all              # collapses everything into one task

  model.proj.hotpath:
    worker_type: G.8X          # per-model override -> pulled out
```

The pulled-out node runs as its own Airflow task with its own
config; the rest of the graph still batches under `dbt__dbt_all__<runner>`.
The pull-out can cause the remaining group to split into multiple
tasks if it disconnects the graph (each connected subgraph becomes
its own collapsed task with a `__<idx>` suffix).

## Conflict handling

- **Two tags on one node with disagreeing values for the same field
  (via `overrides[tag.<t>]`)** — `ValueError` at DAG-parse. Fix by
  aligning the values or removing one tag.
- **Two tags on one node with disagreeing task-id prefixes (via
  `overrides[tag.<t>]: {mode: single, name: ...}`)** — `ValueError`.
- **`overrides.all` conflict with another layer** — never an error;
  higher layer wins per key. `all:` is by design the lowest override
  layer.
